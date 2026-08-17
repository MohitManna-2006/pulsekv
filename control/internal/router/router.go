// Package router owns the two hashes that decide where a key lives.
//
// They answer different questions and must stay distinct:
//
//	key   -> shard   ShardForKey.  Fixed forever for a given ShardCount,
//	                 regardless of cluster membership.
//	shard -> node    AssignShards. Recomputed whenever membership changes.
//	shard -> owners  AssignShardOwners. The same computation carried further
//	                 down the ranking, so a shard has a primary and an ordered
//	                 list of replicas rather than a single owner.
//
// Keeping shard *identity* independent of shard *ownership* is the whole point:
// it lets ownership move during a membership change without any key changing
// which shard it belongs to. Collapsing the two into one hash-key-to-node step
// is what produces the "everything reshuffles when a node joins" behaviour this
// design exists to avoid.
//
// Both functions are pure: no config, no gRPC, no clock, no randomness. That is
// what lets router_test.go assert the movement invariant exactly rather than
// approximately, and it is what will let Phase 5's several control-plane
// replicas independently compute byte-identical shard maps without coordinating.
package router

import (
	"hash/fnv"
)

// ShardForKey maps a key to one of shardCount logical cluster shards.
//
// FNV-1a over the raw key bytes, the same family v1's hashtable and the v2
// engine already use: deterministic, dependency-free, and stable across
// processes and restarts. Explicitly NOT hash/maphash, which is seeded randomly
// per process and would give two control-plane replicas different answers for
// the same key.
//
// These cluster shards are unrelated to the engine's own 256 lock shards; those
// exist for mutex striping inside a single node.
func ShardForKey(key []byte, shardCount uint32) uint32 {
	if shardCount == 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(key) // hash.Hash.Write never returns an error
	return uint32(h.Sum64() % uint64(shardCount))
}

// nodeSeed hashes a node ID exactly once. AssignShards keeps these across the
// whole shard loop, so a 256-shard, 32-node recomputation does 32 string hashes
// instead of 8,192.
func nodeSeed(nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	return h.Sum64()
}

// mix64 is the splitmix64 finalizer: a bijection on uint64 with full avalanche,
// so a one-bit change anywhere in the input changes about half the output bits.
//
// It is here because FNV-1a alone is NOT good enough for rendezvous hashing, and
// that is not a theoretical objection -- it was measured. FNV-1a's last step is
// `h ^= b; h *= prime`, and with prime ~= 2^40 a difference in the final input
// bytes only reaches bits ~40-47. Weights are compared as whole 64-bit numbers,
// so the comparison is dominated by the high bits, which are almost entirely
// determined by the shared prefix. Node IDs that differ only in a trailing
// character -- "node-0" through "node-31", i.e. exactly what a real cluster
// looks like -- therefore get weights that are ordered the *same way* for every
// shard, and one node wins far more than its share.
//
// Hashing "<shard>:<nodeID>" with plain FNV-1a over 256 shards produced:
//
//	32 nodes   12 of 32 nodes owned ZERO shards; the busiest owned 39 (expected 8)
//	16 nodes   min 3, max 39 (expected 16)
//	4 -> 5     127 of 256 shards moved (ideal ~51)
//	8 -> 9     131 of 256 shards moved (ideal ~28)
//
// With the finalizer, over 2-64 nodes: no node ever owns zero shards, and
// movement tracks the ideal (4->5: 68, 8->9: 38, 16->17: 8, 32->33: 4).
//
// Worth being precise about what was and was not broken: the movement invariant
// held either way, because it is structural to taking an argmax per shard and
// does not depend on hash quality at all. What was broken was the distribution,
// which is why Step 2.1 asks for both kinds of test -- the invariant test alone
// would have passed on a hash this bad.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// weight scores one (shard, node) pair. Rendezvous hashing gives the shard to
// whichever node scores highest, which is what makes the assignment depend only
// on the pair itself and not on any ordering, ring position, or previous state.
//
// Split into seeds so AssignShards can hoist the per-node half out of the loop;
// this is the un-hoisted form, kept for tests and for anyone reading the
// algorithm rather than the optimisation.
func weight(shard uint32, nodeID string) uint64 {
	return mix64(nodeSeed(nodeID) ^ mix64(uint64(shard)))
}

// AssignShards computes the shard-to-owner map for a node set, using rendezvous
// (highest-random-weight) hashing.
//
// The property this buys, and the reason it was chosen over a hash ring or Jump
// Hash (design doc section 4.1): each shard's owner is independently the argmax
// over the current nodes, so a node leaving can only affect the shards it
// personally won, and a node joining can only take shards -- it can never cause
// a shard to move between two nodes that were both present before and after.
// That holds exactly, not statistically, and router_test.go asserts it as such.
//
// Cost is O(shardCount x len(nodeIDs)). At the design doc's target of 256 shards
// over tens of nodes that is a few thousand hashes per recomputation, which
// happens on a metadata read or a client topology refresh -- not on the data
// path. Maglev-style precomputed lookup tables are the documented escape hatch
// if cluster size ever makes this measurable.
//
// Returns an empty (non-nil) map when there are no nodes or no shards; callers
// get "nobody owns anything", never a nil-map panic.
func AssignShards(nodeIDs []string, shardCount uint32) map[uint32]string {
	assignment := make(map[uint32]string, shardCount)
	if len(nodeIDs) == 0 || shardCount == 0 {
		return assignment
	}

	// Hash each node ID once, not once per (shard, node) pair.
	seeds := make([]uint64, len(nodeIDs))
	for i, id := range nodeIDs {
		seeds[i] = nodeSeed(id)
	}

	for shard := uint32(0); shard < shardCount; shard++ {
		shardSeed := mix64(uint64(shard))
		var (
			bestID string
			bestW  uint64
			found  bool
		)
		for i, id := range nodeIDs {
			w := mix64(seeds[i] ^ shardSeed) // == weight(shard, id)

			// The `!found` term handles the first candidate without
			// special-casing it, and without assuming a node ID is never the
			// empty string.
			//
			// The tie-break is lexicographic and deliberate. A 64-bit weight
			// collision is vanishingly unlikely, but "unlikely" is not
			// "impossible", and leaving ties to map or slice order would make
			// two control-plane replicas disagree exactly when one happened.
			if !found || w > bestW || (w == bestW && id < bestID) {
				bestID, bestW, found = id, w, true
			}
		}
		assignment[shard] = bestID
	}
	return assignment
}

// ShardOwners is every node holding one shard, in promotion order.
//
// Replicas are ranks 2..N of the same rendezvous ranking that produced Primary,
// so "which replica gets promoted when the primary dies" needs no election and
// no coordination: it is whatever AssignShards already returns for the smaller
// node set. Replicas is nil when the replication factor is 0 or the cluster has
// no other live node to hold a copy.
type ShardOwners struct {
	Primary  string
	Replicas []string
}

// rankedNode is one candidate during the top-K selection below.
type rankedNode struct {
	id     string
	weight uint64
}

// ranksBefore is the single ordering rule shared by AssignShards and
// AssignShardOwners: higher weight wins, and an exact weight tie is broken
// lexicographically so two control-plane replicas cannot disagree.
//
// AssignShards inlines this comparison rather than calling it, because it runs
// once per (shard, node) pair on a hot recomputation path. The two must stay
// identical; TestShardOwnersPrimaryMatchesAssignShards is what enforces that.
func ranksBefore(a, b rankedNode) bool {
	return a.weight > b.weight || (a.weight == b.weight && a.id < b.id)
}

// AssignShardOwners computes primary-plus-replica placement for a node set.
//
// For each shard it takes the top 1+replicaFactor distinct nodes by the exact
// weight function AssignShards uses; rank 1 is the primary and ranks 2..N are
// the replicas in order. That is what makes AssignShardOwners(...)[s].Primary
// identically equal to AssignShards(...)[s] for every input, which in turn is
// what lets every Phase 2/3 caller keep reading shard_to_node_id unmodified.
//
// Fewer than 1+replicaFactor live nodes is not an error. The shard simply gets
// as many replicas as there are other nodes, which is the honest answer: a
// three-node cluster cannot hold three copies plus a primary, and refusing to
// publish a map would be strictly worse than publishing a less-replicated one.
//
// A negative replicaFactor is treated as 0 rather than rejected, so a
// mis-parsed config degrades to "no replication" instead of panicking a
// control plane.
//
// Cost is O(shardCount x len(nodeIDs) x replicaFactor). The K in the top-K is
// 1, 2, or 3 in every configuration the design doc contemplates, so the
// insertion scan below is cheaper than sorting all N candidates per shard.
func AssignShardOwners(nodeIDs []string, shardCount uint32, replicaFactor int) map[uint32]ShardOwners {
	assignment := make(map[uint32]ShardOwners, shardCount)
	if len(nodeIDs) == 0 || shardCount == 0 {
		return assignment
	}
	if replicaFactor < 0 {
		replicaFactor = 0
	}

	// Same hoist as AssignShards: hash each node ID once for the whole loop.
	seeds := make([]uint64, len(nodeIDs))
	for i, id := range nodeIDs {
		seeds[i] = nodeSeed(id)
	}

	wanted := replicaFactor + 1
	if wanted > len(nodeIDs) {
		wanted = len(nodeIDs)
	}
	top := make([]rankedNode, 0, wanted)

	for shard := uint32(0); shard < shardCount; shard++ {
		shardSeed := mix64(uint64(shard))
		top = top[:0]

		for i, id := range nodeIDs {
			candidate := rankedNode{id: id, weight: mix64(seeds[i] ^ shardSeed)}

			// Reject early once the list is full and this candidate cannot
			// displace even the last entry.
			if len(top) == wanted && !ranksBefore(candidate, top[len(top)-1]) {
				continue
			}
			if len(top) < wanted {
				top = append(top, rankedNode{})
			}
			position := len(top) - 1
			for position > 0 && ranksBefore(candidate, top[position-1]) {
				top[position] = top[position-1]
				position--
			}
			top[position] = candidate
		}

		owners := ShardOwners{Primary: top[0].id}
		if len(top) > 1 {
			owners.Replicas = make([]string, 0, len(top)-1)
			for _, replica := range top[1:] {
				owners.Replicas = append(owners.Replicas, replica.id)
			}
		}
		assignment[shard] = owners
	}
	return assignment
}

// PrimaryMap projects an owner map down to the plain shard-to-primary map that
// every pre-Phase-4 caller expects. Useful for asserting the two agree.
func PrimaryMap(owners map[uint32]ShardOwners) map[uint32]string {
	primaries := make(map[uint32]string, len(owners))
	for shard, owner := range owners {
		primaries[shard] = owner.Primary
	}
	return primaries
}

// OwnerForKey resolves a key straight to its owning node ID, given a shard map.
//
// shardCount is taken as len(shardMap) by the callers that have only the map;
// pass it explicitly here so this stays a pure function over exactly the inputs
// it uses. ok is false when the map has no entry for the key's shard, which
// means the caller is holding an incomplete or stale map -- worth surfacing
// rather than silently routing to the zero value.
func OwnerForKey(key []byte, shardCount uint32, shardMap map[uint32]string) (string, bool) {
	nodeID, ok := shardMap[ShardForKey(key, shardCount)]
	return nodeID, ok && nodeID != ""
}
