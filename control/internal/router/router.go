// Package router owns the two hashes that decide where a key lives.
//
// They answer different questions and must stay distinct:
//
//	key   -> shard   ShardForKey.  Fixed forever for a given ShardCount,
//	                 regardless of cluster membership.
//	shard -> node    AssignShards. Recomputed whenever membership changes.
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
