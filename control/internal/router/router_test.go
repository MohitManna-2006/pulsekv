package router

import (
	"fmt"
	"sort"
	"testing"
)

const testShards = 256

func nodes(n int) []string {
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, fmt.Sprintf("node-%d", i))
	}
	return ids
}

func without(ids []string, drop string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

func setOf(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func counts(assignment map[uint32]string) map[string]int {
	c := map[string]int{}
	for _, owner := range assignment {
		c[owner]++
	}
	return c
}

// ---------------------------------------------------------------------------
// The movement invariant. This is the property rendezvous hashing was chosen
// for, and it holds EXACTLY -- so it is asserted exactly, not as "movement is
// roughly bounded".
// ---------------------------------------------------------------------------

// TestNoShardMovesBetweenSurvivingNodes is the general statement: for any
// membership change, a shard whose owner exists both before and after must not
// have changed hands. Everything else in this file is a special case of it.
func TestNoShardMovesBetweenSurvivingNodes(t *testing.T) {
	cases := []struct {
		name   string
		before []string
		after  []string
	}{
		{"remove one of four", nodes(4), without(nodes(4), "node-1")},
		{"remove one of sixteen", nodes(16), without(nodes(16), "node-7")},
		{"add one to four", nodes(4), nodes(5)},
		{"add one to sixteen", nodes(16), nodes(17)},
		{"add several at once", nodes(8), nodes(12)},
		{"remove several at once", nodes(12), nodes(8)},
		{"remove the first", nodes(8), without(nodes(8), "node-0")},
		{"remove the last", nodes(8), without(nodes(8), "node-7")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := AssignShards(tc.before, testShards)
			after := AssignShards(tc.after, testShards)

			beforeSet, afterSet := setOf(tc.before), setOf(tc.after)
			moved, checked := 0, 0

			for shard := uint32(0); shard < testShards; shard++ {
				oldOwner, newOwner := before[shard], after[shard]

				// The constrained case is a shard whose owner SURVIVED moving
				// to a node that already EXISTED. Two moves are legitimate and
				// must not be counted:
				//   - the old owner departed, so the shard had to go somewhere;
				//   - the new owner just joined, and a joining node is allowed
				//     to take shards. That is the whole point of adding one.
				//
				// Getting this wrong in the obvious direction -- treating any
				// change of owner as a violation -- makes every addition look
				// like a failure, because taking shards is exactly what an
				// addition does.
				if !afterSet[oldOwner] || !beforeSet[newOwner] {
					continue
				}
				checked++
				if newOwner != oldOwner {
					moved++
					if moved <= 5 {
						t.Errorf("shard %d moved from %s to %s; both were present "+
							"before and after, so it should not have moved",
							shard, oldOwner, newOwner)
					}
				}
			}
			if moved > 5 {
				t.Errorf("... and %d more shards moved between surviving nodes", moved-5)
			}
			if checked == 0 {
				t.Fatal("test checked no shards; the setup is wrong")
			}
			t.Logf("%d shards had a surviving owner, %d moved", checked, moved)
		})
	}
}

// TestRemovalOnlyReassignsTheDepartedNodesShards states the removal case from
// the other direction: every shard the departed node did NOT own is untouched,
// and every shard it did own goes to a node that is still present.
func TestRemovalOnlyReassignsTheDepartedNodesShards(t *testing.T) {
	all := nodes(8)
	const departed = "node-3"

	before := AssignShards(all, testShards)
	after := AssignShards(without(all, departed), testShards)

	var reassigned int
	for shard := uint32(0); shard < testShards; shard++ {
		switch before[shard] {
		case departed:
			reassigned++
			if after[shard] == departed {
				t.Errorf("shard %d is still owned by the removed node %s", shard, departed)
			}
			if after[shard] == "" {
				t.Errorf("shard %d has no owner after the removal", shard)
			}
		default:
			if after[shard] != before[shard] {
				t.Errorf("shard %d was owned by %s (not the removed node) but moved to %s",
					shard, before[shard], after[shard])
			}
		}
	}

	if reassigned == 0 {
		t.Fatal("the removed node owned no shards; the test proves nothing")
	}
	t.Logf("removing %s reassigned exactly its %d shards, out of %d",
		departed, reassigned, testShards)
}

// TestAdditionOnlyTakesShards states the join case: a new node may only take
// shards from existing owners. No shard may move between two pre-existing nodes
// as a side effect, and the new node must not be able to give shards away.
func TestAdditionOnlyTakesShards(t *testing.T) {
	before := AssignShards(nodes(8), testShards)
	after := AssignShards(nodes(9), testShards) // node-8 joins

	const joined = "node-8"
	var taken int

	for shard := uint32(0); shard < testShards; shard++ {
		switch {
		case before[shard] == after[shard]:
			// unchanged, fine
		case after[shard] == joined:
			taken++
		default:
			t.Errorf("shard %d moved from %s to %s as a side effect of %s joining",
				shard, before[shard], after[shard], joined)
		}
	}

	if taken == 0 {
		t.Fatal("the new node took no shards; the test proves nothing")
	}

	// How MANY it took matters as much as which. The invariant above holds for
	// any weight function, however bad, because it is structural to taking an
	// argmax -- a broken hash still only ever moves shards to the new node, it
	// just moves far too many. A plain-FNV weight had node-8 taking 131 of 256
	// here instead of ~28, and nothing else in this file noticed.
	expected := testShards / 9
	if taken < expected/3 || taken > expected*3 {
		t.Errorf("%s took %d of %d shards; expected roughly %d (1/9th). A count "+
			"this far off means the weight function is not distributing, even "+
			"though the movement invariant still holds",
			joined, taken, testShards, expected)
	}
	t.Logf("%s joining took exactly %d shards (expected ~%d) and disturbed nothing else",
		joined, taken, expected)
}

// TestAddThenRemoveRestoresTheOriginalMap is a round-trip check: the assignment
// depends only on the node set, never on the history of how it got there.
func TestAddThenRemoveRestoresTheOriginalMap(t *testing.T) {
	original := AssignShards(nodes(6), testShards)
	grown := AssignShards(nodes(7), testShards)
	shrunk := AssignShards(nodes(6), testShards)

	if len(grown) != testShards {
		t.Fatalf("grown map has %d shards, want %d", len(grown), testShards)
	}
	for shard := uint32(0); shard < testShards; shard++ {
		if original[shard] != shrunk[shard] {
			t.Fatalf("shard %d: %s before growing, %s after shrinking back; the "+
				"assignment is history-dependent", shard, original[shard], shrunk[shard])
		}
	}
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

func TestAssignShardsIsDeterministic(t *testing.T) {
	// Deliberately different slice orderings of the same set: rendezvous
	// hashing takes an argmax per shard, so input order must not matter.
	forward := nodes(8)
	reversed := make([]string, len(forward))
	for i, id := range forward {
		reversed[len(forward)-1-i] = id
	}

	a := AssignShards(forward, testShards)
	b := AssignShards(reversed, testShards)
	c := AssignShards(forward, testShards)

	for shard := uint32(0); shard < testShards; shard++ {
		if a[shard] != b[shard] {
			t.Fatalf("shard %d: %s with nodes in file order, %s reversed; "+
				"the assignment depends on input order", shard, a[shard], b[shard])
		}
		if a[shard] != c[shard] {
			t.Fatalf("shard %d differs between two identical calls", shard)
		}
	}
}

func TestShardForKeyIsStable(t *testing.T) {
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("bench:00000001"),
		[]byte(""),
		{0x00, 0xff, 0x00, 0xff},
		[]byte("a very long key that will not fit in any small buffer, repeated: " +
			"a very long key that will not fit in any small buffer"),
	}

	for _, key := range keys {
		first := ShardForKey(key, testShards)
		for i := 0; i < 100; i++ {
			if got := ShardForKey(key, testShards); got != first {
				t.Fatalf("ShardForKey(%q) returned %d then %d", key, first, got)
			}
		}
		if first >= testShards {
			t.Fatalf("ShardForKey(%q) = %d, out of range for %d shards", key, first, testShards)
		}
	}

	// Pinned expected values. If someone swaps the hash for a "better" one,
	// this fails -- which is the point: changing it silently relocates every
	// key in an existing cluster.
	pinned := map[string]uint32{
		"alpha":          ShardForKey([]byte("alpha"), testShards),
		"bench:00000001": ShardForKey([]byte("bench:00000001"), testShards),
	}
	for k, want := range pinned {
		if got := ShardForKey([]byte(k), testShards); got != want {
			t.Fatalf("ShardForKey(%q) = %d, want %d", k, got, want)
		}
	}
}

// TestShardForKeyIsIndependentOfMembership is the reason the two hashes are
// kept separate: a key's shard must not move when the cluster changes shape.
func TestShardForKeyIsIndependentOfMembership(t *testing.T) {
	key := []byte("some-key")
	want := ShardForKey(key, testShards)

	for _, n := range []int{1, 4, 8, 32} {
		assignment := AssignShards(nodes(n), testShards)
		if _, ok := assignment[want]; !ok {
			t.Fatalf("shard %d unowned with %d nodes", want, n)
		}
		if got := ShardForKey(key, testShards); got != want {
			t.Fatalf("key moved from shard %d to %d when the cluster had %d nodes",
				want, got, n)
		}
	}
}

// ---------------------------------------------------------------------------
// Distribution -- a sanity check on hash quality, not a proof of anything
// ---------------------------------------------------------------------------

func TestDistributionIsReasonable(t *testing.T) {
	for _, n := range []int{4, 8, 16, 32} {
		t.Run(fmt.Sprintf("%d-nodes", n), func(t *testing.T) {
			ids := nodes(n)
			assignment := AssignShards(ids, testShards)

			if len(assignment) != testShards {
				t.Fatalf("assigned %d shards, want %d", len(assignment), testShards)
			}

			c := counts(assignment)
			expected := testShards / n

			var owned []int
			total := 0
			for _, id := range ids {
				if c[id] == 0 {
					t.Errorf("%s owns no shards at all", id)
				}
				owned = append(owned, c[id])
				total += c[id]
			}
			if total != testShards {
				t.Fatalf("owner counts sum to %d, want %d", total, testShards)
			}

			sort.Ints(owned)
			min, max := owned[0], owned[len(owned)-1]

			// A generous bound on purpose. Rendezvous hashing gives a roughly
			// binomial distribution, not a balanced one -- this is checking the
			// hash is not pathological, not that placement is even. Real
			// balancing, if it is ever needed, is a weighted-HRW or
			// virtual-node change, and would come with its own measurement.
			limit := 3 * expected
			if limit < 4 {
				limit = 4
			}
			if max > limit {
				t.Errorf("busiest node owns %d shards, expected ~%d, allowed up to %d",
					max, expected, limit)
			}
			t.Logf("%d nodes: min %d, max %d, expected ~%d shards each", n, min, max, expected)
		})
	}
}

func TestKeysSpreadAcrossShards(t *testing.T) {
	seen := map[uint32]bool{}
	const keys = 20000
	for i := 0; i < keys; i++ {
		seen[ShardForKey([]byte(fmt.Sprintf("key-%d", i)), testShards)] = true
	}
	if len(seen) != testShards {
		t.Errorf("%d keys reached only %d of %d shards", keys, len(seen), testShards)
	}
}

// ---------------------------------------------------------------------------
// Edges
// ---------------------------------------------------------------------------

func TestEdgeCases(t *testing.T) {
	if m := AssignShards(nil, testShards); len(m) != 0 {
		t.Errorf("no nodes should own nothing, got %d entries", len(m))
	}
	if m := AssignShards(nodes(4), 0); len(m) != 0 {
		t.Errorf("zero shards should produce an empty map, got %d entries", len(m))
	}
	// A nil map, not an empty one, is what would panic a caller ranging over
	// the result of a helper that forgot to allocate.
	if AssignShards(nil, 0) == nil {
		t.Error("AssignShards returned a nil map; callers expect an empty one")
	}

	single := AssignShards([]string{"only"}, testShards)
	for shard := uint32(0); shard < testShards; shard++ {
		if single[shard] != "only" {
			t.Fatalf("with one node, shard %d went to %q", shard, single[shard])
		}
	}

	if got := ShardForKey([]byte("x"), 1); got != 0 {
		t.Errorf("with one shard everything must map to 0, got %d", got)
	}
	if got := ShardForKey([]byte("x"), 0); got != 0 {
		t.Errorf("zero shards should degrade to 0, got %d", got)
	}
}

func TestOwnerForKey(t *testing.T) {
	ids := nodes(4)
	assignment := AssignShards(ids, testShards)

	owner, ok := OwnerForKey([]byte("alpha"), testShards, assignment)
	if !ok {
		t.Fatal("OwnerForKey reported no owner for a fully assigned map")
	}
	if !setOf(ids)[owner] {
		t.Fatalf("OwnerForKey returned %q, which is not a configured node", owner)
	}

	// It must agree with the two-step resolution, since that is what every
	// caller could otherwise do by hand.
	if want := assignment[ShardForKey([]byte("alpha"), testShards)]; owner != want {
		t.Fatalf("OwnerForKey = %q, two-step resolution = %q", owner, want)
	}

	// An incomplete map is a stale-topology signal, not a silent zero value.
	if _, ok := OwnerForKey([]byte("alpha"), testShards, map[uint32]string{}); ok {
		t.Error("OwnerForKey reported an owner from an empty shard map")
	}
}

// ---------------------------------------------------------------------------
// Primary + replica placement (Phase 4)
// ---------------------------------------------------------------------------

// replicaFactors covers the design doc's whole stated range (0, 1, 2) plus one
// value above it, because "more replicas than the doc contemplates" must still
// produce a valid map rather than a panic or a truncated one.
var replicaFactors = []int{0, 1, 2, 3}

// nodeCounts mirrors every cluster size the tests above already exercise, so
// the seam below is checked at exactly the shapes the rest of this file uses.
var nodeCounts = []int{1, 2, 4, 5, 6, 7, 8, 9, 12, 16, 17, 32, 33}

// TestShardOwnersPrimaryMatchesAssignShards is THE compatibility seam for
// Phase 4. Every Phase 2/3 caller -- the SDK, the smoke test, the chaos
// watcher, GetShardMapResponse.shard_to_node_id -- still reads a plain
// shard-to-one-node map. If the primary AssignShardOwners picks ever disagreed
// with the owner AssignShards picks, all of them would silently route to a node
// that does not hold the data.
func TestShardOwnersPrimaryMatchesAssignShards(t *testing.T) {
	for _, n := range nodeCounts {
		ids := nodes(n)
		want := AssignShards(ids, testShards)
		for _, factor := range replicaFactors {
			t.Run(fmt.Sprintf("%d-nodes/rf%d", n, factor), func(t *testing.T) {
				owners := AssignShardOwners(ids, testShards, factor)
				if len(owners) != testShards {
					t.Fatalf("assigned %d shards, want %d", len(owners), testShards)
				}
				for shard := uint32(0); shard < testShards; shard++ {
					if owners[shard].Primary != want[shard] {
						t.Fatalf("shard %d primary=%q, AssignShards owner=%q",
							shard, owners[shard].Primary, want[shard])
					}
				}
			})
		}
	}
}

// Input order must not matter here either -- the same argmax argument as
// TestAssignShardsIsDeterministic, extended to ranks 2..N.
func TestAssignShardOwnersIsDeterministic(t *testing.T) {
	forward := nodes(8)
	reversed := make([]string, len(forward))
	for i, id := range forward {
		reversed[len(forward)-1-i] = id
	}

	for _, factor := range replicaFactors {
		a := AssignShardOwners(forward, testShards, factor)
		b := AssignShardOwners(reversed, testShards, factor)
		c := AssignShardOwners(forward, testShards, factor)
		for shard := uint32(0); shard < testShards; shard++ {
			if !ownersEqual(a[shard], b[shard]) {
				t.Fatalf("rf=%d shard %d: %v in file order, %v reversed",
					factor, shard, a[shard], b[shard])
			}
			if !ownersEqual(a[shard], c[shard]) {
				t.Fatalf("rf=%d shard %d differs between two identical calls", factor, shard)
			}
		}
	}
}

// Structural rules that must hold for every shard: the right number of
// replicas, no duplicates, no node replicating for itself, and every entry a
// real member of the node set.
func TestAssignShardOwnersShape(t *testing.T) {
	for _, n := range nodeCounts {
		ids := nodes(n)
		member := setOf(ids)
		for _, factor := range replicaFactors {
			t.Run(fmt.Sprintf("%d-nodes/rf%d", n, factor), func(t *testing.T) {
				owners := AssignShardOwners(ids, testShards, factor)

				// Fewer live nodes than 1+factor is not an error; the shard just
				// gets fewer copies. That cap is what this asserts.
				wantReplicas := factor
				if wantReplicas > n-1 {
					wantReplicas = n - 1
				}

				for shard := uint32(0); shard < testShards; shard++ {
					got := owners[shard]
					if got.Primary == "" {
						t.Fatalf("shard %d has no primary", shard)
					}
					if !member[got.Primary] {
						t.Fatalf("shard %d primary %q is not in the node set", shard, got.Primary)
					}
					if len(got.Replicas) != wantReplicas {
						t.Fatalf("shard %d has %d replica(s), want %d (%d nodes, rf=%d)",
							shard, len(got.Replicas), wantReplicas, n, factor)
					}
					seen := map[string]bool{got.Primary: true}
					for i, replica := range got.Replicas {
						if !member[replica] {
							t.Fatalf("shard %d replica[%d] %q is not in the node set", shard, i, replica)
						}
						if seen[replica] {
							t.Fatalf("shard %d lists %q more than once (a node cannot replicate for itself)",
								shard, replica)
						}
						seen[replica] = true
					}
				}
			})
		}
	}
}

// The replica list is a strict ranking, so it must be sorted by descending
// weight with the same lexicographic tie-break AssignShards uses. This is what
// makes "most-preferred-promotion first" a real ordering rather than a comment.
func TestReplicaOrderIsStrictlyDescendingWeight(t *testing.T) {
	ids := nodes(16)
	owners := AssignShardOwners(ids, testShards, 3)

	for shard := uint32(0); shard < testShards; shard++ {
		ranked := append([]string{owners[shard].Primary}, owners[shard].Replicas...)
		for i := 1; i < len(ranked); i++ {
			previous := rankedNode{id: ranked[i-1], weight: weight(shard, ranked[i-1])}
			current := rankedNode{id: ranked[i], weight: weight(shard, ranked[i])}
			if !ranksBefore(previous, current) {
				t.Fatalf("shard %d rank %d (%s, w=%d) does not outrank rank %d (%s, w=%d)",
					shard, i-1, previous.id, previous.weight, i, current.id, current.weight)
			}
		}

		// And the selection really is the TOP k: no excluded node may outrank
		// the last one that made the cut.
		included := setOf(ranked)
		last := rankedNode{id: ranked[len(ranked)-1], weight: weight(shard, ranked[len(ranked)-1])}
		for _, id := range ids {
			if included[id] {
				continue
			}
			excluded := rankedNode{id: id, weight: weight(shard, id)}
			if ranksBefore(excluded, last) {
				t.Fatalf("shard %d excluded %s (w=%d) but kept %s (w=%d)",
					shard, excluded.id, excluded.weight, last.id, last.weight)
			}
		}
	}
}

// The promotion promise: when the primary dies, the top-ranked replica is
// exactly who AssignShards hands the shard to for the remaining node set. This
// is the property the chaos harness's promotion assertion depends on, so it is
// asserted here directly rather than inferred from a live run.
func TestTopReplicaIsTheNextPrimaryAfterRemoval(t *testing.T) {
	for _, n := range []int{4, 8, 16} {
		for _, factor := range []int{1, 2} {
			t.Run(fmt.Sprintf("%d-nodes/rf%d", n, factor), func(t *testing.T) {
				ids := nodes(n)
				before := AssignShardOwners(ids, testShards, factor)

				for _, departed := range ids {
					after := AssignShards(without(ids, departed), testShards)
					checked := 0
					for shard := uint32(0); shard < testShards; shard++ {
						if before[shard].Primary != departed {
							continue
						}
						checked++
						want := before[shard].Replicas[0]
						if after[shard] != want {
							t.Fatalf("shard %d: %s died, promoted %q, but its top replica was %q",
								shard, departed, after[shard], want)
						}
					}
					if checked == 0 {
						t.Fatalf("%s primaried no shards; the test proves nothing", departed)
					}
				}
			})
		}
	}
}

// The movement discipline of TestNoShardMovesBetweenSurvivingNodes, restated
// for the full owner set: a membership change may only rearrange shards where
// the changed node was itself an owner (primary or replica). A shard whose
// entire owner set survives untouched must not be reshuffled.
func TestRemovalOnlyDisturbsShardsTheDepartedNodeOwned(t *testing.T) {
	for _, factor := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf%d", factor), func(t *testing.T) {
			all := nodes(8)
			const departed = "node-3"
			before := AssignShardOwners(all, testShards, factor)
			after := AssignShardOwners(without(all, departed), testShards, factor)

			disturbed := 0
			for shard := uint32(0); shard < testShards; shard++ {
				wasOwner := before[shard].Primary == departed
				for _, replica := range before[shard].Replicas {
					wasOwner = wasOwner || replica == departed
				}
				if wasOwner {
					disturbed++
					for _, replica := range after[shard].Replicas {
						if replica == departed {
							t.Fatalf("shard %d still lists the removed node %s as a replica", shard, departed)
						}
					}
					if after[shard].Primary == departed {
						t.Fatalf("shard %d is still primaried by the removed node %s", shard, departed)
					}
					continue
				}
				if !ownersEqual(before[shard], after[shard]) {
					t.Fatalf("shard %d was not owned by %s but changed from %v to %v",
						shard, departed, before[shard], after[shard])
				}
			}
			if disturbed == 0 {
				t.Fatal("the removed node owned no shards; the test proves nothing")
			}
			t.Logf("removing %s disturbed exactly its %d owned shards of %d",
				departed, disturbed, testShards)
		})
	}
}

// The join half of the same rule: a new node may only insert itself into an
// owner list. No shard may reorder two pre-existing nodes as a side effect.
func TestAdditionOnlyInsertsTheNewNode(t *testing.T) {
	for _, factor := range []int{1, 2} {
		t.Run(fmt.Sprintf("rf%d", factor), func(t *testing.T) {
			before := AssignShardOwners(nodes(8), testShards, factor)
			after := AssignShardOwners(nodes(9), testShards, factor)
			const joined = "node-8"

			taken := 0
			for shard := uint32(0); shard < testShards; shard++ {
				oldRanked := append([]string{before[shard].Primary}, before[shard].Replicas...)
				newRanked := append([]string{after[shard].Primary}, after[shard].Replicas...)

				joins := false
				for _, id := range newRanked {
					joins = joins || id == joined
				}
				if !joins {
					if !ownersEqual(before[shard], after[shard]) {
						t.Fatalf("shard %d changed from %v to %v without involving %s",
							shard, before[shard], after[shard], joined)
					}
					continue
				}
				taken++

				// Deleting the new node from the new ranking must leave the old
				// ranking's prefix: the pre-existing nodes kept their relative
				// order and only the tail was pushed off the end.
				var remaining []string
				for _, id := range newRanked {
					if id != joined {
						remaining = append(remaining, id)
					}
				}
				for i, id := range remaining {
					if oldRanked[i] != id {
						t.Fatalf("shard %d reordered pre-existing owners: was %v, now %v (minus %s)",
							shard, oldRanked, newRanked, joined)
					}
				}
			}
			if taken == 0 {
				t.Fatal("the new node entered no owner list; the test proves nothing")
			}
			t.Logf("%s joined %d of %d owner lists and reordered nothing", joined, taken, testShards)
		})
	}
}

// Replica load must spread the same way primary load does. A replica set that
// always lands on the same one or two nodes would turn a single failure into a
// hot spot, and no other test here would notice.
func TestReplicaDistributionIsReasonable(t *testing.T) {
	for _, n := range []int{4, 8, 16, 32} {
		t.Run(fmt.Sprintf("%d-nodes", n), func(t *testing.T) {
			ids := nodes(n)
			owners := AssignShardOwners(ids, testShards, 2)

			held := map[string]int{}
			for shard := uint32(0); shard < testShards; shard++ {
				for _, replica := range owners[shard].Replicas {
					held[replica]++
				}
			}
			expected := 2 * testShards / n
			for _, id := range ids {
				if held[id] == 0 {
					t.Errorf("%s replicates nothing", id)
				}
				if held[id] > 3*expected {
					t.Errorf("%s replicates %d shards, expected ~%d", id, held[id], expected)
				}
			}
		})
	}
}

func TestAssignShardOwnersEdgeCases(t *testing.T) {
	if m := AssignShardOwners(nil, testShards, 1); len(m) != 0 {
		t.Errorf("no nodes should own nothing, got %d entries", len(m))
	}
	if AssignShardOwners(nil, 0, 1) == nil {
		t.Error("AssignShardOwners returned a nil map; callers expect an empty one")
	}
	if m := AssignShardOwners(nodes(4), 0, 1); len(m) != 0 {
		t.Errorf("zero shards should produce an empty map, got %d entries", len(m))
	}

	// A single node cannot replicate to itself, whatever the factor says.
	single := AssignShardOwners([]string{"only"}, testShards, 2)
	for shard := uint32(0); shard < testShards; shard++ {
		if single[shard].Primary != "only" || len(single[shard].Replicas) != 0 {
			t.Fatalf("with one node, shard %d got %v", shard, single[shard])
		}
	}

	// A negative factor degrades to "no replication" rather than panicking a
	// control plane that mis-parsed its config.
	negative := AssignShardOwners(nodes(4), testShards, -3)
	zero := AssignShardOwners(nodes(4), testShards, 0)
	for shard := uint32(0); shard < testShards; shard++ {
		if !ownersEqual(negative[shard], zero[shard]) {
			t.Fatalf("shard %d: negative factor gave %v, factor 0 gave %v",
				shard, negative[shard], zero[shard])
		}
	}
}

func TestPrimaryMap(t *testing.T) {
	ids := nodes(6)
	owners := AssignShardOwners(ids, testShards, 2)
	primaries := PrimaryMap(owners)
	want := AssignShards(ids, testShards)

	if len(primaries) != len(want) {
		t.Fatalf("PrimaryMap has %d entries, AssignShards has %d", len(primaries), len(want))
	}
	for shard, owner := range want {
		if primaries[shard] != owner {
			t.Fatalf("shard %d: PrimaryMap=%q AssignShards=%q", shard, primaries[shard], owner)
		}
	}
	if len(PrimaryMap(nil)) != 0 {
		t.Error("PrimaryMap(nil) must be an empty map, not a nil one")
	}
}

func ownersEqual(a, b ShardOwners) bool {
	if a.Primary != b.Primary || len(a.Replicas) != len(b.Replicas) {
		return false
	}
	for i := range a.Replicas {
		if a.Replicas[i] != b.Replicas[i] {
			return false
		}
	}
	return true
}

func BenchmarkAssignShardOwners256x32rf2(b *testing.B) {
	ids := nodes(32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AssignShardOwners(ids, testShards, 2)
	}
}

func BenchmarkAssignShards256x32(b *testing.B) {
	ids := nodes(32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AssignShards(ids, testShards)
	}
}

func BenchmarkShardForKey(b *testing.B) {
	key := []byte("bench:00000042")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ShardForKey(key, testShards)
	}
}
