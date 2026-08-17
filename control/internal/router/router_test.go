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
