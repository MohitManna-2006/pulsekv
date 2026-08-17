package membership

import (
	"fmt"
	"sync"
	"testing"
)

func mustMeta(t *testing.T, meta NodeMeta) []byte {
	t.Helper()
	raw, err := EncodeNodeMeta(meta)
	if err != nil {
		t.Fatalf("EncodeNodeMeta(%+v): %v", meta, err)
	}
	return raw
}

func dataMeta(t *testing.T, id, address string) []byte {
	t.Helper()
	return mustMeta(t, NodeMeta{
		Version: NodeMetaVersion,
		Role:    RoleData,
		NodeID:  id,
		Address: address,
	})
}

func controlMeta(t *testing.T) []byte {
	t.Helper()
	return mustMeta(t, NodeMeta{Version: NodeMetaVersion, Role: RoleControl})
}

func TestViewPublishesSortedImmutableEffectiveSet(t *testing.T) {
	v := newView()

	if err := v.upsert("observer", controlMeta(t)); err != nil {
		t.Fatalf("add observer: %v", err)
	}
	if got := v.Snapshot(); got.Generation != 0 || len(got.Nodes) != 0 {
		t.Fatalf("control observer changed snapshot: %+v", got)
	}

	if err := v.upsert("agent-b", dataMeta(t, "node-b", "127.0.0.1:7101")); err != nil {
		t.Fatalf("add node-b: %v", err)
	}
	if err := v.upsert("agent-a", dataMeta(t, "node-a", "127.0.0.1:7100")); err != nil {
		t.Fatalf("add node-a: %v", err)
	}
	want := Snapshot{Generation: 2, Nodes: []Node{
		{NodeID: "node-a", Address: "127.0.0.1:7100"},
		{NodeID: "node-b", Address: "127.0.0.1:7101"},
	}}
	assertSnapshot(t, v.Snapshot(), want)

	// A repeated event is not a topology change.
	if err := v.upsert("agent-a", dataMeta(t, "node-a", "127.0.0.1:7100")); err != nil {
		t.Fatalf("repeat node-a: %v", err)
	}
	assertSnapshot(t, v.Snapshot(), want)

	// Snapshot storage belongs to the caller, not the shared view.
	mutated := v.Snapshot()
	mutated.Nodes[0].NodeID = "corrupt"
	assertSnapshot(t, v.Snapshot(), want)
}

func TestViewRetainsLastGoodAcrossAmbiguity(t *testing.T) {
	v := newView()
	if err := v.upsert("agent-a", dataMeta(t, "node-a", "127.0.0.1:7100")); err != nil {
		t.Fatal(err)
	}
	if err := v.upsert("agent-b", dataMeta(t, "node-b", "127.0.0.1:7101")); err != nil {
		t.Fatal(err)
	}
	lastGood := v.Snapshot()

	// Two gossip identities claiming one logical ID is ambiguous. Retain the
	// complete map, but retain the raw event too so a leave can resolve it.
	if err := v.upsert("agent-duplicate-id", dataMeta(t, "node-a", "127.0.0.1:7102")); err != nil {
		t.Fatal(err)
	}
	assertSnapshot(t, v.Snapshot(), lastGood)

	v.remove("agent-a")
	wantResolved := Snapshot{Generation: lastGood.Generation + 1, Nodes: []Node{
		{NodeID: "node-a", Address: "127.0.0.1:7102"},
		{NodeID: "node-b", Address: "127.0.0.1:7101"},
	}}
	assertSnapshot(t, v.Snapshot(), wantResolved)

	// The same policy applies to duplicate service endpoints.
	if err := v.upsert("agent-c", dataMeta(t, "node-c", "127.0.0.1:7101")); err != nil {
		t.Fatal(err)
	}
	assertSnapshot(t, v.Snapshot(), wantResolved)
	v.remove("agent-c")
	assertSnapshot(t, v.Snapshot(), wantResolved) // ambiguity resolved to the same set

	// Invalid metadata is rejected before it can replace a valid record.
	if err := v.upsert("agent-b", []byte(`{"version":9}`)); err == nil {
		t.Fatal("invalid update succeeded")
	}
	assertSnapshot(t, v.Snapshot(), wantResolved)

	// A role update is a real removal from the effective data-node set.
	if err := v.upsert("agent-b", controlMeta(t)); err != nil {
		t.Fatal(err)
	}
	wantOne := Snapshot{Generation: wantResolved.Generation + 1, Nodes: []Node{
		{NodeID: "node-a", Address: "127.0.0.1:7102"},
	}}
	assertSnapshot(t, v.Snapshot(), wantOne)
	v.remove("agent-b") // removing an ignored control record is not a change
	assertSnapshot(t, v.Snapshot(), wantOne)
	v.remove("agent-duplicate-id")
	assertSnapshot(t, v.Snapshot(), Snapshot{Generation: wantOne.Generation + 1})
}

func TestViewConcurrentSnapshotsAndUpdates(t *testing.T) {
	v := newView()
	const writers = 8
	const iterations = 200

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("agent-%d", i)
			id := fmt.Sprintf("node-%d", i)
			address := fmt.Sprintf("127.0.0.1:%d", 7100+i)
			raw := dataMeta(t, id, address)
			for j := 0; j < iterations; j++ {
				if err := v.upsert(name, raw); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
				if j%3 == 0 {
					v.remove(name)
				}
			}
		}()
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				snapshot := v.Snapshot()
				for k := 1; k < len(snapshot.Nodes); k++ {
					if snapshot.Nodes[k-1].NodeID >= snapshot.Nodes[k].NodeID {
						t.Errorf("snapshot is not strictly sorted: %+v", snapshot.Nodes)
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

func assertSnapshot(t *testing.T, got, want Snapshot) {
	t.Helper()
	if got.Generation != want.Generation {
		t.Fatalf("generation = %d, want %d (nodes=%+v)", got.Generation, want.Generation, got.Nodes)
	}
	if len(got.Nodes) != len(want.Nodes) {
		t.Fatalf("nodes = %+v, want %+v", got.Nodes, want.Nodes)
	}
	for i := range got.Nodes {
		if got.Nodes[i] != want.Nodes[i] {
			t.Fatalf("nodes = %+v, want %+v", got.Nodes, want.Nodes)
		}
	}
}
