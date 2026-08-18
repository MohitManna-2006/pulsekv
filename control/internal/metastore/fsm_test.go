package metastore

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"

	"pulsekv/control/internal/membership"
)

func nodes(ids ...string) []membership.Node {
	out := make([]membership.Node, 0, len(ids))
	for i, id := range ids {
		out = append(out, membership.Node{NodeID: id, Address: "127.0.0.1:" + itoa(7100+i)})
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func applyState(t *testing.T, machine *fsm, index uint64, state State) any {
	t.Helper()
	raw, err := encodeCommand(state)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return machine.Apply(&raft.Log{Index: index, Data: raw})
}

// The generation is the log index of the entry that CHANGED the state, not the
// latest index. Phase 3 published "a generation moves only when the live set
// moves" and clients act on that: a bump is how the chaos harness and the SDK
// know a membership transition happened.
func TestApplyAdvancesGenerationOnlyOnContentChange(t *testing.T) {
	machine := newFSM()

	applyState(t, machine, 7, State{Nodes: nodes("node-0", "node-1"), ReplicationFactor: 1})
	if got := machine.State(); got.Generation != 7 || len(got.Nodes) != 2 {
		t.Fatalf("after first apply: generation=%d nodes=%d", got.Generation, len(got.Nodes))
	}

	// Same content at a later index: no bump.
	applyState(t, machine, 9, State{Nodes: nodes("node-0", "node-1"), ReplicationFactor: 1})
	if got := machine.State().Generation; got != 7 {
		t.Fatalf("re-proposing identical content moved the generation to %d", got)
	}

	// A different replication factor is a content change even with the same
	// nodes -- it changes every shard's owner list.
	applyState(t, machine, 11, State{Nodes: nodes("node-0", "node-1"), ReplicationFactor: 2})
	if got := machine.State(); got.Generation != 11 || got.ReplicationFactor != 2 {
		t.Fatalf("factor change: generation=%d factor=%d", got.Generation, got.ReplicationFactor)
	}

	applyState(t, machine, 13, State{Nodes: nodes("node-0"), ReplicationFactor: 2})
	if got := machine.State().Generation; got != 13 {
		t.Fatalf("membership change did not bump the generation: %d", got)
	}
}

// Two replicas that applied the same entries must hold byte-identical state,
// which means node ordering cannot depend on how the proposer happened to
// enumerate its gossip view.
func TestApplyNormalizesNodeOrder(t *testing.T) {
	forward := newFSM()
	reversed := newFSM()

	ordered := nodes("node-0", "node-1", "node-2")
	backwards := []membership.Node{ordered[2], ordered[1], ordered[0]}

	applyState(t, forward, 4, State{Nodes: ordered, ReplicationFactor: 1})
	applyState(t, reversed, 4, State{Nodes: backwards, ReplicationFactor: 1})

	a, b := forward.State(), reversed.State()
	if !a.SameContent(b) || a.Generation != b.Generation {
		t.Fatalf("two orderings of the same set diverged:\n%+v\n%+v", a, b)
	}
	for i := range a.Nodes {
		if a.Nodes[i] != ordered[i] {
			t.Fatalf("node %d = %+v, want %+v", i, a.Nodes[i], ordered[i])
		}
	}
}

// A malformed entry must fail deterministically on every replica rather than
// panicking one of them: identical state everywhere is the property that
// matters, and a panic breaks it in the worst possible way.
func TestApplyRejectsMalformedEntriesWithoutChangingState(t *testing.T) {
	machine := newFSM()
	applyState(t, machine, 3, State{Nodes: nodes("node-0"), ReplicationFactor: 1})
	before := machine.State()

	cases := []struct {
		name string
		data []byte
	}{
		{"not json", []byte("{{{")},
		{"wrong version", []byte(`{"version":99,"nodes":[],"replication_factor":1}`)},
		{"node with no address", []byte(`{"version":1,"nodes":[{"NodeID":"n","Address":""}],"replication_factor":1}`)},
		{"negative factor", []byte(`{"version":1,"nodes":[],"replication_factor":-1}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := machine.Apply(&raft.Log{Index: 5, Data: tc.data})
			if _, isErr := result.(error); !isErr {
				t.Fatalf("Apply returned %#v, want an error", result)
			}
			if got := machine.State(); !got.SameContent(before) || got.Generation != before.Generation {
				t.Fatalf("a rejected entry changed the state: %+v", got)
			}
		})
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	machine := newFSM()
	applyState(t, machine, 21, State{Nodes: nodes("node-0", "node-1", "node-2"), ReplicationFactor: 2})
	want := machine.State()

	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snapshot.Release()

	// A replica that restarts and restores must report the SAME generation it
	// had before, or clients would see the topology appear to move backwards.
	restored := newFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got := restored.State()
	if !got.SameContent(want) || got.Generation != want.Generation {
		t.Fatalf("restored %+v, want %+v", got, want)
	}

	// Restore replaces wholesale; it must not merge with what was there.
	dirty := newFSM()
	applyState(t, dirty, 99, State{Nodes: nodes("stale-a", "stale-b", "stale-c", "stale-d"), ReplicationFactor: 0})
	if err := dirty.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore over existing state: %v", err)
	}
	if got := dirty.State(); !got.SameContent(want) {
		t.Fatalf("restore merged instead of replacing: %+v", got)
	}
}

func TestRestoreRejectsMalformedSnapshots(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantSub string
	}{
		{"not json", "{{{", "restore metadata snapshot"},
		{"wrong version", `{"version":42}`, "not the supported version"},
		{"negative factor", `{"version":1,"replication_factor":-2}`, "negative replication factor"},
		{"empty node id", `{"version":1,"nodes":[{"NodeID":"","Address":"a"}]}`, "empty ID or address"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			machine := newFSM()
			err := machine.Restore(io.NopCloser(strings.NewReader(tc.body)))
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("Restore error = %v, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

// The membership.Source adaptation: metadata.Service reads this and cannot tell
// it apart from a gossip view, except that the replication factor is now set.
func TestStateSnapshotCarriesTheAgreedReplicationFactor(t *testing.T) {
	state := State{Nodes: nodes("node-0"), ReplicationFactor: 0, Generation: 12}
	snapshot := state.Snapshot()

	if snapshot.Generation != 12 || len(snapshot.Nodes) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.ReplicationFactor == nil {
		t.Fatal("a Raft-backed snapshot must set the replication factor, even when it is 0")
	}
	if *snapshot.ReplicationFactor != 0 {
		t.Fatalf("replication factor = %d, want 0", *snapshot.ReplicationFactor)
	}

	// The returned slice must not alias the state's.
	snapshot.Nodes[0].NodeID = "mutated"
	if state.Nodes[0].NodeID != "node-0" {
		t.Fatal("Snapshot aliased the state's node slice")
	}
}

// The command carries no generation: it is assigned from the log index at apply
// time, so a replayed or hand-written entry cannot dictate one.
func TestEncodedCommandOmitsGeneration(t *testing.T) {
	raw, err := encodeCommand(State{Nodes: nodes("node-0"), ReplicationFactor: 1, Generation: 77})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, present := decoded["generation"]; present {
		t.Fatalf("command carries a generation: %s", raw)
	}
}

type memorySink struct {
	bytes.Buffer
	cancelled bool
}

func (s *memorySink) Close() error  { return nil }
func (s *memorySink) ID() string    { return "test" }
func (s *memorySink) Cancel() error { s.cancelled = true; return nil }
