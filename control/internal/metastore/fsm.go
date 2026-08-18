package metastore

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"

	"pulsekv/control/internal/membership"
)

// fsm applies committed log entries to State.
//
// Apply runs on a single Raft goroutine, but reads come from every gRPC handler
// on the process, so the state is guarded and every read hands out a clone.
type fsm struct {
	mu    sync.RWMutex
	state State

	// applied is the highest log index this FSM has actually CONSUMED, as
	// opposed to the index Raft has handed to the FSM goroutine. The two are
	// not the same, and the difference is exactly the gap Store.ServeReady
	// exists to close: raft.AppliedIndex advances when a batch is queued on the
	// FSM's channel, which can be one goroutine handoff before Apply runs.
	// Guarded by mu, so a reader that takes state and applied together sees a
	// consistent pair.
	applied uint64
}

var _ raft.FSM = (*fsm)(nil)

func newFSM() *fsm { return &fsm{} }

// State returns a copy of the last applied state.
func (f *fsm) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.Clone()
}

// AppliedIndex is the highest log index this FSM has consumed. Zero means it
// has neither applied an entry nor restored a snapshot since the process
// started, which is precisely the state that must not be published.
func (f *fsm) AppliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// Apply installs one committed command.
//
// The generation advances to this entry's log index ONLY when the content
// actually changed. A leader that re-proposes an identical state -- which the
// bridge avoids but a retry could still produce -- must not look to clients
// like a membership change, because the whole published meaning of a generation
// bump is "the live set moved".
//
// A malformed entry is a permanent, deterministic failure of that entry on
// every replica, so returning the error (rather than panicking) keeps every
// replica's state identical, which is the property that actually matters.
func (f *fsm) Apply(entry *raft.Log) any {
	cmd, err := decodeCommand(entry.Data)
	if err != nil {
		// The entry is still consumed, and every replica consumes it the same
		// way. Recording it keeps a malformed entry from stalling readiness
		// forever on a state machine that is in fact perfectly up to date.
		f.noteApplied(entry.Index)
		return fmt.Errorf("apply log index %d: %w", entry.Index, err)
	}

	next := State{
		Nodes:             normalizeNodes(cmd.Nodes),
		ReplicationFactor: cmd.ReplicationFactor,
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// Recorded for EVERY entry, including one whose content matches. "I have
	// consumed through here" and "the cluster changed here" are different
	// facts: the generation deliberately only tracks the second.
	if entry.Index > f.applied {
		f.applied = entry.Index
	}
	if f.state.SameContent(next) {
		return f.state.Clone()
	}
	next.Generation = entry.Index
	f.state = next
	return next.Clone()
}

func (f *fsm) noteApplied(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index > f.applied {
		f.applied = index
	}
}

// Snapshot captures the state for log compaction. It is taken under the read
// lock and then serialised outside it, because Persist can be slow and Apply
// must not be blocked on disk.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state.Clone(), applied: f.applied}, nil
}

// Restore replaces the state wholesale from a snapshot. Raft calls it during
// startup or when a follower has fallen too far behind to catch up from the
// log, so it must not merge with whatever was there before.
func (f *fsm) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	var persisted persistedState
	if err := json.NewDecoder(reader).Decode(&persisted); err != nil {
		return fmt.Errorf("restore metadata snapshot: %w", err)
	}
	if persisted.Version != stateVersion {
		return fmt.Errorf("metadata snapshot version %d is not the supported version %d",
			persisted.Version, stateVersion)
	}
	if persisted.ReplicationFactor < 0 {
		return fmt.Errorf("metadata snapshot has a negative replication factor %d",
			persisted.ReplicationFactor)
	}
	for _, node := range persisted.Nodes {
		if node.NodeID == "" || node.Address == "" {
			return fmt.Errorf("metadata snapshot contains a node with an empty ID or address")
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = State{
		Nodes:             normalizeNodes(persisted.Nodes),
		ReplicationFactor: persisted.ReplicationFactor,
		Generation:        persisted.Generation,
	}
	// A restore is a consumption too. Without this a replica that recovered
	// from a snapshot would report having applied nothing, and ServeReady would
	// go looking for entries the snapshot already covers.
	f.applied = persisted.AppliedIndex
	if f.applied < persisted.Generation {
		// Tolerates a snapshot written before this field existed: the
		// generation is itself an index this FSM demonstrably consumed.
		f.applied = persisted.Generation
	}
	return nil
}

// persistedState is the on-disk snapshot shape. It carries the generation,
// unlike a command: a replica that restarts and restores must report the same
// generation it had before, or a client would see the topology appear to move
// backwards.
type persistedState struct {
	Version           int               `json:"version"`
	Nodes             []membership.Node `json:"nodes"`
	ReplicationFactor int               `json:"replication_factor"`
	Generation        uint64            `json:"generation"`

	// AppliedIndex is the highest log index the FSM had consumed when this
	// snapshot was taken. It rides along because a restored replica has to know
	// how far its own state reaches, not just what the state says. Absent from
	// a snapshot written before this field existed, which Restore tolerates.
	AppliedIndex uint64 `json:"applied_index,omitempty"`
}

type fsmSnapshot struct {
	state   State
	applied uint64
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	raw, err := json.Marshal(persistedState{
		Version:           stateVersion,
		Nodes:             s.state.Nodes,
		ReplicationFactor: s.state.ReplicationFactor,
		Generation:        s.state.Generation,
		AppliedIndex:      s.applied,
	})
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("marshal metadata snapshot: %w", err)
	}
	if _, err := sink.Write(raw); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write metadata snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
