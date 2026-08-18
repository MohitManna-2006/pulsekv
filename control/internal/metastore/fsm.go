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
}

var _ raft.FSM = (*fsm)(nil)

func newFSM() *fsm { return &fsm{} }

// State returns a copy of the last applied state.
func (f *fsm) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.Clone()
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
		return fmt.Errorf("apply log index %d: %w", entry.Index, err)
	}

	next := State{
		Nodes:             normalizeNodes(cmd.Nodes),
		ReplicationFactor: cmd.ReplicationFactor,
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state.SameContent(next) {
		return f.state.Clone()
	}
	next.Generation = entry.Index
	f.state = next
	return next.Clone()
}

// Snapshot captures the state for log compaction. It is taken under the read
// lock and then serialised outside it, because Persist can be slow and Apply
// must not be blocked on disk.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{state: f.State()}, nil
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
}

type fsmSnapshot struct {
	state State
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	raw, err := json.Marshal(persistedState{
		Version:           stateVersion,
		Nodes:             s.state.Nodes,
		ReplicationFactor: s.state.ReplicationFactor,
		Generation:        s.state.Generation,
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
