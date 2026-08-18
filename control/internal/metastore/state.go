// Package metastore is the Raft-backed metadata plane: the one place in
// PulseKV where real consensus is the right tool.
//
// WHAT IS REPLICATED, AND WHAT DELIBERATELY IS NOT.
//
// The Raft log carries exactly two things: the live data-node set and the
// replication factor. It does NOT carry the shard map or the owner map, and
// that is the central design decision of this phase rather than an omission.
//
// router.AssignShards and router.AssignShardOwners are pure functions of
// (node IDs, shard count, replication factor). They take no config, no clock,
// and no randomness, and router_test.go asserts their determinism exactly. So
// once every replica agrees on the *input*, every replica independently
// computes a byte-identical shard map with nothing further to coordinate.
// Replicating the derived map instead would put 256 map entries through
// consensus to re-derive something already agreed, and would introduce a second
// way for two replicas to disagree -- one where the input matched but the
// derivation had drifted.
//
// The design doc says the same thing from the other direction: this is a
// "low-volume, latency-insensitive write path". Membership changes are rare and
// tiny. Cache writes never come near it; Phase 4's replication is untouched.
package metastore

import (
	"encoding/json"
	"fmt"
	"sort"

	"pulsekv/control/internal/membership"
)

// stateVersion is written into every snapshot and command so a future format
// change can be detected rather than silently misread as the current one.
const stateVersion = 1

// State is the entire replicated state machine.
//
// Two fields, both inputs to placement, neither derived from the other. If this
// struct ever grows a map[uint32]string, something has gone wrong -- see the
// package comment.
type State struct {
	// Nodes is the agreed live data-node set, sorted by (NodeID, Address) so
	// two replicas that applied the same log hold byte-identical state.
	Nodes []membership.Node `json:"nodes"`

	// ReplicationFactor is the agreed replicas-per-shard. Phase 4 left this as
	// per-replica local config, which meant two control planes could publish
	// different owner maps for the same membership; agreeing on it here is what
	// closes that gap.
	ReplicationFactor int `json:"replication_factor"`

	// Generation is the Raft log index of the entry that last CHANGED this
	// state. It is deliberately not the latest applied index: the published
	// contract since Phase 3 is that a generation changes only when the
	// effective node set changes, and heartbeat or configuration entries change
	// neither.
	//
	// Using the Raft index rather than a process-local counter is a real
	// upgrade on Phase 3, where generation was explicitly "diagnostic, not a
	// globally unique revision" because a restarted publisher could reuse a
	// number for different content. A committed log index cannot: two replicas
	// reporting the same generation are reporting the same committed state.
	Generation uint64 `json:"generation"`
}

// Clone returns a deep copy. Every read path hands out clones, so no caller can
// mutate the state an apply is about to replace.
func (s State) Clone() State {
	return State{
		Nodes:             append([]membership.Node(nil), s.Nodes...),
		ReplicationFactor: s.ReplicationFactor,
		Generation:        s.Generation,
	}
}

// Snapshot renders the state as the membership.Snapshot that
// ClusterMetadataService already consumes. This is the whole adaptation layer:
// metadata.Service cannot tell a Raft-backed source from a gossip-backed one.
func (s State) Snapshot() membership.Snapshot {
	factor := s.ReplicationFactor
	return membership.Snapshot{
		Generation:        s.Generation,
		Nodes:             append([]membership.Node(nil), s.Nodes...),
		ReplicationFactor: &factor,
	}
}

// SameContent reports whether two states describe the same cluster, ignoring
// the generation. The leader uses it to decide whether its gossip view differs
// from committed state; comparing generations too would make every comparison
// differ and propose on every tick.
func (s State) SameContent(other State) bool {
	if s.ReplicationFactor != other.ReplicationFactor || len(s.Nodes) != len(other.Nodes) {
		return false
	}
	for i := range s.Nodes {
		if s.Nodes[i] != other.Nodes[i] {
			return false
		}
	}
	return true
}

// normalizeNodes sorts and copies a node set into the canonical order the log
// carries. Proposals come from a gossip view that is already sorted, but making
// the FSM responsible for it means a hand-written or replayed entry cannot put
// two replicas into states that differ only by ordering.
func normalizeNodes(nodes []membership.Node) []membership.Node {
	out := append([]membership.Node(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Address < out[j].Address
	})
	return out
}

// command is one Raft log entry.
//
// JSON rather than a binary encoding on purpose: this log sees a handful of
// entries per membership change, so the encoding cost is irrelevant and being
// able to read a log entry or a snapshot file with `cat` during a failure
// investigation is worth considerably more.
type command struct {
	Version int `json:"version"`

	// Nodes and ReplicationFactor together are the complete desired state. A
	// full-state command rather than a delta, because the state is two small
	// fields and a delta log would need its own conflict reasoning for no gain.
	Nodes             []membership.Node `json:"nodes"`
	ReplicationFactor int               `json:"replication_factor"`
}

func encodeCommand(state State) ([]byte, error) {
	raw, err := json.Marshal(command{
		Version:           stateVersion,
		Nodes:             normalizeNodes(state.Nodes),
		ReplicationFactor: state.ReplicationFactor,
	})
	if err != nil {
		return nil, fmt.Errorf("encode metadata command: %w", err)
	}
	return raw, nil
}

func decodeCommand(raw []byte) (command, error) {
	var cmd command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return command{}, fmt.Errorf("decode metadata command: %w", err)
	}
	if cmd.Version != stateVersion {
		return command{}, fmt.Errorf(
			"metadata command version %d is not the supported version %d", cmd.Version, stateVersion)
	}
	for _, node := range cmd.Nodes {
		if node.NodeID == "" || node.Address == "" {
			return command{}, fmt.Errorf("metadata command contains a node with an empty ID or address")
		}
	}
	if cmd.ReplicationFactor < 0 {
		return command{}, fmt.Errorf("metadata command has a negative replication factor %d",
			cmd.ReplicationFactor)
	}
	return cmd, nil
}
