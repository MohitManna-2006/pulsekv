package membership

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/hashicorp/memberlist"
)

// Node is one currently routable data-plane node.
type Node struct {
	NodeID  string
	Address string
}

// Snapshot is an immutable-in-practice view of the effective data-node set.
// Generation changes only when that valid set changes. Snapshot returns fresh
// slice storage, so callers cannot mutate the view shared with another RPC.
type Snapshot struct {
	Generation uint64
	Nodes      []Node

	// ReplicationFactor is the cluster-agreed replicas-per-shard when this
	// source is authoritative about configuration, and nil when it is not.
	//
	// A gossip view always leaves it nil: memberlist observes liveness, not
	// configuration, and inventing a number here would be a guess. Phase 5's
	// Raft-backed source does set it, because the factor is one of the two
	// things the metadata group agrees on.
	//
	// It rides on the Snapshot rather than being fetched separately for a
	// correctness reason, not convenience: the node set and the factor are
	// both inputs to one placement computation, and reading them through two
	// calls could interleave with an apply and produce a shard map describing
	// a state that never existed. One read, one coherent answer.
	//
	// A pointer because 0 is a real replication factor, the same reason
	// config.ReplicationFactorSetting is one.
	ReplicationFactor *int
}

// Source is the seam consumed by ClusterMetadataService.
//
// Phase 3 satisfied it with the gossip Manager below. Phase 5 satisfies it with
// a Raft-backed view of committed state instead, and that substitution is the
// entire integration: metadata's RPC handlers never learned that consensus
// exists.
type Source interface {
	Snapshot() Snapshot
}

type memberRecord struct {
	meta NodeMeta
	raw  []byte
}

// view retains raw member records separately from its published snapshot.
// That distinction matters when two live peers ambiguously advertise the same
// data-node ID or service address: the candidate is rejected, the last-good
// snapshot remains available, and a later leave/update can resolve the
// ambiguity without losing either event.
type view struct {
	mu sync.RWMutex

	records map[string]memberRecord // memberlist name -> application metadata
	current Snapshot
}

func newView() *view {
	return &view{records: make(map[string]memberRecord)}
}

func (v *view) Snapshot() Snapshot {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return cloneSnapshot(v.current)
}

func cloneSnapshot(in Snapshot) Snapshot {
	out := Snapshot{
		Generation: in.Generation,
		Nodes:      append([]Node(nil), in.Nodes...),
	}
	if in.ReplicationFactor != nil {
		// Copy the value, not the pointer: a Snapshot handed to an RPC must not
		// alias anything a later apply could rewrite underneath it.
		factor := *in.ReplicationFactor
		out.ReplicationFactor = &factor
	}
	return out
}

func (v *view) upsert(memberName string, raw []byte) error {
	meta, err := DecodeNodeMeta(raw)
	if err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	previous, existed := v.records[memberName]
	if existed && bytes.Equal(previous.raw, raw) {
		return nil
	}
	v.records[memberName] = memberRecord{meta: meta, raw: bytes.Clone(raw)}
	v.publishCandidateLocked()
	return nil
}

func (v *view) remove(memberName string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, exists := v.records[memberName]; !exists {
		return
	}
	delete(v.records, memberName)
	v.publishCandidateLocked()
}

func (v *view) publishCandidateLocked() {
	nodes := make([]Node, 0, len(v.records))
	seenIDs := make(map[string]struct{}, len(v.records))
	seenAddresses := make(map[string]struct{}, len(v.records))
	for _, record := range v.records {
		if record.meta.Role != RoleData {
			continue
		}
		if _, duplicate := seenIDs[record.meta.NodeID]; duplicate {
			return // ambiguous: retain the last complete, valid snapshot
		}
		if _, duplicate := seenAddresses[record.meta.Address]; duplicate {
			return // ambiguous: retain the last complete, valid snapshot
		}
		seenIDs[record.meta.NodeID] = struct{}{}
		seenAddresses[record.meta.Address] = struct{}{}
		nodes = append(nodes, Node{NodeID: record.meta.NodeID, Address: record.meta.Address})
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].NodeID != nodes[j].NodeID {
			return nodes[i].NodeID < nodes[j].NodeID
		}
		return nodes[i].Address < nodes[j].Address
	})
	if nodesEqual(v.current.Nodes, nodes) {
		return
	}
	v.current = Snapshot{Generation: v.current.Generation + 1, Nodes: nodes}
}

func nodesEqual(a, b []Node) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// eventDelegate callbacks run while memberlist holds its own node lock. They
// must remain bounded and, critically, must never call back into Memberlist
// (Members, Leave, Shutdown, and friends would deadlock). Updating this small
// independent view is intentionally all they do.
type eventDelegate struct {
	view *view
}

func (d *eventDelegate) NotifyJoin(node *memberlist.Node) {
	_ = d.view.upsert(node.Name, node.Meta)
}

func (d *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	_ = d.view.upsert(node.Name, node.Meta)
}

func (d *eventDelegate) NotifyLeave(node *memberlist.Node) {
	d.view.remove(node.Name)
}

// aliveDelegate rejects malformed application metadata before memberlist
// admits a peer. Global duplicate ID/address ambiguity still belongs to view:
// retaining both raw records there lets a later transition resolve it safely.
type aliveDelegate struct{}

func (aliveDelegate) NotifyAlive(node *memberlist.Node) error {
	if node == nil || node.Name == "" {
		return errors.New("memberlist node name must not be empty")
	}
	_, err := DecodeNodeMeta(node.Meta)
	return err
}
