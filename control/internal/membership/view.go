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
}

// Source is the seam consumed by ClusterMetadataService. Phase 5 can replace
// this gossip-derived implementation without changing metadata RPC handlers.
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
	return Snapshot{
		Generation: in.Generation,
		Nodes:      append([]Node(nil), in.Nodes...),
	}
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
