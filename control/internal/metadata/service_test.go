package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"maps"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/router"
)

type mutableSource struct {
	mu       sync.RWMutex
	snapshot membership.Snapshot
}

func (s *mutableSource) Snapshot() membership.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return membership.Snapshot{
		Generation: s.snapshot.Generation,
		Nodes:      append([]membership.Node(nil), s.snapshot.Nodes...),
	}
}

func (s *mutableSource) set(snapshot membership.Snapshot) {
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
}

func newTestService(t *testing.T, shardCount uint32, source membership.Source) *Service {
	t.Helper()
	svc, err := New(&config.Config{ShardCount: shardCount}, source)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return svc
}

func TestMetadataPublishesOneMembershipGeneration(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{
		Generation: 7,
		Nodes: []membership.Node{
			{NodeID: "node-0", Address: "127.0.0.1:7100"},
			{NodeID: "node-1", Address: "127.0.0.1:7101"},
			{NodeID: "node-2", Address: "127.0.0.1:7102"},
		},
	}}
	svc := newTestService(t, 17, source)

	nodes, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("GetNodeList: %v", err)
	}
	shards, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if nodes.GetTopologyGeneration() != 7 || shards.GetTopologyGeneration() != 7 {
		t.Fatalf("generations = nodes %d, shards %d; want both 7",
			nodes.GetTopologyGeneration(), shards.GetTopologyGeneration())
	}
	if len(nodes.GetTopologyFingerprint()) != sha256.Size ||
		!bytes.Equal(nodes.GetTopologyFingerprint(), shards.GetTopologyFingerprint()) {
		t.Fatalf("fingerprints = nodes %x, shards %x; want one SHA-256 identity",
			nodes.GetTopologyFingerprint(), shards.GetTopologyFingerprint())
	}
	if shards.GetShardCount() != 17 {
		t.Fatalf("shard_count = %d, want 17", shards.GetShardCount())
	}
	if len(nodes.GetNodes()) != 3 {
		t.Fatalf("node count = %d, want 3", len(nodes.GetNodes()))
	}
	ids := make([]string, 0, len(nodes.GetNodes()))
	for _, node := range nodes.GetNodes() {
		if !node.GetAlive() {
			t.Errorf("published member %q has alive=false", node.GetNodeId())
		}
		ids = append(ids, node.GetNodeId())
	}
	want := router.AssignShards(ids, 17)
	if !maps.Equal(shards.GetShardToNodeId(), want) {
		t.Fatalf("shard map = %v, want HRW assignment %v", shards.GetShardToNodeId(), want)
	}
}

func TestGetShardMapIsDeterministicWithinGeneration(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{
		Generation: 3,
		Nodes: []membership.Node{
			{NodeID: "node-a", Address: "127.0.0.1:7100"},
			{NodeID: "node-b", Address: "127.0.0.1:7101"},
		},
	}}
	svc := newTestService(t, 256, source)

	first, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("first GetShardMap: %v", err)
	}
	second, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("second GetShardMap: %v", err)
	}
	if first.GetTopologyGeneration() != second.GetTopologyGeneration() ||
		!maps.Equal(first.GetShardToNodeId(), second.GetShardToNodeId()) {
		t.Fatalf("repeated reads differ: first=%v second=%v", first, second)
	}
}

func TestMembershipRemovalMovesOnlyDepartedOwnersShards(t *testing.T) {
	source := &mutableSource{}
	beforeNodes := []membership.Node{
		{NodeID: "node-0", Address: "127.0.0.1:7100"},
		{NodeID: "node-1", Address: "127.0.0.1:7101"},
		{NodeID: "node-2", Address: "127.0.0.1:7102"},
		{NodeID: "node-3", Address: "127.0.0.1:7103"},
	}
	source.set(membership.Snapshot{Generation: 10, Nodes: beforeNodes})
	svc := newTestService(t, 256, source)

	before, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("before GetShardMap: %v", err)
	}
	afterNodes := append([]membership.Node(nil), beforeNodes[:2]...)
	afterNodes = append(afterNodes, beforeNodes[3])
	source.set(membership.Snapshot{Generation: 11, Nodes: afterNodes})
	after, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("after GetShardMap: %v", err)
	}

	moved := 0
	departedOwned := 0
	for shard, oldOwner := range before.GetShardToNodeId() {
		newOwner := after.GetShardToNodeId()[shard]
		if oldOwner == "node-2" {
			departedOwned++
			if newOwner == oldOwner {
				t.Errorf("departed node still owns shard %d", shard)
			}
		}
		if oldOwner != newOwner {
			moved++
			if oldOwner != "node-2" {
				t.Errorf("shard %d moved between survivors: %s -> %s", shard, oldOwner, newOwner)
			}
		}
	}
	if moved != departedOwned {
		t.Fatalf("moved %d shards, want exactly departed owner's %d", moved, departedOwned)
	}
}

func TestMetadataPublishesAuthoritativeEmptyTopology(t *testing.T) {
	svc := newTestService(t, 8, &mutableSource{snapshot: membership.Snapshot{Generation: 4}})
	nodes, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("GetNodeList: %v", err)
	}
	shards, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if len(nodes.GetNodes()) != 0 || len(shards.GetShardToNodeId()) != 0 {
		t.Fatalf("empty topology = nodes %v, shards %v", nodes.GetNodes(), shards.GetShardToNodeId())
	}
	if shards.GetShardCount() != 8 {
		t.Fatalf("shard_count = %d, want 8", shards.GetShardCount())
	}
	if !bytes.Equal(nodes.GetTopologyFingerprint(), shards.GetTopologyFingerprint()) {
		t.Fatalf("empty-topology fingerprints differ: %x != %x",
			nodes.GetTopologyFingerprint(), shards.GetTopologyFingerprint())
	}
}

func TestTopologyFingerprintDoesNotReuseLocalGenerationForDifferentContent(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{Generation: 1, Nodes: []membership.Node{
		{NodeID: "node-a", Address: "127.0.0.1:7100"},
	}}}
	svc := newTestService(t, 8, source)
	oldNodes, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("old GetNodeList: %v", err)
	}

	// Model a different publisher that reused process-local generation 1.
	source.set(membership.Snapshot{Generation: 1, Nodes: []membership.Node{
		{NodeID: "node-b", Address: "127.0.0.1:7101"},
	}})
	newShards, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("new GetShardMap: %v", err)
	}
	if oldNodes.GetTopologyGeneration() != newShards.GetTopologyGeneration() {
		t.Fatal("test setup did not reuse the local generation")
	}
	if bytes.Equal(oldNodes.GetTopologyFingerprint(), newShards.GetTopologyFingerprint()) {
		t.Fatalf("different topology content reused fingerprint %x", oldNodes.GetTopologyFingerprint())
	}
}

func TestMetadataRejectsInvalidSourceSnapshot(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{
		Generation: 2,
		Nodes: []membership.Node{
			{NodeID: "node-a", Address: "127.0.0.1:7100"},
			{NodeID: "node-a", Address: "127.0.0.1:7101"},
		},
	}}
	svc := newTestService(t, 8, source)
	if _, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{}); status.Code(err) != codes.Internal {
		t.Fatalf("GetShardMap error = %v, want INTERNAL", err)
	}
}

func TestNewRejectsMissingInputs(t *testing.T) {
	validSource := &mutableSource{}
	if _, err := New(nil, validSource); err == nil {
		t.Fatal("New(nil config) succeeded")
	}
	if _, err := New(&config.Config{ShardCount: 8}, nil); err == nil {
		t.Fatal("New(nil source) succeeded")
	}
	if _, err := New(&config.Config{}, validSource); err == nil {
		t.Fatal("New(zero shards) succeeded")
	}
}
