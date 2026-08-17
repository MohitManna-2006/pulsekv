package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
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
	return newTestServiceWithReplicas(t, shardCount, 0, source)
}

func newTestServiceWithReplicas(t *testing.T, shardCount uint32, replicationFactor int,
	source membership.Source) *Service {
	t.Helper()
	svc, err := New(&config.Config{
		ShardCount:        shardCount,
		ReplicationFactor: replicationFactor,
	}, source)
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
	if _, err := New(&config.Config{ShardCount: 8, ReplicationFactor: -1}, validSource); err == nil {
		t.Fatal("New(negative replication factor) succeeded")
	}
}

// ---------------------------------------------------------------------------
// Phase 4: primary + replica placement
// ---------------------------------------------------------------------------

func fourNodeSource(generation uint64) *mutableSource {
	return &mutableSource{snapshot: membership.Snapshot{
		Generation: generation,
		Nodes: []membership.Node{
			{NodeID: "node-0", Address: "127.0.0.1:7100"},
			{NodeID: "node-1", Address: "127.0.0.1:7101"},
			{NodeID: "node-2", Address: "127.0.0.1:7102"},
			{NodeID: "node-3", Address: "127.0.0.1:7103"},
		},
	}}
}

// The compatibility guarantee, asserted at the RPC boundary rather than only in
// the router: whatever else shard_to_owners says, shard_to_node_id stays
// exactly the primary column, because that is what every Phase 2/3 consumer
// reads.
func TestGetShardMapPublishesOwnersThatAgreeWithShardMap(t *testing.T) {
	for _, factor := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("rf%d", factor), func(t *testing.T) {
			source := fourNodeSource(11)
			svc := newTestServiceWithReplicas(t, 64, factor, source)

			resp, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
			if err != nil {
				t.Fatalf("GetShardMap: %v", err)
			}
			if resp.GetReplicationFactor() != uint32(factor) {
				t.Fatalf("replication_factor = %d, want %d", resp.GetReplicationFactor(), factor)
			}
			if len(resp.GetShardToOwners()) != 64 {
				t.Fatalf("owner map has %d entries, want 64", len(resp.GetShardToOwners()))
			}

			ids := []string{"node-0", "node-1", "node-2", "node-3"}
			wantOwners := router.AssignShardOwners(ids, 64, factor)
			wantPrimaries := router.AssignShards(ids, 64)

			for shard := uint32(0); shard < 64; shard++ {
				entry := resp.GetShardToOwners()[shard]
				if entry == nil {
					t.Fatalf("shard %d has no owner entry", shard)
				}
				if entry.GetPrimary() != resp.GetShardToNodeId()[shard] {
					t.Fatalf("shard %d: primary %q but shard_to_node_id %q",
						shard, entry.GetPrimary(), resp.GetShardToNodeId()[shard])
				}
				if entry.GetPrimary() != wantPrimaries[shard] {
					t.Fatalf("shard %d primary %q, want HRW owner %q",
						shard, entry.GetPrimary(), wantPrimaries[shard])
				}
				want := wantOwners[shard].Replicas
				if len(entry.GetReplicas()) != len(want) {
					t.Fatalf("shard %d has %d replica(s), want %d",
						shard, len(entry.GetReplicas()), len(want))
				}
				for i := range want {
					if entry.GetReplicas()[i] != want[i] {
						t.Fatalf("shard %d replica[%d] = %q, want %q",
							shard, i, entry.GetReplicas()[i], want[i])
					}
				}
			}
		})
	}
}

// Replica placement is part of what the fingerprint identifies, so the same
// membership at two replication factors must not look like the same topology.
func TestTopologyFingerprintCoversReplicationFactor(t *testing.T) {
	source := fourNodeSource(3)
	unreplicated := newTestServiceWithReplicas(t, 32, 0, source)
	replicated := newTestServiceWithReplicas(t, 32, 1, source)

	a, err := unreplicated.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	b, err := replicated.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}

	if maps.Equal(a.GetShardToNodeId(), b.GetShardToNodeId()) == false {
		t.Fatal("changing the replication factor must not move any primary")
	}
	if bytes.Equal(a.GetTopologyFingerprint(), b.GetTopologyFingerprint()) {
		t.Fatal("two different replication factors produced the same topology fingerprint")
	}

	// GetNodeList must publish the same identity as GetShardMap, or the
	// coherence retry in internal/topology can never converge.
	nodesResp, err := replicated.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("GetNodeList: %v", err)
	}
	if !bytes.Equal(nodesResp.GetTopologyFingerprint(), b.GetTopologyFingerprint()) {
		t.Fatal("GetNodeList and GetShardMap disagree on the topology fingerprint")
	}
}

// Fewer live nodes than 1 + replication_factor is a real operational state, not
// an error. It must publish the shards it can replicate rather than refusing.
func TestGetShardMapDegradesWhenReplicasExceedLiveNodes(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{
		Generation: 5,
		Nodes: []membership.Node{
			{NodeID: "node-0", Address: "127.0.0.1:7100"},
			{NodeID: "node-1", Address: "127.0.0.1:7101"},
		},
	}}
	svc := newTestServiceWithReplicas(t, 16, 3, source)

	resp, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if resp.GetReplicationFactor() != 3 {
		t.Fatalf("replication_factor = %d, want the configured 3", resp.GetReplicationFactor())
	}
	for shard := uint32(0); shard < 16; shard++ {
		entry := resp.GetShardToOwners()[shard]
		if len(entry.GetReplicas()) != 1 {
			t.Fatalf("shard %d has %d replica(s); two live nodes can only hold one",
				shard, len(entry.GetReplicas()))
		}
		if entry.GetReplicas()[0] == entry.GetPrimary() {
			t.Fatalf("shard %d replicates to its own primary %q", shard, entry.GetPrimary())
		}
	}
}

// An empty cluster is an authoritative topology, not a metadata failure. That
// stays true with replication configured: it publishes no owners at all rather
// than a map full of empty owner entries.
func TestEmptyTopologyPublishesNoOwners(t *testing.T) {
	svc := newTestServiceWithReplicas(t, 16, 2, &mutableSource{
		snapshot: membership.Snapshot{Generation: 4},
	})

	resp, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if len(resp.GetShardToNodeId()) != 0 || len(resp.GetShardToOwners()) != 0 {
		t.Fatalf("empty cluster published %d owner(s) and %d owner entries",
			len(resp.GetShardToNodeId()), len(resp.GetShardToOwners()))
	}
	if resp.GetShardCount() != 16 {
		t.Fatalf("shard count = %d, want 16 even with no live nodes", resp.GetShardCount())
	}
	if resp.GetReplicationFactor() != 2 {
		t.Fatalf("replication_factor = %d, want 2", resp.GetReplicationFactor())
	}
}
