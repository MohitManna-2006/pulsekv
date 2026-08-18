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
	out := membership.Snapshot{
		Generation: s.snapshot.Generation,
		Nodes:      append([]membership.Node(nil), s.snapshot.Nodes...),
	}
	// Propagated by value, like the real sources do. A fake that dropped it
	// would make every assertion about the agreed factor vacuous.
	if s.snapshot.ReplicationFactor != nil {
		factor := *s.snapshot.ReplicationFactor
		out.ReplicationFactor = &factor
	}
	return out
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

// ---------------------------------------------------------------------------
// Phase 5: the source may be authoritative about configuration
// ---------------------------------------------------------------------------

// A source that reports an agreed replication factor overrides local config.
//
// This is what makes the Raft-backed factor real: Phase 4 left every control
// plane applying its own configured number, so two replicas could publish owner
// maps computed from different factors for the same membership. Once the group
// agrees on it, the agreed value is the one placement uses.
func TestAgreedReplicationFactorOverridesLocalConfig(t *testing.T) {
	source := fourNodeSource(21)

	// Local config says 0; the source says 2.
	agreed := 2
	source.snapshot.ReplicationFactor = &agreed
	svc := newTestServiceWithReplicas(t, 32, 0, source)

	resp, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if resp.GetReplicationFactor() != 2 {
		t.Fatalf("published replication_factor = %d, want the agreed 2", resp.GetReplicationFactor())
	}

	want := router.AssignShardOwners([]string{"node-0", "node-1", "node-2", "node-3"}, 32, 2)
	for shard := uint32(0); shard < 32; shard++ {
		entry := resp.GetShardToOwners()[shard]
		if len(entry.GetReplicas()) != len(want[shard].Replicas) {
			t.Fatalf("shard %d has %d replica(s); the agreed factor of 2 wants %d",
				shard, len(entry.GetReplicas()), len(want[shard].Replicas))
		}
	}

	// An agreed 0 must win over a configured 2, the same way round. A pointer
	// is what makes that expressible.
	zero := 0
	source.snapshot.ReplicationFactor = &zero
	configuredTwo := newTestServiceWithReplicas(t, 32, 2, source)
	resp, err = configuredTwo.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if resp.GetReplicationFactor() != 0 {
		t.Fatalf("published replication_factor = %d, want the agreed 0", resp.GetReplicationFactor())
	}
	for shard := uint32(0); shard < 32; shard++ {
		if len(resp.GetShardToOwners()[shard].GetReplicas()) != 0 {
			t.Fatalf("shard %d has replicas at an agreed factor of 0", shard)
		}
	}
}

// A gossip source leaves the factor nil, and the service must then behave
// exactly as it did in Phase 3/4: local config decides.
func TestNilAgreedFactorFallsBackToLocalConfig(t *testing.T) {
	source := fourNodeSource(22)
	if source.snapshot.ReplicationFactor != nil {
		t.Fatal("the test source must model a gossip view, which knows no factor")
	}
	svc := newTestServiceWithReplicas(t, 32, 1, source)

	resp, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if resp.GetReplicationFactor() != 1 {
		t.Fatalf("published replication_factor = %d, want the configured 1", resp.GetReplicationFactor())
	}
}

// The agreed factor is part of what the fingerprint identifies, because it
// changes every shard's owner list.
func TestFingerprintFollowsTheAgreedFactor(t *testing.T) {
	source := fourNodeSource(23)
	svc := newTestServiceWithReplicas(t, 32, 0, source)

	one, two := 1, 2
	source.snapshot.ReplicationFactor = &one
	first, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	source.snapshot.ReplicationFactor = &two
	second, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if bytes.Equal(first.GetTopologyFingerprint(), second.GetTopologyFingerprint()) {
		t.Fatal("changing the agreed factor did not change the topology fingerprint")
	}
}

// The leader fields are diagnostic and must NOT reach the fingerprint. Two
// replicas holding the same committed state have to fingerprint the same even
// when one has not yet noticed an election -- otherwise a perfectly valid
// follower response would look like a different topology.
func TestLeaderInfoIsReportedButNotFingerprinted(t *testing.T) {
	source := fourNodeSource(24)

	plain := newTestServiceWithReplicas(t, 32, 1, source)
	withoutLeader, err := plain.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if withoutLeader.GetRaftLeaderId() != "" || withoutLeader.GetRaftTerm() != 0 {
		t.Fatalf("a service with no Raft group reported leader %q term %d",
			withoutLeader.GetRaftLeaderId(), withoutLeader.GetRaftTerm())
	}

	svc, err := New(&config.Config{ShardCount: 32, ReplicationFactor: 1}, source,
		WithLeaderInfo(func() (string, uint64) { return "cp-1", 7 }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	withLeader, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("GetShardMap: %v", err)
	}
	if withLeader.GetRaftLeaderId() != "cp-1" || withLeader.GetRaftTerm() != 7 {
		t.Fatalf("leader = %q term %d, want cp-1 term 7",
			withLeader.GetRaftLeaderId(), withLeader.GetRaftTerm())
	}
	if !bytes.Equal(withLeader.GetTopologyFingerprint(), withoutLeader.GetTopologyFingerprint()) {
		t.Fatal("leadership information leaked into the topology fingerprint")
	}

	// And GetNodeList must still agree with GetShardMap, or the coherence
	// retry in internal/topology could never converge.
	nodesResp, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("GetNodeList: %v", err)
	}
	if !bytes.Equal(nodesResp.GetTopologyFingerprint(), withLeader.GetTopologyFingerprint()) {
		t.Fatal("GetNodeList and GetShardMap disagree on the fingerprint")
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
