package topology

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/router"
)

type scriptedMetadata struct {
	metadatav1.ClusterMetadataServiceClient

	mu         sync.Mutex
	nodeCalls  int
	shardCalls int
	nodeGens   []uint64
	shardGens  []uint64
	nodeFPs    [][]byte
	shardFPs   [][]byte
	shardCount uint32
	nodes      []*metadatav1.NodeInfo
	shards     map[uint32]string

	owners            map[uint32]*metadatav1.ShardOwners
	replicationFactor uint32
}

func (m *scriptedMetadata) GetNodeList(context.Context, *metadatav1.GetNodeListRequest,
	...grpc.CallOption) (*metadatav1.GetNodeListResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation := m.nodeGens[min(m.nodeCalls, len(m.nodeGens)-1)]
	var fingerprint []byte
	if len(m.nodeFPs) > 0 {
		fingerprint = m.nodeFPs[min(m.nodeCalls, len(m.nodeFPs)-1)]
	}
	m.nodeCalls++
	return &metadatav1.GetNodeListResponse{
		Nodes:               m.nodes,
		TopologyGeneration:  generation,
		TopologyFingerprint: bytes.Clone(fingerprint),
	}, nil
}

func (m *scriptedMetadata) GetShardMap(context.Context, *metadatav1.GetShardMapRequest,
	...grpc.CallOption) (*metadatav1.GetShardMapResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation := m.shardGens[min(m.shardCalls, len(m.shardGens)-1)]
	var fingerprint []byte
	if len(m.shardFPs) > 0 {
		fingerprint = m.shardFPs[min(m.shardCalls, len(m.shardFPs)-1)]
	}
	m.shardCalls++
	return &metadatav1.GetShardMapResponse{
		ShardToNodeId:       m.shards,
		TopologyGeneration:  generation,
		TopologyFingerprint: bytes.Clone(fingerprint),
		ShardCount:          m.shardCount,
		ShardToOwners:       m.owners,
		ReplicationFactor:   m.replicationFactor,
	}, nil
}

func TestFetchUsesFingerprintAcrossReusedGeneration(t *testing.T) {
	oldFingerprint := bytes.Repeat([]byte{0x11}, FingerprintSize)
	newFingerprint := Fingerprint(FingerprintInput{
		ShardCount: 1,
		Nodes:      map[string]string{"node-a": "127.0.0.1:7100"},
		ShardMap:   map[uint32]string{0: "node-a"},
	})
	metadata := &scriptedMetadata{
		nodeGens:   []uint64{1, 1},
		shardGens:  []uint64{1, 1},
		nodeFPs:    [][]byte{oldFingerprint, newFingerprint},
		shardFPs:   [][]byte{newFingerprint, newFingerprint},
		shardCount: 1,
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		},
		shards: map[uint32]string{0: "node-a"},
	}

	snapshot, err := Fetch(context.Background(), metadata)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(snapshot.Fingerprint, newFingerprint) {
		t.Fatalf("fingerprint = %x, want %x", snapshot.Fingerprint, newFingerprint)
	}
	if metadata.nodeCalls != 2 || metadata.shardCalls != 2 {
		t.Fatalf("calls = nodes %d, shards %d; want retry after reused generation",
			metadata.nodeCalls, metadata.shardCalls)
	}
}

func TestFetchAcceptsAuthoritativeEmptyTopology(t *testing.T) {
	fingerprint := Fingerprint(FingerprintInput{
		ShardCount: 256,
		Nodes:      map[string]string{},
		ShardMap:   map[uint32]string{},
	})
	metadata := &scriptedMetadata{
		nodeGens:   []uint64{9},
		shardGens:  []uint64{9},
		nodeFPs:    [][]byte{fingerprint},
		shardFPs:   [][]byte{fingerprint},
		shardCount: 256,
		shards:     map[uint32]string{},
	}

	snapshot, err := Fetch(context.Background(), metadata)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if snapshot.ShardCount != 256 || len(snapshot.Nodes) != 0 || len(snapshot.ShardMap) != 0 {
		t.Fatalf("empty snapshot = %+v", snapshot)
	}
}

func TestFetchRejectsFingerprintThatDoesNotDescribeContent(t *testing.T) {
	fingerprint := bytes.Repeat([]byte{0x44}, FingerprintSize)
	metadata := &scriptedMetadata{
		nodeGens:   []uint64{1},
		shardGens:  []uint64{1},
		nodeFPs:    [][]byte{fingerprint},
		shardFPs:   [][]byte{fingerprint},
		shardCount: 1,
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		},
		shards: map[uint32]string{0: "node-a"},
	}

	_, err := Fetch(context.Background(), metadata)
	if err == nil || !strings.Contains(err.Error(), "does not match response content") {
		t.Fatalf("Fetch error = %v, want fingerprint/content mismatch", err)
	}
}

func TestFingerprintIsIndependentOfMapIterationOrder(t *testing.T) {
	first := Fingerprint(FingerprintInput{
		ShardCount: 2,
		Nodes:      map[string]string{"node-b": "127.0.0.1:7101", "node-a": "127.0.0.1:7100"},
		ShardMap:   map[uint32]string{1: "node-b", 0: "node-a"},
		Owners: map[uint32]router.ShardOwners{
			1: {Primary: "node-b", Replicas: []string{"node-a"}},
			0: {Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	second := Fingerprint(FingerprintInput{
		ShardCount: 2,
		Nodes:      map[string]string{"node-a": "127.0.0.1:7100", "node-b": "127.0.0.1:7101"},
		ShardMap:   map[uint32]string{0: "node-a", 1: "node-b"},
		Owners: map[uint32]router.ShardOwners{
			0: {Primary: "node-a", Replicas: []string{"node-b"}},
			1: {Primary: "node-b", Replicas: []string{"node-a"}},
		},
	})
	if !bytes.Equal(first, second) {
		t.Fatalf("fingerprints differ: %x != %x", first, second)
	}
}

func TestFetchRetriesTornGenerations(t *testing.T) {
	metadata := &scriptedMetadata{
		nodeGens:  []uint64{1, 2},
		shardGens: []uint64{2, 2},
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		},
		shards: map[uint32]string{0: "node-a"},
	}

	snapshot, err := Fetch(context.Background(), metadata)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if snapshot.Generation != 2 {
		t.Fatalf("generation = %d, want 2", snapshot.Generation)
	}
	if metadata.nodeCalls != 2 || metadata.shardCalls != 2 {
		t.Fatalf("calls = nodes %d, shards %d; want 2 each",
			metadata.nodeCalls, metadata.shardCalls)
	}
}

func TestFetchRejectsPermanentlyTornGenerations(t *testing.T) {
	metadata := &scriptedMetadata{
		nodeGens:  []uint64{1},
		shardGens: []uint64{2},
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		},
		shards: map[uint32]string{0: "node-a"},
	}

	_, err := Fetch(context.Background(), metadata)
	if err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("Fetch error = %v, want generation convergence error", err)
	}
	if metadata.nodeCalls != maxCoherenceAttempts || metadata.shardCalls != maxCoherenceAttempts {
		t.Fatalf("calls = nodes %d, shards %d; want %d each",
			metadata.nodeCalls, metadata.shardCalls, maxCoherenceAttempts)
	}
}

// ---------------------------------------------------------------------------
// Phase 4: replica placement travels with the topology and is part of its
// identity.
// ---------------------------------------------------------------------------

func twoNodeOwnerFixture() *scriptedMetadata {
	nodes := map[string]string{"node-a": "127.0.0.1:7100", "node-b": "127.0.0.1:7101"}
	shards := map[uint32]string{0: "node-a", 1: "node-b"}
	owners := map[uint32]router.ShardOwners{
		0: {Primary: "node-a", Replicas: []string{"node-b"}},
		1: {Primary: "node-b", Replicas: []string{"node-a"}},
	}
	fingerprint := Fingerprint(FingerprintInput{
		ShardCount: 2, ReplicationFactor: 1, Nodes: nodes, ShardMap: shards, Owners: owners,
	})
	return &scriptedMetadata{
		nodeGens:   []uint64{4},
		shardGens:  []uint64{4},
		nodeFPs:    [][]byte{fingerprint},
		shardFPs:   [][]byte{fingerprint},
		shardCount: 2,
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
			{NodeId: "node-b", Address: "127.0.0.1:7101", Alive: true},
		},
		shards: shards,
		owners: map[uint32]*metadatav1.ShardOwners{
			0: {Primary: "node-a", Replicas: []string{"node-b"}},
			1: {Primary: "node-b", Replicas: []string{"node-a"}},
		},
		replicationFactor: 1,
	}
}

func TestFetchInstallsReplicaPlacement(t *testing.T) {
	snapshot, err := Fetch(context.Background(), twoNodeOwnerFixture())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if snapshot.ReplicationFactor != 1 {
		t.Fatalf("replication factor = %d, want 1", snapshot.ReplicationFactor)
	}
	if len(snapshot.Owners) != 2 {
		t.Fatalf("owners = %v, want two shards", snapshot.Owners)
	}
	for shard, want := range map[uint32]string{0: "node-b", 1: "node-a"} {
		replicas := snapshot.Replicas(shard)
		if len(replicas) != 1 || replicas[0] != want {
			t.Fatalf("shard %d replicas = %v, want [%s]", shard, replicas, want)
		}
		if snapshot.Owners[shard].Primary != snapshot.ShardMap[shard] {
			t.Fatalf("shard %d primary %q != shard map owner %q",
				shard, snapshot.Owners[shard].Primary, snapshot.ShardMap[shard])
		}
	}
	// Reads are primary-only in Phase 4, so a replica must not make an address
	// look like an owner for routing or connection-retirement purposes.
	if !snapshot.OwnsAddress("127.0.0.1:7100") || !snapshot.OwnsAddress("127.0.0.1:7101") {
		t.Fatal("both nodes primary a shard here and must be owners")
	}
}

// Replica placement is part of the topology's identity: two clusters that agree
// on every primary but disagree on replicas are not the same topology, and a
// fingerprint that could not tell them apart would let a node keep forwarding
// writes to a peer that no longer holds the shard.
func TestFingerprintDistinguishesReplicaPlacement(t *testing.T) {
	base := FingerprintInput{
		ShardCount: 2,
		Nodes:      map[string]string{"node-a": "127.0.0.1:7100", "node-b": "127.0.0.1:7101"},
		ShardMap:   map[uint32]string{0: "node-a", 1: "node-b"},
	}

	noReplicas := base
	noReplicas.Owners = map[uint32]router.ShardOwners{
		0: {Primary: "node-a"}, 1: {Primary: "node-b"},
	}
	withReplicas := base
	withReplicas.ReplicationFactor = 1
	withReplicas.Owners = map[uint32]router.ShardOwners{
		0: {Primary: "node-a", Replicas: []string{"node-b"}},
		1: {Primary: "node-b", Replicas: []string{"node-a"}},
	}

	if bytes.Equal(Fingerprint(noReplicas), Fingerprint(withReplicas)) {
		t.Fatal("adding replicas did not change the topology fingerprint")
	}

	// The replication factor alone must move it too: a cluster configured for
	// two replicas but currently able to place one is a different, and worse,
	// state than a cluster configured for one and placing one.
	sameOwnersHigherFactor := withReplicas
	sameOwnersHigherFactor.ReplicationFactor = 2
	if bytes.Equal(Fingerprint(withReplicas), Fingerprint(sameOwnersHigherFactor)) {
		t.Fatal("changing the replication factor did not change the fingerprint")
	}
}

// The compatibility seam again, this time enforced on the client side: a
// publisher whose two ownership views disagree is refused outright rather than
// having one of them silently win.
func TestFetchRejectsOwnerMapThatContradictsShardMap(t *testing.T) {
	metadata := twoNodeOwnerFixture()
	metadata.owners[0] = &metadatav1.ShardOwners{Primary: "node-b", Replicas: []string{"node-a"}}

	_, err := Fetch(context.Background(), metadata)
	if err == nil || !strings.Contains(err.Error(), "shard_to_node_id") {
		t.Fatalf("Fetch error = %v, want a primary/owner disagreement", err)
	}
}

func TestFetchRejectsMalformedOwnerMaps(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*scriptedMetadata)
		wantSub string
	}{
		{
			name:    "unknown replica",
			mutate:  func(m *scriptedMetadata) { m.owners[0].Replicas = []string{"node-ghost"} },
			wantSub: "unknown replica",
		},
		{
			name:    "self replica",
			mutate:  func(m *scriptedMetadata) { m.owners[0].Replicas = []string{"node-a"} },
			wantSub: "twice",
		},
		{
			name:    "duplicate replica",
			mutate:  func(m *scriptedMetadata) { m.owners[1].Replicas = []string{"node-a", "node-a"} },
			wantSub: "twice",
		},
		{
			name:    "partial owner map",
			mutate:  func(m *scriptedMetadata) { delete(m.owners, 1) },
			wantSub: "shard owner entries",
		},
		{
			name:    "more replicas than live nodes",
			mutate:  func(m *scriptedMetadata) { m.owners[0].Replicas = []string{"node-b", "node-b", "node-b"} },
			wantSub: "replicas for",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := twoNodeOwnerFixture()
			tc.mutate(metadata)
			_, err := Fetch(context.Background(), metadata)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("Fetch error = %v, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

// A pre-Phase-4 publisher sends no owner map at all. That must still install as
// a valid topology with no replicas, not fail validation.
func TestFetchAcceptsTopologyWithoutOwnerMap(t *testing.T) {
	nodes := map[string]string{"node-a": "127.0.0.1:7100"}
	shards := map[uint32]string{0: "node-a"}
	fingerprint := Fingerprint(FingerprintInput{ShardCount: 1, Nodes: nodes, ShardMap: shards})
	metadata := &scriptedMetadata{
		nodeGens:   []uint64{3},
		shardGens:  []uint64{3},
		nodeFPs:    [][]byte{fingerprint},
		shardFPs:   [][]byte{fingerprint},
		shardCount: 1,
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		},
		shards: shards,
	}

	snapshot, err := Fetch(context.Background(), metadata)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if snapshot.Owners != nil {
		t.Fatalf("owners = %v, want nil for a pre-Phase-4 publisher", snapshot.Owners)
	}
	if snapshot.Replicas(0) != nil {
		t.Fatalf("replicas = %v, want nil", snapshot.Replicas(0))
	}
}

func TestOwnerAddressesExcludeKnownUnownedNodes(t *testing.T) {
	snapshot, err := Validate(7, []*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: "127.0.0.1:7100", Alive: true},
		{NodeId: "node-b", Address: "127.0.0.1:7101", Alive: false},
	}, map[uint32]string{0: "node-a", 1: "node-a"})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	addresses := snapshot.OwnerAddresses()
	if len(addresses) != 1 || addresses[0] != "127.0.0.1:7100" {
		t.Fatalf("owner addresses = %v, want only node-a", addresses)
	}
	if !snapshot.OwnsAddress("127.0.0.1:7100") {
		t.Fatal("owner address was not recognized")
	}
	if snapshot.OwnsAddress("127.0.0.1:7101") {
		t.Fatal("unowned address was recognized as an owner")
	}
}
