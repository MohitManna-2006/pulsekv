package topology

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	metadatav1 "pulsekv/control/gen/metadata/v1"
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
	}, nil
}

func TestFetchUsesFingerprintAcrossReusedGeneration(t *testing.T) {
	oldFingerprint := bytes.Repeat([]byte{0x11}, FingerprintSize)
	newFingerprint := Fingerprint(1,
		map[string]string{"node-a": "127.0.0.1:7100"},
		map[uint32]string{0: "node-a"})
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
	fingerprint := Fingerprint(256, map[string]string{}, map[uint32]string{})
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
	first := Fingerprint(2,
		map[string]string{"node-b": "127.0.0.1:7101", "node-a": "127.0.0.1:7100"},
		map[uint32]string{1: "node-b", 0: "node-a"})
	second := Fingerprint(2,
		map[string]string{"node-a": "127.0.0.1:7100", "node-b": "127.0.0.1:7101"},
		map[uint32]string{0: "node-a", 1: "node-b"})
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
