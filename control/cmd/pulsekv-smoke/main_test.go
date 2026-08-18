package main

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/router"
)

func TestShardMapDifferences(t *testing.T) {
	tests := []struct {
		name         string
		got          map[uint32]string
		want         map[uint32]string
		wantProblems []string
	}{
		{
			name:         "exact match",
			got:          map[uint32]string{0: "node-0", 1: "node-1"},
			want:         map[uint32]string{0: "node-0", 1: "node-1"},
			wantProblems: []string{},
		},
		{
			name:         "wrong owner",
			got:          map[uint32]string{0: "node-1"},
			want:         map[uint32]string{0: "node-0"},
			wantProblems: []string{`shard 0 owner "node-1", router.AssignShards wants "node-0"`},
		},
		{
			name: "missing and unexpected",
			got:  map[uint32]string{2: "node-2"},
			want: map[uint32]string{1: "node-1"},
			wantProblems: []string{
				`live map missing shard 1 (router.AssignShards owner "node-1")`,
				`live map has unexpected shard 2 owned by "node-2"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shardMapDifferences(tt.got, tt.want, 5)
			if !reflect.DeepEqual(got, tt.wantProblems) {
				t.Fatalf("shardMapDifferences() = %#v, want %#v", got, tt.wantProblems)
			}
		})
	}
}

func TestShardMapDifferencesIsBoundedAndSorted(t *testing.T) {
	got := map[uint32]string{3: "bad", 1: "bad", 2: "bad"}
	want := map[uint32]string{1: "good", 2: "good", 3: "good"}

	problems := shardMapDifferences(got, want, 2)
	wantProblems := []string{
		`shard 1 owner "bad", router.AssignShards wants "good"`,
		`shard 2 owner "bad", router.AssignShards wants "good"`,
		"shard map differs on 1 more shard(s)",
	}
	if !reflect.DeepEqual(problems, wantProblems) {
		t.Fatalf("shardMapDifferences() = %#v, want %#v", problems, wantProblems)
	}
}

func TestRoutingSampleKeysCoverDistinctOwners(t *testing.T) {
	nodeIDs := []string{"node-0", "node-1", "node-2", "node-3"}
	const shardCount = 256
	shardMap := router.AssignShards(nodeIDs, shardCount)

	keys, err := routingSampleKeys("test-routing", 6, shardCount, shardMap)
	if err != nil {
		t.Fatalf("routingSampleKeys() error: %v", err)
	}
	if len(keys) != 6 {
		t.Fatalf("routingSampleKeys() returned %d keys, want 6", len(keys))
	}

	seenKeys := make(map[string]struct{}, len(keys))
	seenOwners := make(map[string]struct{}, len(nodeIDs))
	for _, key := range keys {
		if _, duplicate := seenKeys[string(key)]; duplicate {
			t.Fatalf("routingSampleKeys() returned duplicate key %q", key)
		}
		seenKeys[string(key)] = struct{}{}
		owner, ok := router.OwnerForKey(key, shardCount, shardMap)
		if !ok {
			t.Fatalf("sample key %q has no owner", key)
		}
		seenOwners[owner] = struct{}{}
	}
	if len(seenOwners) != len(nodeIDs) {
		t.Fatalf("sample keys covered %d owners, want all %d", len(seenOwners), len(nodeIDs))
	}
}

// The exclusion node must hold NO copy of the shard. Picking merely "not the
// primary" was correct until Phase 4 and is now actively wrong: a replica is
// supposed to have the key, so an assertion against one would fail exactly when
// replication is working.
func TestNonHolderSkipsReplicasNotJustThePrimary(t *testing.T) {
	nodes := []string{"node-0", "node-1", "node-2"}
	topology := liveRoutingTopology{
		shardMap: map[uint32]string{0: "node-0", 1: "node-2"},
		owners: map[uint32]router.ShardOwners{
			0: {Primary: "node-0", Replicas: []string{"node-1"}},
			1: {Primary: "node-2"},
		},
	}

	if got := nonHolder(0, topology, nodes); got != "node-2" {
		t.Fatalf("nonHolder(shard 0) = %q; node-1 is a replica and must be skipped", got)
	}
	if got := nonHolder(1, topology, nodes); got != "node-0" {
		t.Fatalf("nonHolder(shard 1) = %q, want node-0", got)
	}

	// Without an owner map -- a pre-Phase-4 publisher -- only the primary is
	// excluded, which is exactly the old behaviour.
	unreplicated := liveRoutingTopology{shardMap: map[uint32]string{0: "node-0"}}
	if got := nonHolder(0, unreplicated, nodes); got != "node-1" {
		t.Fatalf("nonHolder with no owner map = %q, want node-1", got)
	}

	// Fully replicated: no node can serve as an exclusion proof.
	full := liveRoutingTopology{
		shardMap: map[uint32]string{0: "node-0"},
		owners: map[uint32]router.ShardOwners{
			0: {Primary: "node-0", Replicas: []string{"node-1", "node-2"}},
		},
	}
	if got := nonHolder(0, full, nodes); got != "" {
		t.Fatalf("nonHolder on a fully replicated shard = %q, want empty", got)
	}
}

func TestCheckRoutingUsesSDKAndProvesPhysicalPlacement(t *testing.T) {
	const shardCount = 256

	nodeIDs := []string{"node-0", "node-1", "node-2", "node-3"}
	nodeServers := make(map[string]*routingTestNode, len(nodeIDs))
	nodes := make([]config.Node, 0, len(nodeIDs))
	nodeInfos := make([]*metadatav1.NodeInfo, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		node := &routingTestNode{values: make(map[string][]byte)}
		address := startRoutingTestNode(t, node)
		host, port := splitRoutingTestAddress(t, address)
		nodeServers[id] = node
		nodes = append(nodes, config.Node{NodeID: id, Host: host, Port: port})
		nodeInfos = append(nodeInfos, &metadatav1.NodeInfo{
			NodeId: id, Address: address, Alive: true,
		})
	}

	shardMap := router.AssignShards(nodeIDs, shardCount)
	metadataAddress := startRoutingTestMetadata(t, &routingTestMetadata{
		nodes: nodeInfos, shardMap: shardMap,
	})
	metadataHost, metadataPort := splitRoutingTestAddress(t, metadataAddress)
	cfg := &config.Config{
		ControlPlanes: config.ControlPlaneList{{
			NodeID: "cp-0", Host: metadataHost, Port: metadataPort,
		}},
		ShardCount: shardCount,
		Nodes:      nodes,
	}

	conn, err := dial(metadataAddress)
	if err != nil {
		t.Fatalf("dial metadata: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	r := &reporter{}
	checkRouting(r, cfg, conn, 2*time.Second)
	if len(r.results) != 1+routingSampleCount {
		t.Fatalf("checkRouting recorded %d results, want %d: %#v",
			len(r.results), 1+routingSampleCount, r.results)
	}
	for _, result := range r.results {
		if result.err != nil {
			t.Errorf("checkRouting %s failed: %v", result.name, result.err)
		}
	}

	seenKeys := make(map[string]string, routingSampleCount)
	for id, node := range nodeServers {
		node.mu.Lock()
		if len(node.values) == 0 {
			t.Errorf("%s received no routed sample", id)
		}
		for key := range node.values {
			if previous, duplicate := seenKeys[key]; duplicate {
				t.Errorf("key %q was stored on both %s and %s", key, previous, id)
			}
			seenKeys[key] = id
			owner, ok := router.OwnerForKey([]byte(key), shardCount, shardMap)
			if !ok || owner != id {
				t.Errorf("key %q stored on %s, predicted owner is %q (ok=%v)", key, id, owner, ok)
			}
		}
		node.mu.Unlock()
	}
	if len(seenKeys) != routingSampleCount {
		t.Errorf("nodes stored %d routed samples, want %d", len(seenKeys), routingSampleCount)
	}
}

type routingTestNode struct {
	nodev1.UnimplementedNodeServiceServer
	mu     sync.Mutex
	values map[string][]byte
}

func (n *routingTestNode) Put(_ context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
	n.mu.Lock()
	n.values[string(req.GetKey())] = append([]byte(nil), req.GetValue()...)
	n.mu.Unlock()
	return &nodev1.PutResponse{Ok: true}, nil
}

func (n *routingTestNode) Get(_ context.Context, req *nodev1.GetRequest) (*nodev1.GetResponse, error) {
	n.mu.Lock()
	value, found := n.values[string(req.GetKey())]
	value = append([]byte(nil), value...)
	n.mu.Unlock()
	return &nodev1.GetResponse{Found: found, Value: value}, nil
}

type routingTestMetadata struct {
	metadatav1.UnimplementedClusterMetadataServiceServer
	nodes    []*metadatav1.NodeInfo
	shardMap map[uint32]string
}

func (m *routingTestMetadata) GetNodeList(context.Context,
	*metadatav1.GetNodeListRequest) (*metadatav1.GetNodeListResponse, error) {
	return &metadatav1.GetNodeListResponse{Nodes: m.nodes}, nil
}

func (m *routingTestMetadata) GetShardMap(context.Context,
	*metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	return &metadatav1.GetShardMapResponse{ShardToNodeId: m.shardMap}, nil
}

func startRoutingTestNode(t *testing.T, node nodev1.NodeServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for routing test node: %v", err)
	}
	server := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(server, node)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func startRoutingTestMetadata(t *testing.T, metadata metadatav1.ClusterMetadataServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for routing test metadata: %v", err)
	}
	server := grpc.NewServer()
	metadatav1.RegisterClusterMetadataServiceServer(server, metadata)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func splitRoutingTestAddress(t *testing.T, address string) (string, int) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split address %q: %v", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse port in %q: %v", address, err)
	}
	return host, port
}
