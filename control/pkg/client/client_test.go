package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
)

type testMetadata struct {
	metadatav1.UnimplementedClusterMetadataServiceServer

	mu         sync.RWMutex
	nodes      []*metadatav1.NodeInfo
	shards     map[uint32]string
	nodeGen    uint64
	shardGen   uint64
	shardCount uint32
	nodeErr    error
	shardErr   error
	nodeCalls  atomic.Int64
	shardCalls atomic.Int64
}

func (m *testMetadata) GetNodeList(context.Context, *metadatav1.GetNodeListRequest) (*metadatav1.GetNodeListResponse, error) {
	m.nodeCalls.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.nodeErr != nil {
		return nil, m.nodeErr
	}
	return &metadatav1.GetNodeListResponse{
		Nodes:              m.nodes,
		TopologyGeneration: m.nodeGen,
	}, nil
}

func (m *testMetadata) GetShardMap(context.Context, *metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	m.shardCalls.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shardErr != nil {
		return nil, m.shardErr
	}
	return &metadatav1.GetShardMapResponse{
		ShardToNodeId:      m.shards,
		TopologyGeneration: m.shardGen,
		ShardCount:         m.shardCount,
	}, nil
}

func (m *testMetadata) setTopology(nodes []*metadatav1.NodeInfo, shards map[uint32]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = nodes
	m.shards = shards
	m.shardCount = uint32(len(shards))
	m.nodeErr = nil
	m.shardErr = nil
}

func (m *testMetadata) setTopologyGeneration(nodes []*metadatav1.NodeInfo,
	shards map[uint32]string, generation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = nodes
	m.shards = shards
	m.shardCount = uint32(len(shards))
	m.nodeGen = generation
	m.shardGen = generation
	m.nodeErr = nil
	m.shardErr = nil
}

func (m *testMetadata) setGenerations(nodeGeneration, shardGeneration uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeGen = nodeGeneration
	m.shardGen = shardGeneration
	m.nodeErr = nil
	m.shardErr = nil
}

func (m *testMetadata) setErrors(nodeErr, shardErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeErr = nodeErr
	m.shardErr = shardErr
}

type testNode struct {
	nodev1.UnimplementedNodeServiceServer

	mu           sync.Mutex
	values       map[string][]byte
	vanishOnScan map[string]bool
	puts         int
	chunkedPuts  int
	chunkedGets  int
	prefixScans  int

	// Phase 4: what the last Put asked for, and what this fake pretends its
	// replicas did about it.
	lastRequestedAcks atomic.Uint32
	replicasAcked     uint32
	putErr            error
}

func newTestNode() *testNode {
	return &testNode{values: make(map[string][]byte), vanishOnScan: make(map[string]bool)}
}

func (n *testNode) Put(_ context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
	n.lastRequestedAcks.Store(req.GetRequireReplicaAcks())
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.putErr != nil {
		return nil, n.putErr
	}
	n.values[string(req.GetKey())] = append([]byte(nil), req.GetValue()...)
	n.puts++
	return &nodev1.PutResponse{Ok: true, ReplicasAcked: n.replicasAcked}, nil
}

func (n *testNode) Get(_ context.Context, req *nodev1.GetRequest) (*nodev1.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	value, ok := n.values[string(req.GetKey())]
	if ok && len(value) > int(nodev1.UnaryLimit_UNARY_VALUE_LIMIT_BYTES) {
		return nil, status.Error(codes.FailedPrecondition, "use GetChunked")
	}
	return &nodev1.GetResponse{Found: ok, Value: append([]byte(nil), value...)}, nil
}

func (n *testNode) PutChunked(stream grpc.ClientStreamingServer[nodev1.PutChunk, nodev1.PutResponse]) error {
	var (
		key    []byte
		value  []byte
		chunks int
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunks == 0 {
			key = bytes.Clone(chunk.GetKey())
		}
		value = append(value, chunk.GetData()...)
		chunks++
	}
	if chunks == 0 {
		return status.Error(codes.InvalidArgument, "empty PutChunked stream")
	}

	n.mu.Lock()
	n.values[string(key)] = value
	n.puts++
	n.chunkedPuts++
	n.mu.Unlock()
	return stream.SendAndClose(&nodev1.PutResponse{Ok: true})
}

func (n *testNode) GetChunked(req *nodev1.GetRequest, stream grpc.ServerStreamingServer[nodev1.GetChunk]) error {
	n.mu.Lock()
	value, ok := n.values[string(req.GetKey())]
	value = bytes.Clone(value)
	if ok {
		n.chunkedGets++
	}
	n.mu.Unlock()
	if !ok {
		return nil
	}

	const chunkSize = 1024 * 1024
	totalChunks := 1
	if len(value) > 0 {
		totalChunks = (len(value) + chunkSize - 1) / chunkSize
	}
	for i := 0; i < totalChunks; i++ {
		lo := i * chunkSize
		hi := min(lo+chunkSize, len(value))
		if err := stream.Send(&nodev1.GetChunk{
			ChunkIndex:  uint32(i),
			TotalChunks: uint32(totalChunks),
			TotalLength: uint64(len(value)),
			Data:        value[lo:hi],
		}); err != nil {
			return err
		}
	}
	return nil
}

func (n *testNode) PrefixMatch(req *nodev1.PrefixMatchRequest, stream grpc.ServerStreamingServer[nodev1.PrefixMatchResponse]) error {
	n.mu.Lock()
	n.prefixScans++
	keys := make([]string, 0)
	for key := range n.values {
		if strings.HasPrefix(key, string(req.GetPrefix())) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		values[key] = append([]byte(nil), n.values[key]...)
	}
	n.mu.Unlock()

	for _, key := range keys {
		if err := stream.Send(&nodev1.PrefixMatchResponse{Key: []byte(key), Value: values[key]}); err != nil {
			return err
		}
		n.mu.Lock()
		if n.vanishOnScan[key] {
			delete(n.values, key)
		}
		n.mu.Unlock()
	}
	return nil
}

func (n *testNode) has(key []byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.values[string(key)]
	return ok
}

func serveTestGRPC(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func keyOwnedBy(t *testing.T, owner string, shards map[uint32]string, shardCount uint32, prefix string) []byte {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("%s:%d", prefix, i))
		if got, ok := router.OwnerForKey(key, shardCount, shards); ok && got == owner {
			return key
		}
	}
	t.Fatalf("could not find a key owned by %s", owner)
	return nil
}

func waitForTopologyGeneration(t *testing.T, c *Client, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		generation := c.topology.Generation
		c.mu.RUnlock()
		if generation == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	c.mu.RLock()
	got := c.topology.Generation
	c.mu.RUnlock()
	t.Fatalf("topology generation = %d, want %d before deadline", got, want)
}

// ---------------------------------------------------------------------------
// Phase 4: PutWithAck
// ---------------------------------------------------------------------------

// PutWithAck must reach the primary exactly the way Put already does -- the SDK
// deliberately learns nothing about replicas -- while carrying the requested
// ack count and returning what the primary reports.
func TestPutWithAckRoutesToThePrimaryAndReportsAcks(t *testing.T) {
	nodeA, nodeB := newTestNode(), newTestNode()
	nodeA.replicasAcked = 2
	nodeB.replicasAcked = 2
	addrA := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeA) })
	addrB := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeB) })

	ids := []string{"node-a", "node-b"}
	const shardCount = 32
	shards := router.AssignShards(ids, shardCount)
	md := &testMetadata{
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: addrA, Alive: true},
			{NodeId: "node-b", Address: addrB, Alive: true},
		},
		shards: shards,
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	key := keyOwnedBy(t, "node-a", shards, shardCount, "ack")
	acked, err := c.PutWithAck(context.Background(), key, []byte("value"), 2)
	if err != nil {
		t.Fatalf("PutWithAck: %v", err)
	}
	if acked != 2 {
		t.Fatalf("acked = %d, want the 2 the primary reported", acked)
	}
	if !nodeA.has(key) {
		t.Fatal("strong-ack write did not land on the primary")
	}
	if nodeB.has(key) {
		t.Fatal("the SDK contacted a node that does not primary this key")
	}
	if got := nodeA.lastRequestedAcks.Load(); got != 2 {
		t.Fatalf("primary saw require_replica_acks = %d, want 2", got)
	}

	// Plain Put is unchanged: still asks for nothing, still reports nothing.
	if err := c.Put(context.Background(), key, []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := nodeA.lastRequestedAcks.Load(); got != 0 {
		t.Fatalf("plain Put asked for %d ack(s); it must ask for none", got)
	}
}

// An ack shortfall is a real error the caller must see, not something the SDK
// smooths over. It also must not be mistaken for a lost write.
func TestPutWithAckSurfacesTheNodesError(t *testing.T) {
	node := newTestNode()
	node.putErr = status.Error(codes.DeadlineExceeded,
		"replicated to 0 of the 2 requested replica(s); the local write is committed")
	addr := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, node) })

	const shardCount = 8
	shards := router.AssignShards([]string{"node-a"}, shardCount)
	md := &testMetadata{
		nodes:  []*metadatav1.NodeInfo{{NodeId: "node-a", Address: addr, Alive: true}},
		shards: shards,
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	acked, err := c.PutWithAck(context.Background(), []byte("k"), []byte("v"), 2)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("PutWithAck error = %v, want DEADLINE_EXCEEDED to reach the caller", err)
	}
	if acked != 0 {
		t.Fatalf("acked = %d, want 0 on a failed strong-ack write", acked)
	}
	if got := node.lastRequestedAcks.Load(); got != 2 {
		t.Fatalf("node saw require_replica_acks = %d, want 2", got)
	}
}

func TestClientRoutesAndScansCluster(t *testing.T) {
	nodeA, nodeB := newTestNode(), newTestNode()
	addrA := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeA) })
	addrB := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeB) })

	ids := []string{"node-a", "node-b"}
	const shardCount = 32
	shards := router.AssignShards(ids, shardCount)
	md := &testMetadata{
		nodes: []*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: addrA, Alive: true},
			{NodeId: "node-b", Address: addrB, Alive: true},
		},
		shards: shards,
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	keyA := keyOwnedBy(t, "node-a", shards, shardCount, "route-a")
	keyB := keyOwnedBy(t, "node-b", shards, shardCount, "route-b")
	for _, tc := range []struct {
		key   []byte
		value []byte
		owner *testNode
		other *testNode
	}{
		{keyA, []byte("value-a"), nodeA, nodeB},
		{keyB, []byte("value-b"), nodeB, nodeA},
	} {
		if err := c.Put(context.Background(), tc.key, tc.value); err != nil {
			t.Fatalf("Put(%q): %v", tc.key, err)
		}
		if !tc.owner.has(tc.key) {
			t.Errorf("key %q did not land on predicted owner", tc.key)
		}
		if tc.other.has(tc.key) {
			t.Errorf("key %q also landed on non-owner", tc.key)
		}
		got, found, err := c.Get(context.Background(), tc.key)
		if err != nil || !found || !bytes.Equal(got, tc.value) {
			t.Errorf("Get(%q) = (%q, %v, %v), want (%q, true, nil)",
				tc.key, got, found, err, tc.value)
		}
	}

	if got, found, err := c.Get(context.Background(), []byte("missing")); err != nil || found || got != nil {
		t.Errorf("missing Get = (%q, %v, %v), want (nil, false, nil)", got, found, err)
	}

	stableA := keyOwnedBy(t, "node-a", shards, shardCount, "notes:stable-a")
	stableB := keyOwnedBy(t, "node-b", shards, shardCount, "notes:stable-b")
	vanish := keyOwnedBy(t, "node-a", shards, shardCount, "notes:vanish")
	for _, pair := range []struct {
		key, value []byte
	}{
		{stableA, []byte("one")},
		{stableB, []byte("two")},
		{vanish, []byte("gone")},
	} {
		if err := c.Put(context.Background(), pair.key, pair.value); err != nil {
			t.Fatalf("Put(%q): %v", pair.key, err)
		}
	}
	nodeA.mu.Lock()
	nodeA.vanishOnScan[string(vanish)] = true
	nodeA.mu.Unlock()

	matches, err := c.PrefixMatch(context.Background(), []byte("notes:"))
	if err != nil {
		t.Fatalf("PrefixMatch: %v", err)
	}
	if got := matches[string(stableA)]; !bytes.Equal(got, []byte("one")) {
		t.Errorf("stable A = %q, want one", got)
	}
	if got := matches[string(stableB)]; !bytes.Equal(got, []byte("two")) {
		t.Errorf("stable B = %q, want two", got)
	}
	if _, ok := matches[string(vanish)]; ok {
		t.Error("PrefixMatch returned a key that vanished after the non-snapshot scan")
	}
	if len(matches) != 2 {
		t.Errorf("PrefixMatch returned %d matches, want 2: %v", len(matches), matches)
	}

	deadline := time.Now().Add(time.Second)
	for (md.nodeCalls.Load() < 2 || md.shardCalls.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if md.nodeCalls.Load() < 2 || md.shardCalls.Load() < 2 {
		t.Errorf("metadata was not refreshed: node calls=%d shard calls=%d",
			md.nodeCalls.Load(), md.shardCalls.Load())
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := c.Get(context.Background(), keyA); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close error = %v, want ErrClosed", err)
	}
}

func TestRefreshInstallsOnlyCompleteValidTopology(t *testing.T) {
	nodeA, nodeB := newTestNode(), newTestNode()
	addrA := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeA) })
	addrB := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeB) })
	nodes := []*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: addrA, Alive: true},
		{NodeId: "node-b", Address: addrB, Alive: true},
	}
	md := &testMetadata{nodes: nodes, shards: map[uint32]string{0: "node-a"}}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	initialKey := []byte("refresh:initial")
	if err := c.Put(context.Background(), initialKey, []byte("a")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	if !nodeA.has(initialKey) || nodeB.has(initialKey) {
		t.Fatal("initial topology did not route to node-a")
	}

	md.setTopology(nodes, map[uint32]string{0: "node-b"})
	if err := c.refresh(context.Background()); err != nil {
		t.Fatalf("valid refresh: %v", err)
	}
	updatedKey := []byte("refresh:updated")
	if err := c.Put(context.Background(), updatedKey, []byte("b")); err != nil {
		t.Fatalf("Put after valid refresh: %v", err)
	}
	if !nodeB.has(updatedKey) || nodeA.has(updatedKey) {
		t.Fatal("valid refreshed topology did not route to node-b")
	}

	md.setTopology(nodes, map[uint32]string{0: "unknown-node"})
	if err := c.refresh(context.Background()); err == nil {
		t.Fatal("invalid topology refresh succeeded")
	}
	afterInvalidKey := []byte("refresh:after-invalid")
	if err := c.Put(context.Background(), afterInvalidKey, []byte("still-b")); err != nil {
		t.Fatalf("Put after invalid refresh: %v", err)
	}
	if !nodeB.has(afterInvalidKey) || nodeA.has(afterInvalidKey) {
		t.Fatal("invalid refresh replaced the last complete topology")
	}

	md.setErrors(errors.New("metadata unavailable"), nil)
	if err := c.refresh(context.Background()); err == nil {
		t.Fatal("failing topology refresh succeeded")
	}
	afterFailureKey := []byte("refresh:after-failure")
	if err := c.Put(context.Background(), afterFailureKey, []byte("still-b")); err != nil {
		t.Fatalf("Put after failed refresh: %v", err)
	}
	if !nodeB.has(afterFailureKey) || nodeA.has(afterFailureKey) {
		t.Fatal("failed refresh replaced the last complete topology")
	}
}

func TestRefreshRejectsTornGenerationAndRetainsLastGood(t *testing.T) {
	nodeA, nodeB := newTestNode(), newTestNode()
	addrA := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeA) })
	addrB := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeB) })
	nodes := []*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: addrA, Alive: true},
		{NodeId: "node-b", Address: addrB, Alive: true},
	}
	md := &testMetadata{}
	md.setTopologyGeneration(nodes, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	md.setTopology(nodes, map[uint32]string{0: "node-b"})
	md.setGenerations(2, 3)
	if err := c.refresh(context.Background()); err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("torn refresh error = %v, want convergence error", err)
	}

	key := []byte("torn-generation")
	if err := c.Put(context.Background(), key, []byte("last-good")); err != nil {
		t.Fatalf("Put with retained topology: %v", err)
	}
	if !nodeA.has(key) || nodeB.has(key) {
		t.Fatal("torn refresh replaced the last complete generation")
	}
	c.mu.RLock()
	generation := c.topology.Generation
	c.mu.RUnlock()
	if generation != 1 {
		t.Fatalf("installed generation = %d, want retained generation 1", generation)
	}
}

func TestBackgroundRefreshTracksJoinAndLeave(t *testing.T) {
	nodeA, nodeB := newTestNode(), newTestNode()
	addrA := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeA) })
	addrB := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, nodeB) })
	nodeAInfo := &metadatav1.NodeInfo{NodeId: "node-a", Address: addrA, Alive: true}
	nodeBInfo := &metadatav1.NodeInfo{NodeId: "node-b", Address: addrB, Alive: true}

	md := &testMetadata{}
	md.setTopologyGeneration([]*metadatav1.NodeInfo{nodeAInfo}, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})
	c, err := New(mdAddr,
		WithRefreshInterval(2*time.Millisecond),
		WithRefreshTimeout(250*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// node-b joins and takes the only shard.
	md.setTopologyGeneration([]*metadatav1.NodeInfo{nodeAInfo, nodeBInfo},
		map[uint32]string{0: "node-b"}, 2)
	waitForTopologyGeneration(t, c, 2)
	joinedKey := []byte("after-join")
	if err := c.Put(context.Background(), joinedKey, []byte("b")); err != nil {
		t.Fatalf("Put after join: %v", err)
	}
	if !nodeB.has(joinedKey) || nodeA.has(joinedKey) {
		t.Fatal("background refresh did not route through the joined node")
	}

	// node-b leaves; ownership returns to node-a without recreating the client.
	md.setTopologyGeneration([]*metadatav1.NodeInfo{nodeAInfo},
		map[uint32]string{0: "node-a"}, 3)
	waitForTopologyGeneration(t, c, 3)
	leftKey := []byte("after-leave")
	if err := c.Put(context.Background(), leftKey, []byte("a")); err != nil {
		t.Fatalf("Put after leave: %v", err)
	}
	if !nodeA.has(leftKey) || nodeB.has(leftKey) {
		t.Fatal("background refresh did not route away from the departed node")
	}
}

func TestRefreshInstallsAuthoritativeEmptyTopology(t *testing.T) {
	node := newTestNode()
	address := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, node) })
	md := &testMetadata{}
	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: address, Alive: true},
	}, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})
	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Put(context.Background(), []byte("before-empty"), []byte("value")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	c.mu.RLock()
	oldConn := c.nodeConns[address]
	c.mu.RUnlock()
	if oldConn == nil {
		t.Fatal("initial Put did not cache a node connection")
	}

	md.mu.Lock()
	md.nodes = nil
	md.shards = map[uint32]string{}
	md.nodeGen = 2
	md.shardGen = 2
	md.shardCount = 8
	md.mu.Unlock()
	if err := c.refresh(context.Background()); err != nil {
		t.Fatalf("refresh empty topology: %v", err)
	}
	if err := c.Put(context.Background(), []byte("after-empty"), []byte("value")); !errors.Is(err, ErrNoLiveNodes) {
		t.Fatalf("Put error = %v, want ErrNoLiveNodes", err)
	}
	if state := oldConn.GetState(); state != connectivity.Shutdown {
		t.Fatalf("retired connection state = %s, want SHUTDOWN", state)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.topology.Generation != 2 || c.topology.ShardCount != 8 ||
		len(c.topology.Nodes) != 0 || len(c.topology.ShardMap) != 0 {
		t.Fatalf("installed empty topology = %+v", c.topology)
	}
}

func TestRefreshAddressChangeRetiresOldConnection(t *testing.T) {
	nodeAtOldAddress, nodeAtNewAddress := newTestNode(), newTestNode()
	oldAddress := serveTestGRPC(t, func(s *grpc.Server) {
		nodev1.RegisterNodeServiceServer(s, nodeAtOldAddress)
	})
	newAddress := serveTestGRPC(t, func(s *grpc.Server) {
		nodev1.RegisterNodeServiceServer(s, nodeAtNewAddress)
	})
	md := &testMetadata{}
	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: oldAddress, Alive: true},
	}, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})
	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Put(context.Background(), []byte("before-address-change"), []byte("old")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	c.mu.RLock()
	oldConn := c.nodeConns[oldAddress]
	c.mu.RUnlock()
	if oldConn == nil {
		t.Fatal("initial operation did not cache the old connection")
	}

	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: newAddress, Alive: true},
	}, map[uint32]string{0: "node-a"}, 2)
	if err := c.refresh(context.Background()); err != nil {
		t.Fatalf("refresh address change: %v", err)
	}

	c.mu.RLock()
	_, oldConnCached := c.nodeConns[oldAddress]
	_, oldClientCached := c.nodeClients[oldAddress]
	c.mu.RUnlock()
	if oldConnCached || oldClientCached {
		t.Fatal("old address remained in the connection cache")
	}
	if state := oldConn.GetState(); state != connectivity.Shutdown {
		t.Fatalf("old connection state = %s, want SHUTDOWN", state)
	}

	key := []byte("after-address-change")
	if err := c.Put(context.Background(), key, []byte("new")); err != nil {
		t.Fatalf("Put after address change: %v", err)
	}
	if !nodeAtNewAddress.has(key) || nodeAtOldAddress.has(key) {
		t.Fatal("address refresh did not route to the new endpoint")
	}
}

func TestConnectionCreatedDuringRemovalIsNotCached(t *testing.T) {
	oldNode, newNode := newTestNode(), newTestNode()
	oldAddress := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, oldNode) })
	newAddress := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, newNode) })
	md := &testMetadata{}
	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: oldAddress, Alive: true},
	}, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})
	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	originalFactory := c.nodeConnFactory
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	c.nodeConnFactory = func(address string) (*grpc.ClientConn, error) {
		close(dialStarted)
		<-releaseDial
		return originalFactory(address)
	}
	dialResult := make(chan error, 1)
	go func() {
		_, err := c.clientForAddress(oldAddress)
		dialResult <- err
	}()
	<-dialStarted

	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-b", Address: newAddress, Alive: true},
	}, map[uint32]string{0: "node-b"}, 2)
	if err := c.refresh(context.Background()); err != nil {
		t.Fatalf("refresh removal: %v", err)
	}
	close(releaseDial)
	if err := <-dialResult; err == nil || !strings.Contains(err.Error(), "stopped owning shards") {
		t.Fatalf("racing dial error = %v, want stale-address error", err)
	}

	c.mu.RLock()
	_, connCached := c.nodeConns[oldAddress]
	_, clientCached := c.nodeClients[oldAddress]
	c.mu.RUnlock()
	if connCached || clientCached {
		t.Fatal("connection created during removal was cached after the refresh")
	}
}

func TestPrefixMatchScansOnlyShardOwners(t *testing.T) {
	owner, knownButUnowned := newTestNode(), newTestNode()
	ownerAddress := serveTestGRPC(t, func(s *grpc.Server) {
		nodev1.RegisterNodeServiceServer(s, owner)
	})
	unownedAddress := serveTestGRPC(t, func(s *grpc.Server) {
		nodev1.RegisterNodeServiceServer(s, knownButUnowned)
	})
	md := &testMetadata{}
	md.setTopologyGeneration([]*metadatav1.NodeInfo{
		{NodeId: "node-a", Address: ownerAddress, Alive: true},
		{NodeId: "node-b", Address: unownedAddress, Alive: false},
	}, map[uint32]string{0: "node-a"}, 1)
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})
	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	key := []byte("owner-only:one")
	if err := c.Put(context.Background(), key, []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	matches, err := c.PrefixMatch(context.Background(), []byte("owner-only:"))
	if err != nil {
		t.Fatalf("PrefixMatch: %v", err)
	}
	if got := matches[string(key)]; !bytes.Equal(got, []byte("value")) {
		t.Fatalf("match = %q, want value", got)
	}

	owner.mu.Lock()
	ownerScans := owner.prefixScans
	owner.mu.Unlock()
	knownButUnowned.mu.Lock()
	unownedScans := knownButUnowned.prefixScans
	knownButUnowned.mu.Unlock()
	if ownerScans != 1 || unownedScans != 0 {
		t.Fatalf("prefix scans = owner %d, unowned %d; want 1 and 0", ownerScans, unownedScans)
	}
}

func TestClientLargeValueUsesChunkedTransport(t *testing.T) {
	node := newTestNode()
	nodeAddr := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, node) })
	md := &testMetadata{
		nodes:  []*metadatav1.NodeInfo{{NodeId: "node-a", Address: nodeAddr, Alive: true}},
		shards: map[uint32]string{0: "node-a"},
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	value := make([]byte, int(nodev1.UnaryLimit_UNARY_VALUE_LIMIT_BYTES)+137)
	for i := range value {
		value[i] = byte(i*31 + 7)
	}
	key := []byte("large-value")
	if err := c.Put(context.Background(), key, value); err != nil {
		t.Fatalf("large Put: %v", err)
	}
	got, found, err := c.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("large Get: %v", err)
	}
	if !found || !bytes.Equal(got, value) {
		t.Fatalf("large Get = (%d bytes, %v), want (%d bytes, true)", len(got), found, len(value))
	}

	node.mu.Lock()
	chunkedPuts, chunkedGets := node.chunkedPuts, node.chunkedGets
	node.mu.Unlock()
	if chunkedPuts != 1 || chunkedGets != 1 {
		t.Fatalf("chunked RPC counts = Put %d, Get %d; want 1 each", chunkedPuts, chunkedGets)
	}
}

func TestConcurrentFirstDialReusesOneConnection(t *testing.T) {
	node := newTestNode()
	nodeAddr := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, node) })
	md := &testMetadata{
		nodes:  []*metadatav1.NodeInfo{{NodeId: "node-a", Address: nodeAddr, Alive: true}},
		shards: map[uint32]string{0: "node-a"},
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	const callers = 32
	start := make(chan struct{})
	clients := make(chan nodev1.NodeServiceClient, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, err := c.clientForAddress(nodeAddr)
			if err != nil {
				errs <- err
				return
			}
			clients <- client
		}()
	}
	close(start)
	wg.Wait()
	close(clients)
	close(errs)
	for err := range errs {
		t.Errorf("clientForAddress: %v", err)
	}

	var first nodev1.NodeServiceClient
	for client := range clients {
		if first == nil {
			first = client
			continue
		}
		if client != first {
			t.Error("concurrent first dial returned different cached clients")
		}
	}
	c.mu.RLock()
	connCount, clientCount := len(c.nodeConns), len(c.nodeClients)
	c.mu.RUnlock()
	if connCount != 1 || clientCount != 1 {
		t.Fatalf("connection cache has %d connections and %d clients, want 1 each", connCount, clientCount)
	}
}

func TestConcurrentCloseWaitsForSameShutdown(t *testing.T) {
	node := newTestNode()
	nodeAddr := serveTestGRPC(t, func(s *grpc.Server) { nodev1.RegisterNodeServiceServer(s, node) })
	md := &testMetadata{
		nodes:  []*metadatav1.NodeInfo{{NodeId: "node-a", Address: nodeAddr, Alive: true}},
		shards: map[uint32]string{0: "node-a"},
	}
	mdAddr := serveTestGRPC(t, func(s *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(s, md)
	})

	c, err := New(mdAddr, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Hold shutdown at the refresh-loop barrier so the behavior of a second
	// Close caller is observable without depending on gRPC close timing.
	refreshDone := make(chan struct{})
	refreshCancelled := make(chan struct{})
	c.mu.Lock()
	c.refreshDone = refreshDone
	c.refreshCancel = func() { close(refreshCancelled) }
	c.mu.Unlock()

	firstResult := make(chan error, 1)
	go func() { firstResult <- c.Close() }()
	<-refreshCancelled

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondResult <- c.Close()
	}()
	<-secondStarted
	var (
		secondErr     error
		returnedEarly bool
	)
	select {
	case secondErr = <-secondResult:
		returnedEarly = true
	case <-time.After(25 * time.Millisecond):
	}

	close(refreshDone)
	firstErr := <-firstResult
	if !returnedEarly {
		secondErr = <-secondResult
	}
	if returnedEarly {
		t.Errorf("second Close returned before shutdown completed: %v", secondErr)
	}
	if !errors.Is(firstErr, secondErr) || !errors.Is(secondErr, firstErr) {
		t.Fatalf("concurrent Close errors differ: first=%v second=%v", firstErr, secondErr)
	}
}

func TestValidateTopologyRejectsIncompleteMaps(t *testing.T) {
	nodes := []*metadatav1.NodeInfo{{NodeId: "node-a", Address: "127.0.0.1:1"}}
	cases := []struct {
		name   string
		shards map[uint32]string
		want   string
	}{
		{"empty", map[uint32]string{}, "zero shard count"},
		{"gap", map[uint32]string{0: "node-a", 2: "node-a"}, "missing shard 1"},
		{"unknown owner", map[uint32]string{0: "node-b"}, "unknown owner"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateTopology(nodes, tc.shards)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateTopology error = %v, want substring %q", err, tc.want)
			}
		})
	}

	t.Run("duplicate address", func(t *testing.T) {
		_, err := validateTopology([]*metadatav1.NodeInfo{
			{NodeId: "node-a", Address: "127.0.0.1:7100"},
			{NodeId: "node-b", Address: "127.0.0.1:7100"},
		}, map[uint32]string{0: "node-a", 1: "node-b"})
		if err == nil || !strings.Contains(err.Error(), "duplicate node address") {
			t.Fatalf("validateTopology error = %v, want duplicate node address", err)
		}
	})
}
