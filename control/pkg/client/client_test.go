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
	return &metadatav1.GetNodeListResponse{Nodes: m.nodes}, nil
}

func (m *testMetadata) GetShardMap(context.Context, *metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	m.shardCalls.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.shardErr != nil {
		return nil, m.shardErr
	}
	return &metadatav1.GetShardMapResponse{ShardToNodeId: m.shards}, nil
}

func (m *testMetadata) setTopology(nodes []*metadatav1.NodeInfo, shards map[uint32]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = nodes
	m.shards = shards
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
}

func newTestNode() *testNode {
	return &testNode{values: make(map[string][]byte), vanishOnScan: make(map[string]bool)}
}

func (n *testNode) Put(_ context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.values[string(req.GetKey())] = append([]byte(nil), req.GetValue()...)
	n.puts++
	return &nodev1.PutResponse{Ok: true}, nil
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
		{"empty", map[uint32]string{}, "empty shard map"},
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
