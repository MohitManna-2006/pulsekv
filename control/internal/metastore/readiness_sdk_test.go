package metastore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/metadata"
	sdk "pulsekv/control/pkg/client"
)

// Exit criterion 3: a caller holding the full replica list must see nothing at
// all while one replica is restarting. The readiness gate on its own only turns
// a wrong answer into an error; this is the test that the error is invisible
// from above, because the SDK moves to a replica that has actually caught up.
//
// The load runs against real data-node stubs so "zero client-visible errors"
// means zero, rather than "zero of the one error we thought to look for".

// memoryNode is the smallest NodeService that can serve the SDK's unary path.
type memoryNode struct {
	nodev1.UnimplementedNodeServiceServer
	mu     sync.RWMutex
	values map[string][]byte
}

func newMemoryNode() *memoryNode { return &memoryNode{values: make(map[string][]byte)} }

func (n *memoryNode) Put(_ context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.values[string(req.GetKey())] = append([]byte(nil), req.GetValue()...)
	return &nodev1.PutResponse{Ok: true}, nil
}

func (n *memoryNode) Get(_ context.Context, req *nodev1.GetRequest) (*nodev1.GetResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	value, found := n.values[string(req.GetKey())]
	return &nodev1.GetResponse{Value: append([]byte(nil), value...), Found: found}, nil
}

// listenOn serves on a specific address so a restarted replica reappears where
// the SDK's endpoint list already points, exactly as a restarted process does.
func listenOn(t *testing.T, address string, register func(*grpc.Server)) *grpc.Server {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen on %s: %v", address, err)
	}
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	return server
}

func serveMetadataOn(t *testing.T, store *Store, address string) *grpc.Server {
	t.Helper()
	svc, err := metadata.New(&config.Config{ShardCount: 64, ReplicationFactor: 0},
		store, metadata.WithLeaderInfo(store.Leader))
	if err != nil {
		t.Fatalf("build metadata service: %v", err)
	}
	return listenOn(t, address, func(server *grpc.Server) { svc.Register(server) })
}

// sdkFixture is a live three-replica metadata group on stable addresses, two
// real data nodes, and an SDK client pointed at all three replicas -- the
// Phase 5.6 multi-endpoint list a real caller uses.
type sdkFixture struct {
	group     *group
	addresses map[string]string // replica ID -> metadata address
	servers   map[string]*grpc.Server
	client    *sdk.Client
}

func newSDKFixture(t *testing.T) *sdkFixture {
	t.Helper()
	g := newGroup(t, 3)

	nodes := make([]membership.Node, 0, 2)
	for i := 0; i < 2; i++ {
		address := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		server := listenOn(t, address, func(server *grpc.Server) {
			nodev1.RegisterNodeServiceServer(server, newMemoryNode())
		})
		t.Cleanup(server.Stop)
		nodes = append(nodes, membership.Node{NodeID: fmt.Sprintf("node-%d", i), Address: address})
	}

	g.waitLeader("")
	leader := g.leaderStore()
	if leader == nil {
		t.Fatal("no leader to propose from")
	}
	if err := leader.Propose(State{Nodes: nodes, ReplicationFactor: 0}); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	generation := leader.State().Generation
	g.waitState(func(s State) bool { return s.Generation == generation }, "the committed membership")

	f := &sdkFixture{
		group:     g,
		addresses: make(map[string]string, len(g.peers)),
		servers:   make(map[string]*grpc.Server, len(g.peers)),
	}
	var endpoints []string
	for _, peer := range g.peers {
		address := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		f.addresses[peer.NodeID] = address
		f.servers[peer.NodeID] = serveMetadataOn(t, g.stores[peer.NodeID], address)
		endpoints = append(endpoints, address)
	}
	t.Cleanup(func() {
		for _, server := range f.servers {
			server.Stop()
		}
	})

	// A short refresh timeout as well as a short interval. Without it one
	// refresh can spend seconds waiting on endpoints whose process is gone,
	// which would let a test window elapse before the client had even looked at
	// the replica under test.
	client, err := sdk.New(strings.Join(endpoints, ","),
		sdk.WithRefreshInterval(20*time.Millisecond),
		sdk.WithRefreshTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatalf("open SDK client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	f.client = client
	return f
}

// loadRunner drives sustained routed traffic through the SDK and records every
// failure, not merely the one the fix is about.
type loadRunner struct {
	operations atomic.Int64
	failures   atomic.Int64
	noLive     atomic.Int64
	firstErr   chan error
	cancel     context.CancelFunc
	done       sync.WaitGroup
}

func (f *sdkFixture) startLoad(t *testing.T, workers int) *loadRunner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runner := &loadRunner{firstErr: make(chan error, 1), cancel: cancel}
	for worker := 0; worker < workers; worker++ {
		runner.done.Add(1)
		go func(worker int) {
			defer runner.done.Done()
			key := []byte(fmt.Sprintf("readiness-key-%d", worker))
			value := []byte(fmt.Sprintf("readiness-value-%d", worker))
			for ctx.Err() == nil {
				callCtx, cancelCall := context.WithTimeout(ctx, 2*time.Second)
				err := f.client.Put(callCtx, key, value)
				if err == nil {
					_, _, err = f.client.Get(callCtx, key)
				}
				cancelCall()
				if ctx.Err() != nil {
					return
				}
				runner.operations.Add(1)
				if err == nil {
					continue
				}
				runner.failures.Add(1)
				if errors.Is(err, sdk.ErrNoLiveNodes) {
					runner.noLive.Add(1)
				}
				select {
				case runner.firstErr <- fmt.Errorf("worker %d: %w", worker, err):
				default:
				}
			}
		}(worker)
	}
	t.Cleanup(runner.stop)
	return runner
}

func (r *loadRunner) stop() {
	r.cancel()
	r.done.Wait()
}

func (r *loadRunner) awaitOperations(t *testing.T, want int64) {
	t.Helper()
	deadline := time.Now().Add(testConverge)
	for r.operations.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d operation(s) completed, wanted %d", r.operations.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *loadRunner) assertClean(t *testing.T, when string) {
	t.Helper()
	if failed := r.failures.Load(); failed != 0 {
		var sample error
		select {
		case sample = <-r.firstErr:
		default:
		}
		t.Fatalf("%d of %d client operations failed %s (%d of them ErrNoLiveNodes); first: %v",
			failed, r.operations.Load(), when, r.noLive.Load(), sample)
	}
}

// The realistic shape: replicas restarting one at a time under live traffic,
// the group never losing quorum. A caller holding the full replica list must
// not notice.
func TestSDKSeesNothingWhileAReplicaRestarts(t *testing.T) {
	fixture := newSDKFixture(t)
	load := fixture.startLoad(t, 4)
	defer load.stop()

	for _, peer := range fixture.group.peers {
		time.Sleep(150 * time.Millisecond)
		fixture.servers[peer.NodeID].Stop()
		fixture.group.stop(peer.NodeID)
		time.Sleep(100 * time.Millisecond)
		fixture.group.start(peer.NodeID)
		fixture.servers[peer.NodeID] = serveMetadataOn(t, fixture.group.stores[peer.NodeID],
			fixture.addresses[peer.NodeID])

		// Let it catch up before disturbing the next one, so the group never
		// loses quorum: the failure under test is the readiness window, not an
		// unavailable cluster.
		deadline := time.Now().Add(testConverge)
		for fixture.group.stores[peer.NodeID].ServeReady() != nil {
			if time.Now().After(deadline) {
				t.Fatalf("%s never caught up after restart: %v",
					peer.NodeID, fixture.group.stores[peer.NodeID].ServeReady())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	load.awaitOperations(t, 100)
	load.assertClean(t, "while replicas restarted one at a time")
	t.Logf("%d client operations across three rolling replica restarts, 0 failures, "+
		"0 ErrNoLiveNodes", load.operations.Load())
}

// The deterministic half of criterion 3, and the one that discriminates against
// the pre-fix code.
//
// A rolling restart is a weak test on its own: the SDK prefers whichever
// replica last answered, so after failing over away from a replica it is
// stopped it will not go back until its new favourite fails. That stickiness is
// why this bug survived Phase 5 and why it surfaced through the chaos watcher,
// which pins one replica, rather than through the SDK.
//
// So force the case: take the whole control plane down and bring exactly one
// replica back. That replica cannot reach a quorum, cannot catch up, and is the
// only endpoint the SDK can reach. Pre-fix it answered "the cluster has no live
// data nodes", the SDK installed that, and every routed call failed with
// ErrNoLiveNodes. Post-fix it refuses, the SDK keeps the last complete topology
// it holds, and the data plane is untouched.
func TestSDKKeepsRoutingWhenTheOnlyReachableReplicaIsCatchingUp(t *testing.T) {
	fixture := newSDKFixture(t)

	load := fixture.startLoad(t, 4)
	defer load.stop()
	load.awaitOperations(t, 20)

	// Everything down; one replica back, alone.
	for _, peer := range fixture.group.peers {
		fixture.servers[peer.NodeID].Stop()
		fixture.group.stop(peer.NodeID)
	}
	lonely := fixture.group.peers[0].NodeID
	fixture.group.start(lonely)
	fixture.servers[lonely] = serveMetadataOn(t, fixture.group.stores[lonely],
		fixture.addresses[lonely])

	// It must genuinely be stuck, or the assertion below proves nothing.
	if err := fixture.group.stores[lonely].ServeReady(); err == nil {
		t.Fatal("the lone replica reported itself caught up without a quorum")
	}

	// Long enough for many refresh cycles to have reached this replica and
	// acted on its answer. Measured against the pre-fix code, the SDK installs
	// the empty topology and starts failing within about a second of the window
	// opening; the margin here is deliberate.
	before := load.operations.Load()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fixture.group.stores[lonely].ServeReady() == nil {
			t.Fatal("the lone replica became ready without a quorum")
		}
		load.assertClean(t, "while the only reachable control-plane replica was catching up")
		time.Sleep(20 * time.Millisecond)
	}
	if load.operations.Load()-before < 100 {
		t.Fatalf("only %d operation(s) ran during the window; the test proved little",
			load.operations.Load()-before)
	}
	// The client must still be routing on the topology it already had, not
	// merely failing quietly in a way the counters would not catch.
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 2*time.Second)
	if err := fixture.client.Put(probeCtx, []byte("readiness-probe"), []byte("v")); err != nil {
		cancelProbe()
		t.Fatalf("routed write failed while the lone replica was catching up: %v", err)
	}
	cancelProbe()
	t.Logf("%d client operations while the only reachable replica refused to publish a "+
		"topology, 0 failures", load.operations.Load()-before)

	// The quorum returning must restore normal service rather than leave the
	// client pinned to a topology nobody is confirming any more.
	for _, peer := range fixture.group.peers[1:] {
		fixture.group.start(peer.NodeID)
		fixture.servers[peer.NodeID] = serveMetadataOn(t, fixture.group.stores[peer.NodeID],
			fixture.addresses[peer.NodeID])
	}
	readyDeadline := time.Now().Add(testConverge)
	for fixture.group.stores[lonely].ServeReady() != nil {
		if time.Now().After(readyDeadline) {
			t.Fatalf("the replica never caught up once the quorum returned: %v",
				fixture.group.stores[lonely].ServeReady())
		}
		time.Sleep(5 * time.Millisecond)
	}
	load.awaitOperations(t, load.operations.Load()+20)
	load.assertClean(t, "after the quorum returned")
}
