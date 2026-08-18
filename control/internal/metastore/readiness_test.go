package metastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/metadata"
)

// These tests are the regression half of the restart-readiness fix. They
// reproduce, deterministically, what a Phase 6 chaos run hit once as
// "generation did not increase: 8 -> 0": a restarted replica answering
// GetNodeList with an authoritative-looking empty topology before it had caught
// up with the group.
//
// The reproduction runs a real metadata gRPC server over a real Raft store, in
// the same startup order control/cmd/controlplane/main.go uses -- metastore.New,
// then metadata.New over it, then net.Listen -- because the gap being closed
// lives precisely between the second and third of those.

func testNodes(n int) []membership.Node {
	out := make([]membership.Node, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, membership.Node{
			NodeID:  fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("127.0.0.1:71%02d", i),
		})
	}
	return out
}

// serveMetadata puts one replica behind a real ClusterMetadataService, exactly
// as the control-plane process does, and returns its address.
func serveMetadata(t *testing.T, store *Store) string {
	t.Helper()
	svc, err := metadata.New(&config.Config{ShardCount: 256, ReplicationFactor: 1},
		store, metadata.WithLeaderInfo(store.Leader))
	if err != nil {
		t.Fatalf("build metadata service: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	svc.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func dialMetadata(t *testing.T, address string) metadatav1.ClusterMetadataServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return metadatav1.NewClusterMetadataServiceClient(conn)
}

// answer is one observation of a single replica, kept in the shape the bug is
// stated in: an OK response carrying nothing is the failure, not an error.
type answer struct {
	at         time.Duration
	ok         bool
	generation uint64
	nodes      int
	code       codes.Code
}

func askOnce(client metadatav1.ClusterMetadataServiceClient, since time.Time) answer {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetNodeList(ctx, &metadatav1.GetNodeListRequest{})
	if err != nil {
		return answer{at: time.Since(since), code: status.Code(err)}
	}
	return answer{
		at:         time.Since(since),
		ok:         true,
		generation: resp.GetTopologyGeneration(),
		nodes:      len(resp.GetNodes()),
	}
}

// commitFourNodes brings a group to one committed, non-empty membership and
// returns its generation.
func commitFourNodes(t *testing.T, g *group) uint64 {
	t.Helper()
	g.waitLeader("")
	leader := g.leaderStore()
	if leader == nil {
		t.Fatal("no leader to propose from")
	}
	if err := leader.Propose(State{Nodes: testNodes(4), ReplicationFactor: 1}); err != nil {
		t.Fatalf("propose membership: %v", err)
	}
	generation := leader.State().Generation
	if generation == 0 {
		t.Fatal("proposal did not advance the generation")
	}
	g.waitState(func(s State) bool { return s.Generation == generation }, "the committed membership")
	return generation
}

// THE regression test, and deliberately free of any timing assumption: a
// replica restarted without a quorum can never catch up, so the window this fix
// closes is held open for as long as the test cares to look.
//
// Before the fix this replica answered every one of those calls with HTTP-200
// nothing: generation 0, zero live data nodes, a fingerprint over an empty
// cluster -- indistinguishable on the wire from a cluster that really had lost
// every data node, and enough to make a client install it and stop routing.
func TestRestartedReplicaRefusesToPublishAnUncaughtUpTopology(t *testing.T) {
	g := newGroup(t, 3)
	generation := commitFourNodes(t, g)

	// Everything goes down, then only the victim comes back. It holds a
	// complete local log and no way to confirm it, which is exactly the state
	// the guard exists for.
	victim := g.peers[0].NodeID
	for _, peer := range g.peers {
		g.stop(peer.NodeID)
	}
	g.start(victim)
	store := g.stores[victim]
	address := serveMetadata(t, store)
	client := dialMetadata(t, address)

	start := time.Now()
	var served, refused int
	for time.Since(start) < 2*time.Second {
		got := askOnce(client, start)
		if got.ok {
			served++
			t.Fatalf("a replica with no quorum published an authoritative topology after %s: "+
				"generation %d, %d live node(s) (committed generation is %d)",
				got.at.Round(time.Millisecond), got.generation, got.nodes, generation)
		}
		if got.code != codes.Unavailable {
			t.Fatalf("uncaught-up replica returned %s, want %s", got.code, codes.Unavailable)
		}
		refused++
		time.Sleep(5 * time.Millisecond)
	}
	if refused == 0 {
		t.Fatal("the poll loop made no observations")
	}
	t.Logf("refused %d consecutive direct reads over %s without ever publishing an empty topology",
		refused, time.Since(start).Round(time.Millisecond))

	// And it must not refuse forever: restoring the quorum has to let it
	// through, or the guard would have converted a restart into an outage.
	for _, peer := range g.peers[1:] {
		g.start(peer.NodeID)
	}
	deadline := time.Now().Add(testConverge)
	for {
		got := askOnce(client, start)
		if got.ok {
			if got.generation != generation || got.nodes != 4 {
				t.Fatalf("caught-up replica published generation %d with %d node(s), want %d and 4",
					got.generation, got.nodes, generation)
			}
			t.Logf("began serving generation %d with %d node(s) once the quorum returned",
				got.generation, got.nodes)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica never became ready after the quorum returned (last code %s)", got.code)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The same property against a live group, which is the shape the chaos run
// actually hit: kill the leader, restart it, and read only that replica from
// the instant its listener opens.
func TestRestartedLeaderNeverPublishesAnEmptyTopologyWhileCatchingUp(t *testing.T) {
	g := newGroup(t, 3)
	generation := commitFourNodes(t, g)

	leaderID, _ := g.waitLeader("")
	g.stop(leaderID)
	// Let the survivors elect someone, exactly as the chaos scenario does
	// before restarting the replica it killed.
	g.waitLeader(leaderID)

	start := time.Now()
	g.start(leaderID)
	address := serveMetadata(t, g.stores[leaderID])
	client := dialMetadata(t, address)

	var refused, served int
	var firstServed time.Duration = -1
	deadline := time.Now().Add(testConverge)
	for time.Now().Before(deadline) {
		got := askOnce(client, start)
		if !got.ok {
			if got.code != codes.Unavailable {
				t.Fatalf("restarted replica returned %s, want %s", got.code, codes.Unavailable)
			}
			refused++
			continue
		}
		// An OK answer must describe the cluster, never the zero value the FSM
		// holds before it has applied anything.
		if got.nodes == 0 || got.generation == 0 {
			t.Fatalf("restarted replica published an empty authoritative topology %s after start: "+
				"generation %d, %d live node(s) (committed generation is %d)",
				got.at.Round(time.Millisecond), got.generation, got.nodes, generation)
		}
		if served == 0 {
			firstServed = got.at
		}
		served++
		if served >= 20 {
			break
		}
	}
	if served == 0 {
		t.Fatal("the restarted replica never became ready")
	}
	t.Logf("refused %d read(s) while catching up, then served generation %d from %s after process start",
		refused, generation, firstServed.Round(time.Millisecond))
}

// ServeReady's own contract, stated directly rather than through a server.
func TestServeReadyLatchesAndDoesNotReopenOnLostContact(t *testing.T) {
	g := newGroup(t, 3)
	commitFourNodes(t, g)

	follower := ""
	for _, peer := range g.peers {
		if !g.stores[peer.NodeID].IsLeader() {
			follower = peer.NodeID
			break
		}
	}
	store := g.stores[follower]

	deadline := time.Now().Add(testConverge)
	for store.ServeReady() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("follower never became ready: %v", store.ServeReady())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Losing the rest of the group makes this replica's answer stale, which
	// Phase 5 documents as safe. It must NOT make it refuse: that would turn a
	// documented staleness bound into an outage.
	for _, peer := range g.peers {
		if peer.NodeID != follower {
			g.stop(peer.NodeID)
		}
	}
	// Well past an election timeout, so the replica has definitely noticed.
	time.Sleep(4 * testElectionTimeout)
	if err := store.ServeReady(); err != nil {
		t.Fatalf("a caught-up replica stopped serving after losing contact: %v", err)
	}
	if state := store.State(); len(state.Nodes) != 4 {
		t.Fatalf("stale but caught-up replica holds %d node(s), want 4", len(state.Nodes))
	}
}

// A replica that has heard from nobody must refuse, and must say why in terms
// an operator can act on.
func TestServeReadyRefusesBeforeAnyLeaderContact(t *testing.T) {
	g := newGroup(t, 3)
	commitFourNodes(t, g)

	victim := g.peers[0].NodeID
	for _, peer := range g.peers {
		g.stop(peer.NodeID)
	}
	g.start(victim)

	err := g.stores[victim].ServeReady()
	if err == nil {
		t.Fatal("a replica that has heard from no leader reported itself ready")
	}
	if !errors.Is(err, ErrCatchingUp) {
		t.Fatalf("ServeReady returned %v, want an ErrCatchingUp", err)
	}
}

// The FSM's own applied mark is what makes ServeReady's last condition
// trustworthy, so it has to survive the one path that replaces state wholesale.
// Without this, a replica that recovered from a snapshot would report having
// consumed nothing, and readiness would go hunting for entries the snapshot
// already covers -- entries a compacted log can no longer produce.
func TestSnapshotRoundTripPreservesTheAppliedMark(t *testing.T) {
	source := newFSM()
	source.Apply(&raft.Log{Index: 11, Type: raft.LogCommand, Data: mustEncode(t, State{
		Nodes:             testNodes(3),
		ReplicationFactor: 2,
	})})
	// A second entry with identical content: it advances what the FSM has
	// consumed without advancing the generation, and the mark must follow the
	// former rather than the latter.
	source.Apply(&raft.Log{Index: 14, Type: raft.LogCommand, Data: mustEncode(t, State{
		Nodes:             testNodes(3),
		ReplicationFactor: 2,
	})})
	if got := source.AppliedIndex(); got != 14 {
		t.Fatalf("applied mark = %d after consuming index 14, want 14", got)
	}
	if got := source.State().Generation; got != 11 {
		t.Fatalf("generation = %d, want 11: a re-proposed identical state is not a membership change", got)
	}

	snapshot, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	restored := newFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := restored.AppliedIndex(); got != 14 {
		t.Fatalf("restored applied mark = %d, want 14", got)
	}
	if got := restored.State().Generation; got != 11 {
		t.Fatalf("restored generation = %d, want 11", got)
	}
}

func mustEncode(t *testing.T, state State) []byte {
	t.Helper()
	raw, err := encodeCommand(state)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return raw
}
