// Command pulsekv-smoke is the live PulseKV v2 cluster verifier.
//
// It is the "small throwaway client using the generated stubs" that
// deploy/smoke-test.sh needs, kept inside the Go module so it compiles against
// the same checked-in stubs the control plane does -- a smoke test built from
// a different copy of the contract is not testing the contract.
//
// Two modes:
//
//	--mode=wait   poll every process's HealthCheck until all report ok, or fail
//	              loudly naming exactly which ones did not come up. Used by
//	              deploy/run-local-cluster.sh.
//	--mode=smoke  assert the live contract and Phase 3 routing: exact HRW
//	              metadata, SDK round-trips, direct physical placement, and
//	              every data-plane RPC. Used by deploy/smoke-test.sh.
//	--mode=topology-wait
//	              wait for an exact live membership set and coherent shard map.
//	              Used by the Phase 3 node lifecycle and chaos scripts.
//	--mode=leader print the Raft leader ID and term, so a script knows which
//	              control-plane process to act on.
//	--mode=leader-wait
//	              wait until every reachable replica agrees on one leader and
//	              term, optionally requiring it to have changed.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	adapterv1 "pulsekv/control/gen/adapter/v1"
	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
	pulsekvclient "pulsekv/control/pkg/client"
)

const (
	// Taken from the contract rather than hardcoded here, so this file cannot
	// drift from what the node enforces.
	unaryValueLimit = int(nodev1.UnaryLimit_UNARY_VALUE_LIMIT_BYTES)

	// Matched to the node's own ceiling; the chunked round-trip below moves
	// 6 MiB and must not be cut off by the client's default 4 MiB limit.
	maxMessageBytes = 8 * 1024 * 1024

	// Enough keys to exercise more than one route in the four-node dev
	// cluster without making the smoke leg materially slower.
	routingSampleCount = 6
)

func main() {
	var (
		configPath = flag.String("config", "deploy/cluster.config.yaml",
			"path to the static cluster config")
		mode = flag.String("mode", "smoke",
			"`wait`, `topology-wait`, or `smoke` (assert the live contract and routing)")
		timeout = flag.Duration("timeout", 15*time.Second,
			"overall budget for a wait mode")
		rpcTimeout = flag.Duration("rpc-timeout", 2*time.Second,
			"per-RPC deadline")
		pollInterval = flag.Duration("poll-interval", 250*time.Millisecond,
			"delay between polls in --mode=wait")
		expectLive = flag.String("expect-live", "",
			"comma-separated exact live node IDs for --mode=topology-wait (default: all configured nodes)")
		expectPresent = flag.String("expect-present", "",
			"comma-separated node IDs that must be present in --mode=topology-wait (does not constrain other nodes)")
		expectAbsent = flag.String("expect-absent", "",
			"comma-separated node IDs that must be absent in --mode=topology-wait")
		targetNode = flag.String("node", "",
			"for --mode=wait: limit data-node health checks to this specific node ID (empty = all configured nodes)")
		minGeneration = flag.Uint64("min-generation", 0,
			"minimum topology generation for --mode=topology-wait")
		leaderChangedFrom = flag.String("expect-leader-change-from", "",
			"for --mode=leader-wait: require the converged leader to differ from this replica ID")
		minReplicas = flag.Int("min-replicas", 1,
			"for --mode=leader-wait: how many replicas must answer and agree")
		minControlPlane = flag.Int("min-control-plane", 0,
			"for --mode=wait: how many control-plane replicas must be healthy (0 = all). "+
				"A Raft group tolerates losing a minority, so a lifecycle check that "+
				"demanded every replica would be stricter than the system it manages")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: %v\n", err)
		os.Exit(2)
	}

	switch *mode {
	case "wait":
		os.Exit(runWait(cfg, *targetNode, *minControlPlane, *timeout, *rpcTimeout, *pollInterval))
	case "smoke":
		os.Exit(runSmoke(cfg, *rpcTimeout))
	case "leader":
		os.Exit(runLeader(cfg, *rpcTimeout))
	case "leader-wait":
		os.Exit(runLeaderWait(cfg, *leaderChangedFrom, *minReplicas,
			*timeout, *rpcTimeout, *pollInterval))
	case "topology-wait":
		var live []string
		if *expectLive != "" || *expectPresent == "" {
			live, err = expectedNodeIDs(cfg, *expectLive)
			if err != nil {
				fmt.Fprintf(os.Stderr, "pulsekv-smoke: %v\n", err)
				os.Exit(2)
			}
		}
		present, err := parseNodeIDs(*expectPresent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pulsekv-smoke: --expect-present: %v\n", err)
			os.Exit(2)
		}
		absent, err := parseNodeIDs(*expectAbsent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pulsekv-smoke: --expect-absent: %v\n", err)
			os.Exit(2)
		}
		os.Exit(runTopologyWait(cfg, live, present, absent, *minGeneration,
			*timeout, *rpcTimeout, *pollInterval))
	default:
		fmt.Fprintf(os.Stderr,
			"pulsekv-smoke: unknown --mode %q (want wait, smoke, topology-wait, leader, or leader-wait)\n", *mode)
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// wait mode
// ---------------------------------------------------------------------------

// process is one thing we expect to be listening.
type process struct {
	label   string // "controlplane" or a node ID
	address string
	isNode  bool
}

func processes(cfg *config.Config, targetNode string) []process {
	ps := make([]process, 0, len(cfg.ControlPlanes)+len(cfg.Nodes))
	for _, replica := range cfg.ControlPlanes {
		ps = append(ps, process{label: "controlplane:" + replica.NodeID, address: replica.Address()})
	}
	for _, n := range cfg.Nodes {
		if targetNode == "" || n.NodeID == targetNode {
			ps = append(ps, process{label: n.NodeID, address: n.Address(), isNode: true})
		}
	}
	return ps
}

// runWait polls until every data node and the required number of control-plane
// replicas report healthy.
//
// minControlPlane exists because Phase 5 made the control plane a group. Boot
// asks for all of them -- a cluster that silently comes up one replica short is
// worth catching. A targeted node restart asks only for a quorum, because it
// legitimately runs while a replica is down, and demanding all of them would
// make the lifecycle scripts refuse to work during exactly the failover the
// chaos harness creates.
func runWait(cfg *config.Config, targetNode string, minControlPlane int, budget, rpcTimeout, interval time.Duration) int {
	ps := processes(cfg, targetNode)
	replicas := len(cfg.ControlPlanes)
	required := minControlPlane
	if required <= 0 || required > replicas {
		required = replicas
	}
	deadline := time.Now().Add(budget)

	expectedDataNodes := len(cfg.Nodes)
	if targetNode != "" {
		expectedDataNodes = 1
	}

	if required < replicas {
		fmt.Printf("waiting up to %s for %d data service(s) and %d of %d control-plane replica(s)...\n",
			budget, expectedDataNodes, required, replicas)
	} else {
		fmt.Printf("waiting up to %s for %d service(s) to report healthy...\n", budget, len(ps))
	}

	var lastErrs map[string]error
	var healthyReplicas int
	for attempt := 1; ; attempt++ {
		lastErrs = probeAll(ps, rpcTimeout)

		healthyReplicas = replicas
		dataFailures := 0
		for _, p := range ps {
			if _, bad := lastErrs[p.label]; !bad {
				continue
			}
			if p.isNode {
				dataFailures++
			} else {
				healthyReplicas--
			}
		}
		if dataFailures == 0 && healthyReplicas >= required {
			fmt.Printf("all %d data service(s) and %d of %d control-plane replica(s) healthy after %d poll(s)\n",
				expectedDataNodes, healthyReplicas, replicas, attempt)
			return 0
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(interval)
	}

	// Fail loudly and specifically. "cluster did not come up" is useless;
	// "node-2 on 127.0.0.1:7102 refused the connection" is actionable.
	fmt.Fprintf(os.Stderr,
		"\nFAILED: %d process(es) unhealthy within %s (%d of %d control-plane replica(s) up, want %d)\n",
		len(lastErrs), budget, healthyReplicas, replicas, required)
	for _, p := range ps {
		if err, bad := lastErrs[p.label]; bad {
			fmt.Fprintf(os.Stderr, "  %-18s %-22s %v\n", p.label, p.address, compact(err))
		}
	}
	return 1
}

// ---------------------------------------------------------------------------
// topology-wait mode
// ---------------------------------------------------------------------------

func runTopologyWait(cfg *config.Config, expected, present, absent []string, minGeneration uint64,
	budget, rpcTimeout, interval time.Duration) int {

	deadline := time.Now().Add(budget)
	var lastErr error
	for attempt := 1; ; attempt++ {
		lastErr = probeTopology(cfg.ControlPlaneAddresses(), cfg.ShardCount,
			expected, present, absent, minGeneration, rpcTimeout)
		if lastErr == nil {
			targetSummary := strings.Join(expected, ",")
			if len(present) > 0 {
				targetSummary = fmt.Sprintf("present=[%s]", strings.Join(present, ","))
			}
			fmt.Printf("topology converged after %d poll(s): generation >= %d, live=%s\n",
				attempt, minGeneration, targetSummary)
			return 0
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "FAILED: topology did not converge within %s: %v\n", budget, lastErr)
			return 1
		}
		time.Sleep(interval)
	}
}

// probeTopology checks one coherent topology, read from whichever
// control-plane replica answers first.
//
// Any replica is an acceptable source: they serve from their own applied Raft
// log, which is a prefix of the leader's committed one. A follower can be a
// heartbeat behind, so this poll simply retries until whichever replica it
// reached has caught up -- it never sees a contradictory answer, only an
// earlier one.
func probeTopology(controlPlanes []string, shardCount uint32, expected, present, absent []string, minGeneration uint64,
	rpcTimeout time.Duration) error {

	snapshot, _, err := fetchFromAnyReplica(controlPlanes, rpcTimeout)
	if err != nil {
		return err
	}
	if snapshot.Generation < minGeneration {
		return fmt.Errorf("generation=%d, want at least %d", snapshot.Generation, minGeneration)
	}
	if snapshot.ShardCount != shardCount {
		return fmt.Errorf("shard count=%d, want configured %d", snapshot.ShardCount, shardCount)
	}

	if len(present) > 0 {
		for _, id := range present {
			if _, ok := snapshot.Nodes[id]; !ok {
				return fmt.Errorf("required node %q is absent in live topology (live=%v)",
					id, sortedNodeIDs(snapshot.Nodes))
			}
		}
		for _, id := range absent {
			if _, ok := snapshot.Nodes[id]; ok {
				return fmt.Errorf("node %q is still present (live=%v)", id, sortedNodeIDs(snapshot.Nodes))
			}
		}
		return nil
	}

	want := make(map[string]bool, len(expected))
	for _, id := range expected {
		want[id] = true
	}
	if len(snapshot.Nodes) != len(want) {
		return fmt.Errorf("live nodes=%v, want exactly %v", sortedNodeIDs(snapshot.Nodes), expected)
	}
	for id := range snapshot.Nodes {
		if !want[id] {
			return fmt.Errorf("unexpected live node %q (live=%v, want=%v)",
				id, sortedNodeIDs(snapshot.Nodes), expected)
		}
	}
	for _, id := range expected {
		if _, ok := snapshot.Nodes[id]; !ok {
			return fmt.Errorf("expected node %q is absent (live=%v)", id, sortedNodeIDs(snapshot.Nodes))
		}
	}
	for _, id := range absent {
		if _, ok := snapshot.Nodes[id]; ok {
			return fmt.Errorf("node %q is still present (live=%v)", id, sortedNodeIDs(snapshot.Nodes))
		}
	}

	wantMap := router.AssignShards(expected, shardCount)
	if differences := shardMapDifferences(snapshot.ShardMap, wantMap, 5); len(differences) > 0 {
		return fmt.Errorf("generation %d is not the exact HRW map for live nodes: %s",
			snapshot.Generation, strings.Join(differences, "; "))
	}
	return nil
}

// ---------------------------------------------------------------------------
// control-plane replica helpers
// ---------------------------------------------------------------------------

// dialAnyReplica returns a connection to the first replica that answers
// HealthCheck. The caller owns the connection.
func dialAnyReplica(addresses []string, rpcTimeout time.Duration) (string, *grpc.ClientConn, error) {
	var firstErr error
	for _, address := range addresses {
		conn, err := dial(address)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", address, err)
			}
			continue
		}
		// GetNodeList rather than HealthCheck: this connection is about to be
		// used for topology reads, and HealthCheck answers for a replica that
		// is alive but has not caught up since restarting -- which now declines
		// to publish a topology. Selecting on the call we actually need means
		// this picks a replica that can serve it.
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, err = metadatav1.NewClusterMetadataServiceClient(conn).GetNodeList(
			ctx, &metadatav1.GetNodeListRequest{})
		cancel()
		if err == nil {
			return address, conn, nil
		}
		conn.Close()
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", address, err)
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no control-plane replicas configured")
	}
	return "", nil, firstErr
}

// fetchFromAnyReplica reads one coherent topology from the first replica that
// answers, and reports which one that was.
func fetchFromAnyReplica(addresses []string, rpcTimeout time.Duration) (clustertopology.Snapshot, string, error) {
	var firstErr error
	for _, address := range addresses {
		conn, err := dial(address)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", address, err)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		snapshot, err := clustertopology.Fetch(ctx, metadatav1.NewClusterMetadataServiceClient(conn))
		cancel()
		conn.Close()
		if err == nil {
			return snapshot, address, nil
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", address, err)
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no control-plane replicas configured")
	}
	return clustertopology.Snapshot{}, "", firstErr
}

// ---------------------------------------------------------------------------
// leader modes
// ---------------------------------------------------------------------------

// runLeader prints `leader_id<TAB>term` from the first replica that answers.
//
// deploy/chaos-test.sh uses it to discover which control-plane process to kill.
// The harness owns every assertion about leadership; this only answers "who".
func runLeader(cfg *config.Config, rpcTimeout time.Duration) int {
	snapshot, address, err := fetchFromAnyReplica(cfg.ControlPlaneAddresses(), rpcTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: no control-plane replica answered: %v\n", err)
		return 1
	}
	if snapshot.RaftLeaderID == "" {
		fmt.Fprintf(os.Stderr,
			"pulsekv-smoke: %s reports no Raft leader (term %d); the group may be electing\n",
			address, snapshot.RaftTerm)
		return 1
	}
	fmt.Printf("%s\t%d\n", snapshot.RaftLeaderID, snapshot.RaftTerm)
	return 0
}

// runLeaderWait blocks until every reachable replica agrees on one leader, and
// on a leader different from --expect-leader-change-from when that is given.
//
// Convergence is the real property: "some replica named a leader" would pass
// while two replicas still disagreed, which is exactly the state Phase 5 exists
// to make impossible.
func runLeaderWait(cfg *config.Config, changedFrom string, minReplicas int,
	budget, rpcTimeout, interval time.Duration) int {

	deadline := time.Now().Add(budget)
	var lastErr error
	for attempt := 1; ; attempt++ {
		leader, term, reached, err := observeLeader(cfg.ControlPlaneAddresses(), rpcTimeout)
		switch {
		case err != nil:
			lastErr = err
		case reached < minReplicas:
			lastErr = fmt.Errorf("only %d of %d replica(s) answered, want at least %d",
				reached, len(cfg.ControlPlanes), minReplicas)
		case leader == "":
			lastErr = errors.New("reachable replicas do not agree on a leader yet")
		case changedFrom != "" && leader == changedFrom:
			lastErr = fmt.Errorf("leader is still %s at term %d", leader, term)
		default:
			fmt.Printf("leader converged after %d poll(s): %s at term %d (%d replica(s) agree)\n",
				attempt, leader, term, reached)
			return 0
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "FAILED: leadership did not converge within %s: %v\n", budget, lastErr)
			return 1
		}
		time.Sleep(interval)
	}
}

// observeLeader returns the leader every reachable replica agrees on, or an
// empty ID when they disagree or any of them names no leader at all.
func observeLeader(addresses []string, rpcTimeout time.Duration) (string, uint64, int, error) {
	var (
		leader   string
		term     uint64
		reached  int
		firstErr error
	)
	for _, address := range addresses {
		conn, err := dial(address)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := metadatav1.NewClusterMetadataServiceClient(conn).GetShardMap(
			ctx, &metadatav1.GetShardMapRequest{})
		cancel()
		conn.Close()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reached++
		if resp.GetRaftLeaderId() == "" {
			return "", 0, reached, nil // this replica sees no leader yet
		}
		if leader == "" {
			leader, term = resp.GetRaftLeaderId(), resp.GetRaftTerm()
			continue
		}
		if resp.GetRaftLeaderId() != leader || resp.GetRaftTerm() != term {
			return "", 0, reached, nil // replicas disagree; not converged
		}
	}
	if reached == 0 {
		if firstErr == nil {
			firstErr = errors.New("no control-plane replicas configured")
		}
		return "", 0, 0, firstErr
	}
	return leader, term, reached, nil
}

func sortedNodeIDs(nodes map[string]string) []string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func expectedNodeIDs(cfg *config.Config, text string) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return cfg.NodeIDs(), nil
	}
	ids, err := parseNodeIDs(text)
	if err != nil {
		return nil, fmt.Errorf("--expect-live: %w", err)
	}
	return ids, nil
}

func parseNodeIDs(text string) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	seen := make(map[string]bool)
	var ids []string
	for _, raw := range strings.Split(text, ",") {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, errors.New("contains an empty node ID")
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate node ID %q", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// probeAll health-checks every process once, returning only the failures.
//
// Connections are built fresh per round on purpose: gRPC's reconnect backoff
// grows past this poller's entire budget after a handful of refused
// connections, so a persistent client would stop actually retrying well before
// the timeout expires.
//
// Probes run concurrently so one round costs one rpcTimeout rather than N of
// them. That matters at the 8-32 node scale the design doc targets for Phase 3
// chaos testing: serially, a cluster where several nodes are unreachable
// (rather than actively refusing) would blow the entire wait budget inside the
// first round and never report what was wrong.
func probeAll(ps []process, rpcTimeout time.Duration) map[string]error {
	var (
		mu       sync.Mutex
		failures = make(map[string]error)
		wg       sync.WaitGroup
	)
	for _, p := range ps {
		wg.Add(1)
		go func(p process) {
			defer wg.Done()
			err := probeOne(p, rpcTimeout)
			if err == nil {
				return
			}
			mu.Lock()
			failures[p.label] = err
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return failures
}

func probeOne(p process, rpcTimeout time.Duration) error {
	conn, err := dial(p.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	if p.isNode {
		resp, err := nodev1.NewNodeServiceClient(conn).HealthCheck(ctx, &nodev1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if !resp.GetOk() {
			return fmt.Errorf("HealthCheck returned ok=false")
		}
		return nil
	}

	resp, err := metadatav1.NewClusterMetadataServiceClient(conn).HealthCheck(ctx, &metadatav1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if !resp.GetOk() {
		return fmt.Errorf("HealthCheck returned ok=false")
	}
	return nil
}

// ---------------------------------------------------------------------------
// smoke mode
// ---------------------------------------------------------------------------

func runSmoke(cfg *config.Config, rpcTimeout time.Duration) int {
	r := &reporter{}

	cpAddress, cpConn, err := dialAnyReplica(cfg.ControlPlaneAddresses(), rpcTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: cannot reach any control-plane replica (%s): %v\n",
			strings.Join(cfg.ControlPlaneAddresses(), ", "), err)
		return 1
	}
	_ = cpAddress
	defer cpConn.Close()

	checkControlPlane(r, cfg, cpConn, rpcTimeout)
	checkRouting(r, cfg, cpConn, rpcTimeout)
	checkReplication(r, cfg, cpConn, rpcTimeout)

	for _, n := range cfg.Nodes {
		conn, err := dial(n.Address())
		if err != nil {
			r.fail(n.NodeID+"/dial", err)
			continue
		}
		checkNode(r, n, cfg, conn, rpcTimeout)
		conn.Close()
	}

	return r.report()
}

func checkControlPlane(r *reporter, cfg *config.Config, conn *grpc.ClientConn, rpcTimeout time.Duration) {
	md := metadatav1.NewClusterMetadataServiceClient(conn)
	var nodeGeneration uint64
	var nodeFingerprint []byte

	// --- HealthCheck must be real, not a stub.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := md.HealthCheck(ctx, &metadatav1.HealthCheckRequest{})
		switch {
		case err != nil:
			r.fail("controlplane/HealthCheck", err)
		case !resp.GetOk():
			r.fail("controlplane/HealthCheck", errors.New("ok=false"))
		case resp.GetUptimeSeconds() < 0:
			r.fail("controlplane/HealthCheck",
				fmt.Errorf("negative uptime_seconds=%d", resp.GetUptimeSeconds()))
		default:
			r.pass("controlplane/HealthCheck",
				fmt.Sprintf("ok=true uptime=%ds", resp.GetUptimeSeconds()))
		}
	}()

	// --- GetNodeList must match the config exactly, and report real liveness.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := md.GetNodeList(ctx, &metadatav1.GetNodeListRequest{})
		if err != nil {
			r.fail("controlplane/GetNodeList", err)
			return
		}
		nodeGeneration = resp.GetTopologyGeneration()
		nodeFingerprint = bytes.Clone(resp.GetTopologyFingerprint())

		want := make(map[string]string, len(cfg.Nodes)) // node ID -> address
		for _, n := range cfg.Nodes {
			want[n.NodeID] = n.Address()
		}

		var problems []string
		if nodeGeneration == 0 {
			problems = append(problems, "topology_generation=0; Phase 3 requires a published generation")
		}
		if len(nodeFingerprint) != clustertopology.FingerprintSize {
			problems = append(problems, fmt.Sprintf("topology_fingerprint has %d bytes, want %d",
				len(nodeFingerprint), clustertopology.FingerprintSize))
		}
		var dead []string
		seen := make(map[string]bool, len(resp.GetNodes()))

		for _, got := range resp.GetNodes() {
			wantAddr, known := want[got.GetNodeId()]
			switch {
			case !known:
				problems = append(problems, fmt.Sprintf("unexpected node %q", got.GetNodeId()))
				continue
			case seen[got.GetNodeId()]:
				problems = append(problems, fmt.Sprintf("duplicate node %q", got.GetNodeId()))
				continue
			case got.GetAddress() != wantAddr:
				problems = append(problems, fmt.Sprintf("%s address %q, want %q",
					got.GetNodeId(), got.GetAddress(), wantAddr))
			}
			seen[got.GetNodeId()] = true
			if !got.GetAlive() {
				dead = append(dead, got.GetNodeId())
			}
		}
		for id := range want {
			if !seen[id] {
				problems = append(problems, fmt.Sprintf("missing node %q", id))
			}
		}
		// All configured nodes are running at smoke time, so a not-alive
		// node means the probe path itself is broken.
		if len(dead) > 0 {
			sort.Strings(dead)
			problems = append(problems, "not alive: "+strings.Join(dead, ", "))
		}

		if len(problems) > 0 {
			sort.Strings(problems)
			r.fail("controlplane/GetNodeList", errors.New(strings.Join(problems, "; ")))
			return
		}
		r.pass("controlplane/GetNodeList",
			fmt.Sprintf("generation=%d; %d node(s), all alive, addresses match config",
				nodeGeneration, len(resp.GetNodes())))
	}()

	// --- GetShardMap must cover every shard with a configured owner and match
	// a fresh, independent computation of the routing algorithm exactly.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := md.GetShardMap(ctx, &metadatav1.GetShardMapRequest{})
		if err != nil {
			r.fail("controlplane/GetShardMap", err)
			return
		}

		known := make(map[string]bool, len(cfg.Nodes))
		for _, n := range cfg.Nodes {
			known[n.NodeID] = true
		}

		got := resp.GetShardToNodeId()
		want := router.AssignShards(cfg.NodeIDs(), cfg.ShardCount)
		var problems []string
		if resp.GetTopologyGeneration() != nodeGeneration {
			problems = append(problems, fmt.Sprintf(
				"topology generation=%d, preceding node-list generation=%d",
				resp.GetTopologyGeneration(), nodeGeneration))
		}
		if !bytes.Equal(resp.GetTopologyFingerprint(), nodeFingerprint) {
			problems = append(problems, "topology fingerprint differs from preceding node list")
		}
		if resp.GetShardCount() != cfg.ShardCount {
			problems = append(problems, fmt.Sprintf("reported shard_count=%d, want %d",
				resp.GetShardCount(), cfg.ShardCount))
		}
		if uint32(len(got)) != cfg.ShardCount {
			problems = append(problems, fmt.Sprintf("%d entries, want shard_count=%d",
				len(got), cfg.ShardCount))
		}
		owners := make(map[string]int, len(cfg.Nodes))
		for shard, owner := range got {
			if shard >= cfg.ShardCount {
				problems = append(problems, fmt.Sprintf("shard %d out of range", shard))
			}
			if !known[owner] {
				problems = append(problems, fmt.Sprintf("shard %d owned by unknown node %q", shard, owner))
			}
			owners[owner]++
		}
		// Round-robin over N nodes with shard_count >= N must give every
		// node work. A shard map that strands a node is a routing bug.
		for _, n := range cfg.Nodes {
			if cfg.ShardCount >= uint32(len(cfg.Nodes)) && owners[n.NodeID] == 0 {
				problems = append(problems, fmt.Sprintf("node %q owns no shards", n.NodeID))
			}
		}
		problems = append(problems, shardMapDifferences(got, want, 5)...)

		if len(problems) > 0 {
			sort.Strings(problems)
			// Long lists of identical complaints help nobody.
			if len(problems) > 5 {
				problems = append(problems[:5], fmt.Sprintf("(+%d more)", len(problems)-5))
			}
			r.fail("controlplane/GetShardMap", errors.New(strings.Join(problems, "; ")))
			return
		}
		r.pass("controlplane/GetShardMap",
			fmt.Sprintf("generation=%d; %d shard(s) over %d node(s), exact router.AssignShards match",
				resp.GetTopologyGeneration(), len(got), len(owners)))
	}()

	// --- AdapterService is generated and dial-able but has no server in
	// Phase 0. Assert that honestly rather than assuming it.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		_, err := adapterv1.NewAdapterServiceClient(conn).HealthCheck(ctx, &adapterv1.HealthCheckRequest{})
		r.wantUnimplemented("controlplane/AdapterService.HealthCheck", err)
	}()
}

// checkRouting closes the gap between "the SDK returned my value" and "the
// SDK put it on the node the live shard map predicts". It fetches topology
// independently, writes through the public SDK, then bypasses the SDK for two
// direct physical-placement reads per key: a hit on the predicted owner and a
// miss on a different node.
func checkRouting(r *reporter, cfg *config.Config, conn *grpc.ClientConn, rpcTimeout time.Duration) {
	md := metadatav1.NewClusterMetadataServiceClient(conn)
	topology, err := fetchLiveRoutingTopology(md, rpcTimeout)
	if err != nil {
		r.fail("routing/live metadata", err)
		return
	}

	wantShardMap := router.AssignShards(cfg.NodeIDs(), cfg.ShardCount)
	if differences := shardMapDifferences(topology.shardMap, wantShardMap, 5); len(differences) > 0 {
		r.fail("routing/live metadata", fmt.Errorf(
			"live GetShardMap does not exactly match router.AssignShards: %s",
			strings.Join(differences, "; ")))
		return
	}
	if len(topology.nodes) < 2 {
		r.fail("routing/live metadata", fmt.Errorf(
			"need at least two live nodes to prove a predicted-owner hit and a different-node miss; got %d",
			len(topology.nodes)))
		return
	}
	r.pass("routing/live metadata", fmt.Sprintf(
		"generation=%d; fetched %d node(s) and %d shard(s); exact router.AssignShards match",
		topology.generation, len(topology.nodes), len(topology.shardMap)))

	routed, err := pulsekvclient.New(
		cfg.ControlPlaneEndpoints(),
		pulsekvclient.WithRefreshInterval(0),
		pulsekvclient.WithRefreshTimeout(rpcTimeout),
	)
	if err != nil {
		r.fail("routing/client.New", err)
		return
	}
	defer routed.Close()

	prefix := fmt.Sprintf("smoke:routing:%d:%d", os.Getpid(), time.Now().UnixNano())
	keys, err := routingSampleKeys(prefix, routingSampleCount, cfg.ShardCount, topology.shardMap)
	if err != nil {
		r.fail("routing/sample keys", err)
		return
	}

	nodeIDs := make([]string, 0, len(topology.nodes))
	for id := range topology.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	// These clients intentionally do not go through pkg/client. They are the
	// independent observation path that proves where the SDK physically wrote.
	directConns := make(map[string]*grpc.ClientConn, len(topology.nodes))
	directClients := make(map[string]nodev1.NodeServiceClient, len(topology.nodes))
	defer func() {
		for _, directConn := range directConns {
			_ = directConn.Close()
		}
	}()
	for id, address := range topology.nodes {
		directConn, err := dial(address)
		if err != nil {
			r.fail("routing/direct dial", fmt.Errorf("%s at %s: %w", id, address, err))
			return
		}
		directConns[id] = directConn
		directClients[id] = nodev1.NewNodeServiceClient(directConn)
	}

	for i, key := range keys {
		owner, ok := router.OwnerForKey(key, cfg.ShardCount, topology.shardMap)
		if !ok {
			r.fail(fmt.Sprintf("routing/key[%d]", i), fmt.Errorf(
				"live shard map has no owner for shard %d", router.ShardForKey(key, cfg.ShardCount)))
			continue
		}
		shard := router.ShardForKey(key, cfg.ShardCount)
		other := nonHolder(shard, topology, nodeIDs)
		if other == "" {
			// Every live node holds a copy, so no node can prove the write did
			// not go everywhere. Legal at a replication factor of len(nodes)-1;
			// say so rather than failing, or passing silently.
			r.pass(fmt.Sprintf("routing/key[%d]", i), fmt.Sprintf(
				"shard %d owner=%s; every live node holds this shard, so no "+
					"exclusion node exists", shard, owner))
			continue
		}

		value := deterministicValue(256+i*37, uint64(101+i))
		var problems []string

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		putErr := routed.Put(ctx, key, value)
		cancel()
		if putErr != nil {
			problems = append(problems, fmt.Sprintf("SDK Put: %v", putErr))
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
			got, found, getErr := routed.Get(ctx, key)
			cancel()
			switch {
			case getErr != nil:
				problems = append(problems, fmt.Sprintf("SDK Get: %v", getErr))
			case !found:
				problems = append(problems, "SDK Get returned found=false after Put")
			case !bytes.Equal(got, value):
				problems = append(problems, fmt.Sprintf(
					"SDK Get returned %d bytes with different contents", len(got)))
			}

			ownerResp, directErr := directGet(directClients[owner], key, rpcTimeout)
			switch {
			case directErr != nil:
				problems = append(problems, fmt.Sprintf("direct Get on predicted owner %s: %v", owner, directErr))
			case !ownerResp.GetFound():
				problems = append(problems, fmt.Sprintf("predicted owner %s returned found=false", owner))
			case !bytes.Equal(ownerResp.GetValue(), value):
				problems = append(problems, fmt.Sprintf(
					"predicted owner %s returned %d bytes with different contents",
					owner, len(ownerResp.GetValue())))
			}

			otherResp, directErr := directGet(directClients[other], key, rpcTimeout)
			switch {
			case directErr != nil:
				problems = append(problems, fmt.Sprintf("direct Get on non-holder %s: %v", other, directErr))
			case otherResp.GetFound():
				problems = append(problems, fmt.Sprintf(
					"%s holds no copy of shard %d but returned found=true", other, shard))
			}
		}

		name := fmt.Sprintf("routing/key[%d]", i)
		if len(problems) > 0 {
			r.fail(name, errors.New(strings.Join(problems, "; ")))
			continue
		}
		r.pass(name, fmt.Sprintf(
			"shard=%d owner=%s; SDK round-trip; direct owner hit; %s (holds no copy "+
				"of this shard) miss", shard, owner, other))
	}
}

// checkReplication is the Phase 4 counterpart to checkRouting, and it proves
// the same kind of thing the same way: not "the SDK returned my value", but
// "the value is physically on the machines the shard map says hold it".
//
// The write is a strong-ack Put, so by the time it returns the primary claims
// every replica has stored it. That claim is then checked directly, against each
// replica's own address, bypassing the SDK entirely. A replica is never routed
// to by a client -- reads are primary-only in Phase 4 -- so a direct hit there
// can only mean replication actually moved the bytes.
func checkReplication(r *reporter, cfg *config.Config, conn *grpc.ClientConn, rpcTimeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	snapshot, err := clustertopology.Fetch(ctx, metadatav1.NewClusterMetadataServiceClient(conn))
	cancel()
	if err != nil {
		r.fail("replication/live metadata", err)
		return
	}

	if len(snapshot.Owners) == 0 {
		r.fail("replication/live metadata",
			errors.New("metadata published no shard_to_owners map; the control plane predates Phase 4"))
		return
	}

	// The LIVE replication factor is what this leg checks against, not the
	// config's. The config is launch inventory -- Phase 3 already stopped
	// treating its node list as runtime truth for exactly this reason -- and
	// the control plane can be started with an override. What must hold is that
	// whatever factor the cluster is running, placement is exactly the router's
	// computation at that factor.
	liveFactor := int(snapshot.ReplicationFactor)
	wantOwners := router.AssignShardOwners(cfg.NodeIDs(), cfg.ShardCount, liveFactor)
	for shard := uint32(0); shard < cfg.ShardCount; shard++ {
		got := snapshot.Owners[shard]
		want := wantOwners[shard]
		if got.Primary != want.Primary || !slices.Equal(got.Replicas, want.Replicas) {
			r.fail("replication/live metadata", fmt.Errorf(
				"shard %d owners = {%s %v}, router.AssignShardOwners wants {%s %v}",
				shard, got.Primary, got.Replicas, want.Primary, want.Replicas))
			return
		}
		if got.Primary != snapshot.ShardMap[shard] {
			r.fail("replication/live metadata", fmt.Errorf(
				"shard %d primary %q disagrees with shard_to_node_id %q",
				shard, got.Primary, snapshot.ShardMap[shard]))
			return
		}
	}
	detail := fmt.Sprintf(
		"replication_factor=%d; %d shard(s) carry primary+replica owners matching "+
			"router.AssignShardOwners exactly", liveFactor, len(snapshot.Owners))
	if liveFactor != cfg.ReplicationFactor {
		detail += fmt.Sprintf(" (config says %d; the cluster was started with an override)",
			cfg.ReplicationFactor)
	}
	r.pass("replication/live metadata", detail)

	if liveFactor == 0 {
		// 0 is a legal, supported configuration. Assert that it is genuinely
		// unreplicated rather than skipping quietly, so a cluster that was meant
		// to replicate and silently is not cannot pass this leg.
		for shard := uint32(0); shard < cfg.ShardCount; shard++ {
			if len(snapshot.Owners[shard].Replicas) != 0 {
				r.fail("replication/factor 0", fmt.Errorf(
					"shard %d has %d replica(s) at a live replication_factor of 0",
					shard, len(snapshot.Owners[shard].Replicas)))
				return
			}
		}
		r.pass("replication/factor 0",
			"no shard has a replica, and no strong-ack write is possible; nothing to prove")
		return
	}

	routed, err := pulsekvclient.New(
		cfg.ControlPlaneEndpoints(),
		pulsekvclient.WithRefreshInterval(0),
		pulsekvclient.WithRefreshTimeout(rpcTimeout),
	)
	if err != nil {
		r.fail("replication/client.New", err)
		return
	}
	defer routed.Close()

	// A shard with the full complement of replicas, so the strongest available
	// ack count is exercised rather than whatever the first shard happens to have.
	var chosen uint32
	found := false
	for shard := uint32(0); shard < cfg.ShardCount && !found; shard++ {
		if len(snapshot.Owners[shard].Replicas) == liveFactor {
			chosen, found = shard, true
		}
	}
	if !found {
		r.fail("replication/shard selection", fmt.Errorf(
			"no shard has the live cluster's %d replica(s); it has %d live node(s)",
			liveFactor, len(snapshot.Nodes)))
		return
	}

	owners := snapshot.Owners[chosen]
	prefix := fmt.Sprintf("smoke:replication:%d:%d", os.Getpid(), time.Now().UnixNano())
	key, err := keyForShard(prefix, chosen, cfg.ShardCount)
	if err != nil {
		r.fail("replication/sample key", err)
		return
	}
	value := deterministicValue(8192, 41)
	acks := uint32(len(owners.Replicas))

	acked, err := putWithAckConverging(routed, key, value, acks, rpcTimeout)
	if err != nil {
		r.fail("replication/PutWithAck", fmt.Errorf(
			"strong-ack write to shard %d (primary %s, %d replica(s)): %w",
			chosen, owners.Primary, acks, err))
		return
	}
	if acked < acks {
		r.fail("replication/PutWithAck", fmt.Errorf(
			"reported %d ack(s) for %d requested; an OK response must not undercount", acked, acks))
		return
	}
	r.pass("replication/PutWithAck", fmt.Sprintf(
		"shard %d: %d of %d replica(s) acked before the write returned", chosen, acked, acks))

	// The proof itself: direct NodeService.Get on each holder's own address.
	var problems []string
	for _, holder := range append([]string{owners.Primary}, owners.Replicas...) {
		address := snapshot.Nodes[holder]
		if address == "" {
			problems = append(problems, fmt.Sprintf("%s has no address", holder))
			continue
		}
		directConn, err := dial(address)
		if err != nil {
			problems = append(problems, fmt.Sprintf("dial %s at %s: %v", holder, address, err))
			continue
		}
		resp, err := directGet(nodev1.NewNodeServiceClient(directConn), key, rpcTimeout)
		directConn.Close()
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("direct Get on %s: %v", holder, err))
		case !resp.GetFound():
			problems = append(problems, fmt.Sprintf("%s returned found=false", holder))
		case !bytes.Equal(resp.GetValue(), value):
			problems = append(problems, fmt.Sprintf(
				"%s returned %d bytes with different contents", holder, len(resp.GetValue())))
		}
	}
	if len(problems) > 0 {
		r.fail("replication/direct replica reads", errors.New(strings.Join(problems, "; ")))
		return
	}
	r.pass("replication/direct replica reads", fmt.Sprintf(
		"shard %d is byte-identical on its primary %s and all %d replica(s) %v, read directly "+
			"rather than through the SDK", chosen, owners.Primary, acks, owners.Replicas))

	// Asking for more acks than exist must fail fast and specifically, not hang
	// until the deadline and then look like a network problem.
	overKey, err := keyForShard(prefix+":over", chosen, cfg.ShardCount)
	if err != nil {
		r.fail("replication/over-ack sample key", err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	_, err = routed.PutWithAck(ctx, overKey, value, acks+1)
	cancel()
	r.wantCode("replication/PutWithAck(too many acks)", err, codes.InvalidArgument, "replica")
}

// putWithAckConverging absorbs the one bounded window in which a primary can
// legitimately refuse a strong-ack write: the SDK and each data node learn
// ownership independently, so for up to one node poll interval after a
// membership change the primary may not yet know it is the primary, or may see
// fewer replicas than the map does. It says which, with INVALID_ARGUMENT, and
// the write is idempotent, so retrying is the correct response.
//
// Only INVALID_ARGUMENT is retried. DEADLINE_EXCEEDED means the fan-out really
// did fall short, which is a finding rather than a timing artefact.
func putWithAckConverging(routed *pulsekvclient.Client, key, value []byte,
	acks uint32, rpcTimeout time.Duration) (uint32, error) {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		acked, err := routed.PutWithAck(ctx, key, value, acks)
		cancel()
		if err == nil {
			return acked, nil
		}
		if status.Code(err) != codes.InvalidArgument {
			return 0, err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("primary did not converge on its replica set after %d attempt(s): %w",
				attempt, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// keyForShard finds a deterministic key that hashes into want.
func keyForShard(prefix string, want, shardCount uint32) ([]byte, error) {
	const maxCandidates = 1_000_000
	for candidate := 0; candidate < maxCandidates; candidate++ {
		key := []byte(fmt.Sprintf("%s:%d", prefix, candidate))
		if router.ShardForKey(key, shardCount) == want {
			return key, nil
		}
	}
	return nil, fmt.Errorf("no key hashed into shard %d after %d candidates", want, maxCandidates)
}

type liveRoutingTopology struct {
	generation uint64
	nodes      map[string]string
	shardMap   map[uint32]string
	owners     map[uint32]router.ShardOwners
}

// nonHolder returns a node that holds no copy of shard -- neither its primary
// nor one of its replicas -- or "" when every live node holds one.
//
// Phase 2's version of this only skipped the owner, because before replication
// "not the owner" and "does not have the key" were the same statement. They are
// not any more: a replica is supposed to have the key, so picking one and
// asserting a miss would fail for the best possible reason. Excluding the whole
// owner set keeps the assertion meaningful and makes it strictly stronger --
// the claim becomes "this key is on its holders and on nothing else".
func nonHolder(shard uint32, topology liveRoutingTopology, sortedIDs []string) string {
	holders := map[string]bool{topology.shardMap[shard]: true}
	for _, replica := range topology.owners[shard].Replicas {
		holders[replica] = true
	}
	for _, id := range sortedIDs {
		if !holders[id] {
			return id
		}
	}
	return ""
}

func fetchLiveRoutingTopology(md metadatav1.ClusterMetadataServiceClient,
	rpcTimeout time.Duration) (liveRoutingTopology, error) {

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	snapshot, err := clustertopology.Fetch(ctx, md)
	if err != nil {
		return liveRoutingTopology{}, err
	}
	return liveRoutingTopology{
		generation: snapshot.Generation,
		nodes:      snapshot.Nodes,
		shardMap:   snapshot.ShardMap,
		owners:     snapshot.Owners,
	}, nil
}

func routingSampleKeys(prefix string, count int, shardCount uint32,
	shardMap map[uint32]string) ([][]byte, error) {

	if count <= 0 {
		return nil, errors.New("routing sample count must be positive")
	}
	owners := make(map[string]struct{})
	for _, owner := range shardMap {
		if owner != "" {
			owners[owner] = struct{}{}
		}
	}
	if len(owners) == 0 {
		return nil, errors.New("cannot sample keys from a shard map with no owners")
	}

	// First cover as many distinct owners as the sample budget permits. This
	// makes the normal four-node smoke test exercise all four SDK routes rather
	// than merely hoping six arbitrary keys happen to spread out.
	wantDistinct := count
	if wantDistinct > len(owners) {
		wantDistinct = len(owners)
	}
	seenOwners := make(map[string]struct{}, wantDistinct)
	keys := make([][]byte, 0, count)
	candidate := 0
	const maxCandidates = 1_000_000
	for candidate < maxCandidates && len(seenOwners) < wantDistinct {
		key := []byte(fmt.Sprintf("%s:%d", prefix, candidate))
		candidate++
		owner, ok := router.OwnerForKey(key, shardCount, shardMap)
		if !ok {
			continue
		}
		if _, seen := seenOwners[owner]; seen {
			continue
		}
		seenOwners[owner] = struct{}{}
		keys = append(keys, key)
	}
	if len(seenOwners) < wantDistinct {
		return nil, fmt.Errorf("found keys for only %d of %d shard-map owner(s) after %d candidates",
			len(seenOwners), wantDistinct, maxCandidates)
	}

	for len(keys) < count && candidate < maxCandidates {
		key := []byte(fmt.Sprintf("%s:%d", prefix, candidate))
		candidate++
		if _, ok := router.OwnerForKey(key, shardCount, shardMap); ok {
			keys = append(keys, key)
		}
	}
	if len(keys) != count {
		return nil, fmt.Errorf("found only %d of %d requested routing sample keys", len(keys), count)
	}
	return keys, nil
}

func directGet(client nodev1.NodeServiceClient, key []byte,
	rpcTimeout time.Duration) (*nodev1.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.Get(ctx, &nodev1.GetRequest{Key: key})
}

// Phase 1: every NodeService RPC is real. These assertions replaced Phase 0's
// "must return UNIMPLEMENTED" ones, exactly as the Phase 0 summary said they
// would have to.
func checkNode(r *reporter, n config.Node, cfg *config.Config,
	conn *grpc.ClientConn, rpcTimeout time.Duration) {

	client := nodev1.NewNodeServiceClient(conn)
	id := n.NodeID

	// Per-node key namespace so four nodes checked in sequence cannot see each
	// other's keys through PrefixMatch.
	ns := "smoke:" + id + ":"
	key := func(s string) []byte { return []byte(ns + s) }

	// --- HealthCheck must be real and must know its own identity.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := client.HealthCheck(ctx, &nodev1.HealthCheckRequest{})
		switch {
		case err != nil:
			r.fail(id+"/HealthCheck", err)
		case !resp.GetOk():
			r.fail(id+"/HealthCheck", errors.New("ok=false"))
		case resp.GetNodeId() != id:
			r.fail(id+"/HealthCheck",
				fmt.Errorf("reported node_id %q, config says %q", resp.GetNodeId(), id))
		case resp.GetUptimeSeconds() < 0:
			r.fail(id+"/HealthCheck",
				fmt.Errorf("negative uptime_seconds=%d", resp.GetUptimeSeconds()))
		default:
			r.pass(id+"/HealthCheck",
				fmt.Sprintf("ok=true node_id=%s uptime=%ds", resp.GetNodeId(), resp.GetUptimeSeconds()))
		}
	}()

	// --- Put then Get: the whole point of the phase.
	small := deterministicValue(4096, 1)
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		if _, err := client.Put(ctx, &nodev1.PutRequest{Key: key("small"), Value: small}); err != nil {
			r.fail(id+"/Put", err)
			return
		}
		resp, err := client.Get(ctx, &nodev1.GetRequest{Key: key("small")})
		switch {
		case err != nil:
			r.fail(id+"/Put+Get", err)
		case !resp.GetFound():
			r.fail(id+"/Put+Get", errors.New("found=false immediately after a successful Put"))
		case !bytes.Equal(resp.GetValue(), small):
			r.fail(id+"/Put+Get", fmt.Errorf("got %d bytes, wrote %d, contents differ",
				len(resp.GetValue()), len(small)))
		default:
			r.pass(id+"/Put+Get", fmt.Sprintf("%d-byte value round-tripped byte-for-byte", len(small)))
		}
	}()

	// --- A miss is a successful response, not an error.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := client.Get(ctx, &nodev1.GetRequest{Key: key("never-written")})
		switch {
		case err != nil:
			r.fail(id+"/Get(miss)", fmt.Errorf("a miss must be OK with found=false, got: %w", err))
		case resp.GetFound():
			r.fail(id+"/Get(miss)", errors.New("found=true for a key that was never written"))
		default:
			r.pass(id+"/Get(miss)", "found=false, status OK")
		}
	}()

	// --- Overwrite replaces, and does not append or leak the old value.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		replacement := deterministicValue(777, 2)
		if _, err := client.Put(ctx, &nodev1.PutRequest{Key: key("small"), Value: replacement}); err != nil {
			r.fail(id+"/Put(overwrite)", err)
			return
		}
		resp, err := client.Get(ctx, &nodev1.GetRequest{Key: key("small")})
		switch {
		case err != nil:
			r.fail(id+"/Put(overwrite)", err)
		case !bytes.Equal(resp.GetValue(), replacement):
			r.fail(id+"/Put(overwrite)", fmt.Errorf("got %d bytes, expected the %d-byte replacement",
				len(resp.GetValue()), len(replacement)))
		default:
			r.pass(id+"/Put(overwrite)", "a shorter overwrite fully replaces the previous value")
		}
	}()

	// --- Argument validation.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		_, err := client.Get(ctx, &nodev1.GetRequest{Key: nil})
		r.wantCode(id+"/Get(empty key)", err, codes.InvalidArgument, "key")
	}()

	// --- An oversized unary Put fails fast and names the RPC that can carry it.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		oversized := make([]byte, unaryValueLimit+1)
		_, err := client.Put(ctx, &nodev1.PutRequest{Key: key("too-big-unary"), Value: oversized})
		r.wantCode(id+"/Put(>4MiB)", err, codes.InvalidArgument, "PutChunked")
	}()

	// --- The headline of Step 1.2: a multi-megabyte value through the
	//     chunked path, verified byte for byte.
	big := deterministicValue(6*1024*1024, 3)
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		if err := putChunked(ctx, client, key("big"), big, 1024*1024); err != nil {
			r.fail(id+"/PutChunked", err)
			return
		}
		got, found, err := getChunked(ctx, client, key("big"))
		switch {
		case err != nil:
			r.fail(id+"/PutChunked+GetChunked", err)
		case !found:
			r.fail(id+"/PutChunked+GetChunked", errors.New("empty stream after a successful PutChunked"))
		case !bytes.Equal(got, big):
			r.fail(id+"/PutChunked+GetChunked",
				fmt.Errorf("got %d bytes, wrote %d, contents differ", len(got), len(big)))
		default:
			r.pass(id+"/PutChunked+GetChunked",
				fmt.Sprintf("%s round-tripped byte-for-byte over %d chunks",
					humanSize(len(big)), (len(big)+1024*1024-1)/(1024*1024)))
		}
	}()

	// --- Reading that value with unary Get must refuse, specifically.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		_, err := client.Get(ctx, &nodev1.GetRequest{Key: key("big")})
		r.wantCode(id+"/Get(>4MiB stored)", err, codes.FailedPrecondition, "GetChunked")
	}()

	// --- GetChunked on a miss is an empty stream, not an error.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		got, found, err := getChunked(ctx, client, key("also-never-written"))
		switch {
		case err != nil:
			r.fail(id+"/GetChunked(miss)", err)
		case found || len(got) != 0:
			r.fail(id+"/GetChunked(miss)", errors.New("a miss produced chunks"))
		default:
			r.pass(id+"/GetChunked(miss)", "empty stream, status OK")
		}
	}()

	// --- The framing rules, each violated on purpose.
	checkChunkedRejections(r, id, key, client, cfg, rpcTimeout)

	// --- Capacity reflects what we just stored.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := client.Capacity(ctx, &nodev1.CapacityRequest{})
		switch {
		case err != nil:
			r.fail(id+"/Capacity", err)
		case resp.GetResidentKeys() == 0:
			r.fail(id+"/Capacity", errors.New("resident_keys=0 after writing several keys"))
		case resp.GetBytesInRamTier()+resp.GetBytesInNvmeTier() == 0:
			r.fail(id+"/Capacity", errors.New("both tiers report zero bytes after writes"))
		default:
			r.pass(id+"/Capacity",
				fmt.Sprintf("keys=%d ram=%s nvme=%s", resp.GetResidentKeys(),
					humanSize(int(resp.GetBytesInRamTier())),
					humanSize(int(resp.GetBytesInNvmeTier()))))
		}
	}()

	// --- PrefixMatch sees exactly this node's namespace.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		stream, err := client.PrefixMatch(ctx, &nodev1.PrefixMatchRequest{Prefix: []byte(ns)})
		if err != nil {
			r.fail(id+"/PrefixMatch", err)
			return
		}

		seen := map[string]*nodev1.PrefixMatchResponse{}
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				r.fail(id+"/PrefixMatch", err)
				return
			}
			seen[string(msg.GetKey())] = msg
		}

		var problems []string
		if _, ok := seen[ns+"small"]; !ok {
			problems = append(problems, "did not return the small key")
		}
		bigMsg, ok := seen[ns+"big"]
		if !ok {
			problems = append(problems, "did not return the big key")
		}
		for k := range seen {
			if !strings.HasPrefix(k, ns) {
				problems = append(problems, fmt.Sprintf("returned out-of-namespace key %q", k))
			}
		}
		if len(problems) > 0 {
			sort.Strings(problems)
			r.fail(id+"/PrefixMatch", errors.New(strings.Join(problems, "; ")))
			return
		}
		r.pass(id+"/PrefixMatch", fmt.Sprintf("%d key(s), all within the requested prefix", len(seen)))

		// A value above the unary limit is flagged rather than silently
		// returned as empty -- the distinction the value_omitted field exists
		// to make.
		switch {
		case bigMsg == nil:
			// already reported above
		case !bigMsg.GetValueOmitted():
			r.fail(id+"/PrefixMatch(large value)",
				errors.New("a 6 MiB value was not marked value_omitted"))
		case len(bigMsg.GetValue()) != 0:
			r.fail(id+"/PrefixMatch(large value)",
				errors.New("value_omitted is set but a value was still inlined"))
		default:
			r.pass(id+"/PrefixMatch(large value)",
				"the 6 MiB value is flagged value_omitted, not inlined")
		}
	}()
}

// checkChunkedRejections violates one framing rule at a time. Each of these is
// a way a buggy or hostile client could otherwise get a corrupt value stored.
func checkChunkedRejections(r *reporter, id string, key func(string) []byte,
	client nodev1.NodeServiceClient, cfg *config.Config, rpcTimeout time.Duration) {

	send := func(chunks []*nodev1.PutChunk) error {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		stream, err := client.PutChunked(ctx)
		if err != nil {
			return err
		}
		for _, c := range chunks {
			if err := stream.Send(c); err != nil {
				// A stream the server has already rejected surfaces the real
				// status on CloseAndRecv, not on Send.
				break
			}
		}
		_, err = stream.CloseAndRecv()
		return err
	}

	// Chunk indices out of order. gRPC guarantees ordering, so this can only
	// be a broken client.
	r.wantCode(id+"/PutChunked(out of order)", send([]*nodev1.PutChunk{
		{Key: key("bad1"), ChunkIndex: 0, TotalChunks: 2, TotalLength: 8, Data: []byte("1234")},
		{ChunkIndex: 5, TotalChunks: 2, TotalLength: 8, Data: []byte("5678")},
	}), codes.InvalidArgument, "chunk_index")

	// Stream ends early: fewer chunks than declared.
	r.wantCode(id+"/PutChunked(short stream)", send([]*nodev1.PutChunk{
		{Key: key("bad2"), ChunkIndex: 0, TotalChunks: 3, TotalLength: 12, Data: []byte("1234")},
	}), codes.InvalidArgument, "declared")

	// More data than total_length claimed. Cut off at the point it starts
	// lying, not after buffering all of it.
	r.wantCode(id+"/PutChunked(data > total_length)", send([]*nodev1.PutChunk{
		{Key: key("bad3"), ChunkIndex: 0, TotalChunks: 1, TotalLength: 2, Data: []byte("far too much")},
	}), codes.InvalidArgument, "total_length")

	// total_length that is under-declared relative to the data actually sent
	// across two chunks.
	r.wantCode(id+"/PutChunked(length mismatch)", send([]*nodev1.PutChunk{
		{Key: key("bad4"), ChunkIndex: 0, TotalChunks: 2, TotalLength: 8, Data: []byte("1234")},
		{ChunkIndex: 1, TotalChunks: 2, TotalLength: 8, Data: []byte("56")},
	}), codes.InvalidArgument, "total_length")

	// First chunk with no key.
	r.wantCode(id+"/PutChunked(no key)", send([]*nodev1.PutChunk{
		{ChunkIndex: 0, TotalChunks: 1, TotalLength: 4, Data: []byte("1234")},
	}), codes.InvalidArgument, "key")

	// A declared length past the node's hard ceiling must be refused on the
	// number alone, before a byte is buffered.
	r.wantCode(id+"/PutChunked(> max-value-bytes)", send([]*nodev1.PutChunk{
		{
			Key:         key("bad5"),
			ChunkIndex:  0,
			TotalChunks: 1,
			TotalLength: cfg.Engine.MaxValueBytes + 1,
			Data:        []byte("x"),
		},
	}), codes.OutOfRange, "max-value-bytes")

	// A key that changes mid-stream would splice two writes into one value.
	r.wantCode(id+"/PutChunked(key changed)", send([]*nodev1.PutChunk{
		{Key: key("bad6"), ChunkIndex: 0, TotalChunks: 2, TotalLength: 8, Data: []byte("1234")},
		{Key: key("other"), ChunkIndex: 1, TotalChunks: 2, TotalLength: 8, Data: []byte("5678")},
	}), codes.InvalidArgument, "different key")

	// Finally: none of the rejected writes may have stored anything.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		var stored []string
		for _, k := range []string{"bad1", "bad2", "bad3", "bad4", "bad5", "bad6", "other", "too-big-unary"} {
			resp, err := client.Get(ctx, &nodev1.GetRequest{Key: key(k)})
			if err == nil && resp.GetFound() {
				stored = append(stored, k)
			}
		}
		if len(stored) > 0 {
			r.fail(id+"/rejected writes stored nothing",
				fmt.Errorf("these keys exist after their writes were rejected: %s",
					strings.Join(stored, ", ")))
			return
		}
		r.pass(id+"/rejected writes stored nothing",
			"all 8 rejected writes left no key behind")
	}()
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

type result struct {
	name   string
	detail string
	err    error
}

type reporter struct{ results []result }

func (r *reporter) pass(name, detail string) {
	r.results = append(r.results, result{name: name, detail: detail})
}

func (r *reporter) fail(name string, err error) {
	r.results = append(r.results, result{name: name, err: err})
}

// wantUnimplemented records a pass only if err is a gRPC UNIMPLEMENTED status.
// Still used for AdapterService, which genuinely has no server until Phase 7 --
// a success there would mean something is answering that should not be.
func (r *reporter) wantUnimplemented(name string, err error) {
	r.wantCode(name, err, codes.Unimplemented, "")
}

// wantCode records a pass only if err carries the expected gRPC status code and
// its message mentions `substr`.
//
// A success is a failure here, and the message check is not decoration: an
// oversized unary Put returning a bare INVALID_ARGUMENT is not good enough,
// because the whole point is that it tells the caller to use PutChunked. These
// assertions are what stop an error path from degrading into a generic one.
func (r *reporter) wantCode(name string, err error, want codes.Code, substr string) {
	if err == nil {
		r.fail(name, fmt.Errorf("returned OK; expected %s (a rejected call must not silently succeed)", want))
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		r.fail(name, fmt.Errorf("non-gRPC error: %v", err))
		return
	}
	if st.Code() != want {
		r.fail(name, fmt.Errorf("expected %s, got %s: %s", want, st.Code(), st.Message()))
		return
	}
	if substr != "" && !strings.Contains(st.Message(), substr) {
		r.fail(name, fmt.Errorf("%s message does not mention %q: %s", want, substr, st.Message()))
		return
	}
	detail := want.String() + " as expected"
	if substr != "" {
		detail += fmt.Sprintf(", message names %q", substr)
	}
	r.pass(name, detail)
}

// report prints every check and returns the process exit code.
func (r *reporter) report() int {
	width := 0
	for _, res := range r.results {
		if len(res.name) > width {
			width = len(res.name)
		}
	}

	failed := 0
	for _, res := range r.results {
		if res.err != nil {
			failed++
			fmt.Printf("FAIL  %-*s  %s\n", width, res.name, compact(res.err))
			continue
		}
		fmt.Printf("ok    %-*s  %s\n", width, res.name, res.detail)
	}

	fmt.Printf("\nsmoke: %d check(s), %d passed, %d failed\n",
		len(r.results), len(r.results)-failed, failed)
	if failed > 0 {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func dial(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		))
}

// deterministicValue produces non-compressible bytes that no allocator or
// filesystem will reproduce by accident, so a byte-for-byte comparison after a
// round trip means something. Same xorshift64* the C engine tests use.
func deterministicValue(n int, seed uint64) []byte {
	buf := make([]byte, n)
	state := seed*2862933555777941757 + 3037000493
	if state == 0 {
		state = 1
	}
	var scratch [8]byte
	for off := 0; off < n; off += 8 {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		binary.LittleEndian.PutUint64(scratch[:], state*0x2545F4914F6CDD1D)
		copy(buf[off:], scratch[:])
	}
	return buf
}

func putChunked(ctx context.Context, client nodev1.NodeServiceClient,
	key, value []byte, chunkSize int) error {

	total := (len(value) + chunkSize - 1) / chunkSize
	if total == 0 {
		total = 1 // a zero-length value is still one chunk
	}

	stream, err := client.PutChunked(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < total; i++ {
		lo := i * chunkSize
		hi := lo + chunkSize
		if hi > len(value) {
			hi = len(value)
		}
		chunk := &nodev1.PutChunk{
			ChunkIndex:  uint32(i),
			TotalChunks: uint32(total),
			TotalLength: uint64(len(value)),
			Data:        value[lo:hi],
		}
		if i == 0 {
			chunk.Key = key
		}
		if err := stream.Send(chunk); err != nil {
			break // the real status arrives on CloseAndRecv
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// getChunked reassembles a streamed value. found is false for the empty stream
// that means a miss.
func getChunked(ctx context.Context, client nodev1.NodeServiceClient, key []byte) ([]byte, bool, error) {
	stream, err := client.GetChunked(ctx, &nodev1.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	var (
		out   []byte
		found bool
		next  uint32
	)
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		if chunk.GetChunkIndex() != next {
			return nil, false, fmt.Errorf("server sent chunk_index %d, expected %d",
				chunk.GetChunkIndex(), next)
		}
		next++
		found = true
		if out == nil {
			out = make([]byte, 0, chunk.GetTotalLength())
		}
		out = append(out, chunk.GetData()...)
	}
	return out, found, nil
}

func humanSize(n int) string {
	switch {
	case n >= 1024*1024:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1024*1024))
	case n >= 1024:
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// shardMapDifferences returns a bounded, deterministic description of every
// way got differs from want. The bound keeps one stale control plane from
// printing hundreds of near-identical shard complaints, while the exact map
// comparison still checks every entry.
func shardMapDifferences(got, want map[uint32]string, limit int) []string {
	var shards []uint32
	seen := make(map[uint32]struct{}, len(got)+len(want))
	for shard := range got {
		seen[shard] = struct{}{}
	}
	for shard := range want {
		seen[shard] = struct{}{}
	}
	for shard := range seen {
		gotOwner, gotOK := got[shard]
		wantOwner, wantOK := want[shard]
		if gotOK != wantOK || gotOwner != wantOwner {
			shards = append(shards, shard)
		}
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })

	count := len(shards)
	if limit >= 0 && len(shards) > limit {
		shards = shards[:limit]
	}
	problems := make([]string, 0, len(shards)+1)
	for _, shard := range shards {
		gotOwner, gotOK := got[shard]
		wantOwner, wantOK := want[shard]
		switch {
		case !gotOK:
			problems = append(problems, fmt.Sprintf(
				"live map missing shard %d (router.AssignShards owner %q)", shard, wantOwner))
		case !wantOK:
			problems = append(problems, fmt.Sprintf(
				"live map has unexpected shard %d owned by %q", shard, gotOwner))
		default:
			problems = append(problems, fmt.Sprintf(
				"shard %d owner %q, router.AssignShards wants %q", shard, gotOwner, wantOwner))
		}
	}
	if count > len(shards) {
		problems = append(problems, fmt.Sprintf("shard map differs on %d more shard(s)", count-len(shards)))
	}
	return problems
}

// compact flattens gRPC's multi-line error strings so one failure stays on one
// line in the report.
func compact(err error) string {
	if err == nil {
		return ""
	}
	s := strings.Join(strings.Fields(err.Error()), " ")
	const max = 160
	if len(s) > max {
		s = s[:max-3] + "..."
	}
	return s
}
