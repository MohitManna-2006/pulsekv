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
		expectAbsent = flag.String("expect-absent", "",
			"comma-separated node IDs that must be absent in --mode=topology-wait")
		minGeneration = flag.Uint64("min-generation", 0,
			"minimum topology generation for --mode=topology-wait")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: %v\n", err)
		os.Exit(2)
	}

	switch *mode {
	case "wait":
		os.Exit(runWait(cfg, *timeout, *rpcTimeout, *pollInterval))
	case "smoke":
		os.Exit(runSmoke(cfg, *rpcTimeout))
	case "topology-wait":
		live, err := expectedNodeIDs(cfg, *expectLive)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pulsekv-smoke: %v\n", err)
			os.Exit(2)
		}
		absent, err := parseNodeIDs(*expectAbsent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pulsekv-smoke: --expect-absent: %v\n", err)
			os.Exit(2)
		}
		os.Exit(runTopologyWait(cfg, live, absent, *minGeneration,
			*timeout, *rpcTimeout, *pollInterval))
	default:
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: unknown --mode %q (want wait, smoke, or topology-wait)\n", *mode)
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

func processes(cfg *config.Config) []process {
	ps := []process{{label: "controlplane", address: cfg.ControlPlane.Address()}}
	for _, n := range cfg.Nodes {
		ps = append(ps, process{label: n.NodeID, address: n.Address(), isNode: true})
	}
	return ps
}

func runWait(cfg *config.Config, budget, rpcTimeout, interval time.Duration) int {
	ps := processes(cfg)
	deadline := time.Now().Add(budget)

	fmt.Printf("waiting up to %s for %d service(s) to report healthy...\n", budget, len(ps))

	var lastErrs map[string]error
	for attempt := 1; ; attempt++ {
		lastErrs = probeAll(ps, rpcTimeout)
		if len(lastErrs) == 0 {
			fmt.Printf("all %d service(s) healthy after %d poll(s)\n", len(ps), attempt)
			return 0
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(interval)
	}

	// Fail loudly and specifically. "cluster did not come up" is useless;
	// "node-2 on 127.0.0.1:7102 refused the connection" is actionable.
	fmt.Fprintf(os.Stderr, "\nFAILED: %d of %d process(es) did not become healthy within %s\n",
		len(lastErrs), len(ps), budget)
	for _, p := range ps {
		if err, bad := lastErrs[p.label]; bad {
			fmt.Fprintf(os.Stderr, "  %-14s %-22s %v\n", p.label, p.address, compact(err))
		}
	}
	return 1
}

// ---------------------------------------------------------------------------
// topology-wait mode
// ---------------------------------------------------------------------------

func runTopologyWait(cfg *config.Config, expected, absent []string, minGeneration uint64,
	budget, rpcTimeout, interval time.Duration) int {

	deadline := time.Now().Add(budget)
	var lastErr error
	for attempt := 1; ; attempt++ {
		lastErr = probeTopology(cfg.ControlPlane.Address(), cfg.ShardCount,
			expected, absent, minGeneration, rpcTimeout)
		if lastErr == nil {
			fmt.Printf("topology converged after %d poll(s): generation >= %d, live=[%s]\n",
				attempt, minGeneration, strings.Join(expected, ","))
			return 0
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "FAILED: topology did not converge within %s: %v\n", budget, lastErr)
			return 1
		}
		time.Sleep(interval)
	}
}

func probeTopology(controlPlane string, shardCount uint32, expected, absent []string, minGeneration uint64,
	rpcTimeout time.Duration) error {

	conn, err := dial(controlPlane)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	snapshot, err := clustertopology.Fetch(ctx, metadatav1.NewClusterMetadataServiceClient(conn))
	if err != nil {
		return err
	}
	if snapshot.Generation < minGeneration {
		return fmt.Errorf("generation=%d, want at least %d", snapshot.Generation, minGeneration)
	}
	if snapshot.ShardCount != shardCount {
		return fmt.Errorf("shard count=%d, want configured %d", snapshot.ShardCount, shardCount)
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

	cpConn, err := dial(cfg.ControlPlane.Address())
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: cannot reach control plane at %s: %v\n",
			cfg.ControlPlane.Address(), err)
		return 1
	}
	defer cpConn.Close()

	checkControlPlane(r, cfg, cpConn, rpcTimeout)
	checkRouting(r, cfg, cpConn, rpcTimeout)

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
		cfg.ControlPlane.Address(),
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
		other := differentNode(owner, nodeIDs)
		if other == "" {
			r.fail(fmt.Sprintf("routing/key[%d]", i), fmt.Errorf(
				"no node differs from predicted owner %q", owner))
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
				problems = append(problems, fmt.Sprintf("direct Get on non-owner %s: %v", other, directErr))
			case otherResp.GetFound():
				problems = append(problems, fmt.Sprintf("non-owner %s unexpectedly returned found=true", other))
			}
		}

		name := fmt.Sprintf("routing/key[%d]", i)
		if len(problems) > 0 {
			r.fail(name, errors.New(strings.Join(problems, "; ")))
			continue
		}
		r.pass(name, fmt.Sprintf(
			"shard=%d owner=%s; SDK round-trip; direct owner hit; %s miss",
			router.ShardForKey(key, cfg.ShardCount), owner, other))
	}
}

type liveRoutingTopology struct {
	generation uint64
	nodes      map[string]string
	shardMap   map[uint32]string
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

func differentNode(owner string, sortedNodeIDs []string) string {
	for _, id := range sortedNodeIDs {
		if id != owner {
			return id
		}
	}
	return ""
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
