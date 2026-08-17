// Command pulsekv-smoke is the Phase 0 cluster verifier.
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
//	--mode=smoke  assert the full Phase 0 contract: real HealthCheck data
//	              everywhere, static metadata that matches the config, and
//	              UNIMPLEMENTED -- not a fake success -- from every RPC that
//	              Phase 0 does not implement. Used by deploy/smoke-test.sh.
package main

import (
	"context"
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
)

func main() {
	var (
		configPath = flag.String("config", "deploy/cluster.config.yaml",
			"path to the static cluster config")
		mode = flag.String("mode", "smoke",
			"`wait` (poll until healthy) or `smoke` (assert the full Phase 0 contract)")
		timeout = flag.Duration("timeout", 15*time.Second,
			"overall budget for --mode=wait")
		rpcTimeout = flag.Duration("rpc-timeout", 2*time.Second,
			"per-RPC deadline")
		pollInterval = flag.Duration("poll-interval", 250*time.Millisecond,
			"delay between polls in --mode=wait")
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
	default:
		fmt.Fprintf(os.Stderr, "pulsekv-smoke: unknown --mode %q (want wait or smoke)\n", *mode)
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

	fmt.Printf("waiting up to %s for %d process(es) to report healthy...\n", budget, len(ps))

	var lastErrs map[string]error
	for attempt := 1; ; attempt++ {
		lastErrs = probeAll(ps, rpcTimeout)
		if len(lastErrs) == 0 {
			fmt.Printf("all %d process(es) healthy after %d poll(s)\n", len(ps), attempt)
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

	for _, n := range cfg.Nodes {
		conn, err := dial(n.Address())
		if err != nil {
			r.fail(n.NodeID+"/dial", err)
			continue
		}
		checkNode(r, n, conn, rpcTimeout)
		conn.Close()
	}

	return r.report()
}

func checkControlPlane(r *reporter, cfg *config.Config, conn *grpc.ClientConn, rpcTimeout time.Duration) {
	md := metadatav1.NewClusterMetadataServiceClient(conn)

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

		want := make(map[string]string, len(cfg.Nodes)) // node ID -> address
		for _, n := range cfg.Nodes {
			want[n.NodeID] = n.Address()
		}

		var problems []string
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
			fmt.Sprintf("%d node(s), all alive, addresses match config", len(resp.GetNodes())))
	}()

	// --- GetShardMap must cover every shard with a configured owner.
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
		var problems []string
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
			fmt.Sprintf("%d shard(s) over %d node(s), all owners known", len(got), len(owners)))
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

func checkNode(r *reporter, n config.Node, conn *grpc.ClientConn, rpcTimeout time.Duration) {
	client := nodev1.NewNodeServiceClient(conn)

	// --- HealthCheck must be real and must know its own identity.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		resp, err := client.HealthCheck(ctx, &nodev1.HealthCheckRequest{})
		switch {
		case err != nil:
			r.fail(n.NodeID+"/HealthCheck", err)
		case !resp.GetOk():
			r.fail(n.NodeID+"/HealthCheck", errors.New("ok=false"))
		case resp.GetNodeId() != n.NodeID:
			r.fail(n.NodeID+"/HealthCheck",
				fmt.Errorf("reported node_id %q, config says %q", resp.GetNodeId(), n.NodeID))
		case resp.GetUptimeSeconds() < 0:
			r.fail(n.NodeID+"/HealthCheck",
				fmt.Errorf("negative uptime_seconds=%d", resp.GetUptimeSeconds()))
		default:
			r.pass(n.NodeID+"/HealthCheck",
				fmt.Sprintf("ok=true node_id=%s uptime=%ds", resp.GetNodeId(), resp.GetUptimeSeconds()))
		}
	}()

	// --- Everything else is Phase 1. UNIMPLEMENTED, never a fake success.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		_, err := client.Get(ctx, &nodev1.GetRequest{Key: []byte("smoke")})
		r.wantUnimplemented(n.NodeID+"/Get", err)
	}()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		_, err := client.Put(ctx, &nodev1.PutRequest{Key: []byte("smoke"), Value: []byte("v")})
		r.wantUnimplemented(n.NodeID+"/Put", err)
	}()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		// Server streaming: the status lands on the first Recv, not on the
		// call that opens the stream.
		stream, err := client.PrefixMatch(ctx, &nodev1.PrefixMatchRequest{Prefix: []byte("smoke")})
		if err == nil {
			_, err = stream.Recv()
			if errors.Is(err, io.EOF) {
				err = nil // a clean empty stream is a success, which is wrong here
			}
		}
		r.wantUnimplemented(n.NodeID+"/PrefixMatch", err)
	}()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()
		_, err := client.Capacity(ctx, &nodev1.CapacityRequest{})
		r.wantUnimplemented(n.NodeID+"/Capacity", err)
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
// A success is a failure here: Phase 0 returning fake data from an
// unimplemented RPC is exactly the thing this check exists to catch.
func (r *reporter) wantUnimplemented(name string, err error) {
	if err == nil {
		r.fail(name, errors.New("returned OK; expected UNIMPLEMENTED (a stub must not fake success)"))
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		r.fail(name, fmt.Errorf("non-gRPC error: %v", err))
		return
	}
	if st.Code() != codes.Unimplemented {
		r.fail(name, fmt.Errorf("expected UNIMPLEMENTED, got %s: %s", st.Code(), st.Message()))
		return
	}
	r.pass(name, "UNIMPLEMENTED as expected")
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
	return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
