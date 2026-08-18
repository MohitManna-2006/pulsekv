// Command pulsekv-metrics is PulseKV's Prometheus exporter.
//
// Phase 9.3. Until now every number this system produced was an artefact of a
// test run: the smoke suite's check counts, the chaos report's JSON, a
// benchmark's final table. All of them answer "did that run pass". None answers
// "what is the cluster doing right now", which is the question an operator asks
// and the one a dashboard would need.
//
// WHAT IT SCRAPES, AND WHAT IT MEASURES ITSELF.
//
// Two different kinds of metric, kept clearly apart because they carry
// different weight:
//
//   - SCRAPED. Read straight from the cluster's own RPCs: the metadata group's
//     leader and committed generation, each node's tier occupancy and its
//     lifetime spill/promotion counters, and Phase 6's bulk-vs-gRPC split.
//     These are the cluster's own accounting, not this exporter's opinion.
//   - PROBED. Measured by doing the thing and timing it: a canary write, its
//     arrival at each replica (replication lag), a read back, and the
//     separation of control-plane routing time from data transfer time. A probe
//     is a real client request, so it is honest about what a client would see —
//     and it is also load, which is why the interval is configurable and the
//     probe is skippable.
//
// It is deliberately a SEPARATE PROCESS rather than an endpoint on the control
// plane. Metrics collection that shares a process with the thing it measures
// stops reporting at exactly the moment worth reporting on, and the control
// plane's whole job is to be the thing that stays up. This also keeps the
// Phase 9 diff out of the serving path entirely.
//
// No UI, no dashboard, no storage. It exposes /metrics in Prometheus text
// exposition format and nothing else; anything that can scrape that can consume
// it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/promexport"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
	"pulsekv/control/internal/transport"
)

const defaultMaxMessageBytes = 8 * 1024 * 1024

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		controlPlane = flag.String("control-plane", "127.0.0.1:7000",
			"comma-separated ClusterMetadataService addresses; every replica is scraped separately")
		listen = flag.String("listen", ":9095",
			"address to serve Prometheus metrics on, at /metrics")
		interval = flag.Duration("interval", 5*time.Second,
			"how often to scrape the cluster")
		rpcTimeout = flag.Duration("rpc-timeout", 3*time.Second,
			"deadline for each scrape RPC")
		probe = flag.Bool("probe", true,
			"run the canary probe that measures replication lag and the latency breakdown; "+
				"this issues real writes and reads, so it is load as well as measurement")
		probeInterval = flag.Duration("probe-interval", 15*time.Second,
			"how often to run the canary probe, which is more expensive than a scrape")
		probeValueSize = flag.Int("probe-value-size", 64*1024,
			"canary value size; large enough to be a real transfer, small enough not to distort a run")
		probeKeyPrefix = flag.String("probe-key-prefix", "pulsekv-metrics-probe",
			"key namespace for canary keys, so they cannot collide with a workload")
		replicationLagBudget = flag.Duration("replication-lag-budget", 10*time.Second,
			"how long the probe waits for a replica to receive the canary before recording a timeout")
	)
	flag.Parse()

	addresses := splitList(*controlPlane)
	if len(addresses) == 0 {
		fmt.Fprintln(os.Stderr, "pulsekv-metrics: --control-plane must name at least one replica")
		os.Exit(2)
	}
	if *interval <= 0 || *rpcTimeout <= 0 || *probeInterval <= 0 {
		fmt.Fprintln(os.Stderr, "pulsekv-metrics: intervals and timeouts must be positive")
		os.Exit(2)
	}

	exporter, err := newExporter(exporterConfig{
		controlPlanes:        addresses,
		rpcTimeout:           *rpcTimeout,
		probeEnabled:         *probe,
		probeValueSize:       *probeValueSize,
		probeKeyPrefix:       *probeKeyPrefix,
		replicationLagBudget: *replicationLagBudget,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-metrics: %v\n", err)
		os.Exit(1)
	}
	defer exporter.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go exporter.scrapeLoop(ctx, *interval)
	if *probe {
		go exporter.probeLoop(ctx, *probeInterval)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", exporter.serveMetrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "pulsekv-metrics: try /metrics", http.StatusNotFound)
	})
	server := &http.Server{Addr: *listen, Handler: mux}

	log.Printf("scraping %d control-plane replica(s) every %s; probe=%v every %s",
		len(addresses), *interval, *probe, *probeInterval)
	log.Printf("serving Prometheus metrics on %s/metrics", *listen)

	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "pulsekv-metrics: %v\n", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		log.Printf("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
}

func splitList(text string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, raw := range strings.Split(text, ",") {
		value := strings.TrimSpace(raw)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

// ---------------------------------------------------------------------------
// Exporter
// ---------------------------------------------------------------------------

type exporterConfig struct {
	controlPlanes        []string
	rpcTimeout           time.Duration
	probeEnabled         bool
	probeValueSize       int
	probeKeyPrefix       string
	replicationLagBudget time.Duration
}

type replicaClient struct {
	address  string
	conn     *grpc.ClientConn
	metadata metadatav1.ClusterMetadataServiceClient
}

type exporter struct {
	cfg      exporterConfig
	replicas []replicaClient

	mu    sync.RWMutex
	state metricState

	// nodeConns is a lazily-built pool of data-node connections. Node
	// membership changes at runtime, which is the whole point of the cluster,
	// so the pool is keyed by address and pruned when an address leaves.
	nodeMu    sync.Mutex
	nodeConns map[string]*grpc.ClientConn
}

func newExporter(cfg exporterConfig) (*exporter, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
		),
	}
	e := &exporter{cfg: cfg, nodeConns: make(map[string]*grpc.ClientConn)}
	for _, address := range cfg.controlPlanes {
		conn, err := grpc.NewClient(address, dialOptions...)
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("dial control plane %s: %w", address, err)
		}
		e.replicas = append(e.replicas, replicaClient{
			address:  address,
			conn:     conn,
			metadata: metadatav1.NewClusterMetadataServiceClient(conn),
		})
	}
	e.state.startedAt = time.Now()
	e.state.nodes = make(map[string]nodeMetrics)
	e.state.replicaViews = make(map[string]replicaView)
	e.state.replicationLag = make(map[string]replicaLag)
	return e, nil
}

func (e *exporter) Close() {
	for _, replica := range e.replicas {
		if replica.conn != nil {
			_ = replica.conn.Close()
		}
	}
	e.nodeMu.Lock()
	for _, conn := range e.nodeConns {
		_ = conn.Close()
	}
	e.nodeConns = nil
	e.nodeMu.Unlock()
}

func (e *exporter) nodeClient(address string) (nodev1.NodeServiceClient, error) {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	if conn, ok := e.nodeConns[address]; ok {
		return nodev1.NewNodeServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
		))
	if err != nil {
		return nil, fmt.Errorf("dial node %s: %w", address, err)
	}
	e.nodeConns[address] = conn
	return nodev1.NewNodeServiceClient(conn), nil
}

func (e *exporter) retainNodeConns(live map[string]bool) {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	for address, conn := range e.nodeConns {
		if live[address] {
			continue
		}
		_ = conn.Close()
		delete(e.nodeConns, address)
	}
}

// ---------------------------------------------------------------------------
// Scrape
// ---------------------------------------------------------------------------

func (e *exporter) scrapeLoop(ctx context.Context, interval time.Duration) {
	e.scrapeOnce(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.scrapeOnce(ctx)
		}
	}
}

func (e *exporter) scrapeOnce(ctx context.Context) {
	now := time.Now()
	views := e.scrapeReplicas(ctx)

	e.mu.Lock()
	e.state.scrapes++
	e.state.lastScrape = now
	e.state.replicaViews = views
	e.observeLeadershipLocked(views, now)
	e.observeConvergenceLocked(views, now)
	e.mu.Unlock()

	// Node scraping needs a topology, and any replica that answered has one.
	var nodes map[string]string
	var shardCount uint32
	var owners map[uint32]router.ShardOwners
	for _, view := range views {
		if view.reachable && len(view.nodes) > 0 {
			nodes, shardCount, owners = view.nodes, view.shardCount, view.owners
			break
		}
	}
	if len(nodes) == 0 {
		return
	}
	e.scrapeNodes(ctx, nodes)

	e.mu.Lock()
	e.state.shardCount = shardCount
	e.state.owners = owners
	e.mu.Unlock()
}

// replicaView is one control-plane replica's answer at one instant.
type replicaView struct {
	address     string
	reachable   bool
	err         string
	generation  uint64
	fingerprint string
	leaderID    string
	term        uint64
	liveNodes   int
	shardCount  uint32
	nodes       map[string]string
	owners      map[uint32]router.ShardOwners
	// latency of the metadata pair, which IS the control-plane routing cost a
	// client pays before it can address a single byte of data.
	metadataLatency time.Duration
}

func (e *exporter) scrapeReplicas(ctx context.Context) map[string]replicaView {
	views := make(map[string]replicaView, len(e.replicas))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range e.replicas {
		wg.Add(1)
		go func(replica replicaClient) {
			defer wg.Done()
			view := e.scrapeReplica(ctx, replica)
			mu.Lock()
			views[replica.address] = view
			mu.Unlock()
		}(e.replicas[i])
	}
	wg.Wait()
	return views
}

func (e *exporter) scrapeReplica(ctx context.Context, replica replicaClient) replicaView {
	view := replicaView{address: replica.address}
	rpcCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
	defer cancel()

	start := time.Now()
	snapshot, err := clustertopology.Fetch(rpcCtx, replica.metadata)
	view.metadataLatency = time.Since(start)
	if err != nil {
		// A replica that is still catching up after a restart answers
		// Unavailable here by design (the restart-readiness fix). Reporting it
		// as unreachable is correct: it is up, and it is deliberately not
		// publishing a topology yet.
		view.err = err.Error()
		return view
	}
	view.reachable = true
	view.generation = snapshot.Generation
	view.fingerprint = fmt.Sprintf("%x", snapshot.Fingerprint)
	view.leaderID = snapshot.RaftLeaderID
	view.term = snapshot.RaftTerm
	view.liveNodes = len(snapshot.Nodes)
	view.shardCount = snapshot.ShardCount
	view.nodes = snapshot.Nodes
	view.owners = snapshot.Owners
	return view
}

// nodeMetrics is one data node's scraped state.
type nodeMetrics struct {
	address   string
	reachable bool
	uptime    int64

	residentKeys   uint64
	bytesRAM       uint64
	bytesNVMe      uint64
	keysRAM        uint64
	keysNVMe       uint64
	spills         uint64
	promotions     uint64
	spillErrors    uint64
	evictDrops     uint64
	bulkWrites     uint64
	bulkReads      uint64
	bulkSharedMem  uint64
	bulkFallbacks  uint64
	capacityLatncy time.Duration
}

func (e *exporter) scrapeNodes(ctx context.Context, nodes map[string]string) {
	live := make(map[string]bool, len(nodes))
	for _, address := range nodes {
		live[address] = true
	}
	e.retainNodeConns(live)

	scraped := make(map[string]nodeMetrics, len(nodes))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for id, address := range nodes {
		wg.Add(1)
		go func(id, address string) {
			defer wg.Done()
			metrics := e.scrapeNode(ctx, address)
			mu.Lock()
			scraped[id] = metrics
			mu.Unlock()
		}(id, address)
	}
	wg.Wait()

	e.mu.Lock()
	e.state.nodes = scraped
	e.mu.Unlock()
}

func (e *exporter) scrapeNode(ctx context.Context, address string) nodeMetrics {
	metrics := nodeMetrics{address: address}
	client, err := e.nodeClient(address)
	if err != nil {
		return metrics
	}

	healthCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
	health, err := client.HealthCheck(healthCtx, &nodev1.HealthCheckRequest{})
	cancel()
	if err != nil {
		return metrics
	}
	metrics.reachable = health.GetOk()
	metrics.uptime = health.GetUptimeSeconds()

	capacityCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
	start := time.Now()
	capacity, err := client.Capacity(capacityCtx, &nodev1.CapacityRequest{})
	metrics.capacityLatncy = time.Since(start)
	cancel()
	if err != nil {
		return metrics
	}
	metrics.residentKeys = capacity.GetResidentKeys()
	metrics.bytesRAM = capacity.GetBytesInRamTier()
	metrics.bytesNVMe = capacity.GetBytesInNvmeTier()
	metrics.keysRAM = capacity.GetKeysInRamTier()
	metrics.keysNVMe = capacity.GetKeysInNvmeTier()
	metrics.spills = capacity.GetSpills()
	metrics.promotions = capacity.GetPromotions()
	metrics.spillErrors = capacity.GetSpillErrors()
	metrics.evictDrops = capacity.GetEvictDrops()
	metrics.bulkWrites = capacity.GetBulkWrites()
	metrics.bulkReads = capacity.GetBulkReads()
	metrics.bulkSharedMem = capacity.GetBulkSharedMemoryReads()
	metrics.bulkFallbacks = capacity.GetBulkFallbacks()
	return metrics
}

// ---------------------------------------------------------------------------
// Derived observations: leadership stability and convergence
// ---------------------------------------------------------------------------

// observeLeadershipLocked tracks Raft leader stability across scrapes.
//
// Stability is the metric that matters, not the identity: a cluster whose
// leader is whoever, steadily, is healthy, and one that re-elects every few
// seconds is not, even though both always have a leader.
func (e *exporter) observeLeadershipLocked(views map[string]replicaView, now time.Time) {
	leader, term, agreed := "", uint64(0), true
	first := true
	for _, view := range views {
		if !view.reachable {
			continue
		}
		if first {
			leader, term, first = view.leaderID, view.term, false
			continue
		}
		if view.leaderID != leader || view.term != term {
			agreed = false
		}
	}
	if first {
		// Nobody answered. Recording that is more useful than carrying the
		// previous leader forward as though it were still observed.
		e.state.leaderAgreement = false
		return
	}
	e.state.leaderAgreement = agreed
	if leader != e.state.currentLeader || term != e.state.currentTerm {
		if e.state.currentLeader != "" || e.state.currentTerm != 0 {
			e.state.leaderChanges++
		}
		e.state.currentLeader = leader
		e.state.currentTerm = term
		e.state.leaderSince = now
	}
}

// observeConvergenceLocked measures how long the replicas take to agree on a
// new committed generation.
//
// This is the closest honest proxy for gossip convergence time that an external
// observer can measure. Gossip itself is internal to memberlist and exposes no
// convergence event; what IS observable, and what actually matters downstream,
// is the interval between the first replica publishing a new generation and the
// last one publishing it. Named for what it measures rather than for gossip, so
// nobody reads it as instrumentation it is not.
func (e *exporter) observeConvergenceLocked(views map[string]replicaView, now time.Time) {
	highest := uint64(0)
	reachable := 0
	agreedOnHighest := 0
	for _, view := range views {
		if !view.reachable {
			continue
		}
		reachable++
		if view.generation > highest {
			highest = view.generation
		}
	}
	if reachable == 0 {
		return
	}
	for _, view := range views {
		if view.reachable && view.generation == highest {
			agreedOnHighest++
		}
	}

	if highest > e.state.highestGeneration {
		// A new committed generation just appeared on at least one replica.
		e.state.highestGeneration = highest
		e.state.generationFirstSeen = now
		e.state.generationConverged = agreedOnHighest == reachable
		if e.state.generationConverged {
			e.state.lastConvergence = 0
		}
		e.state.generationChanges++
		return
	}
	if !e.state.generationConverged && agreedOnHighest == reachable && reachable > 1 {
		e.state.generationConverged = true
		e.state.lastConvergence = now.Sub(e.state.generationFirstSeen)
		if e.state.lastConvergence > e.state.maxConvergence {
			e.state.maxConvergence = e.state.lastConvergence
		}
		e.state.convergenceSamples++
		e.state.convergenceTotal += e.state.lastConvergence
	}
}

// ---------------------------------------------------------------------------
// Probe: replication lag and the latency breakdown
// ---------------------------------------------------------------------------

type replicaLag struct {
	seconds   float64
	timedOut  bool
	observed  time.Time
	forShard  uint32
	forHolder string
}

func (e *exporter) probeLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.probeOnce(ctx)
		}
	}
}

// probeOnce writes one canary and measures what a client would actually see.
//
// The three timings it separates are the per-phase breakdown step 9.3 asks for,
// and they are separated by construction rather than by estimate: the metadata
// fetch is a distinct RPC pair to the control plane, the write and read are
// distinct RPCs to a data node, and the replication lag is the wall-clock gap
// between the primary acknowledging and a replica being able to serve it.
func (e *exporter) probeOnce(ctx context.Context) {
	e.mu.RLock()
	var view replicaView
	for _, candidate := range e.state.replicaViews {
		if candidate.reachable && len(candidate.nodes) > 0 {
			view = candidate
			break
		}
	}
	e.mu.RUnlock()
	if !view.reachable || view.shardCount == 0 {
		return
	}

	// A fresh key each round, so a hit can only mean this round's write
	// arrived — a reused key would be satisfied by the previous round.
	key := []byte(fmt.Sprintf("%s:%d", e.cfg.probeKeyPrefix, time.Now().UnixNano()))
	shard := router.ShardForKey(key, view.shardCount)
	placement, ok := view.owners[shard]
	if !ok {
		return
	}
	primaryAddress := view.nodes[placement.Primary]
	if primaryAddress == "" {
		return
	}
	primary, err := e.nodeClient(primaryAddress)
	if err != nil {
		return
	}

	value := make([]byte, e.cfg.probeValueSize)
	for i := range value {
		value[i] = byte(i * 31)
	}

	writeCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
	writeStart := time.Now()
	writeErr := transport.Put(writeCtx, primary, key, value)
	writeLatency := time.Since(writeStart)
	cancel()

	e.mu.Lock()
	e.state.probes++
	e.state.probeMetadataLatency = view.metadataLatency
	e.state.probeWriteLatency = writeLatency
	if writeErr != nil {
		e.state.probeWriteErrors++
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	readCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
	readStart := time.Now()
	got, found, readErr := transport.Get(readCtx, primary, key)
	readLatency := time.Since(readStart)
	cancel()

	e.mu.Lock()
	e.state.probeReadLatency = readLatency
	switch {
	case readErr != nil:
		e.state.probeReadErrors++
	case !found:
		e.state.probeMisses++
	case len(got) != len(value):
		e.state.probeMismatches++
	default:
		e.state.probeHits++
	}
	e.mu.Unlock()

	// Replication lag, measured per replica against its own address. Reads go
	// to primaries only in this system, so a hit here can only mean Phase 4's
	// async forwarding actually delivered the bytes.
	for _, replicaID := range placement.Replicas {
		address := view.nodes[replicaID]
		if address == "" {
			continue
		}
		lag := e.measureReplicationLag(ctx, address, key, writeStart)
		lag.forShard = shard
		lag.forHolder = replicaID
		e.mu.Lock()
		e.state.replicationLag[replicaID] = lag
		e.mu.Unlock()
	}
}

func (e *exporter) measureReplicationLag(ctx context.Context, address string, key []byte,
	writtenAt time.Time) replicaLag {

	deadline := time.Now().Add(e.cfg.replicationLagBudget)
	client, err := e.nodeClient(address)
	if err != nil {
		return replicaLag{timedOut: true, observed: time.Now()}
	}
	for time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, e.cfg.rpcTimeout)
		_, found, err := transport.Get(pollCtx, client, key)
		cancel()
		if err == nil && found {
			return replicaLag{
				seconds:  time.Since(writtenAt).Seconds(),
				observed: time.Now(),
			}
		}
		select {
		case <-ctx.Done():
			return replicaLag{timedOut: true, observed: time.Now()}
		case <-time.After(20 * time.Millisecond):
		}
	}
	return replicaLag{timedOut: true, observed: time.Now()}
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type metricState struct {
	startedAt  time.Time
	lastScrape time.Time
	scrapes    uint64

	replicaViews map[string]replicaView
	nodes        map[string]nodeMetrics
	shardCount   uint32
	owners       map[uint32]router.ShardOwners

	currentLeader   string
	currentTerm     uint64
	leaderSince     time.Time
	leaderChanges   uint64
	leaderAgreement bool

	highestGeneration   uint64
	generationFirstSeen time.Time
	generationConverged bool
	generationChanges   uint64
	lastConvergence     time.Duration
	maxConvergence      time.Duration
	convergenceSamples  uint64
	convergenceTotal    time.Duration

	probes               uint64
	probeHits            uint64
	probeMisses          uint64
	probeMismatches      uint64
	probeWriteErrors     uint64
	probeReadErrors      uint64
	probeMetadataLatency time.Duration
	probeWriteLatency    time.Duration
	probeReadLatency     time.Duration
	replicationLag       map[string]replicaLag
}

// ---------------------------------------------------------------------------
// Prometheus exposition
// ---------------------------------------------------------------------------

func (e *exporter) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	e.mu.RLock()
	state := e.snapshotLocked()
	e.mu.RUnlock()

	w.Header().Set("Content-Type", promexport.ContentType)
	out := &promexport.Writer{}
	writeState(out, state, e.cfg)
	_, _ = w.Write([]byte(out.String()))
}

// snapshotLocked copies the maps so exposition never formats a map another
// goroutine is rewriting.
func (e *exporter) snapshotLocked() metricState {
	state := e.state
	state.replicaViews = make(map[string]replicaView, len(e.state.replicaViews))
	for k, v := range e.state.replicaViews {
		state.replicaViews[k] = v
	}
	state.nodes = make(map[string]nodeMetrics, len(e.state.nodes))
	for k, v := range e.state.nodes {
		state.nodes[k] = v
	}
	state.replicationLag = make(map[string]replicaLag, len(e.state.replicationLag))
	for k, v := range e.state.replicationLag {
		state.replicationLag[k] = v
	}
	return state
}

func writeState(out *promexport.Writer, s metricState, cfg exporterConfig) {
	now := time.Now()

	out.Help("pulsekv_exporter_uptime_seconds", "gauge",
		"Seconds since this exporter started.")
	out.Metric("pulsekv_exporter_uptime_seconds", nil, now.Sub(s.startedAt).Seconds())

	out.Help("pulsekv_scrapes_total", "counter", "Cluster scrapes completed.")
	out.Metric("pulsekv_scrapes_total", nil, float64(s.scrapes))

	// --- Control plane: Raft leader stability ------------------------------
	out.Help("pulsekv_control_plane_up", "gauge",
		"1 when a control-plane replica published a topology on the last scrape. A replica that "+
			"is running but still catching up after a restart reports 0 here, by design.")
	out.Help("pulsekv_control_plane_topology_generation", "gauge",
		"Committed metadata generation this replica is serving.")
	out.Help("pulsekv_control_plane_live_nodes", "gauge",
		"Live data nodes in this replica's published membership.")
	out.Help("pulsekv_control_plane_raft_term", "gauge",
		"Raft term as this replica understands it.")
	out.Help("pulsekv_control_plane_metadata_latency_seconds", "gauge",
		"Time for one coherent GetNodeList+GetShardMap pair against this replica. This is the "+
			"control-plane routing cost a client pays before addressing any data.")

	addresses := sortedKeys(s.replicaViews)
	for _, address := range addresses {
		view := s.replicaViews[address]
		labels := map[string]string{"replica": address}
		out.Metric("pulsekv_control_plane_up", labels, promexport.Bool(view.reachable))
		out.Metric("pulsekv_control_plane_metadata_latency_seconds", labels,
			view.metadataLatency.Seconds())
		if !view.reachable {
			continue
		}
		out.Metric("pulsekv_control_plane_topology_generation", labels, float64(view.generation))
		out.Metric("pulsekv_control_plane_live_nodes", labels, float64(view.liveNodes))
		out.Metric("pulsekv_control_plane_raft_term", labels, float64(view.term))
	}

	out.Help("pulsekv_raft_leader", "gauge",
		"1 for the replica ID the reachable replicas currently agree is leader.")
	if s.currentLeader != "" {
		out.Metric("pulsekv_raft_leader", map[string]string{"leader": s.currentLeader}, 1)
	}
	out.Help("pulsekv_raft_term", "gauge", "Current agreed Raft term.")
	out.Metric("pulsekv_raft_term", nil, float64(s.currentTerm))
	out.Help("pulsekv_raft_leader_changes_total", "counter",
		"Leader or term changes observed since this exporter started.")
	out.Metric("pulsekv_raft_leader_changes_total", nil, float64(s.leaderChanges))
	out.Help("pulsekv_raft_leader_stable_seconds", "gauge",
		"Seconds the current leader and term have held. The stability metric: a cluster that "+
			"always has a leader but re-elects constantly is not healthy.")
	if !s.leaderSince.IsZero() {
		out.Metric("pulsekv_raft_leader_stable_seconds", nil, now.Sub(s.leaderSince).Seconds())
	}
	out.Help("pulsekv_raft_leader_agreement", "gauge",
		"1 when every reachable replica names the same leader and term.")
	out.Metric("pulsekv_raft_leader_agreement", nil, promexport.Bool(s.leaderAgreement))

	// --- Convergence -------------------------------------------------------
	out.Help("pulsekv_metadata_generation_changes_total", "counter",
		"Committed metadata generations observed since this exporter started.")
	out.Metric("pulsekv_metadata_generation_changes_total", nil, float64(s.generationChanges))
	out.Help("pulsekv_metadata_convergence_seconds", "gauge",
		"Time from the first replica publishing a new committed generation to the last one "+
			"publishing it. Gossip exposes no convergence event of its own; this is the "+
			"externally observable convergence that actually reaches clients.")
	out.Metric("pulsekv_metadata_convergence_seconds", nil, s.lastConvergence.Seconds())
	out.Help("pulsekv_metadata_convergence_seconds_max", "gauge",
		"Largest convergence interval observed since this exporter started.")
	out.Metric("pulsekv_metadata_convergence_seconds_max", nil, s.maxConvergence.Seconds())
	out.Help("pulsekv_metadata_convergence_samples_total", "counter",
		"Convergence intervals measured.")
	out.Metric("pulsekv_metadata_convergence_samples_total", nil, float64(s.convergenceSamples))
	out.Help("pulsekv_metadata_converged", "gauge",
		"1 when every reachable replica is serving the highest committed generation seen.")
	out.Metric("pulsekv_metadata_converged", nil, promexport.Bool(s.generationConverged))

	// --- Data nodes: tiering ----------------------------------------------
	out.Help("pulsekv_node_up", "gauge", "1 when the node answered HealthCheck.")
	out.Help("pulsekv_node_uptime_seconds", "gauge", "Node process uptime.")
	out.Help("pulsekv_node_resident_keys", "gauge", "Keys this node answers for, across both tiers.")
	out.Help("pulsekv_node_tier_bytes", "gauge", "Value bytes held, by tier.")
	out.Help("pulsekv_node_tier_keys", "gauge", "Keys held, by tier.")
	out.Help("pulsekv_node_spills_total", "counter", "Values moved RAM -> NVMe since node start.")
	out.Help("pulsekv_node_promotions_total", "counter", "Values moved NVMe -> RAM since node start.")
	out.Help("pulsekv_node_spill_errors_total", "counter",
		"Spill writes that failed. The NVMe tier degrading, not the node failing: the entry is "+
			"dropped and the node keeps serving.")
	out.Help("pulsekv_node_evict_drops_total", "counter",
		"Entries dropped under memory pressure rather than spilled.")
	out.Help("pulsekv_node_bulk_transfers_total", "counter",
		"Phase 6 bulk transport traffic by kind. kind=fallback counts requests that could not "+
			"use it and went back to gRPC, which is normal operation: the bulk path is never required.")
	out.Help("pulsekv_node_capacity_latency_seconds", "gauge",
		"Time for this node's Capacity RPC.")

	for _, id := range sortedKeys(s.nodes) {
		node := s.nodes[id]
		labels := map[string]string{"node": id, "address": node.address}
		out.Metric("pulsekv_node_up", labels, promexport.Bool(node.reachable))
		if !node.reachable {
			continue
		}
		out.Metric("pulsekv_node_uptime_seconds", labels, float64(node.uptime))
		out.Metric("pulsekv_node_resident_keys", labels, float64(node.residentKeys))
		out.Metric("pulsekv_node_capacity_latency_seconds", labels, node.capacityLatncy.Seconds())
		out.Metric("pulsekv_node_tier_bytes", promexport.With(labels, "tier", "ram"), float64(node.bytesRAM))
		out.Metric("pulsekv_node_tier_bytes", promexport.With(labels, "tier", "nvme"), float64(node.bytesNVMe))
		out.Metric("pulsekv_node_tier_keys", promexport.With(labels, "tier", "ram"), float64(node.keysRAM))
		out.Metric("pulsekv_node_tier_keys", promexport.With(labels, "tier", "nvme"), float64(node.keysNVMe))
		out.Metric("pulsekv_node_spills_total", labels, float64(node.spills))
		out.Metric("pulsekv_node_promotions_total", labels, float64(node.promotions))
		out.Metric("pulsekv_node_spill_errors_total", labels, float64(node.spillErrors))
		out.Metric("pulsekv_node_evict_drops_total", labels, float64(node.evictDrops))
		out.Metric("pulsekv_node_bulk_transfers_total", promexport.With(labels, "kind", "write"), float64(node.bulkWrites))
		out.Metric("pulsekv_node_bulk_transfers_total", promexport.With(labels, "kind", "read"), float64(node.bulkReads))
		out.Metric("pulsekv_node_bulk_transfers_total", promexport.With(labels, "kind", "shared_memory_read"), float64(node.bulkSharedMem))
		out.Metric("pulsekv_node_bulk_transfers_total", promexport.With(labels, "kind", "fallback"), float64(node.bulkFallbacks))
	}

	// --- Probe: hit rate, replication lag, latency breakdown ---------------
	if !cfg.probeEnabled {
		out.Help("pulsekv_probe_enabled", "gauge",
			"0 when --probe=false: the hit-rate, replication-lag and latency-breakdown metrics "+
				"below are then absent rather than zero.")
		out.Metric("pulsekv_probe_enabled", nil, 0)
		return
	}
	out.Help("pulsekv_probe_enabled", "gauge", "1 when the canary probe is running.")
	out.Metric("pulsekv_probe_enabled", nil, 1)

	out.Help("pulsekv_probe_total", "counter", "Canary probes attempted.")
	out.Metric("pulsekv_probe_total", nil, float64(s.probes))
	out.Help("pulsekv_probe_results_total", "counter",
		"Canary probe outcomes. hit/miss is the cache hit rate this exporter can measure "+
			"directly; an application's own hit rate is its to report, and the Phase 7/8 demo "+
			"scripts report theirs.")
	out.Metric("pulsekv_probe_results_total", map[string]string{"result": "hit"}, float64(s.probeHits))
	out.Metric("pulsekv_probe_results_total", map[string]string{"result": "miss"}, float64(s.probeMisses))
	out.Metric("pulsekv_probe_results_total", map[string]string{"result": "mismatch"}, float64(s.probeMismatches))
	out.Metric("pulsekv_probe_results_total", map[string]string{"result": "write_error"}, float64(s.probeWriteErrors))
	out.Metric("pulsekv_probe_results_total", map[string]string{"result": "read_error"}, float64(s.probeReadErrors))

	out.Help("pulsekv_probe_latency_seconds", "gauge",
		"Latest canary latency, split by phase. metadata is control-plane routing; write and "+
			"read are data transfer to and from the owning node. The split is by construction, "+
			"not by estimate: they are separate RPCs to separate processes.")
	out.Metric("pulsekv_probe_latency_seconds", map[string]string{"phase": "metadata"}, s.probeMetadataLatency.Seconds())
	out.Metric("pulsekv_probe_latency_seconds", map[string]string{"phase": "write"}, s.probeWriteLatency.Seconds())
	out.Metric("pulsekv_probe_latency_seconds", map[string]string{"phase": "read"}, s.probeReadLatency.Seconds())

	out.Help("pulsekv_replication_lag_seconds", "gauge",
		"Time from the primary acknowledging a canary write to this replica being able to serve "+
			"it, read directly against the replica's own address.")
	out.Help("pulsekv_replication_lag_timeout", "gauge",
		"1 when the replica did not receive the last canary within the lag budget.")
	for _, id := range sortedKeys(s.replicationLag) {
		lag := s.replicationLag[id]
		labels := map[string]string{"node": id}
		out.Metric("pulsekv_replication_lag_timeout", labels, promexport.Bool(lag.timedOut))
		if !lag.timedOut {
			out.Metric("pulsekv_replication_lag_seconds", labels, lag.seconds)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
