// Command pulsekv-cluster-bench measures the public PulseKV cluster SDK.
//
// The benchmark is correctness-first: it verifies the live metadata against
// the router, proves representative keys land only on their predicted owners,
// verifies every measured read byte-for-byte, and excludes warmup operations
// from both latency samples and throughput timing.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
	"pulsekv/control/internal/transport"
	sdk "pulsekv/control/pkg/client"
)

const (
	defaultMaxMessageBytes = 8 * 1024 * 1024
	maxRoutingCandidates   = 1_000_000
)

func main() {
	var (
		controlPlane = flag.String("control-plane", "127.0.0.1:7000",
			"comma-separated ClusterMetadataService addresses; any replica answers")
		concurrency     = flag.Int("concurrency", 16, "number of concurrent SDK workers")
		valueSize       = flag.Int("value-size", 16*1024, "value size in bytes")
		keys            = flag.Int("keys", 2048, "working set size, in keys")
		ops             = flag.Int("ops", 50000, "measured operations (excludes warmup)")
		warmupOps       = flag.Int("warmup-ops", 5000, "operations to run and discard before measuring")
		readRatio       = flag.Float64("read-ratio", 0.8, "fraction of mixed operations that are reads")
		seed            = flag.Int64("seed", 1, "PRNG seed for reproducible key and operation selection")
		keyPrefix       = flag.String("key-prefix", "cluster-bench", "key namespace, so benchmark runs can be isolated")
		metadataTimeout = flag.Duration("metadata-timeout", 5*time.Second, "deadline for each metadata RPC")
		rpcTimeout      = flag.Duration("rpc-timeout", 60*time.Second, "deadline for each SDK or direct-node operation")
		refreshInterval = flag.Duration("refresh-interval", time.Second,
			"SDK topology polling interval; zero disables refresh")

		// Phase 9.1. Every one of these defaults to the pre-Phase-9 behaviour,
		// so an existing invocation is unchanged. See llm_shape.go.
		keyDistribution = flag.String("key-distribution", distributionUniform,
			"`uniform` or `zipf`; zipf models LLM serving, where a few shared prefixes take most traffic")
		zipfS = flag.Float64("zipf-s", 1.1,
			"Zipf skew exponent, must exceed 1; larger concentrates traffic harder")
		replicas = flag.Int("replicas", 1,
			"independent SDK clients to spread workers across, modelling that many inference replicas")
		duration = flag.Duration("duration", 0,
			"run for this long instead of --ops; enables interval reporting and bounded latency sampling")
		interval = flag.Duration("interval", 0,
			"emit a time-series row this often; defaults to 30s in --duration mode")
		jsonPath = flag.String("json", "",
			"write the structured run report to this path")
		continueOnError = flag.Bool("continue-on-error", false,
			"count RPC errors and unexpected misses and keep running, instead of stopping the worker; "+
				"value MISMATCHES are never tolerated either way")
		latencySamples = flag.Int("latency-samples", 250000,
			"reservoir size for full-run percentiles in --duration mode")
		metricsListen = flag.String("metrics-listen", "",
			"serve this generator's live workload metrics at http://ADDR/metrics while it runs")
	)
	flag.Parse()

	b := &benchmark{
		controlPlane:    *controlPlane,
		workers:         *concurrency,
		valueSize:       *valueSize,
		keys:            *keys,
		ops:             *ops,
		warmup:          *warmupOps,
		readRatio:       *readRatio,
		seed:            *seed,
		keyPrefix:       *keyPrefix,
		metadataTimeout: *metadataTimeout,
		rpcTimeout:      *rpcTimeout,
		refreshInterval: *refreshInterval,
		distribution:    *keyDistribution,
		zipfS:           *zipfS,
		replicas:        *replicas,
		duration:        *duration,
		interval:        *interval,
		jsonPath:        *jsonPath,
		continueOnError: *continueOnError,
		latencySamples:  *latencySamples,
		metricsListen:   *metricsListen,
	}
	if err := b.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-cluster-bench: %v\n", err)
		os.Exit(2)
	}
	if err := b.run(); err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-cluster-bench: %v\n", err)
		os.Exit(1)
	}
}

type benchmark struct {
	controlPlane    string
	workers         int
	valueSize       int
	keys            int
	ops             int
	warmup          int
	readRatio       float64
	seed            int64
	keyPrefix       string
	metadataTimeout time.Duration
	rpcTimeout      time.Duration
	refreshInterval time.Duration

	// Phase 9.1 additions; see llm_shape.go for why each exists.
	distribution    string
	zipfS           float64
	replicas        int
	duration        time.Duration
	interval        time.Duration
	jsonPath        string
	continueOnError bool
	latencySamples  int
	metricsListen   string

	// measureStarted marks the beginning of the measured phase, so the metrics
	// endpoint can report elapsed time without the populate and warmup phases
	// inflating it.
	measureStarted time.Time

	// Live state for a sustained run. Nil in the fixed --ops mode, which keeps
	// its exact original accounting.
	live      *liveCounters
	buckets   []*intervalBucket
	sampler   *reservoir
	keyCounts []int64
}

func (b *benchmark) validate() error {
	switch {
	case strings.TrimSpace(b.controlPlane) == "":
		return fmt.Errorf("--control-plane must not be empty")
	case b.workers < 1:
		return fmt.Errorf("--concurrency must be positive")
	case b.valueSize < 0:
		return fmt.Errorf("--value-size must not be negative")
	case b.keys < 1:
		return fmt.Errorf("--keys must be positive")
	case b.ops < 1:
		return fmt.Errorf("--ops must be positive")
	case b.warmup < 0:
		return fmt.Errorf("--warmup-ops must not be negative")
	case math.IsNaN(b.readRatio) || b.readRatio < 0 || b.readRatio > 1:
		return fmt.Errorf("--read-ratio must be within [0,1]")
	case strings.TrimSpace(b.keyPrefix) == "":
		return fmt.Errorf("--key-prefix must not be empty")
	case b.metadataTimeout <= 0:
		return fmt.Errorf("--metadata-timeout must be positive")
	case b.rpcTimeout <= 0:
		return fmt.Errorf("--rpc-timeout must be positive")
	case b.refreshInterval < 0:
		return fmt.Errorf("--refresh-interval must not be negative")
	case b.distribution != distributionUniform && b.distribution != distributionZipf:
		return fmt.Errorf("--key-distribution must be %q or %q, got %q",
			distributionUniform, distributionZipf, b.distribution)
	case b.distribution == distributionZipf && b.zipfS <= 1:
		return fmt.Errorf("--zipf-s must be greater than 1, got %g", b.zipfS)
	case b.distribution == distributionZipf && b.keys < 2:
		return fmt.Errorf("--key-distribution=zipf needs at least 2 keys, got %d", b.keys)
	case b.replicas < 1:
		return fmt.Errorf("--replicas must be positive")
	case b.replicas > b.workers:
		return fmt.Errorf("--replicas %d exceeds --concurrency %d: every simulated replica "+
			"needs at least one worker driving it", b.replicas, b.workers)
	case b.duration < 0:
		return fmt.Errorf("--duration must not be negative")
	case b.interval < 0:
		return fmt.Errorf("--interval must not be negative")
	case b.latencySamples < 1000:
		return fmt.Errorf("--latency-samples must be at least 1000")
	default:
		return nil
	}
}

// sustained reports whether this is a duration-bounded run.
func (b *benchmark) sustained() bool { return b.duration > 0 }

// reportInterval is the time-series cadence actually used.
func (b *benchmark) reportInterval() time.Duration {
	if b.interval > 0 {
		return b.interval
	}
	return 30 * time.Second
}

func (b *benchmark) run() (runErr error) {
	// Fetch through an independent connection before constructing the SDK.
	// The routing proof must not trust the SDK's private topology as its oracle.
	topology, err := b.fetchTopology()
	if err != nil {
		return fmt.Errorf("fetch metadata: %w", err)
	}

	// One SDK client per simulated inference replica. Each carries its own
	// topology refresh loop, connection pool, and preferred metadata replica,
	// which is what makes --replicas model separate serving processes rather
	// than just more goroutines. --replicas 1 is the original single client.
	clients := make([]*sdk.Client, 0, b.replicas)
	defer func() {
		for _, client := range clients {
			if err := client.Close(); err != nil && runErr == nil {
				runErr = fmt.Errorf("close SDK client: %w", err)
			}
		}
	}()
	for replica := 0; replica < b.replicas; replica++ {
		client, err := sdk.New(b.controlPlane,
			sdk.WithRefreshInterval(b.refreshInterval),
			sdk.WithRefreshTimeout(b.metadataTimeout))
		if err != nil {
			return fmt.Errorf("create SDK client for replica %d: %w", replica, err)
		}
		clients = append(clients, client)
	}
	cluster := clients[0]

	b.printHeader(topology)
	b.ensureKeyCounts()
	b.serveMetrics()
	if err := b.proveRouting(cluster, topology); err != nil {
		return fmt.Errorf("routing verification: %w", err)
	}

	populateStart := time.Now()
	if err := b.populate(cluster); err != nil {
		return fmt.Errorf("populate: %w", err)
	}
	populateElapsed := time.Since(populateStart)
	fmt.Printf("populate      %d keys in %s  (%.0f keys/s, %s/s)\n",
		b.keys, populateElapsed.Round(time.Millisecond),
		float64(b.keys)/populateElapsed.Seconds(),
		humanBytes(int64(float64(b.keys)*float64(b.valueSize)/populateElapsed.Seconds())))

	if b.warmup > 0 {
		warmupStart := time.Now()
		warmup := summarize(b.mixedAcross(clients, b.warmup, b.seed, false))
		if err := warmup.correctnessError("warmup", b.warmup); err != nil {
			return err
		}
		fmt.Printf("warmup       %d operations in %s; %d reads verified (discarded)\n",
			b.warmup, time.Since(warmupStart).Round(time.Millisecond), warmup.verified)
	}

	if b.sustained() {
		return b.runSustained(clients)
	}

	b.live = &liveCounters{}
	measureStart := time.Now()
	b.measureStarted = measureStart
	measuredResults := b.mixedAcross(clients, b.ops, b.seed+1_000_003, true)
	measureElapsed := time.Since(measureStart)
	measured := summarize(measuredResults)
	fmt.Println()
	b.report(measured, measureElapsed)
	if b.distribution == distributionZipf {
		b.reportConcentration()
	}
	if b.jsonPath != "" {
		if err := b.writeFixedReport(measured, measureElapsed); err != nil {
			return err
		}
	}
	return measured.correctnessError("measurement", b.ops)
}

func (b *benchmark) printHeader(t clusterTopology) {
	alive := 0
	for _, node := range t.nodes {
		if node.alive {
			alive++
		}
	}
	fmt.Println("=== PulseKV cluster benchmark ===")
	fmt.Println()
	fmt.Printf("control plane %s\n", b.controlPlane)
	fmt.Printf("topology      generation %d; %d shards, %d nodes (%d reported alive), %d shard owners\n",
		t.generation, t.shardCount, len(t.nodes), alive, len(t.ownerIDs))
	fmt.Printf("workers       %d\n", b.workers)
	fmt.Printf("value size    %d B\n", b.valueSize)
	fmt.Printf("working set   %d keys (%s)\n", b.keys,
		humanBytes(int64(b.keys)*int64(b.valueSize)))
	fmt.Printf("operations    %d measured, %d warmup (discarded)\n", b.ops, b.warmup)
	fmt.Printf("read ratio    %.2f\n", b.readRatio)
	fmt.Printf("seed          %d\n", b.seed)
	fmt.Printf("key prefix    %s\n", b.keyPrefix)
	fmt.Printf("timeouts      metadata=%s rpc=%s refresh=%s\n",
		b.metadataTimeout, b.rpcTimeout, b.refreshInterval)
	fmt.Printf("path          %s\n", b.pathName())
	fmt.Println()
}

func (b *benchmark) pathName() string {
	if b.valueSize > transport.UnaryValueLimit {
		return "SDK PutChunked; SDK Get with automatic GetChunked fallback"
	}
	return "SDK unary Put/Get"
}

// ---------------------------------------------------------------------------
// Independent metadata and placement verification
// ---------------------------------------------------------------------------

type nodeEndpoint struct {
	address string
	alive   bool
}

type clusterTopology struct {
	generation uint64
	shardCount uint32
	shardMap   map[uint32]string
	nodes      map[string]nodeEndpoint
	nodeIDs    []string
	ownerIDs   []string

	// replicas is the shard's replica set, beyond its primary. The routing
	// proof needs it: a replica legitimately holds a copy of the shard, so
	// "some node that is not the primary must miss" is only a valid assertion
	// about a node that is not a replica either. See holderFreeNode.
	replicas map[uint32][]string
}

// fetchTopology reads the cluster shape from whichever control-plane replica
// answers first.
//
// --control-plane may name several replicas, comma-separated, since Phase 5
// made the metadata plane a Raft group. Any of them serves the same committed
// state; a follower can be a heartbeat behind, never contradictory. One Fetch
// stays on ONE replica so its two RPCs observe the same publisher.
func (b *benchmark) fetchTopology() (clusterTopology, error) {
	addresses := splitEndpoints(b.controlPlane)
	if len(addresses) == 0 {
		return clusterTopology{}, fmt.Errorf("--control-plane listed no usable address")
	}

	var firstErr error
	for _, address := range addresses {
		conn, err := grpc.NewClient(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
				grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
			))
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("create metadata client for %s: %w", address, err)
			}
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), b.metadataTimeout)
		snapshot, err := clustertopology.Fetch(ctx, metadatav1.NewClusterMetadataServiceClient(conn))
		cancel()
		conn.Close()
		if err == nil {
			return validateTopology(snapshot)
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("control plane %s: %w", address, err)
		}
	}
	return clusterTopology{}, firstErr
}

// splitEndpoints parses a comma-separated address list, dropping blanks and
// duplicates.
func splitEndpoints(text string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, raw := range strings.Split(text, ",") {
		address := strings.TrimSpace(raw)
		if address == "" || seen[address] {
			continue
		}
		seen[address] = true
		out = append(out, address)
	}
	return out
}

func validateTopology(snapshot clustertopology.Snapshot) (clusterTopology, error) {
	nodes := make(map[string]nodeEndpoint, len(snapshot.Nodes))
	nodeIDs := make([]string, 0, len(snapshot.Nodes))
	for id, address := range snapshot.Nodes {
		nodes[id] = nodeEndpoint{address: address, alive: true}
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	owners := make(map[string]bool, len(nodes))
	for _, owner := range snapshot.ShardMap {
		owners[owner] = true
	}

	// Phase 2 placement is a pure function of the static node list and shard
	// count. Cross-checking it here prevents a malformed but superficially
	// complete map from becoming the routing proof's own oracle.
	want := router.AssignShards(nodeIDs, snapshot.ShardCount)
	for shard := uint32(0); shard < snapshot.ShardCount; shard++ {
		if snapshot.ShardMap[shard] != want[shard] {
			return clusterTopology{}, fmt.Errorf(
				"metadata shard %d owner is %q, want HRW owner %q",
				shard, snapshot.ShardMap[shard], want[shard])
		}
	}

	ownerIDs := make([]string, 0, len(owners))
	for id := range owners {
		ownerIDs = append(ownerIDs, id)
	}
	sort.Strings(ownerIDs)
	replicas := make(map[uint32][]string, len(snapshot.Owners))
	for shard, placement := range snapshot.Owners {
		replicas[shard] = append([]string(nil), placement.Replicas...)
	}
	return clusterTopology{
		generation: snapshot.Generation,
		shardCount: snapshot.ShardCount,
		shardMap:   snapshot.ShardMap,
		nodes:      nodes,
		nodeIDs:    nodeIDs,
		ownerIDs:   ownerIDs,
		replicas:   replicas,
	}, nil
}

type routingSample struct {
	key   []byte
	shard uint32
	owner string
}

func (b *benchmark) routingSamples(t clusterTopology) ([]routingSample, error) {
	nonce := time.Now().UnixNano()
	found := make(map[string]routingSample, len(t.ownerIDs))
	for candidate := 0; candidate < maxRoutingCandidates && len(found) < len(t.ownerIDs); candidate++ {
		key := []byte(fmt.Sprintf("%s:routing-check:%d:%08d", b.keyPrefix, nonce, candidate))
		owner, ok := router.OwnerForKey(key, t.shardCount, t.shardMap)
		if !ok {
			return nil, fmt.Errorf("candidate key %q has no owner", key)
		}
		if _, exists := found[owner]; !exists {
			found[owner] = routingSample{
				key:   key,
				shard: router.ShardForKey(key, t.shardCount),
				owner: owner,
			}
		}
	}
	if len(found) != len(t.ownerIDs) {
		return nil, fmt.Errorf("found representative keys for %d of %d shard owners after %d candidates",
			len(found), len(t.ownerIDs), maxRoutingCandidates)
	}
	samples := make([]routingSample, 0, len(found))
	for _, owner := range t.ownerIDs {
		samples = append(samples, found[owner])
	}
	return samples, nil
}

type directNodes struct {
	clients map[string]nodev1.NodeServiceClient
	conns   []*grpc.ClientConn
}

func dialDirectNodes(t clusterTopology) (*directNodes, error) {
	direct := &directNodes{
		clients: make(map[string]nodev1.NodeServiceClient, len(t.nodes)),
		conns:   make([]*grpc.ClientConn, 0, len(t.nodes)),
	}
	for _, id := range t.nodeIDs {
		conn, err := grpc.NewClient(t.nodes[id].address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
				grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
			))
		if err != nil {
			_ = direct.Close()
			return nil, fmt.Errorf("create direct client for %s (%s): %w", id, t.nodes[id].address, err)
		}
		direct.conns = append(direct.conns, conn)
		direct.clients[id] = nodev1.NewNodeServiceClient(conn)
	}
	return direct, nil
}

func (d *directNodes) Close() error {
	var firstErr error
	for _, conn := range d.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// holderFreeNode returns a node that holds no copy of shard at all -- not its
// primary, and not one of its replicas.
//
// The distinction is the whole validity of the non-owner assertion below, and
// getting it wrong is a false failure rather than a missed one. This function
// previously returned "the first node that is not the primary", which under
// any replication factor above zero can be one of that shard's replicas. A
// replica is SUPPOSED to hold the value -- that is what Phase 4 built -- so the
// proof would then report correct replication as a routing violation. Measured
// on the eight-node fixture at replication_factor 1, that fired in 1 of 3 runs.
//
// Reports false when every node holds a copy, which is the honest answer for a
// cluster whose replication factor covers it entirely; the caller then skips
// the assertion rather than inventing one.
func (t clusterTopology) holderFreeNode(shard uint32, primary string) (string, bool) {
	holders := map[string]bool{primary: true}
	for _, replica := range t.replicas[shard] {
		holders[replica] = true
	}
	// Prefer another shard owner, then any configured node, so the proof stays
	// meaningful on a cluster with very few shards.
	for _, id := range t.ownerIDs {
		if !holders[id] {
			return id, true
		}
	}
	for _, id := range t.nodeIDs {
		if !holders[id] {
			return id, true
		}
	}
	return "", false
}

func (b *benchmark) proveRouting(cluster *sdk.Client, t clusterTopology) (runErr error) {
	if len(t.nodeIDs) < 2 {
		return fmt.Errorf("at least two nodes are required to prove a non-owner miss")
	}
	samples, err := b.routingSamples(t)
	if err != nil {
		return err
	}
	direct, err := dialDirectNodes(t)
	if err != nil {
		return err
	}
	defer func() {
		if err := direct.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close direct node clients: %w", err)
		}
	}()

	value := make([]byte, b.valueSize)
	for i, sample := range samples {
		valueFor(value, b.keys+i+1)
		ctx, cancel := context.WithTimeout(context.Background(), b.rpcTimeout)
		err := cluster.Put(ctx, sample.key, value)
		cancel()
		if err != nil {
			return fmt.Errorf("SDK Put for owner %s shard %d: %w", sample.owner, sample.shard, err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), b.rpcTimeout)
		got, found, err := transport.Get(ctx, direct.clients[sample.owner], sample.key)
		cancel()
		if err != nil {
			return fmt.Errorf("direct Get from predicted owner %s: %w", sample.owner, err)
		}
		if !found {
			return fmt.Errorf("predicted owner %s missed shard %d key %q", sample.owner, sample.shard, sample.key)
		}
		if !bytes.Equal(got, value) {
			return fmt.Errorf("predicted owner %s returned the wrong value for shard %d", sample.owner, sample.shard)
		}

		other, ok := t.holderFreeNode(sample.shard, sample.owner)
		if !ok {
			// Every node holds a copy of this shard. Saying so is better than
			// either failing or silently printing a proof that did not happen.
			fmt.Printf("routing       shard %-4d owner %-12s hit/value match; "+
				"no node is free of this shard at replication factor %d, non-owner check skipped\n",
				sample.shard, sample.owner, len(t.replicas[sample.shard]))
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), b.rpcTimeout)
		_, found, err = transport.Get(ctx, direct.clients[other], sample.key)
		cancel()
		if err != nil {
			return fmt.Errorf("direct Get from non-holder %s: %w", other, err)
		}
		if found {
			return fmt.Errorf("node %s holds no copy of shard %d (primary %s, replicas %v) "+
				"but returned key %q", other, sample.shard, sample.owner,
				t.replicas[sample.shard], sample.key)
		}
		fmt.Printf("routing       shard %-4d owner %-12s hit/value match; "+
			"%-12s (holds no copy) miss\n", sample.shard, sample.owner, other)
	}
	fmt.Printf("routing       verified one representative key for each of %d shard owner(s)\n\n", len(samples))
	return nil
}

// ---------------------------------------------------------------------------
// Deterministic workload
// ---------------------------------------------------------------------------

// valueFor regenerates the exact bytes stored under key index i, avoiding a
// second in-memory copy of the benchmark's working set.
func valueFor(buf []byte, i int) {
	state := uint64(i)*2862933555777941757 + 3037000493
	if state == 0 {
		state = 1
	}
	var scratch [8]byte
	for off := 0; off < len(buf); off += len(scratch) {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		binary.LittleEndian.PutUint64(scratch[:], state*0x2545F4914F6CDD1D)
		copy(buf[off:], scratch[:])
	}
}

func (b *benchmark) keyFor(i int) []byte {
	return []byte(fmt.Sprintf("%s:%08d", b.keyPrefix, i))
}

func (b *benchmark) put(ctx context.Context, cluster *sdk.Client, key, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, b.rpcTimeout)
	defer cancel()
	return cluster.Put(ctx, key, value)
}

func (b *benchmark) get(ctx context.Context, cluster *sdk.Client, key []byte) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, b.rpcTimeout)
	defer cancel()
	return cluster.Get(ctx, key)
}

func (b *benchmark) populate(cluster *sdk.Client) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		wg       sync.WaitGroup
		firstErr error
		once     sync.Once
	)
	perWorker := (b.keys + b.workers - 1) / b.workers
	for worker := 0; worker < b.workers; worker++ {
		lo := worker * perWorker
		hi := min(lo+perWorker, b.keys)
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			value := make([]byte, b.valueSize)
			for keyIndex := lo; keyIndex < hi; keyIndex++ {
				if ctx.Err() != nil {
					return
				}
				valueFor(value, keyIndex)
				if err := b.put(ctx, cluster, b.keyFor(keyIndex), value); err != nil {
					once.Do(func() {
						firstErr = fmt.Errorf("key %d: %w", keyIndex, err)
						cancel()
					})
					return
				}
			}
		}(lo, hi)
	}
	wg.Wait()
	return firstErr
}

type latencySample struct {
	duration time.Duration
	isRead   bool
}

type workerResult struct {
	samples    []latencySample
	reads      int
	writes     int
	hits       int
	verified   int
	misses     int
	mismatches int
	rpcErrors  int
	firstErr   error
}

// mixedAcross runs the mixed read/write workload, spreading workers across the
// simulated replicas' clients.
//
// A fixed operation count; the sustained form is runSustained below. Both share
// one worker body -- runWorker -- so the correctness rules cannot differ
// between a benchmark run and a soak run.
func (b *benchmark) mixedAcross(clients []*sdk.Client, total int, phaseSeed int64, record bool) []workerResult {
	results := make([]workerResult, b.workers)
	perWorker := (total + b.workers - 1) / b.workers

	var wg sync.WaitGroup
	for worker := 0; worker < b.workers; worker++ {
		operations := min(perWorker, total-worker*perWorker)
		if operations <= 0 {
			continue
		}
		wg.Add(1)
		go func(worker, operations int) {
			defer wg.Done()
			result := &results[worker]
			if record {
				result.samples = make([]latencySample, 0, operations)
			}
			b.runWorker(clients[worker%len(clients)], worker, result, phaseSeed, record,
				func(done int) bool { return done < operations })
		}(worker, operations)
	}
	wg.Wait()
	return results
}

// runWorker is the single load loop, shared by the fixed-count and sustained
// paths.
//
// keepGoing decides when to stop, which is the only thing that differs between
// "50,000 operations" and "four hours". Every correctness rule below -- verify
// every read against the value its key index derives, count a mismatch
// separately from a miss, never treat an unverified read as throughput -- is
// therefore identical in both, which is the point of not forking this file.
func (b *benchmark) runWorker(cluster *sdk.Client, worker int, result *workerResult,
	phaseSeed int64, record bool, keepGoing func(done int) bool) {

	chooser, err := newKeyChooser(b.distribution, b.zipfS, b.keys, phaseSeed+int64(worker)*7919)
	if err != nil {
		// validate() already rejected an unusable configuration; reaching here
		// would be a programming error, not a user one.
		result.recordRPCError(fmt.Errorf("worker %d key chooser: %w", worker, err))
		return
	}
	rng := rand.New(rand.NewSource(phaseSeed + int64(worker)*104729))
	value := make([]byte, b.valueSize)

	for operation := 0; keepGoing(operation); operation++ {
		keyIndex := chooser.next()
		key := b.keyFor(keyIndex)
		isRead := rng.Float64() < b.readRatio
		if b.keyCounts != nil {
			atomic.AddInt64(&b.keyCounts[keyIndex], 1)
		}

		if isRead {
			result.reads++
			b.noteLive(func(l *liveCounters) { l.reads.Add(1) })
			start := time.Now()
			got, found, err := b.get(context.Background(), cluster, key)
			elapsed := time.Since(start)
			b.observe(worker, result, record, latencySample{duration: elapsed, isRead: true})
			if err != nil {
				result.recordRPCError(fmt.Errorf("Get key %d: %w", keyIndex, err))
				b.noteLive(func(l *liveCounters) { l.rpcErrors.Add(1) })
				if b.continueOnError {
					continue
				}
				return
			}
			if !found {
				result.misses++
				b.noteLive(func(l *liveCounters) { l.misses.Add(1) })
				continue
			}
			result.hits++
			b.noteLive(func(l *liveCounters) { l.hits.Add(1) })
			valueFor(value, keyIndex)
			if bytes.Equal(got, value) {
				result.verified++
				b.noteLive(func(l *liveCounters) { l.verified.Add(1) })
			} else {
				// Never tolerated, whatever --continue-on-error says. A miss is
				// a cache that does not have something; a mismatch is a cache
				// that returned the wrong bytes, and no amount of fault
				// injection makes that acceptable.
				result.mismatches++
				b.noteLive(func(l *liveCounters) { l.mismatches.Add(1) })
				result.recordRPCError(fmt.Errorf("key %d returned %d bytes that do not match "+
					"the value written for it", keyIndex, len(got)))
				return
			}
			continue
		}

		result.writes++
		b.noteLive(func(l *liveCounters) { l.writes.Add(1) })
		valueFor(value, keyIndex)
		start := time.Now()
		err := b.put(context.Background(), cluster, key, value)
		elapsed := time.Since(start)
		b.observe(worker, result, record, latencySample{duration: elapsed})
		if err != nil {
			result.recordRPCError(fmt.Errorf("Put key %d: %w", keyIndex, err))
			b.noteLive(func(l *liveCounters) { l.rpcErrors.Add(1) })
			if b.continueOnError {
				continue
			}
			return
		}
	}
}

// observe routes one latency sample to whichever collectors this run uses.
func (b *benchmark) observe(worker int, result *workerResult, record bool, sample latencySample) {
	if record && !b.sustained() {
		result.samples = append(result.samples, sample)
	}
	if b.buckets != nil {
		b.buckets[worker].add(sample.duration, sample.isRead)
	}
	if b.sampler != nil {
		b.sampler.add(sample)
	}
}

func (b *benchmark) noteLive(update func(*liveCounters)) {
	if b.live != nil {
		update(b.live)
	}
}

func (r *workerResult) recordRPCError(err error) {
	r.rpcErrors++
	if r.firstErr == nil {
		r.firstErr = err
	}
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

type resultSummary struct {
	all, reads, writes            []time.Duration
	nReads, nWrites               int
	hits, verified                int
	misses, mismatches, rpcErrors int
	firstErr                      error
}

func summarize(results []workerResult) resultSummary {
	var summary resultSummary
	for i := range results {
		result := &results[i]
		summary.nReads += result.reads
		summary.nWrites += result.writes
		summary.hits += result.hits
		summary.verified += result.verified
		summary.misses += result.misses
		summary.mismatches += result.mismatches
		summary.rpcErrors += result.rpcErrors
		if summary.firstErr == nil {
			summary.firstErr = result.firstErr
		}
		for _, sample := range result.samples {
			summary.all = append(summary.all, sample.duration)
			if sample.isRead {
				summary.reads = append(summary.reads, sample.duration)
			} else {
				summary.writes = append(summary.writes, sample.duration)
			}
		}
	}
	sortDurations(summary.all)
	sortDurations(summary.reads)
	sortDurations(summary.writes)
	return summary
}

func (s resultSummary) correctnessError(stage string, expectedOperations int) error {
	total := s.nReads + s.nWrites
	if s.rpcErrors == 0 && s.misses == 0 && s.mismatches == 0 &&
		s.verified == s.nReads && total == expectedOperations {
		return nil
	}
	if s.firstErr != nil {
		return fmt.Errorf("%s correctness failure: %d/%d operations completed, %d/%d reads verified, "+
			"%d value mismatches, %d unexpected misses, %d RPC errors (first: %v)",
			stage, total, expectedOperations, s.verified, s.nReads,
			s.mismatches, s.misses, s.rpcErrors, s.firstErr)
	}
	return fmt.Errorf("%s correctness failure: %d/%d operations completed, %d/%d reads verified, "+
		"%d value mismatches, %d unexpected misses, %d RPC errors",
		stage, total, expectedOperations, s.verified, s.nReads,
		s.mismatches, s.misses, s.rpcErrors)
}

func (b *benchmark) report(summary resultSummary, elapsed time.Duration) {
	fmt.Printf("%-10s %8s %9s %9s %9s %9s %9s %9s\n",
		"", "count", "min", "p50", "p99", "p999", "max", "mean")
	printRow("read", summary.reads)
	printRow("write", summary.writes)
	printRow("overall", summary.all)

	total := summary.nReads + summary.nWrites
	fmt.Printf("\nthroughput    %.0f ops/s over %s  (%s/s of value payload)\n",
		float64(total)/elapsed.Seconds(), elapsed.Round(time.Millisecond),
		humanBytes(int64(float64(total)*float64(b.valueSize)/elapsed.Seconds())))
	fmt.Printf("verification  %d reads, %d hits, %d verified byte-for-byte, "+
		"%d mismatches, %d unexpected misses, %d RPC errors\n",
		summary.nReads, summary.hits, summary.verified,
		summary.mismatches, summary.misses, summary.rpcErrors)
	fmt.Println()
}

func printRow(name string, sorted []time.Duration) {
	if len(sorted) == 0 {
		fmt.Printf("%-10s %8d %9s %9s %9s %9s %9s %9s\n",
			name, 0, "-", "-", "-", "-", "-", "-")
		return
	}
	var sum time.Duration
	for _, duration := range sorted {
		sum += duration
	}
	fmt.Printf("%-10s %8d %9s %9s %9s %9s %9s %9s\n",
		name, len(sorted),
		milliseconds(sorted[0]),
		milliseconds(percentile(sorted, 0.50)),
		milliseconds(percentile(sorted, 0.99)),
		milliseconds(percentile(sorted, 0.999)),
		milliseconds(sorted[len(sorted)-1]),
		milliseconds(sum/time.Duration(len(sorted))))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(p * float64(len(sorted)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func milliseconds(duration time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(duration.Nanoseconds())/1e6)
}

func sortDurations(durations []time.Duration) {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
}

func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	divisor, exponent := int64(unit), 0
	for value := bytes / unit; value >= unit && exponent < 3; value /= unit {
		divisor *= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(divisor), "KMGT"[exponent])
}
