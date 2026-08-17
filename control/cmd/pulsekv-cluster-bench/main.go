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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
	"pulsekv/control/internal/transport"
	sdk "pulsekv/control/pkg/client"
)

const (
	defaultMaxMessageBytes = 8 * 1024 * 1024
	maxRoutingCandidates   = 1_000_000
)

func main() {
	var (
		controlPlane    = flag.String("control-plane", "127.0.0.1:7000", "ClusterMetadataService address")
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
	default:
		return nil
	}
}

func (b *benchmark) run() (runErr error) {
	// Fetch through an independent connection before constructing the SDK.
	// The routing proof must not trust the SDK's private topology as its oracle.
	topology, err := b.fetchTopology()
	if err != nil {
		return fmt.Errorf("fetch metadata: %w", err)
	}

	cluster, err := sdk.New(b.controlPlane,
		sdk.WithRefreshInterval(0),
		sdk.WithRefreshTimeout(b.metadataTimeout))
	if err != nil {
		return fmt.Errorf("create SDK client: %w", err)
	}
	defer func() {
		if err := cluster.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close SDK client: %w", err)
		}
	}()

	b.printHeader(topology)
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
		warmup := summarize(b.mixed(cluster, b.warmup, b.seed, false))
		if err := warmup.correctnessError("warmup", b.warmup); err != nil {
			return err
		}
		fmt.Printf("warmup       %d operations in %s; %d reads verified (discarded)\n",
			b.warmup, time.Since(warmupStart).Round(time.Millisecond), warmup.verified)
	}

	measureStart := time.Now()
	measuredResults := b.mixed(cluster, b.ops, b.seed+1_000_003, true)
	measureElapsed := time.Since(measureStart)
	measured := summarize(measuredResults)
	fmt.Println()
	b.report(measured, measureElapsed)
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
	fmt.Printf("topology      %d shards, %d nodes (%d reported alive), %d shard owners\n",
		t.shardCount, len(t.nodes), alive, len(t.ownerIDs))
	fmt.Printf("workers       %d\n", b.workers)
	fmt.Printf("value size    %d B\n", b.valueSize)
	fmt.Printf("working set   %d keys (%s)\n", b.keys,
		humanBytes(int64(b.keys)*int64(b.valueSize)))
	fmt.Printf("operations    %d measured, %d warmup (discarded)\n", b.ops, b.warmup)
	fmt.Printf("read ratio    %.2f\n", b.readRatio)
	fmt.Printf("seed          %d\n", b.seed)
	fmt.Printf("key prefix    %s\n", b.keyPrefix)
	fmt.Printf("timeouts      metadata=%s rpc=%s\n", b.metadataTimeout, b.rpcTimeout)
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
	shardCount uint32
	shardMap   map[uint32]string
	nodes      map[string]nodeEndpoint
	nodeIDs    []string
	ownerIDs   []string
}

func (b *benchmark) fetchTopology() (clusterTopology, error) {
	conn, err := grpc.NewClient(b.controlPlane,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
		))
	if err != nil {
		return clusterTopology{}, fmt.Errorf("create metadata client: %w", err)
	}
	defer conn.Close()

	metadata := metadatav1.NewClusterMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), b.metadataTimeout)
	nodes, err := metadata.GetNodeList(ctx, &metadatav1.GetNodeListRequest{})
	cancel()
	if err != nil {
		return clusterTopology{}, fmt.Errorf("GetNodeList: %w", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), b.metadataTimeout)
	shards, err := metadata.GetShardMap(ctx, &metadatav1.GetShardMapRequest{})
	cancel()
	if err != nil {
		return clusterTopology{}, fmt.Errorf("GetShardMap: %w", err)
	}
	return validateTopology(nodes.GetNodes(), shards.GetShardToNodeId())
}

func validateTopology(nodeList []*metadatav1.NodeInfo, shardMap map[uint32]string) (clusterTopology, error) {
	if len(nodeList) == 0 {
		return clusterTopology{}, fmt.Errorf("metadata returned no nodes")
	}
	nodes := make(map[string]nodeEndpoint, len(nodeList))
	addresses := make(map[string]string, len(nodeList))
	nodeIDs := make([]string, 0, len(nodeList))
	for i, node := range nodeList {
		if node == nil {
			return clusterTopology{}, fmt.Errorf("metadata node %d is nil", i)
		}
		id, address := node.GetNodeId(), node.GetAddress()
		if id == "" || address == "" {
			return clusterTopology{}, fmt.Errorf("metadata node %d has an empty ID or address", i)
		}
		if _, exists := nodes[id]; exists {
			return clusterTopology{}, fmt.Errorf("metadata returned duplicate node ID %q", id)
		}
		if previous, exists := addresses[address]; exists {
			return clusterTopology{}, fmt.Errorf("metadata nodes %q and %q share address %q", previous, id, address)
		}
		nodes[id] = nodeEndpoint{address: address, alive: node.GetAlive()}
		addresses[address] = id
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	if len(shardMap) == 0 {
		return clusterTopology{}, fmt.Errorf("metadata returned an empty shard map")
	}
	if uint64(len(shardMap)) > uint64(^uint32(0)) {
		return clusterTopology{}, fmt.Errorf("metadata shard map is too large")
	}
	shardCount := uint32(len(shardMap))
	owners := make(map[string]bool, len(nodes))
	for shard := uint32(0); shard < shardCount; shard++ {
		owner, ok := shardMap[shard]
		if !ok {
			return clusterTopology{}, fmt.Errorf("metadata shard map is missing shard %d", shard)
		}
		if _, ok := nodes[owner]; !ok {
			return clusterTopology{}, fmt.Errorf("metadata shard %d has unknown owner %q", shard, owner)
		}
		owners[owner] = true
	}

	// Phase 2 placement is a pure function of the static node list and shard
	// count. Cross-checking it here prevents a malformed but superficially
	// complete map from becoming the routing proof's own oracle.
	want := router.AssignShards(nodeIDs, shardCount)
	for shard := uint32(0); shard < shardCount; shard++ {
		if shardMap[shard] != want[shard] {
			return clusterTopology{}, fmt.Errorf(
				"metadata shard %d owner is %q, want HRW owner %q", shard, shardMap[shard], want[shard])
		}
	}

	ownerIDs := make([]string, 0, len(owners))
	for id := range owners {
		ownerIDs = append(ownerIDs, id)
	}
	sort.Strings(ownerIDs)
	return clusterTopology{
		shardCount: shardCount,
		shardMap:   shardMap,
		nodes:      nodes,
		nodeIDs:    nodeIDs,
		ownerIDs:   ownerIDs,
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

func (t clusterTopology) otherNode(owner string) (string, bool) {
	// Prefer another shard owner, then fall back to any configured node. The
	// latter keeps the proof meaningful for a cluster with only one shard.
	for _, id := range t.ownerIDs {
		if id != owner {
			return id, true
		}
	}
	for _, id := range t.nodeIDs {
		if id != owner {
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

		other, ok := t.otherNode(sample.owner)
		if !ok {
			return fmt.Errorf("no non-owner node is available for owner %s", sample.owner)
		}
		ctx, cancel = context.WithTimeout(context.Background(), b.rpcTimeout)
		_, found, err = transport.Get(ctx, direct.clients[other], sample.key)
		cancel()
		if err != nil {
			return fmt.Errorf("direct Get from non-owner %s: %w", other, err)
		}
		if found {
			return fmt.Errorf("non-owner %s unexpectedly stored shard %d key %q owned by %s",
				other, sample.shard, sample.key, sample.owner)
		}
		fmt.Printf("routing       shard %-4d owner %-12s hit/value match; non-owner %-12s miss\n",
			sample.shard, sample.owner, other)
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

func (b *benchmark) mixed(cluster *sdk.Client, total int, phaseSeed int64, record bool) []workerResult {
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
			rng := rand.New(rand.NewSource(phaseSeed + int64(worker)*7919))
			value := make([]byte, b.valueSize)

			for operation := 0; operation < operations; operation++ {
				keyIndex := rng.Intn(b.keys)
				key := b.keyFor(keyIndex)
				isRead := rng.Float64() < b.readRatio

				if isRead {
					result.reads++
					start := time.Now()
					got, found, err := b.get(context.Background(), cluster, key)
					if record {
						result.samples = append(result.samples, latencySample{duration: time.Since(start), isRead: true})
					}
					if err != nil {
						result.recordRPCError(fmt.Errorf("Get key %d: %w", keyIndex, err))
						return
					}
					if !found {
						result.misses++
						continue
					}
					result.hits++
					valueFor(value, keyIndex)
					if bytes.Equal(got, value) {
						result.verified++
					} else {
						result.mismatches++
					}
					continue
				}

				result.writes++
				valueFor(value, keyIndex)
				start := time.Now()
				err := b.put(context.Background(), cluster, key, value)
				if record {
					result.samples = append(result.samples, latencySample{duration: time.Since(start)})
				}
				if err != nil {
					result.recordRPCError(fmt.Errorf("Put key %d: %w", keyIndex, err))
					return
				}
			}
		}(worker, operations)
	}
	wg.Wait()
	return results
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
