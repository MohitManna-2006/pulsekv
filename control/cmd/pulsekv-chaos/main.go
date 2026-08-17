// Command pulsekv-chaos is the passive Phase 3 correctness verifier.
//
// It never starts, stops, or signals cluster processes. deploy/chaos-test.sh
// owns lifecycle mutation; this command seeds stable data, sustains reads, and
// proves each observed removal/rejoin topology epoch before advancing an atomic
// progress file.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
	sdk "pulsekv/control/pkg/client"
)

const (
	maxMessageBytes = 8 * 1024 * 1024
	maxKeyAttempts  = 1_000_000
)

type settings struct {
	controlPlane      string
	target            string
	cycles            int
	seed              int64
	readyFile         string
	reportPath        string
	transitionTimeout time.Duration
	pollInterval      time.Duration
	refreshInterval   time.Duration
	rpcTimeout        time.Duration
	workers           int
}

func main() {
	var cfg settings
	flag.StringVar(&cfg.controlPlane, "control-plane", "127.0.0.1:7000", "ClusterMetadataService address")
	flag.StringVar(&cfg.target, "target", "", "node ID that will be repeatedly removed and rejoined")
	flag.IntVar(&cfg.cycles, "cycles", 3, "number of target removal/rejoin cycles")
	flag.Int64Var(&cfg.seed, "seed", 7, "non-negative deterministic workload seed")
	flag.StringVar(&cfg.readyFile, "ready-file", "run/chaos/ready", "atomic progress-file path")
	flag.StringVar(&cfg.reportPath, "report", "run/chaos/report.json", "atomic JSON report path")
	flag.DurationVar(&cfg.transitionTimeout, "transition-timeout", 15*time.Second, "deadline for each expected topology transition and SDK convergence proof")
	flag.DurationVar(&cfg.pollInterval, "poll-interval", 25*time.Millisecond, "coherent-topology polling interval")
	flag.DurationVar(&cfg.refreshInterval, "refresh-interval", 50*time.Millisecond, "SDK metadata refresh interval")
	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", 2*time.Second, "deadline for each metadata or data RPC")
	flag.IntVar(&cfg.workers, "workers", 4, "concurrent stable-key read workers")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-chaos: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, runErr := run(ctx, cfg)
	if runErr != nil {
		report.Success = false
		report.Error = runErr.Error()
	} else {
		report.Success = true
	}
	if err := writeJSONAtomic(cfg.reportPath, report); err != nil {
		if runErr == nil {
			runErr = fmt.Errorf("write JSON report: %w", err)
		} else {
			runErr = errors.Join(runErr, fmt.Errorf("write JSON report: %w", err))
		}
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-chaos: FAILED: %v\n", runErr)
		os.Exit(1)
	}
	fmt.Printf("PASS: verified %d/%d transitions; stable reads=%d; report=%s\n",
		report.TransitionsVerified, report.TransitionsExpected,
		report.Load.Verified, cfg.reportPath)
}

func (c settings) validate() error {
	switch {
	case strings.TrimSpace(c.controlPlane) == "":
		return errors.New("--control-plane must not be empty")
	case strings.TrimSpace(c.target) == "":
		return errors.New("--target must not be empty")
	case c.cycles <= 0:
		return errors.New("--cycles must be positive")
	case c.seed < 0:
		return errors.New("--seed must not be negative")
	case strings.TrimSpace(c.readyFile) == "":
		return errors.New("--ready-file must not be empty")
	case strings.TrimSpace(c.reportPath) == "":
		return errors.New("--report must not be empty")
	case filepath.Clean(c.readyFile) == filepath.Clean(c.reportPath):
		return errors.New("--ready-file and --report must differ")
	case c.transitionTimeout <= 0:
		return errors.New("--transition-timeout must be positive")
	case c.pollInterval <= 0:
		return errors.New("--poll-interval must be positive")
	case c.refreshInterval <= 0:
		return errors.New("--refresh-interval must be positive")
	case c.rpcTimeout <= 0:
		return errors.New("--rpc-timeout must be positive")
	case c.workers <= 0:
		return errors.New("--workers must be positive")
	default:
		return nil
	}
}

type runReport struct {
	Version             int                `json:"version"`
	Success             bool               `json:"success"`
	Error               string             `json:"error,omitempty"`
	ControlPlane        string             `json:"control_plane"`
	Target              string             `json:"target"`
	Cycles              int                `json:"cycles"`
	Seed                int64              `json:"seed"`
	Workers             int                `json:"workers"`
	StartedAt           time.Time          `json:"started_at"`
	CompletedAt         time.Time          `json:"completed_at"`
	ElapsedMillis       int64              `json:"elapsed_millis"`
	BaselineGeneration  uint64             `json:"baseline_generation"`
	BaselineFingerprint string             `json:"baseline_fingerprint,omitempty"`
	BaselineNodes       []string           `json:"baseline_nodes"`
	ShardCount          uint32             `json:"shard_count"`
	TargetOwnedShards   int                `json:"target_owned_shards"`
	StableKeys          int                `json:"stable_keys"`
	TransitionsExpected int                `json:"transitions_expected"`
	TransitionsVerified int                `json:"transitions_verified"`
	BaselineProof       epochProofReport   `json:"baseline_proof"`
	Transitions         []transitionReport `json:"transitions"`
	Load                loadReport         `json:"load"`
}

type transitionReport struct {
	Index          int              `json:"index"`
	Cycle          int              `json:"cycle"`
	Kind           string           `json:"kind"`
	FromGeneration uint64           `json:"from_generation"`
	ToGeneration   uint64           `json:"to_generation"`
	ElapsedMillis  int64            `json:"elapsed_millis"`
	MovedShards    int              `json:"moved_shards"`
	StableShards   int              `json:"stable_shards"`
	Proof          epochProofReport `json:"routing_proof"`
}

type epochProofReport struct {
	Epoch           string `json:"epoch"`
	Generation      uint64 `json:"generation"`
	Key             string `json:"key"`
	Shard           uint32 `json:"shard"`
	Owner           string `json:"owner"`
	NonOwner        string `json:"non_owner"`
	NonOwnerOutcome string `json:"non_owner_outcome"`
	Attempts        int    `json:"sdk_put_attempts"`
}

type loadReport struct {
	Operations uint64 `json:"operations"`
	Verified   uint64 `json:"verified"`
	Misses     uint64 `json:"misses"`
	Mismatches uint64 `json:"mismatches"`
	RPCErrors  uint64 `json:"rpc_errors"`
}

type loadCounters struct {
	operations atomic.Uint64
	verified   atomic.Uint64
	misses     atomic.Uint64
	mismatches atomic.Uint64
	rpcErrors  atomic.Uint64
}

func (c *loadCounters) snapshot() loadReport {
	return loadReport{
		Operations: c.operations.Load(),
		Verified:   c.verified.Load(),
		Misses:     c.misses.Load(),
		Mismatches: c.mismatches.Load(),
		RPCErrors:  c.rpcErrors.Load(),
	}
}

func run(ctx context.Context, cfg settings) (report runReport, runErr error) {
	started := time.Now()
	report = runReport{
		Version:             1,
		ControlPlane:        cfg.controlPlane,
		Target:              cfg.target,
		Cycles:              cfg.cycles,
		Seed:                cfg.seed,
		Workers:             cfg.workers,
		StartedAt:           started.UTC(),
		TransitionsExpected: 2 * cfg.cycles,
	}
	var counters *loadCounters
	defer func() {
		report.CompletedAt = time.Now().UTC()
		report.ElapsedMillis = time.Since(started).Milliseconds()
		if counters != nil {
			report.Load = counters.snapshot()
		}
	}()

	if err := prepareProgressFile(cfg.readyFile); err != nil {
		return report, err
	}

	metadataConn, err := newGRPCConn(cfg.controlPlane)
	if err != nil {
		return report, fmt.Errorf("create metadata connection: %w", err)
	}
	defer metadataConn.Close()
	metadata := metadatav1.NewClusterMetadataServiceClient(metadataConn)

	baseline, err := fetchTopology(ctx, metadata, cfg.rpcTimeout)
	if err != nil {
		return report, fmt.Errorf("fetch baseline topology: %w", err)
	}
	if err := validateBaseline(baseline, cfg.target); err != nil {
		return report, err
	}
	report.BaselineGeneration = baseline.Generation
	report.BaselineFingerprint = hex.EncodeToString(baseline.Fingerprint)
	report.BaselineNodes = sortedNodeIDs(baseline.Nodes)
	report.ShardCount = baseline.ShardCount

	targetShards := shardsOwnedBy(baseline, cfg.target)
	report.TargetOwnedShards = len(targetShards)
	topologyIdentity := report.BaselineFingerprint
	if topologyIdentity == "" {
		topologyIdentity = fmt.Sprintf("legacy-generation-%d", baseline.Generation)
	}
	namespace := fmt.Sprintf("pulsekv-chaos:s%d:t%s:%s", cfg.seed, topologyIdentity, cfg.target)
	stable, err := buildStableEntries(baseline, cfg.target, namespace)
	if err != nil {
		return report, err
	}
	report.StableKeys = len(stable)

	cluster, err := sdk.New(cfg.controlPlane,
		sdk.WithRefreshInterval(cfg.refreshInterval),
		sdk.WithRefreshTimeout(cfg.rpcTimeout))
	if err != nil {
		return report, fmt.Errorf("create cluster SDK: %w", err)
	}
	defer func() {
		if err := cluster.Close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close cluster SDK: %w", err)
		}
	}()

	fmt.Println("=== PulseKV Phase 3 chaos verifier ===")
	fmt.Printf("baseline      generation %d; %d nodes; %d shards; target %s owns %d\n",
		baseline.Generation, len(baseline.Nodes), baseline.ShardCount, cfg.target, len(targetShards))
	fmt.Printf("stable set    %d immutable survivor-owned keys; %d workers\n", len(stable), cfg.workers)

	if err := seedStableKeys(ctx, cluster, stable, cfg.rpcTimeout); err != nil {
		return report, err
	}
	report.BaselineProof, err = verifyEpoch(ctx, cluster, baseline, baseline, cfg.target,
		targetShards[0], namespace+":baseline", "baseline", cfg, nil)
	if err != nil {
		return report, fmt.Errorf("baseline routing proof: %w", err)
	}

	loadCtx, stopLoad := context.WithCancel(ctx)
	counters, loadErrors, loadWG := startStableLoad(loadCtx, cluster, stable, cfg)
	defer func() {
		stopLoad()
		loadWG.Wait()
	}()
	if err := writeProgress(cfg.readyFile, 0); err != nil {
		return report, fmt.Errorf("publish readiness: %w", err)
	}
	fmt.Printf("ready         seeded and reading; progress 0/%d at %s\n",
		report.TransitionsExpected, cfg.readyFile)

	previous := baseline
	transitionIndex := 0
	for cycle := 1; cycle <= cfg.cycles; cycle++ {
		transitionIndex++
		transitionStarted := time.Now()
		removed, movement, _, err := waitForTransition(ctx, metadata, loadErrors,
			previous, baseline, cfg.target, "removal", cfg)
		if err != nil {
			return report, fmt.Errorf("cycle %d target removal: %w", cycle, err)
		}
		proofCfg := cfg
		proofCfg.transitionTimeout -= time.Since(transitionStarted)
		if proofCfg.transitionTimeout <= 0 {
			return report, fmt.Errorf("cycle %d removal exhausted its %s transition budget before routing proof",
				cycle, cfg.transitionTimeout)
		}
		proof, err := verifyEpoch(ctx, cluster, removed, baseline, cfg.target,
			targetShards[(transitionIndex-1)%len(targetShards)],
			fmt.Sprintf("%s:transition:%d", namespace, transitionIndex),
			fmt.Sprintf("cycle-%d-removal", cycle), proofCfg, loadErrors)
		if err != nil {
			return report, fmt.Errorf("cycle %d removal routing proof: %w", cycle, err)
		}
		elapsed := time.Since(transitionStarted)
		report.Transitions = append(report.Transitions, transitionReport{
			Index: transitionIndex, Cycle: cycle, Kind: "removal",
			FromGeneration: previous.Generation, ToGeneration: removed.Generation,
			ElapsedMillis: elapsed.Milliseconds(), MovedShards: movement.moved,
			StableShards: movement.stable, Proof: proof,
		})
		report.TransitionsVerified = transitionIndex
		if err := writeProgress(cfg.readyFile, transitionIndex); err != nil {
			return report, fmt.Errorf("publish removal progress: %w", err)
		}
		fmt.Printf("transition %d/%d removal gen %d->%d in %s: moved=%d stable=%d\n",
			transitionIndex, report.TransitionsExpected, previous.Generation, removed.Generation,
			elapsed.Round(time.Millisecond), movement.moved, movement.stable)
		previous = removed

		transitionIndex++
		transitionStarted = time.Now()
		rejoined, movement, _, err := waitForTransition(ctx, metadata, loadErrors,
			previous, baseline, cfg.target, "rejoin", cfg)
		if err != nil {
			return report, fmt.Errorf("cycle %d target rejoin: %w", cycle, err)
		}
		proofCfg = cfg
		proofCfg.transitionTimeout -= time.Since(transitionStarted)
		if proofCfg.transitionTimeout <= 0 {
			return report, fmt.Errorf("cycle %d rejoin exhausted its %s transition budget before routing proof",
				cycle, cfg.transitionTimeout)
		}
		proof, err = verifyEpoch(ctx, cluster, rejoined, baseline, cfg.target,
			targetShards[(transitionIndex-1)%len(targetShards)],
			fmt.Sprintf("%s:transition:%d", namespace, transitionIndex),
			fmt.Sprintf("cycle-%d-rejoin", cycle), proofCfg, loadErrors)
		if err != nil {
			return report, fmt.Errorf("cycle %d rejoin routing proof: %w", cycle, err)
		}
		elapsed = time.Since(transitionStarted)
		report.Transitions = append(report.Transitions, transitionReport{
			Index: transitionIndex, Cycle: cycle, Kind: "rejoin",
			FromGeneration: previous.Generation, ToGeneration: rejoined.Generation,
			ElapsedMillis: elapsed.Milliseconds(), MovedShards: movement.moved,
			StableShards: movement.stable, Proof: proof,
		})
		report.TransitionsVerified = transitionIndex
		if err := writeProgress(cfg.readyFile, transitionIndex); err != nil {
			return report, fmt.Errorf("publish rejoin progress: %w", err)
		}
		fmt.Printf("transition %d/%d rejoin  gen %d->%d in %s: moved=%d stable=%d\n",
			transitionIndex, report.TransitionsExpected, previous.Generation, rejoined.Generation,
			elapsed.Round(time.Millisecond), movement.moved, movement.stable)
		previous = rejoined
	}

	stopLoad()
	loadWG.Wait()
	report.Load = counters.snapshot()
	if report.Load.Misses != 0 || report.Load.Mismatches != 0 || report.Load.RPCErrors != 0 {
		return report, fmt.Errorf("stable load had misses=%d mismatches=%d RPC errors=%d",
			report.Load.Misses, report.Load.Mismatches, report.Load.RPCErrors)
	}
	if report.Load.Verified == 0 {
		return report, errors.New("stable load completed without verifying a read")
	}
	fmt.Printf("stable load   operations=%d verified=%d misses=%d mismatches=%d rpc_errors=%d\n",
		report.Load.Operations, report.Load.Verified, report.Load.Misses,
		report.Load.Mismatches, report.Load.RPCErrors)
	return report, nil
}

func prepareProgressFile(path string) error {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return fmt.Errorf("ready-file path is a directory: %s", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect ready file: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale ready file: %w", err)
	}
	return nil
}

func newGRPCConn(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		))
}

func fetchTopology(ctx context.Context, metadata metadatav1.ClusterMetadataServiceClient,
	timeout time.Duration) (clustertopology.Snapshot, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return clustertopology.Fetch(rpcCtx, metadata)
}

func validateBaseline(snapshot clustertopology.Snapshot, target string) error {
	if len(snapshot.Nodes) < 2 {
		return fmt.Errorf("chaos verification requires at least two live nodes, got %d", len(snapshot.Nodes))
	}
	if _, ok := snapshot.Nodes[target]; !ok {
		return fmt.Errorf("target %q is absent from baseline nodes %v", target, sortedNodeIDs(snapshot.Nodes))
	}
	if err := validateExactHRW(snapshot); err != nil {
		return fmt.Errorf("baseline topology: %w", err)
	}
	if len(shardsOwnedBy(snapshot, target)) == 0 {
		return fmt.Errorf("target %q owns no baseline shards", target)
	}
	return nil
}

func validateExactHRW(snapshot clustertopology.Snapshot) error {
	ids := sortedNodeIDs(snapshot.Nodes)
	want := router.AssignShards(ids, snapshot.ShardCount)
	if maps.Equal(snapshot.ShardMap, want) {
		return nil
	}
	for shard := uint32(0); shard < snapshot.ShardCount; shard++ {
		if snapshot.ShardMap[shard] != want[shard] {
			return fmt.Errorf("shard %d owner=%q, want HRW owner %q",
				shard, snapshot.ShardMap[shard], want[shard])
		}
	}
	return errors.New("shard map differs from HRW assignment")
}

func sortedNodeIDs(nodes map[string]string) []string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func shardsOwnedBy(snapshot clustertopology.Snapshot, owner string) []uint32 {
	shards := make([]uint32, 0)
	for shard := uint32(0); shard < snapshot.ShardCount; shard++ {
		if snapshot.ShardMap[shard] == owner {
			shards = append(shards, shard)
		}
	}
	return shards
}

type stableEntry struct {
	key   []byte
	value []byte
	shard uint32
	owner string
}

func buildStableEntries(snapshot clustertopology.Snapshot, target, namespace string) ([]stableEntry, error) {
	wanted := make(map[uint32]struct{})
	for shard := uint32(0); shard < snapshot.ShardCount; shard++ {
		if snapshot.ShardMap[shard] != target {
			wanted[shard] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return nil, errors.New("target owns every shard; no survivor-stable workload can be built")
	}

	entries := make(map[uint32]stableEntry, len(wanted))
	for candidate := 0; candidate < maxKeyAttempts && len(entries) < len(wanted); candidate++ {
		key := []byte(fmt.Sprintf("%s:stable:%d", namespace, candidate))
		shard := router.ShardForKey(key, snapshot.ShardCount)
		if _, ok := wanted[shard]; !ok {
			continue
		}
		if _, exists := entries[shard]; exists {
			continue
		}
		entries[shard] = stableEntry{
			key: key, value: []byte(fmt.Sprintf("stable-value:%s:%d", namespace, shard)),
			shard: shard, owner: snapshot.ShardMap[shard],
		}
	}
	if len(entries) != len(wanted) {
		return nil, fmt.Errorf("found deterministic keys for %d of %d survivor-owned shards",
			len(entries), len(wanted))
	}

	result := make([]stableEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].shard < result[j].shard })
	return result, nil
}

func seedStableKeys(ctx context.Context, cluster *sdk.Client, entries []stableEntry, timeout time.Duration) error {
	for _, entry := range entries {
		rpcCtx, cancel := context.WithTimeout(ctx, timeout)
		err := cluster.Put(rpcCtx, entry.key, entry.value)
		cancel()
		if err != nil {
			return fmt.Errorf("seed stable shard %d owner %s: %w", entry.shard, entry.owner, err)
		}

		rpcCtx, cancel = context.WithTimeout(ctx, timeout)
		value, found, err := cluster.Get(rpcCtx, entry.key)
		cancel()
		if err != nil {
			return fmt.Errorf("verify stable shard %d owner %s: %w", entry.shard, entry.owner, err)
		}
		if !found || !bytes.Equal(value, entry.value) {
			return fmt.Errorf("verify stable shard %d owner %s: found=%v value_match=%v",
				entry.shard, entry.owner, found, bytes.Equal(value, entry.value))
		}
	}
	return nil
}

func startStableLoad(ctx context.Context, cluster *sdk.Client, entries []stableEntry,
	cfg settings) (*loadCounters, <-chan error, *sync.WaitGroup) {
	counters := &loadCounters{}
	errorsOut := make(chan error, 1)
	wg := &sync.WaitGroup{}
	started := &sync.WaitGroup{}
	started.Add(cfg.workers)
	for worker := 0; worker < cfg.workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.seed + int64(worker+1)*7919))
			started.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				entry := entries[rng.Intn(len(entries))]
				rpcCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
				value, found, err := cluster.Get(rpcCtx, entry.key)
				cancel()
				if ctx.Err() != nil {
					return
				}
				counters.operations.Add(1)
				switch {
				case err != nil:
					counters.rpcErrors.Add(1)
					reportFirst(errorsOut, fmt.Errorf("worker %d stable shard %d RPC: %w", worker, entry.shard, err))
				case !found:
					counters.misses.Add(1)
					reportFirst(errorsOut, fmt.Errorf("worker %d stable shard %d missed", worker, entry.shard))
				case !bytes.Equal(value, entry.value):
					counters.mismatches.Add(1)
					reportFirst(errorsOut, fmt.Errorf("worker %d stable shard %d value mismatch", worker, entry.shard))
				default:
					counters.verified.Add(1)
				}
			}
		}(worker)
	}
	started.Wait()
	return counters, errorsOut, wg
}

func reportFirst(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}

type movementStats struct {
	moved  int
	stable int
}

func validateRemoval(before, after clustertopology.Snapshot, target string) (movementStats, error) {
	if after.Generation <= before.Generation {
		return movementStats{}, fmt.Errorf("generation did not increase: %d -> %d", before.Generation, after.Generation)
	}
	if _, ok := before.Nodes[target]; !ok {
		return movementStats{}, fmt.Errorf("target %q was absent before removal", target)
	}
	if _, ok := after.Nodes[target]; ok {
		return movementStats{}, fmt.Errorf("target %q is still present", target)
	}
	expectedNodes := maps.Clone(before.Nodes)
	delete(expectedNodes, target)
	if !maps.Equal(after.Nodes, expectedNodes) {
		return movementStats{}, fmt.Errorf("removal changed nodes other than %q: got %v, want %v",
			target, sortedNodeIDs(after.Nodes), sortedNodeIDs(expectedNodes))
	}
	if after.ShardCount != before.ShardCount {
		return movementStats{}, fmt.Errorf("shard count changed: %d -> %d", before.ShardCount, after.ShardCount)
	}
	if err := validateExactHRW(after); err != nil {
		return movementStats{}, fmt.Errorf("removed topology: %w", err)
	}

	var stats movementStats
	for shard := uint32(0); shard < before.ShardCount; shard++ {
		oldOwner, newOwner := before.ShardMap[shard], after.ShardMap[shard]
		if oldOwner == target {
			if newOwner == target || newOwner == "" {
				return movementStats{}, fmt.Errorf("target-owned shard %d was not reassigned", shard)
			}
			stats.moved++
			continue
		}
		if newOwner != oldOwner {
			return movementStats{}, fmt.Errorf("shard %d moved survivor-to-survivor from %s to %s",
				shard, oldOwner, newOwner)
		}
		stats.stable++
	}
	if stats.moved == 0 {
		return movementStats{}, fmt.Errorf("target %q owned no shards before removal", target)
	}
	return stats, nil
}

func validateRejoin(baseline, before, after clustertopology.Snapshot,
	target string) (movementStats, error) {
	if after.Generation <= before.Generation {
		return movementStats{}, fmt.Errorf("generation did not increase: %d -> %d", before.Generation, after.Generation)
	}
	if _, ok := before.Nodes[target]; ok {
		return movementStats{}, fmt.Errorf("target %q was present before rejoin", target)
	}
	if !maps.Equal(after.Nodes, baseline.Nodes) {
		return movementStats{}, fmt.Errorf("rejoin did not restore baseline nodes: got %v, want %v",
			sortedNodeIDs(after.Nodes), sortedNodeIDs(baseline.Nodes))
	}
	if after.ShardCount != baseline.ShardCount || !maps.Equal(after.ShardMap, baseline.ShardMap) {
		return movementStats{}, errors.New("rejoin did not restore the exact baseline shard map")
	}
	if err := validateExactHRW(after); err != nil {
		return movementStats{}, fmt.Errorf("rejoined topology: %w", err)
	}

	var stats movementStats
	for shard := uint32(0); shard < before.ShardCount; shard++ {
		oldOwner, newOwner := before.ShardMap[shard], after.ShardMap[shard]
		if oldOwner == newOwner {
			stats.stable++
			continue
		}
		if newOwner != target {
			return movementStats{}, fmt.Errorf("shard %d moved from survivor %s to survivor %s on rejoin",
				shard, oldOwner, newOwner)
		}
		stats.moved++
	}
	if stats.moved == 0 {
		return movementStats{}, fmt.Errorf("rejoining target %q took no shards", target)
	}
	return stats, nil
}

func waitForTransition(ctx context.Context, metadata metadatav1.ClusterMetadataServiceClient,
	loadErrors <-chan error, previous, baseline clustertopology.Snapshot,
	target, kind string, cfg settings) (clustertopology.Snapshot, movementStats, time.Duration, error) {
	started := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, cfg.transitionTimeout)
	defer cancel()
	var lastErr error

	for {
		select {
		case err := <-loadErrors:
			return clustertopology.Snapshot{}, movementStats{}, time.Since(started),
				fmt.Errorf("stable load correctness failure: %w", err)
		case <-waitCtx.Done():
			if lastErr == nil {
				lastErr = waitCtx.Err()
			}
			return clustertopology.Snapshot{}, movementStats{}, time.Since(started),
				fmt.Errorf("transition timed out after %s: %w", cfg.transitionTimeout, lastErr)
		default:
		}

		current, err := fetchTopology(waitCtx, metadata, cfg.rpcTimeout)
		if err != nil {
			lastErr = err
		} else if sameTopology(previous, current) {
			if current.Generation != previous.Generation {
				return clustertopology.Snapshot{}, movementStats{}, time.Since(started),
					fmt.Errorf("generation changed without topology change: %d -> %d",
						previous.Generation, current.Generation)
			}
		} else {
			var movement movementStats
			switch kind {
			case "removal":
				movement, err = validateRemoval(previous, current, target)
			case "rejoin":
				movement, err = validateRejoin(baseline, previous, current, target)
			default:
				err = fmt.Errorf("unknown transition kind %q", kind)
			}
			if err != nil {
				return clustertopology.Snapshot{}, movementStats{}, time.Since(started), err
			}
			return current, movement, time.Since(started), nil
		}

		select {
		case err := <-loadErrors:
			return clustertopology.Snapshot{}, movementStats{}, time.Since(started),
				fmt.Errorf("stable load correctness failure: %w", err)
		case <-waitCtx.Done():
		case <-time.After(cfg.pollInterval):
		}
	}
}

func sameTopology(a, b clustertopology.Snapshot) bool {
	return a.ShardCount == b.ShardCount && maps.Equal(a.Nodes, b.Nodes) && maps.Equal(a.ShardMap, b.ShardMap)
}

func verifyEpoch(ctx context.Context, cluster *sdk.Client, snapshot,
	baseline clustertopology.Snapshot, target string, shard uint32, keyPrefix,
	epoch string, cfg settings, loadErrors <-chan error) (epochProofReport, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, cfg.transitionTimeout)
	defer cancel()
	var lastErr error

	for attempt := 1; ; attempt++ {
		if loadErrors != nil {
			select {
			case err := <-loadErrors:
				return epochProofReport{}, fmt.Errorf("stable load correctness failure: %w", err)
			default:
			}
		}
		if err := deadlineCtx.Err(); err != nil {
			return epochProofReport{}, fmt.Errorf("SDK did not converge for epoch %s: %w (last: %v)",
				epoch, err, lastErr)
		}

		key, err := keyForShard(fmt.Sprintf("%s:attempt:%d", keyPrefix, attempt),
			shard, snapshot.ShardCount)
		if err != nil {
			return epochProofReport{}, err
		}
		value := []byte(fmt.Sprintf("epoch-value:%s:g%d:attempt%d", epoch, snapshot.Generation, attempt))
		owner := snapshot.ShardMap[shard]

		rpcCtx, rpcCancel := context.WithTimeout(deadlineCtx, cfg.rpcTimeout)
		err = cluster.Put(rpcCtx, key, value)
		rpcCancel()
		if err != nil {
			lastErr = fmt.Errorf("SDK Put: %w", err)
			if err := waitInterval(deadlineCtx, cfg.pollInterval); err != nil {
				continue
			}
			continue
		}

		ownerValue, found, err := directGet(deadlineCtx, snapshot.Nodes[owner], key, cfg.rpcTimeout)
		if err != nil {
			lastErr = fmt.Errorf("direct owner %s Get: %w", owner, err)
			_ = waitInterval(deadlineCtx, cfg.pollInterval)
			continue
		}
		if !found {
			lastErr = fmt.Errorf("predicted owner %s missed; SDK still used an older topology", owner)
			_ = waitInterval(deadlineCtx, cfg.pollInterval)
			continue
		}
		if !bytes.Equal(ownerValue, value) {
			return epochProofReport{}, fmt.Errorf("predicted owner %s returned a different value", owner)
		}

		nonOwner, outcome, err := verifyNonOwner(deadlineCtx, snapshot, baseline,
			target, owner, key, cfg.rpcTimeout)
		if err != nil {
			lastErr = err
			_ = waitInterval(deadlineCtx, cfg.pollInterval)
			continue
		}
		return epochProofReport{
			Epoch: epoch, Generation: snapshot.Generation, Key: string(key), Shard: shard,
			Owner: owner, NonOwner: nonOwner, NonOwnerOutcome: outcome, Attempts: attempt,
		}, nil
	}
}

func keyForShard(prefix string, want, shardCount uint32) ([]byte, error) {
	for candidate := 0; candidate < maxKeyAttempts; candidate++ {
		key := []byte(fmt.Sprintf("%s:%d", prefix, candidate))
		if router.ShardForKey(key, shardCount) == want {
			return key, nil
		}
	}
	return nil, fmt.Errorf("could not find a key for shard %d after %d candidates", want, maxKeyAttempts)
}

func verifyNonOwner(ctx context.Context, snapshot, baseline clustertopology.Snapshot,
	target, owner string, key []byte, timeout time.Duration) (string, string, error) {
	ids := sortedNodeIDs(snapshot.Nodes)
	for _, id := range ids {
		if id == owner {
			continue
		}
		_, found, err := directGet(ctx, snapshot.Nodes[id], key, timeout)
		if err != nil {
			return "", "", fmt.Errorf("direct non-owner %s Get: %w", id, err)
		}
		if found {
			return "", "", fmt.Errorf("non-owner %s unexpectedly returned found=true", id)
		}
		return id, "miss", nil
	}

	// A two-node baseline leaves one live node while the target is removed, so
	// there is no live non-owner to query. Probe the removed endpoint instead:
	// an explicit miss is preferred, while connection refusal is an equally
	// strong exclusion proof for an endpoint that membership has removed.
	address := baseline.Nodes[target]
	if address == "" || target == owner {
		return "", "", errors.New("topology has no non-owner endpoint for routing proof")
	}
	_, found, err := directGet(ctx, address, key, timeout)
	if err != nil {
		return target, "removed-node-unreachable", nil
	}
	if found {
		return "", "", fmt.Errorf("removed non-owner %s unexpectedly returned found=true", target)
	}
	return target, "miss", nil
}

func directGet(ctx context.Context, address string, key []byte,
	timeout time.Duration) ([]byte, bool, error) {
	conn, err := newGRPCConn(address)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := nodev1.NewNodeServiceClient(conn).Get(rpcCtx, &nodev1.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	if !resp.GetFound() {
		return nil, false, nil
	}
	return bytes.Clone(resp.GetValue()), true, nil
}

func waitInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func writeProgress(path string, verified int) error {
	return writeAtomic(path, []byte(fmt.Sprintf("%d\n", verified)), 0o644)
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomic(path, data, 0o644)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(mode); err != nil {
		return fmt.Errorf("set temporary file mode: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish %s: %w", path, err)
	}
	return nil
}
