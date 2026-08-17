// Command pulsekv-chaos is the passive Phase 3/4 correctness verifier.
//
// It never starts, stops, or signals cluster processes. deploy/chaos-test.sh
// owns lifecycle mutation; this command seeds stable data, sustains reads, and
// proves each observed removal/rejoin topology epoch before advancing an atomic
// progress file.
//
// Phase 4 added the promotion proof. Every Phase 3 assertion is unchanged and
// still runs; what is new is a second key set, seeded on shards the TARGET
// primaries with require_replica_acks = replication_factor, so the write is
// provably on every replica before the kill rather than racing in-flight async
// replication. After the removal those keys must be served byte-for-byte by the
// promoted replica, and after the rejoin by the restarted target -- which is
// only possible if newly-owned-shard catch-up actually refilled its empty
// engine. At replication factor 0 there is nothing to promote and the whole
// section is skipped, which keeps 0 a legal, exercised configuration.
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
	promotionKeys     int
	catchUpTimeout    time.Duration
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
	flag.IntVar(&cfg.promotionKeys, "promotion-keys", 8,
		"target-primaried shards to seed with strong-ack writes for the promotion proof "+
			"(0 disables it; ignored when the cluster's replication factor is 0)")
	flag.DurationVar(&cfg.catchUpTimeout, "catch-up-timeout", 20*time.Second,
		"how long to wait for a rejoined target to backfill its promotion keys from a peer")
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
	case c.promotionKeys < 0:
		return errors.New("--promotion-keys must not be negative")
	case c.catchUpTimeout <= 0:
		return errors.New("--catch-up-timeout must be positive")
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

	// Phase 4.
	ReplicationFactor uint32 `json:"replication_factor"`
	PromotionKeys     int    `json:"promotion_keys"`
	// Skipped is the honest record of why no promotion was proven, so a passing
	// report at replication factor 0 cannot be mistaken for a passing report
	// that actually exercised promotion.
	PromotionSkipped string `json:"promotion_skipped,omitempty"`
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
	Promotion      *promotionReport `json:"promotion_proof,omitempty"`
}

// promotionReport records that the keys written before a kill were still served,
// byte for byte, by whichever node took over -- the promoted replica after a
// removal, the restarted target after a rejoin.
type promotionReport struct {
	Kind          string `json:"kind"` // "replica-promotion" or "catch-up-after-rejoin"
	Shards        int    `json:"shards"`
	KeysVerified  int    `json:"keys_verified"`
	ElapsedMillis int64  `json:"elapsed_millis"`
	// A worked example, so the report shows a real placement rather than only
	// a count that passed.
	ExampleShard    uint32 `json:"example_shard"`
	ExamplePrevious string `json:"example_previous_primary"`
	ExampleNow      string `json:"example_new_primary"`
}

// promotionEntry is one key deliberately placed on a shard the target primaries,
// written with require_replica_acks so it is provably on every replica before
// the target is killed. Without the strong ack this proof would be racing
// asynchronous replication, and a pass would mean nothing.
type promotionEntry struct {
	key   []byte
	base  string // value stem; the round suffix makes each cycle's bytes distinct
	value []byte
	shard uint32
	// replicas is the placement this key was last strong-ack seeded against.
	replicas []string
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
	report.ReplicationFactor = baseline.ReplicationFactor

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

	promotion, skipReason := buildPromotionEntries(baseline, cfg.target, targetShards,
		namespace, cfg.promotionKeys)
	report.PromotionKeys = len(promotion)
	report.PromotionSkipped = skipReason

	fmt.Println("=== PulseKV Phase 3/4 chaos verifier ===")
	fmt.Printf("baseline      generation %d; %d nodes; %d shards; target %s owns %d\n",
		baseline.Generation, len(baseline.Nodes), baseline.ShardCount, cfg.target, len(targetShards))
	fmt.Printf("stable set    %d immutable survivor-owned keys; %d workers\n", len(stable), cfg.workers)
	if skipReason != "" {
		fmt.Printf("promotion     SKIPPED: %s\n", skipReason)
	} else {
		fmt.Printf("promotion     %d target-primaried shard(s), replication factor %d, "+
			"strong-ack seeded\n", len(promotion), baseline.ReplicationFactor)
	}

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

	// Seeded BEFORE readiness is published, and re-seeded before every later
	// removal, because the shell mutates the cluster the moment it sees the
	// progress count. A key written after that signal would be racing the kill.
	if err := seedPromotionKeys(ctx, cluster, promotion, baseline, cfg); err != nil {
		return report, err
	}
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
		// The Phase 4 headline: the keys that were on the target are still
		// there, byte for byte, on whichever replica was promoted. Asserted
		// against the physical node the new map names, not through the SDK.
		promotionProof, err := verifyPromotion(ctx, previous, removed, promotion,
			"replica-promotion", cfg.rpcTimeout, cfg.catchUpTimeout)
		if err != nil {
			return report, fmt.Errorf("cycle %d promotion proof: %w", cycle, err)
		}

		elapsed := time.Since(transitionStarted)
		report.Transitions = append(report.Transitions, transitionReport{
			Index: transitionIndex, Cycle: cycle, Kind: "removal",
			FromGeneration: previous.Generation, ToGeneration: removed.Generation,
			ElapsedMillis: elapsed.Milliseconds(), MovedShards: movement.moved,
			StableShards: movement.stable, Proof: proof, Promotion: promotionProof,
		})
		report.TransitionsVerified = transitionIndex
		if err := writeProgress(cfg.readyFile, transitionIndex); err != nil {
			return report, fmt.Errorf("publish removal progress: %w", err)
		}
		fmt.Printf("transition %d/%d removal gen %d->%d in %s: moved=%d stable=%d%s\n",
			transitionIndex, report.TransitionsExpected, previous.Generation, removed.Generation,
			elapsed.Round(time.Millisecond), movement.moved, movement.stable,
			describePromotion(promotionProof))
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
		// The restarted target primaries these shards again, and its engine
		// came up EMPTY -- the spill tier is purged at start, and there is no
		// WAL. If it answers these keys, the only way it can have them is
		// newly-owned-shard catch-up, which is the Phase 3 gap Phase 4 owns.
		catchUpProof, err := verifyPromotion(ctx, previous, rejoined, promotion,
			"catch-up-after-rejoin", cfg.rpcTimeout, cfg.catchUpTimeout)
		if err != nil {
			return report, fmt.Errorf("cycle %d catch-up proof: %w", cycle, err)
		}

		elapsed = time.Since(transitionStarted)
		report.Transitions = append(report.Transitions, transitionReport{
			Index: transitionIndex, Cycle: cycle, Kind: "rejoin",
			FromGeneration: previous.Generation, ToGeneration: rejoined.Generation,
			ElapsedMillis: elapsed.Milliseconds(), MovedShards: movement.moved,
			StableShards: movement.stable, Proof: proof, Promotion: catchUpProof,
		})
		report.TransitionsVerified = transitionIndex
		if err := writeProgress(cfg.readyFile, transitionIndex); err != nil {
			return report, fmt.Errorf("publish rejoin progress: %w", err)
		}
		fmt.Printf("transition %d/%d rejoin  gen %d->%d in %s: moved=%d stable=%d%s\n",
			transitionIndex, report.TransitionsExpected, previous.Generation, rejoined.Generation,
			elapsed.Round(time.Millisecond), movement.moved, movement.stable,
			describePromotion(catchUpProof))
		previous = rejoined

		// Re-seed for the next cycle's kill. Fresh values on the same keys, so
		// a stale copy left on some node cannot make the next proof pass.
		if cycle < cfg.cycles {
			refreshPromotionValues(promotion, cycle)
			if err := seedPromotionKeys(ctx, cluster, promotion, rejoined, cfg); err != nil {
				return report, fmt.Errorf("cycle %d re-seed: %w", cycle+1, err)
			}
		}
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

// ---------------------------------------------------------------------------
// Phase 4: promotion and catch-up proof
// ---------------------------------------------------------------------------

// buildPromotionEntries picks deterministic keys on shards the target is the
// PRIMARY for -- the mirror image of the stable set, which deliberately avoids
// them. Returns a human-readable reason instead of entries when the proof does
// not apply, so a skipped run says so rather than silently passing.
func buildPromotionEntries(snapshot clustertopology.Snapshot, target string,
	targetShards []uint32, namespace string, want int) ([]promotionEntry, string) {
	if want == 0 {
		return nil, "--promotion-keys is 0"
	}
	if snapshot.ReplicationFactor == 0 {
		return nil, "cluster replication factor is 0, so no shard has a replica to promote"
	}
	if len(snapshot.Owners) == 0 {
		return nil, "metadata published no shard owner map; the control plane predates Phase 4"
	}

	// Only shards that actually have a live replica can prove a promotion. At a
	// factor above the cluster size some may have none.
	usable := make([]uint32, 0, len(targetShards))
	for _, shard := range targetShards {
		if len(snapshot.Owners[shard].Replicas) > 0 {
			usable = append(usable, shard)
		}
	}
	if len(usable) == 0 {
		return nil, fmt.Sprintf("target %q primaries %d shard(s), none with a live replica",
			target, len(targetShards))
	}
	if want > len(usable) {
		want = len(usable)
	}

	entries := make([]promotionEntry, 0, want)
	for i := 0; i < want; i++ {
		shard := usable[i]
		key, err := keyForShard(fmt.Sprintf("%s:promotion:%d", namespace, shard),
			shard, snapshot.ShardCount)
		if err != nil {
			return nil, fmt.Sprintf("could not build a key for target-primaried shard %d", shard)
		}
		base := fmt.Sprintf("promotion-value:%s:shard%d", namespace, shard)
		entries = append(entries, promotionEntry{
			key:      key,
			base:     base,
			value:    promotionValue(base, 0),
			shard:    shard,
			replicas: append([]string(nil), snapshot.Owners[shard].Replicas...),
		})
	}
	return entries, ""
}

func promotionValue(base string, round int) []byte {
	return []byte(fmt.Sprintf("%s:round%d", base, round))
}

// refreshPromotionValues gives every key fresh bytes between cycles. Reusing
// the same value would let a stale copy left behind on some node satisfy the
// next round's assertion without replication or catch-up having done anything.
func refreshPromotionValues(entries []promotionEntry, round int) {
	for i := range entries {
		entries[i].value = promotionValue(entries[i].base, round)
	}
}

// seedPromotionKeys writes each promotion key through the SDK with
// require_replica_acks set to the shard's live replica count.
//
// The strong ack is the entire point. With a fire-and-forget write the
// post-kill assertion would be racing asynchronous replication, and a pass
// would prove only that the race happened to be won. Requiring every replica to
// ack first makes the assertion deterministic: the data provably existed on the
// promoted node before the primary died.
func seedPromotionKeys(ctx context.Context, cluster *sdk.Client, entries []promotionEntry,
	snapshot clustertopology.Snapshot, cfg settings) error {
	for i := range entries {
		entry := &entries[i]
		// Re-read from the current snapshot: replica placement can have moved
		// since the entries were built, and asking for a stale count would fail
		// with INVALID_ARGUMENT rather than tell us anything useful.
		replicas := snapshot.Owners[entry.shard].Replicas
		if len(replicas) == 0 {
			return fmt.Errorf("promotion shard %d has no live replica to ack", entry.shard)
		}
		entry.replicas = append([]string(nil), replicas...)

		acked, err := putWithAckConverging(ctx, cluster, entry.key, entry.value,
			uint32(len(replicas)), cfg)
		if err != nil {
			return fmt.Errorf("strong-ack seed of shard %d (%d replica(s)): %w",
				entry.shard, len(replicas), err)
		}
		if int(acked) < len(replicas) {
			return fmt.Errorf("strong-ack seed of shard %d reported %d of %d acks",
				entry.shard, acked, len(replicas))
		}

		// The ack says the replicas stored it. Verify that directly, against
		// each replica's own address, rather than believing the count.
		for _, replica := range replicas {
			address := snapshot.Nodes[replica]
			if address == "" {
				return fmt.Errorf("replica %q of shard %d has no address", replica, entry.shard)
			}
			value, found, err := directGet(ctx, address, entry.key, cfg.rpcTimeout)
			if err != nil {
				return fmt.Errorf("direct read of shard %d on replica %s: %w", entry.shard, replica, err)
			}
			if !found || !bytes.Equal(value, entry.value) {
				return fmt.Errorf("replica %s acked shard %d but does not hold the value "+
					"(found=%v, match=%v)", replica, entry.shard, found, bytes.Equal(value, entry.value))
			}
		}
	}
	return nil
}

// putWithAckConverging retries a strong-ack write that the primary refuses
// because it has not yet caught up with the topology.
//
// This is not papering over a race; it is the documented shape of the contract.
// The SDK and each data node learn ownership independently and asynchronously,
// so for up to one node poll interval after a membership change the primary can
// legitimately answer "I am not the primary for that shard yet" or "I only see
// N replicas". The node refuses rather than hanging, and says which case it is;
// the caller's correct response is to retry, since the write is idempotent.
//
// A NON-INVALID_ARGUMENT failure is returned immediately. DEADLINE_EXCEEDED in
// particular means the fan-out really did fall short, which is a result, not a
// timing artefact, and retrying past it would hide exactly what this harness
// exists to catch.
func putWithAckConverging(ctx context.Context, cluster *sdk.Client, key, value []byte,
	acks uint32, cfg settings) (uint32, error) {
	deadline := time.Now().Add(cfg.transitionTimeout)
	var lastErr error
	for attempt := 1; ; attempt++ {
		rpcCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
		acked, err := cluster.PutWithAck(rpcCtx, key, value, acks)
		cancel()
		if err == nil {
			return acked, nil
		}
		if status.Code(err) != codes.InvalidArgument {
			return 0, err
		}
		lastErr = err
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("primary did not converge on its replica set within %s "+
				"(%d attempt(s)): %w", cfg.transitionTimeout, attempt, lastErr)
		}
		if err := waitInterval(ctx, cfg.pollInterval); err != nil {
			return 0, err
		}
	}
}

// verifyPromotion asserts that every promotion key is served, byte for byte, by
// whichever node now primaries its shard -- read directly from that node's own
// address, never through the SDK, so this cannot pass on routing alone.
//
// A bounded retry is deliberate and means different things on each path. After
// a removal the promoted replica already holds the data (the strong ack proved
// it) and the retry only absorbs the moment between the map changing and the
// node's own view catching up. After a rejoin the restarted target starts with
// an empty engine and has to backfill from a peer, so the retry is genuinely
// waiting on catch-up.
func verifyPromotion(ctx context.Context, before, after clustertopology.Snapshot,
	entries []promotionEntry, kind string, rpcTimeout, budget time.Duration) (*promotionReport, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	started := time.Now()
	deadline := time.Now().Add(budget)
	report := &promotionReport{Kind: kind, Shards: len(entries)}

	for _, entry := range entries {
		owner := after.ShardMap[entry.shard]
		if owner == "" {
			return nil, fmt.Errorf("shard %d has no owner after the transition", entry.shard)
		}
		address := after.Nodes[owner]
		if address == "" {
			return nil, fmt.Errorf("new owner %q of shard %d has no address", owner, entry.shard)
		}

		// Promotion means the shard went to the node the PREVIOUS map ranked
		// first among its replicas -- not merely to some node that answers.
		if kind == "replica-promotion" {
			replicas := before.Owners[entry.shard].Replicas
			if len(replicas) == 0 {
				return nil, fmt.Errorf("shard %d had no replica to promote", entry.shard)
			}
			if owner != replicas[0] {
				return nil, fmt.Errorf(
					"shard %d was promoted to %q, but its top-ranked replica was %q",
					entry.shard, owner, replicas[0])
			}
		}

		var lastErr error
		verified := false
		for !verified {
			value, found, err := directGet(ctx, address, entry.key, rpcTimeout)
			switch {
			case err != nil:
				lastErr = fmt.Errorf("direct read on %s: %w", owner, err)
			case !found:
				lastErr = fmt.Errorf("new owner %s does not hold the key", owner)
			case !bytes.Equal(value, entry.value):
				// Wrong bytes is never a timing artefact. Fail immediately
				// rather than retrying until the budget runs out.
				return nil, fmt.Errorf("shard %d: new owner %s returned %d bytes that "+
					"differ from the acked value", entry.shard, owner, len(value))
			default:
				verified = true
			}
			if verified {
				break
			}
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("shard %d (%s): %w", entry.shard, kind, lastErr)
			}
			if err := waitInterval(ctx, 100*time.Millisecond); err != nil {
				return nil, err
			}
		}

		report.KeysVerified++
		if report.ExamplePrevious == "" {
			report.ExampleShard = entry.shard
			report.ExamplePrevious = before.ShardMap[entry.shard]
			report.ExampleNow = owner
		}
	}

	report.ElapsedMillis = time.Since(started).Milliseconds()
	return report, nil
}

func describePromotion(report *promotionReport) string {
	if report == nil {
		return ""
	}
	return fmt.Sprintf(" %s=%d/%d key(s) (shard %d: %s -> %s, %dms)",
		report.Kind, report.KeysVerified, report.Shards,
		report.ExampleShard, report.ExamplePrevious, report.ExampleNow, report.ElapsedMillis)
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
			target, owner, key, shard, cfg.rpcTimeout)
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
	target, owner string, key []byte, shard uint32, timeout time.Duration) (string, string, error) {
	// Skip every node that HOLDS this shard, not just its primary. Before
	// Phase 4 those were the same set; now a replica is supposed to have the
	// key, so probing one would turn working replication into a failed
	// exclusion proof -- intermittently, depending on which shard the epoch's
	// key landed on.
	holders := map[string]bool{owner: true}
	for _, replica := range snapshot.Owners[shard].Replicas {
		holders[replica] = true
	}

	ids := sortedNodeIDs(snapshot.Nodes)
	for _, id := range ids {
		if holders[id] {
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

	// A two-node baseline leaves one live node while the target is removed, and
	// a fully-replicated shard leaves no live node without a copy. Either way
	// there is no live non-holder to query, so probe the removed endpoint
	// instead: an explicit miss is preferred, while connection refusal is an
	// equally strong exclusion proof for an endpoint membership has removed.
	address := baseline.Nodes[target]
	if address == "" || holders[target] {
		return "", "", errors.New("topology has no non-holder endpoint for routing proof")
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
