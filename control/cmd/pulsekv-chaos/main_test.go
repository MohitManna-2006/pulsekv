package main

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
)

func testSnapshot(generation uint64, ids ...string) clustertopology.Snapshot {
	return testReplicatedSnapshot(generation, 0, ids...)
}

func testReplicatedSnapshot(generation uint64, replicationFactor int,
	ids ...string) clustertopology.Snapshot {
	nodes := make(map[string]string, len(ids))
	for _, id := range ids {
		nodes[id] = "address-for-" + id
	}
	const shards = uint32(256)
	snapshot := clustertopology.Snapshot{
		Generation: generation,
		ShardCount: shards,
		ShardMap:   router.AssignShards(ids, shards),
		Nodes:      nodes,
	}
	if replicationFactor > 0 {
		snapshot.ReplicationFactor = uint32(replicationFactor)
		snapshot.Owners = router.AssignShardOwners(ids, shards, replicationFactor)
	}
	return snapshot
}

func TestValidateRemovalMovesExactlyTargetShards(t *testing.T) {
	before := testSnapshot(11, "node-a", "node-b", "node-c", "node-d")
	after := testSnapshot(12, "node-a", "node-b", "node-d")

	stats, err := validateRemoval(before, after, "node-c")
	if err != nil {
		t.Fatalf("validateRemoval: %v", err)
	}
	wantMoved := len(shardsOwnedBy(before, "node-c"))
	if stats.moved != wantMoved {
		t.Fatalf("moved = %d, want target's %d shards", stats.moved, wantMoved)
	}
	if stats.moved+stats.stable != int(before.ShardCount) {
		t.Fatalf("moved + stable = %d, want %d", stats.moved+stats.stable, before.ShardCount)
	}
	for shard := uint32(0); shard < before.ShardCount; shard++ {
		if before.ShardMap[shard] != "node-c" && after.ShardMap[shard] != before.ShardMap[shard] {
			t.Fatalf("survivor-owned shard %d moved from %s to %s",
				shard, before.ShardMap[shard], after.ShardMap[shard])
		}
	}
}

func TestValidateBaselineRequiresTargetAndAtLeastTwoNodes(t *testing.T) {
	two := testSnapshot(1, "node-a", "node-b")
	if err := validateBaseline(two, "node-b"); err != nil {
		t.Fatalf("two-node baseline: %v", err)
	}
	one := testSnapshot(1, "node-a")
	if err := validateBaseline(one, "node-a"); err == nil || !strings.Contains(err.Error(), "at least two") {
		t.Fatalf("one-node error = %v, want minimum-node error", err)
	}
	if err := validateBaseline(two, "node-c"); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("missing-target error = %v, want absent error", err)
	}
}

func TestValidateRemovalRejectsWrongTransitions(t *testing.T) {
	baseline := testSnapshot(4, "node-a", "node-b", "node-c")
	validRemoved := testSnapshot(5, "node-a", "node-c")

	t.Run("generation must increase", func(t *testing.T) {
		bad := validRemoved
		bad.Generation = baseline.Generation
		if _, err := validateRemoval(baseline, bad, "node-b"); err == nil || !strings.Contains(err.Error(), "generation") {
			t.Fatalf("error = %v, want generation error", err)
		}
	})

	t.Run("only target may disappear", func(t *testing.T) {
		bad := testSnapshot(5, "node-a")
		if _, err := validateRemoval(baseline, bad, "node-b"); err == nil || !strings.Contains(err.Error(), "other") {
			t.Fatalf("error = %v, want exact membership error", err)
		}
	})

	t.Run("survivor shard may not move", func(t *testing.T) {
		bad := validRemoved
		bad.ShardMap = maps.Clone(validRemoved.ShardMap)
		for shard := uint32(0); shard < baseline.ShardCount; shard++ {
			oldOwner := baseline.ShardMap[shard]
			if oldOwner == "node-a" {
				bad.ShardMap[shard] = "node-c"
				break
			}
		}
		if _, err := validateRemoval(baseline, bad, "node-b"); err == nil {
			t.Fatal("survivor-to-survivor movement was accepted")
		}
	})
}

func TestValidateRejoinRestoresBaselineAndOnlyMovesToJoiner(t *testing.T) {
	baseline := testSnapshot(20, "node-a", "node-b", "node-c", "node-d")
	removed := testSnapshot(21, "node-a", "node-b", "node-d")
	rejoined := testSnapshot(22, "node-a", "node-b", "node-c", "node-d")

	stats, err := validateRejoin(baseline, removed, rejoined, "node-c")
	if err != nil {
		t.Fatalf("validateRejoin: %v", err)
	}
	if stats.moved != len(shardsOwnedBy(baseline, "node-c")) {
		t.Fatalf("moved = %d, want %d", stats.moved, len(shardsOwnedBy(baseline, "node-c")))
	}
	for shard := uint32(0); shard < baseline.ShardCount; shard++ {
		if removed.ShardMap[shard] != rejoined.ShardMap[shard] && rejoined.ShardMap[shard] != "node-c" {
			t.Fatalf("shard %d moved to non-joiner %s", shard, rejoined.ShardMap[shard])
		}
	}

	bad := rejoined
	bad.ShardMap = maps.Clone(rejoined.ShardMap)
	bad.ShardMap[0] = "node-a"
	if maps.Equal(bad.ShardMap, baseline.ShardMap) {
		bad.ShardMap[0] = "node-b"
	}
	if _, err := validateRejoin(baseline, removed, bad, "node-c"); err == nil || !strings.Contains(err.Error(), "baseline shard map") {
		t.Fatalf("error = %v, want baseline restoration error", err)
	}
}

func TestBuildStableEntriesCoversEverySurvivorOwnedShardDeterministically(t *testing.T) {
	baseline := testSnapshot(1, "node-a", "node-b", "node-c", "node-d")
	first, err := buildStableEntries(baseline, "node-c", "deterministic-test")
	if err != nil {
		t.Fatalf("first buildStableEntries: %v", err)
	}
	second, err := buildStableEntries(baseline, "node-c", "deterministic-test")
	if err != nil {
		t.Fatalf("second buildStableEntries: %v", err)
	}
	if len(first) != int(baseline.ShardCount)-len(shardsOwnedBy(baseline, "node-c")) {
		t.Fatalf("stable keys = %d, want one per survivor-owned shard", len(first))
	}
	if len(first) != len(second) {
		t.Fatalf("deterministic builds differ in length: %d vs %d", len(first), len(second))
	}
	seen := make(map[uint32]bool, len(first))
	for i, entry := range first {
		if entry.shard != router.ShardForKey(entry.key, baseline.ShardCount) {
			t.Fatalf("entry %d key maps to wrong shard", i)
		}
		if baseline.ShardMap[entry.shard] == "node-c" {
			t.Fatalf("entry %d uses target-owned shard %d", i, entry.shard)
		}
		if entry.owner != baseline.ShardMap[entry.shard] {
			t.Fatalf("entry %d owner = %s, want %s", i, entry.owner, baseline.ShardMap[entry.shard])
		}
		if seen[entry.shard] {
			t.Fatalf("duplicate stable shard %d", entry.shard)
		}
		seen[entry.shard] = true
		if !maps.Equal(map[string]string{"key": string(entry.key), "value": string(entry.value)},
			map[string]string{"key": string(second[i].key), "value": string(second[i].value)}) {
			t.Fatalf("entry %d differs between deterministic builds", i)
		}
	}
}

func TestProgressAndReportWritesAreAtomicAndReplaceExistingFiles(t *testing.T) {
	dir := t.TempDir()
	progressPath := filepath.Join(dir, "nested", "ready")
	if err := writeProgress(progressPath, 0); err != nil {
		t.Fatalf("write progress 0: %v", err)
	}
	if err := writeProgress(progressPath, 7); err != nil {
		t.Fatalf("write progress 7: %v", err)
	}
	data, err := os.ReadFile(progressPath)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if string(data) != "7\n" {
		t.Fatalf("progress = %q, want 7\\n", data)
	}

	reportPath := filepath.Join(dir, "report.json")
	want := runReport{
		Version: 1, Success: true, Target: "node-c", Cycles: 2,
		StartedAt: time.Unix(1, 0).UTC(), CompletedAt: time.Unix(2, 0).UTC(),
		TransitionsExpected: 4, TransitionsVerified: 4,
	}
	if err := writeJSONAtomic(reportPath, want); err != nil {
		t.Fatalf("write report: %v", err)
	}
	reportData, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var got runReport
	if err := json.Unmarshal(reportData, &got); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if got.Version != 1 || !got.Success || got.Target != "node-c" || got.TransitionsVerified != 4 {
		t.Fatalf("decoded report = %+v", got)
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("atomic writes left temporary files: %v", temps)
	}
}

// ---------------------------------------------------------------------------
// Phase 4: promotion proof selection
// ---------------------------------------------------------------------------

// The promotion set must sit on shards the target PRIMARIES -- the exact
// complement of the survivor-stable set, which deliberately avoids them. A key
// on a survivor-owned shard would survive the kill for reasons that have
// nothing to do with replication and would prove nothing.
func TestPromotionEntriesLandOnTargetPrimariedShards(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2", "node-3"}
	snapshot := testReplicatedSnapshot(9, 1, ids...)
	const target = "node-2"
	targetShards := shardsOwnedBy(snapshot, target)

	entries, skip := buildPromotionEntries(snapshot, target, targetShards, "ns", 5)
	if skip != "" {
		t.Fatalf("promotion unexpectedly skipped: %s", skip)
	}
	if len(entries) != 5 {
		t.Fatalf("built %d entries, want 5", len(entries))
	}

	seen := map[uint32]bool{}
	for _, entry := range entries {
		if snapshot.ShardMap[entry.shard] != target {
			t.Fatalf("shard %d is primaried by %q, not the target %q",
				entry.shard, snapshot.ShardMap[entry.shard], target)
		}
		if got := router.ShardForKey(entry.key, snapshot.ShardCount); got != entry.shard {
			t.Fatalf("key %q hashes to shard %d, not its declared shard %d",
				entry.key, got, entry.shard)
		}
		if len(entry.replicas) == 0 {
			t.Fatalf("shard %d has no replica, so it cannot prove a promotion", entry.shard)
		}
		if seen[entry.shard] {
			t.Fatalf("shard %d appears twice", entry.shard)
		}
		seen[entry.shard] = true
	}
}

// Every reason the proof cannot run must be reported as a reason, never as a
// silent empty set. A report that says "0 promotion keys" with no explanation
// is indistinguishable from a report that passed without testing anything.
func TestPromotionEntriesExplainEverySkip(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2", "node-3"}
	const target = "node-2"

	replicated := testReplicatedSnapshot(9, 1, ids...)
	unreplicated := testReplicatedSnapshot(9, 0, ids...)

	cases := []struct {
		name      string
		snapshot  clustertopology.Snapshot
		requested int
		wantSub   string
	}{
		{"replication factor zero", unreplicated, 4, "replication factor is 0"},
		{"promotion keys disabled", replicated, 0, "--promotion-keys is 0"},
		{"no owner map published", clustertopology.Snapshot{
			ShardCount: replicated.ShardCount, ShardMap: replicated.ShardMap,
			Nodes: replicated.Nodes, ReplicationFactor: 1,
		}, 4, "predates Phase 4"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries, skip := buildPromotionEntries(tc.snapshot, target,
				shardsOwnedBy(tc.snapshot, target), "ns", tc.requested)
			if len(entries) != 0 {
				t.Fatalf("built %d entries, want none", len(entries))
			}
			if !strings.Contains(skip, tc.wantSub) {
				t.Fatalf("skip reason = %q, want it to mention %q", skip, tc.wantSub)
			}
		})
	}
}

// Asking for more shards than the target primaries with a live replica takes
// what is available rather than failing. The proof is weaker, not broken.
func TestPromotionEntriesCapToAvailableShards(t *testing.T) {
	ids := []string{"node-0", "node-1"}
	snapshot := testReplicatedSnapshot(3, 1, ids...)
	const target = "node-1"
	targetShards := shardsOwnedBy(snapshot, target)

	entries, skip := buildPromotionEntries(snapshot, target, targetShards, "ns", len(targetShards)+50)
	if skip != "" {
		t.Fatalf("promotion unexpectedly skipped: %s", skip)
	}
	if len(entries) != len(targetShards) {
		t.Fatalf("built %d entries, want the %d available", len(entries), len(targetShards))
	}
}

// Every cycle must write distinct bytes, or a stale copy left on some node
// could satisfy the next round's assertion for free.
func TestRefreshPromotionValuesChangesEveryValue(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2", "node-3"}
	snapshot := testReplicatedSnapshot(9, 1, ids...)
	entries, skip := buildPromotionEntries(snapshot, "node-2",
		shardsOwnedBy(snapshot, "node-2"), "ns", 3)
	if skip != "" {
		t.Fatalf("promotion unexpectedly skipped: %s", skip)
	}

	seen := map[string]bool{}
	for round := 0; round < 4; round++ {
		if round > 0 {
			refreshPromotionValues(entries, round)
		}
		for _, entry := range entries {
			if seen[string(entry.value)] {
				t.Fatalf("round %d reused the value %q", round, entry.value)
			}
			seen[string(entry.value)] = true
			// The suffix must be replaced, not accumulated.
			if strings.Count(string(entry.value), ":round") != 1 {
				t.Fatalf("value %q accumulated round suffixes", entry.value)
			}
		}
	}
}

func TestSettingsRequireDynamicVerifierInputs(t *testing.T) {
	valid := settings{
		controlPlane: "127.0.0.1:7000", target: "node-c", cycles: 1, seed: 0,
		readyFile: "ready", reportPath: "report.json", transitionTimeout: time.Second,
		pollInterval: time.Millisecond, refreshInterval: time.Millisecond,
		rpcTimeout: time.Second, workers: 1,
		promotionKeys: 4, catchUpTimeout: time.Second,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid settings: %v", err)
	}

	// 0 promotion keys is a legal setting, not a missing one: it is how a run
	// against a replication-factor-0 cluster is expressed.
	disabled := valid
	disabled.promotionKeys = 0
	if err := disabled.validate(); err != nil {
		t.Fatalf("--promotion-keys=0 must be accepted: %v", err)
	}

	cases := []struct {
		name string
		edit func(*settings)
	}{
		{"empty target", func(c *settings) { c.target = "" }},
		{"zero cycles", func(c *settings) { c.cycles = 0 }},
		{"negative seed", func(c *settings) { c.seed = -1 }},
		{"same outputs", func(c *settings) { c.reportPath = c.readyFile }},
		{"zero poll", func(c *settings) { c.pollInterval = 0 }},
		{"zero refresh", func(c *settings) { c.refreshInterval = 0 }},
		{"zero workers", func(c *settings) { c.workers = 0 }},
		{"negative promotion keys", func(c *settings) { c.promotionKeys = -1 }},
		{"zero catch-up timeout", func(c *settings) { c.catchUpTimeout = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.edit(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("invalid settings were accepted")
			}
		})
	}
}
