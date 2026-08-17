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
	nodes := make(map[string]string, len(ids))
	for _, id := range ids {
		nodes[id] = "address-for-" + id
	}
	const shards = uint32(256)
	return clustertopology.Snapshot{
		Generation: generation,
		ShardCount: shards,
		ShardMap:   router.AssignShards(ids, shards),
		Nodes:      nodes,
	}
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

func TestSettingsRequireDynamicVerifierInputs(t *testing.T) {
	valid := settings{
		controlPlane: "127.0.0.1:7000", target: "node-c", cycles: 1, seed: 0,
		readyFile: "ready", reportPath: "report.json", transitionTimeout: time.Second,
		pollInterval: time.Millisecond, refreshInterval: time.Millisecond,
		rpcTimeout: time.Second, workers: 1,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid settings: %v", err)
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
