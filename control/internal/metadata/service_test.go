package metadata

import (
	"context"
	"maps"
	"testing"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/router"
)

func TestGetShardMapIsDeterministicAndUsesHRW(t *testing.T) {
	cfg := &config.Config{
		ShardCount: 17,
		Nodes: []config.Node{
			{NodeID: "node-0", Host: "127.0.0.1", Port: 7100},
			{NodeID: "node-1", Host: "127.0.0.1", Port: 7101},
			{NodeID: "node-2", Host: "127.0.0.1", Port: 7102},
		},
	}

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	first, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("first GetShardMap: %v", err)
	}
	second, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("second GetShardMap: %v", err)
	}

	want := router.AssignShards(cfg.NodeIDs(), cfg.ShardCount)
	if !maps.Equal(first.GetShardToNodeId(), want) {
		t.Fatalf("first shard map = %v, want HRW assignment %v", first.GetShardToNodeId(), want)
	}
	if !maps.Equal(second.GetShardToNodeId(), want) {
		t.Fatalf("second shard map = %v, want HRW assignment %v", second.GetShardToNodeId(), want)
	}
	if !maps.Equal(first.GetShardToNodeId(), second.GetShardToNodeId()) {
		t.Fatalf("repeated GetShardMap calls differ: first %v, second %v",
			first.GetShardToNodeId(), second.GetShardToNodeId())
	}
}
