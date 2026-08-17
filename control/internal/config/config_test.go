package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

const goodConfig = `
control_plane:
  port: 7000
shard_count: 8
nodes:
  - node_id: node-0
    port: 7100
  - node_id: node-1
    port: 7101
`

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load(write(t, goodConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got, want := cfg.ControlPlane.Address(), "127.0.0.1:7000"; got != want {
		t.Errorf("control plane address = %q, want %q", got, want)
	}
	if got, want := cfg.Nodes[1].Address(), "127.0.0.1:7101"; got != want {
		t.Errorf("node-1 address = %q, want %q", got, want)
	}
	if got, want := len(cfg.Nodes), 2; got != want {
		t.Errorf("node count = %d, want %d", got, want)
	}
}

func TestLoadDefaultsShardCount(t *testing.T) {
	cfg, err := Load(write(t, `
control_plane:
  port: 7000
nodes:
  - node_id: node-0
    port: 7100
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ShardCount != DefaultShardCount {
		t.Errorf("shard count = %d, want default %d", cfg.ShardCount, DefaultShardCount)
	}
}

// The scripts and the server both read this file. A config that half-parses is
// worse than one that refuses to load, so every one of these must be rejected.
func TestLoadRejectsBadConfigs(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "duplicate node id",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: n, port: 7100}\n  - {node_id: n, port: 7101}\n",
			want: "already used by nodes[0]",
		},
		{
			name: "duplicate port",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100}\n  - {node_id: b, port: 7100}\n",
			want: "already used by nodes[0]",
		},
		{
			name: "node collides with control plane",
			body: "control_plane:\n  port: 7100\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "already used by control_plane",
		},
		{
			name: "empty node id",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: \"\", port: 7100}\n",
			want: "must not be empty",
		},
		{
			name: "no nodes",
			body: "control_plane:\n  port: 7000\nnodes: []\n",
			want: "at least one node is required",
		},
		{
			name: "port out of range",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: a, port: 70000}\n",
			want: "not in 1..65535",
		},
		{
			name: "typo'd key",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node-id: a, port: 7100}\n",
			want: "field node-id not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(write(t, tc.body))
			if err == nil {
				t.Fatalf("Load succeeded; want an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v\nwant it to contain %q", err, tc.want)
			}
		})
	}
}

func TestShardMapCoversEveryShard(t *testing.T) {
	cfg, err := Load(write(t, goodConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	m := cfg.ShardMap()
	if got, want := uint32(len(m)), cfg.ShardCount; got != want {
		t.Fatalf("shard map has %d entries, want %d", got, want)
	}

	known := map[string]int{}
	for shard := uint32(0); shard < cfg.ShardCount; shard++ {
		owner, ok := m[shard]
		if !ok {
			t.Fatalf("shard %d has no owner", shard)
		}
		known[owner]++
	}
	// Round-robin over 2 nodes with 8 shards: 4 each. Phase 2.1 replaces this
	// with rendezvous hashing, at which point the balance assertion becomes a
	// distribution assertion rather than an exact one.
	for _, n := range cfg.Nodes {
		if known[n.NodeID] != int(cfg.ShardCount)/len(cfg.Nodes) {
			t.Errorf("node %s owns %d shard(s), want %d",
				n.NodeID, known[n.NodeID], int(cfg.ShardCount)/len(cfg.Nodes))
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("Load on a missing file succeeded; want an error")
	}
}
