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
	if got, want := cfg.ControlPlane.GossipAddress(), "127.0.0.1:7200"; got != want {
		t.Errorf("control plane gossip address = %q, want %q", got, want)
	}
	if got, want := cfg.Nodes[1].Address(), "127.0.0.1:7101"; got != want {
		t.Errorf("node-1 address = %q, want %q", got, want)
	}
	if got, want := cfg.Nodes[1].GossipAddress(), "127.0.0.1:7202"; got != want {
		t.Errorf("node-1 gossip address = %q, want %q", got, want)
	}
	if got, want := cfg.Membership.ClusterName, DefaultClusterName; got != want {
		t.Errorf("membership cluster name = %q, want %q", got, want)
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
			name: "node service collides with control plane gossip",
			body: "control_plane:\n  port: 7000\n  gossip_port: 7100\nnodes:\n  - {node_id: a, port: 7100, gossip_port: 7201}\n",
			want: "already used by control_plane.gossip",
		},
		{
			name: "duplicate gossip address",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100, gossip_port: 7300}\n  - {node_id: b, port: 7101, gossip_port: 7300}\n",
			want: "already used by nodes[0].gossip",
		},
		{
			name: "gossip port out of range",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100, gossip_port: 70000}\n",
			want: "gossip_port: 70000 is not in 1..65535",
		},
		{
			name: "hostname cannot be a memberlist bind address",
			body: "control_plane:\n  host: localhost\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "must be an IP address because memberlist binds directly",
		},
		{
			name: "unspecified address cannot be advertised",
			body: "control_plane:\n  host: 0.0.0.0\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "cannot be advertised to gossip peers",
		},
		{
			name: "cluster label too long",
			body: "control_plane:\n  port: 7000\nmembership:\n  cluster_name: " + strings.Repeat("x", MaxClusterNameBytes+1) + "\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "exceeds memberlist's 255-byte label limit",
		},
		{
			name: "empty node id",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: \"\", port: 7100}\n",
			want: "must not be empty",
		},
		{
			name: "unsafe node id",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: ../outside, port: 7100}\n",
			want: "must match [A-Za-z0-9][A-Za-z0-9._-]*",
		},
		{
			name: "node id delimiter",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: 'node,other', port: 7100}\n",
			want: "must match [A-Za-z0-9][A-Za-z0-9._-]*",
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

// The whole reason replication_factor is parsed through a pointer: 0 is a
// setting, not an absence. A config that says 0 must get 0, and a config that
// says nothing must get the default -- and the two must be distinguishable.
func TestReplicationFactorDistinguishesZeroFromUnset(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"unset defaults", goodConfig, DefaultReplicationFactor},
		{"explicit zero", goodConfig + "\nreplication_factor: 0\n", 0},
		{"explicit one", goodConfig + "\nreplication_factor: 1\n", 1},
		{"explicit two", goodConfig + "\nreplication_factor: 2\n", 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(write(t, tc.body))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.ReplicationFactor != tc.want {
				t.Fatalf("replication factor = %d, want %d", cfg.ReplicationFactor, tc.want)
			}
		})
	}
}

func TestReplicationFactorRejectsOutOfRangeValues(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantSub string
	}{
		{"negative", "-1", "must not be negative"},
		{"far above the maximum", "1000", "exceeds the supported maximum"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(write(t, goodConfig+"\nreplication_factor: "+tc.value+"\n"))
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("Load error = %v, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

// More replicas than there are other nodes is legal and starts. It just means
// every shard holds fewer copies than the number implies, which is exactly the
// kind of thing that looks fine until a node dies.
func TestReplicationFactorAboveClusterSizeWarnsButLoads(t *testing.T) {
	cfg, err := Load(write(t, goodConfig+"\nreplication_factor: 5\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ReplicationFactor != 5 {
		t.Fatalf("replication factor = %d, want 5", cfg.ReplicationFactor)
	}

	var found bool
	for _, warning := range cfg.Warnings() {
		found = found || strings.Contains(warning, "replication_factor")
	}
	if !found {
		t.Fatalf("warnings = %v, want one naming replication_factor", cfg.Warnings())
	}

	// And the achievable factor must not warn.
	quiet, err := Load(write(t, goodConfig+"\nreplication_factor: 1\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, warning := range quiet.Warnings() {
		if strings.Contains(warning, "replication_factor") {
			t.Fatalf("unexpected replication warning at an achievable factor: %s", warning)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("Load on a missing file succeeded; want an error")
	}
}
