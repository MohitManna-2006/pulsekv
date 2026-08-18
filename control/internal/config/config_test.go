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

	if got, want := len(cfg.ControlPlanes), 1; got != want {
		t.Fatalf("control plane replica count = %d, want %d", got, want)
	}
	if got, want := cfg.ControlPlanes[0].Address(), "127.0.0.1:7000"; got != want {
		t.Errorf("control plane address = %q, want %q", got, want)
	}
	if got, want := cfg.ControlPlanes[0].GossipAddress(), "127.0.0.1:7240"; got != want {
		t.Errorf("control plane gossip address = %q, want %q", got, want)
	}
	if got, want := cfg.ControlPlanes[0].RaftAddress(), "127.0.0.1:7300"; got != want {
		t.Errorf("control plane raft address = %q, want %q", got, want)
	}
	if got, want := cfg.ControlPlanes[0].NodeID, "cp-0"; got != want {
		t.Errorf("control plane node ID = %q, want %q", got, want)
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
			want: "already used by control_plane[0].gossip",
		},
		{
			name: "duplicate gossip address",
			body: "control_plane:\n  port: 7000\nnodes:\n  - {node_id: a, port: 7100, gossip_port: 7205}\n  - {node_id: b, port: 7101, gossip_port: 7205}\n",
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
			name: "no control-plane replicas",
			body: "control_plane: []\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "at least one replica is required",
		},
		{
			name: "duplicate control-plane id",
			body: "control_plane:\n  - {node_id: cp, port: 7000}\n  - {node_id: cp, port: 7001}\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "already used by control_plane[0]",
		},
		{
			name: "two replicas sharing a raft port",
			body: "control_plane:\n  - {node_id: cp-0, port: 7000, raft_port: 7300}\n  - {node_id: cp-1, port: 7001, raft_port: 7300}\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "already used by control_plane[0].raft",
		},
		{
			name: "raft election timeout too small",
			body: "control_plane:\n  port: 7000\nraft:\n  election_timeout_ms: 5\nnodes:\n  - {node_id: a, port: 7100}\n",
			want: "is too small",
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

// ---------------------------------------------------------------------------
// Phase 5: the control-plane group
// ---------------------------------------------------------------------------

const threeReplicaConfig = `
control_plane:
  - node_id: cp-0
    port: 7000
  - node_id: cp-1
    port: 7001
  - node_id: cp-2
    port: 7002
shard_count: 8
nodes:
  - {node_id: node-0, port: 7100}
`

func TestControlPlaneListDefaultsEachReplica(t *testing.T) {
	cfg, err := Load(write(t, threeReplicaConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.ControlPlanes) != 3 {
		t.Fatalf("replica count = %d, want 3", len(cfg.ControlPlanes))
	}

	// Every default is indexed, so three replicas that name only their gRPC
	// port still land on three distinct gossip and Raft endpoints.
	wantGossip := []string{"127.0.0.1:7240", "127.0.0.1:7241", "127.0.0.1:7242"}
	wantRaft := []string{"127.0.0.1:7300", "127.0.0.1:7301", "127.0.0.1:7302"}
	for i, replica := range cfg.ControlPlanes {
		if got := replica.GossipAddress(); got != wantGossip[i] {
			t.Errorf("replica %d gossip address = %q, want %q", i, got, wantGossip[i])
		}
		if got := replica.RaftAddress(); got != wantRaft[i] {
			t.Errorf("replica %d raft address = %q, want %q", i, got, wantRaft[i])
		}
	}

	if got, want := cfg.ControlPlaneEndpoints(), "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"; got != want {
		t.Errorf("endpoints = %q, want %q", got, want)
	}
	if got := cfg.ControlPlaneIDs(); len(got) != 3 || got[2] != "cp-2" {
		t.Errorf("control plane IDs = %v", got)
	}
	if replica, ok := cfg.ControlPlaneByID("cp-1"); !ok || replica.Port != 7001 {
		t.Errorf("ControlPlaneByID(cp-1) = %+v, ok=%v", replica, ok)
	}
	if _, ok := cfg.ControlPlaneByID("cp-9"); ok {
		t.Error("ControlPlaneByID accepted an unknown replica")
	}
}

// A config written against the pre-Phase-5 shape -- a bare mapping rather than
// a list -- must parse as a one-replica group. A single-voter Raft group is
// valid, elects itself immediately, and is what every one-replica test uses.
func TestControlPlaneAcceptsLegacySingleMapping(t *testing.T) {
	cfg, err := Load(write(t, goodConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.ControlPlanes) != 1 {
		t.Fatalf("legacy mapping produced %d replicas, want 1", len(cfg.ControlPlanes))
	}
	if got, want := cfg.ControlPlaneEndpoints(), "127.0.0.1:7000"; got != want {
		t.Errorf("endpoints = %q, want %q", got, want)
	}
}

func TestRaftDefaultsAndDerivedTimings(t *testing.T) {
	cfg, err := Load(write(t, threeReplicaConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Raft.ElectionTimeoutMillis != DefaultRaftElectionTimeoutMillis {
		t.Errorf("election timeout = %d, want default %d",
			cfg.Raft.ElectionTimeoutMillis, DefaultRaftElectionTimeoutMillis)
	}
	if cfg.Raft.DataDir != DefaultRaftDataDir {
		t.Errorf("raft data dir = %q, want %q", cfg.Raft.DataDir, DefaultRaftDataDir)
	}

	// The derived timings must satisfy hashicorp/raft's own ordering rule, which
	// is the entire reason they are derived rather than configured.
	if cfg.Raft.LeaderLeaseTimeout() > cfg.Raft.HeartbeatTimeout() {
		t.Errorf("leader lease %s exceeds heartbeat %s",
			cfg.Raft.LeaderLeaseTimeout(), cfg.Raft.HeartbeatTimeout())
	}
	if cfg.Raft.ProposeInterval() >= cfg.Raft.ElectionTimeout() {
		t.Errorf("propose interval %s is not comfortably under the election timeout %s",
			cfg.Raft.ProposeInterval(), cfg.Raft.ElectionTimeout())
	}
}

// An even-sized group is legal but strictly worse than the odd size below it.
// It must load, and it must say so.
func TestEvenControlPlaneGroupWarns(t *testing.T) {
	cfg, err := Load(write(t, `
control_plane:
  - {node_id: cp-0, port: 7000}
  - {node_id: cp-1, port: 7001}
shard_count: 8
nodes:
  - {node_id: node-0, port: 7100}
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var found bool
	for _, warning := range cfg.Warnings() {
		found = found || strings.Contains(warning, "even-sized Raft group")
	}
	if !found {
		t.Fatalf("warnings = %v, want one about the even-sized group", cfg.Warnings())
	}

	odd, err := Load(write(t, threeReplicaConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, warning := range odd.Warnings() {
		if strings.Contains(warning, "even-sized Raft group") {
			t.Fatalf("three replicas warned about being even-sized: %s", warning)
		}
	}
}
