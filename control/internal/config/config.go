// Package config loads the local cluster's launch and bootstrap inventory.
//
// Phase 3 stopped treating Nodes as authoritative metadata: each entry starts
// one data-plane process and one gossip sidecar, and the live gossip view is
// what ClusterMetadataService publishes.
//
// Phase 5 made `control_plane` a LIST. There are now several control-plane
// replicas forming a Raft group, and the agreed membership in that group -- not
// any one replica's gossip view -- is what the metadata service publishes. This
// file is still launch/bootstrap inventory only: it says which processes to
// create and which addresses they may bootstrap through, never who is live.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultShardCount is the number of logical cluster shards when the
	// config does not say. 256 gives a 4-node dev cluster 64 shards each,
	// which is fine-grained enough that Phase 3's rebalancing has something
	// to actually move around.
	//
	// Note this is a *cluster* shard count and is unrelated to v1's 256
	// intra-process hashtable shards, which exist for lock striping.
	DefaultShardCount = 256

	// DefaultHost is where a node listens when the config omits `host`. The
	// local dev cluster is single-machine by definition.
	DefaultHost = "127.0.0.1"

	// Engine defaults, mirroring PK_ENGINE_DEFAULT_* in
	// node/engine/include/pulsekv_engine.h. Duplicated rather than shared
	// because Go cannot read a C header; the smoke test asserts the node
	// actually runs with what this file says, which is what keeps them
	// honest.
	DefaultRAMBudgetBytes = 256 * 1024 * 1024
	DefaultMaxValueBytes  = 64 * 1024 * 1024
	DefaultDataRoot       = "run/data"
	DefaultClusterName    = "pulsekv-v2"

	// Port bases. The four ranges are deliberately disjoint so a 32-node
	// cluster and a 5-replica control-plane group never overlap:
	//
	//	control-plane gRPC    7000 + i
	//	data-node     gRPC    7100 + i   (7100-7131 for 32 nodes)
	//	data-node     gossip  7201 + i   (7201-7232 for 32 nodes)
	//	control-plane gossip  7240 + i
	//	control-plane Raft    7300 + i
	//
	// A config may of course set any of them explicitly; these are only what a
	// replica gets when the file leaves the port out.
	DefaultControlPlanePortBase       = 7000
	DefaultControlPlaneGossipPortBase = 7240
	DefaultControlPlaneRaftPortBase   = 7300
	DefaultNodeGossipPortBase         = 7201

	MaxClusterNameBytes = 255 // memberlist.LabelMaxSize
	MaxNodeIDBytes      = 64

	// DefaultControlPlaneReplicas is what `deploy/*.yaml` ships. Three is the
	// smallest group that tolerates one failure, which is the property Phase 5
	// exists to demonstrate; the implementation plan's range is 3-5.
	DefaultControlPlaneReplicas = 3

	// Raft timing. One knob rather than four, because heartbeat, election, and
	// lease timeouts have ordering constraints between them that hashicorp/raft
	// validates at startup -- deriving the other three from this one makes an
	// invalid combination unrepresentable rather than a late startup error.
	//
	// 500 ms is chosen for a local dev loop: failover lands in roughly
	// 0.5-1.5 s, which is fast enough that a chaos cycle is not dominated by
	// waiting and slow enough that a busy laptop does not trigger spurious
	// elections. It is not a production number.
	DefaultRaftElectionTimeoutMillis = 500

	// How often the leader compares its own gossip view with the committed
	// membership and proposes the difference. Well under the election timeout,
	// so a membership change is committed long before it could be mistaken for
	// leader trouble.
	DefaultRaftProposeIntervalMillis = 200

	// Log entries between automatic snapshots. Membership changes are rare and
	// tiny, so this is about bounding replay time after a restart, not size.
	DefaultRaftSnapshotThreshold = 1024

	DefaultRaftDataDir = "run/raft"

	// DefaultReplicationFactor is the number of replicas per shard, beyond the
	// primary, when the config does not say. The design doc's range is 0, 1, or
	// 2; the default is 1 rather than 0 because a cluster that never keeps a
	// second copy would never exercise replication at all, and "the feature is
	// configured off by default" is a bad default for the feature's own dev
	// cluster. An explicit 0 remains a fully supported configuration.
	DefaultReplicationFactor = 1

	// MaxReplicationFactor bounds the configured value. Well above the design
	// doc's range, but finite: a typo'd replication_factor of 1000 would make
	// every shard's owner list the whole cluster and turn one client write into
	// a broadcast, which is worth refusing at startup rather than at 3am.
	MaxReplicationFactor = 8

	// EngineShards is the engine's fixed lock-shard count (PK_TABLE_SHARDS).
	// Needed here only to warn about the per-shard budget split.
	EngineShards = 256

	// UnaryValueLimitBytes is the wire limit above which Get/Put refuse and
	// the chunked RPCs are required. Fixed in proto/node.proto.
	UnaryValueLimitBytes = 4 * 1024 * 1024

	maxPort = 65535
)

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ControlPlane is one control-plane replica. It serves
// ClusterMetadataService on Port, observes gossip on GossipPort, and is one
// voter in the Raft metadata group on RaftPort.
//
// NodeID is the replica's Raft server ID, so it must be stable across restarts:
// a replica that came back under a different ID would look like a brand-new
// voter to the group rather than the one that left.
type ControlPlane struct {
	NodeID     string `yaml:"node_id"`
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	GossipPort int    `yaml:"gossip_port"`
	RaftPort   int    `yaml:"raft_port"`
}

// Address renders the replica as a gRPC dial target.
func (c ControlPlane) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// GossipAddress is this replica's memberlist TCP/UDP endpoint.
func (c ControlPlane) GossipAddress() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.GossipPort))
}

// RaftAddress is this replica's Raft transport endpoint.
func (c ControlPlane) RaftAddress() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.RaftPort))
}

// ControlPlaneList is the `control_plane:` section.
//
// It exists as a named type only to accept a bare mapping as well as a
// sequence. Phase 5 turned one replica into several, and a config written
// against the older shape parses as a one-replica group instead of failing with
// a yaml type error -- a single-voter Raft group is perfectly valid, elects
// itself immediately, and is genuinely useful in tests.
type ControlPlaneList []ControlPlane

func (l *ControlPlaneList) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.MappingNode {
		var single ControlPlane
		if err := value.Decode(&single); err != nil {
			return err
		}
		*l = ControlPlaneList{single}
		return nil
	}
	var many []ControlPlane
	if err := value.Decode(&many); err != nil {
		return err
	}
	*l = many
	return nil
}

// Raft configures the metadata group. Timings are raw milliseconds rather than
// duration strings for the same reason the engine settings are raw byte counts:
// two readers of this file cannot disagree about what a suffix means.
type Raft struct {
	// DataDir roots each replica's log and snapshot state, relative to the
	// directory holding the config file. Each replica owns <data_dir>/<node_id>
	// exclusively -- unlike the engine's spill tier, this state is meant to
	// survive a restart, because it is what makes a rejoining replica catch up
	// rather than start over.
	DataDir string `yaml:"data_dir"`

	// ElectionTimeoutMillis drives heartbeat, election, and leader-lease
	// timeouts together. See DefaultRaftElectionTimeoutMillis.
	ElectionTimeoutMillis int `yaml:"election_timeout_ms"`

	// ProposeIntervalMillis is how often the leader reconciles its gossip view
	// with committed membership.
	ProposeIntervalMillis int `yaml:"propose_interval_ms"`

	SnapshotThreshold uint64 `yaml:"snapshot_threshold"`
}

// HeartbeatTimeout, ElectionTimeout, and LeaderLeaseTimeout are derived rather
// than configured, so they cannot be set into a combination hashicorp/raft
// rejects (it requires the lease timeout to be no larger than the heartbeat).
func (r Raft) ElectionTimeout() time.Duration {
	return time.Duration(r.ElectionTimeoutMillis) * time.Millisecond
}

func (r Raft) HeartbeatTimeout() time.Duration { return r.ElectionTimeout() }

func (r Raft) LeaderLeaseTimeout() time.Duration { return r.ElectionTimeout() / 2 }

func (r Raft) ProposeInterval() time.Duration {
	return time.Duration(r.ProposeIntervalMillis) * time.Millisecond
}

// Node is one data-plane process: a node/grpc_shim binary serving NodeService.
type Node struct {
	NodeID     string `yaml:"node_id"`
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	GossipPort int    `yaml:"gossip_port"`
}

// Address renders the node as a gRPC dial target. This is exactly the string
// reported as NodeInfo.address.
func (n Node) Address() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
}

// GossipAddress is the memberlist TCP/UDP endpoint for this node's Go
// membership sidecar.
func (n Node) GossipAddress() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.GossipPort))
}

// Membership contains settings shared by all gossip participants.
type Membership struct {
	// ClusterName becomes memberlist's packet label, preventing an accidental
	// merge with an unrelated gossip ring. Gossip encryption is a later
	// production-hardening concern; the dev ring binds to loopback by default.
	ClusterName string `yaml:"cluster_name"`
}

// Engine is the per-node data-plane configuration, applied identically to
// every node. The control plane does not use these itself; it carries them so
// deploy/run-local-cluster.sh can read the cluster's shape through one parser
// instead of two.
type Engine struct {
	RAMBudgetBytes uint64 `yaml:"ram_budget_bytes"`
	MaxValueBytes  uint64 `yaml:"max_value_bytes"`

	// Root for per-node spill directories, relative to the directory holding
	// the config file. Each node gets <data_root>/<node_id>.
	DataRoot string `yaml:"data_root"`
}

// Config is the whole of deploy/cluster.config.yaml.
type Config struct {
	ControlPlanes ControlPlaneList `yaml:"control_plane"`
	Membership    Membership       `yaml:"membership"`
	ShardCount    uint32           `yaml:"shard_count"`
	Engine        Engine           `yaml:"engine"`
	Raft          Raft             `yaml:"raft"`
	Nodes         []Node           `yaml:"nodes"`

	// ReplicationFactorSetting is the raw YAML value, and it is a pointer for
	// one specific reason: 0 is a meaningful setting here, not an absent one.
	// Every other field in this file can treat its zero value as "not
	// configured" and default it; replication_factor: 0 means "no replicas",
	// which is a legal configuration the design doc names explicitly. Reading
	// it through an int would make that setting unreachable.
	//
	// Callers use ReplicationFactor below, which applyDefaults resolves.
	ReplicationFactorSetting *int `yaml:"replication_factor"`

	// ReplicationFactor is the effective number of replicas per shard, beyond
	// the primary. Resolved by applyDefaults; never read from YAML directly.
	ReplicationFactor int `yaml:"-"`

	// Path records where this config came from, for error messages.
	Path string `yaml:"-"`
}

// Load reads, defaults, and validates a cluster config. A config that does not
// validate is a hard failure: a dev cluster that half-starts because two nodes
// share a port is worse than one that refuses to start.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cluster config: %w", err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	// Reject unknown keys. A typo'd `node-id:` silently producing an empty
	// node ID is the kind of thing that costs an hour later.
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	cfg.Path = path
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster config %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.ShardCount == 0 {
		c.ShardCount = DefaultShardCount
	}
	for i := range c.ControlPlanes {
		replica := &c.ControlPlanes[i]
		if replica.NodeID == "" {
			replica.NodeID = fmt.Sprintf("cp-%d", i)
		}
		if replica.Host == "" {
			replica.Host = DefaultHost
		}
		if replica.Port == 0 {
			replica.Port = DefaultControlPlanePortBase + i
		}
		if replica.GossipPort == 0 {
			replica.GossipPort = DefaultControlPlaneGossipPortBase + i
		}
		if replica.RaftPort == 0 {
			replica.RaftPort = DefaultControlPlaneRaftPortBase + i
		}
	}
	if c.Raft.DataDir == "" {
		c.Raft.DataDir = DefaultRaftDataDir
	}
	if c.Raft.ElectionTimeoutMillis == 0 {
		c.Raft.ElectionTimeoutMillis = DefaultRaftElectionTimeoutMillis
	}
	if c.Raft.ProposeIntervalMillis == 0 {
		c.Raft.ProposeIntervalMillis = DefaultRaftProposeIntervalMillis
	}
	if c.Raft.SnapshotThreshold == 0 {
		c.Raft.SnapshotThreshold = DefaultRaftSnapshotThreshold
	}
	if c.Membership.ClusterName == "" {
		c.Membership.ClusterName = DefaultClusterName
	}
	if c.Engine.RAMBudgetBytes == 0 {
		c.Engine.RAMBudgetBytes = DefaultRAMBudgetBytes
	}
	if c.Engine.MaxValueBytes == 0 {
		c.Engine.MaxValueBytes = DefaultMaxValueBytes
	}
	if c.Engine.DataRoot == "" {
		c.Engine.DataRoot = DefaultDataRoot
	}
	if c.ReplicationFactorSetting == nil {
		c.ReplicationFactor = DefaultReplicationFactor
	} else {
		c.ReplicationFactor = *c.ReplicationFactorSetting
	}
	for i := range c.Nodes {
		if c.Nodes[i].Host == "" {
			c.Nodes[i].Host = DefaultHost
		}
		if c.Nodes[i].GossipPort == 0 {
			c.Nodes[i].GossipPort = DefaultNodeGossipPortBase + i
		}
	}
}

// Warnings reports configurations that are legal and will start, but that will
// behave in a way the operator probably did not intend. Returned rather than
// logged so the caller decides where they go; the control plane prints them at
// startup, which is where a dev cluster's boot log will surface them.
func (c *Config) Warnings() []string {
	var out []string

	// The one that actually catches people: the RAM budget is split 256 ways,
	// so a budget that looks generous can give each shard less than a single
	// value. The engine keeps such a value resident anyway (it never evicts a
	// shard's only entry), which means the node quietly runs above its stated
	// budget instead of spilling.
	perShard := c.Engine.RAMBudgetBytes / EngineShards
	if perShard < c.Engine.MaxValueBytes {
		out = append(out, fmt.Sprintf(
			"engine.ram_budget_bytes/%d = %d bytes per shard, which is smaller than "+
				"engine.max_value_bytes (%d): a single max-size value exceeds its shard's "+
				"share, so it stays resident and the node can run above the stated budget. "+
				"Raise ram_budget_bytes to at least %d to avoid it.",
			EngineShards, perShard, c.Engine.MaxValueBytes,
			c.Engine.MaxValueBytes*EngineShards))
	}

	if c.Engine.MaxValueBytes < UnaryValueLimitBytes {
		out = append(out, fmt.Sprintf(
			"engine.max_value_bytes (%d) is below the %d-byte unary wire limit: "+
				"values between the two are accepted by Put and then rejected by the "+
				"engine, which is a confusing pair of errors to debug.",
			c.Engine.MaxValueBytes, UnaryValueLimitBytes))
	}

	// Legal, and it starts, but every shard silently gets fewer copies than
	// asked for -- which looks exactly like working replication until a node
	// dies. Worth saying out loud in the boot log.
	if c.ReplicationFactor > 0 && len(c.Nodes) > 0 && c.ReplicationFactor > len(c.Nodes)-1 {
		out = append(out, fmt.Sprintf(
			"replication_factor (%d) exceeds the %d other configured node(s): every shard will hold "+
				"at most %d replica(s), because a node never replicates to itself. This is not an "+
				"error, but the cluster is less replicated than the number suggests.",
			c.ReplicationFactor, len(c.Nodes)-1, len(c.Nodes)-1))
	}

	// An even-sized group needs a larger quorum than the odd size below it while
	// tolerating exactly the same number of failures, so it is strictly worse
	// on both counts. Legal, and it runs; just never what someone meant.
	if len(c.ControlPlanes)%2 == 0 && len(c.ControlPlanes) > 0 {
		out = append(out, fmt.Sprintf(
			"control_plane has %d replicas: an even-sized Raft group needs a quorum of %d but still "+
				"tolerates only %d failure(s), exactly like a %d-replica group. Use an odd count.",
			len(c.ControlPlanes), len(c.ControlPlanes)/2+1,
			(len(c.ControlPlanes)-1)/2, len(c.ControlPlanes)-1))
	}

	nonLoopback := false
	for _, replica := range c.ControlPlanes {
		nonLoopback = nonLoopback || !net.ParseIP(replica.Host).IsLoopback()
	}
	for _, node := range c.Nodes {
		nonLoopback = nonLoopback || !net.ParseIP(node.Host).IsLoopback()
	}
	if nonLoopback {
		out = append(out,
			"membership is bound or advertised beyond loopback, but the local Phase 3 config only supplies a cluster label; labels isolate accidental rings but do not authenticate or encrypt gossip")
	}

	return out
}

// Validate collects every problem rather than reporting the first, so a
// mis-edited config takes one round trip to fix instead of five.
func (c *Config) Validate() error {
	var problems []error

	if len(c.ControlPlanes) == 0 {
		problems = append(problems, errors.New("control_plane: at least one replica is required"))
	}
	// An even-sized Raft group tolerates no more failures than the odd size
	// below it while needing a larger quorum, so it is strictly worse. Legal,
	// but worth saying; see Warnings.
	seenControlID := make(map[string]int, len(c.ControlPlanes))
	for i, replica := range c.ControlPlanes {
		where := fmt.Sprintf("control_plane[%d]", i)
		switch {
		case replica.NodeID == "":
			problems = append(problems, fmt.Errorf("%s.node_id: must not be empty", where))
		case len(replica.NodeID) > MaxNodeIDBytes:
			problems = append(problems, fmt.Errorf("%s.node_id: %d bytes exceeds the %d-byte limit",
				where, len(replica.NodeID), MaxNodeIDBytes))
		case !nodeIDPattern.MatchString(replica.NodeID):
			problems = append(problems, fmt.Errorf(
				"%s.node_id: %q must match [A-Za-z0-9][A-Za-z0-9._-]*", where, replica.NodeID))
		default:
			if previous, duplicate := seenControlID[replica.NodeID]; duplicate {
				problems = append(problems, fmt.Errorf(
					"%s.node_id: %q already used by control_plane[%d]", where, replica.NodeID, previous))
			}
			seenControlID[replica.NodeID] = i
		}
		if err := validatePort(where+".port", replica.Port); err != nil {
			problems = append(problems, err)
		}
		if err := validatePort(where+".gossip_port", replica.GossipPort); err != nil {
			problems = append(problems, err)
		}
		if err := validatePort(where+".raft_port", replica.RaftPort); err != nil {
			problems = append(problems, err)
		}
		if err := validateGossipHost(where+".host", replica.Host); err != nil {
			problems = append(problems, err)
		}
	}
	if c.Raft.DataDir == "" {
		problems = append(problems, errors.New("raft.data_dir: must not be empty"))
	}
	// hashicorp/raft rejects sub-5ms timeouts outright, and anything near that
	// on a shared machine elects a new leader every few heartbeats.
	if c.Raft.ElectionTimeoutMillis < 50 {
		problems = append(problems, fmt.Errorf(
			"raft.election_timeout_ms: %d is too small; use at least 50",
			c.Raft.ElectionTimeoutMillis))
	}
	if c.Raft.ProposeIntervalMillis <= 0 {
		problems = append(problems, fmt.Errorf(
			"raft.propose_interval_ms: %d must be positive", c.Raft.ProposeIntervalMillis))
	}
	if c.Membership.ClusterName == "" {
		problems = append(problems, errors.New("membership.cluster_name: must not be empty"))
	} else if len(c.Membership.ClusterName) > MaxClusterNameBytes {
		problems = append(problems, fmt.Errorf("membership.cluster_name: %d bytes exceeds memberlist's %d-byte label limit",
			len(c.Membership.ClusterName), MaxClusterNameBytes))
	}
	if len(c.Nodes) == 0 {
		problems = append(problems, errors.New("nodes: at least one node is required"))
	}
	if c.ShardCount == 0 {
		problems = append(problems, errors.New("shard_count: must be greater than zero"))
	}
	if c.Engine.RAMBudgetBytes == 0 {
		problems = append(problems, errors.New("engine.ram_budget_bytes: must be greater than zero"))
	}
	if c.Engine.MaxValueBytes == 0 {
		problems = append(problems, errors.New("engine.max_value_bytes: must be greater than zero"))
	}
	if c.Engine.DataRoot == "" {
		problems = append(problems, errors.New("engine.data_root: must not be empty"))
	}
	if c.ReplicationFactor < 0 {
		problems = append(problems, fmt.Errorf(
			"replication_factor: %d must not be negative (0 disables replication)", c.ReplicationFactor))
	} else if c.ReplicationFactor > MaxReplicationFactor {
		problems = append(problems, fmt.Errorf(
			"replication_factor: %d exceeds the supported maximum of %d",
			c.ReplicationFactor, MaxReplicationFactor))
	}

	seenID := make(map[string]int, len(c.Nodes))
	// memberlist listens on both TCP and UDP. Its TCP endpoint must not share
	// an address with any gRPC listener or other gossip participant.
	seenAddr := make(map[string]string, 2+2*len(c.Nodes))
	addAddress := func(address, owner string) {
		if previous, exists := seenAddr[address]; exists {
			problems = append(problems, fmt.Errorf(
				"%s: address %s already used by %s", owner, address, previous))
			return
		}
		seenAddr[address] = owner
	}
	for i, replica := range c.ControlPlanes {
		where := fmt.Sprintf("control_plane[%d]", i)
		addAddress(replica.Address(), where)
		addAddress(replica.GossipAddress(), where+".gossip")
		addAddress(replica.RaftAddress(), where+".raft")
	}

	for i, n := range c.Nodes {
		where := fmt.Sprintf("nodes[%d]", i)

		switch {
		case n.NodeID == "":
			problems = append(problems, fmt.Errorf("%s.node_id: must not be empty", where))
		case len(n.NodeID) > MaxNodeIDBytes:
			problems = append(problems, fmt.Errorf("%s.node_id: %d bytes exceeds the %d-byte limit",
				where, len(n.NodeID), MaxNodeIDBytes))
		case !nodeIDPattern.MatchString(n.NodeID):
			problems = append(problems, fmt.Errorf(
				"%s.node_id: %q must match [A-Za-z0-9][A-Za-z0-9._-]*", where, n.NodeID))
		default:
			if prev, dup := seenID[n.NodeID]; dup {
				problems = append(problems, fmt.Errorf(
					"%s.node_id: %q already used by nodes[%d]", where, n.NodeID, prev))
			}
			seenID[n.NodeID] = i
		}

		if err := validatePort(where+".port", n.Port); err != nil {
			problems = append(problems, err)
		}
		if err := validatePort(where+".gossip_port", n.GossipPort); err != nil {
			problems = append(problems, err)
		}
		if err := validateGossipHost(where+".host", n.Host); err != nil {
			problems = append(problems, err)
		}
		addAddress(n.Address(), where)
		addAddress(n.GossipAddress(), where+".gossip")
	}

	return errors.Join(problems...)
}

func validateGossipHost(field, host string) error {
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("%s: %q must be an IP address because memberlist binds directly to it", field, host)
	}
	if ip.IsUnspecified() {
		return fmt.Errorf("%s: %q cannot be advertised to gossip peers; use a routable interface address", field, host)
	}
	return nil
}

func validatePort(field string, port int) error {
	if port <= 0 || port > maxPort {
		return fmt.Errorf("%s: %d is not in 1..%d", field, port, maxPort)
	}
	return nil
}

// NodeIDs returns the configured data-node IDs in file order.
func (c *Config) NodeIDs() []string {
	ids := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}

// ControlPlaneIDs returns the configured control-plane replica IDs in file
// order. That order is also the Raft bootstrap order, so it must be identical
// on every replica -- which it is, because they all read this same file.
func (c *Config) ControlPlaneIDs() []string {
	ids := make([]string, 0, len(c.ControlPlanes))
	for _, replica := range c.ControlPlanes {
		ids = append(ids, replica.NodeID)
	}
	return ids
}

// ControlPlaneAddresses returns every replica's gRPC address, in file order.
// This is the list handed to clients and data nodes so neither is stuck when
// the specific replica it happens to prefer is down or mid-election.
func (c *Config) ControlPlaneAddresses() []string {
	addresses := make([]string, 0, len(c.ControlPlanes))
	for _, replica := range c.ControlPlanes {
		addresses = append(addresses, replica.Address())
	}
	return addresses
}

// ControlPlaneEndpoints renders ControlPlaneAddresses as one comma-separated
// string, which is the form every multi-endpoint flag in this repo takes.
func (c *Config) ControlPlaneEndpoints() string {
	return strings.Join(c.ControlPlaneAddresses(), ",")
}

// ControlPlaneGossipSeeds returns every replica's gossip address. A data
// sidecar seeds from all of them rather than one, so a control-plane replica
// being down does not delay a data node joining the ring.
func (c *Config) ControlPlaneGossipSeeds() []string {
	seeds := make([]string, 0, len(c.ControlPlanes))
	for _, replica := range c.ControlPlanes {
		seeds = append(seeds, replica.GossipAddress())
	}
	return seeds
}

// ControlPlaneByID finds one replica's entry. ok is false for an unknown ID,
// which is how `--node-id` is validated at startup.
func (c *Config) ControlPlaneByID(id string) (ControlPlane, bool) {
	for _, replica := range c.ControlPlanes {
		if replica.NodeID == id {
			return replica, true
		}
	}
	return ControlPlane{}, false
}
