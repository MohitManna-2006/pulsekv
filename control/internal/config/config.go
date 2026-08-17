// Package config loads the local cluster's launch and bootstrap inventory.
//
// Phase 3 no longer treats Nodes as authoritative metadata: each entry starts
// one data-plane process and one gossip sidecar, and the live gossip view is
// what ClusterMetadataService publishes. Phase 5 replaces the derived shard
// map with a Raft-backed state machine.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"

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

	DefaultControlPlaneGossipPort = 7200
	DefaultNodeGossipPortBase     = 7201
	MaxClusterNameBytes           = 255 // memberlist.LabelMaxSize
	MaxNodeIDBytes                = 64

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

// Endpoint is a host:port the control plane itself listens on.
type Endpoint struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	GossipPort int    `yaml:"gossip_port"`
}

// Address renders the endpoint as a gRPC dial target.
func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

// GossipAddress is the memberlist TCP/UDP endpoint for the control plane.
func (e Endpoint) GossipAddress() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.GossipPort))
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
	ControlPlane Endpoint   `yaml:"control_plane"`
	Membership   Membership `yaml:"membership"`
	ShardCount   uint32     `yaml:"shard_count"`
	Engine       Engine     `yaml:"engine"`
	Nodes        []Node     `yaml:"nodes"`

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
	if c.ControlPlane.Host == "" {
		c.ControlPlane.Host = DefaultHost
	}
	if c.ControlPlane.GossipPort == 0 {
		c.ControlPlane.GossipPort = DefaultControlPlaneGossipPort
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

	nonLoopback := !net.ParseIP(c.ControlPlane.Host).IsLoopback()
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

	if err := validatePort("control_plane.port", c.ControlPlane.Port); err != nil {
		problems = append(problems, err)
	}
	if err := validatePort("control_plane.gossip_port", c.ControlPlane.GossipPort); err != nil {
		problems = append(problems, err)
	}
	if err := validateGossipHost("control_plane.host", c.ControlPlane.Host); err != nil {
		problems = append(problems, err)
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
	addAddress(c.ControlPlane.Address(), "control_plane")
	addAddress(c.ControlPlane.GossipAddress(), "control_plane.gossip")

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

// NodeIDs returns the configured node IDs in file order.
func (c *Config) NodeIDs() []string {
	ids := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}
