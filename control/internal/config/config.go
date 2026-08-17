// Package config loads the static cluster definition that Phase 0 uses in
// place of a membership protocol.
//
// Everything here is deliberately dumb: a YAML file lists the control plane's
// port and one entry per data-plane node. Phase 3 replaces the node list with
// gossip membership and Phase 5 replaces the shard map with a Raft-backed
// state machine. Both of those swap out the *source* of this data without
// changing the ClusterMetadataService contract that exposes it, which is the
// whole reason this file exists rather than the node list being hardcoded.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
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
	// Phase 0 dev cluster is single-machine by definition.
	DefaultHost = "127.0.0.1"

	// Engine defaults, mirroring PK_ENGINE_DEFAULT_* in
	// node/engine/include/pulsekv_engine.h. Duplicated rather than shared
	// because Go cannot read a C header; the smoke test asserts the node
	// actually runs with what this file says, which is what keeps them
	// honest.
	DefaultRAMBudgetBytes = 256 * 1024 * 1024
	DefaultMaxValueBytes  = 64 * 1024 * 1024
	DefaultDataRoot       = "run/data"

	// EngineShards is the engine's fixed lock-shard count (PK_TABLE_SHARDS).
	// Needed here only to warn about the per-shard budget split.
	EngineShards = 256

	// UnaryValueLimitBytes is the wire limit above which Get/Put refuse and
	// the chunked RPCs are required. Fixed in proto/node.proto.
	UnaryValueLimitBytes = 4 * 1024 * 1024

	maxPort = 65535
)

// Endpoint is a host:port the control plane itself listens on.
type Endpoint struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// Address renders the endpoint as a gRPC dial target.
func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

// Node is one data-plane process: a node/grpc_shim binary serving NodeService.
type Node struct {
	NodeID string `yaml:"node_id"`
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
}

// Address renders the node as a gRPC dial target. This is exactly the string
// reported as NodeInfo.address.
func (n Node) Address() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
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
	ControlPlane Endpoint `yaml:"control_plane"`
	ShardCount   uint32   `yaml:"shard_count"`
	Engine       Engine   `yaml:"engine"`
	Nodes        []Node   `yaml:"nodes"`

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
	if c.Engine.RAMBudgetBytes == 0 {
		c.Engine.RAMBudgetBytes = DefaultRAMBudgetBytes
	}
	if c.Engine.MaxValueBytes == 0 {
		c.Engine.MaxValueBytes = DefaultMaxValueBytes
	}
	if c.Engine.DataRoot == "" {
		c.Engine.DataRoot = DefaultDataRoot
	}
	for i := range c.Nodes {
		if c.Nodes[i].Host == "" {
			c.Nodes[i].Host = DefaultHost
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

	return out
}

// Validate collects every problem rather than reporting the first, so a
// mis-edited config takes one round trip to fix instead of five.
func (c *Config) Validate() error {
	var problems []error

	if err := validatePort("control_plane.port", c.ControlPlane.Port); err != nil {
		problems = append(problems, err)
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

	seenID := make(map[string]int, len(c.Nodes))
	// The control plane's own port participates in the uniqueness check --
	// on a single-machine dev cluster it is competing for the same range.
	seenAddr := map[string]string{c.ControlPlane.Address(): "control_plane"}

	for i, n := range c.Nodes {
		where := fmt.Sprintf("nodes[%d]", i)

		switch {
		case n.NodeID == "":
			problems = append(problems, fmt.Errorf("%s.node_id: must not be empty", where))
		default:
			if prev, dup := seenID[n.NodeID]; dup {
				problems = append(problems, fmt.Errorf(
					"%s.node_id: %q already used by nodes[%d]", where, n.NodeID, prev))
			}
			seenID[n.NodeID] = i
		}

		if err := validatePort(where+".port", n.Port); err != nil {
			problems = append(problems, err)
			continue
		}
		addr := n.Address()
		if owner, dup := seenAddr[addr]; dup {
			problems = append(problems, fmt.Errorf("%s: address %s already used by %s", where, addr, owner))
		}
		seenAddr[addr] = where
	}

	return errors.Join(problems...)
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

// ShardMap assigns every logical shard to a node.
//
// Phase 0 placeholder: plain round-robin over the configured node list. It is
// deterministic and obviously not the real strategy -- Phase 2.1 replaces this
// with rendezvous (highest-random-weight) hashing, which is what gives the
// minimal-key-movement property the design doc requires. Nothing should be
// built on the *distribution* this produces; only on the shape of the map.
func (c *Config) ShardMap() map[uint32]string {
	m := make(map[uint32]string, c.ShardCount)
	if len(c.Nodes) == 0 {
		return m
	}
	for shard := uint32(0); shard < c.ShardCount; shard++ {
		m[shard] = c.Nodes[int(shard)%len(c.Nodes)].NodeID
	}
	return m
}
