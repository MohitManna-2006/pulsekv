// Package metadata implements ClusterMetadataService.
//
// Phase 3 serves immutable generations from the SWIM gossip membership view.
// Every published node is an active data member, and the shard map is the pure
// rendezvous-hash assignment over exactly that generation's node IDs.
//
// Phase 4 adds primary-plus-replica placement to the same generation. This
// package's entire role in replication is deciding placement: it never sits in
// a write path. A data node reads the owner map, learns which peers hold copies
// of the shards it primaries, and does the forwarding itself.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
)

// Service serves ClusterMetadataService from a maintained membership source.
type Service struct {
	metadatav1.UnimplementedClusterMetadataServiceServer

	shardCount        uint32
	replicationFactor int
	source            membership.Source
	configPath        string
	started           time.Time
	leaderInfo        func() (string, uint64)
}

// Option customises a Service.
type Option func(*Service)

// WithStartTime pins the instant uptime is measured from. Used by tests; the
// server otherwise uses process start.
func WithStartTime(t time.Time) Option {
	return func(s *Service) { s.started = t }
}

// WithLeaderInfo supplies the Raft leader ID and term reported on
// GetShardMapResponse.
//
// Purely diagnostic, and deliberately kept out of the topology fingerprint: two
// replicas holding the same committed state must produce the same fingerprint
// even when one has not yet noticed an election. It exists so the chaos harness
// can assert "the leader actually changed" directly instead of inferring it
// from timing.
//
// Unset on a control plane with no Raft group, which reports an empty ID and
// term 0 — an honest "not applicable" rather than a fabricated value.
func WithLeaderInfo(fn func() (leaderID string, term uint64)) Option {
	return func(s *Service) { s.leaderInfo = fn }
}

// New builds a Service over cfg's fixed shard count and a live membership
// source. The config's Nodes remain launch inventory and are not consulted by
// request handlers.
func New(cfg *config.Config, source membership.Source, opts ...Option) (*Service, error) {
	if cfg == nil {
		return nil, errors.New("metadata config must not be nil")
	}
	if source == nil {
		return nil, errors.New("metadata membership source must not be nil")
	}
	if cfg.ShardCount == 0 {
		return nil, errors.New("metadata shard count must be positive")
	}
	if cfg.ReplicationFactor < 0 {
		return nil, fmt.Errorf("metadata replication factor must not be negative, got %d",
			cfg.ReplicationFactor)
	}
	s := &Service{
		shardCount:        cfg.ShardCount,
		replicationFactor: cfg.ReplicationFactor,
		source:            source,
		configPath:        cfg.Path,
		started:           time.Now(),
	}
	for _, o := range opts {
		o(s)
	}

	return s, nil
}

// Close exists for lifecycle symmetry. The membership manager is owned and
// closed by the control-plane process, not by this service.
func (s *Service) Close() error { return nil }

// Register wires the service into a gRPC server.
func (s *Service) Register(srv grpc.ServiceRegistrar) {
	metadatav1.RegisterClusterMetadataServiceServer(srv, s)
}

// HealthCheck reports real liveness and uptime for the control plane itself.
func (s *Service) HealthCheck(_ context.Context, _ *metadatav1.HealthCheckRequest) (*metadatav1.HealthCheckResponse, error) {
	return &metadatav1.HealthCheckResponse{
		Ok:            true,
		UptimeSeconds: int64(time.Since(s.started).Seconds()),
	}, nil
}

// GetNodeList returns the active data nodes in one published generation.
func (s *Service) GetNodeList(_ context.Context, _ *metadatav1.GetNodeListRequest) (*metadatav1.GetNodeListResponse, error) {
	snapshot, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	nodes := make([]*metadatav1.NodeInfo, 0, len(snapshot.Nodes))
	for _, n := range snapshot.Nodes {
		nodes = append(nodes, &metadatav1.NodeInfo{
			NodeId:  n.NodeID,
			Address: n.Address,
			Alive:   true,
		})
	}
	placement, err := s.placement(snapshot)
	if err != nil {
		return nil, err
	}
	return &metadatav1.GetNodeListResponse{
		Nodes:               nodes,
		TopologyGeneration:  snapshot.Generation,
		TopologyFingerprint: clustertopology.Fingerprint(placement.fingerprintInput()),
	}, nil
}

// GetShardMap returns the assignment for one published membership generation.
//
// Both views of ownership come from the same placement computation, so they
// cannot describe different generations: shard_to_node_id is exactly the
// primary column of shard_to_owners, and placement() refuses to return a pair
// where that is not true.
func (s *Service) GetShardMap(_ context.Context, _ *metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	snapshot, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	placement, err := s.placement(snapshot)
	if err != nil {
		return nil, err
	}

	wireOwners := make(map[uint32]*metadatav1.ShardOwners, len(placement.owners))
	for shard, owner := range placement.owners {
		entry := &metadatav1.ShardOwners{Primary: owner.Primary}
		if len(owner.Replicas) > 0 {
			entry.Replicas = append([]string(nil), owner.Replicas...)
		}
		wireOwners[shard] = entry
	}

	leaderID, term := s.leader()
	return &metadatav1.GetShardMapResponse{
		ShardToNodeId:       placement.shardMap,
		TopologyGeneration:  snapshot.Generation,
		TopologyFingerprint: clustertopology.Fingerprint(placement.fingerprintInput()),
		ShardCount:          s.shardCount,
		ShardToOwners:       wireOwners,
		ReplicationFactor:   placement.replicationFactor,
		RaftLeaderId:        leaderID,
		RaftTerm:            term,
	}, nil
}

// leader reports the metadata group's current leader as this replica sees it,
// or an empty ID when there is no Raft group at all.
func (s *Service) leader() (string, uint64) {
	if s.leaderInfo == nil {
		return "", 0
	}
	return s.leaderInfo()
}

// placement is one generation's complete, self-consistent ownership decision.
type placement struct {
	shardCount        uint32
	replicationFactor uint32
	addresses         map[string]string
	shardMap          map[uint32]string
	owners            map[uint32]router.ShardOwners
}

func (p placement) fingerprintInput() clustertopology.FingerprintInput {
	return clustertopology.FingerprintInput{
		ShardCount:        p.shardCount,
		ReplicationFactor: p.replicationFactor,
		Nodes:             p.addresses,
		ShardMap:          p.shardMap,
		Owners:            p.owners,
	}
}

// placement computes both ownership views from one membership snapshot and
// asserts they agree before either can reach a client.
//
// The assertion is not defensive padding. router.AssignShardOwners is a second
// implementation of the same argmax that router.AssignShards performs, sharing
// only the weight function; a divergence between them would send every
// pre-Phase-4 consumer -- the SDK, the smoke test, the chaos watcher -- to a
// node that does not hold the shard, and nothing downstream would notice.
// Failing the RPC is strictly better than publishing a map that lies.
func (s *Service) placement(snapshot membership.Snapshot) (placement, error) {
	addresses := make(map[string]string, len(snapshot.Nodes))
	nodeIDs := make([]string, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		addresses[node.NodeID] = node.Address
		nodeIDs = append(nodeIDs, node.NodeID)
	}

	// The replication factor comes from the snapshot when the source is
	// authoritative about configuration, and from local config when it is not.
	//
	// Phase 5's Raft-backed source sets it; gossip leaves it nil, so a Phase 3/4
	// control plane behaves byte-identically. Taking it from the SNAPSHOT rather
	// than reading it separately is what keeps this coherent: the node set and
	// the factor are both inputs to the computation below, and fetching them
	// through two calls could interleave with a Raft apply and produce a shard
	// map describing a state that never existed.
	replicationFactor := s.replicationFactor
	if snapshot.ReplicationFactor != nil {
		replicationFactor = *snapshot.ReplicationFactor
	}
	if replicationFactor < 0 {
		return placement{}, status.Errorf(codes.Internal,
			"agreed replication factor %d is negative", replicationFactor)
	}

	shardMap := router.AssignShards(nodeIDs, s.shardCount)
	owners := router.AssignShardOwners(nodeIDs, s.shardCount, replicationFactor)
	if len(shardMap) != len(owners) {
		return placement{}, status.Errorf(codes.Internal,
			"shard map has %d entries but the owner map has %d", len(shardMap), len(owners))
	}
	for shard, owner := range shardMap {
		if owners[shard].Primary != owner {
			return placement{}, status.Errorf(codes.Internal,
				"shard %d owner %q disagrees with computed primary %q",
				shard, owner, owners[shard].Primary)
		}
	}

	return placement{
		shardCount:        s.shardCount,
		replicationFactor: uint32(replicationFactor),
		addresses:         addresses,
		shardMap:          shardMap,
		owners:            owners,
	}, nil
}

func (s *Service) snapshot() (membership.Snapshot, error) {
	snapshot := s.source.Snapshot()
	seenIDs := make(map[string]bool, len(snapshot.Nodes))
	seenAddresses := make(map[string]bool, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node.NodeID == "" || node.Address == "" {
			return membership.Snapshot{}, status.Error(codes.Internal,
				"membership source returned an empty node ID or address")
		}
		if seenIDs[node.NodeID] {
			return membership.Snapshot{}, status.Errorf(codes.Internal,
				"membership source returned duplicate node ID %q", node.NodeID)
		}
		if seenAddresses[node.Address] {
			return membership.Snapshot{}, status.Errorf(codes.Internal,
				"membership source returned duplicate node address %q", node.Address)
		}
		seenIDs[node.NodeID] = true
		seenAddresses[node.Address] = true
	}
	return snapshot, nil
}

// LogSummary prints what this service is serving, once, at startup. Useful
// because "the control plane is up" and "the control plane is up with the
// config you meant" are different claims.
func (s *Service) LogSummary() {
	snapshot := s.source.Snapshot()
	where := s.configPath
	if where == "" {
		where = "launch config"
	}
	log.Printf("metadata: gossip generation %d has %d live data node(s); serving %d shard(s) "+
		"at replication factor %d (bootstrap %s)",
		snapshot.Generation, len(snapshot.Nodes), s.shardCount, s.replicationFactor, where)
	for _, n := range snapshot.Nodes {
		log.Printf("metadata:   %-10s %s", n.NodeID, n.Address)
	}
}
