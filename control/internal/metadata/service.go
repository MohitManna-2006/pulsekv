// Package metadata implements ClusterMetadataService.
//
// Phase 3 serves immutable generations from the SWIM gossip membership view.
// Every published node is an active data member, and the shard map is the pure
// rendezvous-hash assignment over exactly that generation's node IDs.
package metadata

import (
	"context"
	"errors"
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

	shardCount uint32
	source     membership.Source
	configPath string
	started    time.Time
}

// Option customises a Service.
type Option func(*Service)

// WithStartTime pins the instant uptime is measured from. Used by tests; the
// server otherwise uses process start.
func WithStartTime(t time.Time) Option {
	return func(s *Service) { s.started = t }
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
	s := &Service{
		shardCount: cfg.ShardCount,
		source:     source,
		configPath: cfg.Path,
		started:    time.Now(),
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
	return &metadatav1.GetNodeListResponse{
		Nodes:               nodes,
		TopologyGeneration:  snapshot.Generation,
		TopologyFingerprint: topologyFingerprint(s.shardCount, snapshot.Nodes, nil),
	}, nil
}

// GetShardMap returns the assignment for one published membership generation.
func (s *Service) GetShardMap(_ context.Context, _ *metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	snapshot, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	nodeIDs := make([]string, 0, len(snapshot.Nodes))
	for _, n := range snapshot.Nodes {
		nodeIDs = append(nodeIDs, n.NodeID)
	}
	shardMap := router.AssignShards(nodeIDs, s.shardCount)
	return &metadatav1.GetShardMapResponse{
		ShardToNodeId:       shardMap,
		TopologyGeneration:  snapshot.Generation,
		TopologyFingerprint: topologyFingerprint(s.shardCount, snapshot.Nodes, shardMap),
		ShardCount:          s.shardCount,
	}, nil
}

// topologyFingerprint is the cross-publisher identity used to join the two
// metadata RPCs safely. The membership generation is intentionally not part of
// it: two observers can reach the same valid topology through different event
// histories, while a restarted observer can reuse an old local generation for
// different content.
func topologyFingerprint(shardCount uint32, nodes []membership.Node, shardMap map[uint32]string) []byte {
	addresses := make(map[string]string, len(nodes))
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		addresses[node.NodeID] = node.Address
		ids = append(ids, node.NodeID)
	}
	if shardMap == nil {
		shardMap = router.AssignShards(ids, shardCount)
	}
	return clustertopology.Fingerprint(shardCount, addresses, shardMap)
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
	log.Printf("metadata: gossip generation %d has %d live data node(s); serving %d shard(s) (bootstrap %s)",
		snapshot.Generation, len(snapshot.Nodes), s.shardCount, where)
	for _, n := range snapshot.Nodes {
		log.Printf("metadata:   %-10s %s", n.NodeID, n.Address)
	}
}
