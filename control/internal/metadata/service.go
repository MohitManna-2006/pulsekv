// Package metadata implements ClusterMetadataService.
//
// Phase 0 scope: the node list and shard map are static, read once from
// deploy/cluster.config.yaml at startup. The only thing computed at request
// time is NodeInfo.alive, which is a direct, bounded NodeService.HealthCheck
// probe of each node -- see aliveness() for why that is not the same thing as
// the gossip membership Phase 3 adds.
package metadata

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
)

// DefaultProbeTimeout bounds how long GetNodeList will wait on an unresponsive
// node before calling it not-alive. Kept short: the caller is asking for the
// cluster's shape, not for a definitive verdict on any one node.
const DefaultProbeTimeout = 300 * time.Millisecond

// Service serves ClusterMetadataService from static configuration.
type Service struct {
	metadatav1.UnimplementedClusterMetadataServiceServer

	cfg      *config.Config
	shardMap map[uint32]string // immutable after New
	started  time.Time

	probeTimeout time.Duration
	// clients is keyed by node ID. grpc.NewClient is lazy, so constructing
	// these up front costs nothing until the first probe and saves a
	// connection setup per GetNodeList afterwards.
	clients map[string]nodev1.NodeServiceClient
	conns   []*grpc.ClientConn
}

// Option customises a Service.
type Option func(*Service)

// WithProbeTimeout overrides how long each liveness probe may take.
func WithProbeTimeout(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.probeTimeout = d
		}
	}
}

// WithStartTime pins the instant uptime is measured from. Used by tests; the
// server otherwise uses process start.
func WithStartTime(t time.Time) Option {
	return func(s *Service) { s.started = t }
}

// New builds a Service over cfg. The returned Service owns one lazy gRPC
// client connection per configured node and must be Closed.
func New(cfg *config.Config, opts ...Option) (*Service, error) {
	s := &Service{
		cfg:          cfg,
		shardMap:     cfg.ShardMap(),
		started:      time.Now(),
		probeTimeout: DefaultProbeTimeout,
		clients:      make(map[string]nodev1.NodeServiceClient, len(cfg.Nodes)),
	}
	for _, o := range opts {
		o(s)
	}

	for _, n := range cfg.Nodes {
		conn, err := grpc.NewClient(n.Address(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			s.Close()
			return nil, err
		}
		s.conns = append(s.conns, conn)
		s.clients[n.NodeID] = nodev1.NewNodeServiceClient(conn)
	}
	return s, nil
}

// Close releases the per-node client connections.
func (s *Service) Close() error {
	var firstErr error
	for _, c := range s.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.conns = nil
	return firstErr
}

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

// GetNodeList returns the statically configured nodes, each annotated with a
// freshly probed liveness bit.
func (s *Service) GetNodeList(ctx context.Context, _ *metadatav1.GetNodeListRequest) (*metadatav1.GetNodeListResponse, error) {
	alive := s.aliveness(ctx)

	nodes := make([]*metadatav1.NodeInfo, 0, len(s.cfg.Nodes))
	for _, n := range s.cfg.Nodes {
		nodes = append(nodes, &metadatav1.NodeInfo{
			NodeId:  n.NodeID,
			Address: n.Address(),
			Alive:   alive[n.NodeID],
		})
	}
	return &metadatav1.GetNodeListResponse{Nodes: nodes}, nil
}

// GetShardMap returns the static shard assignment computed at startup.
func (s *Service) GetShardMap(_ context.Context, _ *metadatav1.GetShardMapRequest) (*metadatav1.GetShardMapResponse, error) {
	return &metadatav1.GetShardMapResponse{ShardToNodeId: s.shardMap}, nil
}

// aliveness probes every node's NodeService.HealthCheck in parallel, bounded
// by probeTimeout.
//
// This is deliberately *not* membership. It is a point-in-time question asked
// synchronously by the caller, with no failure detector, no suspicion state,
// and no effect on the shard map -- a node that fails this probe still owns
// its shards. Phase 3 replaces this with SWIM gossip, at which point aliveness
// becomes a maintained view rather than a probe, and starts feeding shard
// recomputation. Reporting alive=true unconditionally here would have been
// less code and a lie.
func (s *Service) aliveness(ctx context.Context) map[string]bool {
	ctx, cancel := context.WithTimeout(ctx, s.probeTimeout)
	defer cancel()

	var (
		mu     sync.Mutex
		result = make(map[string]bool, len(s.clients))
		wg     sync.WaitGroup
	)

	for nodeID, client := range s.clients {
		wg.Add(1)
		go func(nodeID string, client nodev1.NodeServiceClient) {
			defer wg.Done()

			resp, err := client.HealthCheck(ctx, &nodev1.HealthCheckRequest{})
			ok := err == nil && resp.GetOk()

			mu.Lock()
			result[nodeID] = ok
			mu.Unlock()
		}(nodeID, client)
	}
	wg.Wait()
	return result
}

// LogSummary prints what this service is serving, once, at startup. Useful
// because "the control plane is up" and "the control plane is up with the
// config you meant" are different claims.
func (s *Service) LogSummary() {
	log.Printf("metadata: serving %d node(s) and %d shard(s) from %s",
		len(s.cfg.Nodes), s.cfg.ShardCount, s.cfg.Path)
	for _, n := range s.cfg.Nodes {
		log.Printf("metadata:   %-10s %s", n.NodeID, n.Address())
	}
}
