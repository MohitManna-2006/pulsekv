// Package client provides the public, LLM-agnostic PulseKV cluster SDK.
//
// A Client discovers the live cluster topology from ClusterMetadataService,
// atomically installs coherent membership snapshots, routes each key through
// the cluster's rendezvous-hash shard map, and reuses gRPC connections to the
// owning data-plane nodes. Applications should import this package rather than
// depending on the internal routing or transport packages directly.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
	clustertopology "pulsekv/control/internal/topology"
	"pulsekv/control/internal/transport"
)

const (
	defaultRefreshInterval = 5 * time.Second
	defaultRefreshTimeout  = 5 * time.Second
	// The unary wire limit is 4 MiB. Leave room for protobuf framing while
	// still bounding accidental oversized unary messages.
	defaultMaxMessageBytes = 8 * 1024 * 1024
)

var (
	// ErrClosed is returned when an operation is attempted after Close.
	ErrClosed = errors.New("pulsekv client is closed")
	// ErrNoLiveNodes means metadata authoritatively published an empty live
	// membership. It is distinct from a metadata transport failure, for which
	// the client deliberately retains its last complete topology.
	ErrNoLiveNodes = errors.New("pulsekv cluster has no live data nodes")
)

type options struct {
	refreshInterval time.Duration
	refreshTimeout  time.Duration
	dialOptions     []grpc.DialOption
}

// Option customises a Client.
type Option func(*options) error

// WithRefreshInterval changes how often the client polls cluster metadata.
// A zero interval disables background refresh, which is useful for short-lived
// tools and tests. Refresh is deliberately a simple poll rather than push
// invalidation.
func WithRefreshInterval(interval time.Duration) Option {
	return func(o *options) error {
		if interval < 0 {
			return fmt.Errorf("refresh interval must not be negative: %s", interval)
		}
		o.refreshInterval = interval
		return nil
	}
}

// WithRefreshTimeout changes the deadline used by initial and background
// metadata polls.
func WithRefreshTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		if timeout <= 0 {
			return fmt.Errorf("refresh timeout must be positive: %s", timeout)
		}
		o.refreshTimeout = timeout
		return nil
	}
}

// WithDialOptions appends gRPC dial options for both control-plane and node
// connections. It is primarily useful for custom dialers and transport policy;
// later options override earlier gRPC settings when the setting is singular.
func WithDialOptions(dialOptions ...grpc.DialOption) Option {
	return func(o *options) error {
		o.dialOptions = append(o.dialOptions, dialOptions...)
		return nil
	}
}

type topology = clustertopology.Snapshot

// Client is a concurrency-safe PulseKV cluster client.
type Client struct {
	metadataConn *grpc.ClientConn
	metadata     metadatav1.ClusterMetadataServiceClient
	dialOptions  []grpc.DialOption

	refreshInterval time.Duration
	refreshTimeout  time.Duration
	refreshCancel   context.CancelFunc
	refreshDone     chan struct{}
	closeDone       chan struct{}
	refreshMu       sync.Mutex

	mu              sync.RWMutex
	closed          bool
	closeErr        error
	topology        topology
	nodeConns       map[string]*grpc.ClientConn
	nodeClients     map[string]nodev1.NodeServiceClient
	nodeConnFactory func(string) (*grpc.ClientConn, error)
}

// New connects to a PulseKV control plane, fetches and validates the current
// topology, and starts periodic metadata refresh. The initial metadata fetch is
// eager: a successful return means the client has a complete authoritative
// topology, which may explicitly contain no live data nodes.
func New(controlPlaneAddr string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(controlPlaneAddr) == "" {
		return nil, errors.New("control-plane address must not be empty")
	}

	cfg := options{
		refreshInterval: defaultRefreshInterval,
		refreshTimeout:  defaultRefreshTimeout,
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
				grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
			),
		},
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("configure client: %w", err)
		}
	}

	conn, err := grpc.NewClient(controlPlaneAddr, cfg.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("create control-plane client for %s: %w", controlPlaneAddr, err)
	}

	c := &Client{
		metadataConn:    conn,
		metadata:        metadatav1.NewClusterMetadataServiceClient(conn),
		dialOptions:     append([]grpc.DialOption(nil), cfg.dialOptions...),
		refreshInterval: cfg.refreshInterval,
		refreshTimeout:  cfg.refreshTimeout,
		refreshDone:     make(chan struct{}),
		closeDone:       make(chan struct{}),
		nodeConns:       make(map[string]*grpc.ClientConn),
		nodeClients:     make(map[string]nodev1.NodeServiceClient),
	}
	c.nodeConnFactory = func(address string) (*grpc.ClientConn, error) {
		return grpc.NewClient(address, c.dialOptions...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.refreshTimeout)
	err = c.refresh(ctx)
	cancel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("load PulseKV topology from %s: %w", controlPlaneAddr, err)
	}

	if c.refreshInterval > 0 {
		refreshCtx, refreshCancel := context.WithCancel(context.Background())
		c.refreshCancel = refreshCancel
		go c.refreshLoop(refreshCtx)
	} else {
		close(c.refreshDone)
	}
	return c, nil
}

// Get reads key from its owning node. found is false for a normal cache miss.
// Values above the unary limit are fetched through the chunked transport
// transparently.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	node, err := c.clientForKey(key)
	if err != nil {
		return nil, false, err
	}
	value, found, err = transport.Get(ctx, node, key)
	if err != nil {
		return nil, false, fmt.Errorf("get key: %w", err)
	}
	return value, found, nil
}

// Put writes key and value to the key's owning node. Values above the unary
// limit use the chunked transport transparently.
//
// The owning node commits locally and answers immediately; if it is the primary
// for the key's shard it then replicates in the background. Callers who need
// the write to be on replicas before Put returns should use PutWithAck.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.putWithAck(ctx, key, value, 0)
	return err
}

// PutWithAck writes key and value and blocks until acks replicas have stored
// it, returning how many actually acked.
//
// This is the design doc's "optional stronger mode": ordinary cache writes
// should not pay replication latency, but a caller storing something a
// recompute cannot replace can choose to. Concretely:
//
//   - acks == 0 behaves exactly like Put and returns 0. Nothing was waited for,
//     so nothing is claimed.
//   - acks > 0 returns only once that many replicas have the value. The count
//     returned can exceed acks when replicas ack faster than the primary can
//     stop counting.
//   - acks above the shard's live replica count fails with INVALID_ARGUMENT
//     rather than blocking until the deadline.
//
// An error from a strong-ack write does NOT mean the write was lost. The
// primary commits locally before forwarding anything and never rolls that back,
// so a failure here means "stored, but less replicated than you asked for".
// Retrying is safe; the write is idempotent.
//
// Replica addresses are deliberately not the SDK's concern. It routes to the
// primary exactly as it always has, and the primary owns the fan-out.
func (c *Client) PutWithAck(ctx context.Context, key, value []byte, acks uint32) (uint32, error) {
	return c.putWithAck(ctx, key, value, acks)
}

func (c *Client) putWithAck(ctx context.Context, key, value []byte, acks uint32) (uint32, error) {
	node, err := c.clientForKey(key)
	if err != nil {
		return 0, err
	}
	acked, err := transport.PutWithAck(ctx, node, key, value, acks)
	if err != nil {
		return 0, fmt.Errorf("put key: %w", err)
	}
	return acked, nil
}

// PrefixMatch returns the current values of keys matching prefix across every
// node in the cluster.
//
// A prefix scan is not a snapshot: a listed key can disappear before the SDK
// re-fetches its value. Such a miss is a normal race and is silently skipped.
// Any non-miss fetch or scan error still fails the operation.
func (c *Client) PrefixMatch(ctx context.Context, prefix []byte) (map[string][]byte, error) {
	addresses, err := c.nodeAddresses()
	if err != nil {
		return nil, err
	}

	keys := make(map[string][]byte)
	for _, address := range addresses {
		node, err := c.clientForAddress(address)
		if err != nil {
			return nil, err
		}
		stream, err := node.PrefixMatch(ctx, &nodev1.PrefixMatchRequest{Prefix: prefix})
		if err != nil {
			return nil, fmt.Errorf("scan node %s: %w", address, err)
		}
		for {
			match, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("scan node %s: %w", address, err)
			}
			key := append([]byte(nil), match.GetKey()...)
			keys[string(key)] = key
		}
	}

	values := make(map[string][]byte, len(keys))
	for text, key := range keys {
		value, found, err := c.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("fetch prefix match %q: %w", text, err)
		}
		if !found {
			continue
		}
		values[text] = value
	}
	return values, nil
}

// Close stops metadata polling and releases all gRPC connections. It is safe
// to call Close more than once; concurrent callers wait for the same shutdown
// and receive the same result.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	}
	c.closed = true
	cancel := c.refreshCancel
	done := c.refreshDone
	metadataConn := c.metadataConn
	conns := make([]*grpc.ClientConn, 0, len(c.nodeConns))
	for _, conn := range c.nodeConns {
		conns = append(conns, conn)
	}
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	<-done

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := metadataConn.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	c.mu.Lock()
	c.closeErr = firstErr
	close(c.closeDone)
	c.mu.Unlock()
	return firstErr
}

func (c *Client) refreshLoop(ctx context.Context) {
	defer close(c.refreshDone)
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, c.refreshTimeout)
			_ = c.refresh(refreshCtx) // retain the last complete topology on failure
			cancel()
		}
	}
}

func (c *Client) refresh(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	next, err := clustertopology.Fetch(ctx, c.metadata)
	if err != nil {
		return err
	}

	var retired []*grpc.ClientConn
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.topology = next
	for address, conn := range c.nodeConns {
		if next.OwnsAddress(address) {
			continue
		}
		delete(c.nodeConns, address)
		delete(c.nodeClients, address)
		retired = append(retired, conn)
	}
	c.mu.Unlock()

	// Closing outside mu avoids blocking routing and shutdown on gRPC cleanup.
	// Removed entries are disjoint from the active connections Close snapshots.
	for _, conn := range retired {
		_ = conn.Close()
	}
	return nil
}

func validateTopology(nodes []*metadatav1.NodeInfo, shardMap map[uint32]string) (topology, error) {
	return clustertopology.Validate(0, nodes, shardMap)
}

func (c *Client) clientForKey(key []byte) (nodev1.NodeServiceClient, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	if len(c.topology.Nodes) == 0 || len(c.topology.ShardMap) == 0 {
		c.mu.RUnlock()
		return nil, ErrNoLiveNodes
	}
	owner, ok := router.OwnerForKey(key, c.topology.ShardCount, c.topology.ShardMap)
	if !ok {
		shard := router.ShardForKey(key, c.topology.ShardCount)
		c.mu.RUnlock()
		return nil, fmt.Errorf("no owner for shard %d", shard)
	}
	address, ok := c.topology.Nodes[owner]
	c.mu.RUnlock()
	if !ok || address == "" {
		return nil, fmt.Errorf("owner %q has no node address", owner)
	}
	return c.clientForAddress(address)
}

func (c *Client) clientForAddress(address string) (nodev1.NodeServiceClient, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	if !c.topology.OwnsAddress(address) {
		c.mu.RUnlock()
		return nil, fmt.Errorf("node address %s no longer owns a shard", address)
	}
	if node := c.nodeClients[address]; node != nil {
		c.mu.RUnlock()
		return node, nil
	}
	c.mu.RUnlock()

	conn, err := c.nodeConnFactory(address)
	if err != nil {
		return nil, fmt.Errorf("create node client for %s: %w", address, err)
	}
	node := nodev1.NewNodeServiceClient(conn)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return nil, ErrClosed
	}
	if !c.topology.OwnsAddress(address) {
		_ = conn.Close()
		return nil, fmt.Errorf("node address %s stopped owning shards while its connection was created", address)
	}
	if existing := c.nodeClients[address]; existing != nil {
		_ = conn.Close()
		return existing, nil
	}
	c.nodeConns[address] = conn
	c.nodeClients[address] = node
	return node, nil
}

func (c *Client) nodeAddresses() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	return c.topology.OwnerAddresses(), nil
}
