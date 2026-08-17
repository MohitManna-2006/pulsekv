// Package client provides the public, LLM-agnostic PulseKV cluster SDK.
//
// A Client discovers the static cluster topology from ClusterMetadataService,
// routes each key through the cluster's rendezvous-hash shard map, and reuses
// gRPC connections to the owning data-plane nodes. Applications should import
// this package rather than depending on the internal routing or transport
// packages directly.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/router"
	"pulsekv/control/internal/transport"
)

const (
	defaultRefreshInterval = 5 * time.Second
	defaultRefreshTimeout  = 5 * time.Second
	// The unary wire limit is 4 MiB. Leave room for protobuf framing while
	// still bounding accidental oversized unary messages.
	defaultMaxMessageBytes = 8 * 1024 * 1024
)

// ErrClosed is returned when an operation is attempted after Close.
var ErrClosed = errors.New("pulsekv client is closed")

type options struct {
	refreshInterval time.Duration
	refreshTimeout  time.Duration
	dialOptions     []grpc.DialOption
}

// Option customises a Client.
type Option func(*options) error

// WithRefreshInterval changes how often the client polls cluster metadata.
// A zero interval disables background refresh, which is useful for short-lived
// tools and tests. Phase 2 topology is static, so refresh is deliberately a
// simple poll rather than push invalidation.
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

type topology struct {
	shardCount uint32
	shardMap   map[uint32]string
	nodes      map[string]string // node ID -> NodeService address
}

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

	mu          sync.RWMutex
	closed      bool
	closeErr    error
	topology    topology
	nodeConns   map[string]*grpc.ClientConn
	nodeClients map[string]nodev1.NodeServiceClient
}

// New connects to a PulseKV control plane, fetches and validates the current
// topology, and starts periodic metadata refresh. The initial metadata fetch is
// eager: a successful return means the client has a complete routable map.
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
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	node, err := c.clientForKey(key)
	if err != nil {
		return err
	}
	if err := transport.Put(ctx, node, key, value); err != nil {
		return fmt.Errorf("put key: %w", err)
	}
	return nil
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
	nodesResp, err := c.metadata.GetNodeList(ctx, &metadatav1.GetNodeListRequest{})
	if err != nil {
		return fmt.Errorf("get node list: %w", err)
	}
	shardsResp, err := c.metadata.GetShardMap(ctx, &metadatav1.GetShardMapRequest{})
	if err != nil {
		return fmt.Errorf("get shard map: %w", err)
	}

	next, err := validateTopology(nodesResp.GetNodes(), shardsResp.GetShardToNodeId())
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.topology = next
	return nil
}

func validateTopology(nodes []*metadatav1.NodeInfo, shardMap map[uint32]string) (topology, error) {
	if len(nodes) == 0 {
		return topology{}, errors.New("metadata returned no nodes")
	}
	byID := make(map[string]string, len(nodes))
	byAddress := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node == nil || node.GetNodeId() == "" || node.GetAddress() == "" {
			return topology{}, errors.New("metadata returned a node with an empty ID or address")
		}
		if _, exists := byID[node.GetNodeId()]; exists {
			return topology{}, fmt.Errorf("metadata returned duplicate node ID %q", node.GetNodeId())
		}
		if previousID, exists := byAddress[node.GetAddress()]; exists {
			return topology{}, fmt.Errorf("metadata returned duplicate node address %q for IDs %q and %q",
				node.GetAddress(), previousID, node.GetNodeId())
		}
		byID[node.GetNodeId()] = node.GetAddress()
		byAddress[node.GetAddress()] = node.GetNodeId()
	}
	if len(shardMap) == 0 {
		return topology{}, errors.New("metadata returned an empty shard map")
	}
	if uint64(len(shardMap)) > uint64(^uint32(0)) {
		return topology{}, errors.New("metadata shard map is too large")
	}
	shardCount := uint32(len(shardMap))
	shards := make(map[uint32]string, len(shardMap))
	for shard := uint32(0); shard < shardCount; shard++ {
		owner, ok := shardMap[shard]
		if !ok {
			return topology{}, fmt.Errorf("metadata shard map is missing shard %d", shard)
		}
		if _, ok := byID[owner]; !ok {
			return topology{}, fmt.Errorf("metadata shard %d has unknown owner %q", shard, owner)
		}
		shards[shard] = owner
	}
	return topology{shardCount: shardCount, shardMap: shards, nodes: byID}, nil
}

func (c *Client) clientForKey(key []byte) (nodev1.NodeServiceClient, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	owner, ok := router.OwnerForKey(key, c.topology.shardCount, c.topology.shardMap)
	if !ok {
		shard := router.ShardForKey(key, c.topology.shardCount)
		c.mu.RUnlock()
		return nil, fmt.Errorf("no owner for shard %d", shard)
	}
	address, ok := c.topology.nodes[owner]
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
	if node := c.nodeClients[address]; node != nil {
		c.mu.RUnlock()
		return node, nil
	}
	c.mu.RUnlock()

	conn, err := grpc.NewClient(address, c.dialOptions...)
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
	seen := make(map[string]bool, len(c.topology.nodes))
	addresses := make([]string, 0, len(c.topology.nodes))
	for _, address := range c.topology.nodes {
		if !seen[address] {
			seen[address] = true
			addresses = append(addresses, address)
		}
	}
	sort.Strings(addresses)
	return addresses, nil
}
