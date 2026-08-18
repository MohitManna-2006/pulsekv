package metastore

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"pulsekv/control/internal/membership"
)

const (
	// How many snapshots to keep on disk. Two is the usual floor: enough that a
	// snapshot being written cannot leave the replica with none to restore from.
	retainedSnapshots = 2

	// Raft transport dial pool and timeout. A three-replica local group needs
	// almost nothing here; the timeout matters more than the pool.
	transportMaxPool = 3
	transportTimeout = 3 * time.Second
)

// Peer is one voter in the metadata group.
type Peer struct {
	NodeID  string
	Address string // Raft transport address, not the gRPC one
}

// Config describes this replica's participation in the metadata group.
type Config struct {
	// NodeID must be stable across restarts: it is the Raft server ID, and a
	// replica returning under a new one looks like a different voter.
	NodeID string

	// BindAddress is where this replica's Raft transport listens. Peers is the
	// complete group, including this replica, in an order every replica agrees
	// on -- they all read it from the same config file.
	BindAddress string
	Peers       []Peer

	// DataDir is owned exclusively by this replica. Unlike the engine's spill
	// tier this is meant to survive a restart: it is what lets a rejoining
	// replica catch up from where it left off instead of replaying everything.
	// Empty selects in-memory stores, which is appropriate ONLY for tests --
	// a real replica that forgets its log on restart is not a Raft voter, it is
	// a liability.
	DataDir string

	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotThreshold  uint64

	// ApplyTimeout bounds one proposal. Only the bridge proposes, and only when
	// it holds leadership, so this is a liveness guard rather than a hot path.
	ApplyTimeout time.Duration

	Logger *log.Logger
}

// Store owns this replica's Raft instance and exposes the committed state as a
// membership.Source.
//
// The Source implementation is the whole point: metadata.Service takes one of
// these instead of a gossip Manager and is otherwise unchanged. It never learns
// that consensus is involved.
type Store struct {
	config    Config
	fsm       *fsm
	raft      *raft.Raft
	transport *raft.NetworkTransport
	boltStore *raftboltdb.BoltStore
}

var _ membership.Source = (*Store)(nil)

// ErrNotLeader is returned by Propose on a replica that does not currently hold
// leadership. It is a normal outcome, not a failure: exactly one replica may
// propose, and every other one is expected to see this.
var ErrNotLeader = errors.New("metastore: this replica is not the Raft leader")

// New starts this replica's Raft instance and, if the group has no existing
// state, bootstraps it from Config.Peers.
//
// Every replica may call BootstrapCluster with the same peer list: hashicorp/raft
// refuses the ones that arrive after the first, which is exactly the behaviour
// wanted here. It removes the need for a designated bootstrap replica, and with
// it the failure mode where that one replica is the one that did not start.
func New(cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Warn, // Info narrates every heartbeat; Warn is the signal
		Output: loggerOutput(cfg.Logger),
	})

	advertise, err := net.ResolveTCPAddr("tcp", cfg.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("resolve raft bind address %s: %w", cfg.BindAddress, err)
	}
	transport, err := raft.NewTCPTransportWithLogger(
		cfg.BindAddress, advertise, transportMaxPool, transportTimeout, logger)
	if err != nil {
		return nil, fmt.Errorf("start raft transport on %s: %w", cfg.BindAddress, err)
	}

	logStore, stableStore, snapshotStore, boltStore, err := openStores(cfg, logger)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = logger
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold
	// CommitTimeout bounds how long a leader batches before flushing followers.
	// Well under the heartbeat so a membership change commits promptly.
	raftConfig.CommitTimeout = cfg.HeartbeatTimeout / 10

	machine := newFSM()
	node, err := raft.NewRaft(raftConfig, machine, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		closeStores(transport, boltStore)
		return nil, fmt.Errorf("start raft node %s: %w", cfg.NodeID, err)
	}

	store := &Store{
		config:    cfg,
		fsm:       machine,
		raft:      node,
		transport: transport,
		boltStore: boltStore,
	}

	existing, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("inspect existing raft state: %w", err)
	}
	if !existing {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(peer.NodeID),
				Address:  raft.ServerAddress(peer.Address),
			})
		}
		// Losing this race is the expected case for two of three replicas.
		if err := node.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil &&
			!errors.Is(err, raft.ErrCantBootstrap) {
			_ = store.Close()
			return nil, fmt.Errorf("bootstrap raft group: %w", err)
		}
	}
	return store, nil
}

func (c *Config) validate() error {
	var problems []error
	if c.NodeID == "" {
		problems = append(problems, errors.New("metastore node ID must not be empty"))
	}
	if c.BindAddress == "" {
		problems = append(problems, errors.New("metastore bind address must not be empty"))
	}
	if len(c.Peers) == 0 {
		problems = append(problems, errors.New("metastore peer list must not be empty"))
	}
	found := false
	seen := make(map[string]bool, len(c.Peers))
	for _, peer := range c.Peers {
		if peer.NodeID == "" || peer.Address == "" {
			problems = append(problems, errors.New("metastore peer must have an ID and an address"))
			continue
		}
		if seen[peer.NodeID] {
			problems = append(problems, fmt.Errorf("metastore peer %q appears twice", peer.NodeID))
		}
		seen[peer.NodeID] = true
		found = found || peer.NodeID == c.NodeID
	}
	// A replica missing from its own peer list would start, fail to bootstrap
	// into the group, and then quietly never be able to vote.
	if len(c.Peers) > 0 && !found {
		problems = append(problems, fmt.Errorf(
			"metastore node %q is not in its own peer list", c.NodeID))
	}
	if c.HeartbeatTimeout <= 0 || c.ElectionTimeout <= 0 || c.LeaderLeaseTimeout <= 0 {
		problems = append(problems, errors.New("metastore raft timeouts must be positive"))
	}
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		problems = append(problems, fmt.Errorf(
			"metastore leader lease timeout %s exceeds heartbeat timeout %s",
			c.LeaderLeaseTimeout, c.HeartbeatTimeout))
	}
	if c.ApplyTimeout <= 0 {
		problems = append(problems, errors.New("metastore apply timeout must be positive"))
	}
	return errors.Join(problems...)
}

func openStores(cfg Config, logger hclog.Logger) (raft.LogStore, raft.StableStore,
	raft.SnapshotStore, *raftboltdb.BoltStore, error) {

	if cfg.DataDir == "" {
		// Tests only. Explicitly not reachable from a config file: config.Raft
		// requires a non-empty data_dir.
		inmem := raft.NewInmemStore()
		return inmem, inmem, raft.NewInmemSnapshotStore(), nil, nil
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("create raft data directory %s: %w", cfg.DataDir, err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open raft log store in %s: %w", cfg.DataDir, err)
	}
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, retainedSnapshots, logger)
	if err != nil {
		_ = boltStore.Close()
		return nil, nil, nil, nil, fmt.Errorf("open raft snapshot store in %s: %w", cfg.DataDir, err)
	}
	return boltStore, boltStore, snapshots, boltStore, nil
}

func closeStores(transport *raft.NetworkTransport, boltStore *raftboltdb.BoltStore) {
	if transport != nil {
		_ = transport.Close()
	}
	if boltStore != nil {
		_ = boltStore.Close()
	}
}

func loggerOutput(logger *log.Logger) io.Writer {
	if logger == nil {
		return os.Stderr
	}
	return logger.Writer()
}

// Snapshot implements membership.Source from committed state.
//
// It is answered from THIS replica's applied log, with no round trip to the
// leader. That is safe because Raft guarantees an applied log is a prefix of
// the leader's committed log: a follower's answer can be behind, but it can
// never be a different reality. See docs/pulsekv-v2-phase5-summary.md for the
// measured staleness bound.
func (s *Store) Snapshot() membership.Snapshot {
	return s.fsm.State().Snapshot()
}

// State returns the raw committed state, for the bridge's comparison and for
// diagnostics.
func (s *Store) State() State { return s.fsm.State() }

// Leader reports the current leader's server ID and term as this replica
// understands them. Both are diagnostic: they are deliberately not part of the
// topology fingerprint, because two replicas at the same committed state must
// produce the same fingerprint even if one has not yet noticed an election.
//
// An empty ID means this replica currently sees no leader, which is the honest
// answer during an election rather than a stale guess.
func (s *Store) Leader() (string, uint64) {
	_, id := s.raft.LeaderWithID()
	return string(id), s.raft.CurrentTerm()
}

// IsLeader reports whether this replica may propose.
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// LeaderCh exposes Raft's leadership transition channel so a caller can react
// to gaining or losing leadership without polling.
func (s *Store) LeaderCh() <-chan bool { return s.raft.LeaderCh() }

// Propose commits desired as the new agreed state.
//
// Only the leader may call this and only the bridge does. A non-leader gets
// ErrNotLeader rather than a silent no-op, which is what makes the chaos
// harness's fencing check possible to state positively.
func (s *Store) Propose(desired State) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	raw, err := encodeCommand(desired)
	if err != nil {
		return err
	}
	future := s.raft.Apply(raw, s.config.ApplyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) ||
			errors.Is(err, raft.ErrLeadershipTransferInProgress) {
			// Leadership moved between the check above and the commit. The new
			// leader will propose the same difference from its own view.
			return fmt.Errorf("%w: %v", ErrNotLeader, err)
		}
		return fmt.Errorf("commit metadata proposal: %w", err)
	}
	// Apply returns whatever fsm.Apply returned, which is an error only for a
	// malformed entry -- worth surfacing rather than treating a committed but
	// unapplied entry as success.
	if applyErr, ok := future.Response().(error); ok && applyErr != nil {
		return fmt.Errorf("apply metadata proposal: %w", applyErr)
	}
	return nil
}

// WaitForLeader blocks until this replica can name a leader, or the timeout
// expires. Used at startup so a process does not announce readiness while the
// group is still electing.
func (s *Store) WaitForLeader(timeout time.Duration) (string, uint64, error) {
	deadline := time.Now().Add(timeout)
	for {
		if id, term := s.Leader(); id != "" {
			return id, term, nil
		}
		if time.Now().After(deadline) {
			return "", 0, fmt.Errorf("no raft leader elected within %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Close shuts the Raft node down and releases the transport and stores.
func (s *Store) Close() error {
	var firstErr error
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil {
			firstErr = err
		}
	}
	if s.transport != nil {
		if err := s.transport.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.boltStore != nil {
		if err := s.boltStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
