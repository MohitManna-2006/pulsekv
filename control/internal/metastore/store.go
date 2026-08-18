package metastore

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
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

	// logStore is retained so ServeReady can distinguish "the FSM has applied
	// everything committed" from "there is nothing committed to apply".
	logStore raft.LogStore

	// startupIndex is the last index this replica already had on disk when the
	// process started -- log or snapshot, whichever is further along. It is the
	// floor ServeReady catches up to, because a replica must never answer from
	// a state older than its own persisted log.
	startupIndex uint64

	// caughtUp latches the readiness decision. See ServeReady for why it
	// latches rather than tracking liveness.
	caughtUp atomic.Bool
}

var (
	_ membership.Source    = (*Store)(nil)
	_ membership.Readiness = (*Store)(nil)
)

// ErrNotLeader is returned by Propose on a replica that does not currently hold
// leadership. It is a normal outcome, not a failure: exactly one replica may
// propose, and every other one is expected to see this.
var ErrNotLeader = errors.New("metastore: this replica is not the Raft leader")

// ErrCatchingUp is returned by ServeReady while this replica's applied state is
// not yet trustworthy. Like ErrNotLeader it is a normal startup outcome rather
// than a failure: every replica reports it for a moment after it starts.
var ErrCatchingUp = errors.New("metastore: this replica has not caught up with the metadata group")

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
		logStore:  logStore,
		// Read BEFORE BootstrapCluster, so a fresh replica records 0 rather
		// than the configuration entry bootstrap is about to write. What is on
		// disk now is exactly what this process inherited from its last life.
		startupIndex: node.LastIndex(),
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

// ServeReady reports whether this replica's applied state may be published as
// an authoritative answer yet.
//
// This is proposeAllowed's reasoning applied to the read-serving side. A
// process that has just started holds a zero-valued FSM: no nodes, generation
// 0. That is byte-identical to a genuinely empty cluster, which Phase 3
// established as an authoritative state clients install and act on. Serving it
// before the replica has caught up is not the stale-but-consistent answer
// Phase 5 promises -- an applied log that is a prefix of the leader's committed
// one -- it is a claim about a cluster this replica has not looked at. A client
// that installs it stops routing and reports ErrNoLiveNodes for the duration.
//
// Four conditions, all of them real convergence signals rather than timers --
// the settle window in bridge.go already establishes that a fixed timer is the
// wrong tool for this job:
//
//  1. This replica has heard from a leader since it started. Without it the
//     rest is vacuous -- a fresh process has commit index 0 and applied index
//     0, which trivially satisfies "I have applied everything committed".
//  2. What the group says is committed covers at least the log this replica
//     already had on disk when it started. A replica must never answer from a
//     state older than its own persisted log, which is exactly the case a
//     rejoining follower with an intact raft.db hits.
//  3. Everything committed has been handed to the state machine.
//  4. The state machine has actually consumed it. That is a different claim
//     from (3), and skipping it leaked an empty topology in 2 real-cluster
//     restarts out of 5 -- see unappliedCommandIndex.
//
// The floor in (2) is capped at the current last index so an uncommitted tail
// that a new leader truncates cannot strand this replica short of a bar it can
// never reach.
//
// It LATCHES. Once caught up, a replica that later loses contact with the
// leader keeps serving: its state is then stale, which is the documented and
// safe Phase 5 behaviour (see docs/pulsekv-v2-phase5-summary.md §4). Reverting
// to "not ready" on every partition would turn a documented staleness bound
// into an outage, and would make this guard strictly worse than the thing it
// replaces.
//
// One residual, narrow and named rather than left to be discovered. A replica
// whose local state was wiped entirely, catching up from a leader whose log
// exceeds one MaxAppendEntries batch, can have its commit index land on a
// prefix that contains no command entry, which opens the gate on an empty
// state. Reaching it takes 60-odd elections before the group ever agreed a node
// set. Closing it would need the LEADER's commit index, which a follower has no
// way to observe.
func (s *Store) ServeReady() error {
	if s.caughtUp.Load() {
		return nil
	}

	_, leaderID := s.raft.LeaderWithID()
	if leaderID == "" {
		return fmt.Errorf("%w: no leader has been seen since this replica started", ErrCatchingUp)
	}

	floor := s.startupIndex
	if last := s.raft.LastIndex(); last < floor {
		floor = last
	}
	if floor < 1 {
		// Even a replica that started with nothing must wait for the group's
		// first committed entry: until then "committed through 0" carries no
		// information about anyone else's state.
		floor = 1
	}

	commit := s.raft.CommitIndex()
	if commit < floor {
		return fmt.Errorf("%w: committed through index %d, needs %d (leader %s)",
			ErrCatchingUp, commit, floor, leaderID)
	}
	if applied := s.raft.AppliedIndex(); applied < commit {
		return fmt.Errorf("%w: applied index %d of %d committed (leader %s)",
			ErrCatchingUp, applied, commit, leaderID)
	}

	// And the FSM must have CONSUMED what Raft handed it, which is not the same
	// claim. See fsm.applied and unappliedCommandIndex.
	consumed := s.fsm.AppliedIndex()
	if pending, ok := s.unappliedCommandIndex(consumed, commit); ok {
		return fmt.Errorf("%w: committed command at index %d is not in the state machine yet "+
			"(consumed through %d, leader %s)", ErrCatchingUp, pending, consumed, leaderID)
	}

	s.caughtUp.Store(true)
	return nil
}

// unappliedCommandIndex reports the newest committed command entry this
// replica's state machine has not consumed, if there is one.
//
// This is the condition raft.AppliedIndex cannot express. That index advances
// when a batch of entries is queued on the FSM's channel, not when the FSM has
// run them -- hashicorp/raft's own documentation says so -- and a readiness
// check that trusted it would open the gate one goroutine handoff before the
// state machine held anything. Measured on the dev fixture, that handoff was
// enough to leak an empty topology in 2 restarts out of 5.
//
// The FSM's own mark cannot simply be compared against the commit index
// instead, because a Raft FSM never sees every entry: no-op entries (one per
// election) and configuration entries are not dispatched to it at all, so its
// mark legitimately trails the commit index forever. What the FSM must have
// consumed is every COMMAND entry, and that is what this looks for.
//
// The scan walks back from the commit index and stops at the FSM's mark, so its
// length is the number of trailing non-command entries -- normally zero, and
// bounded by the number of elections since the last membership change. Once
// this replica is caught up the range is empty and the whole check is free;
// once it is ready the check is not reached at all.
func (s *Store) unappliedCommandIndex(consumed, commit uint64) (uint64, bool) {
	if s.logStore == nil || commit <= consumed {
		return 0, false
	}
	var entry raft.Log
	for index := commit; index > consumed; index-- {
		if err := s.logStore.GetLog(index, &entry); err != nil {
			// Below a snapshot this replica already restored, or otherwise not
			// readable. Either way there is nothing further this check can
			// establish, and the snapshot restore accounted for the state.
			return 0, false
		}
		if entry.Type == raft.LogCommand {
			return index, true
		}
	}
	return 0, false
}

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
