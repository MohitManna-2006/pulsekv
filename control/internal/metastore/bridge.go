package metastore

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"pulsekv/control/internal/membership"
)

// Bridge is the one-way valve between gossip and Raft.
//
// Every control-plane replica keeps observing gossip — that is not gated on
// leadership, and removing it would be a mistake. A follower's gossip view is
// not authoritative and metadata.Service never reads it, but it earns its keep
// twice over: the group gets redundant liveness detection, and whichever
// replica becomes leader next already has a warm view to propose from instead
// of a cold start at exactly the moment the cluster can least afford one.
//
// Only the leader proposes. That is the whole of the write path into the
// metadata plane: one loop, on one replica, comparing its own gossip view with
// committed state and proposing the difference.
type Bridge struct {
	store    *Store
	gossip   membership.Source
	factor   int
	interval time.Duration
	logger   *log.Logger

	// settleWindow guards the one genuinely dangerous proposal: emptying a
	// non-empty membership. See proposeAllowed.
	settleWindow time.Duration

	proposals atomic.Uint64
	deferrals atomic.Uint64
}

// BridgeConfig configures the proposer loop.
type BridgeConfig struct {
	Store  *Store
	Gossip membership.Source

	// ReplicationFactor is this replica's configured factor. The leader
	// proposes it, so the group agrees on one value rather than each replica
	// applying its own — which is the Phase 4 limitation this closes.
	ReplicationFactor int

	Interval time.Duration

	// SettleWindow is how long a newly-elected leader waits before it is
	// willing to propose an EMPTY node set over a non-empty one. Zero selects
	// a default derived from Interval.
	SettleWindow time.Duration

	Logger *log.Logger
}

func NewBridge(cfg BridgeConfig) (*Bridge, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("metastore bridge requires a store")
	case cfg.Gossip == nil:
		return nil, errors.New("metastore bridge requires a gossip source")
	case cfg.Interval <= 0:
		return nil, errors.New("metastore bridge interval must be positive")
	case cfg.ReplicationFactor < 0:
		return nil, errors.New("metastore bridge replication factor must not be negative")
	}
	settle := cfg.SettleWindow
	if settle <= 0 {
		// Long enough for memberlist's push/pull to populate a fresh
		// participant's view, short enough that a genuinely empty cluster is
		// reported promptly.
		settle = 10 * cfg.Interval
	}
	return &Bridge{
		store:        cfg.Store,
		gossip:       cfg.Gossip,
		factor:       cfg.ReplicationFactor,
		interval:     cfg.Interval,
		logger:       cfg.Logger,
		settleWindow: settle,
	}, nil
}

// Run drives the proposer until ctx is cancelled.
func (b *Bridge) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	// leaderSince is zero while this replica is a follower. Tracking it is what
	// lets proposeAllowed distinguish "the cluster is empty" from "this replica
	// has not looked yet".
	var leaderSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !b.store.IsLeader() {
			if !leaderSince.IsZero() {
				b.logf("metastore: lost leadership; no longer proposing membership")
				leaderSince = time.Time{}
			}
			continue
		}
		if leaderSince.IsZero() {
			leaderSince = time.Now()
			b.logf("metastore: gained leadership; reconciling gossip with committed membership")
		}

		b.reconcile(leaderSince)
	}
}

// reconcile proposes the difference between this leader's gossip view and the
// committed state, if there is one.
func (b *Bridge) reconcile(leaderSince time.Time) {
	observed := b.gossip.Snapshot()
	committed := b.store.State()

	desired := State{
		Nodes:             normalizeNodes(observed.Nodes),
		ReplicationFactor: b.factor,
	}
	if committed.SameContent(desired) {
		return
	}
	if !b.proposeAllowed(committed, desired, leaderSince) {
		b.deferrals.Add(1)
		return
	}

	if err := b.store.Propose(desired); err != nil {
		if errors.Is(err, ErrNotLeader) {
			// Raced a leadership change. The new leader proposes from its own
			// view; nothing to retry here.
			return
		}
		b.logf("metastore: proposal failed: %v", err)
		return
	}
	b.proposals.Add(1)
	b.logf("metastore: committed membership of %d data node(s), replication factor %d",
		len(desired.Nodes), desired.ReplicationFactor)
}

// proposeAllowed rejects exactly one proposal: wiping a non-empty membership
// during a new leader's first moments.
//
// The hazard is concrete. A replica can win an election before its own gossip
// participant has finished exchanging state — memberlist join and push/pull are
// not instant — and its view is legitimately empty at that instant. Proposing
// it would commit "no live data nodes", every client would install an
// authoritative empty topology and start failing with ErrNoLiveNodes, and the
// next tick would put it all back. A self-inflicted outage lasting one interval,
// caused by treating "I have not looked yet" as "there is nothing there".
//
// Waiting out the settle window before believing an emptying view costs a
// bounded delay in the one case where the cluster really did go empty, and
// prevents the flap in the case where it did not. An empty view that persists
// past the window is proposed normally: a genuinely empty cluster is an
// authoritative state, which Phase 3 already established.
func (b *Bridge) proposeAllowed(committed, desired State, leaderSince time.Time) bool {
	if len(desired.Nodes) > 0 || len(committed.Nodes) == 0 {
		return true
	}
	if time.Since(leaderSince) >= b.settleWindow {
		return true
	}
	b.logf("metastore: deferring a proposal that would empty %d committed data node(s); "+
		"this leader's gossip view is %s old and may still be converging",
		len(committed.Nodes), time.Since(leaderSince).Round(time.Millisecond))
	return false
}

// Stats reports how many proposals this replica has committed and how many it
// deferred under the settle rule. Diagnostics for the summary and the chaos
// report, not a control input.
func (b *Bridge) Stats() (proposals, deferrals uint64) {
	return b.proposals.Load(), b.deferrals.Load()
}

func (b *Bridge) logf(format string, args ...any) {
	if b.logger == nil {
		return
	}
	b.logger.Printf(format, args...)
}
