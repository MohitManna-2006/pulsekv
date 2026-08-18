package metastore

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pulsekv/control/internal/membership"
)

// Test timings are far tighter than the dev fixture's 500 ms so the suite stays
// fast, but keep the same ordering constraints a real group has.
const (
	testElectionTimeout = 150 * time.Millisecond
	testConverge        = 10 * time.Second
)

// freePort reserves an ephemeral port, closes it, and returns the number.
//
// There is a small window in which something else could take it. The
// alternative -- binding with ":0" and letting Raft advertise what it got --
// does not work, because a Raft server's advertised address is part of the
// bootstrap configuration every peer must agree on before any listener exists.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}

// group is a live Raft metadata group over real TCP transports and on-disk
// stores, which is the only way to test what actually ships.
type group struct {
	t       *testing.T
	dir     string
	peers   []Peer
	stores  map[string]*Store
	stopped map[string]bool
}

func newGroup(t *testing.T, size int) *group {
	t.Helper()
	dir := t.TempDir()
	peers := make([]Peer, 0, size)
	for i := 0; i < size; i++ {
		peers = append(peers, Peer{
			NodeID:  fmt.Sprintf("cp-%d", i),
			Address: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		})
	}

	g := &group{
		t:       t,
		dir:     dir,
		peers:   peers,
		stores:  make(map[string]*Store, size),
		stopped: make(map[string]bool, size),
	}
	for _, peer := range peers {
		g.start(peer.NodeID)
	}
	t.Cleanup(g.closeAll)
	return g
}

// start brings one replica up, using the same on-disk state it had before if it
// is being restarted. That persistence is the point: a rejoining replica must
// return as the voter that left, not as a new one.
func (g *group) start(nodeID string) {
	g.t.Helper()
	var address string
	for _, peer := range g.peers {
		if peer.NodeID == nodeID {
			address = peer.Address
		}
	}
	store, err := New(Config{
		NodeID:             nodeID,
		BindAddress:        address,
		Peers:              g.peers,
		DataDir:            filepath.Join(g.dir, nodeID),
		HeartbeatTimeout:   testElectionTimeout,
		ElectionTimeout:    testElectionTimeout,
		LeaderLeaseTimeout: testElectionTimeout / 2,
		SnapshotThreshold:  1024,
		ApplyTimeout:       5 * time.Second,
		Logger:             log.New(os.Stderr, "", 0),
	})
	if err != nil {
		g.t.Fatalf("start replica %s: %v", nodeID, err)
	}
	g.stores[nodeID] = store
	g.stopped[nodeID] = false
}

func (g *group) stop(nodeID string) {
	g.t.Helper()
	if g.stopped[nodeID] {
		return
	}
	if err := g.stores[nodeID].Close(); err != nil {
		g.t.Logf("closing %s: %v", nodeID, err)
	}
	g.stopped[nodeID] = true
}

func (g *group) closeAll() {
	for id := range g.stores {
		g.stop(id)
	}
}

func (g *group) live() []*Store {
	out := make([]*Store, 0, len(g.stores))
	for id, store := range g.stores {
		if !g.stopped[id] {
			out = append(out, store)
		}
	}
	return out
}

// waitLeader blocks until every live replica names the same leader and term.
// Agreement, not merely "somebody answered", is the property under test.
func (g *group) waitLeader(exclude string) (string, uint64) {
	g.t.Helper()
	deadline := time.Now().Add(testConverge)
	for {
		leader, term, agreed := g.observeLeader()
		if agreed && leader != "" && leader != exclude {
			return leader, term
		}
		if time.Now().After(deadline) {
			g.t.Fatalf("no agreed leader within %s (last: %q term %d, agreed=%v, excluding %q)",
				testConverge, leader, term, agreed, exclude)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (g *group) observeLeader() (string, uint64, bool) {
	var leader string
	var term uint64
	first := true
	for _, store := range g.live() {
		id, storeTerm := store.Leader()
		if id == "" {
			return "", 0, false
		}
		if first {
			leader, term, first = id, storeTerm, false
			continue
		}
		if id != leader || storeTerm != term {
			return "", 0, false
		}
	}
	return leader, term, !first
}

func (g *group) leaderStore() *Store {
	for _, store := range g.live() {
		if store.IsLeader() {
			return store
		}
	}
	return nil
}

// waitState blocks until every live replica has applied a state satisfying
// want. Followers are allowed to lag; they are not allowed to disagree.
func (g *group) waitState(want func(State) bool, describe string) {
	g.t.Helper()
	deadline := time.Now().Add(testConverge)
	for {
		converged := true
		for _, store := range g.live() {
			if !want(store.State()) {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		if time.Now().After(deadline) {
			for _, peer := range g.peers {
				if g.stopped[peer.NodeID] {
					continue
				}
				g.t.Logf("%s state = %+v", peer.NodeID, g.stores[peer.NodeID].State())
			}
			g.t.Fatalf("replicas did not converge on %s within %s", describe, testConverge)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Exit criterion 1, at the package level: a three-replica group elects a leader
// and every replica answers from the same committed state.
func TestGroupElectsLeaderAndConvergesOnCommittedState(t *testing.T) {
	g := newGroup(t, 3)
	leader, term := g.waitLeader("")
	if term == 0 {
		t.Fatal("leader elected at term 0")
	}
	t.Logf("elected %s at term %d", leader, term)

	if err := g.leaderStore().Propose(State{
		Nodes: nodes("node-0", "node-1"), ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}

	g.waitState(func(s State) bool {
		return len(s.Nodes) == 2 && s.ReplicationFactor == 1
	}, "two data nodes at replication factor 1")

	// Every replica must report the identical generation, which is what makes
	// a follower's answer safe to serve. A process-local counter could not
	// promise this; a committed log index does.
	var generation uint64
	for _, store := range g.live() {
		state := store.State()
		if generation == 0 {
			generation = state.Generation
			continue
		}
		if state.Generation != generation {
			t.Fatalf("replicas report generations %d and %d for the same state",
				generation, state.Generation)
		}
	}
	if generation == 0 {
		t.Fatal("committed state has generation 0")
	}
}

// Only the leader may propose. Every follower gets ErrNotLeader -- a normal
// outcome, and the thing that makes the fencing check below statable.
func TestOnlyTheLeaderMayPropose(t *testing.T) {
	g := newGroup(t, 3)
	g.waitLeader("")

	proposed := 0
	for _, store := range g.live() {
		err := store.Propose(State{Nodes: nodes("node-0"), ReplicationFactor: 1})
		if store.IsLeader() {
			if err != nil {
				t.Fatalf("the leader could not propose: %v", err)
			}
			proposed++
			continue
		}
		if err == nil {
			t.Fatal("a follower committed a proposal")
		}
	}
	if proposed != 1 {
		t.Fatalf("%d replicas accepted a proposal, want exactly 1", proposed)
	}
}

// Exit criteria 2 and 5: killing the leader re-elects, and the old leader comes
// back fenced.
//
// The fencing check is positive rather than assumed. While the old leader is
// down the committed membership is changed to something it never saw. When it
// returns it must (a) not be the leader, (b) adopt the newer state rather than
// reasserting its own, and (c) be unable to commit anything, verified by
// actually asking it to.
func TestLeaderFailoverFencesTheOldLeader(t *testing.T) {
	g := newGroup(t, 3)
	oldLeader, oldTerm := g.waitLeader("")

	if err := g.leaderStore().Propose(State{
		Nodes: nodes("node-0", "node-1"), ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("seed proposal: %v", err)
	}
	g.waitState(func(s State) bool { return len(s.Nodes) == 2 }, "the pre-failover membership")
	staleGeneration := g.stores[oldLeader].State().Generation

	g.stop(oldLeader)
	newLeader, newTerm := g.waitLeader(oldLeader)
	if newLeader == oldLeader {
		t.Fatalf("leadership did not move away from %s", oldLeader)
	}
	if newTerm <= oldTerm {
		t.Fatalf("new term %d did not advance past %d", newTerm, oldTerm)
	}
	t.Logf("failover: %s (term %d) -> %s (term %d)", oldLeader, oldTerm, newLeader, newTerm)

	// Advance the committed state to something the old leader never applied.
	// Without this the fencing check would be vacuous: agreeing with a state you
	// already had proves nothing.
	if err := g.leaderStore().Propose(State{
		Nodes: nodes("node-0", "node-1", "node-2", "node-3"), ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("post-failover proposal: %v", err)
	}
	g.waitState(func(s State) bool {
		return len(s.Nodes) == 4 && s.ReplicationFactor == 2
	}, "the post-failover membership")

	g.start(oldLeader)
	rejoined := g.stores[oldLeader]

	// (c) It cannot commit. Asked directly, repeatedly, across the window in
	// which it is catching up.
	for attempt := 0; attempt < 20; attempt++ {
		if err := rejoined.Propose(State{
			Nodes: nodes("ghost-0"), ReplicationFactor: 0,
		}); err == nil {
			t.Fatal("the rejoined former leader committed a proposal")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// (a) It is a follower, and everyone agrees who leads.
	agreedLeader, agreedTerm := g.waitLeader("")
	if agreedLeader == oldLeader {
		t.Fatalf("%s re-took leadership immediately; the live leader %s was healthy",
			oldLeader, newLeader)
	}
	if agreedTerm < newTerm {
		t.Fatalf("agreed term %d went backwards from %d", agreedTerm, newTerm)
	}

	// (b) It adopted the newer state rather than reasserting its own.
	g.waitState(func(s State) bool {
		return len(s.Nodes) == 4 && s.ReplicationFactor == 2
	}, "the rejoined replica adopting the newer committed state")
	if got := rejoined.State().Generation; got <= staleGeneration {
		t.Fatalf("rejoined replica is still at generation %d; it left at %d and the "+
			"cluster has moved on", got, staleGeneration)
	}
	if ghost := rejoined.State(); len(ghost.Nodes) == 1 && ghost.Nodes[0].NodeID == "ghost-0" {
		t.Fatal("the rejoined replica's own rejected proposal reached its state")
	}
}

// A replica that restarts must return as the voter that left, carrying its log.
// That is the whole reason DataDir is persistent.
func TestRestartedReplicaRetainsCommittedState(t *testing.T) {
	g := newGroup(t, 3)
	g.waitLeader("")
	if err := g.leaderStore().Propose(State{
		Nodes: nodes("node-0", "node-1", "node-2"), ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	g.waitState(func(s State) bool { return len(s.Nodes) == 3 }, "three data nodes")

	// Restart a follower, so the group keeps a leader throughout.
	var follower string
	for _, peer := range g.peers {
		if !g.stores[peer.NodeID].IsLeader() {
			follower = peer.NodeID
			break
		}
	}
	before := g.stores[follower].State()

	g.stop(follower)
	g.start(follower)

	g.waitState(func(s State) bool {
		return len(s.Nodes) == 3 && s.ReplicationFactor == 2
	}, "the restarted replica's recovered state")
	if got := g.stores[follower].State(); got.Generation != before.Generation {
		t.Fatalf("restarted replica reports generation %d, want the committed %d",
			got.Generation, before.Generation)
	}
}

// A one-replica group is a legal configuration -- it is what the legacy
// single-mapping config parses to -- and it must elect itself immediately.
func TestSingleReplicaGroupIsSelfSufficient(t *testing.T) {
	g := newGroup(t, 1)
	leader, _ := g.waitLeader("")
	if leader != "cp-0" {
		t.Fatalf("leader = %q, want the only replica", leader)
	}
	if err := g.leaderStore().Propose(State{Nodes: nodes("node-0"), ReplicationFactor: 0}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	g.waitState(func(s State) bool { return len(s.Nodes) == 1 }, "one data node")

	// The Source adaptation must be live end to end.
	snapshot := g.stores["cp-0"].Snapshot()
	if len(snapshot.Nodes) != 1 || snapshot.ReplicationFactor == nil || *snapshot.ReplicationFactor != 0 {
		t.Fatalf("membership snapshot = %+v", snapshot)
	}
}

func TestConfigValidationRejectsUnusableGroups(t *testing.T) {
	base := Config{
		NodeID:             "cp-0",
		BindAddress:        "127.0.0.1:1",
		Peers:              []Peer{{NodeID: "cp-0", Address: "127.0.0.1:1"}},
		HeartbeatTimeout:   time.Second,
		ElectionTimeout:    time.Second,
		LeaderLeaseTimeout: time.Second / 2,
		ApplyTimeout:       time.Second,
	}
	if err := base.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := []struct {
		name    string
		edit    func(*Config)
		wantSub string
	}{
		{"no id", func(c *Config) { c.NodeID = "" }, "node ID"},
		{"no bind address", func(c *Config) { c.BindAddress = "" }, "bind address"},
		{"no peers", func(c *Config) { c.Peers = nil }, "peer list"},
		{"not in own peer list", func(c *Config) {
			c.Peers = []Peer{{NodeID: "cp-9", Address: "127.0.0.1:2"}}
		}, "not in its own peer list"},
		{"duplicate peer", func(c *Config) {
			c.Peers = append(c.Peers, Peer{NodeID: "cp-0", Address: "127.0.0.1:2"})
		}, "appears twice"},
		{"lease longer than heartbeat", func(c *Config) {
			c.LeaderLeaseTimeout = 2 * c.HeartbeatTimeout
		}, "exceeds heartbeat timeout"},
		{"no apply timeout", func(c *Config) { c.ApplyTimeout = 0 }, "apply timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Peers = append([]Peer(nil), base.Peers...)
			tc.edit(&cfg)
			err := cfg.validate()
			if err == nil {
				t.Fatal("invalid config accepted")
			}
			if !contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) &&
		func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		}())
}

var _ membership.Source = (*Store)(nil)
