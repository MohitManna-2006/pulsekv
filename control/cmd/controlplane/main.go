// Command controlplane is one PulseKV v2 control-plane replica.
//
// Phase 5 scope: several of these processes form a Raft group that agrees on
// the metadata plane's two inputs — the live data-node set and the replication
// factor. Each replica still participates in the SWIM gossip ring, but only the
// Raft leader turns what it sees there into committed state. Every replica,
// leader or follower, serves ClusterMetadataService from its own applied log,
// which is safe because an applied log is always a prefix of the leader's
// committed one: a follower can be behind, never divergent.
//
// Shard ownership is NOT replicated. router.AssignShards and AssignShardOwners
// are pure functions of the agreed input, so every replica derives a
// byte-identical map locally with nothing to coordinate. See
// control/internal/metastore for why that is the right layer.
//
// It doubles as the config reader for deploy/*.sh -- `--print-nodes`,
// `--print-control-plane`, and friends let the shell scripts get the cluster's
// shape from the same parser the server uses, instead of hand-rolling YAML
// parsing in awk and quietly disagreeing with the server about what the file
// says.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/metadata"
	"pulsekv/control/internal/metastore"
)

// How long startup waits for the group to elect someone before giving up and
// logging that it is serving without a leader. Not fatal: a replica that came
// up before its peers must keep running and keep trying, or a staggered boot
// could never converge.
const leaderWaitTimeout = 15 * time.Second

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		configPath = flag.String("config", "deploy/cluster.config.yaml",
			"path to the static cluster config")
		nodeID = flag.String("node-id", "",
			"which control_plane replica in the config this process is; may be omitted only when the config has exactly one")
		portOverride = flag.Int("port", 0,
			"listen port; overrides this replica's control_plane port when non-zero")
		replicationOverride = flag.Int("replication-factor", -1,
			"replicas per shard beyond the primary; overrides replication_factor from the "+
				"config when non-negative. -1 means 'use the config', which is why the "+
				"sentinel is not 0: 0 is a real setting")
		gossipJoin = flag.String("gossip-join", "",
			"optional comma-separated extra memberlist seed addresses")
		printNodes = flag.Bool("print-nodes", false,
			"print `node_id<TAB>host<TAB>port` for each configured data node and exit")
		printControlPlane = flag.Bool("print-control-plane", false,
			"print `node_id<TAB>host<TAB>port` for each configured control-plane replica and exit")
		printEngine = flag.Bool("print-engine", false,
			"print `ram_budget_bytes<TAB>max_value_bytes<TAB>data_root` and exit")
		printGossip = flag.Bool("print-gossip", false,
			"print `participant<TAB>host<TAB>gossip-port` for bootstrap inventory and exit")
		printReplication = flag.Bool("print-replication", false,
			"print the effective replication factor and exit")
		printRaft = flag.Bool("print-raft", false,
			"print `node_id<TAB>host<TAB>raft_port` per replica, then a `#<TAB>data_dir` line, and exit")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	if *replicationOverride >= 0 {
		cfg.ReplicationFactor = *replicationOverride
		if err := cfg.Validate(); err != nil {
			log.Fatalf("fatal: --replication-factor %d is not usable: %v", *replicationOverride, err)
		}
	}

	// Config-dump modes: no server, no logging noise, machine-readable stdout.
	// These run before replica selection, because the scripts use them to
	// discover which replicas exist in the first place.
	switch {
	case *printNodes:
		for _, n := range cfg.Nodes {
			fmt.Printf("%s\t%s\t%d\n", n.NodeID, n.Host, n.Port)
		}
		return
	case *printControlPlane:
		for _, replica := range cfg.ControlPlanes {
			fmt.Printf("%s\t%s\t%d\n", replica.NodeID, replica.Host, replica.Port)
		}
		return
	case *printEngine:
		fmt.Printf("%d\t%d\t%s\n", cfg.Engine.RAMBudgetBytes,
			cfg.Engine.MaxValueBytes, cfg.Engine.DataRoot)
		return
	case *printGossip:
		for _, replica := range cfg.ControlPlanes {
			fmt.Printf("%s\t%s\t%d\n", replica.NodeID, replica.Host, replica.GossipPort)
		}
		for _, n := range cfg.Nodes {
			fmt.Printf("%s\t%s\t%d\n", n.NodeID, n.Host, n.GossipPort)
		}
		return
	case *printReplication:
		fmt.Printf("%d\n", cfg.ReplicationFactor)
		return
	case *printRaft:
		for _, replica := range cfg.ControlPlanes {
			fmt.Printf("%s\t%s\t%d\n", replica.NodeID, replica.Host, replica.RaftPort)
		}
		fmt.Printf("#\t%s\n", raftDataRoot(cfg))
		return
	}

	replica, err := selectReplica(cfg, *nodeID)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	if *portOverride != 0 {
		replica.Port = *portOverride
	}
	log.SetPrefix("[" + replica.NodeID + "] ")

	// Legal-but-probably-not-what-you-meant configurations. Printed once, at
	// startup, where a dev cluster's boot log will actually surface them.
	for _, w := range cfg.Warnings() {
		log.Printf("config warning: %s", w)
	}

	if err := run(cfg, replica, splitCommaList(*gossipJoin)); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// selectReplica resolves --node-id against the config.
//
// A one-replica config may omit it, which keeps every single-replica invocation
// (including tests and the config-dump paths above) working unchanged. More
// than one replica and no --node-id is a hard error rather than a guess: two
// processes silently choosing the same identity would both try to bind the same
// ports and one would win for reasons nobody could see.
func selectReplica(cfg *config.Config, nodeID string) (config.ControlPlane, error) {
	if nodeID == "" {
		if len(cfg.ControlPlanes) == 1 {
			return cfg.ControlPlanes[0], nil
		}
		return config.ControlPlane{}, fmt.Errorf(
			"--node-id is required: %s defines %d control-plane replicas (%s)",
			cfg.Path, len(cfg.ControlPlanes), strings.Join(cfg.ControlPlaneIDs(), ", "))
	}
	replica, ok := cfg.ControlPlaneByID(nodeID)
	if !ok {
		return config.ControlPlane{}, fmt.Errorf(
			"--node-id %q is not a configured control-plane replica (have %s)",
			nodeID, strings.Join(cfg.ControlPlaneIDs(), ", "))
	}
	return replica, nil
}

// raftDataRoot resolves raft.data_dir relative to the config file, the same way
// engine.data_root is resolved.
func raftDataRoot(cfg *config.Config) string {
	if filepath.IsAbs(cfg.Raft.DataDir) {
		return cfg.Raft.DataDir
	}
	base := filepath.Dir(cfg.Path)
	if base == "" {
		base = "."
	}
	return filepath.Join(base, cfg.Raft.DataDir)
}

func run(cfg *config.Config, replica config.ControlPlane, extraSeeds []string) error {
	// ---------------------------------------------------------------------
	// Gossip. Every replica observes, leader or not -- see metastore.Bridge
	// for why a follower's view still earns its keep.
	// ---------------------------------------------------------------------
	members, err := membership.New(membership.Config{
		Name:          "controlplane:" + replica.NodeID + "@" + replica.GossipAddress(),
		Role:          membership.RoleControl,
		BindAddr:      replica.Host,
		BindPort:      replica.GossipPort,
		AdvertiseAddr: replica.Host,
		AdvertisePort: replica.GossipPort,
		ClusterLabel:  cfg.Membership.ClusterName,
		Local:         isLoopback(replica.Host),
		TCPTimeout:    membership.DefaultTCPTimeout,
		Logger:        log.Default(),
	})
	if err != nil {
		return fmt.Errorf("start gossip observer: %w", err)
	}
	defer members.Close()

	// ---------------------------------------------------------------------
	// Raft metadata group.
	// ---------------------------------------------------------------------
	peers := make([]metastore.Peer, 0, len(cfg.ControlPlanes))
	for _, entry := range cfg.ControlPlanes {
		peers = append(peers, metastore.Peer{NodeID: entry.NodeID, Address: entry.RaftAddress()})
	}
	store, err := metastore.New(metastore.Config{
		NodeID:             replica.NodeID,
		BindAddress:        replica.RaftAddress(),
		Peers:              peers,
		DataDir:            filepath.Join(raftDataRoot(cfg), replica.NodeID),
		HeartbeatTimeout:   cfg.Raft.HeartbeatTimeout(),
		ElectionTimeout:    cfg.Raft.ElectionTimeout(),
		LeaderLeaseTimeout: cfg.Raft.LeaderLeaseTimeout(),
		SnapshotThreshold:  cfg.Raft.SnapshotThreshold,
		ApplyTimeout:       cfg.Raft.ElectionTimeout(),
		Logger:             log.Default(),
	})
	if err != nil {
		return fmt.Errorf("start raft metadata store: %w", err)
	}
	defer store.Close()

	bridge, err := metastore.NewBridge(metastore.BridgeConfig{
		Store:             store,
		Gossip:            members,
		ReplicationFactor: cfg.ReplicationFactor,
		Interval:          cfg.Raft.ProposeInterval(),
		Logger:            log.Default(),
	})
	if err != nil {
		return fmt.Errorf("start metadata bridge: %w", err)
	}

	// The metadata service reads COMMITTED state, not this replica's gossip.
	// That substitution is the whole of the Phase 5 integration: metadata's
	// handlers are unchanged and never learn that consensus is involved.
	svc, err := metadata.New(cfg, store, metadata.WithLeaderInfo(store.Leader))
	if err != nil {
		return fmt.Errorf("build metadata service: %w", err)
	}
	defer svc.Close()

	lis, err := net.Listen("tcp", replica.Address())
	if err != nil {
		return fmt.Errorf("listen on %s: %w", replica.Address(), err)
	}

	srv := grpc.NewServer()
	svc.Register(srv)
	// Reflection is on so `grpcurl` works against a running cluster without
	// pointing it at the .proto files. It costs nothing and makes the dev
	// cluster inspectable by hand.
	reflection.Register(srv)

	// NodeService and AdapterService are deliberately NOT registered here.
	// gRPC answers UNIMPLEMENTED for them, which is the honest answer:
	// AdapterService's server side arrives in Phase 7, and NodeService is
	// served by the data-plane nodes, not by this process.

	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go logMembershipChanges(monitorCtx, store)
	go logLeadershipChanges(monitorCtx, store)
	go bridge.Run(monitorCtx)

	seeds := append([]string(nil), extraSeeds...)
	seeds = append(seeds, cfg.ControlPlaneGossipSeeds()...)
	for _, node := range cfg.Nodes {
		seeds = append(seeds, node.GossipAddress())
	}
	go joinGossipPeers(monitorCtx, members, uniqueStrings(seeds),
		uniqueStrings(cfg.ControlPlaneGossipSeeds()), members.LocalGossipAddress())

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	svc.LogSummary()
	log.Printf("membership: observer listening on %s, cluster=%q",
		members.LocalGossipAddress(), cfg.Membership.ClusterName)
	log.Printf("raft: %s of %d replica(s) on %s, election timeout %s, state at %s",
		replica.NodeID, len(cfg.ControlPlanes), replica.RaftAddress(),
		cfg.Raft.ElectionTimeout(), filepath.Join(raftDataRoot(cfg), replica.NodeID))
	log.Printf("listening on %s (pid %d)", replica.Address(), os.Getpid())

	// Announce the leader once it exists. Not fatal if it does not: a replica
	// that started before its peers has to keep running for them to arrive.
	go func() {
		leader, term, err := store.WaitForLeader(leaderWaitTimeout)
		if err != nil {
			log.Printf("raft: %v; serving from the last applied state until the group converges", err)
			return
		}
		log.Printf("raft: leader is %s at term %d", leader, term)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		return err
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		srv.GracefulStop()
		return nil
	}
}

// joinGossipPeers lets a replica reattach after a restart. Failure is not
// fatal: on first cluster boot the data sidecars do not exist yet and will join
// this observer as their seed. Retrying makes both startup orders converge
// without making static launch inventory authoritative.
//
// Once ANY seed answers, this keeps going through the remaining CONTROL-PLANE
// seeds. That is deliberate and bounded to a handful of peers: any replica may
// become the Raft leader, and the leader proposes membership from its own
// gossip view, so a replica that only ever push/pulled with one peer is one
// election away from publishing an incomplete cluster. Data-node seeds are not
// re-contacted -- there can be dozens of them, and they seed from us.
//
// self is skipped so a replica does not spend attempts dialing its own listener.
func joinGossipPeers(ctx context.Context, manager *membership.Manager,
	seeds, controlPlaneSeeds []string, self string) {
	if len(seeds) == 0 {
		return
	}
	for {
		for _, seed := range seeds {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if seed == self {
				continue
			}
			joined, err := manager.Join([]string{seed})
			if err == nil {
				log.Printf("membership: contacted %d bootstrap gossip peer(s) via %s", joined, seed)
				syncControlPlanePeers(ctx, manager, controlPlaneSeeds, self, seed)
				return
			}
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// syncControlPlanePeers push/pulls with the other replicas, best effort. A
// replica that is down is not a problem: it will contact us when it starts.
func syncControlPlanePeers(ctx context.Context, manager *membership.Manager,
	seeds []string, self, alreadyJoined string) {
	synced := 0
	for _, seed := range seeds {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if seed == self || seed == alreadyJoined {
			continue
		}
		if _, err := manager.Join([]string{seed}); err == nil {
			synced++
		}
	}
	if synced > 0 {
		log.Printf("membership: directly synced with %d further control-plane replica(s)", synced)
	}
}

// logMembershipChanges narrates the COMMITTED membership, not this replica's
// gossip view. On a follower those differ, and the committed one is what the
// process actually serves.
func logMembershipChanges(ctx context.Context, source membership.Source) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := source.Snapshot()
			if snapshot.Generation == last {
				continue
			}
			last = snapshot.Generation
			ids := make([]string, 0, len(snapshot.Nodes))
			for _, node := range snapshot.Nodes {
				ids = append(ids, node.NodeID)
			}
			log.Printf("metadata: committed generation %d with %d data node(s): [%s]",
				snapshot.Generation, len(ids), strings.Join(ids, ", "))
		}
	}
}

// logLeadershipChanges makes elections visible in the process log. The chaos
// harness asserts against the RPC fields rather than these lines, but a human
// reading a failed run wants to see when leadership moved.
func logLeadershipChanges(ctx context.Context, store *metastore.Store) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastID string
	var lastTerm uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id, term := store.Leader()
			if id == lastID && term == lastTerm {
				continue
			}
			lastID, lastTerm = id, term
			switch {
			case id == "":
				log.Printf("raft: no leader at term %d (election in progress)", term)
			case store.IsLeader():
				log.Printf("raft: THIS replica is the leader at term %d", term)
			default:
				log.Printf("raft: following %s at term %d", id, term)
			}
		}
	}
}

func splitCommaList(text string) []string {
	var out []string
	for _, raw := range strings.Split(text, ",") {
		if value := strings.TrimSpace(raw); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func isLoopback(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
