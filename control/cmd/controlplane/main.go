// Command controlplane is the PulseKV v2 control-plane process.
//
// Phase 3 scope: it participates in the SWIM gossip ring as an observer,
// publishes the live data-node membership, and computes shard ownership with
// rendezvous hashing. The Raft metadata plane in Phase 5 will make ownership
// authoritative across multiple control-plane replicas.
//
// It doubles as the config reader for deploy/*.sh -- `--print-nodes` and
// `--print-control-plane` let the shell scripts get the cluster's shape from
// the same parser the server uses, instead of hand-rolling YAML parsing in
// awk and quietly disagreeing with the server about what the file says.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
	"pulsekv/control/internal/metadata"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[controlplane] ")

	var (
		configPath = flag.String("config", "deploy/cluster.config.yaml",
			"path to the static cluster config")
		portOverride = flag.Int("port", 0,
			"listen port; overrides control_plane.port from the config when non-zero")
		gossipJoin = flag.String("gossip-join", "",
			"optional comma-separated memberlist seed addresses for an additional control-plane observer")
		printNodes = flag.Bool("print-nodes", false,
			"print `node_id<TAB>host<TAB>port` for each configured node and exit")
		printControlPlane = flag.Bool("print-control-plane", false,
			"print `host<TAB>port` for the control plane and exit")
		printEngine = flag.Bool("print-engine", false,
			"print `ram_budget_bytes<TAB>max_value_bytes<TAB>data_root` and exit")
		printGossip = flag.Bool("print-gossip", false,
			"print `participant<TAB>host<TAB>gossip-port` for bootstrap inventory and exit")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	if *portOverride != 0 {
		cfg.ControlPlane.Port = *portOverride
		if err := cfg.Validate(); err != nil {
			log.Fatalf("fatal: --port %d is not usable: %v", *portOverride, err)
		}
	}

	// Config-dump modes: no server, no logging noise, machine-readable stdout.
	if *printNodes {
		for _, n := range cfg.Nodes {
			fmt.Printf("%s\t%s\t%d\n", n.NodeID, n.Host, n.Port)
		}
		return
	}
	if *printControlPlane {
		fmt.Printf("%s\t%d\n", cfg.ControlPlane.Host, cfg.ControlPlane.Port)
		return
	}
	if *printEngine {
		fmt.Printf("%d\t%d\t%s\n", cfg.Engine.RAMBudgetBytes,
			cfg.Engine.MaxValueBytes, cfg.Engine.DataRoot)
		return
	}
	if *printGossip {
		fmt.Printf("controlplane\t%s\t%d\n", cfg.ControlPlane.Host, cfg.ControlPlane.GossipPort)
		for _, n := range cfg.Nodes {
			fmt.Printf("%s\t%s\t%d\n", n.NodeID, n.Host, n.GossipPort)
		}
		return
	}

	// Legal-but-probably-not-what-you-meant configurations. Printed once, at
	// startup, where a dev cluster's boot log will actually surface them.
	for _, w := range cfg.Warnings() {
		log.Printf("config warning: %s", w)
	}

	if err := run(cfg, splitCommaList(*gossipJoin)); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run(cfg *config.Config, gossipSeeds []string) error {
	members, err := membership.New(membership.Config{
		Name:          "controlplane@" + cfg.ControlPlane.GossipAddress(),
		Role:          membership.RoleControl,
		BindAddr:      cfg.ControlPlane.Host,
		BindPort:      cfg.ControlPlane.GossipPort,
		AdvertiseAddr: cfg.ControlPlane.Host,
		AdvertisePort: cfg.ControlPlane.GossipPort,
		ClusterLabel:  cfg.Membership.ClusterName,
		Local:         isLoopback(cfg.ControlPlane.Host),
		TCPTimeout:    membership.DefaultTCPTimeout,
		Logger:        log.Default(),
	})
	if err != nil {
		return fmt.Errorf("start gossip observer: %w", err)
	}
	defer members.Close()
	svc, err := metadata.New(cfg, members)
	if err != nil {
		return fmt.Errorf("build metadata service: %w", err)
	}
	defer svc.Close()

	lis, err := net.Listen("tcp", cfg.ControlPlane.Address())
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ControlPlane.Address(), err)
	}

	srv := grpc.NewServer()
	svc.Register(srv)
	// Reflection is on so `grpcurl` works against a running cluster without
	// pointing it at the .proto files. It costs nothing and makes the dev
	// cluster inspectable by hand.
	reflection.Register(srv)

	// NodeService and AdapterService are deliberately NOT registered here.
	// gRPC answers UNIMPLEMENTED for them, which is the honest Phase 0
	// answer: AdapterService's server side arrives in Phase 7, and
	// NodeService is served by the data-plane nodes, not by this process.

	svc.LogSummary()
	log.Printf("membership: observer listening on %s, cluster=%q",
		members.LocalGossipAddress(), cfg.Membership.ClusterName)
	log.Printf("listening on %s (pid %d)", cfg.ControlPlane.Address(), os.Getpid())

	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go logMembershipChanges(monitorCtx, members)
	bootstrapSeeds := append([]string(nil), gossipSeeds...)
	for _, node := range cfg.Nodes {
		bootstrapSeeds = append(bootstrapSeeds, node.GossipAddress())
	}
	go joinGossipPeers(monitorCtx, members, uniqueStrings(bootstrapSeeds))

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

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

// joinGossipPeers lets an observer reattach after a control-plane-only restart.
// Failure is not fatal: on first cluster boot the data sidecars do not exist
// yet and will join this observer as their seed. Retrying makes both startup
// orders converge without making static launch inventory authoritative.
func joinGossipPeers(ctx context.Context, manager *membership.Manager, seeds []string) {
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
			joined, err := manager.Join([]string{seed})
			if err == nil {
				log.Printf("membership: contacted %d bootstrap gossip peer(s) via %s", joined, seed)
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
			log.Printf("membership: published generation %d with %d data node(s): [%s]",
				snapshot.Generation, len(ids), strings.Join(ids, ", "))
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
