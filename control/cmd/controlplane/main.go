// Command controlplane is the PulseKV v2 control-plane process.
//
// Phase 0 scope: it serves ClusterMetadataService only, from the static
// deploy/cluster.config.yaml. Gossip membership (Phase 3), the rendezvous-hash
// router and generic client SDK (Phase 2), and the Raft metadata plane
// (Phase 5) all attach here later.
//
// It doubles as the config reader for deploy/*.sh -- `--print-nodes` and
// `--print-control-plane` let the shell scripts get the cluster's shape from
// the same parser the server uses, instead of hand-rolling YAML parsing in
// awk and quietly disagreeing with the server about what the file says.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"pulsekv/control/internal/config"
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
		probeTimeout = flag.Duration("probe-timeout", metadata.DefaultProbeTimeout,
			"per-request budget for probing node liveness during GetNodeList")
		printNodes = flag.Bool("print-nodes", false,
			"print `node_id<TAB>host<TAB>port` for each configured node and exit")
		printControlPlane = flag.Bool("print-control-plane", false,
			"print `host<TAB>port` for the control plane and exit")
		printEngine = flag.Bool("print-engine", false,
			"print `ram_budget_bytes<TAB>max_value_bytes<TAB>data_root` and exit")
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

	// Legal-but-probably-not-what-you-meant configurations. Printed once, at
	// startup, where a dev cluster's boot log will actually surface them.
	for _, w := range cfg.Warnings() {
		log.Printf("config warning: %s", w)
	}

	if err := run(cfg, *probeTimeout); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run(cfg *config.Config, probeTimeout time.Duration) error {
	svc, err := metadata.New(cfg, metadata.WithProbeTimeout(probeTimeout))
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
	log.Printf("listening on %s (pid %d)", cfg.ControlPlane.Address(), os.Getpid())

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
