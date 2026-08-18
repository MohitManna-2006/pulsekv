// Command pulsekv-member is the Go SWIM sidecar for one C++ data-plane node.
//
// It advertises the node only after a real NodeService health/identity check.
// Once joined, a bounded watchdog withdraws an unhealthy local service and
// keeps monitoring it. Recovery creates a fresh participant and rejoins through
// the control plane or any configured data peer, so a transient local outage
// cannot leave a healthy service permanently absent. Killing the sidecar itself
// remains the ungraceful path that exercises SWIM suspicion/death.
package main

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
)

type options struct {
	configPath       string
	nodeID           string
	startupTimeout   time.Duration
	joinRetry        time.Duration
	joinTimeout      time.Duration
	healthInterval   time.Duration
	healthTimeout    time.Duration
	failureThreshold int
	leaveTimeout     time.Duration
}

func main() {
	var opts options
	flag.StringVar(&opts.configPath, "config", "deploy/cluster.config.yaml", "cluster launch/bootstrap config")
	flag.StringVar(&opts.nodeID, "node-id", "", "configured data-node ID represented by this sidecar")
	flag.DurationVar(&opts.startupTimeout, "startup-timeout", 15*time.Second,
		"budget for NodeService readiness and initial gossip join")
	flag.DurationVar(&opts.joinRetry, "join-retry", 200*time.Millisecond,
		"delay between gossip seed join attempts")
	flag.DurationVar(&opts.joinTimeout, "join-timeout", membership.DefaultTCPTimeout,
		"maximum TCP time for one gossip seed join attempt")
	flag.DurationVar(&opts.healthInterval, "health-interval", 250*time.Millisecond,
		"local NodeService watchdog interval")
	flag.DurationVar(&opts.healthTimeout, "health-timeout", 200*time.Millisecond,
		"deadline for each local NodeService health check")
	flag.IntVar(&opts.failureThreshold, "failure-threshold", 3,
		"consecutive local health failures before graceful membership withdrawal")
	flag.DurationVar(&opts.leaveTimeout, "leave-timeout", membership.DefaultLeaveTimeout,
		"budget for broadcasting a graceful leave on SIGINT/SIGTERM")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[member " + opts.nodeID + "] ")
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	if err := run(signalCtx, opts); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	if err := validateOptions(opts); err != nil {
		return err
	}
	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}
	node, ok := configuredNode(cfg, opts.nodeID)
	if !ok {
		return fmt.Errorf("node ID %q is not present in %s", opts.nodeID, opts.configPath)
	}

	deadline := time.Now().Add(opts.startupTimeout)
	if err := waitForNode(ctx, node, deadline, opts.healthTimeout, opts.joinRetry); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	seeds := bootstrapSeeds(cfg, node.NodeID)

	for {
		members, err := newMembership(cfg, node, opts)
		if err != nil {
			return fmt.Errorf("start gossip sidecar: %w", err)
		}
		if err := joinUntil(ctx, members, seeds, deadline, opts.joinTimeout, opts.joinRetry); err != nil {
			closeErr := members.Close()
			if errors.Is(err, context.Canceled) {
				return closeErr
			}
			return errors.Join(err, closeErr)
		}
		// Having joined SOMEONE, now push/pull with every remaining
		// control-plane replica. See syncControlPlanes for why one is not
		// enough any more.
		synced := syncControlPlanes(ctx, members, cfg, deadline, opts.joinTimeout)
		log.Printf("joined cluster %q; advertising NodeService %s from gossip %s "+
			"(directly synced with %d of %d control-plane replica(s))",
			cfg.Membership.ClusterName, node.Address(), members.LocalGossipAddress(),
			synced, len(cfg.ControlPlanes))
		failed, err := monitorNode(ctx, members, node, opts)
		if err != nil {
			return err
		}
		if !failed {
			return nil
		}

		log.Printf("local NodeService withdrawn; waiting for health before rejoining")
		deadline = time.Time{} // recovery is self-healing, not a one-shot startup budget
		if err := waitForNode(ctx, node, deadline, opts.healthTimeout, opts.joinRetry); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		log.Printf("local NodeService recovered; rebuilding gossip participant")
	}
}

func newMembership(cfg *config.Config, node config.Node, opts options) (*membership.Manager, error) {
	return membership.New(membership.Config{
		Name:          "data:" + node.NodeID,
		Role:          membership.RoleData,
		NodeID:        node.NodeID,
		NodeAddress:   node.Address(),
		BindAddr:      node.Host,
		BindPort:      node.GossipPort,
		AdvertiseAddr: node.Host,
		AdvertisePort: node.GossipPort,
		ClusterLabel:  cfg.Membership.ClusterName,
		Local:         isLoopback(node.Host),
		TCPTimeout:    opts.joinTimeout,
		LeaveTimeout:  opts.leaveTimeout,
		Logger:        log.Default(),
	})
}

func monitorNode(ctx context.Context, members *membership.Manager, node config.Node,
	opts options) (failed bool, returnErr error) {
	ticker := time.NewTicker(opts.healthInterval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("received shutdown signal, broadcasting graceful leave")
			return false, members.Close()
		case <-ticker.C:
			err := probeNodeContext(ctx, node, opts.healthTimeout)
			if err == nil {
				if failures > 0 {
					log.Printf("local NodeService recovered after %d failed probe(s)", failures)
				}
				failures = 0
				continue
			}
			if errors.Is(err, context.Canceled) {
				return false, members.Close()
			}
			failures++
			log.Printf("local NodeService probe failed (%d/%d): %v",
				failures, opts.failureThreshold, err)
			if failures < opts.failureThreshold {
				continue
			}
			log.Printf("local NodeService failed %d consecutive probes; broadcasting leave", failures)
			return true, members.Close()
		}
	}
}

func validateOptions(opts options) error {
	var problems []error
	if strings.TrimSpace(opts.nodeID) == "" {
		problems = append(problems, errors.New("--node-id must not be empty"))
	}
	if opts.startupTimeout <= 0 {
		problems = append(problems, errors.New("--startup-timeout must be positive"))
	}
	if opts.joinRetry <= 0 {
		problems = append(problems, errors.New("--join-retry must be positive"))
	}
	if opts.joinTimeout <= 0 {
		problems = append(problems, errors.New("--join-timeout must be positive"))
	}
	if opts.healthInterval <= 0 {
		problems = append(problems, errors.New("--health-interval must be positive"))
	}
	if opts.healthTimeout <= 0 {
		problems = append(problems, errors.New("--health-timeout must be positive"))
	}
	if opts.failureThreshold <= 0 {
		problems = append(problems, errors.New("--failure-threshold must be positive"))
	}
	if opts.leaveTimeout <= 0 {
		problems = append(problems, errors.New("--leave-timeout must be positive"))
	}
	return errors.Join(problems...)
}

func configuredNode(cfg *config.Config, nodeID string) (config.Node, bool) {
	for _, node := range cfg.Nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return config.Node{}, false
}

func waitForNode(ctx context.Context, node config.Node, deadline time.Time,
	rpcTimeout, retry time.Duration) error {
	lastErr := errors.New("no health probe completed")
	for {
		attemptTimeout, err := boundedAttemptTimeout(deadline, rpcTimeout)
		if err != nil {
			return fmt.Errorf("NodeService %s did not become ready before startup timeout: %w",
				node.Address(), lastErr)
		}
		lastErr = probeNodeContext(ctx, node, attemptTimeout)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, context.Canceled) {
			return lastErr
		}
		if err := waitRetry(ctx, deadline, retry); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("NodeService %s did not become ready before startup timeout: %w",
					node.Address(), lastErr)
			}
			return err
		}
	}
}

func probeNode(node config.Node, timeout time.Duration) error {
	return probeNodeContext(context.Background(), node, timeout)
}

func probeNodeContext(ctx context.Context, node config.Node, timeout time.Duration) error {
	conn, err := grpc.NewClient(node.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := nodev1.NewNodeServiceClient(conn).HealthCheck(rpcCtx, &nodev1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if !resp.GetOk() {
		return errors.New("HealthCheck returned ok=false")
	}
	if resp.GetNodeId() != node.NodeID {
		return fmt.Errorf("HealthCheck node_id=%q, want %q", resp.GetNodeId(), node.NodeID)
	}
	return nil
}

// bootstrapSeeds lists every gossip endpoint this sidecar may join through.
//
// Every control-plane replica comes first, not just one: Phase 5 turned the
// control plane into a group, and seeding from a single replica would make a
// data node's ability to join the ring depend on which replica happened to be
// up. Data peers follow, which is what already let a node recover when no
// control-plane process was reachable at all.
func bootstrapSeeds(cfg *config.Config, localNodeID string) []string {
	seeds := cfg.ControlPlaneGossipSeeds()
	for _, node := range cfg.Nodes {
		if node.NodeID != localNodeID {
			seeds = append(seeds, node.GossipAddress())
		}
	}
	return uniqueStrings(seeds)
}

// syncControlPlanes push/pulls with every control-plane replica, best effort.
//
// This is not redundancy for its own sake. Phase 5 made any replica a possible
// Raft leader, and the leader proposes committed membership from ITS OWN gossip
// view -- so a replica that has not heard of this node cannot publish it, no
// matter how healthy everything else is.
//
// Until Phase 5 there was exactly one control plane, every sidecar seeded from
// it, and its Join push/pull gave it a complete view immediately. With three
// replicas, seeding from the first one that answers leaves the other two to
// learn this node through gossip propagation -- which is a best-effort UDP
// broadcast with a bounded retransmit count, backstopped only by the 15 s
// push/pull interval. That is how a freshly booted cluster ends up with a
// leader publishing three of four nodes for fifteen seconds.
//
// One extra TCP sync per replica at startup removes the guesswork. Failures are
// ignored: the node has already joined the ring, and a replica that is down
// will pick this node up from gossip or its own push/pull when it returns.
func syncControlPlanes(ctx context.Context, manager *membership.Manager, cfg *config.Config,
	deadline time.Time, joinTimeout time.Duration) int {
	synced := 0
	for _, seed := range cfg.ControlPlaneGossipSeeds() {
		select {
		case <-ctx.Done():
			return synced
		default:
		}
		if _, err := boundedAttemptTimeout(deadline, joinTimeout); err != nil {
			return synced // out of startup budget; the ring already has us
		}
		if joined, err := manager.Join([]string{seed}); err == nil && joined > 0 {
			synced++
		}
	}
	return synced
}

func joinUntil(ctx context.Context, manager *membership.Manager, seeds []string,
	deadline time.Time, joinTimeout, retry time.Duration) error {
	lastErr := errors.New("no gossip seed attempt completed")
	for {
		for _, seed := range seeds {
			attemptTimeout, err := boundedAttemptTimeout(deadline, joinTimeout)
			if err != nil || attemptTimeout < joinTimeout {
				return fmt.Errorf("could not join gossip seeds before startup timeout: %w", lastErr)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			joined, err := manager.Join([]string{seed})
			if err == nil && joined > 0 {
				return nil
			}
			if err == nil {
				err = errors.New("seed returned no joined peers")
			}
			lastErr = fmt.Errorf("%s: %w", seed, err)
		}
		if err := waitRetry(ctx, deadline, retry); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("could not join gossip seeds before startup timeout: %w", lastErr)
			}
			return err
		}
	}
}

func boundedAttemptTimeout(deadline time.Time, maximum time.Duration) (time.Duration, error) {
	if deadline.IsZero() {
		return maximum, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	if remaining < maximum {
		return remaining, nil
	}
	return maximum, nil
}

func waitRetry(ctx context.Context, deadline time.Time, retry time.Duration) error {
	wait := retry
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		if remaining < wait {
			wait = remaining
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
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
