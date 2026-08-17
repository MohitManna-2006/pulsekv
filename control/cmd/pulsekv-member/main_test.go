package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/config"
	"pulsekv/control/internal/membership"
)

type healthNode struct {
	nodev1.UnimplementedNodeServiceServer
	nodeID string
	ok     bool
}

func (n healthNode) HealthCheck(context.Context, *nodev1.HealthCheckRequest) (*nodev1.HealthCheckResponse, error) {
	return &nodev1.HealthCheckResponse{Ok: n.ok, NodeId: n.nodeID}, nil
}

func serveHealthNode(t *testing.T, nodeID string, ok bool) config.Node {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(srv, healthNode{nodeID: nodeID, ok: ok})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	host, portText, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var port int
	if _, err := fmt.Sscan(portText, &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return config.Node{NodeID: nodeID, Host: host, Port: port}
}

func TestProbeNodeValidatesIdentityAndHealth(t *testing.T) {
	good := serveHealthNode(t, "node-a", true)
	if err := probeNode(good, time.Second); err != nil {
		t.Fatalf("probe healthy node: %v", err)
	}

	wrong := good
	wrong.NodeID = "node-b"
	if err := probeNode(wrong, time.Second); err == nil || !strings.Contains(err.Error(), "want \"node-b\"") {
		t.Fatalf("wrong-identity error = %v", err)
	}

	unhealthy := serveHealthNode(t, "node-c", false)
	if err := probeNode(unhealthy, time.Second); err == nil || !strings.Contains(err.Error(), "ok=false") {
		t.Fatalf("unhealthy error = %v", err)
	}
}

func TestValidateOptions(t *testing.T) {
	valid := options{
		nodeID:           "node-a",
		startupTimeout:   time.Second,
		joinRetry:        time.Millisecond,
		joinTimeout:      time.Second,
		healthInterval:   time.Second,
		healthTimeout:    time.Millisecond,
		failureThreshold: 1,
		leaveTimeout:     time.Second,
	}
	if err := validateOptions(valid); err != nil {
		t.Fatalf("valid options: %v", err)
	}

	cases := []struct {
		name string
		edit func(*options)
		want string
	}{
		{"empty node", func(o *options) { o.nodeID = "" }, "--node-id"},
		{"zero startup", func(o *options) { o.startupTimeout = 0 }, "--startup-timeout"},
		{"zero retry", func(o *options) { o.joinRetry = 0 }, "--join-retry"},
		{"zero join timeout", func(o *options) { o.joinTimeout = 0 }, "--join-timeout"},
		{"zero health interval", func(o *options) { o.healthInterval = 0 }, "--health-interval"},
		{"zero health timeout", func(o *options) { o.healthTimeout = 0 }, "--health-timeout"},
		{"zero threshold", func(o *options) { o.failureThreshold = 0 }, "--failure-threshold"},
		{"zero leave", func(o *options) { o.leaveTimeout = 0 }, "--leave-timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := valid
			tc.edit(&got)
			if err := validateOptions(got); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want mention %s", err, tc.want)
			}
		})
	}
}

type mutableHealthNode struct {
	nodev1.UnimplementedNodeServiceServer
	nodeID string
	ok     atomic.Bool
}

func (n *mutableHealthNode) HealthCheck(context.Context,
	*nodev1.HealthCheckRequest) (*nodev1.HealthCheckResponse, error) {
	return &nodev1.HealthCheckResponse{Ok: n.ok.Load(), NodeId: n.nodeID}, nil
}

func TestSidecarBootstrapsThroughDataPeerAndRejoinsAfterRecovery(t *testing.T) {
	const (
		clusterName = "member-recovery-test"
		targetID    = "node-a"
	)

	seed, err := membership.New(membership.Config{
		Name:         "seed-peer",
		Role:         membership.RoleControl,
		BindAddr:     "127.0.0.1",
		BindPort:     0,
		ClusterLabel: clusterName,
		Local:        true,
		TCPTimeout:   50 * time.Millisecond,
		LeaveTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create seed peer: %v", err)
	}
	t.Cleanup(func() { _ = seed.Close() })
	seedHost, seedPortText, err := net.SplitHostPort(seed.LocalGossipAddress())
	if err != nil {
		t.Fatalf("split seed address: %v", err)
	}
	var seedPort int
	if _, err := fmt.Sscan(seedPortText, &seedPort); err != nil {
		t.Fatalf("parse seed port: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen health node: %v", err)
	}
	serverNode := &mutableHealthNode{nodeID: targetID}
	serverNode.ok.Store(true)
	grpcServer := grpc.NewServer()
	nodev1.RegisterNodeServiceServer(grpcServer, serverNode)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	targetHost, targetPortText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split target address: %v", err)
	}
	var targetPort int
	if _, err := fmt.Sscan(targetPortText, &targetPort); err != nil {
		t.Fatalf("parse target port: %v", err)
	}

	configPath := filepath.Join(t.TempDir(), "cluster.yaml")
	configBody := fmt.Sprintf(`
control_plane:
  host: 127.0.0.1
  port: %d
  gossip_port: %d
membership:
  cluster_name: %s
shard_count: 8
nodes:
  - {node_id: %s, host: %s, port: %d, gossip_port: %d}
  - {node_id: node-b, host: %s, port: %d, gossip_port: %d}
`, unusedTCPPort(t), unusedTCPPort(t), clusterName,
		targetID, targetHost, targetPort, unusedTCPPort(t),
		seedHost, unusedTCPPort(t), seedPort)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, options{
			configPath:       configPath,
			nodeID:           targetID,
			startupTimeout:   3 * time.Second,
			joinRetry:        10 * time.Millisecond,
			joinTimeout:      50 * time.Millisecond,
			healthInterval:   20 * time.Millisecond,
			healthTimeout:    50 * time.Millisecond,
			failureThreshold: 2,
			leaveTimeout:     time.Second,
		})
	}()
	t.Cleanup(cancel)

	waitForPublishedNode(t, seed, targetID, true, 3*time.Second)
	serverNode.ok.Store(false)
	waitForPublishedNode(t, seed, targetID, false, 3*time.Second)
	serverNode.ok.Store(true)
	waitForPublishedNode(t, seed, targetID, true, 3*time.Second)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sidecar shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sidecar did not stop after context cancellation")
	}
}

func unusedTCPPort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

func waitForPublishedNode(t *testing.T, source membership.Source, nodeID string,
	present bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		found := false
		for _, node := range source.Snapshot().Nodes {
			found = found || node.NodeID == nodeID
		}
		if found == present {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	found := false
	for _, node := range source.Snapshot().Nodes {
		found = found || node.NodeID == nodeID
	}
	t.Fatalf("node %q presence=%v, want %v (snapshot=%+v)",
		nodeID, found, present, source.Snapshot())
}
