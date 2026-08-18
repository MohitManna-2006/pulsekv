package client

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
)

// Step 3 of the restart-readiness fix, stated as a test rather than assumed: a
// replica that ANSWERS with an error must be treated exactly like one that
// cannot be reached at all. The readiness gate turns a wrong answer into an
// Unavailable, and that is only an improvement if the SDK moves on rather than
// surfacing it.
//
// The distinction matters because the two failures do not look alike at the
// transport: a stopped replica refuses the connection, while a replica that is
// up but catching up completes the RPC and returns a status.
func TestRefreshFallsBackWhenAReplicaAnswersWithAnError(t *testing.T) {
	catchingUp := status.Error(codes.Unavailable,
		"this control-plane replica is still catching up and will not publish a topology yet")

	// The replica the client prefers is up, reachable, and refusing.
	notReady := &testMetadata{}
	notReady.setErrors(catchingUp, catchingUp)
	notReadyAddress := serveTestGRPC(t, func(srv *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(srv, notReady)
	})

	nodes := []*metadatav1.NodeInfo{{NodeId: "node-0", Address: "127.0.0.1:7100", Alive: true}}
	shards := map[uint32]string{0: "node-0", 1: "node-0"}
	ready := &testMetadata{}
	ready.setTopologyGeneration(nodes, shards, 12)
	readyAddress := serveTestGRPC(t, func(srv *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(srv, ready)
	})

	client, err := New(notReadyAddress+","+readyAddress, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New across a catching-up replica and a ready one: %v", err)
	}
	defer client.Close()

	client.mu.RLock()
	generation := client.topology.Generation
	nodeCount := len(client.topology.Nodes)
	preferred := client.preferred
	client.mu.RUnlock()

	if nodeCount == 0 {
		t.Fatal("the client installed an empty topology instead of falling back to a ready replica")
	}
	if generation != 12 {
		t.Fatalf("installed generation %d, want the ready replica's 12", generation)
	}
	// Sticking to whichever replica answered is what keeps a healthy client off
	// the one that is still starting.
	if preferred != 1 {
		t.Fatalf("preferred endpoint = %d, want 1 (the replica that answered)", preferred)
	}
	if notReady.nodeCalls.Load() == 0 {
		t.Fatal("the catching-up replica was never tried; the fallback proved nothing")
	}
}

// The other half: when EVERY replica is catching up, the client must keep the
// topology it already holds rather than replacing it with nothing. This is what
// makes the gate invisible to a caller during a full control-plane restart.
func TestRefreshRetainsTopologyWhenEveryReplicaIsCatchingUp(t *testing.T) {
	nodes := []*metadatav1.NodeInfo{{NodeId: "node-0", Address: "127.0.0.1:7100", Alive: true}}
	shards := map[uint32]string{0: "node-0", 1: "node-0"}

	first := &testMetadata{}
	first.setTopologyGeneration(nodes, shards, 3)
	firstAddress := serveTestGRPC(t, func(srv *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(srv, first)
	})
	second := &testMetadata{}
	second.setTopologyGeneration(nodes, shards, 3)
	secondAddress := serveTestGRPC(t, func(srv *grpc.Server) {
		metadatav1.RegisterClusterMetadataServiceServer(srv, second)
	})

	client, err := New(firstAddress+","+secondAddress, WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	catchingUp := status.Error(codes.Unavailable, "still catching up")
	first.setErrors(catchingUp, catchingUp)
	second.setErrors(catchingUp, catchingUp)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.refresh(ctx); err == nil {
		t.Fatal("refresh reported success while every replica was catching up")
	}

	client.mu.RLock()
	generation := client.topology.Generation
	nodeCount := len(client.topology.Nodes)
	client.mu.RUnlock()
	if generation != 3 || nodeCount != 1 {
		t.Fatalf("client dropped its last complete topology: generation %d with %d node(s), want 3 and 1",
			generation, nodeCount)
	}
	if _, err := client.clientForKey([]byte("any-key")); err != nil {
		t.Fatalf("routing broke while every replica was catching up: %v", err)
	}
}
