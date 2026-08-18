package metadata

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metadatav1 "pulsekv/control/gen/metadata/v1"
	"pulsekv/control/internal/membership"
)

// gatedSource is a Source that also implements membership.Readiness, which is
// how a Raft-backed store presents itself.
type gatedSource struct {
	mutableSource

	mu    sync.RWMutex
	ready error
}

func (s *gatedSource) ServeReady() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

func (s *gatedSource) setReady(err error) {
	s.mu.Lock()
	s.ready = err
	s.mu.Unlock()
}

// The bug, at the service level: a source holding a zero-valued state must not
// be published as an authoritative empty cluster while it says it is catching
// up. Both RPCs, because a caller reads them as a pair.
func TestUncaughtUpSourceIsRefusedRatherThanPublishedEmpty(t *testing.T) {
	source := &gatedSource{}
	source.set(membership.Snapshot{}) // exactly what a freshly started FSM holds
	source.setReady(errors.New("committed through index 0, needs 9"))
	svc := newTestService(t, 256, source)

	nodes, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err == nil {
		t.Fatalf("GetNodeList published an authoritative topology while catching up: "+
			"%d node(s) at generation %d", len(nodes.GetNodes()), nodes.GetTopologyGeneration())
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("GetNodeList while catching up returned %s, want %s", got, codes.Unavailable)
	}
	// The message has to identify the condition. A caller reading it during an
	// incident must be able to tell "this replica is still starting" from
	// "this replica is broken".
	if msg := status.Convert(err).Message(); !contains(msg, "catching up") ||
		!contains(msg, "needs 9") {
		t.Fatalf("GetNodeList error does not identify the startup condition: %q", msg)
	}

	shards, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{})
	if err == nil {
		t.Fatalf("GetShardMap published a shard map while catching up: %d shard(s) at generation %d",
			len(shards.GetShardToNodeId()), shards.GetTopologyGeneration())
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("GetShardMap while catching up returned %s, want %s", got, codes.Unavailable)
	}
}

// The other half of the contract, and the reason the gate is a source's own
// judgement rather than "is the node set empty": a caught-up replica in a
// genuinely empty cluster must still publish that, because Phase 3 made an
// empty cluster an authoritative state clients install.
func TestCaughtUpSourceStillPublishesAGenuinelyEmptyCluster(t *testing.T) {
	source := &gatedSource{}
	source.set(membership.Snapshot{Generation: 4})
	source.setReady(nil)
	svc := newTestService(t, 256, source)

	nodes, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{})
	if err != nil {
		t.Fatalf("GetNodeList on a caught-up empty cluster: %v", err)
	}
	if len(nodes.GetNodes()) != 0 {
		t.Fatalf("expected no nodes, got %d", len(nodes.GetNodes()))
	}
	if nodes.GetTopologyGeneration() != 4 {
		t.Fatalf("generation = %d, want 4", nodes.GetTopologyGeneration())
	}
	if _, err := svc.GetShardMap(context.Background(), &metadatav1.GetShardMapRequest{}); err != nil {
		t.Fatalf("GetShardMap on a caught-up empty cluster: %v", err)
	}
}

// A source that cannot answer the readiness question -- every Phase 3/4
// gossip-backed control plane -- must behave exactly as it did before.
func TestSourceWithoutReadinessIsAlwaysServed(t *testing.T) {
	source := &mutableSource{snapshot: membership.Snapshot{}}
	svc := newTestService(t, 256, source)
	if svc.readiness != nil {
		t.Fatal("a plain Source must not be treated as reporting readiness")
	}
	if _, err := svc.GetNodeList(context.Background(), &metadatav1.GetNodeListRequest{}); err != nil {
		t.Fatalf("GetNodeList on a gossip-backed source: %v", err)
	}
}

// HealthCheck is process liveness, not topology authority. The deploy scripts
// wait on it to decide a replica has started at all, so gating it would
// deadlock a boot that the readiness gate is meant to protect.
func TestHealthCheckIsNotGatedOnReadiness(t *testing.T) {
	source := &gatedSource{}
	source.setReady(errors.New("no leader has been seen since this replica started"))
	svc := newTestService(t, 256, source)

	resp, err := svc.HealthCheck(context.Background(), &metadatav1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck while catching up: %v", err)
	}
	if !resp.GetOk() {
		t.Fatal("HealthCheck reported not-ok while catching up")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
