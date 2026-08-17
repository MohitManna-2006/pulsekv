package membership

import (
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T, name string, role Role, nodeID, nodeAddress string) Config {
	t.Helper()
	return Config{
		Name:             name,
		Role:             role,
		NodeID:           nodeID,
		NodeAddress:      nodeAddress,
		BindAddr:         "127.0.0.1",
		BindPort:         0,
		ClusterLabel:     "pulsekv-test-" + strings.ReplaceAll(t.Name(), "/", "-"),
		Local:            true,
		ProbeInterval:    40 * time.Millisecond,
		ProbeTimeout:     20 * time.Millisecond,
		GossipInterval:   10 * time.Millisecond,
		PushPullInterval: 100 * time.Millisecond,
		SuspicionMult:    1,
		LeaveTimeout:     time.Second,
		Logger:           log.New(io.Discard, "", 0),
	}
}

func TestManagerJoinAndGracefulLeave(t *testing.T) {
	observer, err := New(testConfig(t, "observer", RoleControl, "", ""))
	if err != nil {
		t.Fatalf("New observer: %v", err)
	}
	t.Cleanup(func() { _ = observer.Close() })

	data, err := New(testConfig(t, "agent-a", RoleData, "node-a", "127.0.0.1:7100"))
	if err != nil {
		t.Fatalf("New data: %v", err)
	}
	t.Cleanup(func() { _ = data.Shutdown() })

	if got := observer.Snapshot(); got.Generation != 0 || len(got.Nodes) != 0 {
		t.Fatalf("observer initial snapshot = %+v", got)
	}
	if got := data.Snapshot(); got.Generation != 1 || len(got.Nodes) != 1 {
		t.Fatalf("data initial snapshot = %+v", got)
	}
	joined, err := data.Join([]string{observer.LocalGossipAddress()})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if joined != 1 {
		t.Fatalf("Join contacted %d seeds, want 1", joined)
	}
	waitForNodes(t, observer, []Node{{NodeID: "node-a", Address: "127.0.0.1:7100"}}, 3*time.Second)

	if err := data.Close(); err != nil {
		t.Fatalf("data Close: %v", err)
	}
	waitForNodes(t, observer, nil, 3*time.Second)
	if err := data.Close(); err != nil {
		t.Fatalf("repeated data Close: %v", err)
	}
	if _, err := data.Join([]string{observer.LocalGossipAddress()}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Join after Close error = %v, want ErrClosed", err)
	}
}

func TestManagerAbruptShutdownIsDetectedAndSameIdentityCanRejoin(t *testing.T) {
	observer, err := New(testConfig(t, "observer", RoleControl, "", ""))
	if err != nil {
		t.Fatalf("New observer: %v", err)
	}
	t.Cleanup(func() { _ = observer.Close() })

	cfg := testConfig(t, "agent-a", RoleData, "node-a", "127.0.0.1:7100")
	data, err := New(cfg)
	if err != nil {
		t.Fatalf("New data: %v", err)
	}
	if _, err := data.Join([]string{observer.LocalGossipAddress()}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	waitForNodes(t, observer, []Node{{NodeID: "node-a", Address: "127.0.0.1:7100"}}, 3*time.Second)

	_, portText, ok := strings.Cut(data.LocalGossipAddress(), ":")
	if !ok {
		t.Fatalf("local gossip address %q is not host:port", data.LocalGossipAddress())
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatalf("parse gossip port %q: %v", portText, err)
	}
	if err := data.Shutdown(); err != nil {
		t.Fatalf("abrupt Shutdown: %v", err)
	}
	waitForNodes(t, observer, nil, 5*time.Second)

	// A real deployment restarts a sidecar on its stable gossip port. The
	// previous member is dead (not gracefully left), so using the same name and
	// address exercises memberlist's incarnation refutation path.
	cfg.BindPort = port
	restarted, err := New(cfg)
	if err != nil {
		t.Fatalf("restart data: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if _, err := restarted.Join([]string{observer.LocalGossipAddress()}); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	waitForNodes(t, observer, []Node{{NodeID: "node-a", Address: "127.0.0.1:7100"}}, 5*time.Second)
}

func TestManagerRetainsLastGoodUntilDuplicateIdentityResolves(t *testing.T) {
	observer, err := New(testConfig(t, "observer", RoleControl, "", ""))
	if err != nil {
		t.Fatalf("New observer: %v", err)
	}
	t.Cleanup(func() { _ = observer.Close() })

	first, err := New(testConfig(t, "agent-a", RoleData, "node-a", "127.0.0.1:7100"))
	if err != nil {
		t.Fatalf("New first data member: %v", err)
	}
	t.Cleanup(func() { _ = first.Shutdown() })
	if _, err := first.Join([]string{observer.LocalGossipAddress()}); err != nil {
		t.Fatalf("first Join: %v", err)
	}
	lastGood := waitForNodes(t, observer,
		[]Node{{NodeID: "node-a", Address: "127.0.0.1:7100"}}, 3*time.Second)

	duplicate, err := New(testConfig(t, "agent-duplicate", RoleData, "node-a", "127.0.0.1:7101"))
	if err != nil {
		t.Fatalf("New duplicate data member: %v", err)
	}
	t.Cleanup(func() { _ = duplicate.Close() })
	if _, err := duplicate.Join([]string{observer.LocalGossipAddress()}); err != nil {
		t.Fatalf("duplicate Join: %v", err)
	}
	waitForMemberCount(t, observer, 3, 3*time.Second)
	assertSnapshot(t, observer.Snapshot(), lastGood)

	// Once the original leaves, the retained duplicate record becomes an
	// unambiguous candidate and can be published without rediscovery.
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	want := []Node{{NodeID: "node-a", Address: "127.0.0.1:7101"}}
	resolved := waitForNodes(t, observer, want, 3*time.Second)
	if resolved.Generation != lastGood.Generation+1 {
		t.Fatalf("resolved generation = %d, want %d", resolved.Generation, lastGood.Generation+1)
	}
}

func TestManagerClusterLabelPreventsAccidentalMerge(t *testing.T) {
	observerCfg := testConfig(t, "observer", RoleControl, "", "")
	observer, err := New(observerCfg)
	if err != nil {
		t.Fatalf("New observer: %v", err)
	}
	t.Cleanup(func() { _ = observer.Close() })

	dataCfg := testConfig(t, "agent-a", RoleData, "node-a", "127.0.0.1:7100")
	dataCfg.ClusterLabel += "-different"
	data, err := New(dataCfg)
	if err != nil {
		t.Fatalf("New data: %v", err)
	}
	t.Cleanup(func() { _ = data.Close() })

	if _, err := data.Join([]string{observer.LocalGossipAddress()}); err == nil {
		t.Fatal("Join across different cluster labels succeeded")
	}
	if got := observer.Snapshot(); got.Generation != 0 || len(got.Nodes) != 0 {
		t.Fatalf("mismatched-label member changed observer snapshot: %+v", got)
	}
}

func TestManagerLifecycleIsConcurrentAndIdempotent(t *testing.T) {
	m, err := New(testConfig(t, "agent-a", RoleData, "node-a", "127.0.0.1:7100"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const callers = 16
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- m.Close()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	}
	if err := m.Shutdown(); err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}
	if err := m.Leave(time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Leave after Close error = %v, want ErrClosed", err)
	}
}

func TestManagerRejectsInvalidConfig(t *testing.T) {
	base := testConfig(t, "observer", RoleControl, "", "")
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"name", func(c *Config) { c.Name = "" }, "name"},
		{"bind address", func(c *Config) { c.BindAddr = "localhost" }, "IP address"},
		{"bind port", func(c *Config) { c.BindPort = 70000 }, "bind port"},
		{"advertise address", func(c *Config) { c.AdvertiseAddr = "localhost" }, "IP address"},
		{"advertise port", func(c *Config) { c.AdvertisePort = -1 }, "advertise port"},
		{"advertise pair missing port", func(c *Config) { c.AdvertiseAddr = "127.0.0.1" }, "port must be set"},
		{"advertise pair missing address", func(c *Config) { c.AdvertisePort = 7200 }, "address must be set"},
		{"label", func(c *Config) { c.ClusterLabel = "" }, "label"},
		{"secret", func(c *Config) { c.SecretKey = []byte("short") }, "secret key"},
		{"duration", func(c *Config) { c.GossipInterval = -time.Second }, "durations"},
		{"suspicion", func(c *Config) { c.SuspicionMult = -1 }, "suspicion"},
		{"probe relationship", func(c *Config) {
			c.ProbeInterval = 10 * time.Millisecond
			c.ProbeTimeout = 20 * time.Millisecond
		}, "exceeds"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := New(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func waitForNodes(t *testing.T, source Source, want []Node, timeout time.Duration) Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got Snapshot
	for {
		got = source.Snapshot()
		if nodesEqual(got.Nodes, want) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s: nodes = %+v, want %+v (generation %d)",
				timeout, got.Nodes, want, got.Generation)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForMemberCount(t *testing.T, manager *Manager, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := manager.list.NumMembers(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s: member count = %d, want %d",
				timeout, manager.list.NumMembers(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
