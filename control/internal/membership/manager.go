package membership

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

const (
	DefaultLeaveTimeout = 2 * time.Second
	DefaultTCPTimeout   = time.Second
)

var (
	ErrClosed = errors.New("membership manager is closed")
	ErrLeft   = errors.New("membership manager has left the cluster")
)

// Config describes one memberlist participant. A data sidecar uses RoleData
// and advertises its NodeService endpoint. The control plane uses RoleControl
// and participates only as a gossip observer/bootstrap peer.
type Config struct {
	Name        string
	Role        Role
	NodeID      string
	NodeAddress string

	BindAddr      string
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int

	ClusterLabel string
	SecretKey    []byte
	Local        bool // use memberlist's loopback-optimised defaults

	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	TCPTimeout       time.Duration
	GossipInterval   time.Duration
	PushPullInterval time.Duration
	SuspicionMult    int

	LeaveTimeout time.Duration
	Logger       *log.Logger
}

// Manager owns one memberlist participant and its published data-node view.
// Its lifecycle methods are safe for concurrent and repeated use.
type Manager struct {
	list *memberlist.Memberlist
	view *view

	leaveTimeout time.Duration

	lifecycleMu sync.Mutex
	left        bool
	leaveErr    error
	closed      bool
	closeErr    error
}

var _ Source = (*Manager)(nil)

// New validates cfg, opens the memberlist TCP/UDP listeners, and publishes the
// local participant. It does not contact any seed; callers choose whether an
// observer may bootstrap or a data sidecar must fail closed by how they handle
// Join.
func New(cfg Config) (*Manager, error) {
	meta := NodeMeta{Version: NodeMetaVersion, Role: cfg.Role}
	if cfg.Role == RoleData {
		meta.NodeID = cfg.NodeID
		meta.Address = cfg.NodeAddress
	}
	rawMeta, err := EncodeNodeMeta(meta)
	if err != nil {
		return nil, fmt.Errorf("local node metadata: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	base := memberlist.DefaultLANConfig()
	if cfg.Local {
		base = memberlist.DefaultLocalConfig()
	}
	v := newView()
	base.Name = cfg.Name
	base.BindAddr = cfg.BindAddr
	base.BindPort = cfg.BindPort
	base.AdvertiseAddr = cfg.AdvertiseAddr
	base.AdvertisePort = cfg.AdvertisePort
	base.Label = cfg.ClusterLabel
	base.SecretKey = bytes.Clone(cfg.SecretKey)
	base.Delegate = nodeMetaDelegate{raw: rawMeta}
	base.Events = &eventDelegate{view: v}
	base.Alive = aliveDelegate{}
	base.Logger = cfg.Logger
	if cfg.ProbeInterval > 0 {
		base.ProbeInterval = cfg.ProbeInterval
	}
	if cfg.ProbeTimeout > 0 {
		base.ProbeTimeout = cfg.ProbeTimeout
	}
	if cfg.TCPTimeout > 0 {
		base.TCPTimeout = cfg.TCPTimeout
	}
	if cfg.GossipInterval > 0 {
		base.GossipInterval = cfg.GossipInterval
	}
	if cfg.PushPullInterval > 0 {
		base.PushPullInterval = cfg.PushPullInterval
	}
	if cfg.SuspicionMult > 0 {
		base.SuspicionMult = cfg.SuspicionMult
	}

	list, err := memberlist.Create(base)
	if err != nil {
		return nil, fmt.Errorf("create memberlist participant %q: %w", cfg.Name, err)
	}
	leaveTimeout := cfg.LeaveTimeout
	if leaveTimeout == 0 {
		leaveTimeout = DefaultLeaveTimeout
	}
	return &Manager{list: list, view: v, leaveTimeout: leaveTimeout}, nil
}

func validateConfig(cfg Config) error {
	var problems []error
	if strings.TrimSpace(cfg.Name) == "" {
		problems = append(problems, errors.New("membership name must not be empty"))
	} else if cfg.Name != strings.TrimSpace(cfg.Name) {
		problems = append(problems, errors.New("membership name must not have surrounding whitespace"))
	}
	if cfg.BindAddr == "" {
		problems = append(problems, errors.New("membership bind address must not be empty"))
	} else if net.ParseIP(cfg.BindAddr) == nil {
		problems = append(problems, fmt.Errorf("membership bind address %q must be an IP address", cfg.BindAddr))
	}
	if cfg.BindPort < 0 || cfg.BindPort > 65535 {
		problems = append(problems, fmt.Errorf("membership bind port %d is not in 0..65535", cfg.BindPort))
	}
	if cfg.AdvertiseAddr != "" && net.ParseIP(cfg.AdvertiseAddr) == nil {
		problems = append(problems, fmt.Errorf("membership advertise address %q must be an IP address", cfg.AdvertiseAddr))
	}
	if cfg.AdvertisePort < 0 || cfg.AdvertisePort > 65535 {
		problems = append(problems, fmt.Errorf("membership advertise port %d is not in 0..65535", cfg.AdvertisePort))
	}
	if cfg.AdvertiseAddr != "" && cfg.AdvertisePort == 0 {
		problems = append(problems, errors.New("membership advertise port must be set when advertise address is set"))
	}
	if cfg.AdvertiseAddr == "" && cfg.AdvertisePort != 0 {
		problems = append(problems, errors.New("membership advertise address must be set when advertise port is set"))
	}
	if cfg.ClusterLabel == "" {
		problems = append(problems, errors.New("membership cluster label must not be empty"))
	} else if len(cfg.ClusterLabel) > memberlist.LabelMaxSize {
		problems = append(problems, fmt.Errorf("membership cluster label is %d bytes; limit is %d",
			len(cfg.ClusterLabel), memberlist.LabelMaxSize))
	}
	if len(cfg.SecretKey) != 0 && len(cfg.SecretKey) != 16 && len(cfg.SecretKey) != 24 && len(cfg.SecretKey) != 32 {
		problems = append(problems, fmt.Errorf("membership secret key must be 16, 24, or 32 bytes; got %d", len(cfg.SecretKey)))
	}
	if cfg.ProbeInterval < 0 || cfg.ProbeTimeout < 0 || cfg.TCPTimeout < 0 || cfg.GossipInterval < 0 ||
		cfg.PushPullInterval < 0 || cfg.LeaveTimeout < 0 {
		problems = append(problems, errors.New("membership durations must not be negative"))
	}
	if cfg.SuspicionMult < 0 {
		problems = append(problems, errors.New("membership suspicion multiplier must not be negative"))
	}
	if cfg.ProbeInterval > 0 && cfg.ProbeTimeout > cfg.ProbeInterval {
		problems = append(problems, fmt.Errorf("membership probe timeout %s exceeds probe interval %s",
			cfg.ProbeTimeout, cfg.ProbeInterval))
	}
	return errors.Join(problems...)
}

// Snapshot implements Source.
func (m *Manager) Snapshot() Snapshot {
	return m.view.Snapshot()
}

// LocalGossipAddress is the bound/advertised host:port suitable as a Join seed.
func (m *Manager) LocalGossipAddress() string {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	return m.list.LocalNode().Address()
}

// Join performs memberlist's TCP state exchange against one or more seeds.
// Success means at least one seed was contacted; memberlist deliberately
// discards individual seed errors once any contact succeeds.
func (m *Manager) Join(seeds []string) (int, error) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	if m.left {
		return 0, ErrLeft
	}
	n, err := m.list.Join(append([]string(nil), seeds...))
	if err != nil {
		return n, fmt.Errorf("join membership cluster: %w", err)
	}
	return n, nil
}

// Leave broadcasts a graceful departure but keeps memberlist's listeners
// running. A non-positive timeout uses Config.LeaveTimeout.
func (m *Manager) Leave(timeout time.Duration) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.closed {
		return ErrClosed
	}
	return m.leaveLocked(timeout)
}

func (m *Manager) leaveLocked(timeout time.Duration) error {
	if m.left {
		return m.leaveErr
	}
	if timeout <= 0 {
		timeout = m.leaveTimeout
	}
	m.leaveErr = m.list.Leave(timeout)
	// Memberlist marks itself left before waiting for propagation. Even if
	// the wait times out, retrying cannot produce a new leave broadcast.
	m.left = true
	return m.leaveErr
}

// Close gracefully leaves, then always shuts down the listeners. It is
// idempotent; all callers observe the first close result.
func (m *Manager) Close() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.closed {
		return m.closeErr
	}
	leaveErr := m.leaveLocked(m.leaveTimeout)
	shutdownErr := m.list.Shutdown()
	m.closed = true
	m.closeErr = errors.Join(leaveErr, shutdownErr)
	return m.closeErr
}

// Shutdown abruptly stops gossip without broadcasting a leave. Peers must
// detect this participant through SWIM probing/suspicion, which is the path
// used by failure and chaos tests.
func (m *Manager) Shutdown() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.closed {
		return m.closeErr
	}
	m.closeErr = m.list.Shutdown()
	m.closed = true
	return m.closeErr
}

type nodeMetaDelegate struct {
	raw []byte
}

func (d nodeMetaDelegate) NodeMeta(limit int) []byte {
	if len(d.raw) > limit {
		return nil
	}
	return bytes.Clone(d.raw)
}

func (nodeMetaDelegate) NotifyMsg([]byte) {}

func (nodeMetaDelegate) GetBroadcasts(int, int) [][]byte { return nil }

func (nodeMetaDelegate) LocalState(bool) []byte { return nil }

func (nodeMetaDelegate) MergeRemoteState([]byte, bool) {}
