// Package membership maintains PulseKV's live data-node membership view.
//
// Hashicorp memberlist owns SWIM probing, suspicion, and gossip convergence.
// This package owns the application-level contract layered on top of it:
// versioned node metadata, filtering control-plane observers out of the data
// topology, and publishing immutable, deterministic snapshots for the
// metadata service.
package membership

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/memberlist"
)

const (
	NodeMetaVersion uint8 = 1
	maxNodeIDBytes        = 64
)

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Role distinguishes routing candidates from gossip-only observers.
type Role string

const (
	RoleData    Role = "data"
	RoleControl Role = "control"
)

// NodeMeta is carried in memberlist's bounded per-node metadata field.
// NodeID is deliberately separate from memberlist's transport-level Name: a
// sidecar may have its own process identity while representing a data node.
type NodeMeta struct {
	Version uint8  `json:"version"`
	Role    Role   `json:"role"`
	NodeID  string `json:"node_id,omitempty"`
	Address string `json:"address,omitempty"`
}

// EncodeNodeMeta validates and deterministically encodes metadata for gossip.
func EncodeNodeMeta(meta NodeMeta) ([]byte, error) {
	if err := validateNodeMeta(meta); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encode node metadata: %w", err)
	}
	if len(raw) > memberlist.MetaMaxSize {
		return nil, fmt.Errorf("encoded node metadata is %d bytes; memberlist limit is %d",
			len(raw), memberlist.MetaMaxSize)
	}
	return raw, nil
}

// DecodeNodeMeta validates metadata received from gossip. Unknown JSON fields
// are tolerated so adding optional fields remains rolling-upgrade friendly;
// the explicit version still rejects an incompatible wire shape.
func DecodeNodeMeta(raw []byte) (NodeMeta, error) {
	if len(raw) == 0 {
		return NodeMeta{}, errors.New("node metadata is empty")
	}
	if len(raw) > memberlist.MetaMaxSize {
		return NodeMeta{}, fmt.Errorf("node metadata is %d bytes; memberlist limit is %d",
			len(raw), memberlist.MetaMaxSize)
	}
	var meta NodeMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return NodeMeta{}, fmt.Errorf("decode node metadata: %w", err)
	}
	if err := validateNodeMeta(meta); err != nil {
		return NodeMeta{}, err
	}
	return meta, nil
}

func validateNodeMeta(meta NodeMeta) error {
	if meta.Version != NodeMetaVersion {
		return fmt.Errorf("unsupported node metadata version %d (want %d)",
			meta.Version, NodeMetaVersion)
	}
	switch meta.Role {
	case RoleControl:
		// Observer metadata intentionally has no data-plane identity. Rejecting
		// these fields catches an accidentally misclassified data sidecar.
		if meta.NodeID != "" || meta.Address != "" {
			return errors.New("control metadata must not include node_id or address")
		}
		return nil
	case RoleData:
		if strings.TrimSpace(meta.NodeID) == "" {
			return errors.New("data metadata node_id must not be empty")
		}
		if meta.NodeID != strings.TrimSpace(meta.NodeID) {
			return errors.New("data metadata node_id must not have surrounding whitespace")
		}
		if len(meta.NodeID) > maxNodeIDBytes {
			return fmt.Errorf("data metadata node_id exceeds the %d-byte limit", maxNodeIDBytes)
		}
		if !nodeIDPattern.MatchString(meta.NodeID) {
			return errors.New("data metadata node_id must match [A-Za-z0-9][A-Za-z0-9._-]*")
		}
		if strings.TrimSpace(meta.Address) == "" {
			return errors.New("data metadata address must not be empty")
		}
		if meta.Address != strings.TrimSpace(meta.Address) {
			return errors.New("data metadata address must not have surrounding whitespace")
		}
		if err := validateServiceAddress(meta.Address); err != nil {
			return fmt.Errorf("data metadata address: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown node metadata role %q", meta.Role)
	}
}

func validateServiceAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%q is not host:port: %w", address, err)
	}
	if strings.TrimSpace(host) == "" {
		return errors.New("host must not be empty")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port %q is not in 1..65535", portText)
	}
	return nil
}
