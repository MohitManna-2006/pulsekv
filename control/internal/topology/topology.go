// Package topology fetches and validates coherent cluster-routing snapshots.
//
// ClusterMetadataService exposes the node list and shard map through separate
// RPCs. During membership churn or a publisher restart those calls can observe
// different snapshots, so consumers must match and verify the content-derived
// topology_fingerprint before installing either response. This package keeps
// that rule in one place for the SDK, diagnostics, and benchmarks.
package topology

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	metadatav1 "pulsekv/control/gen/metadata/v1"
)

const (
	maxCoherenceAttempts = 8
	// FingerprintSize is the SHA-256 topology identity size carried by Phase 3
	// metadata servers.
	FingerprintSize = sha256.Size
)

// Snapshot is one complete, validated routing topology.
//
// Maps are owned by the Snapshot and never alias protobuf response maps. Treat
// a Snapshot as immutable after construction.
type Snapshot struct {
	Generation  uint64
	Fingerprint []byte
	ShardCount  uint32
	ShardMap    map[uint32]string
	Nodes       map[string]string // node ID -> NodeService address
}

// Fetch reads node and shard metadata until both responses describe the same
// content-verified topology. A bounded retry prevents a broken or continuously
// churning server from spinning forever when the caller supplied no deadline.
func Fetch(ctx context.Context, metadata metadatav1.ClusterMetadataServiceClient) (Snapshot, error) {
	var nodeGeneration, shardGeneration uint64
	for attempt := 1; attempt <= maxCoherenceAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}

		nodesResp, err := metadata.GetNodeList(ctx, &metadatav1.GetNodeListRequest{})
		if err != nil {
			return Snapshot{}, fmt.Errorf("get node list: %w", err)
		}
		shardsResp, err := metadata.GetShardMap(ctx, &metadatav1.GetShardMapRequest{})
		if err != nil {
			return Snapshot{}, fmt.Errorf("get shard map: %w", err)
		}

		nodeGeneration = nodesResp.GetTopologyGeneration()
		shardGeneration = shardsResp.GetTopologyGeneration()
		nodeFingerprint := nodesResp.GetTopologyFingerprint()
		shardFingerprint := shardsResp.GetTopologyFingerprint()

		// Phase 3 publishers provide a content-derived fingerprint. Generation
		// alone is not a safe join key because a restarted publisher can reuse a
		// process-local counter for different topology content.
		if len(nodeFingerprint) > 0 || len(shardFingerprint) > 0 {
			if len(nodeFingerprint) == 0 || len(shardFingerprint) == 0 {
				continue // publisher changed between an old and new response
			}
			if len(nodeFingerprint) != FingerprintSize || len(shardFingerprint) != FingerprintSize {
				return Snapshot{}, fmt.Errorf(
					"metadata topology fingerprint sizes are invalid (nodes=%d, shards=%d; want %d)",
					len(nodeFingerprint), len(shardFingerprint), FingerprintSize)
			}
			if !bytes.Equal(nodeFingerprint, shardFingerprint) {
				continue
			}
			snapshot, err := validate(shardGeneration, shardsResp.GetShardCount(),
				nodesResp.GetNodes(), shardsResp.GetShardToNodeId())
			if err != nil {
				return Snapshot{}, err
			}
			expectedFingerprint := Fingerprint(snapshot.ShardCount, snapshot.Nodes, snapshot.ShardMap)
			if !bytes.Equal(shardFingerprint, expectedFingerprint) {
				return Snapshot{}, errors.New("metadata topology fingerprint does not match response content")
			}
			snapshot.Fingerprint = bytes.Clone(shardFingerprint)
			return snapshot, nil
		}

		// Backwards-compatible fallback for pre-fingerprint servers. New Phase 3
		// deployments never take this branch.
		if nodeGeneration != shardGeneration {
			continue
		}
		shardCount := shardsResp.GetShardCount()
		if shardCount == 0 && len(shardsResp.GetShardToNodeId()) > 0 {
			shardCount = uint32(len(shardsResp.GetShardToNodeId()))
		}
		return validate(nodeGeneration, shardCount,
			nodesResp.GetNodes(), shardsResp.GetShardToNodeId())
	}

	return Snapshot{}, fmt.Errorf(
		"metadata topology snapshots did not converge after %d attempts (node generation=%d, shard generation=%d)",
		maxCoherenceAttempts, nodeGeneration, shardGeneration)
}

// Fingerprint returns the canonical SHA-256 identity of a complete topology.
// It includes node identities, service addresses, logical shard count, and the
// owner of every published shard. Map iteration order therefore cannot affect
// the result.
func Fingerprint(shardCount uint32, nodes map[string]string, shardMap map[uint32]string) []byte {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	hash := sha256.New()
	_, _ = hash.Write([]byte("pulsekv-topology-v1\x00"))
	writeUint32(hash, shardCount)
	writeUint32(hash, uint32(len(ids)))
	for _, id := range ids {
		writeBytes(hash, []byte(id))
		writeBytes(hash, []byte(nodes[id]))
	}
	writeUint32(hash, uint32(len(shardMap)))
	for shard := uint32(0); shard < shardCount; shard++ {
		owner, ok := shardMap[shard]
		if !ok {
			continue
		}
		writeUint32(hash, shard)
		writeBytes(hash, []byte(owner))
	}
	return hash.Sum(nil)
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeBytes(dst hashWriter, value []byte) {
	writeUint32(dst, uint32(len(value)))
	_, _ = dst.Write(value)
}

func writeUint32(dst hashWriter, value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	_, _ = dst.Write(raw[:])
}

// Validate copies and validates a node-list/shard-map pair from one generation.
func Validate(generation uint64, nodes []*metadatav1.NodeInfo, shardMap map[uint32]string) (Snapshot, error) {
	if uint64(len(shardMap)) > uint64(^uint32(0)) {
		return Snapshot{}, errors.New("metadata shard map is too large")
	}
	return validate(generation, uint32(len(shardMap)), nodes, shardMap)
}

func validate(generation uint64, shardCount uint32, nodes []*metadatav1.NodeInfo,
	shardMap map[uint32]string) (Snapshot, error) {
	if shardCount == 0 {
		return Snapshot{}, errors.New("metadata returned a zero shard count")
	}
	byID := make(map[string]string, len(nodes))
	byAddress := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node == nil || node.GetNodeId() == "" || node.GetAddress() == "" {
			return Snapshot{}, errors.New("metadata returned a node with an empty ID or address")
		}
		if _, exists := byID[node.GetNodeId()]; exists {
			return Snapshot{}, fmt.Errorf("metadata returned duplicate node ID %q", node.GetNodeId())
		}
		if previousID, exists := byAddress[node.GetAddress()]; exists {
			return Snapshot{}, fmt.Errorf("metadata returned duplicate node address %q for IDs %q and %q",
				node.GetAddress(), previousID, node.GetNodeId())
		}
		byID[node.GetNodeId()] = node.GetAddress()
		byAddress[node.GetAddress()] = node.GetNodeId()
	}
	if uint64(len(shardMap)) > uint64(^uint32(0)) {
		return Snapshot{}, errors.New("metadata shard map is too large")
	}
	if len(nodes) == 0 {
		if len(shardMap) != 0 {
			return Snapshot{}, errors.New("metadata returned shard owners with no live nodes")
		}
		return Snapshot{
			Generation: generation,
			ShardCount: shardCount,
			ShardMap:   map[uint32]string{},
			Nodes:      byID,
		}, nil
	}
	if uint32(len(shardMap)) != shardCount {
		return Snapshot{}, fmt.Errorf("metadata returned %d shard owners for shard_count=%d",
			len(shardMap), shardCount)
	}

	shards := make(map[uint32]string, len(shardMap))
	for shard := uint32(0); shard < shardCount; shard++ {
		owner, ok := shardMap[shard]
		if !ok {
			return Snapshot{}, fmt.Errorf("metadata shard map is missing shard %d", shard)
		}
		if _, ok := byID[owner]; !ok {
			return Snapshot{}, fmt.Errorf("metadata shard %d has unknown owner %q", shard, owner)
		}
		shards[shard] = owner
	}

	return Snapshot{
		Generation: generation,
		ShardCount: shardCount,
		ShardMap:   shards,
		Nodes:      byID,
	}, nil
}

// OwnerAddresses returns the sorted, unique addresses that currently own at
// least one shard. Known but unowned nodes must not receive cluster-wide scans.
func (s Snapshot) OwnerAddresses() []string {
	seen := make(map[string]struct{}, len(s.Nodes))
	for _, owner := range s.ShardMap {
		if address := s.Nodes[owner]; address != "" {
			seen[address] = struct{}{}
		}
	}
	addresses := make([]string, 0, len(seen))
	for address := range seen {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses
}

// OwnsAddress reports whether address currently owns at least one shard.
func (s Snapshot) OwnsAddress(address string) bool {
	for _, owner := range s.ShardMap {
		if s.Nodes[owner] == address {
			return true
		}
	}
	return false
}
