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
	"pulsekv/control/internal/router"
)

const (
	maxCoherenceAttempts = 8
	// FingerprintSize is the SHA-256 topology identity size carried by Phase 3
	// and later metadata servers.
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

	// Owners carries the Phase 4 primary-plus-replica placement for every
	// shard. Owners[s].Primary always equals ShardMap[s] -- validate() rejects
	// a response where they disagree -- so a caller that only routes reads and
	// writes can keep using ShardMap and ignore this entirely.
	//
	// Nil against a pre-Phase-4 metadata server, which publishes no owner map.
	Owners map[uint32]router.ShardOwners

	// ReplicationFactor is the configured replicas-per-shard the publisher
	// reported. The count actually present in Owners can be lower when the
	// cluster has fewer live nodes than 1 + ReplicationFactor.
	ReplicationFactor uint32

	// RaftLeaderID and RaftTerm are what the answering replica believed about
	// the metadata group's leadership. Added in Phase 5, and DIAGNOSTIC ONLY:
	// neither is part of the fingerprint, neither is validated, and neither
	// affects routing. They exist so a harness can assert that leadership moved
	// rather than infer it from timing.
	//
	// Empty and zero against a control plane with no Raft group, and empty
	// while the group is electing -- an honest "nobody right now" rather than a
	// stale guess.
	RaftLeaderID string
	RaftTerm     uint64
}

// Replicas returns the replica node IDs for one shard, or nil when the shard
// has none (replication factor 0, a pre-Phase-4 publisher, or an unknown
// shard). The returned slice aliases the Snapshot and must not be mutated.
func (s Snapshot) Replicas(shard uint32) []string {
	return s.Owners[shard].Replicas
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
				nodesResp.GetNodes(), shardsResp.GetShardToNodeId(),
				shardsResp.GetShardToOwners(), shardsResp.GetReplicationFactor())
			if err != nil {
				return Snapshot{}, err
			}
			expectedFingerprint := Fingerprint(snapshot.FingerprintInput())
			if !bytes.Equal(shardFingerprint, expectedFingerprint) {
				return Snapshot{}, errors.New("metadata topology fingerprint does not match response content")
			}
			snapshot.Fingerprint = bytes.Clone(shardFingerprint)
			// Attached after validation and deliberately NOT hashed: two
			// replicas at the same committed state must fingerprint the same
			// even when one has not yet noticed an election.
			snapshot.RaftLeaderID = shardsResp.GetRaftLeaderId()
			snapshot.RaftTerm = shardsResp.GetRaftTerm()
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
		snapshot, err := validate(nodeGeneration, shardCount,
			nodesResp.GetNodes(), shardsResp.GetShardToNodeId(),
			shardsResp.GetShardToOwners(), shardsResp.GetReplicationFactor())
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.RaftLeaderID = shardsResp.GetRaftLeaderId()
		snapshot.RaftTerm = shardsResp.GetRaftTerm()
		return snapshot, nil
	}

	return Snapshot{}, fmt.Errorf(
		"metadata topology snapshots did not converge after %d attempts (node generation=%d, shard generation=%d)",
		maxCoherenceAttempts, nodeGeneration, shardGeneration)
}

// FingerprintInput is the complete content one topology fingerprint covers.
//
// It is a struct rather than a parameter list because Phase 4 widened what
// "the topology" means -- replica placement and the configured replication
// factor are now part of the identity -- and a five-argument hash function is
// how call sites start silently passing the wrong map.
type FingerprintInput struct {
	ShardCount        uint32
	ReplicationFactor uint32
	Nodes             map[string]string // node ID -> NodeService address
	ShardMap          map[uint32]string
	Owners            map[uint32]router.ShardOwners
}

// FingerprintInput extracts the hashable content of a validated Snapshot, so a
// consumer re-derives the identity from exactly what it installed.
func (s Snapshot) FingerprintInput() FingerprintInput {
	return FingerprintInput{
		ShardCount:        s.ShardCount,
		ReplicationFactor: s.ReplicationFactor,
		Nodes:             s.Nodes,
		ShardMap:          s.ShardMap,
		Owners:            s.Owners,
	}
}

// Fingerprint returns the canonical SHA-256 identity of a complete topology:
// node identities, service addresses, logical shard count, replication factor,
// the primary of every published shard, and that shard's ordered replicas.
// Every field is length-prefixed and every map is walked in a fixed order, so
// map iteration order cannot affect the result and no two distinct topologies
// can produce the same byte stream by concatenation.
//
// The "v2" domain tag is not decoration. Phase 3 hashed a strictly smaller
// input under "v1", so a Phase 3 publisher and a Phase 4 client would compute
// different digests for the same cluster. Fetch treats that as a hard error
// rather than installing a half-understood topology, which is the correct
// outcome: the two are genuinely not describing the same thing. The generation
// -only path below still serves pre-fingerprint servers.
func Fingerprint(in FingerprintInput) []byte {
	ids := make([]string, 0, len(in.Nodes))
	for id := range in.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	hash := sha256.New()
	_, _ = hash.Write([]byte("pulsekv-topology-v2\x00"))
	writeUint32(hash, in.ShardCount)
	writeUint32(hash, in.ReplicationFactor)
	writeUint32(hash, uint32(len(ids)))
	for _, id := range ids {
		writeBytes(hash, []byte(id))
		writeBytes(hash, []byte(in.Nodes[id]))
	}
	writeUint32(hash, uint32(len(in.ShardMap)))
	for shard := uint32(0); shard < in.ShardCount; shard++ {
		owner, ok := in.ShardMap[shard]
		if !ok {
			continue
		}
		writeUint32(hash, shard)
		writeBytes(hash, []byte(owner))
	}
	writeUint32(hash, uint32(len(in.Owners)))
	for shard := uint32(0); shard < in.ShardCount; shard++ {
		owners, ok := in.Owners[shard]
		if !ok {
			continue
		}
		writeUint32(hash, shard)
		writeBytes(hash, []byte(owners.Primary))
		writeUint32(hash, uint32(len(owners.Replicas)))
		for _, replica := range owners.Replicas {
			writeBytes(hash, []byte(replica))
		}
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
	return validate(generation, uint32(len(shardMap)), nodes, shardMap, nil, 0)
}

func validate(generation uint64, shardCount uint32, nodes []*metadatav1.NodeInfo,
	shardMap map[uint32]string, ownerMap map[uint32]*metadatav1.ShardOwners,
	replicationFactor uint32) (Snapshot, error) {
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
		if len(shardMap) != 0 || len(ownerMap) != 0 {
			return Snapshot{}, errors.New("metadata returned shard owners with no live nodes")
		}
		return Snapshot{
			Generation:        generation,
			ShardCount:        shardCount,
			ShardMap:          map[uint32]string{},
			Nodes:             byID,
			Owners:            map[uint32]router.ShardOwners{},
			ReplicationFactor: replicationFactor,
		}, nil
	}
	if uint32(len(shardMap)) != shardCount {
		return Snapshot{}, fmt.Errorf("metadata returned %d shard owners for shard_count=%d",
			len(shardMap), shardCount)
	}
	// A publisher either sends the whole owner map or none of it. A partial one
	// would leave some shards silently unreplicated with nothing saying so.
	if len(ownerMap) != 0 && uint32(len(ownerMap)) != shardCount {
		return Snapshot{}, fmt.Errorf("metadata returned %d shard owner entries for shard_count=%d",
			len(ownerMap), shardCount)
	}

	shards := make(map[uint32]string, len(shardMap))
	var owners map[uint32]router.ShardOwners
	if len(ownerMap) != 0 {
		owners = make(map[uint32]router.ShardOwners, len(ownerMap))
	}

	for shard := uint32(0); shard < shardCount; shard++ {
		owner, ok := shardMap[shard]
		if !ok {
			return Snapshot{}, fmt.Errorf("metadata shard map is missing shard %d", shard)
		}
		if _, ok := byID[owner]; !ok {
			return Snapshot{}, fmt.Errorf("metadata shard %d has unknown owner %q", shard, owner)
		}
		shards[shard] = owner

		if owners == nil {
			continue
		}
		entry, ok := ownerMap[shard]
		if !ok || entry == nil {
			return Snapshot{}, fmt.Errorf("metadata shard owner map is missing shard %d", shard)
		}
		// The compatibility seam, checked on the client side too: every
		// pre-Phase-4 consumer routes through shard_to_node_id, so a publisher
		// whose two views disagree would send those consumers to a node that is
		// not the primary. Refuse the whole snapshot rather than pick one.
		if entry.GetPrimary() != owner {
			return Snapshot{}, fmt.Errorf(
				"metadata shard %d has primary %q in shard_to_owners but owner %q in shard_to_node_id",
				shard, entry.GetPrimary(), owner)
		}
		if uint32(len(entry.GetReplicas())) > uint32(len(byID)) {
			return Snapshot{}, fmt.Errorf("metadata shard %d lists %d replicas for %d live node(s)",
				shard, len(entry.GetReplicas()), len(byID))
		}

		seen := map[string]bool{owner: true}
		replicas := make([]string, 0, len(entry.GetReplicas()))
		for _, replica := range entry.GetReplicas() {
			if _, ok := byID[replica]; !ok {
				return Snapshot{}, fmt.Errorf("metadata shard %d has unknown replica %q", shard, replica)
			}
			if seen[replica] {
				return Snapshot{}, fmt.Errorf(
					"metadata shard %d lists %q twice; a node cannot hold two copies of one shard",
					shard, replica)
			}
			seen[replica] = true
			replicas = append(replicas, replica)
		}
		if len(replicas) == 0 {
			replicas = nil
		}
		owners[shard] = router.ShardOwners{Primary: owner, Replicas: replicas}
	}

	return Snapshot{
		Generation:        generation,
		ShardCount:        shardCount,
		ShardMap:          shards,
		Nodes:             byID,
		Owners:            owners,
		ReplicationFactor: replicationFactor,
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
