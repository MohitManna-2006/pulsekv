// Package transport implements the value-carrying NodeService wire protocol.
//
// Put selects the unary or client-streaming RPC from the value size. Get uses
// the unary RPC first and follows the server's FailedPrecondition signal to the
// server-streaming RPC. Callers that already know the stored size can use
// GetWithMode to avoid that probe.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "pulsekv/control/gen/node/v1"
)

const (
	// UnaryValueLimit is the largest value accepted by the unary NodeService
	// Put and Get RPCs.
	UnaryValueLimit = int(nodev1.UnaryLimit_UNARY_VALUE_LIMIT_BYTES)

	// ChunkSize is the payload size used for each PutChunked frame.
	ChunkSize = 1024 * 1024
)

// ReadMode controls which NodeService read path GetWithMode uses.
type ReadMode uint8

const (
	// ReadAuto tries Get first and retries with GetChunked only when the server
	// reports that the stored value exceeds the unary wire limit.
	ReadAuto ReadMode = iota
	// ReadUnary uses Get without a chunked fallback.
	ReadUnary
	// ReadChunked uses GetChunked directly.
	ReadChunked
)

// ReadModeForSize returns the direct read mode for a known stored value size.
func ReadModeForSize(size int) ReadMode {
	if size > UnaryValueLimit {
		return ReadChunked
	}
	return ReadUnary
}

// ErrChunkedAcksUnsupported is returned when a caller asks for replica acks on
// a value too large for the unary path.
//
// PutChunk carries no require_replica_acks field: a chunked write is a
// multi-megabyte value, where blocking the client on replica fan-out is the
// wrong trade in every case Phase 4 contemplates. Refusing loudly is better
// than accepting the call and quietly downgrading it to fire-and-forget, which
// would hand the caller a durability guarantee that was never provided.
var ErrChunkedAcksUnsupported = errors.New(
	"transport: require_replica_acks is not supported for values above the unary limit; " +
		"PutChunked always replicates asynchronously")

// Put writes value with Put when it fits under the unary wire limit and with
// PutChunked otherwise. Replication, if the receiving node is a primary, is
// asynchronous — see PutWithAck for the blocking variant.
func Put(ctx context.Context, client nodev1.NodeServiceClient, key, value []byte, opts ...grpc.CallOption) error {
	_, err := PutWithAck(ctx, client, key, value, 0, opts...)
	return err
}

// PutWithAck writes value and, when acks > 0 and the receiving node is the
// primary for the key's shard, blocks until that many replicas have stored it.
// It returns how many replicas acked before the node responded, which is always
// 0 for acks == 0 because nothing was waited for.
//
// acks greater than the primary's live replica count fails with
// INVALID_ARGUMENT; an ack shortfall or timeout fails with DEADLINE_EXCEEDED.
// Both mean "less replicated than you asked for" rather than "not written":
// the primary committed locally before it forwarded anything, and does not roll
// that back.
func PutWithAck(ctx context.Context, client nodev1.NodeServiceClient, key, value []byte,
	acks uint32, opts ...grpc.CallOption) (replicasAcked uint32, err error) {
	if len(value) <= UnaryValueLimit {
		resp, err := client.Put(ctx, &nodev1.PutRequest{
			Key:                key,
			Value:              value,
			RequireReplicaAcks: acks,
		}, opts...)
		if err != nil {
			return 0, err
		}
		return resp.GetReplicasAcked(), nil
	}
	if acks > 0 {
		return 0, fmt.Errorf("%w (value is %d bytes, limit is %d)",
			ErrChunkedAcksUnsupported, len(value), UnaryValueLimit)
	}

	// This form avoids overflowing len(value)+ChunkSize-1 for an extremely
	// large input on a 64-bit client.
	totalChunks := 1 + (len(value)-1)/ChunkSize
	if uint64(totalChunks) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("transport: value requires %d chunks, protocol limit is %d", totalChunks, uint64(^uint32(0)))
	}
	stream, err := client.PutChunked(ctx, opts...)
	if err != nil {
		return 0, err
	}

	for i := 0; i < totalChunks; i++ {
		lo := i * ChunkSize
		hi := min(lo+ChunkSize, len(value))
		chunk := &nodev1.PutChunk{
			ChunkIndex:  uint32(i),
			TotalChunks: uint32(totalChunks),
			TotalLength: uint64(len(value)),
			Data:        value[lo:hi],
		}
		if i == 0 {
			chunk.Key = key
		}
		if err := stream.Send(chunk); err != nil {
			return 0, err
		}
	}

	// Always 0: PutChunked replicates in the background, so there is nothing to
	// report having waited for.
	if _, err := stream.CloseAndRecv(); err != nil {
		return 0, err
	}
	return 0, nil
}

// Get reads key using the automatic unary-then-chunked strategy. A miss is
// returned as found=false with a nil error.
func Get(ctx context.Context, client nodev1.NodeServiceClient, key []byte, opts ...grpc.CallOption) (value []byte, found bool, err error) {
	return GetWithMode(ctx, client, key, ReadAuto, opts...)
}

// GetWithMode reads key using mode. ReadAuto falls back to GetChunked only for
// FailedPrecondition, the status NodeService.Get uses for a value above the
// unary limit. An empty GetChunked stream is a miss; a one-frame, zero-length
// value is a hit.
func GetWithMode(ctx context.Context, client nodev1.NodeServiceClient, key []byte, mode ReadMode, opts ...grpc.CallOption) (value []byte, found bool, err error) {
	switch mode {
	case ReadAuto:
		value, found, err = getUnary(ctx, client, key, opts...)
		if status.Code(err) != codes.FailedPrecondition {
			return value, found, err
		}
		return getChunked(ctx, client, key, opts...)
	case ReadUnary:
		return getUnary(ctx, client, key, opts...)
	case ReadChunked:
		return getChunked(ctx, client, key, opts...)
	default:
		return nil, false, fmt.Errorf("transport: invalid read mode %d", mode)
	}
}

func getUnary(ctx context.Context, client nodev1.NodeServiceClient, key []byte, opts ...grpc.CallOption) ([]byte, bool, error) {
	resp, err := client.Get(ctx, &nodev1.GetRequest{Key: key}, opts...)
	if err != nil {
		return nil, false, err
	}
	if resp == nil {
		return nil, false, fmt.Errorf("transport: Get returned a nil response")
	}
	if !resp.GetFound() {
		return nil, false, nil
	}
	return resp.GetValue(), true, nil
}

func getChunked(ctx context.Context, client nodev1.NodeServiceClient, key []byte, opts ...grpc.CallOption) ([]byte, bool, error) {
	stream, err := client.GetChunked(ctx, &nodev1.GetRequest{Key: key}, opts...)
	if err != nil {
		return nil, false, err
	}

	var (
		out            []byte
		totalChunks    uint32
		totalLength    uint64
		receivedChunks uint64
		receivedLength uint64
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if receivedChunks == 0 {
				return nil, false, nil
			}
			if receivedChunks != uint64(totalChunks) {
				return nil, false, fmt.Errorf("transport: GetChunked ended after %d of %d chunks", receivedChunks, totalChunks)
			}
			if receivedLength != totalLength {
				return nil, false, fmt.Errorf("transport: GetChunked returned %d of %d bytes", receivedLength, totalLength)
			}
			return out, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if chunk == nil {
			return nil, false, fmt.Errorf("transport: GetChunked returned a nil chunk")
		}

		if receivedChunks == 0 {
			totalChunks = chunk.GetTotalChunks()
			totalLength = chunk.GetTotalLength()
			if totalChunks == 0 {
				return nil, false, fmt.Errorf("transport: GetChunked chunk 0 has total_chunks=0")
			}
		} else if chunk.GetTotalChunks() != totalChunks || chunk.GetTotalLength() != totalLength {
			return nil, false, fmt.Errorf("transport: GetChunked chunk %d changed stream metadata", receivedChunks)
		}

		if receivedChunks >= uint64(totalChunks) {
			return nil, false, fmt.Errorf("transport: GetChunked returned more than %d chunks", totalChunks)
		}
		if chunk.GetChunkIndex() != uint32(receivedChunks) {
			return nil, false, fmt.Errorf("transport: GetChunked got chunk_index=%d, want %d", chunk.GetChunkIndex(), receivedChunks)
		}
		dataLength := uint64(len(chunk.GetData()))
		if dataLength > totalLength-receivedLength {
			return nil, false, fmt.Errorf("transport: GetChunked data exceeds declared total_length=%d", totalLength)
		}

		out = append(out, chunk.GetData()...)
		receivedChunks++
		receivedLength += dataLength
	}
}
