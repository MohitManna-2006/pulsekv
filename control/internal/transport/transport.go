// Package transport implements the value-carrying NodeService wire protocol.
//
// Put selects the unary or client-streaming RPC from the value size. Get uses
// the unary RPC first and follows the server's FailedPrecondition signal to the
// server-streaming RPC. Callers that already know the stored size can use
// GetWithMode to avoid that probe.
package transport

import (
	"context"
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

// Put writes value with Put when it fits under the unary wire limit and with
// PutChunked otherwise.
func Put(ctx context.Context, client nodev1.NodeServiceClient, key, value []byte, opts ...grpc.CallOption) error {
	if len(value) <= UnaryValueLimit {
		_, err := client.Put(ctx, &nodev1.PutRequest{Key: key, Value: value}, opts...)
		return err
	}

	// This form avoids overflowing len(value)+ChunkSize-1 for an extremely
	// large input on a 64-bit client.
	totalChunks := 1 + (len(value)-1)/ChunkSize
	if uint64(totalChunks) > uint64(^uint32(0)) {
		return fmt.Errorf("transport: value requires %d chunks, protocol limit is %d", totalChunks, uint64(^uint32(0)))
	}
	stream, err := client.PutChunked(ctx, opts...)
	if err != nil {
		return err
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
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
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
