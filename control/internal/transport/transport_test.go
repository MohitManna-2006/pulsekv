package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	nodev1 "pulsekv/control/gen/node/v1"
)

func TestPutUsesUnaryThroughLimit(t *testing.T) {
	value := make([]byte, UnaryValueLimit)
	client := &fakeNodeClient{
		put: func(_ context.Context, req *nodev1.PutRequest) (*nodev1.PutResponse, error) {
			if string(req.GetKey()) != "key" || !bytes.Equal(req.GetValue(), value) {
				t.Fatalf("unexpected Put request: key=%q value length=%d", req.GetKey(), len(req.GetValue()))
			}
			return &nodev1.PutResponse{Ok: true}, nil
		},
		putChunked: func(context.Context) (grpc.ClientStreamingClient[nodev1.PutChunk, nodev1.PutResponse], error) {
			t.Fatal("PutChunked called for a value at the unary limit")
			return nil, nil
		},
	}

	if err := Put(context.Background(), client, []byte("key"), value); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestPutChunksValuesAboveLimit(t *testing.T) {
	value := make([]byte, UnaryValueLimit+17)
	for i := range value {
		value[i] = byte(i)
	}
	stream := &fakePutStream{}
	client := &fakeNodeClient{
		put: func(context.Context, *nodev1.PutRequest) (*nodev1.PutResponse, error) {
			t.Fatal("unary Put called for a value above the limit")
			return nil, nil
		},
		putChunked: func(context.Context) (grpc.ClientStreamingClient[nodev1.PutChunk, nodev1.PutResponse], error) {
			return stream, nil
		},
	}

	if err := Put(context.Background(), client, []byte("large-key"), value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	wantChunks := (len(value) + ChunkSize - 1) / ChunkSize
	if len(stream.sent) != wantChunks {
		t.Fatalf("sent %d chunks, want %d", len(stream.sent), wantChunks)
	}
	var joined []byte
	for i, chunk := range stream.sent {
		if chunk.GetChunkIndex() != uint32(i) {
			t.Errorf("chunk %d index=%d", i, chunk.GetChunkIndex())
		}
		if chunk.GetTotalChunks() != uint32(wantChunks) || chunk.GetTotalLength() != uint64(len(value)) {
			t.Errorf("chunk %d metadata=(%d,%d), want (%d,%d)", i, chunk.GetTotalChunks(), chunk.GetTotalLength(), wantChunks, len(value))
		}
		if i == 0 && string(chunk.GetKey()) != "large-key" {
			t.Errorf("first chunk key=%q", chunk.GetKey())
		}
		if i > 0 && len(chunk.GetKey()) != 0 {
			t.Errorf("chunk %d repeats key %q", i, chunk.GetKey())
		}
		if len(chunk.GetData()) > ChunkSize {
			t.Errorf("chunk %d has %d data bytes", i, len(chunk.GetData()))
		}
		joined = append(joined, chunk.GetData()...)
	}
	if !bytes.Equal(joined, value) {
		t.Fatal("chunk data did not reassemble to the input")
	}
	if !stream.closed {
		t.Fatal("CloseAndRecv was not called")
	}
}

func TestGetAutoUnaryHitAndMiss(t *testing.T) {
	tests := []struct {
		name      string
		response  *nodev1.GetResponse
		wantValue []byte
		wantFound bool
	}{
		{name: "hit", response: &nodev1.GetResponse{Found: true, Value: []byte("value")}, wantValue: []byte("value"), wantFound: true},
		{name: "miss", response: &nodev1.GetResponse{}, wantFound: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeNodeClient{
				get: func(_ context.Context, req *nodev1.GetRequest) (*nodev1.GetResponse, error) {
					if string(req.GetKey()) != "key" {
						t.Fatalf("Get key=%q", req.GetKey())
					}
					return tt.response, nil
				},
				getChunked: func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
					t.Fatal("GetChunked called after a successful unary Get")
					return nil, nil
				},
			}

			got, found, err := Get(context.Background(), client, []byte("key"))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if found != tt.wantFound || !bytes.Equal(got, tt.wantValue) {
				t.Fatalf("Get = (%q, %v), want (%q, %v)", got, found, tt.wantValue, tt.wantFound)
			}
		})
	}
}

func TestGetAutoFallsBackOnlyForLargeValueSignal(t *testing.T) {
	t.Run("failed precondition", func(t *testing.T) {
		client := &fakeNodeClient{
			get: func(context.Context, *nodev1.GetRequest) (*nodev1.GetResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "use GetChunked")
			},
			getChunked: func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
				return &fakeGetStream{chunks: []*nodev1.GetChunk{
					{ChunkIndex: 0, TotalChunks: 2, TotalLength: 5, Data: []byte("hel")},
					{ChunkIndex: 1, TotalChunks: 2, TotalLength: 5, Data: []byte("lo")},
				}}, nil
			},
		}

		got, found, err := Get(context.Background(), client, []byte("key"))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !found || string(got) != "hello" {
			t.Fatalf("Get = (%q, %v), want (hello, true)", got, found)
		}
	})

	t.Run("other status", func(t *testing.T) {
		wantErr := status.Error(codes.Unavailable, "offline")
		client := &fakeNodeClient{
			get: func(context.Context, *nodev1.GetRequest) (*nodev1.GetResponse, error) {
				return nil, wantErr
			},
			getChunked: func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
				t.Fatal("GetChunked called for a non-FailedPrecondition error")
				return nil, nil
			},
		}

		_, _, err := Get(context.Background(), client, []byte("key"))
		if !errors.Is(err, wantErr) {
			t.Fatalf("Get error = %v, want %v", err, wantErr)
		}
	})
}

func TestGetChunkedDistinguishesMissFromEmptyValue(t *testing.T) {
	tests := []struct {
		name      string
		chunks    []*nodev1.GetChunk
		wantFound bool
	}{
		{name: "empty stream miss", wantFound: false},
		{name: "zero length value", chunks: []*nodev1.GetChunk{{ChunkIndex: 0, TotalChunks: 1, TotalLength: 0}}, wantFound: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeNodeClient{
				get: func(context.Context, *nodev1.GetRequest) (*nodev1.GetResponse, error) {
					t.Fatal("unary Get called in ReadChunked mode")
					return nil, nil
				},
				getChunked: func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
					return &fakeGetStream{chunks: tt.chunks}, nil
				},
			}

			got, found, err := GetWithMode(context.Background(), client, []byte("key"), ReadChunked)
			if err != nil {
				t.Fatalf("GetWithMode: %v", err)
			}
			if found != tt.wantFound || len(got) != 0 {
				t.Fatalf("GetWithMode = (%q, %v), want empty value and found=%v", got, found, tt.wantFound)
			}
		})
	}
}

func TestGetChunkedRejectsMalformedStreams(t *testing.T) {
	tests := []struct {
		name   string
		chunks []*nodev1.GetChunk
	}{
		{name: "zero total chunks", chunks: []*nodev1.GetChunk{{ChunkIndex: 0, TotalLength: 1, Data: []byte("x")}}},
		{name: "wrong first index", chunks: []*nodev1.GetChunk{{ChunkIndex: 1, TotalChunks: 1, TotalLength: 1, Data: []byte("x")}}},
		{name: "changed metadata", chunks: []*nodev1.GetChunk{
			{ChunkIndex: 0, TotalChunks: 2, TotalLength: 2, Data: []byte("x")},
			{ChunkIndex: 1, TotalChunks: 3, TotalLength: 2, Data: []byte("y")},
		}},
		{name: "short chunk count", chunks: []*nodev1.GetChunk{{ChunkIndex: 0, TotalChunks: 2, TotalLength: 1, Data: []byte("x")}}},
		{name: "extra chunk", chunks: []*nodev1.GetChunk{
			{ChunkIndex: 0, TotalChunks: 1, TotalLength: 2, Data: []byte("x")},
			{ChunkIndex: 1, TotalChunks: 1, TotalLength: 2, Data: []byte("y")},
		}},
		{name: "data exceeds length", chunks: []*nodev1.GetChunk{{ChunkIndex: 0, TotalChunks: 1, TotalLength: 1, Data: []byte("xx")}}},
		{name: "short data", chunks: []*nodev1.GetChunk{{ChunkIndex: 0, TotalChunks: 1, TotalLength: 2, Data: []byte("x")}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeNodeClient{
				getChunked: func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
					return &fakeGetStream{chunks: tt.chunks}, nil
				},
			}
			if _, _, err := GetWithMode(context.Background(), client, []byte("key"), ReadChunked); err == nil {
				t.Fatal("GetWithMode accepted malformed stream")
			}
		})
	}
}

type fakeNodeClient struct {
	nodev1.NodeServiceClient
	get        func(context.Context, *nodev1.GetRequest) (*nodev1.GetResponse, error)
	put        func(context.Context, *nodev1.PutRequest) (*nodev1.PutResponse, error)
	putChunked func(context.Context) (grpc.ClientStreamingClient[nodev1.PutChunk, nodev1.PutResponse], error)
	getChunked func(context.Context, *nodev1.GetRequest) (grpc.ServerStreamingClient[nodev1.GetChunk], error)
}

func (f *fakeNodeClient) Get(ctx context.Context, req *nodev1.GetRequest, _ ...grpc.CallOption) (*nodev1.GetResponse, error) {
	return f.get(ctx, req)
}

func (f *fakeNodeClient) Put(ctx context.Context, req *nodev1.PutRequest, _ ...grpc.CallOption) (*nodev1.PutResponse, error) {
	return f.put(ctx, req)
}

func (f *fakeNodeClient) PutChunked(ctx context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutChunk, nodev1.PutResponse], error) {
	return f.putChunked(ctx)
}

func (f *fakeNodeClient) GetChunked(ctx context.Context, req *nodev1.GetRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.GetChunk], error) {
	return f.getChunked(ctx, req)
}

type fakeClientStream struct{}

func (fakeClientStream) Header() (metadata.MD, error) { return nil, nil }
func (fakeClientStream) Trailer() metadata.MD         { return nil }
func (fakeClientStream) CloseSend() error             { return nil }
func (fakeClientStream) Context() context.Context     { return context.Background() }
func (fakeClientStream) SendMsg(any) error            { return nil }
func (fakeClientStream) RecvMsg(any) error            { return io.EOF }

type fakePutStream struct {
	fakeClientStream
	sent   []*nodev1.PutChunk
	closed bool
}

func (f *fakePutStream) Send(chunk *nodev1.PutChunk) error {
	copyChunk := &nodev1.PutChunk{
		Key:         bytes.Clone(chunk.GetKey()),
		ChunkIndex:  chunk.GetChunkIndex(),
		TotalChunks: chunk.GetTotalChunks(),
		TotalLength: chunk.GetTotalLength(),
		Data:        bytes.Clone(chunk.GetData()),
	}
	f.sent = append(f.sent, copyChunk)
	return nil
}

func (f *fakePutStream) CloseAndRecv() (*nodev1.PutResponse, error) {
	f.closed = true
	return &nodev1.PutResponse{Ok: true}, nil
}

type fakeGetStream struct {
	fakeClientStream
	chunks []*nodev1.GetChunk
	next   int
}

func (f *fakeGetStream) Recv() (*nodev1.GetChunk, error) {
	if f.next == len(f.chunks) {
		return nil, io.EOF
	}
	chunk := f.chunks[f.next]
	f.next++
	return chunk, nil
}
