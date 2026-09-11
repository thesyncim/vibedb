package shardservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frameEncoderTestWriter struct {
	bytes.Buffer
	short    bool
	writeErr error
	writes   int
}

func (w *frameEncoderTestWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short {
		n := len(p) - 1
		if n < 0 {
			n = 0
		}
		if n != 0 {
			_, _ = w.Buffer.Write(p[:n])
		}
		return n, nil
	}
	return w.Buffer.Write(p)
}

func testReplicatedQueryRequest(query []byte) *ReplicatedRequest {
	return &ReplicatedRequest{
		Operation: ReplicatedQueryLeader,
		Authority: serviceauthz.Authority{
			Node:       rafttransport.NodeID{1},
			Generation: 1,
		},
		Capability:    serviceauthz.CapabilityDataRead,
		Fence:         testReplicatedFence(),
		MaxValueBytes: MaxReplicatedSQLResultBytes,
		Query:         query,
	}
}

func testReplicatedQueryRequestForFrameSize(t *testing.T, frameSize int) *ReplicatedRequest {
	t.Helper()
	request := testReplicatedQueryRequest([]byte{'q'})
	base, err := ReplicatedRequestFrameBytes(request)
	if err != nil {
		t.Fatal(err)
	}
	if frameSize < base {
		t.Fatalf("frame size %d is smaller than query base %d", frameSize, base)
	}
	request.Query = bytes.Repeat([]byte{'q'}, len(request.Query)+frameSize-base)
	actual, err := ReplicatedRequestFrameBytes(request)
	if err != nil {
		t.Fatal(err)
	}
	if actual != frameSize {
		t.Fatalf("query frame size = %d, want %d", actual, frameSize)
	}
	return request
}

func assertFrameEncoderArenaCleared(t *testing.T, encoder *FrameEncoder) {
	t.Helper()
	if len(encoder.arena) == 0 {
		t.Fatal("encoder did not retain an arena")
	}
	for index, value := range encoder.arena {
		if value != 0 {
			t.Fatalf("encoder arena byte %d retained %#x", index, value)
		}
	}
}

func TestFrameEncoderBorrowedCanonicalBoundaries(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		frameSize  int
		writeCount int
	}{
		{name: "small", frameSize: 512, writeCount: 1},
		{name: "exact_coalesce_limit", frameSize: replicatedBorrowedCoalesceBytes, writeCount: 1},
		{name: "large_scatter", frameSize: replicatedBorrowedCoalesceBytes + 1, writeCount: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := testReplicatedQueryRequestForFrameSize(t, test.frameSize)
			var want bytes.Buffer
			if err := EncodeReplicatedRequest(&want, request); err != nil {
				t.Fatal(err)
			}
			var got frameEncoderTestWriter
			var encoder FrameEncoder
			if err := encoder.EncodeReplicatedRequestBorrowed(&got, request); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want.Bytes()) {
				t.Fatalf("encoded frame differs from canonical output (got %d bytes, want %d)", got.Len(), want.Len())
			}
			if got.writes != test.writeCount {
				t.Fatalf("writes = %d, want %d", got.writes, test.writeCount)
			}
			// A query carries a payload, so it borrows a pooled scratch buffer
			// for the frame prefix instead of the connection-owned arena;
			// releaseReplicatedBorrowedScratch (tested separately) scrubs that
			// scratch on release, and the arena must stay untouched here.
			if len(encoder.arena) != 0 {
				t.Fatal("payload-bearing request unexpectedly retained an arena")
			}
		})
	}
}

func TestFrameEncoderBorrowedWriteFailuresScrubArena(t *testing.T) {
	t.Parallel()
	request := testReplicatedQueryRequestForFrameSize(t, 512)
	failure := errors.New("test writer failure")
	for _, test := range []struct {
		name    string
		writer  func() *frameEncoderTestWriter
		wantErr error
	}{
		{name: "short_write", writer: func() *frameEncoderTestWriter {
			return &frameEncoderTestWriter{short: true}
		}, wantErr: io.ErrShortWrite},
		{name: "write_error", writer: func() *frameEncoderTestWriter {
			return &frameEncoderTestWriter{writeErr: failure}
		}, wantErr: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			var encoder FrameEncoder
			writer := test.writer()
			err := encoder.EncodeReplicatedRequestBorrowed(writer, request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("encode error = %v, want %v", err, test.wantErr)
			}
			// See TestFrameEncoderBorrowedCanonicalBoundaries: a payload-bearing
			// query never touches the connection-owned arena.
			if len(encoder.arena) != 0 {
				t.Fatal("payload-bearing request unexpectedly retained an arena")
			}
		})
	}
}

func TestFrameEncoderRoundTripReplicatedUsesOwnedArena(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	serverDone := make(chan error, 1)
	go func() {
		request, err := DecodeReplicatedRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if request.Operation != ReplicatedProbe {
			serverDone <- errors.New("server received unexpected probe request")
			return
		}
		state := replicatedWireState(testReplicatedServingState())
		serverDone <- EncodeReplicatedResponse(server, &ReplicatedResponse{
			Kind:     ReplicatedHandshake,
			HasState: true,
			State:    state,
		})
	}()

	// A probe carries no payload, so it is the operation that actually
	// exercises the connection-owned arena path this test is named for;
	// payload-bearing operations borrow a pooled scratch buffer instead (see
	// TestFrameEncoderBorrowedCanonicalBoundaries). A probe fence is loose
	// (unlike other operations' exact fence), so only Group and
	// AllocationGeneration may be set.
	probeFence := testReplicatedFence()
	probeFence.MemberID = 0
	probeFence.StoreID = [16]byte{}
	probeFence.NodeIncarnation = 0
	probeFence.Term = 0
	probeFence.Command = raftservice.CommandFence{}
	request := &ReplicatedRequest{
		Operation: ReplicatedProbe,
		Authority: serviceauthz.Authority{
			Node:       rafttransport.NodeID{1},
			Generation: 1,
		},
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      probeFence,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var encoder FrameEncoder
	response, err := encoder.RoundTripReplicated(ctx, client, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Kind != ReplicatedHandshake || !response.HasState {
		t.Fatalf("response = %+v", response)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	assertFrameEncoderArenaCleared(t, &encoder)
}

func TestReleaseReplicatedBorrowedScratchScrubsBytes(t *testing.T) {
	scratch := &replicatedBorrowedScratch{}
	for index := range scratch.bytes {
		scratch.bytes[index] = 0xff
	}
	const used = 512
	releaseReplicatedBorrowedScratch(scratch, used)
	// releaseReplicatedBorrowedScratch just put this deliberately
	// half-poisoned object into the shared, process-wide
	// replicatedBorrowedScratchPool. A real caller's used always covers
	// everything it wrote, so bytes beyond it are already zero from a prior
	// release; this test manufactures a beyond-used region that would never
	// arise in production specifically to check clearing stops at used. Drain
	// that exact object back out immediately so no other test - or a real
	// EncodeReplicatedRequestBorrowed call sharing this same pool - can Get()
	// a still-poisoned scratch and fail to reproduce their own frame's data.
	defer func() {
		if drained, ok := replicatedBorrowedScratchPool.Get().(*replicatedBorrowedScratch); ok && drained == scratch {
			return
		}
		t.Fatal("could not drain the poisoned scratch back out of the shared pool")
	}()
	for index, value := range scratch.bytes[:used] {
		if value != 0 {
			t.Fatalf("scratch byte %d retained %#x", index, value)
		}
	}
	for index, value := range scratch.bytes[used:] {
		if value != 0xff {
			t.Fatalf("scratch byte %d beyond used length was unexpectedly scrubbed to %#x", used+index, value)
		}
	}
}
