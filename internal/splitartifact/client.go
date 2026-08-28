package splitartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type StreamOptions struct {
	Opener        rafttransport.SnapshotStreamOpener
	SourceNode    rafttransport.NodeID
	Identity      Identity
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	ChunkBytes    uint32
	MaxReconnects int
	Workspace     []byte
}

// Stream is a bounded retrying io.ReadCloser. A transport chunk is fully read
// and checksummed before any of its bytes become visible to the artifact
// verifier. Reconnect therefore resumes at the first byte not yet validated.
type Stream struct {
	ctx        context.Context
	options    StreamOptions
	connection rafttransport.PeerConnection
	buffer     []byte
	cursor     int
	offset     uint64
	reconnects int
	complete   bool
	closed     bool
}

func OpenStream(ctx context.Context, options StreamOptions) (*Stream, error) {
	if ctx == nil || options.Opener == nil || options.SourceNode == (rafttransport.NodeID{}) ||
		!options.Identity.Valid() || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.ChunkBytes < MinChunkBytes || options.ChunkBytes > AbsoluteMaxChunkBytes ||
		options.MaxReconnects < 0 || options.MaxReconnects > AbsoluteMaxReconnects ||
		cap(options.Workspace) < int(options.ChunkBytes) {
		return nil, ErrBound
	}
	options.Workspace = options.Workspace[:options.ChunkBytes]
	return &Stream{ctx: ctx, options: options}, nil
}

func (stream *Stream) Read(dst []byte) (int, error) {
	if stream == nil || stream.closed {
		return 0, ErrProtocol
	}
	if len(dst) == 0 {
		return 0, nil
	}
	for stream.cursor == len(stream.buffer) {
		stream.buffer, stream.cursor = nil, 0
		if stream.complete {
			return 0, io.EOF
		}
		if err := stream.nextChunk(); err != nil {
			return 0, err
		}
	}
	n := copy(dst, stream.buffer[stream.cursor:])
	stream.cursor += n
	return n, nil
}

func (stream *Stream) Close() error {
	if stream == nil || stream.closed {
		return nil
	}
	stream.closed = true
	if stream.connection != nil {
		return stream.connection.Close()
	}
	return nil
}

func (stream *Stream) nextChunk() error {
	for {
		if stream.connection == nil {
			if err := stream.connect(); err != nil {
				if cause := context.Cause(stream.ctx); cause != nil {
					return cause
				}
				if !IsRetryable(err) || stream.reconnects == stream.options.MaxReconnects {
					return err
				}
				stream.reconnects++
				continue
			}
		}
		chunk, complete, err := stream.readChunk()
		if err == nil {
			stream.buffer = chunk
			stream.complete = complete
			return nil
		}
		_ = stream.connection.Close()
		stream.connection = nil
		if cause := context.Cause(stream.ctx); cause != nil {
			return cause
		}
		if !IsRetryable(err) || stream.reconnects == stream.options.MaxReconnects {
			return err
		}
		stream.reconnects++
	}
}

func (stream *Stream) connect() error {
	connection, err := stream.options.Opener.OpenSnapshot(stream.ctx, stream.options.SourceNode)
	if err != nil {
		return err
	}
	if connection == nil || connection.TrafficClass() != rafttransport.TrafficSnapshot {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrUnauthorized
	}
	request := request{Identity: stream.options.Identity, Offset: stream.offset,
		ChunkBytes: stream.options.ChunkBytes}
	var raw [RequestBytes]byte
	encoded, err := appendRequest(raw[:0], request)
	if err == nil {
		when := stream.options.WriteDeadline()
		if when.IsZero() {
			err = ErrBound
		} else if err = connection.SetWriteDeadline(when); err == nil {
			err = writeFull(connection, encoded)
		}
	}
	if err != nil {
		_ = connection.Close()
		return err
	}
	stream.connection = connection
	return nil
}

func (stream *Stream) readChunk() ([]byte, bool, error) {
	when := stream.options.ReadDeadline()
	if when.IsZero() {
		return nil, false, ErrBound
	}
	if err := stream.connection.SetReadDeadline(when); err != nil {
		return nil, false, err
	}
	var raw [ResponseBytes]byte
	if _, err := io.ReadFull(stream.connection, raw[:]); err != nil {
		return nil, false, err
	}
	if !bytes.Equal(raw[:8], responseMagic[:]) || !allZero(raw[9:16]) ||
		!allZero(raw[36:40]) || binary.BigEndian.Uint64(raw[16:24]) != stream.offset ||
		binary.BigEndian.Uint64(raw[24:32]) != stream.options.Identity.ArtifactBytes {
		return nil, false, ErrChunk
	}
	length := binary.BigEndian.Uint32(raw[32:36])
	if raw[8] == responseComplete {
		empty := sha256.Sum256(nil)
		if length != 0 || stream.offset != stream.options.Identity.ArtifactBytes ||
			!bytes.Equal(raw[40:72], empty[:]) {
			return nil, false, ErrChunk
		}
		return nil, true, nil
	}
	if raw[8] != responseChunk || length == 0 || length > stream.options.ChunkBytes ||
		uint64(length) > stream.options.Identity.ArtifactBytes-stream.offset {
		return nil, false, ErrChunk
	}
	chunk := stream.options.Workspace[:length]
	if _, err := io.ReadFull(stream.connection, chunk); err != nil {
		return nil, false, err
	}
	want := sha256.Sum256(chunk)
	if !bytes.Equal(raw[40:72], want[:]) {
		return nil, false, ErrChunk
	}
	stream.offset += uint64(length)
	return chunk, false, nil
}

var _ io.ReadCloser = (*Stream)(nil)

func IsRetryable(err error) bool {
	return err != nil && !errors.Is(err, ErrProtocol) && !errors.Is(err, ErrUnauthorized) &&
		!errors.Is(err, ErrBound) && !errors.Is(err, ErrChunk)
}
