package splitartifact

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type Artifact interface {
	io.Reader
	io.Seeker
	io.Closer
}

type Source interface {
	OpenSplitArtifact(context.Context, Identity) (Artifact, error)
}

type AuthorizeFunc func(rafttransport.PeerIdentity, Identity) bool

type ServiceOptions struct {
	Source           Source
	Authorize        AuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConnections   int
	MaxChunkBytes    uint32
	MaxInflightBytes int64
}

type Service struct {
	source                      Source
	authorize                   AuthorizeFunc
	readDeadline, writeDeadline rafttransport.DeadlineFunc
	slots                       chan *serviceSlot
	maxChunk                    uint32
	maxInflight                 int64
	inflight                    atomic.Int64
	resident                    atomic.Int64
}

type serviceSlot struct{ chunk []byte }

func NewService(options ServiceOptions) (*Service, error) {
	if options.Source == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConnections <= 0 || options.MaxConnections > 4096 ||
		options.MaxChunkBytes < MinChunkBytes || options.MaxChunkBytes > AbsoluteMaxChunkBytes ||
		options.MaxInflightBytes < int64(options.MaxChunkBytes) ||
		options.MaxInflightBytes > int64(options.MaxChunkBytes)*int64(options.MaxConnections) {
		return nil, ErrBound
	}
	service := &Service{source: options.Source, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots: make(chan *serviceSlot, options.MaxConnections), maxChunk: options.MaxChunkBytes,
		maxInflight: options.MaxInflightBytes}
	for range options.MaxConnections {
		service.slots <- new(serviceSlot)
	}
	return service, nil
}

func (service *Service) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficSnapshot {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrProtocol
	}
	var slot *serviceSlot
	select {
	case slot = <-service.slots:
		defer func() { service.slots <- slot }()
	default:
		_ = connection.Close()
		return ErrBound
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := setReadDeadline(connection, service.readDeadline); err != nil {
		return err
	}
	var raw [RequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return err
	}
	request, err := openRequest(raw[:])
	if err != nil {
		return err
	}
	if request.ChunkBytes > service.maxChunk || !service.authorize(connection.PeerIdentity(), request.Identity) {
		return ErrUnauthorized
	}
	charge := int64(request.ChunkBytes)
	for {
		current := service.inflight.Load()
		if current > service.maxInflight-charge {
			return ErrBound
		}
		if service.inflight.CompareAndSwap(current, current+charge) {
			break
		}
	}
	defer service.inflight.Add(-charge)
	workspace := slot.chunk[:0]
	if cap(workspace) < int(request.ChunkBytes) {
		if service.reserveResident(charge) {
			workspace = make([]byte, request.ChunkBytes)
			slot.chunk = workspace[:0]
		} else {
			workspace = make([]byte, request.ChunkBytes)
		}
	} else {
		workspace = workspace[:request.ChunkBytes]
	}
	artifact, err := service.source.OpenSplitArtifact(ctx, request.Identity)
	if err != nil {
		return err
	}
	defer artifact.Close()
	if offset, seekErr := artifact.Seek(int64(request.Offset), io.SeekStart); seekErr != nil || offset != int64(request.Offset) {
		return ErrSource
	}
	offset := request.Offset
	for offset < request.Identity.ArtifactBytes {
		length := min(uint64(request.ChunkBytes), request.Identity.ArtifactBytes-offset)
		chunk := workspace[:int(length)]
		if _, err = io.ReadFull(artifact, chunk); err != nil {
			return err
		}
		if err = writeResponse(connection, service.writeDeadline, responseChunk, offset,
			request.Identity.ArtifactBytes, chunk); err != nil {
			return err
		}
		offset += length
	}
	return writeResponse(connection, service.writeDeadline, responseComplete, offset,
		request.Identity.ArtifactBytes, nil)
}

func (service *Service) reserveResident(bytes int64) bool {
	for {
		current := service.resident.Load()
		if bytes < 0 || current > service.maxInflight-bytes {
			return false
		}
		if service.resident.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

func writeResponse(connection rafttransport.PeerConnection, deadline rafttransport.DeadlineFunc, status byte,
	offset, total uint64, chunk []byte) error {
	when := deadline()
	if when.IsZero() {
		return ErrBound
	}
	if err := connection.SetWriteDeadline(when); err != nil {
		return err
	}
	var raw [ResponseBytes]byte
	copy(raw[:8], responseMagic[:])
	raw[8] = status
	binary.BigEndian.PutUint64(raw[16:24], offset)
	binary.BigEndian.PutUint64(raw[24:32], total)
	binary.BigEndian.PutUint32(raw[32:36], uint32(len(chunk)))
	digest := sha256.Sum256(chunk)
	copy(raw[40:72], digest[:])
	if err := writeFull(connection, raw[:]); err != nil {
		return err
	}
	return writeFull(connection, chunk)
}

func setReadDeadline(connection interface{ SetReadDeadline(time.Time) error }, deadline rafttransport.DeadlineFunc) error {
	when := deadline()
	if when.IsZero() {
		return ErrBound
	}
	return connection.SetReadDeadline(when)
}

func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}
