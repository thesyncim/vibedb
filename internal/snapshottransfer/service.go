package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	requestBytes          = 8 + DescriptorBytes + 8
	responseBytes         = 72
	responseChunk    byte = 1
	responseComplete byte = 2
)

var (
	requestMagic  = [8]byte{'V', 'B', 'S', 'R', 'E', 'Q', 0, 0}
	responseMagic = [8]byte{'V', 'B', 'S', 'R', 'E', 'S', 0, 0}
)

// RequestDiscriminator identifies replica snapshot data streams on the shared
// mutually authenticated snapshot listener.
func RequestDiscriminator() [8]byte { return requestMagic }

// AuthorizeFunc compares the descriptor against the caller's current durable
// store/incarnation/schema/replica-set and snapshot lineage fence.
type AuthorizeFunc func(Descriptor) bool

type ServiceOptions struct {
	Repository       *Repository
	Registry         *rafttransport.StaticRegistry
	Authorize        AuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConnections   int
	MaxChunkBytes    uint32
	MaxInflightBytes int64
}

type Stats struct {
	Connections      int64
	InflightBytes    int64
	ResidentBytes    int64
	ResidentCapacity int64
	Chunks           uint64
	Bytes            uint64
}

type Service struct {
	repository    *Repository
	registry      *rafttransport.StaticRegistry
	authorize     AuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan *serviceSlot
	maxChunk      uint32
	maxInflight   int64
	inflight      atomic.Int64
	resident      atomic.Int64
	chunks        atomic.Uint64
	bytes         atomic.Uint64
}

type serviceSlot struct{ chunk []byte }

func NewService(o ServiceOptions) (*Service, error) {
	if o.Repository == nil || o.Registry == nil || o.Authorize == nil ||
		o.ReadDeadline == nil || o.WriteDeadline == nil || o.MaxConnections <= 0 ||
		o.MaxConnections > 4096 || o.MaxChunkBytes < MinChunkBytes ||
		o.MaxChunkBytes > AbsoluteMaxChunkBytes || o.MaxInflightBytes < int64(o.MaxChunkBytes) ||
		o.MaxInflightBytes > int64(AbsoluteMaxChunkBytes)*int64(o.MaxConnections) {
		return nil, ErrBound
	}
	service := &Service{repository: o.Repository, registry: o.Registry, authorize: o.Authorize,
		readDeadline: o.ReadDeadline, writeDeadline: o.WriteDeadline,
		slots: make(chan *serviceSlot, o.MaxConnections), maxChunk: o.MaxChunkBytes,
		maxInflight: o.MaxInflightBytes}
	for range o.MaxConnections {
		service.slots <- new(serviceSlot)
	}
	return service, nil
}

func (s *Service) ServeTLS(ctx context.Context, raw net.Conn, peerTLS *rafttransport.PeerTLS, handshake rafttransport.DeadlineFunc) error {
	if s == nil || ctx == nil || raw == nil || peerTLS == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return ErrRepository
	}
	conn, err := peerTLS.Server(ctx, raw, rafttransport.TrafficSnapshot, handshake)
	if err != nil {
		return err
	}
	return s.Serve(ctx, conn)
}

// Serve handles exactly one bounded chunk request on one authenticated stream.
func (s *Service) Serve(ctx context.Context, conn rafttransport.PeerConnection) error {
	if s == nil || ctx == nil || conn == nil {
		if conn != nil {
			_ = conn.Close()
		}
		return ErrRepository
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var request [requestBytes]byte
	if deadline := s.readDeadline(); deadline.IsZero() {
		_ = conn.Close()
		return ErrBound
	} else if err := conn.SetReadDeadline(deadline); err != nil {
		_ = conn.Close()
		return err
	}
	if _, err := io.ReadFull(conn, request[:]); err != nil {
		_ = conn.Close()
		return err
	}
	return s.serveRequest(ctx, conn, request)
}

func (s *Service) serveRequest(
	ctx context.Context, conn rafttransport.PeerConnection, request [requestBytes]byte,
) error {
	if s == nil || ctx == nil || conn == nil {
		if conn != nil {
			_ = conn.Close()
		}
		return ErrRepository
	}
	var slot *serviceSlot
	select {
	case slot = <-s.slots:
		defer func() { s.slots <- slot }()
	default:
		_ = conn.Close()
		return ErrBound
	}
	defer conn.Close()
	identity := conn.PeerIdentity()
	if conn.TrafficClass() != rafttransport.TrafficSnapshot || identity.TrustDomain != s.registry.TrustDomain() {
		return ErrStaleFence
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if !bytes.Equal(request[:8], requestMagic[:]) {
		return ErrDescriptor
	}
	d, err := OpenDescriptor(request[8 : 8+DescriptorBytes])
	if err != nil {
		return err
	}
	offset := binary.BigEndian.Uint64(request[8+DescriptorBytes:])
	if d.ChunkBytes > s.maxChunk {
		return ErrBound
	}
	member, err := s.registry.Member(d.Group, identity.Node)
	if err != nil || member != d.TargetMember {
		return ErrStaleFence
	}
	local, err := s.registry.LocalMember(d.Group)
	if err != nil || local != d.SourceMember || !s.authorize(d) {
		return ErrStaleFence
	}
	charge := int64(d.ChunkBytes)
	for {
		current := s.inflight.Load()
		if current > s.maxInflight-charge {
			return ErrBound
		}
		if s.inflight.CompareAndSwap(current, current+charge) {
			break
		}
	}
	defer s.inflight.Add(-charge)
	workspace := slot.chunk[:0]
	retain := false
	if cap(workspace) < int(d.ChunkBytes) {
		delta := int64(d.ChunkBytes) - int64(cap(workspace))
		if s.reserveResident(delta) {
			workspace = make([]byte, 0, d.ChunkBytes)
			slot.chunk = workspace
			retain = true
		} else {
			workspace = make([]byte, 0, d.ChunkBytes)
		}
	}
	chunk, complete, err := s.repository.ReadChunk(d, offset, workspace)
	if retain {
		slot.chunk = chunk[:0]
	}
	if err != nil {
		return err
	}
	var response [responseBytes]byte
	copy(response[:8], responseMagic[:])
	if complete && len(chunk) == 0 {
		response[8] = responseComplete
	} else {
		response[8] = responseChunk
	}
	binary.BigEndian.PutUint64(response[16:24], offset)
	binary.BigEndian.PutUint64(response[24:32], d.ArtifactBytes)
	binary.BigEndian.PutUint32(response[32:36], uint32(len(chunk)))
	digest := sha256.Sum256(chunk)
	copy(response[40:72], digest[:])
	if deadline := s.writeDeadline(); deadline.IsZero() {
		return ErrBound
	} else if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err = writeFull(conn, response[:]); err == nil {
		err = writeFull(conn, chunk)
	}
	if err != nil {
		return err
	}
	s.chunks.Add(1)
	s.bytes.Add(uint64(len(chunk)))
	return nil
}

func (s *Service) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{Connections: int64(cap(s.slots) - len(s.slots)),
		InflightBytes: s.inflight.Load(), ResidentBytes: s.resident.Load(),
		ResidentCapacity: s.maxInflight,
		Chunks:           s.chunks.Load(), Bytes: s.bytes.Load()}
}

func (s *Service) reserveResident(bytes int64) bool {
	for {
		current := s.resident.Load()
		if bytes < 0 || current > s.maxInflight-bytes {
			return false
		}
		if s.resident.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

type Receiver struct {
	Repository    *Repository
	Opener        rafttransport.SnapshotStreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	Workspace     []byte
	chunks        atomic.Uint64
	bytes         atomic.Uint64
}

// Receive resumes until the exact descriptor is atomically published locally.
func (r *Receiver) Receive(ctx context.Context, source rafttransport.NodeID, d Descriptor) error {
	if r == nil || ctx == nil || r.Repository == nil || r.Opener == nil || r.ReadDeadline == nil || r.WriteDeadline == nil ||
		cap(r.Workspace) < int(d.ChunkBytes) || !d.Valid() {
		return ErrBound
	}
	for {
		offset, complete, err := r.Repository.Offset(d)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		conn, err := r.Opener.OpenSnapshot(ctx, source)
		if err != nil {
			return err
		}
		err = r.receiveOne(ctx, conn, d, offset)
		_ = conn.Close()
		if err != nil {
			return err
		}
	}
}

func (r *Receiver) receiveOne(ctx context.Context, conn rafttransport.PeerConnection, d Descriptor, offset uint64) error {
	if conn == nil || conn.TrafficClass() != rafttransport.TrafficSnapshot {
		return ErrStaleFence
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var request [requestBytes]byte
	copy(request[:8], requestMagic[:])
	if _, err := AppendDescriptor(request[8:8:8+DescriptorBytes], d); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(request[8+DescriptorBytes:], offset)
	if deadline := r.WriteDeadline(); deadline.IsZero() {
		return ErrBound
	} else if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := writeFull(conn, request[:]); err != nil {
		return err
	}
	if deadline := r.ReadDeadline(); deadline.IsZero() {
		return ErrBound
	} else if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	var response [responseBytes]byte
	if _, err := io.ReadFull(conn, response[:]); err != nil {
		return err
	}
	if !bytes.Equal(response[:8], responseMagic[:]) || response[9] != 0 || response[10] != 0 || response[11] != 0 ||
		response[12] != 0 || response[13] != 0 || response[14] != 0 || response[15] != 0 ||
		binary.BigEndian.Uint32(response[36:40]) != 0 || binary.BigEndian.Uint64(response[16:24]) != offset ||
		binary.BigEndian.Uint64(response[24:32]) != d.ArtifactBytes {
		return ErrChunk
	}
	length := binary.BigEndian.Uint32(response[32:36])
	if length > d.ChunkBytes {
		return ErrChunk
	}
	if response[8] == responseComplete {
		emptyDigest := sha256.Sum256(nil)
		if length != 0 || offset != d.ArtifactBytes ||
			!bytes.Equal(response[40:72], emptyDigest[:]) {
			return ErrChunk
		}
		return nil
	}
	if response[8] != responseChunk || length == 0 {
		return ErrChunk
	}
	chunk := r.Workspace[:length]
	if _, err := io.ReadFull(conn, chunk); err != nil {
		return err
	}
	var digest [sha256.Size]byte
	copy(digest[:], response[40:72])
	if sha256.Sum256(chunk) != digest {
		return ErrChunk
	}
	_, _, err := r.Repository.Append(d, offset, chunk, digest)
	if err == nil {
		r.chunks.Add(1)
		r.bytes.Add(uint64(len(chunk)))
	}
	return err
}
