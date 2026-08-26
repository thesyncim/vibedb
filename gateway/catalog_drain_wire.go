package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrClusterCatalogDrainWire    = errors.New("gateway: invalid cluster catalog drain wire frame")
	ErrClusterCatalogDrainUnknown = errors.New("gateway: catalog generation or digest is unknown")
)

const (
	clusterCatalogDrainRequestHeaderBytes = 212
	clusterCatalogDrainRequestTailBytes   = sha256.Size
	ClusterCatalogDrainCertificateBytes   = 240
	AbsoluteMaxCatalogDrainConcurrency    = 1024
)

var (
	clusterCatalogDrainRequestMagic     = [8]byte{'V', 'B', 'D', 'R', 'E', 'Q', 0, 0}
	clusterCatalogDrainCertificateMagic = [8]byte{'V', 'B', 'D', 'C', 'E', 'R', 'T', 0}
)

// ClusterCatalogDrainEnvelope is the complete immutable control-plane cut.
// The explicit move request is retained alongside the derived fence so the
// operation and step cannot be hidden behind, or recovered from, a digest.
type ClusterCatalogDrainEnvelope struct {
	Request ClusterCatalogDrainRequest
	Fence   ClusterCatalogDrainFence
}

func NewClusterCatalogDrainEnvelope(
	request ClusterCatalogDrainRequest,
	trust rafttransport.TrustDomain,
	members []ClusterCatalogDrainMember,
) (ClusterCatalogDrainEnvelope, error) {
	if !request.Valid() {
		return ClusterCatalogDrainEnvelope{}, ErrClusterCatalogDrainWire
	}
	fence, err := NewClusterCatalogDrainFence(
		request.fenceOperation(), request.Generation, request.CatalogDigest, trust, members,
	)
	if err != nil {
		return ClusterCatalogDrainEnvelope{}, errors.Join(ErrClusterCatalogDrainWire, err)
	}
	return ClusterCatalogDrainEnvelope{Request: request, Fence: fence}, nil
}

func (envelope ClusterCatalogDrainEnvelope) valid() bool {
	return envelope.Request.Valid() && envelope.Fence.valid() &&
		envelope.Fence.operation == envelope.Request.fenceOperation() &&
		envelope.Fence.generation == envelope.Request.Generation &&
		envelope.Fence.catalogDigest == envelope.Request.CatalogDigest
}

func clusterCatalogDrainRequestBytes(count int) (int, bool) {
	if count <= 0 || count > MaxClusterCatalogDrainGateways ||
		count > (math.MaxInt-clusterCatalogDrainRequestHeaderBytes-clusterCatalogDrainRequestTailBytes)/24 {
		return 0, false
	}
	return clusterCatalogDrainRequestHeaderBytes + count*24 + clusterCatalogDrainRequestTailBytes, true
}

// AppendClusterCatalogDrainEnvelope appends the sole canonical request frame.
// Its maximum size is bounded by the uint16 roster grammar, independently of
// the smaller concurrency bound used while contacting those members.
func AppendClusterCatalogDrainEnvelope(dst []byte, envelope ClusterCatalogDrainEnvelope) ([]byte, error) {
	if !envelope.valid() {
		return dst, ErrClusterCatalogDrainWire
	}
	size, ok := clusterCatalogDrainRequestBytes(envelope.Fence.MemberCount())
	if !ok || len(dst) > math.MaxInt-size {
		return dst, ErrClusterCatalogDrainWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	raw := dst[start:]
	copy(raw[:8], clusterCatalogDrainRequestMagic[:])
	copy(raw[8:40], envelope.Request.Operation[:])
	copy(raw[40:72], envelope.Request.Step[:])
	binary.LittleEndian.PutUint64(raw[72:80], envelope.Request.Generation)
	copy(raw[80:112], envelope.Request.CatalogDigest[:])
	copy(raw[112:128], envelope.Fence.trust.ClusterID[:])
	copy(raw[128:144], envelope.Fence.trust.ClusterIncarnation[:])
	copy(raw[144:176], envelope.Fence.rosterDigest[:])
	copy(raw[176:208], envelope.Fence.digest[:])
	binary.LittleEndian.PutUint32(raw[208:212], uint32(envelope.Fence.MemberCount()))
	offset := clusterCatalogDrainRequestHeaderBytes
	for _, member := range envelope.Fence.members {
		copy(raw[offset:offset+16], member.Node[:])
		binary.LittleEndian.PutUint64(raw[offset+16:offset+24], member.Incarnation)
		offset += 24
	}
	checksum := sha256.Sum256(raw[:offset])
	copy(raw[offset:], checksum[:])
	return dst, nil
}

func OpenClusterCatalogDrainEnvelope(raw []byte) (ClusterCatalogDrainEnvelope, error) {
	if len(raw) < clusterCatalogDrainRequestHeaderBytes+24+clusterCatalogDrainRequestTailBytes ||
		!bytes.Equal(raw[:8], clusterCatalogDrainRequestMagic[:]) {
		return ClusterCatalogDrainEnvelope{}, ErrClusterCatalogDrainWire
	}
	count := binary.LittleEndian.Uint32(raw[208:212])
	size, ok := clusterCatalogDrainRequestBytes(int(count))
	if !ok || len(raw) != size ||
		sha256.Sum256(raw[:size-sha256.Size]) != [sha256.Size]byte(raw[size-sha256.Size:]) {
		return ClusterCatalogDrainEnvelope{}, ErrClusterCatalogDrainWire
	}
	var request ClusterCatalogDrainRequest
	copy(request.Operation[:], raw[8:40])
	copy(request.Step[:], raw[40:72])
	request.Generation = binary.LittleEndian.Uint64(raw[72:80])
	copy(request.CatalogDigest[:], raw[80:112])
	var trust rafttransport.TrustDomain
	copy(trust.ClusterID[:], raw[112:128])
	copy(trust.ClusterIncarnation[:], raw[128:144])
	var encodedRoster, encodedFence [sha256.Size]byte
	copy(encodedRoster[:], raw[144:176])
	copy(encodedFence[:], raw[176:208])
	members := make([]ClusterCatalogDrainMember, count)
	offset := clusterCatalogDrainRequestHeaderBytes
	for index := range members {
		copy(members[index].Node[:], raw[offset:offset+16])
		members[index].Incarnation = binary.LittleEndian.Uint64(raw[offset+16 : offset+24])
		offset += 24
	}
	envelope, err := NewClusterCatalogDrainEnvelope(request, trust, members)
	if err != nil || envelope.Fence.rosterDigest != encodedRoster || envelope.Fence.digest != encodedFence {
		return ClusterCatalogDrainEnvelope{}, ErrClusterCatalogDrainWire
	}
	return envelope, nil
}

func WriteClusterCatalogDrainEnvelope(writer io.Writer, envelope ClusterCatalogDrainEnvelope) error {
	raw, err := AppendClusterCatalogDrainEnvelope(nil, envelope)
	if err != nil {
		return err
	}
	return writeCatalogDrainFull(writer, raw)
}

func ReadClusterCatalogDrainEnvelope(reader io.Reader) (ClusterCatalogDrainEnvelope, error) {
	var header [clusterCatalogDrainRequestHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return ClusterCatalogDrainEnvelope{}, errors.Join(ErrClusterCatalogDrainWire, err)
	}
	count := binary.LittleEndian.Uint32(header[208:212])
	size, ok := clusterCatalogDrainRequestBytes(int(count))
	if !ok {
		return ClusterCatalogDrainEnvelope{}, ErrClusterCatalogDrainWire
	}
	raw := make([]byte, size)
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[len(header):]); err != nil {
		return ClusterCatalogDrainEnvelope{}, errors.Join(ErrClusterCatalogDrainWire, err)
	}
	return OpenClusterCatalogDrainEnvelope(raw)
}

func WriteClusterCatalogDrainAck(writer io.Writer, ack ClusterCatalogDrainAck) error {
	raw, err := AppendClusterCatalogDrainAck(nil, ack)
	if err != nil {
		return err
	}
	return writeCatalogDrainFull(writer, raw)
}

func ReadClusterCatalogDrainAck(reader io.Reader) (ClusterCatalogDrainAck, error) {
	var raw [ClusterCatalogDrainAckBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return ClusterCatalogDrainAck{}, errors.Join(ErrClusterCatalogDrainWire, err)
	}
	return OpenClusterCatalogDrainAck(raw[:])
}

func AppendClusterCatalogDrainCertificate(dst []byte, certificate ClusterCatalogDrainCertificate) ([]byte, error) {
	if !certificate.ValidFor(certificate.Request) ||
		certificate.Proof != clusterCatalogDrainCertificateProof(
			certificate.FenceDigest, certificate.RosterDigest,
		) || len(dst) > math.MaxInt-ClusterCatalogDrainCertificateBytes {
		return dst, ErrClusterCatalogDrainWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, ClusterCatalogDrainCertificateBytes)...)
	raw := dst[start:]
	copy(raw[:8], clusterCatalogDrainCertificateMagic[:])
	copy(raw[8:40], certificate.Request.Operation[:])
	copy(raw[40:72], certificate.Request.Step[:])
	binary.LittleEndian.PutUint64(raw[72:80], certificate.Request.Generation)
	copy(raw[80:112], certificate.Request.CatalogDigest[:])
	copy(raw[112:144], certificate.FenceDigest[:])
	copy(raw[144:176], certificate.RosterDigest[:])
	copy(raw[176:208], certificate.Proof[:])
	checksum := sha256.Sum256(raw[:208])
	copy(raw[208:], checksum[:])
	return dst, nil
}

func OpenClusterCatalogDrainCertificate(raw []byte) (ClusterCatalogDrainCertificate, error) {
	if len(raw) != ClusterCatalogDrainCertificateBytes ||
		!bytes.Equal(raw[:8], clusterCatalogDrainCertificateMagic[:]) ||
		sha256.Sum256(raw[:208]) != [sha256.Size]byte(raw[208:240]) {
		return ClusterCatalogDrainCertificate{}, ErrClusterCatalogDrainWire
	}
	var certificate ClusterCatalogDrainCertificate
	copy(certificate.Request.Operation[:], raw[8:40])
	copy(certificate.Request.Step[:], raw[40:72])
	certificate.Request.Generation = binary.LittleEndian.Uint64(raw[72:80])
	copy(certificate.Request.CatalogDigest[:], raw[80:112])
	copy(certificate.FenceDigest[:], raw[112:144])
	copy(certificate.RosterDigest[:], raw[144:176])
	copy(certificate.Proof[:], raw[176:208])
	expected := clusterCatalogDrainCertificateProof(
		certificate.FenceDigest, certificate.RosterDigest,
	)
	if !certificate.ValidFor(certificate.Request) || certificate.Proof != expected {
		return ClusterCatalogDrainCertificate{}, ErrClusterCatalogDrainWire
	}
	return certificate, nil
}

func clusterCatalogDrainCertificateProof(
	fenceDigest, rosterDigest [sha256.Size]byte,
) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-drain/certificate\x00"))
	hash.Write(fenceDigest[:])
	hash.Write(rosterDigest[:])
	var proof [sha256.Size]byte
	hash.Sum(proof[:0])
	return proof
}

func writeCatalogDrainFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

// ClusterCatalogDigestVerifier proves that this gateway consumed the exact
// replicated catalog bytes, not merely a numerically equal generation.
type ClusterCatalogDigestVerifier interface {
	VerifyClusterCatalogDigest(context.Context, uint64, [sha256.Size]byte) error
}

type ClusterCatalogDrainAuthorizeFunc func(rafttransport.PeerIdentity, ClusterCatalogDrainRequest) bool

type ClusterCatalogDrainControlOptions struct {
	Holder        *CatalogHolder
	Catalog       ClusterCatalogDigestVerifier
	Member        ClusterCatalogDrainMember
	Authorize     ClusterCatalogDrainAuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

// ClusterCatalogDrainControlService handles one exact request on one already
// authenticated gateway-control stream. A disconnect after request receipt is
// harmless: catalog drain is monotonic and exact retries emit the same ack.
type ClusterCatalogDrainControlService struct {
	holder        *CatalogHolder
	catalog       ClusterCatalogDigestVerifier
	member        ClusterCatalogDrainMember
	authorize     ClusterCatalogDrainAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewClusterCatalogDrainControlService(options ClusterCatalogDrainControlOptions) (*ClusterCatalogDrainControlService, error) {
	if options.Holder == nil || options.Catalog == nil || options.Member.Node == (rafttransport.NodeID{}) ||
		options.Member.Incarnation == 0 || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil {
		return nil, ErrClusterCatalogDrainWire
	}
	return &ClusterCatalogDrainControlService{
		holder: options.Holder, catalog: options.Catalog, member: options.Member,
		authorize: options.Authorize, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline,
	}, nil
}

func (service *ClusterCatalogDrainControlService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficGatewayControl {
		return ErrClusterCatalogDrainAuth
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedCatalogDrainDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrClusterCatalogDrainWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	envelope, err := ReadClusterCatalogDrainEnvelope(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != envelope.Fence.trust || !envelope.Fence.contains(service.member) ||
		!service.authorize(peer, envelope.Request) {
		return ErrClusterCatalogDrainAuth
	}
	if err = service.catalog.VerifyClusterCatalogDigest(
		ctx, envelope.Request.Generation, envelope.Request.CatalogDigest,
	); err != nil {
		return errors.Join(ErrClusterCatalogDrainUnknown, err)
	}
	ack, err := CollectClusterCatalogDrainAck(ctx, service.holder, envelope.Fence, service.member)
	if err != nil {
		return err
	}
	if deadline := boundedCatalogDrainDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrClusterCatalogDrainWire
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteClusterCatalogDrainAck(connection, ack)
}

type GatewayCatalogDrainStreamOpener interface {
	OpenGatewayControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type ClusterCatalogDrainClientOptions struct {
	Opener        GatewayCatalogDrainStreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

// ClusterCatalogDrainClient implements ClusterCatalogDrainCollector. Worker
// count bounds sockets and memory, while an arbitrarily larger canonical
// roster drains through the same fixed worker set.
type ClusterCatalogDrainClient struct {
	opener        GatewayCatalogDrainStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	maxConcurrent int
}

// ClusterCatalogDrainRequestCollector is the stronger remote collection seam.
// The original collector interface remains available for in-process kernels,
// while a wire collector receives the operation and step preimages explicitly.
type ClusterCatalogDrainRequestCollector interface {
	CollectClusterCatalogDrainRequest(
		context.Context,
		ClusterCatalogDrainRequest,
		ClusterCatalogDrainFence,
		func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
	) error
}

func NewClusterCatalogDrainClient(options ClusterCatalogDrainClientOptions) (*ClusterCatalogDrainClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxCatalogDrainConcurrency {
		return nil, ErrClusterCatalogDrainWire
	}
	return &ClusterCatalogDrainClient{
		opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline, maxConcurrent: options.MaxConcurrent,
	}, nil
}

func (client *ClusterCatalogDrainClient) CollectClusterCatalogDrain(
	context.Context,
	ClusterCatalogDrainFence,
	func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
) error {
	// A fence retains only the collision-resistant operation+step derivation;
	// transmitting it would violate the protocol's explicit preimage contract.
	return ErrClusterCatalogDrainWire
}

func (client *ClusterCatalogDrainClient) CollectClusterCatalogDrainRequest(
	ctx context.Context,
	request ClusterCatalogDrainRequest,
	fence ClusterCatalogDrainFence,
	accept func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
) error {
	envelope, err := NewClusterCatalogDrainEnvelope(request, fence.trust, fence.members)
	if err != nil || envelope.Fence.digest != fence.digest {
		return ErrClusterCatalogDrainWire
	}
	return client.collectEnvelope(ctx, envelope, accept)
}

func (client *ClusterCatalogDrainClient) collectOne(
	ctx context.Context,
	envelope ClusterCatalogDrainEnvelope,
	encoded []byte,
	member ClusterCatalogDrainMember,
) (rafttransport.PeerIdentity, ClusterCatalogDrainAck, error) {
	connection, err := client.opener.OpenGatewayControl(ctx, member.Node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, err
	}
	if connection == nil {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, ErrClusterCatalogDrainWire
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl ||
		peer.TrustDomain != envelope.Fence.trust || peer.Node != member.Node {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, ErrClusterCatalogDrainAuth
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedCatalogDrainDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, ErrClusterCatalogDrainWire
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, err
	}
	if err = writeCatalogDrainFull(connection, encoded); err != nil {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, err
	}
	if deadline := boundedCatalogDrainDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, ErrClusterCatalogDrainWire
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, err
	}
	ack, err := ReadClusterCatalogDrainAck(connection)
	if err != nil || ack.Member != member || ack.FenceDigest != envelope.Fence.digest {
		return rafttransport.PeerIdentity{}, ClusterCatalogDrainAck{}, errors.Join(ErrClusterCatalogDrainAck, err)
	}
	return peer, ack, nil
}

func (client *ClusterCatalogDrainClient) collectEnvelope(
	ctx context.Context,
	envelope ClusterCatalogDrainEnvelope,
	accept func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
) error {
	if client == nil || ctx == nil || !envelope.valid() || accept == nil {
		return ErrClusterCatalogDrainWire
	}
	encoded, err := AppendClusterCatalogDrainEnvelope(nil, envelope)
	if err != nil {
		return err
	}
	workers := min(client.maxConcurrent, envelope.Fence.MemberCount())
	work := make(chan int)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var group sync.WaitGroup
	var acceptMu sync.Mutex
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range work {
				member, _ := envelope.Fence.Member(index)
				peer, ack, err := client.collectOne(ctx, envelope, encoded, member)
				if err == nil {
					acceptMu.Lock()
					err = accept(peer, ack)
					acceptMu.Unlock()
				}
				if err != nil {
					cancel(err)
					return
				}
			}
		}()
	}
send:
	for index := 0; index < envelope.Fence.MemberCount(); index++ {
		select {
		case work <- index:
		case <-ctx.Done():
			break send
		}
	}
	close(work)
	group.Wait()
	return context.Cause(ctx)
}

func boundedCatalogDrainDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}
