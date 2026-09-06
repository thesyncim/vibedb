package nodecontrol

// This file contains the read-only bootstrap capability used by an empty
// physical node before it owns a group.  It is intentionally separate from
// the prepare/adopt command protocol: reading a committed enrollment row must
// never be able to create a local artifact or publish a serving member.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

var (
	ErrBootstrapRead               = errors.New("nodecontrol: invalid bootstrap enrollment read")
	ErrBootstrapReadUnauthorized   = errors.New("nodecontrol: bootstrap enrollment read is unauthorized")
	ErrBootstrapReadStale          = errors.New("nodecontrol: bootstrap enrollment read is stale")
	ErrBootstrapReadUnavailable    = errors.New("nodecontrol: bootstrap enrollment reader is unavailable")
	ErrBootstrapReadConflict       = errors.New("nodecontrol: bootstrap enrollment reply conflicts")
	ErrBootstrapReadOutcomeUnknown = errors.New("nodecontrol: bootstrap enrollment read outcome is unknown")
	ErrBootstrapReadBound          = errors.New("nodecontrol: bootstrap enrollment read concurrency bound exceeded")
	ErrBootstrapReadRetired        = errors.New("nodecontrol: physical node is retired")
)

const (
	bootstrapReadVersion          = 1
	bootstrapReadRequestHeader    = 84
	bootstrapReadResponseHeader   = 64
	MaxBootstrapReadReplyBytes    = 128 << 10
	MaxBootstrapGatewaySeeds      = 16
	bootstrapReadMaxConcurrency   = 64
	bootstrapReadNonceBytes       = 16
	bootstrapReadResponseSuccess  = 1
	bootstrapReadOperationReadOwn = 1
)

var (
	bootstrapReadRequestMagic  = [8]byte{'V', 'B', 'D', 'B', 'R', 'E', 'A', 'D'}
	bootstrapReadResponseMagic = [8]byte{'V', 'B', 'D', 'B', 'R', 'E', 'S', 'P'}
)

// BootstrapGatewaySeed is a public, bounded gateway endpoint copied from the
// prepared node manifest.  The address is only a dial coordinate; the peer
// identity and SPKI pin are checked again after every TLS handshake.
type BootstrapGatewaySeed struct {
	NodeID         rafttransport.NodeID `json:"node_id"`
	Incarnation    uint64               `json:"incarnation"`
	ControlAddress string               `json:"control_address"`
	SPKIPinDigest  replication.Digest   `json:"spki_pin_digest"`
}

func (seed BootstrapGatewaySeed) Valid() bool {
	if seed.NodeID == (rafttransport.NodeID{}) || seed.Incarnation == 0 ||
		seed.SPKIPinDigest == (replication.Digest{}) || len(seed.ControlAddress) == 0 ||
		len(seed.ControlAddress) > 1024 {
		return false
	}
	host, port, err := net.SplitHostPort(seed.ControlAddress)
	if err != nil || host == "" || port == "" || bytes.IndexByte([]byte(seed.ControlAddress), 0) >= 0 {
		return false
	}
	return true
}

// BootstrapReadOperation names the one operation exposed by this protocol.
// Keeping it explicit prevents a request captured on another control path
// from being replayed as a metadata read.
type BootstrapReadOperation uint8

const OpReadOwnEnrollment BootstrapReadOperation = bootstrapReadOperationReadOwn

func (operation BootstrapReadOperation) valid() bool { return operation == OpReadOwnEnrollment }

// BootstrapReadRequest is fixed width on the wire.  Nonce is generated for
// every fresh query, including failover to a second configured seed.
type BootstrapReadRequest struct {
	Nonce        [bootstrapReadNonceBytes]byte
	Operation    BootstrapReadOperation
	PhysicalNode rafttransport.NodeID
	Incarnation  uint64
	IntentID     [32]byte
}

func (request BootstrapReadRequest) valid() bool {
	return request.Operation.valid() && request.Nonce != ([bootstrapReadNonceBytes]byte{}) &&
		request.PhysicalNode != (rafttransport.NodeID{}) && request.Incarnation != 0 &&
		request.IntentID != ([32]byte{})
}

// BootstrapReadReply is the complete current enrollment row plus the global
// witnesses used to prove that the target was read from one stable physical
// directory/catalog cut.  The full GroupEnrollmentIntent is retained so a
// restart cannot reconstruct a command from endpoint hints.
type BootstrapReadReply struct {
	Nonce                     [bootstrapReadNonceBytes]byte `json:"nonce"`
	Operation                 BootstrapReadOperation        `json:"operation"`
	PhysicalNode              rafttransport.NodeID          `json:"physical_node"`
	Incarnation               uint64                        `json:"incarnation"`
	IntentID                  [32]byte                      `json:"intent_id"`
	Intent                    gateway.GroupEnrollmentIntent `json:"intent"`
	IntentDigest              replication.Digest            `json:"intent_digest"`
	Node                      gateway.NodeRecord            `json:"node"`
	DirectoryCutRevision      uint64                        `json:"directory_cut_revision"`
	DirectoryCutDigest        replication.Digest            `json:"directory_cut_digest"`
	CatalogGeneration         uint64                        `json:"catalog_generation"`
	CatalogHeadDigest         replication.Digest            `json:"catalog_head_digest"`
	EnrollmentDirectoryDigest replication.Digest            `json:"enrollment_directory_digest"`
}

func (reply BootstrapReadReply) valid() bool {
	return reply.Nonce != ([bootstrapReadNonceBytes]byte{}) && reply.Operation.valid() &&
		reply.PhysicalNode != (rafttransport.NodeID{}) && reply.Incarnation != 0 &&
		reply.IntentID != ([32]byte{}) && reply.Intent.Valid() && reply.Intent.IntentID == reply.IntentID &&
		reply.Intent.Target.Node == reply.PhysicalNode && reply.Intent.Target.NodeIncarnation == reply.Incarnation &&
		reply.IntentDigest == reply.Intent.Digest() && reply.Node.Valid() &&
		reply.Node.NodeID == reply.PhysicalNode && reply.Node.Incarnation == reply.Incarnation &&
		reply.Node.Lifecycle != gateway.NodeDecommissioned && reply.DirectoryCutRevision != 0 &&
		reply.DirectoryCutDigest != (replication.Digest{}) && reply.CatalogGeneration != 0 &&
		reply.CatalogHeadDigest != (replication.Digest{}) &&
		reply.EnrollmentDirectoryDigest != (replication.Digest{}) &&
		reply.Node.CatalogGeneration <= reply.CatalogGeneration
}

// AppendBootstrapReadRequest appends the fixed request grammar.
func AppendBootstrapReadRequest(dst []byte, request BootstrapReadRequest) ([]byte, error) {
	if !request.valid() || len(dst) > math.MaxInt-bootstrapReadRequestHeader {
		return dst, ErrBootstrapRead
	}
	start := len(dst)
	dst = append(dst, make([]byte, bootstrapReadRequestHeader)...)
	raw := dst[start:]
	copy(raw[:8], bootstrapReadRequestMagic[:])
	raw[8] = bootstrapReadVersion
	raw[9] = byte(request.Operation)
	// raw[10:12] is reserved and remains zero.
	copy(raw[12:28], request.Nonce[:])
	copy(raw[28:44], request.PhysicalNode[:])
	binary.BigEndian.PutUint64(raw[44:52], request.Incarnation)
	copy(raw[52:84], request.IntentID[:])
	return dst, nil
}

func OpenBootstrapReadRequest(raw []byte) (BootstrapReadRequest, error) {
	if len(raw) != bootstrapReadRequestHeader || !bytes.Equal(raw[:8], bootstrapReadRequestMagic[:]) ||
		raw[8] != bootstrapReadVersion || raw[10] != 0 || raw[11] != 0 {
		return BootstrapReadRequest{}, ErrBootstrapRead
	}
	var request BootstrapReadRequest
	request.Operation = BootstrapReadOperation(raw[9])
	copy(request.Nonce[:], raw[12:28])
	copy(request.PhysicalNode[:], raw[28:44])
	request.Incarnation = binary.BigEndian.Uint64(raw[44:52])
	copy(request.IntentID[:], raw[52:84])
	if !request.valid() {
		return BootstrapReadRequest{}, ErrBootstrapRead
	}
	return request, nil
}

func WriteBootstrapReadRequest(writer io.Writer, request BootstrapReadRequest) error {
	raw, err := AppendBootstrapReadRequest(nil, request)
	if err != nil {
		return err
	}
	return bootstrapReadWriteFull(writer, raw)
}

func ReadBootstrapReadRequest(reader io.Reader) (BootstrapReadRequest, error) {
	var raw [bootstrapReadRequestHeader]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return BootstrapReadRequest{}, errors.Join(ErrBootstrapRead, err)
	}
	return OpenBootstrapReadRequest(raw[:])
}

// AppendBootstrapReadReply writes a length-delimited canonical JSON payload.
// The fixed header binds the payload digest and request nonce before the
// decoder allocates any response object.
func AppendBootstrapReadReply(dst []byte, reply BootstrapReadReply) ([]byte, error) {
	if !reply.valid() {
		return dst, ErrBootstrapRead
	}
	payload, err := vibejson.Marshal(&reply)
	if err != nil || len(payload) == 0 || len(payload) > MaxBootstrapReadReplyBytes ||
		len(dst) > math.MaxInt-bootstrapReadResponseHeader-len(payload) {
		return dst, errors.Join(ErrBootstrapRead, err)
	}
	start := len(dst)
	dst = append(dst, make([]byte, bootstrapReadResponseHeader+len(payload))...)
	raw := dst[start:]
	copy(raw[:8], bootstrapReadResponseMagic[:])
	raw[8] = bootstrapReadVersion
	raw[9] = bootstrapReadResponseSuccess
	// raw[10:12] is reserved and remains zero.
	copy(raw[12:28], reply.Nonce[:])
	binary.BigEndian.PutUint32(raw[28:32], uint32(len(payload)))
	digest := sha256.Sum256(payload)
	copy(raw[32:64], digest[:])
	copy(raw[64:], payload)
	return dst, nil
}

func OpenBootstrapReadReply(raw []byte) (BootstrapReadReply, error) {
	if len(raw) < bootstrapReadResponseHeader || !bytes.Equal(raw[:8], bootstrapReadResponseMagic[:]) ||
		raw[8] != bootstrapReadVersion || raw[9] != bootstrapReadResponseSuccess || raw[10] != 0 || raw[11] != 0 {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	payloadBytes := int(binary.BigEndian.Uint32(raw[28:32]))
	if payloadBytes == 0 || payloadBytes > MaxBootstrapReadReplyBytes || len(raw) != bootstrapReadResponseHeader+payloadBytes {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	if sha256.Sum256(raw[64:]) != [sha256.Size]byte(raw[32:64]) {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	var reply BootstrapReadReply
	if err := vibejson.Unmarshal(raw[64:], &reply); err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapRead, err)
	}
	canonical, err := vibejson.Marshal(&reply)
	if err != nil || !bytes.Equal(canonical, raw[64:]) || !reply.valid() {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapRead, err)
	}
	if !bytes.Equal(reply.Nonce[:], raw[12:28]) {
		return BootstrapReadReply{}, ErrBootstrapReadConflict
	}
	return reply, nil
}

func WriteBootstrapReadReply(writer io.Writer, reply BootstrapReadReply) error {
	raw, err := AppendBootstrapReadReply(nil, reply)
	if err != nil {
		return err
	}
	return bootstrapReadWriteFull(writer, raw)
}

func ReadBootstrapReadReply(reader io.Reader) (BootstrapReadReply, error) {
	var header [bootstrapReadResponseHeader]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapRead, err)
	}
	payloadBytes := int(binary.BigEndian.Uint32(header[28:32]))
	if payloadBytes == 0 || payloadBytes > MaxBootstrapReadReplyBytes {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	raw := make([]byte, bootstrapReadResponseHeader+payloadBytes)
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[bootstrapReadResponseHeader:]); err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapRead, err)
	}
	return OpenBootstrapReadReply(raw)
}

func BootstrapReadRequestDiscriminator() [8]byte { return bootstrapReadRequestMagic }

type BootstrapReadAuthority interface {
	ReadNode(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error)
	ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error)
	ReadEnrollmentIntent(context.Context, [32]byte) (gateway.GroupEnrollmentIntent, error)
	ScanNodeReferences(context.Context, rafttransport.NodeID, uint64) (gateway.NodeReferenceEvidence, error)
}

type BootstrapReadAuthorizeFunc func(rafttransport.PeerIdentity, gateway.NodeRecord) bool

// BootstrapReadAuthenticatedAuthorizeFunc is the dynamic service-directory
// hook. The peer key comes from the completed TLS stream and cannot be copied
// from the fixed request grammar.
type BootstrapReadAuthenticatedAuthorizeFunc func(rafttransport.PeerBinding, gateway.NodeRecord) bool

type BootstrapReadServiceOptions struct {
	Authority              BootstrapReadAuthority
	TrustDomain            rafttransport.TrustDomain
	Authorize              BootstrapReadAuthorizeFunc
	AuthorizeAuthenticated BootstrapReadAuthenticatedAuthorizeFunc
	ReadDeadline           rafttransport.DeadlineFunc
	WriteDeadline          rafttransport.DeadlineFunc
	MaxConcurrent          int
}

type BootstrapReadService struct {
	authority              BootstrapReadAuthority
	trustDomain            rafttransport.TrustDomain
	authorize              BootstrapReadAuthorizeFunc
	authorizeAuthenticated BootstrapReadAuthenticatedAuthorizeFunc
	readDeadline           rafttransport.DeadlineFunc
	writeDeadline          rafttransport.DeadlineFunc
	slots                  chan struct{}
}

func NewBootstrapReadService(options BootstrapReadServiceOptions) (*BootstrapReadService, error) {
	if options.Authority == nil || options.TrustDomain == (rafttransport.TrustDomain{}) ||
		options.Authorize == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > bootstrapReadMaxConcurrency {
		return nil, ErrBootstrapRead
	}
	return &BootstrapReadService{authority: options.Authority, trustDomain: options.TrustDomain,
		authorize: options.Authorize, authorizeAuthenticated: options.AuthorizeAuthenticated,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (service *BootstrapReadService) Serve(ctx context.Context, connection rafttransport.PeerConnection) (resultErr error) {
	if service == nil || ctx == nil || connection == nil || connection.TrafficClass() != rafttransport.TrafficGatewayControl {
		return ErrBootstrapReadUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrBootstrapReadBound
	}
	if deadline := bootstrapReadBoundedDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrBootstrapRead
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, err := ReadBootstrapReadRequest(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != service.trustDomain || peer.Node != request.PhysicalNode {
		return ErrBootstrapReadUnauthorized
	}
	record, err := service.authority.ReadNode(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil {
		return errors.Join(ErrBootstrapReadStale, err)
	}
	if record.Lifecycle == gateway.NodeDecommissioned {
		return ErrBootstrapReadRetired
	}
	if record.Lifecycle != gateway.NodeJoining && record.Lifecycle != gateway.NodeActive && record.Lifecycle != gateway.NodeDraining {
		return ErrBootstrapReadStale
	}
	if record.ServiceKeyDigest != replication.Digest(connection.PeerKeyDigest()) || !service.authorize(peer, record) ||
		service.authorizeAuthenticated != nil && !service.authorizeAuthenticated(rafttransport.Binding(connection), record) {
		return ErrBootstrapReadUnauthorized
	}
	reply, err := service.readStable(ctx, request, record)
	if err != nil {
		return err
	}
	if deadline := bootstrapReadBoundedDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrBootstrapRead
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteBootstrapReadReply(connection, reply)
}

func (service *BootstrapReadService) readStable(
	ctx context.Context, request BootstrapReadRequest, initial gateway.NodeRecord,
) (BootstrapReadReply, error) {
	before, err := service.authority.ReadNodeDirectoryCut(ctx)
	if err != nil || !before.Valid() {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadStale, err)
	}
	intent, err := service.authority.ReadEnrollmentIntent(ctx, request.IntentID)
	if err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadStale, err)
	}
	if !intent.Valid() || intent.State == gateway.EnrollmentCancelled || intent.IntentID != request.IntentID ||
		intent.Target.Node != request.PhysicalNode || intent.Target.NodeIncarnation != request.Incarnation {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	evidence, err := service.authority.ScanNodeReferences(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadStale, err)
	}
	after, err := service.authority.ReadNodeDirectoryCut(ctx)
	if err != nil || !after.Valid() || before.Revision != after.Revision || before.Digest != after.Digest ||
		evidence.DirectoryCutRevision != after.Revision || evidence.DirectoryCutDigest != after.Digest {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	final, err := service.authority.ReadNode(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil || final != initial || final.Lifecycle == gateway.NodeDecommissioned {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	// The reference scan and the directory cut fence cover the global
	// enrollment directory, but the requested row is a separate bounded read.
	// Read it again immediately before publishing the reply.  A controller may
	// cancel, prepare, or complete the row while the scan is in flight; a reply
	// that combines the first row with the later witness would otherwise be a
	// mixed metadata cut.  The final cut below also detects a concurrent row
	// mutation that advanced the directory revision after the first scan.
	finalIntent, err := service.authority.ReadEnrollmentIntent(ctx, request.IntentID)
	if err != nil || !sameBootstrapReadIntent(finalIntent, intent) || !finalIntent.Valid() ||
		finalIntent.State == gateway.EnrollmentCancelled ||
		finalIntent.Target.Node != request.PhysicalNode ||
		finalIntent.Target.NodeIncarnation != request.Incarnation {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	finalCut, err := service.authority.ReadNodeDirectoryCut(ctx)
	if err != nil || !finalCut.Valid() || finalCut.Revision != after.Revision || finalCut.Digest != after.Digest {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	// Catalog publication and enrollment-directory updates do not necessarily
	// advance the physical-node directory cut. Repeating the bounded reference
	// scan closes that second race and ensures the catalog/enrollment witnesses
	// in the reply describe the same observed cut as the row re-read.
	verification, err := service.authority.ScanNodeReferences(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil || verification != evidence {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	reply := BootstrapReadReply{
		Nonce: request.Nonce, Operation: request.Operation, PhysicalNode: request.PhysicalNode,
		Incarnation: request.Incarnation, IntentID: request.IntentID, Intent: finalIntent,
		IntentDigest: finalIntent.Digest(), Node: final, DirectoryCutRevision: finalCut.Revision,
		DirectoryCutDigest: finalCut.Digest, CatalogGeneration: evidence.CatalogGeneration,
		CatalogHeadDigest:         evidence.CatalogHeadDigest,
		EnrollmentDirectoryDigest: evidence.EnrollmentDirectoryDigest,
	}
	if !reply.valid() {
		return BootstrapReadReply{}, ErrBootstrapReadStale
	}
	return reply, nil
}

type BootstrapReadStreamOpener interface {
	OpenBootstrapGatewayControl(context.Context, BootstrapGatewaySeed) (rafttransport.PeerConnection, error)
}

type BootstrapReadClientOptions struct {
	Opener        BootstrapReadStreamOpener
	Seeds         []BootstrapGatewaySeed
	TrustDomain   rafttransport.TrustDomain
	PhysicalNode  rafttransport.NodeID
	Incarnation   uint64
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	Nonce         func() ([bootstrapReadNonceBytes]byte, error)
}

type BootstrapReadClient struct {
	opener        BootstrapReadStreamOpener
	seeds         []BootstrapGatewaySeed
	trustDomain   rafttransport.TrustDomain
	physicalNode  rafttransport.NodeID
	incarnation   uint64
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	nonce         func() ([bootstrapReadNonceBytes]byte, error)
}

func NewBootstrapReadClient(options BootstrapReadClientOptions) (*BootstrapReadClient, error) {
	if options.Opener == nil || len(options.Seeds) == 0 || len(options.Seeds) > MaxBootstrapGatewaySeeds ||
		options.TrustDomain == (rafttransport.TrustDomain{}) || options.PhysicalNode == (rafttransport.NodeID{}) ||
		options.Incarnation == 0 || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrBootstrapRead
	}
	seeds := slices.Clone(options.Seeds)
	seen := make(map[rafttransport.NodeID]struct{}, len(seeds))
	for _, seed := range seeds {
		if !seed.Valid() {
			return nil, ErrBootstrapRead
		}
		if _, found := seen[seed.NodeID]; found {
			return nil, ErrBootstrapRead
		}
		seen[seed.NodeID] = struct{}{}
	}
	nonce := options.Nonce
	if nonce == nil {
		nonce = func() ([bootstrapReadNonceBytes]byte, error) {
			var result [bootstrapReadNonceBytes]byte
			_, err := io.ReadFull(rand.Reader, result[:])
			return result, err
		}
	}
	return &BootstrapReadClient{opener: options.Opener, seeds: seeds, trustDomain: options.TrustDomain,
		physicalNode: options.PhysicalNode, incarnation: options.Incarnation,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline, nonce: nonce}, nil
}

func (client *BootstrapReadClient) ReadEnrollmentIntent(ctx context.Context, intentID [32]byte) (gateway.GroupEnrollmentIntent, error) {
	if client == nil || ctx == nil || intentID == ([32]byte{}) {
		return gateway.GroupEnrollmentIntent{}, ErrBootstrapRead
	}
	if cause := context.Cause(ctx); cause != nil {
		return gateway.GroupEnrollmentIntent{}, cause
	}
	var last error = ErrBootstrapReadUnavailable
	for _, seed := range client.seeds {
		nonce, err := client.nonce()
		if err != nil || nonce == ([bootstrapReadNonceBytes]byte{}) {
			last = errors.Join(ErrBootstrapReadUnavailable, err)
			continue
		}
		connection, openErr := client.opener.OpenBootstrapGatewayControl(ctx, seed)
		if openErr != nil {
			if connection != nil {
				_ = connection.Close()
			}
			last = errors.Join(ErrBootstrapReadUnavailable, openErr)
			continue
		}
		if connection == nil {
			last = ErrBootstrapReadUnavailable
			continue
		}
		intent, readErr := client.readOne(ctx, connection, seed, nonce, intentID)
		if readErr == nil {
			return intent, nil
		}
		last = readErr
	}
	return gateway.GroupEnrollmentIntent{}, last
}

func (client *BootstrapReadClient) readOne(
	ctx context.Context, connection rafttransport.PeerConnection, seed BootstrapGatewaySeed,
	nonce [bootstrapReadNonceBytes]byte, intentID [32]byte,
) (gateway.GroupEnrollmentIntent, error) {
	defer connection.Close()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl || peer.Node != seed.NodeID ||
		peer.TrustDomain != client.trustDomain || replication.Digest(connection.PeerKeyDigest()) != seed.SPKIPinDigest {
		return gateway.GroupEnrollmentIntent{}, ErrBootstrapReadUnauthorized
	}
	request := BootstrapReadRequest{Nonce: nonce, Operation: OpReadOwnEnrollment,
		PhysicalNode: client.physicalNode, Incarnation: client.incarnation, IntentID: intentID}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := bootstrapReadBoundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return gateway.GroupEnrollmentIntent{}, ErrBootstrapRead
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return gateway.GroupEnrollmentIntent{}, err
	}
	if err := WriteBootstrapReadRequest(connection, request); err != nil {
		return gateway.GroupEnrollmentIntent{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	if deadline := bootstrapReadBoundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return gateway.GroupEnrollmentIntent{}, ErrBootstrapReadOutcomeUnknown
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return gateway.GroupEnrollmentIntent{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	reply, err := ReadBootstrapReadReply(connection)
	if err != nil {
		return gateway.GroupEnrollmentIntent{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	if reply.Nonce != nonce || reply.Operation != request.Operation || reply.PhysicalNode != request.PhysicalNode ||
		reply.Incarnation != request.Incarnation || reply.IntentID != intentID || !reply.valid() {
		return gateway.GroupEnrollmentIntent{}, ErrBootstrapReadConflict
	}
	return reply.Intent, nil
}

func bootstrapReadBoundedDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func bootstrapReadWriteFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

var _ IntentReader = (*BootstrapReadClient)(nil)

// Directory reads decode independent proof and receipt allocations. Compare
// their values before comparing the remaining immutable and recovery fields.
func sameBootstrapReadIntent(left, right gateway.GroupEnrollmentIntent) bool {
	if (left.Proof == nil) != (right.Proof == nil) || (left.Receipt == nil) != (right.Receipt == nil) {
		return false
	}
	if left.Proof != nil && *left.Proof != *right.Proof {
		return false
	}
	if left.Receipt != nil && *left.Receipt != *right.Receipt {
		return false
	}
	left.Proof, right.Proof = nil, nil
	left.Receipt, right.Receipt = nil, nil
	return left == right
}
