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
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"strings"
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
	bootstrapReadVersion        = 1
	bootstrapReadRequestHeader  = 84
	bootstrapReadResponseHeader = 64
	// One durable intent (128 KiB), repeated catalog coordinates (<=128 KiB),
	// requester plus four physical nodes (5 * 32 KiB), and four membership
	// endpoints fit within 1 MiB. Each endpoint has six 4096-byte strings,
	// at most six JSON bytes per input byte, plus <2 KiB of identity metadata:
	// total <=1000 KiB with <24 KiB remaining for fixed route/witness fields.
	// Catalog endpoint aliases need not equal physical-directory handles.
	MaxBootstrapReadReplyBytes    = 1 << 20
	MaxBootstrapGatewaySeeds      = 16
	bootstrapReadMaxConcurrency   = 64
	bootstrapReadNonceBytes       = 16
	bootstrapReadResponseSuccess  = 1
	bootstrapReadResponseFailure  = 2
	maxBootstrapReadErrorBytes    = 1024
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

const (
	OpReadOwnEnrollment BootstrapReadOperation = bootstrapReadOperationReadOwn
	// OpReadOwnEnrollmentRecovery also reads current placement. A completed
	// enrollment is historical evidence, so restart requires this fresh cut.
	OpReadOwnEnrollmentRecovery BootstrapReadOperation = 2
)

func (operation BootstrapReadOperation) valid() bool {
	return operation == OpReadOwnEnrollment || operation == OpReadOwnEnrollmentRecovery
}

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
	Nonce        [bootstrapReadNonceBytes]byte `json:"nonce"`
	Operation    BootstrapReadOperation        `json:"operation"`
	PhysicalNode rafttransport.NodeID          `json:"physical_node"`
	Incarnation  uint64                        `json:"incarnation"`
	IntentID     [32]byte                      `json:"intent_id"`
	Intent       gateway.GroupEnrollmentIntent `json:"intent"`
	IntentDigest replication.Digest            `json:"intent_digest"`
	// IntentMissing is a recovery-only, stable authenticated absence result.
	// Live targets retain their enrollment rows; collected historical rows
	// therefore require no local runtime restoration.
	IntentMissing             bool               `json:"intent_missing,omitempty"`
	Node                      gateway.NodeRecord `json:"node"`
	DirectoryCutRevision      uint64             `json:"directory_cut_revision"`
	DirectoryCutDigest        replication.Digest `json:"directory_cut_digest"`
	CatalogGeneration         uint64             `json:"catalog_generation"`
	CatalogHeadDigest         replication.Digest `json:"catalog_head_digest"`
	EnrollmentDirectoryDigest replication.Digest `json:"enrollment_directory_digest"`
	// CurrentRoute is populated only for recovery reads. Nil on a recovery
	// reply proves this exact allocation is absent from the observed catalog.
	CurrentRoute *gateway.ReplicatedMembershipRoute `json:"current_route,omitempty"`
	// CurrentNodes supplies the physical identities for the bounded current
	// roster in the same order, followed by its optional enrolled target.
	CurrentNodes []gateway.NodeRecord `json:"current_nodes,omitempty"`
}

func (reply BootstrapReadReply) valid() bool {
	if !(reply.Nonce != ([bootstrapReadNonceBytes]byte{}) && reply.Operation.valid() &&
		reply.PhysicalNode != (rafttransport.NodeID{}) && reply.Incarnation != 0 &&
		reply.IntentID != ([32]byte{}) && reply.Node.Valid() &&
		reply.Node.NodeID == reply.PhysicalNode && reply.Node.Incarnation == reply.Incarnation &&
		reply.Node.Lifecycle != gateway.NodeDecommissioned && reply.DirectoryCutRevision != 0 &&
		reply.DirectoryCutDigest != (replication.Digest{}) && reply.CatalogGeneration != 0 &&
		reply.CatalogHeadDigest != (replication.Digest{}) &&
		reply.EnrollmentDirectoryDigest != (replication.Digest{}) &&
		reply.Node.CatalogGeneration <= reply.CatalogGeneration) {
		return false
	}
	if reply.IntentMissing {
		return reply.Operation == OpReadOwnEnrollmentRecovery && reply.Intent == (gateway.GroupEnrollmentIntent{}) &&
			reply.IntentDigest == (replication.Digest{}) && reply.CurrentRoute == nil && len(reply.CurrentNodes) == 0
	}
	return reply.Intent.Valid() && reply.Intent.State != gateway.EnrollmentCancelled &&
		reply.Intent.IntentID == reply.IntentID && reply.Intent.Target.Node == reply.PhysicalNode &&
		reply.Intent.Target.NodeIncarnation == reply.Incarnation && reply.IntentDigest == reply.Intent.Digest() && reply.validRecoveryRoute()
}

// EnrollmentMissing reports an explicit recovery absence only when the full
// request, physical identity, and stable directory/catalog witnesses validate.
// Errors and a zero-value reply never authorize skipping recovery.
func (reply BootstrapReadReply) EnrollmentMissing() bool {
	return reply.IntentMissing && reply.valid()
}

func (reply BootstrapReadReply) validRecoveryRoute() bool {
	if reply.CurrentRoute == nil {
		return len(reply.CurrentNodes) == 0
	}
	if reply.Operation != OpReadOwnEnrollmentRecovery {
		return false
	}
	route := reply.CurrentRoute.Serving
	if route.Group != reply.Intent.Group || route.Distribution != reply.Intent.Distribution ||
		route.Shard != reply.Intent.Shard || route.AllocationGeneration != uint64(reply.Intent.AllocationGeneration) ||
		!route.Command.Valid() || len(route.Replicas) != gateway.ServingReplicaCount {
		return false
	}
	replicas := slices.Clone(route.Replicas)
	if reply.CurrentRoute.HasEnrolledTarget {
		replicas = append(replicas, reply.CurrentRoute.EnrolledTarget)
	} else if reply.CurrentRoute.EnrolledTarget != (gateway.ReplicatedEndpoint{}) {
		return false
	}
	if len(reply.CurrentNodes) != len(replicas) {
		return false
	}
	for index, replica := range replicas {
		if replica.Member == 0 || replica.Node == (rafttransport.NodeID{}) || replica.NodeIncarnation == 0 ||
			replica.StoreID == ([16]byte{}) || replica.Endpoint == "" || replica.NativeEndpoint == "" ||
			replica.ControlEndpoint == "" {
			return false
		}
		for _, value := range []string{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint, replica.DataAddress, replica.Address, replica.ControlAddress} {
			if len(value) == 0 || len(value) > gateway.MaxScalingStringBytes {
				return false
			}
		}
		node := reply.CurrentNodes[index]
		if !node.Valid() || node.Lifecycle == gateway.NodeDecommissioned || node.NodeID != replica.Node ||
			node.Incarnation != replica.NodeIncarnation || node.CatalogGeneration > reply.CatalogGeneration ||
			node.NodeID == reply.PhysicalNode && node != reply.Node ||
			node.DataAddress != replica.DataAddress ||
			node.NativeAddress != replica.Address || node.ControlAddress != replica.ControlAddress {
			return false
		}
		for _, prior := range replicas[:index] {
			if prior.Member == replica.Member || prior.Node == replica.Node {
				return false
			}
		}
	}
	return true
}

// TargetServing reports whether this fresh authenticated recovery cut still
// places the exact enrolled identity in the current serving roster.
func (reply BootstrapReadReply) TargetServing() bool {
	if !reply.valid() || reply.Operation != OpReadOwnEnrollmentRecovery || reply.CurrentRoute == nil {
		return false
	}
	target := reply.Intent.Target
	for _, member := range reply.CurrentRoute.Serving.Replicas {
		if member.Member == target.Member && member.Node == target.Node && member.NodeIncarnation == target.NodeIncarnation &&
			member.StoreID == target.StoreID && member.Endpoint == string(target.Endpoint) &&
			member.NativeEndpoint == string(target.NativeEndpoint) && member.ControlEndpoint == string(target.ControlEndpoint) {
			return true
		}
	}
	return false
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
		raw[8] != bootstrapReadVersion || (raw[9] != bootstrapReadResponseSuccess && raw[9] != bootstrapReadResponseFailure) || raw[10] != 0 || raw[11] != 0 {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	payloadBytes := int(binary.BigEndian.Uint32(raw[28:32]))
	if payloadBytes == 0 || payloadBytes > MaxBootstrapReadReplyBytes || len(raw) != bootstrapReadResponseHeader+payloadBytes {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	if sha256.Sum256(raw[64:]) != [sha256.Size]byte(raw[32:64]) {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	if raw[9] == bootstrapReadResponseFailure {
		var reply BootstrapReadReply
		copy(reply.Nonce[:], raw[12:28])
		if payloadBytes > maxBootstrapReadErrorBytes {
			return reply, ErrBootstrapReadBound
		}
		return reply, fmt.Errorf("%w: gateway bootstrap read: %s", ErrBootstrapRead, raw[64:])
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
	if header[9] == bootstrapReadResponseFailure && payloadBytes > maxBootstrapReadErrorBytes {
		return BootstrapReadReply{}, ErrBootstrapReadBound
	}
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

type bootstrapRecoveryAuthority interface {
	ReadReplicatedCatalogHead(context.Context) (*gateway.Snapshot, replication.Digest, error)
}

// EnrollmentRecoveryReader returns the intent and its current placement from
// one verified catalog/directory cut, rather than inferring live ownership
// from the terminal enrollment row.
type EnrollmentRecoveryReader interface {
	ReadEnrollmentRecovery(context.Context, [32]byte) (BootstrapReadReply, error)
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
	fail := func(failure error) error {
		if deadlineErr := connection.SetWriteDeadline(bootstrapReadBoundedDeadline(ctx, service.writeDeadline())); deadlineErr != nil {
			return errors.Join(failure, deadlineErr)
		}
		return errors.Join(failure, writeBootstrapReadFailure(connection, request.Nonce, failure))
	}
	record, err := service.authority.ReadNode(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil {
		return fail(errors.Join(ErrBootstrapReadStale, err))
	}
	if record.Lifecycle == gateway.NodeDecommissioned {
		return fail(ErrBootstrapReadRetired)
	}
	if record.Lifecycle != gateway.NodeJoining && record.Lifecycle != gateway.NodeActive && record.Lifecycle != gateway.NodeDraining {
		return fail(ErrBootstrapReadStale)
	}
	if record.ServiceKeyDigest != replication.Digest(connection.PeerKeyDigest()) || !service.authorize(peer, record) ||
		service.authorizeAuthenticated != nil && !service.authorizeAuthenticated(rafttransport.Binding(connection), record) {
		return ErrBootstrapReadUnauthorized
	}
	reply, err := service.readStable(ctx, request, record)
	if err != nil {
		return fail(err)
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
		return BootstrapReadReply{}, fmt.Errorf("bootstrap initial directory cut: %w", errors.Join(ErrBootstrapReadStale, err))
	}
	intent, err := service.authority.ReadEnrollmentIntent(ctx, request.IntentID)
	// The authority's direct missing sentinel means a committed read found no
	// row. Joined/wrapped availability or stale errors are not absence proofs.
	intentMissing := request.Operation == OpReadOwnEnrollmentRecovery && err == gateway.ErrEnrollmentIntentMissing
	if err != nil && !intentMissing {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap enrollment lookup: %w", errors.Join(ErrBootstrapReadStale, err))
	}
	if intentMissing {
		intent = gateway.GroupEnrollmentIntent{}
	} else if !intent.Valid() || intent.State == gateway.EnrollmentCancelled || intent.IntentID != request.IntentID ||
		intent.Target.Node != request.PhysicalNode || intent.Target.NodeIncarnation != request.Incarnation ||
		intent.Group.ClusterID != service.trustDomain.ClusterID || intent.Group.ClusterIncarnation != service.trustDomain.ClusterIncarnation {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap requested target binding: %w", ErrBootstrapReadStale)
	}
	var currentRoute *gateway.ReplicatedMembershipRoute
	var currentNodes []gateway.NodeRecord
	var catalogGeneration uint64
	var catalogDigest replication.Digest
	if request.Operation == OpReadOwnEnrollmentRecovery {
		authority, ok := service.authority.(bootstrapRecoveryAuthority)
		if !ok {
			return BootstrapReadReply{}, ErrBootstrapReadUnavailable
		}
		snapshot, digest, readErr := authority.ReadReplicatedCatalogHead(ctx)
		if readErr != nil || snapshot == nil || digest == (replication.Digest{}) {
			return BootstrapReadReply{}, errors.Join(ErrBootstrapReadStale, readErr)
		}
		catalogGeneration, catalogDigest = snapshot.Generation(), digest
		route, found := snapshot.ResolveReplicatedMembershipRoute(intent.Distribution, intent.Shard, nil)
		if !intentMissing && found && route.Serving.Group == intent.Group && route.Serving.AllocationGeneration == uint64(intent.AllocationGeneration) {
			currentRoute = &route
			replicas := slices.Clone(route.Serving.Replicas)
			if route.HasEnrolledTarget {
				replicas = append(replicas, route.EnrolledTarget)
			}
			for _, replica := range replicas {
				found := false
				for _, node := range before.Nodes {
					if node.NodeID == replica.Node && node.Incarnation == replica.NodeIncarnation {
						currentNodes = append(currentNodes, node)
						found = true
						break
					}
				}
				if !found {
					return BootstrapReadReply{}, fmt.Errorf("bootstrap current member missing physical identity: %w", ErrBootstrapReadStale)
				}
			}
		}
	}
	evidence, err := service.authority.ScanNodeReferences(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap reference scan: %w", errors.Join(ErrBootstrapReadStale, err))
	}
	if evidence.NodeID != request.PhysicalNode || evidence.Incarnation != request.Incarnation ||
		evidence.DirectoryRevision != initial.Revision ||
		request.Operation == OpReadOwnEnrollmentRecovery && (evidence.CatalogGeneration != catalogGeneration || evidence.CatalogHeadDigest != catalogDigest) {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap reference scan binding: %w", ErrBootstrapReadStale)
	}
	after, err := service.authority.ReadNodeDirectoryCut(ctx)
	if err != nil || !after.Valid() || before.Revision != after.Revision || before.Digest != after.Digest ||
		evidence.DirectoryCutRevision != after.Revision || evidence.DirectoryCutDigest != after.Digest {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap directory cut changed during scan: %w", ErrBootstrapReadStale)
	}
	final, err := service.authority.ReadNode(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil || final != initial || final.Lifecycle == gateway.NodeDecommissioned {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap physical node changed during scan: %w", ErrBootstrapReadStale)
	}
	// The reference scan and the directory cut fence cover the global
	// enrollment directory, but the requested row is a separate bounded read.
	// Read it again immediately before publishing the reply.  A controller may
	// cancel, prepare, or complete the row while the scan is in flight; a reply
	// that combines the first row with the later witness would otherwise be a
	// mixed metadata cut.  The final cut below also detects a concurrent row
	// mutation that advanced the directory revision after the first scan.
	finalIntent, err := service.authority.ReadEnrollmentIntent(ctx, request.IntentID)
	if intentMissing {
		if err != gateway.ErrEnrollmentIntentMissing {
			return BootstrapReadReply{}, fmt.Errorf("bootstrap enrollment absence changed during scan: %w", errors.Join(ErrBootstrapReadStale, err))
		}
		finalIntent = gateway.GroupEnrollmentIntent{}
	} else if err != nil || !sameBootstrapReadIntent(finalIntent, intent) || !finalIntent.Valid() ||
		finalIntent.State == gateway.EnrollmentCancelled ||
		finalIntent.Target.Node != request.PhysicalNode ||
		finalIntent.Target.NodeIncarnation != request.Incarnation {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap enrollment changed during scan: %w", ErrBootstrapReadStale)
	}
	finalCut, err := service.authority.ReadNodeDirectoryCut(ctx)
	if err != nil || !finalCut.Valid() || finalCut.Revision != after.Revision || finalCut.Digest != after.Digest {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap final directory cut changed: %w", ErrBootstrapReadStale)
	}
	// Catalog publication and enrollment-directory updates do not necessarily
	// advance the physical-node directory cut. Repeating the bounded reference
	// scan closes that second race and ensures the catalog/enrollment witnesses
	// in the reply describe the same observed cut as the row re-read.
	verification, err := service.authority.ScanNodeReferences(ctx, request.PhysicalNode, request.Incarnation)
	if err != nil || verification != evidence {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap reference scan changed: %w", ErrBootstrapReadStale)
	}
	reply := BootstrapReadReply{
		Nonce: request.Nonce, Operation: request.Operation, PhysicalNode: request.PhysicalNode,
		Incarnation: request.Incarnation, IntentID: request.IntentID, Intent: finalIntent,
		IntentDigest: finalIntent.Digest(), Node: final, DirectoryCutRevision: finalCut.Revision,
		DirectoryCutDigest: finalCut.Digest, CatalogGeneration: evidence.CatalogGeneration,
		CatalogHeadDigest:         evidence.CatalogHeadDigest,
		EnrollmentDirectoryDigest: evidence.EnrollmentDirectoryDigest,
		CurrentRoute:              currentRoute,
		CurrentNodes:              currentNodes,
		IntentMissing:             intentMissing,
	}
	if intentMissing {
		reply.IntentDigest = replication.Digest{}
	}
	if !reply.valid() {
		return BootstrapReadReply{}, fmt.Errorf("bootstrap reply witness invalid: %w", ErrBootstrapReadStale)
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
	reply, err := client.read(ctx, intentID, OpReadOwnEnrollment)
	return reply.Intent, err
}

func (client *BootstrapReadClient) ReadEnrollmentRecovery(ctx context.Context, intentID [32]byte) (BootstrapReadReply, error) {
	return client.read(ctx, intentID, OpReadOwnEnrollmentRecovery)
}

func (client *BootstrapReadClient) read(ctx context.Context, intentID [32]byte, operation BootstrapReadOperation) (BootstrapReadReply, error) {
	if client == nil || ctx == nil || intentID == ([32]byte{}) {
		return BootstrapReadReply{}, ErrBootstrapRead
	}
	if cause := context.Cause(ctx); cause != nil {
		return BootstrapReadReply{}, cause
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
		reply, readErr := client.readOne(ctx, connection, seed, nonce, intentID, operation)
		if readErr == nil {
			return reply, nil
		}
		last = readErr
	}
	return BootstrapReadReply{}, last
}

func (client *BootstrapReadClient) readOne(
	ctx context.Context, connection rafttransport.PeerConnection, seed BootstrapGatewaySeed,
	nonce [bootstrapReadNonceBytes]byte, intentID [32]byte, operation BootstrapReadOperation,
) (BootstrapReadReply, error) {
	defer connection.Close()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl || peer.Node != seed.NodeID ||
		peer.TrustDomain != client.trustDomain || replication.Digest(connection.PeerKeyDigest()) != seed.SPKIPinDigest {
		return BootstrapReadReply{}, ErrBootstrapReadUnauthorized
	}
	request := BootstrapReadRequest{Nonce: nonce, Operation: operation,
		PhysicalNode: client.physicalNode, Incarnation: client.incarnation, IntentID: intentID}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := bootstrapReadBoundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return BootstrapReadReply{}, ErrBootstrapRead
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return BootstrapReadReply{}, err
	}
	if err := WriteBootstrapReadRequest(connection, request); err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	if deadline := bootstrapReadBoundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return BootstrapReadReply{}, ErrBootstrapReadOutcomeUnknown
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	reply, err := ReadBootstrapReadReply(connection)
	if reply.Nonce != ([bootstrapReadNonceBytes]byte{}) && reply.Nonce != nonce {
		return BootstrapReadReply{}, ErrBootstrapReadConflict
	}
	if err != nil {
		return BootstrapReadReply{}, errors.Join(ErrBootstrapReadOutcomeUnknown, err)
	}
	if reply.Nonce != nonce || reply.Operation != request.Operation || reply.PhysicalNode != request.PhysicalNode ||
		reply.Incarnation != request.Incarnation || reply.IntentID != intentID || !reply.valid() ||
		!reply.IntentMissing && (reply.Intent.Group.ClusterID != client.trustDomain.ClusterID || reply.Intent.Group.ClusterIncarnation != client.trustDomain.ClusterIncarnation) {
		return BootstrapReadReply{}, ErrBootstrapReadConflict
	}
	return reply, nil
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

func writeBootstrapReadFailure(writer io.Writer, nonce [bootstrapReadNonceBytes]byte, failure error) error {
	detail := strings.NewReplacer("\x00", "", "\r", " ", "\n", " ").Replace(failure.Error())
	if len(detail) > maxBootstrapReadErrorBytes {
		detail = detail[:maxBootstrapReadErrorBytes]
	}
	if detail == "" {
		detail = ErrBootstrapRead.Error()
	}
	var header [bootstrapReadResponseHeader]byte
	copy(header[:8], bootstrapReadResponseMagic[:])
	header[8] = bootstrapReadVersion
	header[9] = bootstrapReadResponseFailure
	copy(header[12:28], nonce[:])
	binary.BigEndian.PutUint32(header[28:32], uint32(len(detail)))
	digest := sha256.Sum256([]byte(detail))
	copy(header[32:64], digest[:])
	if err := bootstrapReadWriteFull(writer, header[:]); err != nil {
		return err
	}
	return bootstrapReadWriteFull(writer, []byte(detail))
}
