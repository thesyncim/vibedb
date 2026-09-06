package rafttransport

// This file contains the small authenticated control grammar used to publish
// one exact member-to-node enrollment on every current voter.  It deliberately
// does not import gateway or nodecontrol: those packages supply the committed
// intent verifier and the service-directory authorizer at their boundary.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

var (
	ErrEnrollmentControl             = errors.New("rafttransport: enrollment control failed")
	ErrEnrollmentControlUnauthorized = errors.New("rafttransport: enrollment control unauthorized")
	ErrEnrollmentControlOutcome      = errors.New("rafttransport: enrollment control outcome unknown")
)

const (
	enrollmentControlVersion byte = 1

	// The discriminator is consumed by shardcontrol.Mux and replayed to the
	// handler.  It is separate from nodecontrol, membership grants, and
	// snapshot bootstrap so a listener never guesses a request grammar.
	// requestHeaderBytes includes the discriminator, version/flags, endpoint
	// length, and every fixed intent coordinate. The endpoint bytes follow it.
	enrollmentRequestHeaderBytes = 8 + 1 + 1 + 2 + 2 +
		32 + 32 + 16 + 8 + 8 + 32 + 1 + 72 + 8 + 8 + 16 + 1 + 32 + 8
	enrollmentAckBytes = 8 + 1 + 1 + 2 + 32 + 72 + 8 + 8 + 16 + 32 + 32

	// EnrollmentControlMaxRequestBytes includes the bounded endpoint. It is
	// intentionally independent of any queue or process capacity setting.
	EnrollmentControlMaxRequestBytes = enrollmentRequestHeaderBytes + MaxPeerEndpointBytes
)

var (
	enrollmentRequestMagic = [8]byte{'V', 'B', 'E', 'N', 'R', 'O', 'L', 0}
	enrollmentAckMagic     = [8]byte{'V', 'B', 'E', 'N', 'A', 'C', 'K', 0}
)

// EnrollmentRequestDiscriminator identifies the fixed enrollment grammar for
// shardcontrol.Mux.
func EnrollmentRequestDiscriminator() [8]byte { return enrollmentRequestMagic }

// EnrollmentAck is the detached receipt returned after the registry has
// published the exact enrollment. A receipt is useful to a controller's
// durable fanout journal; an ACK is never itself a Raft authority grant.
type EnrollmentAck struct {
	IntentDigest        [32]byte
	Group               raftmember.GroupKey
	MemberID            uint64
	Node                NodeID
	DirectoryRevision   uint64
	PeerDirectoryDigest [32]byte
	RosterDigest        [32]byte
}

func (ack EnrollmentAck) valid() bool {
	return ack.IntentDigest != ([32]byte{}) &&
		ack.Group != (raftmember.GroupKey{}) && ack.MemberID != 0 &&
		ack.Node != (NodeID{}) && ack.DirectoryRevision != 0 &&
		ack.PeerDirectoryDigest != ([32]byte{}) && ack.RosterDigest != ([32]byte{})
}

// EnrollmentControlAuthorizer is supplied by the service owner. It must
// check the authenticated connection, including PeerKeyDigest, against the
// committed service directory and require that the caller is an authorized
// current voter for intent.Group. The callback runs before registry locks.
type EnrollmentControlAuthorizer func(
	context.Context,
	PeerConnection,
	EnrollmentIntent,
) error

// EnrollmentControlServiceOptions configures one fixed control endpoint.
// Verifier is the local committed-intent reader/certifier. Authorize is the
// service-directory and current-voter gate; neither endpoint strings nor a
// caller's claimed capacity are authority.
type EnrollmentControlServiceOptions struct {
	Registry *StaticRegistry
	// Transport is the preferred commit owner. When present, the handler
	// publishes the queue before the registry directory cut, preserving the
	// same atomic fence as OrdinaryTransport.EnrollMemberContext. Registry is
	// still retained for detached ACK construction and must be the transport's
	// registry when both are supplied.
	Transport     *OrdinaryTransport
	Verifier      EnrollmentVerifier
	Authorize     EnrollmentControlAuthorizer
	ReadDeadline  DeadlineFunc
	WriteDeadline DeadlineFunc
}

// EnrollmentControlService handles one request per authenticated stream.
// It does not retain connections or start goroutines.
type EnrollmentControlService struct {
	registry      *StaticRegistry
	transport     *OrdinaryTransport
	verifier      EnrollmentVerifier
	authorize     EnrollmentControlAuthorizer
	readDeadline  DeadlineFunc
	writeDeadline DeadlineFunc
}

func NewEnrollmentControlService(
	options EnrollmentControlServiceOptions,
) (*EnrollmentControlService, error) {
	registry := options.Registry
	if registry == nil && options.Transport != nil {
		registry = options.Transport.registry
	}
	if registry == nil || options.Verifier == nil || options.Authorize == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrEnrollmentControl
	}
	if options.Transport != nil && options.Transport.registry != registry {
		return nil, ErrEnrollmentControl
	}
	return &EnrollmentControlService{
		registry: registry, transport: options.Transport, verifier: options.Verifier,
		authorize: options.Authorize, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline,
	}, nil
}

// Serve reads and commits exactly one enrollment request. Remote authorization
// and catalog verification happen before StaticRegistry takes dynamicMu.
func (service *EnrollmentControlService) Serve(
	ctx context.Context,
	connection PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrEnrollmentControlUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := enrollmentControlDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrEnrollmentControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	intent, err := OpenEnrollmentRequest(connection)
	if err != nil {
		return err
	}
	if err = service.authorize(ctx, connection, intent); err != nil {
		return errors.Join(ErrEnrollmentControlUnauthorized, err)
	}
	if service.transport != nil {
		err = service.transport.EnrollMemberContext(ctx, intent, service.verifier)
	} else {
		err = service.registry.EnrollMemberContext(ctx, intent, service.verifier)
	}
	if err != nil {
		return err
	}
	ack, err := service.registry.EnrollmentAck(intent)
	if err != nil {
		return err
	}
	if deadline := enrollmentControlDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrEnrollmentControlOutcome
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return errors.Join(ErrEnrollmentControlOutcome, err)
	}
	if err = WriteEnrollmentAck(connection, ack); err != nil {
		return errors.Join(ErrEnrollmentControlOutcome, err)
	}
	return nil
}

// EnrollmentControlStreamOpener opens an authenticated shard-control stream.
// Implementations must derive the address from their current committed
// physical directory and authenticate the returned PeerConnection.
type EnrollmentControlStreamOpener interface {
	OpenShardControl(context.Context, NodeID) (PeerConnection, error)
}

// EnrollmentControlClient sends one intent to one voter and validates the
// exact ACK. It performs no retries internally; callers use the idempotent
// fanout replay path so retry policy remains tied to the durable intent.
type EnrollmentControlClient struct {
	opener        EnrollmentControlStreamOpener
	registry      *StaticRegistry
	readDeadline  DeadlineFunc
	writeDeadline DeadlineFunc
}

type EnrollmentControlClientOptions struct {
	Opener        EnrollmentControlStreamOpener
	Registry      *StaticRegistry
	ReadDeadline  DeadlineFunc
	WriteDeadline DeadlineFunc
}

func NewEnrollmentControlClient(
	options EnrollmentControlClientOptions,
) (*EnrollmentControlClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrEnrollmentControl
	}
	return &EnrollmentControlClient{
		opener: options.Opener, registry: options.Registry,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
	}, nil
}

// EnrollMember sends one exact committed intent to target. The target's
// handler performs the local verifier read and queue-before-directory commit.
func (client *EnrollmentControlClient) EnrollMember(
	ctx context.Context,
	target NodeID,
	intent EnrollmentIntent,
) (EnrollmentAck, error) {
	if client == nil || ctx == nil || target == (NodeID{}) ||
		!validEnrollmentWireIntent(intent) {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	connection, err := client.opener.OpenShardControl(ctx, target)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return EnrollmentAck{}, err
	}
	if connection == nil {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	defer connection.Close()
	identity := connection.PeerIdentity()
	if connection.TrafficClass() != TrafficShardControl || identity.Node != target ||
		identity.TrustDomain != intent.Domain {
		return EnrollmentAck{}, ErrEnrollmentControlUnauthorized
	}
	if client.registry != nil {
		if err := client.registry.VerifyPeerConnectionBinding(connection); err != nil {
			return EnrollmentAck{}, errors.Join(ErrEnrollmentControlUnauthorized, err)
		}
	}
	if deadline := enrollmentControlDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return EnrollmentAck{}, ErrEnrollmentControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return EnrollmentAck{}, err
	}
	if err = WriteEnrollmentRequest(connection, intent); err != nil {
		return EnrollmentAck{}, errors.Join(ErrEnrollmentControlOutcome, err)
	}
	if deadline := enrollmentControlDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return EnrollmentAck{}, ErrEnrollmentControlOutcome
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return EnrollmentAck{}, errors.Join(ErrEnrollmentControlOutcome, err)
	}
	ack, err := OpenEnrollmentAck(connection)
	if err != nil {
		return EnrollmentAck{}, errors.Join(ErrEnrollmentControlOutcome, err)
	}
	if !ack.valid() || ack.IntentDigest != intent.Digest || ack.Group != intent.Group ||
		ack.MemberID != intent.Member.MemberID || ack.Node != intent.Peer.NodeID ||
		ack.DirectoryRevision < intent.DirectoryRevision {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	return ack, nil
}

// ReplayEnrollment is an explicit restart spelling. StaticRegistry accepts an
// exact already-published intent idempotently, while a changed intent or a
// stale directory revision is rejected.
func (client *EnrollmentControlClient) ReplayEnrollment(
	ctx context.Context,
	target NodeID,
	intent EnrollmentIntent,
) (EnrollmentAck, error) {
	return client.EnrollMember(ctx, target, intent)
}

// EnrollmentFanoutTarget binds a current voter identity to one local or
// remote enrollment operation. The caller must derive the target list from
// the exact committed ConfState; this type never turns the list into
// authority by itself.
type EnrollmentFanoutTarget struct {
	Node   NodeID
	Enroll func(context.Context, EnrollmentIntent) (EnrollmentAck, error)
}

// EnrollmentFanoutResult is bounded by MaxConfStateMembers and is suitable
// for a durable controller journal. Pending includes the failed voter and all
// later voters, so a restart can call Replay with the same exact intent.
type EnrollmentFanoutResult struct {
	IntentDigest [32]byte
	Group        raftmember.GroupKey
	MemberID     uint64
	RosterDigest [32]byte
	Acks         []EnrollmentAck
	Pending      []NodeID
}

// EnrollmentFanout serializes the bounded voter calls. Sequential calls keep
// network I/O outside registry locks and avoid one goroutine per voter on
// repeated churn. Each target operation is idempotent by intent digest.
type EnrollmentFanout struct {
	targets []EnrollmentFanoutTarget
}

func NewEnrollmentFanout(targets []EnrollmentFanoutTarget) (*EnrollmentFanout, error) {
	if len(targets) == 0 || len(targets) > raftmodel.MaxConfStateMembers {
		return nil, ErrEnrollmentControl
	}
	owned := slices.Clone(targets)
	slices.SortFunc(owned, func(left, right EnrollmentFanoutTarget) int {
		return slices.Compare(left.Node[:], right.Node[:])
	})
	for index, target := range owned {
		if target.Node == (NodeID{}) || target.Enroll == nil ||
			(index > 0 && owned[index-1].Node == target.Node) {
			return nil, ErrEnrollmentControl
		}
	}
	return &EnrollmentFanout{targets: owned}, nil
}

// Enroll calls every configured current voter in deterministic NodeID order.
// A failed call returns the partial ACKs and a replayable pending suffix.
func (fanout *EnrollmentFanout) Enroll(
	ctx context.Context,
	intent EnrollmentIntent,
) (EnrollmentFanoutResult, error) {
	return fanout.enroll(ctx, intent)
}

// Replay retries the same exact intent after a crash or partial fanout. It is
// intentionally the same idempotent operation as Enroll.
func (fanout *EnrollmentFanout) Replay(
	ctx context.Context,
	intent EnrollmentIntent,
) (EnrollmentFanoutResult, error) {
	return fanout.enroll(ctx, intent)
}

func (fanout *EnrollmentFanout) enroll(
	ctx context.Context,
	intent EnrollmentIntent,
) (EnrollmentFanoutResult, error) {
	if fanout == nil || ctx == nil {
		return EnrollmentFanoutResult{}, ErrEnrollmentControl
	}
	canonical, err := canonicalEnrollmentIntent(intent)
	if err != nil {
		return EnrollmentFanoutResult{}, err
	}
	intent = canonical
	result := EnrollmentFanoutResult{
		IntentDigest: intent.Digest, Group: intent.Group,
		MemberID: intent.Member.MemberID,
		Acks:     make([]EnrollmentAck, 0, len(fanout.targets)),
		Pending:  make([]NodeID, 0, len(fanout.targets)),
	}
	for index, target := range fanout.targets {
		if err := context.Cause(ctx); err != nil {
			result.Pending = append(result.Pending, targetNodes(fanout.targets[index:])...)
			return result, err
		}
		ack, err := target.Enroll(ctx, intent)
		if err != nil {
			result.Pending = append(result.Pending, targetNodes(fanout.targets[index:])...)
			return result, fmt.Errorf("%w: voter %x: %w", ErrEnrollmentControl, target.Node, err)
		}
		if !ack.valid() || ack.IntentDigest != intent.Digest || ack.Group != intent.Group ||
			ack.MemberID != intent.Member.MemberID || ack.Node != intent.Peer.NodeID {
			result.Pending = append(result.Pending, targetNodes(fanout.targets[index:])...)
			return result, ErrEnrollmentControl
		}
		if index == 0 {
			result.RosterDigest = ack.RosterDigest
		} else if ack.RosterDigest != result.RosterDigest {
			result.Pending = append(result.Pending, targetNodes(fanout.targets[index:])...)
			return result, ErrEnrollmentControl
		}
		result.Acks = append(result.Acks, ack)
	}
	return result, nil
}

func targetNodes(targets []EnrollmentFanoutTarget) []NodeID {
	result := make([]NodeID, len(targets))
	for index := range targets {
		result[index] = targets[index].Node
	}
	return result
}

// EnrollmentAck returns a receipt for a locally applied intent. It lets a
// fanout include the local voter using the same ACK and replay semantics as a
// network client.
func (registry *StaticRegistry) EnrollmentAck(intent EnrollmentIntent) (EnrollmentAck, error) {
	if registry == nil || !validEnrollmentWireIntent(intent) ||
		intent.Domain != registry.TrustDomain() {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	member, err := registry.Member(intent.Group, intent.Peer.NodeID)
	if err != nil || member != intent.Member.MemberID {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	record, ok := registry.memberRecord(
		intent.Group, intent.Member.MemberID, registry.dynamic.Load(),
	)
	if !ok || record.node != intent.Peer.NodeID || record.enrollmentDigest != intent.Digest {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	peer, err := registry.PhysicalPeer(intent.Peer.NodeID)
	if err != nil || peer.State != PeerEnrolled {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	roster, ok := registry.rosterDigest(intent.Group)
	if !ok {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	ack := EnrollmentAck{
		IntentDigest: intent.Digest, Group: intent.Group,
		MemberID: intent.Member.MemberID, Node: intent.Peer.NodeID,
		DirectoryRevision:   registry.PeerDirectoryRevision(),
		PeerDirectoryDigest: registry.PeerDirectoryDigest(), RosterDigest: roster,
	}
	if !ack.valid() {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	return ack, nil
}

// WriteEnrollmentRequest writes the complete fixed grammar. The caller owns
// the authenticated stream and must set its deadline first.
func WriteEnrollmentRequest(writer io.Writer, intent EnrollmentIntent) error {
	raw, err := AppendEnrollmentRequest(nil, intent)
	if err != nil {
		return err
	}
	return writeFull(writer, raw)
}

func AppendEnrollmentRequest(dst []byte, intent EnrollmentIntent) ([]byte, error) {
	canonical, err := canonicalEnrollmentIntent(intent)
	if err != nil || len(canonical.Peer.Endpoint) > MaxPeerEndpointBytes {
		return dst, ErrEnrollmentControl
	}
	intent = canonical
	endpoint := intent.Peer.Endpoint
	if len(dst) > math.MaxInt-enrollmentRequestHeaderBytes-len(endpoint) {
		return dst, ErrEnrollmentControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, enrollmentRequestHeaderBytes+len(endpoint))...)
	b := dst[start:]
	copy(b[0:8], enrollmentRequestMagic[:])
	b[8] = enrollmentControlVersion
	// b[9] is flags; b[10:12] are reserved and must remain zero.
	binary.BigEndian.PutUint16(b[12:14], uint16(len(endpoint)))
	offset := 14
	copy(b[offset:offset+32], intent.Digest[:])
	offset += 32
	copy(b[offset:offset+16], intent.Domain.ClusterID[:])
	offset += 16
	copy(b[offset:offset+16], intent.Domain.ClusterIncarnation[:])
	offset += 16
	copy(b[offset:offset+16], intent.Peer.NodeID[:])
	offset += 16
	binary.BigEndian.PutUint64(b[offset:offset+8], intent.Peer.Incarnation)
	offset += 8
	binary.BigEndian.PutUint64(b[offset:offset+8], intent.Peer.Revision)
	offset += 8
	copy(b[offset:offset+32], intent.Peer.ServiceKeyDigest[:])
	offset += 32
	b[offset] = byte(intent.Peer.State)
	offset++
	appendGroupKey(b[offset:offset+72], intent.Group)
	offset += 72
	binary.BigEndian.PutUint64(b[offset:offset+8], intent.Member.ReplicaSetVersion)
	offset += 8
	binary.BigEndian.PutUint64(b[offset:offset+8], intent.Member.MemberID)
	offset += 8
	copy(b[offset:offset+16], intent.Member.Node[:])
	offset += 16
	b[offset] = byte(intent.Member.Role)
	offset++
	copy(b[offset:offset+32], intent.ExpectedRosterDigest[:])
	offset += 32
	binary.BigEndian.PutUint64(b[offset:offset+8], intent.DirectoryRevision)
	offset += 8
	copy(b[offset:], endpoint)
	return dst, nil
}

// OpenEnrollmentRequest reads a complete bounded request from a connection or
// any io.Reader. The endpoint is discovery data; the target verifier remains
// the only authority for accepting it.
func OpenEnrollmentRequest(reader io.Reader) (EnrollmentIntent, error) {
	if reader == nil {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	header := make([]byte, enrollmentRequestHeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return EnrollmentIntent{}, errors.Join(ErrEnrollmentControl, err)
	}
	if !equalBytes(header[:8], enrollmentRequestMagic[:]) || header[8] != enrollmentControlVersion ||
		header[9] != 0 || binary.BigEndian.Uint16(header[10:12]) != 0 {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	endpointBytes := int(binary.BigEndian.Uint16(header[12:14]))
	if endpointBytes == 0 || endpointBytes > MaxPeerEndpointBytes {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	endpoint := make([]byte, endpointBytes)
	if _, err := io.ReadFull(reader, endpoint); err != nil {
		return EnrollmentIntent{}, errors.Join(ErrEnrollmentControl, err)
	}
	var intent EnrollmentIntent
	offset := 14
	copy(intent.Digest[:], header[offset:offset+32])
	offset += 32
	copy(intent.Domain.ClusterID[:], header[offset:offset+16])
	offset += 16
	copy(intent.Domain.ClusterIncarnation[:], header[offset:offset+16])
	offset += 16
	copy(intent.Peer.NodeID[:], header[offset:offset+16])
	intent.Peer.Node = intent.Peer.NodeID
	offset += 16
	intent.Peer.Incarnation = binary.BigEndian.Uint64(header[offset : offset+8])
	offset += 8
	intent.Peer.Revision = binary.BigEndian.Uint64(header[offset : offset+8])
	offset += 8
	copy(intent.Peer.ServiceKeyDigest[:], header[offset:offset+32])
	offset += 32
	intent.Peer.State = PeerState(header[offset])
	offset++
	intent.Group = openGroupKey(header[offset : offset+72])
	offset += 72
	intent.Member = Member{Group: intent.Group}
	intent.Member.ReplicaSetVersion = binary.BigEndian.Uint64(header[offset : offset+8])
	offset += 8
	intent.Member.MemberID = binary.BigEndian.Uint64(header[offset : offset+8])
	offset += 8
	copy(intent.Member.Node[:], header[offset:offset+16])
	offset += 16
	intent.Member.Role = MemberRole(header[offset])
	offset++
	copy(intent.ExpectedRosterDigest[:], header[offset:offset+32])
	offset += 32
	intent.DirectoryRevision = binary.BigEndian.Uint64(header[offset : offset+8])
	intent.Peer.TrustDomain = intent.Domain
	intent.Peer.Endpoint = string(endpoint)
	intent.Peer.Address = intent.Peer.Endpoint
	intent.Peer.EnrollmentDigest = intent.Digest
	if !validEnrollmentWireIntent(intent) {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	return intent, nil
}

// WriteEnrollmentAck writes a complete fixed receipt.
func WriteEnrollmentAck(writer io.Writer, ack EnrollmentAck) error {
	if writer == nil || !ack.valid() {
		return ErrEnrollmentControl
	}
	raw := make([]byte, enrollmentAckBytes)
	copy(raw[:8], enrollmentAckMagic[:])
	raw[8] = enrollmentControlVersion
	// raw[9:12] remain zero.
	offset := 12
	copy(raw[offset:offset+32], ack.IntentDigest[:])
	offset += 32
	appendGroupKey(raw[offset:offset+72], ack.Group)
	offset += 72
	binary.BigEndian.PutUint64(raw[offset:offset+8], ack.MemberID)
	offset += 8
	binary.BigEndian.PutUint64(raw[offset:offset+8], ack.DirectoryRevision)
	offset += 8
	copy(raw[offset:offset+16], ack.Node[:])
	offset += 16
	copy(raw[offset:offset+32], ack.PeerDirectoryDigest[:])
	offset += 32
	copy(raw[offset:offset+32], ack.RosterDigest[:])
	return writeFull(writer, raw)
}

func OpenEnrollmentAck(reader io.Reader) (EnrollmentAck, error) {
	if reader == nil {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	raw := make([]byte, enrollmentAckBytes)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return EnrollmentAck{}, errors.Join(ErrEnrollmentControl, err)
	}
	if !equalBytes(raw[:8], enrollmentAckMagic[:]) || raw[8] != enrollmentControlVersion ||
		raw[9] != 0 || raw[10] != 0 || raw[11] != 0 {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	var ack EnrollmentAck
	offset := 12
	copy(ack.IntentDigest[:], raw[offset:offset+32])
	offset += 32
	ack.Group = openGroupKey(raw[offset : offset+72])
	offset += 72
	ack.MemberID = binary.BigEndian.Uint64(raw[offset : offset+8])
	offset += 8
	ack.DirectoryRevision = binary.BigEndian.Uint64(raw[offset : offset+8])
	offset += 8
	copy(ack.Node[:], raw[offset:offset+16])
	offset += 16
	copy(ack.PeerDirectoryDigest[:], raw[offset:offset+32])
	offset += 32
	copy(ack.RosterDigest[:], raw[offset:offset+32])
	if !ack.valid() {
		return EnrollmentAck{}, ErrEnrollmentControl
	}
	return ack, nil
}

func validEnrollmentWireIntent(intent EnrollmentIntent) bool {
	_, err := canonicalEnrollmentIntent(intent)
	return err == nil
}

func canonicalEnrollmentIntent(intent EnrollmentIntent) (EnrollmentIntent, error) {
	if intent.Digest == ([32]byte{}) || !validTrustDomain(intent.Domain) ||
		intent.DirectoryRevision == 0 || intent.Group == (raftmember.GroupKey{}) ||
		intent.ExpectedRosterDigest == ([32]byte{}) {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	peer, err := intent.Peer.normalized(intent.Domain)
	if err != nil || peer.NodeID == (NodeID{}) || peer.ServiceKeyDigest == ([32]byte{}) ||
		peer.State != PeerEnrolled || peer.Endpoint == "" || peer.Address != peer.Endpoint {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	if intent.Member.Group != intent.Group || intent.Member.Node != peer.NodeID ||
		intent.Member.Role != MemberEnrolled {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	if err := validateMember(intent.Member); err != nil {
		return EnrollmentIntent{}, ErrEnrollmentControl
	}
	peer.EnrollmentDigest = intent.Digest
	intent.Peer = peer
	return intent, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func enrollmentControlDeadline(ctx context.Context, requested time.Time) time.Time {
	if requested.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(requested) {
		return deadline
	}
	return requested
}
