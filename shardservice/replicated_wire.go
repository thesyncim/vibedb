package shardservice

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const (
	replicatedWireVersion          = 1
	tagReplicatedRequest           = 'P'
	tagReplicatedMembershipRequest = 'M'
	tagReplicatedTransactionRead   = 'T'
	tagReplicatedResponse          = 'A'
	// A response with state has a 297-byte body before a completion. The fixed
	// request digest is zero for nonterminal responses and binds terminal
	// proposal outcomes to the exact canonical command. Read
	// responses add an applied index and value length. These exact ceilings let
	// the client reject a hostile frame header before allocating its body.
	replicatedResponseFixedBodyBytes     = 297
	replicatedReadResponseFixedBodyBytes = replicatedResponseFixedBodyBytes + 12
)

// Membership is fixed-width: the original 279-byte control body plus the
// forwarded 16-byte node identity, 8-byte authorization generation, and the
// 8-byte exact requested capability.
const replicatedMembershipRequestBodyBytes = 311

// Transaction recovery reads are fixed-width: the common 242-byte native
// prefix followed by a one-byte closed read kind, one exact transaction ID,
// an applied floor, a manifest-page ordinal, a scan item limit, and a response
// byte ceiling. For a scan ID is the exclusive after-ID cursor. The dedicated
// tag lets the decoder reject a hostile frame length before allocating its body.
const replicatedTransactionReadRequestBodyBytes = 277

const (
	MaxReplicatedTransactionReadBytes = replicatedstate.MaxTransactionRecoveryReadBytes
	MaxReplicatedTransactionScanItems = replicatedstate.MaxTransactionRecoveryScanRows
	MaxReplicatedTransactionScanBytes = replicatedstate.MaxTransactionRecoveryScanBytes
)

var ErrReplicatedWire = errors.New("shardservice: invalid replicated native frame")

// ReplicatedOperation is the closed byte-native serving operation set. SQL is
// deliberately absent: replicated mode accepts only canonical commands.
type ReplicatedOperation uint8

const (
	ReplicatedProbe ReplicatedOperation = iota + 1
	ReplicatedPropose
	ReplicatedMembership
	ReplicatedReadLeader
	ReplicatedReadFollower
	ReplicatedTransactionRead
)

// ReplicatedTransactionReadKind is the complete RF3 recovery-read surface.
// These operations inspect only the replicated hidden system collection; they
// never consult the legacy process-local transaction journal.
type ReplicatedTransactionReadKind uint8

const (
	ReplicatedTransactionLookupCoordinator ReplicatedTransactionReadKind = iota + 1
	ReplicatedTransactionLookupParticipant
	ReplicatedTransactionReadManifestPage
	ReplicatedTransactionScanCoordinators
)

// ReplicatedTransactionReadRequest is fixed-size on the wire. ID is the exact
// identity for point/page reads and the exclusive after-ID cursor for an
// ordered coordinator scan. MaxRows is exactly one for a point/page read and
// explicitly bounds a scan. MaxBytes bounds the detached canonical response.
type ReplicatedTransactionReadRequest struct {
	Kind           ReplicatedTransactionReadKind
	ID             distributedtxn.ID
	MinimumApplied uint64
	SegmentIndex   uint32
	MaxRows        uint16
	MaxBytes       uint32
}

// ReplicatedFence identifies one exact live Runtime and leadership term. Probe
// requires only Group and AllocationGeneration; Propose requires every field.
type ReplicatedFence struct {
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              raftservice.CommandFence
	MemberID             uint64
	StoreID              [16]byte
	NodeIncarnation      uint64
	Term                 uint64
}

// ReplicatedRequest carries an exact canonical command or asks for a live
// serving handshake. Command aliases the decoded frame and is capacity-clamped.
type ReplicatedRequest struct {
	Operation       ReplicatedOperation
	Authority       serviceauthz.Authority
	Capability      serviceauthz.Capability
	Fence           ReplicatedFence
	Command         []byte
	Membership      ReplicatedMembershipRequest
	Relation        replication.RelationID
	Key             []byte
	MinimumApplied  uint64
	MaxValueBytes   uint32
	TransactionRead ReplicatedTransactionReadRequest
}

// ReplicatedMembershipRequest is a fixed-width control envelope. It contains
// no protobuf and no peer-sized repeated field.
type ReplicatedMembershipRequest struct {
	Kind                      raftservice.MembershipKind
	TransitionID              [16]byte
	MetadataEpoch             uint64
	CatalogGeneration         uint64
	ExpectedReplicaSetVersion uint64
	SourceMember              uint64
	TargetMember              uint64
	TransferTerm              uint64
}

// ReplicatedResponseKind separates definite pre-admission refusals from an
// admitted outcome whose terminal result could not be returned.
type ReplicatedResponseKind uint8

const (
	ReplicatedHandshake ReplicatedResponseKind = iota + 1
	ReplicatedCompletion
	ReplicatedNotLeader
	ReplicatedOutcomeUnknown
	ReplicatedRefusal
	ReplicatedMembershipAccepted
	ReplicatedReadFound
	ReplicatedReadMissing
	ReplicatedTransactionReadResult
)

// ReplicatedRefusalCode is a closed diagnostic class. Deterministic state-
// machine refusals retain their exact raftserve Outcome in Outcome.
type ReplicatedRefusalCode uint8

const (
	ReplicatedRefusalNone ReplicatedRefusalCode = iota
	ReplicatedRefusalStaleFence
	ReplicatedRefusalAdmissionBound
	ReplicatedRefusalProposalRefused
	ReplicatedRefusalDeterministic
	ReplicatedRefusalUnavailable
	ReplicatedRefusalMembershipUnauthorized
	ReplicatedRefusalMembershipStale
	ReplicatedRefusalMembershipMalformed
	ReplicatedRefusalMembershipNotCaughtUp
	ReplicatedRefusalReadBehind
	ReplicatedRefusalReadBufferBound
	ReplicatedRefusalUnauthorized
	ReplicatedRefusalTransactionReadMalformed
)

// ReplicatedMemberState is the fixed-width handshake and leader hint returned
// on every valid native request. It contains no formatted identifiers.
type ReplicatedMemberState struct {
	Fence             ReplicatedFence
	LeaderID          uint64
	Commit            uint64
	Applied           uint64
	CheckpointApplied uint64
}

// ReplicatedResponse owns Completion and returns only typed fixed-width error
// classes. RequestDigest is SHA-256 over the exact canonical proposal and is
// required only for a completion or applied deterministic refusal. It is zero
// for probes, reads, retryable outcomes, and pre-admission refusals. No remote
// diagnostic string is admitted to the hot wire.
type ReplicatedResponse struct {
	Kind          ReplicatedResponseKind
	Refusal       ReplicatedRefusalCode
	HasState      bool
	State         ReplicatedMemberState
	Outcome       raftserve.Outcome
	RequestDigest [sha256.Size]byte
	Completion    []byte
	ReadApplied   uint64
	Value         []byte
	readLease     interface{ Release() }
}

// EncodeReplicatedRequest emits one canonical native request frame.
func EncodeReplicatedRequest(w io.Writer, request *ReplicatedRequest) error {
	if w == nil || !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	payloadHint := len(request.Command) + len(request.Key) + 16
	if request.Operation == ReplicatedMembership {
		payloadHint += 65
	}
	e := newFrameEncoder(payloadHint)
	e.u8(replicatedWireVersion)
	e.u8(uint8(request.Operation))
	e.b = append(e.b, request.Authority.Node[:]...)
	e.u64(request.Authority.Generation)
	e.u64(uint64(request.Capability))
	encodeReplicatedFence(&e, request.Fence)
	switch request.Operation {
	case ReplicatedPropose:
		e.bytes(request.Command)
	case ReplicatedMembership:
		e.bytes(request.Command)
		encodeReplicatedMembership(&e, request.Membership)
	case ReplicatedReadLeader, ReplicatedReadFollower:
		e.u8(uint8(request.Relation))
		e.u64(request.MinimumApplied)
		e.u32(request.MaxValueBytes)
		e.bytes(request.Key)
	case ReplicatedTransactionRead:
		encodeReplicatedTransactionRead(&e, request.TransactionRead)
	}
	if e.err != nil {
		return e.err
	}
	tag := byte(tagReplicatedRequest)
	if request.Operation == ReplicatedMembership {
		tag = tagReplicatedMembershipRequest
	} else if request.Operation == ReplicatedTransactionRead {
		tag = tagReplicatedTransactionRead
	}
	return writeEncodedFrame(w, tag, e.b)
}

// EncodeReplicatedRequestBorrowed emits the fixed request prefix and borrows
// the immutable command or point key as a second buffer, avoiding a payload-sized
// userspace copy on every retry. A TLS stream may encode the two writes as
// separate record sequences; this function does not claim writev.
func EncodeReplicatedRequestBorrowed(w io.Writer, request *ReplicatedRequest) error {
	if w == nil || !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	payloadHint := 0
	if request.Operation == ReplicatedMembership {
		payloadHint = 65
	}
	e := newFrameEncoder(payloadHint)
	e.u8(replicatedWireVersion)
	e.u8(uint8(request.Operation))
	e.b = append(e.b, request.Authority.Node[:]...)
	e.u64(request.Authority.Generation)
	e.u64(uint64(request.Capability))
	encodeReplicatedFence(&e, request.Fence)
	var payload []byte
	tag := byte(tagReplicatedRequest)
	switch request.Operation {
	case ReplicatedProbe:
	case ReplicatedPropose:
		payload = request.Command
		e.u32(uint32(len(payload)))
	case ReplicatedMembership:
		e.u32(0)
		encodeReplicatedMembership(&e, request.Membership)
		tag = tagReplicatedMembershipRequest
	case ReplicatedReadLeader, ReplicatedReadFollower:
		e.u8(uint8(request.Relation))
		e.u64(request.MinimumApplied)
		e.u32(request.MaxValueBytes)
		payload = request.Key
		e.u32(uint32(len(payload)))
	case ReplicatedTransactionRead:
		encodeReplicatedTransactionRead(&e, request.TransactionRead)
		tag = tagReplicatedTransactionRead
	}
	if e.err != nil || len(e.b)+len(payload)-5 > maxFrameBody {
		return errFrameTooLarge
	}
	e.b[0] = tag
	binary.BigEndian.PutUint32(e.b[1:5], uint32(len(e.b)+len(payload)-1))
	buffers := net.Buffers{e.b}
	if len(payload) != 0 {
		buffers = append(buffers, payload)
	}
	written, err := buffers.WriteTo(w)
	if err == nil && written != int64(len(e.b)+len(payload)) {
		return io.ErrShortWrite
	}
	return err
}

// DecodeReplicatedRequest decodes and validates one bounded native request.
func DecodeReplicatedRequest(r io.Reader) (*ReplicatedRequest, error) {
	request, _, err := decodeReplicatedRequest(r, nil)
	return request, err
}

func decodeReplicatedRequest(
	r io.Reader,
	budget *replicatedFrameByteBudget,
) (*ReplicatedRequest, int64, error) {
	body, charged, tag, err := readReplicatedRequestFrame(r, budget)
	if err != nil {
		return nil, 0, err
	}
	d := deccur{b: body}
	if d.u8() != replicatedWireVersion {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, errBadVersion
	}
	request := &ReplicatedRequest{Operation: ReplicatedOperation(d.u8())}
	if !replicatedRequestTagMatches(request.Operation, tag) {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, ErrReplicatedWire
	}
	request.Authority.Node = d.fixed16()
	request.Authority.Generation = d.u64()
	request.Capability = serviceauthz.Capability(d.u64())
	request.Fence = decodeReplicatedFence(&d)
	switch request.Operation {
	case ReplicatedPropose:
		request.Command = d.slice()
	case ReplicatedMembership:
		request.Command = d.slice()
		request.Membership = decodeReplicatedMembership(&d)
	case ReplicatedReadLeader, ReplicatedReadFollower:
		request.Relation = replication.RelationID(d.u8())
		request.MinimumApplied = d.u64()
		request.MaxValueBytes = d.u32()
		request.Key = d.slice()
	case ReplicatedTransactionRead:
		request.TransactionRead = decodeReplicatedTransactionRead(&d)
	}
	if err := d.end(); err != nil {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, err
	}
	request.Command = request.Command[:len(request.Command):len(request.Command)]
	request.Key = request.Key[:len(request.Key):len(request.Key)]
	if !validReplicatedRequest(request) {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, ErrReplicatedWire
	}
	return request, charged, nil
}

// readReplicatedRequestFrame validates the tag and the membership operation's
// exact fixed body size from a stack header before any peer-sized allocation.
func readReplicatedRequestFrame(
	r io.Reader,
	budget *replicatedFrameByteBudget,
) (body []byte, charged int64, tag byte, err error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, 0, err
	}
	tag = header[0]
	length := int32(binary.BigEndian.Uint32(header[1:]))
	if length < 4 {
		return nil, 0, tag, errBadLength
	}
	size := int(length) - 4
	switch tag {
	case tagReplicatedMembershipRequest:
		if size != replicatedMembershipRequestBodyBytes {
			return nil, 0, tag, ErrReplicatedWire
		}
		var prefix [2]byte
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			return nil, 0, tag, err
		}
		if prefix[0] != replicatedWireVersion ||
			ReplicatedOperation(prefix[1]) != ReplicatedMembership {
			return nil, 0, tag, ErrReplicatedWire
		}
		charged = int64(size)
		if budget != nil && !budget.reserve(charged) {
			return nil, 0, tag, errFrameBudget
		}
		body = make([]byte, size)
		copy(body[:2], prefix[:])
		if _, err := io.ReadFull(r, body[2:]); err != nil {
			if budget != nil {
				budget.release(charged)
			}
			return nil, 0, tag, err
		}
		return body, charged, tag, nil
	case tagReplicatedTransactionRead:
		if size != replicatedTransactionReadRequestBodyBytes {
			return nil, 0, tag, ErrReplicatedWire
		}
		var prefix [2]byte
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			return nil, 0, tag, err
		}
		if prefix[0] != replicatedWireVersion ||
			ReplicatedOperation(prefix[1]) != ReplicatedTransactionRead {
			return nil, 0, tag, ErrReplicatedWire
		}
		charged = int64(size)
		if budget != nil && !budget.reserve(charged) {
			return nil, 0, tag, errFrameBudget
		}
		body = make([]byte, size)
		copy(body[:2], prefix[:])
		if _, err := io.ReadFull(r, body[2:]); err != nil {
			if budget != nil {
				budget.release(charged)
			}
			return nil, 0, tag, err
		}
		return body, charged, tag, nil
	case tagReplicatedRequest:
		if size > maxFrameBody {
			return nil, 0, tag, errFrameTooLarge
		}
	default:
		return nil, 0, tag, errBadTag
	}
	if size == 0 {
		return nil, 0, tag, nil
	}
	charged = int64(size)
	if budget != nil && !budget.reserve(charged) {
		return nil, 0, tag, errFrameBudget
	}
	body = make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, tag, err
	}
	return body, charged, tag, nil
}

// EncodeReplicatedResponse emits one canonical typed native response.
func EncodeReplicatedResponse(w io.Writer, response *ReplicatedResponse) error {
	if w == nil || !validReplicatedResponse(response) {
		return ErrReplicatedWire
	}
	bodyHint := replicatedResponseFixedBodyBytes + len(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing ||
		response.Kind == ReplicatedTransactionReadResult {
		bodyHint = replicatedReadResponseFixedBodyBytes + len(response.Value)
	}
	e := encbuf{b: make([]byte, 5, 5+bodyHint)}
	e.u8(replicatedWireVersion)
	e.u8(uint8(response.Kind))
	e.u8(uint8(response.Refusal))
	e.u8(uint8(response.Outcome.Code))
	if response.HasState {
		e.u8(1)
		encodeReplicatedMemberState(&e, response.State)
	} else {
		e.u8(0)
	}
	e.u64(response.Outcome.AppliedIndex)
	e.u64(response.Outcome.CompletionAppliedSequence)
	encodeReplicatedDigest(&e, response.RequestDigest)
	e.bytes(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing ||
		response.Kind == ReplicatedTransactionReadResult {
		e.u64(response.ReadApplied)
		e.bytes(response.Value)
	}
	if e.err != nil {
		return e.err
	}
	return writeEncodedFrame(w, tagReplicatedResponse, e.b)
}

// DecodeReplicatedResponse decodes and validates one bounded native response.
func DecodeReplicatedResponse(r io.Reader) (*ReplicatedResponse, error) {
	return decodeReplicatedResponseLimit(r, maxFrameBody)
}

func decodeReplicatedResponseLimit(r io.Reader, maxBody int) (*ReplicatedResponse, error) {
	body, _, err := readFrameBudgetedLimit(r, tagReplicatedResponse, nil, maxBody)
	if err != nil {
		return nil, err
	}
	d := deccur{b: body}
	if d.u8() != replicatedWireVersion {
		return nil, errBadVersion
	}
	response := &ReplicatedResponse{
		Kind:    ReplicatedResponseKind(d.u8()),
		Refusal: ReplicatedRefusalCode(d.u8()),
	}
	response.Outcome.Code = raftserve.OutcomeCode(d.u8())
	stateMarker := d.u8()
	if stateMarker > 1 {
		return nil, errBadPresence
	}
	response.HasState = stateMarker == 1
	if response.HasState {
		response.State = decodeReplicatedMemberState(&d)
	}
	response.Outcome.AppliedIndex = d.u64()
	response.Outcome.CompletionAppliedSequence = d.u64()
	response.RequestDigest = decodeReplicatedDigest(&d)
	response.Completion = d.slice()
	response.Completion = response.Completion[:len(response.Completion):len(response.Completion)]
	response.Outcome.CompletionBytes = len(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing ||
		response.Kind == ReplicatedTransactionReadResult {
		response.ReadApplied = d.u64()
		response.Value = d.slice()
		response.Value = response.Value[:len(response.Value):len(response.Value)]
	}
	if err := d.end(); err != nil {
		return nil, err
	}
	if !validReplicatedResponse(response) {
		return nil, ErrReplicatedWire
	}
	return response, nil
}

func maximumReplicatedResponseBody(request *ReplicatedRequest) (int, error) {
	if !validReplicatedRequest(request) {
		return 0, ErrReplicatedWire
	}
	switch request.Operation {
	case ReplicatedProbe:
		return replicatedResponseFixedBodyBytes, nil
	case ReplicatedPropose:
		return replicatedResponseFixedBodyBytes +
			replicatedstate.MaxCompletionEnvelopeBytes, nil
	case ReplicatedMembership:
		return replicatedResponseFixedBodyBytes, nil
	case ReplicatedReadLeader, ReplicatedReadFollower:
		return replicatedReadResponseFixedBodyBytes + int(request.MaxValueBytes), nil
	case ReplicatedTransactionRead:
		return replicatedReadResponseFixedBodyBytes +
			replicatedTransactionReadValueHeaderBytes + int(request.TransactionRead.MaxBytes), nil
	default:
		return 0, ErrReplicatedWire
	}
}

func replicatedRequestTagMatches(operation ReplicatedOperation, tag byte) bool {
	switch operation {
	case ReplicatedMembership:
		return tag == tagReplicatedMembershipRequest
	case ReplicatedTransactionRead:
		return tag == tagReplicatedTransactionRead
	default:
		return tag == tagReplicatedRequest
	}
}

func encodeReplicatedTransactionRead(
	e *encbuf,
	request ReplicatedTransactionReadRequest,
) {
	e.u8(uint8(request.Kind))
	e.b = append(e.b, request.ID[:]...)
	e.u64(request.MinimumApplied)
	e.u32(request.SegmentIndex)
	e.b = binary.BigEndian.AppendUint16(e.b, request.MaxRows)
	e.u32(request.MaxBytes)
}

func decodeReplicatedTransactionRead(d *deccur) ReplicatedTransactionReadRequest {
	request := ReplicatedTransactionReadRequest{Kind: ReplicatedTransactionReadKind(d.u8())}
	request.ID = distributedtxn.ID(d.fixed16())
	request.MinimumApplied = d.u64()
	request.SegmentIndex = d.u32()
	if len(d.b) < 2 {
		d.fail(errTruncated)
	} else {
		request.MaxRows = binary.BigEndian.Uint16(d.b[:2])
		d.b = d.b[2:]
	}
	request.MaxBytes = d.u32()
	return request
}

func encodeReplicatedFence(e *encbuf, fence ReplicatedFence) {
	e.fixed16(fence.Group.ClusterID)
	e.fixed16(fence.Group.ClusterIncarnation)
	e.u64(fence.Group.TopologyRecoveryEpoch)
	e.fixed16(fence.Group.ShardIncarnation)
	e.fixed16(fence.Group.GroupID)
	e.u64(fence.AllocationGeneration)
	e.u64(fence.Command.ReplicaSetVersion)
	e.u64(fence.Command.ActivePolicyGeneration)
	e.u64(fence.Command.ProtectionEpoch)
	e.u64(fence.Command.OwnershipEpoch)
	e.u64(fence.Command.SchemaGeneration)
	encodeReplicatedDigest(e, fence.Command.RelationManifestDigest)
	e.u64(fence.Command.RoutingVersion)
	e.u64(fence.Command.RouteGeneration)
	e.u64(fence.MemberID)
	e.fixed16(fence.StoreID)
	e.u64(fence.NodeIncarnation)
	e.u64(fence.Term)
}

func decodeReplicatedFence(d *deccur) ReplicatedFence {
	return ReplicatedFence{
		Group: raftmember.GroupKey{
			ClusterID: d.fixed16(), ClusterIncarnation: d.fixed16(),
			TopologyRecoveryEpoch: d.u64(), ShardIncarnation: d.fixed16(),
			GroupID: d.fixed16(),
		},
		AllocationGeneration: d.u64(),
		Command: raftservice.CommandFence{
			ReplicaSetVersion: d.u64(), ActivePolicyGeneration: d.u64(),
			ProtectionEpoch: d.u64(), OwnershipEpoch: d.u64(),
			SchemaGeneration: d.u64(), RelationManifestDigest: decodeReplicatedDigest(d),
			RoutingVersion: d.u64(), RouteGeneration: d.u64(),
		},
		MemberID: d.u64(), StoreID: d.fixed16(),
		NodeIncarnation: d.u64(), Term: d.u64(),
	}
}

func encodeReplicatedDigest(e *encbuf, digest [32]byte) {
	e.b = append(e.b, digest[:]...)
}

func decodeReplicatedDigest(d *deccur) (digest [32]byte) {
	if len(d.b) < len(digest) {
		d.fail(errTruncated)
		return digest
	}
	copy(digest[:], d.b[:len(digest)])
	d.b = d.b[len(digest):]
	return digest
}

func encodeReplicatedMembership(e *encbuf, request ReplicatedMembershipRequest) {
	e.u8(uint8(request.Kind))
	e.fixed16(request.TransitionID)
	e.u64(request.MetadataEpoch)
	e.u64(request.CatalogGeneration)
	e.u64(request.ExpectedReplicaSetVersion)
	e.u64(request.SourceMember)
	e.u64(request.TargetMember)
	e.u64(request.TransferTerm)
}

func decodeReplicatedMembership(d *deccur) ReplicatedMembershipRequest {
	return ReplicatedMembershipRequest{
		Kind: raftservice.MembershipKind(d.u8()), TransitionID: d.fixed16(),
		MetadataEpoch: d.u64(), CatalogGeneration: d.u64(),
		ExpectedReplicaSetVersion: d.u64(), SourceMember: d.u64(), TargetMember: d.u64(),
		TransferTerm: d.u64(),
	}
}

func encodeReplicatedMemberState(e *encbuf, state ReplicatedMemberState) {
	encodeReplicatedFence(e, state.Fence)
	e.u64(state.LeaderID)
	e.u64(state.Commit)
	e.u64(state.Applied)
	e.u64(state.CheckpointApplied)
}

func decodeReplicatedMemberState(d *deccur) ReplicatedMemberState {
	return ReplicatedMemberState{
		Fence: decodeReplicatedFence(d), LeaderID: d.u64(), Commit: d.u64(),
		Applied: d.u64(), CheckpointApplied: d.u64(),
	}
}

func validReplicatedGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) &&
		group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func validReplicatedFence(fence ReplicatedFence, exact bool) bool {
	if !validReplicatedGroup(fence.Group) || fence.AllocationGeneration == 0 {
		return false
	}
	present := fence.MemberID != 0 || fence.StoreID != ([16]byte{}) ||
		fence.NodeIncarnation != 0 || fence.Term != 0 || fence.Command != (raftservice.CommandFence{})
	if !exact {
		return !present
	}
	return fence.Command.Valid() && fence.MemberID != 0 && fence.StoreID != ([16]byte{}) &&
		fence.NodeIncarnation != 0 && fence.Term != 0
}

func validReplicatedRequest(request *ReplicatedRequest) bool {
	if request == nil {
		return false
	}
	authorityPresent := request.Authority.Node != (rafttransport.NodeID{}) ||
		request.Authority.Generation != 0
	if authorityPresent && (request.Authority.Node == (rafttransport.NodeID{}) ||
		request.Authority.Generation == 0) {
		return false
	}
	if authorityPresent != (request.Capability != 0) ||
		(request.Capability != 0 && !request.Capability.Valid()) {
		return false
	}
	switch request.Operation {
	case ReplicatedProbe:
		return validReplicatedProbeCapability(request.Capability) &&
			validReplicatedFence(request.Fence, false) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) &&
			request.Relation == 0 && len(request.Key) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0 &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{})
	case ReplicatedPropose:
		if !validReplicatedProposalCapability(request.Capability) ||
			request.Membership != (ReplicatedMembershipRequest{}) || request.Relation != 0 ||
			len(request.Key) != 0 || request.MinimumApplied != 0 || request.MaxValueBytes != 0 ||
			request.TransactionRead != (ReplicatedTransactionReadRequest{}) {
			return false
		}
		if !validReplicatedFence(request.Fence, true) || len(request.Command) == 0 ||
			len(request.Command) > replication.MaxCommandBytes {
			return false
		}
		command, err := replication.OpenCommand(request.Command)
		return err == nil && replicatedCommandCapabilityMatches(
			request.Capability, command.Kind(), command.AuthorityClass,
		) && command.ClusterID == request.Fence.Group.ClusterID &&
			command.ClusterIncarnation == request.Fence.Group.ClusterIncarnation &&
			command.TopologyRecoveryEpoch == request.Fence.Group.TopologyRecoveryEpoch &&
			command.ShardIncarnation == request.Fence.Group.ShardIncarnation &&
			command.GroupID == request.Fence.Group.GroupID &&
			command.AllocationGeneration == request.Fence.AllocationGeneration &&
			command.ReplicaSetVersion == request.Fence.Command.ReplicaSetVersion &&
			command.ActivePolicyGeneration == request.Fence.Command.ActivePolicyGeneration &&
			command.ProtectionEpoch == request.Fence.Command.ProtectionEpoch &&
			command.OwnershipEpoch == request.Fence.Command.OwnershipEpoch &&
			command.SchemaGeneration == request.Fence.Command.SchemaGeneration &&
			command.RoutingVersion == request.Fence.Command.RoutingVersion &&
			command.RouteGeneration == request.Fence.Command.RouteGeneration
	case ReplicatedMembership:
		// Semantic validation is deliberately performed by the serialized owner
		// so malformed control requests receive a deterministic refusal class.
		return (request.Capability == 0 || request.Capability == serviceauthz.CapabilityMembership) &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Relation == 0 && len(request.Key) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0 &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{})
	case ReplicatedReadLeader, ReplicatedReadFollower:
		return validReplicatedReadCapability(request.Capability) &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) &&
			request.Relation != 0 && request.Relation <= replication.MaxRelationID &&
			len(request.Key) != 0 && len(request.Key) <= replication.MaxMutationKeyBytes &&
			request.MinimumApplied != 0 && request.MaxValueBytes != 0 &&
			request.MaxValueBytes <= replication.MaxMutationValueBytes &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{})
	case ReplicatedTransactionRead:
		return request.Capability == serviceauthz.CapabilityTransactionRecovery &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) &&
			request.Relation == 0 && len(request.Key) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0 && validReplicatedTransactionRead(request.TransactionRead)
	default:
		return false
	}
}

func replicatedCommandCapabilityMatches(capability serviceauthz.Capability,
	kind replication.CommandKind,
	class replication.CommandAuthorityClass) bool {
	if capability == serviceauthz.CapabilityTopology {
		return class == replication.CommandAuthorityTopology
	}
	if capability == serviceauthz.CapabilityTransactionRecovery {
		return kind == replication.CommandTransaction &&
			class == replication.CommandAuthorityData
	}
	return (capability == 0 || capability == serviceauthz.CapabilityDataWrite) &&
		class == replication.CommandAuthorityData
}

func validReplicatedProbeCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataRead ||
		capability == serviceauthz.CapabilityDataWrite ||
		capability == serviceauthz.CapabilityMembership ||
		capability == serviceauthz.CapabilityTopology ||
		capability == serviceauthz.CapabilityTransactionRecovery
}

func validReplicatedProposalCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataWrite ||
		capability == serviceauthz.CapabilityTopology ||
		capability == serviceauthz.CapabilityTransactionRecovery
}

func validReplicatedReadCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataRead ||
		capability == serviceauthz.CapabilityTopology
}

func validReplicatedTransactionRead(request ReplicatedTransactionReadRequest) bool {
	_, ok := replicatedTransactionRecoveryRead(request)
	return ok
}

func validReplicatedMemberState(state ReplicatedMemberState) bool {
	return validReplicatedFence(state.Fence, true) &&
		state.Commit >= state.Applied && state.Applied >= state.CheckpointApplied
}

func validReplicatedResponse(response *ReplicatedResponse) bool {
	if response == nil || response.HasState != (response.State != ReplicatedMemberState{}) ||
		(response.HasState && !validReplicatedMemberState(response.State)) ||
		len(response.Completion) > replicatedstate.MaxCompletionEnvelopeBytes ||
		response.Outcome.CompletionBytes != len(response.Completion) ||
		len(response.Value) > replication.MaxMutationValueBytes {
		return false
	}
	switch response.Kind {
	case ReplicatedHandshake:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedCompletion:
		completion, err := replication.OpenCompletion(response.Completion)
		return err == nil && validReplicatedCompletionResult(completion) &&
			completion.AppliedSequence == response.Outcome.CompletionAppliedSequence &&
			completion.ClusterID == response.State.Fence.Group.ClusterID &&
			completion.ClusterIncarnation == response.State.Fence.Group.ClusterIncarnation &&
			completion.TopologyRecoveryEpoch == response.State.Fence.Group.TopologyRecoveryEpoch &&
			completion.ShardIncarnation == response.State.Fence.Group.ShardIncarnation &&
			completion.GroupID == response.State.Fence.Group.GroupID &&
			completion.AllocationGeneration == response.State.Fence.AllocationGeneration &&
			completion.ReplicaSetVersion == response.State.Fence.Command.ReplicaSetVersion &&
			completion.ActivePolicyGeneration == response.State.Fence.Command.ActivePolicyGeneration &&
			completion.ProtectionEpoch == response.State.Fence.Command.ProtectionEpoch &&
			completion.RoutingVersion == response.State.Fence.Command.RoutingVersion &&
			completion.RouteGeneration == response.State.Fence.Command.RouteGeneration &&
			response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest != ([sha256.Size]byte{}) &&
			response.Outcome.Code == raftserve.OutcomeCompletion &&
			response.Outcome.AppliedIndex != 0 &&
			response.State.Applied >= response.Outcome.AppliedIndex &&
			len(response.Completion) != 0 && response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedNotLeader, ReplicatedOutcomeUnknown:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedMembershipAccepted:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedRefusal:
		if response.Refusal == ReplicatedRefusalNone || len(response.Completion) != 0 ||
			response.ReadApplied != 0 || len(response.Value) != 0 {
			return false
		}
		if response.Refusal == ReplicatedRefusalDeterministic {
			return response.HasState && response.Outcome.Code > raftserve.OutcomeCompletion &&
				response.RequestDigest != ([sha256.Size]byte{}) &&
				response.Outcome.Code < raftserve.OutcomeProposalRefused &&
				response.Outcome.AppliedIndex != 0 &&
				response.State.Applied >= response.Outcome.AppliedIndex &&
				response.Outcome.CompletionAppliedSequence == 0 &&
				response.Outcome.CompletionBytes == 0
		}
		return response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) &&
			(response.HasState || response.Refusal == ReplicatedRefusalUnavailable ||
				response.Refusal == ReplicatedRefusalUnauthorized) &&
			response.Refusal <= ReplicatedRefusalTransactionReadMalformed
	case ReplicatedReadFound:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied
	case ReplicatedReadMissing:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			len(response.Value) == 0
	case ReplicatedTransactionReadResult:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			validReplicatedTransactionReadValue(response.Value)
	default:
		return false
	}
}

// validReplicatedCompletionResult closes the wire result grammar over the two
// shipped state-machine formats. The generic completion envelope authenticates
// arbitrary result metadata, so opening it alone is insufficient: accepting a
// malformed fixed result here would turn a committed invariant failure into a
// peer-visible success and defer detection to a downstream consumer. Both
// decoders borrow the inline result and allocate nothing on the valid path.
func validReplicatedCompletionResult(completion replication.CompletionView) bool {
	if completion.Storage != replication.CompletionInline ||
		completion.ResultLength != uint64(len(completion.InlineResult)) {
		return false
	}
	switch completion.ResultFormat {
	case replicatedstate.ResultFormatMutation:
		_, err := replicatedstate.OpenMutationCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		return err == nil
	case replicatedstate.ResultFormatTransaction:
		_, err := replicatedstate.OpenTransactionCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		return err == nil
	default:
		return false
	}
}
