package shardservice

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const (
	replicatedWireVersion          = 1
	tagReplicatedRequest           = 'P'
	tagReplicatedMembershipRequest = 'M'
	tagReplicatedTransactionRead   = 'T'
	tagReplicatedRequestLedgerRead = 'L'
	tagReplicatedExecutionPinRead  = 'E'
	tagReplicatedRouteGateRead     = 'G'
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

// Request-ledger recovery reads carry the common 242-byte authenticated RF3
// prefix plus one complete fixed-width RequestKey and exact hidden-row
// selector. Digest-only lookups are deliberately not representable.
const replicatedRequestLedgerReadRequestBodyBytes = 416
const replicatedExecutionPinReadRequestBodyBytes = 282
const replicatedRouteGateReadRequestBodyBytes = 250

const (
	MaxReplicatedTransactionReadBytes = replicatedstate.MaxTransactionRecoveryReadBytes
	MaxReplicatedTransactionScanItems = replicatedstate.MaxTransactionRecoveryScanRows
	MaxReplicatedTransactionScanBytes = replicatedstate.MaxTransactionRecoveryScanBytes
)

var ErrReplicatedWire = errors.New("shardservice: invalid replicated native frame")

// DecodeReplicatedSQLRequest validates the complete nested frame length against
// its admitted outer payload before the general SQL decoder allocates a body.
func DecodeReplicatedSQLRequest(frame []byte) (*ShardRequest, error) {
	if !validReplicatedSQLFrame(frame, tagRequest, MaxReplicatedSQLRequestBytes) {
		return nil, ErrReplicatedWire
	}
	return DecodeRequest(bytes.NewReader(frame))
}

// DecodeReplicatedSQLResponse bounds nested SQL result allocation by the exact
// authenticated native payload, rather than trusting a second length prefix.
func DecodeReplicatedSQLResponse(frame []byte) (*ShardResponse, error) {
	if !validReplicatedSQLFrame(frame, tagResponse, MaxReplicatedSQLResultBytes) {
		return nil, ErrReplicatedWire
	}
	return DecodeResponse(bytes.NewReader(frame))
}

func validReplicatedSQLFrame(frame []byte, tag byte, limit int) bool {
	return len(frame) >= 5 && len(frame) <= limit && frame[0] == tag &&
		binary.BigEndian.Uint32(frame[1:5]) == uint32(len(frame)-1)
}

// ReplicatedOperation is the closed byte-native serving operation set. Writes
// remain canonical commands; SQL queries execute only on quorum-fenced cuts.
type ReplicatedOperation uint8

const (
	ReplicatedProbe ReplicatedOperation = iota + 1
	ReplicatedPropose
	ReplicatedMembership
	ReplicatedReadLeader
	ReplicatedReadFollower
	ReplicatedTransactionRead
	ReplicatedReadBatchLeader
	_ // wire operation 8 is reserved; never reuse a published operation byte.
	ReplicatedRequestLedgerRead
	ReplicatedExecutionPinRead
	ReplicatedRouteGateRead
	ReplicatedQueryLeader
)

// ReplicatedTransactionReadKind is the complete RF3 recovery-read surface.
// These operations inspect only the replicated hidden system collection; they
// never consult the legacy process-local transaction journal.
type ReplicatedTransactionReadKind uint8

const (
	ReplicatedTransactionLookupCoordinator ReplicatedTransactionReadKind = iota + 1
	ReplicatedTransactionLookupTarget
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

type ReplicatedRequestLedgerReadRequest struct {
	Key                   requestledger.RequestKey
	ExpectedRangeIdentity requestledger.Digest
	Kind                  replicatedstate.RequestLedgerReadKind
	Ordinal               uint64
	ContentRoot           requestledger.Digest
	MinimumApplied        uint64
	MaxBytes              uint32
}

type ReplicatedExecutionPinReadRequest struct {
	Pin            executionpin.PinID
	MinimumApplied uint64
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
	Operation         ReplicatedOperation
	Authority         serviceauthz.Authority
	Capability        serviceauthz.Capability
	Fence             ReplicatedFence
	Command           []byte
	Membership        ReplicatedMembershipRequest
	Relation          replication.RelationID
	Key               []byte
	MinimumApplied    uint64
	MaxValueBytes     uint32
	BatchRead         []byte
	Query             []byte
	TransactionRead   ReplicatedTransactionReadRequest
	RequestLedgerRead ReplicatedRequestLedgerReadRequest
	ExecutionPinRead  ReplicatedExecutionPinReadRequest
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
	ReplicatedRequestLedgerReadResult
	ReplicatedReadBatchResult
	ReplicatedExecutionPinReadResult
	ReplicatedRouteGateReadResult
	ReplicatedQueryResult
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
	ReplicatedRefusalRequestLedgerReadMalformed
	ReplicatedRefusalReadIntentActive
	ReplicatedRefusalExecutionPinReadMalformed
	// ReplicatedRefusalRetryRetired is a durable session-window refusal before
	// proposal admission. It binds the exact request but claims no new apply.
	ReplicatedRefusalRetryRetired
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
	// sqlResult is populated only for an in-process semantic query. It is kept
	// out of the wire grammar; remote callers receive Value encoded by the
	// authenticated adapter instead.
	sqlResult *ShardResponse
}

// EncodeReplicatedRequest emits one canonical native request frame with a
// fresh arena. Holders of a persistent native stream should use the
// FrameEncoder method to reuse one arena across the stream's lifetime.
func EncodeReplicatedRequest(w io.Writer, request *ReplicatedRequest) error {
	return (&FrameEncoder{}).EncodeReplicatedRequest(w, request)
}

// EncodeReplicatedRequest emits one canonical native request frame into the
// encoder's owned arena, retaining it for the next call.
func (f *FrameEncoder) EncodeReplicatedRequest(w io.Writer, request *ReplicatedRequest) error {
	if w == nil || !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	payloadHint := len(request.Command) + len(request.Key) + len(request.BatchRead) + len(request.Query) + 16
	if request.Operation == ReplicatedMembership {
		payloadHint += 65
	}
	e := newFrameEncoder(f.arena, payloadHint)
	defer func() { f.arena = keepFrameArena(e.b) }()
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
	case ReplicatedReadBatchLeader:
		e.u64(request.MinimumApplied)
		e.u32(request.MaxValueBytes)
		e.bytes(request.BatchRead)
	case ReplicatedQueryLeader:
		e.u32(request.MaxValueBytes)
		e.bytes(request.Query)
	case ReplicatedTransactionRead:
		encodeReplicatedTransactionRead(&e, request.TransactionRead)
	case ReplicatedRequestLedgerRead:
		encodeReplicatedRequestLedgerRead(&e, request.RequestLedgerRead)
	case ReplicatedRouteGateRead:
		e.u64(request.MinimumApplied)
	case ReplicatedExecutionPinRead:
		encodeReplicatedExecutionPinRead(&e, request.ExecutionPinRead)
	}
	if e.err != nil {
		return e.err
	}
	tag := byte(tagReplicatedRequest)
	if request.Operation == ReplicatedMembership {
		tag = tagReplicatedMembershipRequest
	} else if request.Operation == ReplicatedTransactionRead {
		tag = tagReplicatedTransactionRead
	} else if request.Operation == ReplicatedRequestLedgerRead {
		tag = tagReplicatedRequestLedgerRead
	} else if request.Operation == ReplicatedRouteGateRead {
		tag = tagReplicatedRouteGateRead
	} else if request.Operation == ReplicatedExecutionPinRead {
		tag = tagReplicatedExecutionPinRead
	}
	return writeEncodedFrame(w, tag, e.b)
}

// ReplicatedRequestFrameBytes returns the exact encoded native request size,
// including its five-byte frame header, without allocating a frame. Semantic
// local dispatch uses this for the same aggregate byte admission charged by a
// remote request.
func ReplicatedRequestFrameBytes(request *ReplicatedRequest) (int, error) {
	if !validReplicatedRequest(request) {
		return 0, ErrReplicatedWire
	}
	total := 242 // common prefix through the complete ServingFence
	add := func(n int) error {
		if n < 0 || n > maxFrameBody-total {
			return errFrameTooLarge
		}
		total += n
		return nil
	}
	addBytes := func(value []byte) error {
		return add(4 + len(value))
	}
	switch request.Operation {
	case ReplicatedPropose:
		if err := addBytes(request.Command); err != nil {
			return 0, err
		}
	case ReplicatedMembership:
		if err := add(4 + len(request.Command) + 65); err != nil {
			return 0, err
		}
	case ReplicatedReadLeader, ReplicatedReadFollower:
		if err := add(1 + 8 + 4); err != nil {
			return 0, err
		}
		if err := addBytes(request.Key); err != nil {
			return 0, err
		}
	case ReplicatedReadBatchLeader:
		if err := add(8 + 4); err != nil {
			return 0, err
		}
		if err := addBytes(request.BatchRead); err != nil {
			return 0, err
		}
	case ReplicatedQueryLeader:
		if err := add(4); err != nil {
			return 0, err
		}
		if err := addBytes(request.Query); err != nil {
			return 0, err
		}
	case ReplicatedTransactionRead:
		if err := add(1 + 16 + 8 + 4 + 2 + 4); err != nil {
			return 0, err
		}
	case ReplicatedRequestLedgerRead:
		if err := add(174); err != nil {
			return 0, err
		}
	case ReplicatedExecutionPinRead:
		if err := add(32 + 8); err != nil {
			return 0, err
		}
	case ReplicatedRouteGateRead:
		if err := add(8); err != nil {
			return 0, err
		}
	}
	return total + 5, nil
}

// ValidateReplicatedRequest exposes the exact native request grammar to
// transport-neutral callers without exposing the wire encoder's internals.
func ValidateReplicatedRequest(request *ReplicatedRequest) error {
	if !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	if _, err := ReplicatedRequestFrameBytes(request); err != nil {
		return err
	}
	return nil
}

// ValidateReplicatedResponse exposes the same canonical response checks used
// by DecodeReplicatedResponse. A response with a retained read lease remains
// valid; ownership is released only by the corresponding reply lease.
func ValidateReplicatedResponse(response *ReplicatedResponse) error {
	if !validReplicatedResponse(response) {
		return ErrReplicatedWire
	}
	return nil
}

// EncodeReplicatedRequestBorrowed emits the fixed request prefix and borrows
// the immutable command or point key as a second buffer, avoiding a payload-sized
// userspace copy on every retry. A TLS stream may encode the two writes as
// separate record sequences; this function does not claim writev.
func EncodeReplicatedRequestBorrowed(w io.Writer, request *ReplicatedRequest) error {
	return (&FrameEncoder{}).EncodeReplicatedRequestBorrowed(w, request)
}

// EncodeReplicatedRequestBorrowed emits the fixed request prefix into the
// encoder's owned arena and borrows the immutable payload as a second write
// buffer, retaining the arena for the next call.
func (f *FrameEncoder) EncodeReplicatedRequestBorrowed(w io.Writer, request *ReplicatedRequest) error {
	if w == nil || !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	payloadHint := 0
	if request.Operation == ReplicatedMembership {
		payloadHint = 65
	}
	e := newFrameEncoder(f.arena, payloadHint)
	defer func() { f.arena = keepFrameArena(e.b) }()
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
	case ReplicatedReadBatchLeader:
		e.u64(request.MinimumApplied)
		e.u32(request.MaxValueBytes)
		payload = request.BatchRead
		e.u32(uint32(len(payload)))
	case ReplicatedQueryLeader:
		e.u32(request.MaxValueBytes)
		payload = request.Query
		e.u32(uint32(len(payload)))
	case ReplicatedTransactionRead:
		encodeReplicatedTransactionRead(&e, request.TransactionRead)
		tag = tagReplicatedTransactionRead
	case ReplicatedRequestLedgerRead:
		encodeReplicatedRequestLedgerRead(&e, request.RequestLedgerRead)
		tag = tagReplicatedRequestLedgerRead
	case ReplicatedRouteGateRead:
		e.u64(request.MinimumApplied)
		tag = tagReplicatedRouteGateRead
	case ReplicatedExecutionPinRead:
		encodeReplicatedExecutionPinRead(&e, request.ExecutionPinRead)
		tag = tagReplicatedExecutionPinRead
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
	case ReplicatedReadBatchLeader:
		request.MinimumApplied = d.u64()
		request.MaxValueBytes = d.u32()
		request.BatchRead = d.slice()
	case ReplicatedQueryLeader:
		request.MaxValueBytes = d.u32()
		request.Query = d.slice()
	case ReplicatedTransactionRead:
		request.TransactionRead = decodeReplicatedTransactionRead(&d)
	case ReplicatedRequestLedgerRead:
		request.RequestLedgerRead = decodeReplicatedRequestLedgerRead(&d)
	case ReplicatedRouteGateRead:
		request.MinimumApplied = d.u64()
	case ReplicatedExecutionPinRead:
		request.ExecutionPinRead = decodeReplicatedExecutionPinRead(&d)
	}
	if err := d.end(); err != nil {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, err
	}
	request.Command = request.Command[:len(request.Command):len(request.Command)]
	request.Key = request.Key[:len(request.Key):len(request.Key)]
	request.BatchRead = request.BatchRead[:len(request.BatchRead):len(request.BatchRead)]
	request.Query = request.Query[:len(request.Query):len(request.Query)]
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
	case tagReplicatedRequestLedgerRead:
		if size != replicatedRequestLedgerReadRequestBodyBytes {
			return nil, 0, tag, ErrReplicatedWire
		}
		var prefix [2]byte
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			return nil, 0, tag, err
		}
		if prefix[0] != replicatedWireVersion ||
			ReplicatedOperation(prefix[1]) != ReplicatedRequestLedgerRead {
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
	case tagReplicatedRouteGateRead:
		if size != replicatedRouteGateReadRequestBodyBytes {
			return nil, 0, tag, ErrReplicatedWire
		}
		var prefix [2]byte
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			return nil, 0, tag, err
		}
		if prefix[0] != replicatedWireVersion ||
			ReplicatedOperation(prefix[1]) != ReplicatedRouteGateRead {
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
	case tagReplicatedExecutionPinRead:
		if size != replicatedExecutionPinReadRequestBodyBytes {
			return nil, 0, tag, ErrReplicatedWire
		}
		var prefix [2]byte
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			return nil, 0, tag, err
		}
		if prefix[0] != replicatedWireVersion ||
			ReplicatedOperation(prefix[1]) != ReplicatedExecutionPinRead {
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
	return (&FrameEncoder{}).EncodeReplicatedResponse(w, response)
}

// EncodeReplicatedResponse emits one canonical native response frame into
// the encoder's owned arena, retaining it for the next call. The nested SQL
// envelope keeps a fresh arena on purpose: its bytes are retained by the
// caller past the Write, so they must never alias a reused array.
func (f *FrameEncoder) EncodeReplicatedResponse(w io.Writer, response *ReplicatedResponse) error {
	if w == nil || !validReplicatedResponse(response) {
		return ErrReplicatedWire
	}
	if response.sqlResult != nil {
		// This is the only response encoding boundary. The caller retains the
		// reply lease through both encodings, including a slow socket writer.
		var body bytes.Buffer
		if err := EncodeResponse(&body, response.sqlResult); err != nil {
			return err
		}
		wire := *response
		wire.Value, wire.sqlResult = body.Bytes(), nil
		response = &wire
	}
	bodyHint := replicatedResponseFixedBodyBytes + len(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing ||
		response.Kind == ReplicatedReadBatchResult || response.Kind == ReplicatedQueryResult ||
		response.Kind == ReplicatedTransactionReadResult ||
		response.Kind == ReplicatedRequestLedgerReadResult ||
		response.Kind == ReplicatedExecutionPinReadResult || response.Kind == ReplicatedRouteGateReadResult {
		bodyHint = replicatedReadResponseFixedBodyBytes + len(response.Value)
	}
	e := newFrameEncoder(f.arena, bodyHint)
	defer func() { f.arena = keepFrameArena(e.b) }()
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
		response.Kind == ReplicatedReadBatchResult || response.Kind == ReplicatedQueryResult ||
		response.Kind == ReplicatedTransactionReadResult ||
		response.Kind == ReplicatedRequestLedgerReadResult ||
		response.Kind == ReplicatedExecutionPinReadResult || response.Kind == ReplicatedRouteGateReadResult {
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
		response.Kind == ReplicatedReadBatchResult || response.Kind == ReplicatedQueryResult ||
		response.Kind == ReplicatedTransactionReadResult ||
		response.Kind == ReplicatedRequestLedgerReadResult ||
		response.Kind == ReplicatedExecutionPinReadResult || response.Kind == ReplicatedRouteGateReadResult {
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
	case ReplicatedReadBatchLeader, ReplicatedQueryLeader:
		return replicatedReadResponseFixedBodyBytes + int(request.MaxValueBytes), nil
	case ReplicatedTransactionRead:
		return replicatedReadResponseFixedBodyBytes +
			replicatedTransactionReadValueHeaderBytes + int(request.TransactionRead.MaxBytes), nil
	case ReplicatedRequestLedgerRead:
		return replicatedReadResponseFixedBodyBytes +
			replicatedRequestLedgerReadValueHeaderBytes + int(request.RequestLedgerRead.MaxBytes), nil
	case ReplicatedRouteGateRead:
		return replicatedReadResponseFixedBodyBytes + routegate.StatusBytes, nil
	case ReplicatedExecutionPinRead:
		return replicatedReadResponseFixedBodyBytes + replicatedExecutionPinReadValueBytes, nil
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
	case ReplicatedRequestLedgerRead:
		return tag == tagReplicatedRequestLedgerRead
	case ReplicatedRouteGateRead:
		return tag == tagReplicatedRouteGateRead
	case ReplicatedExecutionPinRead:
		return tag == tagReplicatedExecutionPinRead
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

func encodeReplicatedRequestLedgerRead(
	e *encbuf,
	request ReplicatedRequestLedgerReadRequest,
) {
	e.u8(uint8(request.Key.Scope))
	e.b = append(e.b, request.Key.Principal[:]...)
	e.b = append(e.b, request.Key.Request[:]...)
	e.b = append(e.b, request.Key.TenantDigest[:]...)
	e.u64(request.Key.IssuerEpoch)
	e.u64(request.Key.IssuerSequence)
	e.b = append(e.b, request.Key.IssuerLane[:]...)
	e.b = append(e.b, request.ExpectedRangeIdentity[:]...)
	e.u8(uint8(request.Kind))
	e.u64(request.Ordinal)
	e.b = append(e.b, request.ContentRoot[:]...)
	e.u64(request.MinimumApplied)
	e.u32(request.MaxBytes)
}

func decodeReplicatedRequestLedgerRead(d *deccur) ReplicatedRequestLedgerReadRequest {
	request := ReplicatedRequestLedgerReadRequest{}
	request.Key.Scope = requestledger.ScopeKind(d.u8())
	request.Key.Principal = requestledger.PrincipalID(d.fixed16())
	request.Key.Request = requestledger.RequestID(d.fixed16())
	request.Key.TenantDigest = requestledger.Digest(d.fixed32())
	request.Key.IssuerEpoch = d.u64()
	request.Key.IssuerSequence = d.u64()
	request.Key.IssuerLane = requestledger.IssuerLane(d.fixed8())
	request.ExpectedRangeIdentity = requestledger.Digest(d.fixed32())
	request.Kind = replicatedstate.RequestLedgerReadKind(d.u8())
	request.Ordinal = d.u64()
	request.ContentRoot = requestledger.Digest(d.fixed32())
	request.MinimumApplied = d.u64()
	request.MaxBytes = d.u32()
	return request
}

func encodeReplicatedExecutionPinRead(e *encbuf, request ReplicatedExecutionPinReadRequest) {
	e.b = append(e.b, request.Pin[:]...)
	e.u64(request.MinimumApplied)
}

func decodeReplicatedExecutionPinRead(d *deccur) ReplicatedExecutionPinReadRequest {
	return ReplicatedExecutionPinReadRequest{
		Pin: executionpin.PinID(d.fixed32()), MinimumApplied: d.u64(),
	}
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
	if request.Operation != ReplicatedQueryLeader && len(request.Query) != 0 {
		return false
	}
	if request.Operation != ReplicatedReadBatchLeader && len(request.BatchRead) != 0 {
		return false
	}
	if request.Operation != ReplicatedExecutionPinRead &&
		request.ExecutionPinRead != (ReplicatedExecutionPinReadRequest{}) {
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
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedPropose:
		if !validReplicatedProposalCapability(request.Capability) ||
			request.Membership != (ReplicatedMembershipRequest{}) || request.Relation != 0 ||
			len(request.Key) != 0 || request.MinimumApplied != 0 || request.MaxValueBytes != 0 ||
			request.TransactionRead != (ReplicatedTransactionReadRequest{}) ||
			request.RequestLedgerRead != (ReplicatedRequestLedgerReadRequest{}) {
			return false
		}
		if !validReplicatedFence(request.Fence, true) || len(request.Command) == 0 ||
			len(request.Command) > replication.MaxCommandBytes {
			return false
		}
		command, err := replication.OpenCommand(request.Command)
		if err != nil || !replicatedCommandCapabilityMatches(
			request.Capability, command.Kind(), command.AuthorityClass,
		) || !validReplicatedRequestLedgerPrincipal(request, command) ||
			!replicatedExecutionPinAuthorityMatches(request, command) {
			return false
		}
		// The catalog coordinates its own placement. An unknown journal write
		// must keep its original bytes while settling under the current serving
		// fence; owner admission permits only its exact retained completion.
		if request.Capability == serviceauthz.CapabilityTopology &&
			raftservice.CatalogCommandReplayMatchesFence(command, raftservice.ServingFence{
				Group: request.Fence.Group, AllocationGeneration: request.Fence.AllocationGeneration,
				Command: request.Fence.Command,
			}) {
			return true
		}
		return command.ClusterID == request.Fence.Group.ClusterID &&
			command.ClusterIncarnation == request.Fence.Group.ClusterIncarnation &&
			command.TopologyRecoveryEpoch == request.Fence.Group.TopologyRecoveryEpoch &&
			command.ShardIncarnation == request.Fence.Group.ShardIncarnation &&
			command.GroupID == request.Fence.Group.GroupID &&
			command.AllocationGeneration == request.Fence.AllocationGeneration &&
			replication.CommandMembershipMatches(command.AuthorityClass, command.ReplicaSetVersion, request.Fence.Command.ReplicaSetVersion) &&
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
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedReadLeader, ReplicatedReadFollower:
		return validReplicatedReadCapability(request.Capability) &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) &&
			request.Relation != 0 && request.Relation <= replication.MaxRelationID &&
			len(request.Key) != 0 && len(request.Key) <= replication.MaxMutationKeyBytes &&
			request.MinimumApplied != 0 && request.MaxValueBytes != 0 &&
			request.MaxValueBytes <= replication.MaxMutationValueBytes &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedReadBatchLeader:
		_, batchErr := replicatedstate.OpenPointReadBatch(request.BatchRead)
		return validReplicatedReadCapability(request.Capability) &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) && request.Relation == 0 &&
			len(request.Key) == 0 && request.MinimumApplied != 0 &&
			request.MaxValueBytes != 0 &&
			request.MaxValueBytes <= replicatedstate.MaxPointReadBatchBytes && batchErr == nil &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedQueryLeader:
		return request.Capability == serviceauthz.CapabilityDataRead &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) && request.Relation == 0 &&
			len(request.Key) == 0 && request.MinimumApplied == 0 && len(request.BatchRead) == 0 &&
			request.MaxValueBytes > 0 && request.MaxValueBytes <= MaxReplicatedSQLResultBytes &&
			len(request.Query) > 0 && len(request.Query) <= MaxReplicatedSQLRequestBytes &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedTransactionRead:
		return request.Capability == serviceauthz.CapabilityTransactionRecovery &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) &&
			request.Relation == 0 && len(request.Key) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0 && validReplicatedTransactionRead(request.TransactionRead) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	case ReplicatedRequestLedgerRead:
		return request.Capability == serviceauthz.CapabilityRequestLedger &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) && request.Relation == 0 &&
			len(request.Key) == 0 && request.MinimumApplied == 0 && request.MaxValueBytes == 0 &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			validReplicatedRequestLedgerRead(request.RequestLedgerRead)
	case ReplicatedExecutionPinRead:
		return request.Capability == serviceauthz.CapabilityExecutionPin &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) && request.Relation == 0 &&
			len(request.Key) == 0 && len(request.BatchRead) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0 &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{}) &&
			request.ExecutionPinRead.Pin != (executionpin.PinID{}) &&
			request.ExecutionPinRead.MinimumApplied != 0
	case ReplicatedRouteGateRead:
		return request.Capability == serviceauthz.CapabilityDataWrite &&
			validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Membership == (ReplicatedMembershipRequest{}) && request.Relation == 0 &&
			len(request.Key) == 0 && len(request.BatchRead) == 0 && request.MinimumApplied != 0 &&
			request.MaxValueBytes == 0 &&
			request.TransactionRead == (ReplicatedTransactionReadRequest{}) &&
			request.RequestLedgerRead == (ReplicatedRequestLedgerReadRequest{})
	default:
		return false
	}
}

// validReplicatedRequestLedgerPrincipal keeps local-install identities off the
// authenticated RF3 wire. The outer authority is the trusted gateway service
// (Delegate+RequestLedger); the immutable end-user issuer stays inside Head
// and is not required to equal that service. Later operations carry only the
// key digest, which the state machine binds to retained state.
func validReplicatedRequestLedgerPrincipal(
	request *ReplicatedRequest,
	command replication.CommandView,
) bool {
	if request.Capability != serviceauthz.CapabilityRequestLedger {
		return true
	}
	if request.Authority.Node == (rafttransport.NodeID{}) {
		return false
	}
	if command.ClientID != replication.ID128(request.Authority.Node) {
		return false
	}
	// Admission inspects only Create's authenticated subject. Nil scratch
	// fully validates pending bytes without putting a 256-entry StepRef array
	// on every shard-wire request stack.
	inner, err := command.OpenRequestLedgerInto(nil)
	if err != nil {
		return false
	}
	head, creates := inner.Head()
	if !creates {
		return true
	}
	return head.Key.Scope == requestledger.ScopeAuthenticated
}

func replicatedExecutionPinAuthorityMatches(
	request *ReplicatedRequest,
	command replication.CommandView,
) bool {
	if command.Kind() != replication.CommandExecutionPin {
		return true
	}
	if request == nil || request.Capability != serviceauthz.CapabilityExecutionPin {
		return false
	}
	nested, err := command.OpenExecutionPin()
	return err == nil && nested.AuthorityNode == executionpin.ID(request.Authority.Node) &&
		nested.AuthorityGeneration == request.Authority.Generation
}

func replicatedCommandCapabilityMatches(capability serviceauthz.Capability,
	kind replication.CommandKind,
	class replication.CommandAuthorityClass) bool {
	if capability == serviceauthz.CapabilityTopology {
		return class == replication.CommandAuthorityTopology
	}
	if capability == serviceauthz.CapabilityExecutionPin {
		return replication.IsExecutionPinAuthority(class) &&
			(kind == replication.CommandExecutionPin || kind == replication.CommandSessionOpen ||
				kind == replication.CommandSessionRenew || kind == replication.CommandSessionRevoke ||
				kind == replication.CommandSessionRetire || kind == replication.CommandSessionRelease)
	}
	if capability == serviceauthz.CapabilityTransactionRecovery {
		return kind == replication.CommandTransaction &&
			replication.IsDataAuthority(class)
	}
	if capability == serviceauthz.CapabilityRequestLedger {
		return kind == replication.CommandRequestLedger &&
			class == replication.CommandAuthorityRequestLedger
	}
	return (capability == 0 || capability == serviceauthz.CapabilityDataWrite) &&
		(replication.IsDataAuthority(class) || replication.IsRouteSessionAuthority(class))
}

func validReplicatedProbeCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataRead ||
		capability == serviceauthz.CapabilityDataWrite ||
		capability == serviceauthz.CapabilityMembership ||
		capability == serviceauthz.CapabilityTopology ||
		capability == serviceauthz.CapabilityTransactionRecovery ||
		capability == serviceauthz.CapabilityRequestLedger ||
		capability == serviceauthz.CapabilityExecutionPin
}

func validReplicatedProposalCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataWrite ||
		capability == serviceauthz.CapabilityTopology ||
		capability == serviceauthz.CapabilityTransactionRecovery ||
		capability == serviceauthz.CapabilityRequestLedger ||
		capability == serviceauthz.CapabilityExecutionPin
}

func validReplicatedReadCapability(capability serviceauthz.Capability) bool {
	return capability == 0 || capability == serviceauthz.CapabilityDataRead ||
		capability == serviceauthz.CapabilityTopology
}

func validReplicatedTransactionRead(request ReplicatedTransactionReadRequest) bool {
	_, ok := replicatedTransactionRecoveryRead(request)
	return ok
}

func validReplicatedRequestLedgerRead(request ReplicatedRequestLedgerReadRequest) bool {
	return replicatedstate.ValidateRequestLedgerReadRequest(replicatedstate.RequestLedgerReadRequest{
		Key: request.Key, ExpectedRangeIdentity: request.ExpectedRangeIdentity,
		Kind: request.Kind, Ordinal: request.Ordinal, ContentRoot: request.ContentRoot,
		MinimumApplied: request.MinimumApplied, MaxBytes: request.MaxBytes,
	}) == nil
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
		(response.sqlResult != nil && response.Kind != ReplicatedQueryResult) ||
		len(response.Value) > max(replication.MaxMutationValueBytes,
			replicatedstate.MaxRequestLedgerTerminalReadBytes+
				replicatedRequestLedgerReadValueHeaderBytes) {
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
			// Completion metadata belongs to the original command, not the
			// current serving membership. Consumers still require an exact
			// request digest and command/completion identity match. This permits
			// retained outcomes, never a future membership or a stale proposal.
			completion.ReplicaSetVersion <= response.State.Fence.Command.ReplicaSetVersion &&
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
		if response.Refusal == ReplicatedRefusalRetryRetired {
			return response.HasState && response.RequestDigest != ([sha256.Size]byte{}) &&
				response.Outcome == (raftserve.Outcome{Code: raftserve.OutcomeRetryRetired})
		}
		return response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) &&
			(response.HasState || response.Refusal == ReplicatedRefusalUnavailable ||
				response.Refusal == ReplicatedRefusalUnauthorized) &&
			response.Refusal <= ReplicatedRefusalReadIntentActive
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
	case ReplicatedReadBatchResult:
		_, err := replicatedstate.OpenPointReadBatchValue(response.Value)
		return err == nil && response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied
	case ReplicatedQueryResult:
		if response.sqlResult != nil {
			return response.HasState && response.Refusal == ReplicatedRefusalNone &&
				response.RequestDigest == ([sha256.Size]byte{}) &&
				response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
				response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
				len(response.Value) == 0 && replicatedSemanticSQLResultValid(response.sqlResult)
		}
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			len(response.Value) >= 5 && len(response.Value) <= MaxReplicatedSQLResultBytes
	case ReplicatedTransactionReadResult:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			validReplicatedTransactionReadValue(response.Value)
	case ReplicatedRequestLedgerReadResult:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			validReplicatedRequestLedgerReadValue(response.Value)
	case ReplicatedExecutionPinReadResult:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			validReplicatedExecutionPinReadValue(response.Value)
	case ReplicatedRouteGateReadResult:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.RequestDigest == ([sha256.Size]byte{}) &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			validReplicatedRouteGateReadValue(response.Value)
	default:
		return false
	}
}

// validReplicatedCompletionResult closes the wire result grammar over the
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
	case replicatedstate.ResultFormatRequestLedger:
		_, err := replicatedstate.OpenRequestLedgerCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		return err == nil
	case replicatedstate.ResultFormatRouteGate:
		if completion.ResultCode != replicatedstate.ResultRouteGate {
			return false
		}
		_, err := routegate.OpenOutcome(completion.InlineResult)
		return err == nil
	case replicatedstate.ResultFormatExecutionPin:
		proof, err := executionpin.OpenCompletion(completion.InlineResult)
		if err != nil {
			return false
		}
		switch completion.ResultCode {
		case replicatedstate.ResultApplied:
			if !proof.Found {
				return false
			}
			switch proof.Operation {
			case executionpin.OperationAcquire, executionpin.OperationRenew, executionpin.OperationRecover:
				return proof.Status == executionpin.StatusActive &&
					proof.Lease.Applied == completion.AppliedSequence
			case executionpin.OperationRelease:
				return proof.Status == executionpin.StatusReleased &&
					proof.Terminal.Applied == completion.AppliedSequence
			case executionpin.OperationExpire:
				return proof.Status == executionpin.StatusExpired &&
					proof.Terminal.Applied == completion.AppliedSequence
			}
		case replicatedstate.ResultIndexConflict, replicatedstate.ResultIntentBusy,
			replicatedstate.ResultTargetBound, replicatedstate.ResultStaleFence:
			return !proof.Found
		}
		return false
	default:
		return false
	}
}
