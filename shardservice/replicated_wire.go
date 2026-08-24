package shardservice

import (
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	replicatedWireVersion = 1
	tagReplicatedRequest  = 'P'
	tagReplicatedResponse = 'A'
)

var ErrReplicatedWire = errors.New("shardservice: invalid replicated native frame")

// ReplicatedOperation is the closed byte-native serving operation set. SQL is
// deliberately absent: replicated mode accepts only canonical commands.
type ReplicatedOperation uint8

const (
	ReplicatedProbe ReplicatedOperation = iota + 1
	ReplicatedPropose
	ReplicatedReadLeader
	ReplicatedReadFollower
)

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
	Operation      ReplicatedOperation
	Fence          ReplicatedFence
	Command        []byte
	Relation       replication.RelationID
	Key            []byte
	MinimumApplied uint64
	MaxValueBytes  uint32
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
	ReplicatedReadFound
	ReplicatedReadMissing
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
	ReplicatedRefusalReadBehind
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
// classes. No remote diagnostic string is admitted to the hot wire.
type ReplicatedResponse struct {
	Kind        ReplicatedResponseKind
	Refusal     ReplicatedRefusalCode
	HasState    bool
	State       ReplicatedMemberState
	Outcome     raftserve.Outcome
	Completion  []byte
	ReadApplied uint64
	Value       []byte
}

// EncodeReplicatedRequest emits one canonical native request frame.
func EncodeReplicatedRequest(w io.Writer, request *ReplicatedRequest) error {
	if w == nil || !validReplicatedRequest(request) {
		return ErrReplicatedWire
	}
	e := newFrameEncoder(len(request.Command) + len(request.Key) + 16)
	e.u8(replicatedWireVersion)
	e.u8(uint8(request.Operation))
	encodeReplicatedFence(&e, request.Fence)
	switch request.Operation {
	case ReplicatedPropose:
		e.bytes(request.Command)
	case ReplicatedReadLeader, ReplicatedReadFollower:
		e.u8(uint8(request.Relation))
		e.u64(request.MinimumApplied)
		e.u32(request.MaxValueBytes)
		e.bytes(request.Key)
	}
	if e.err != nil {
		return e.err
	}
	return writeEncodedFrame(w, tagReplicatedRequest, e.b)
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
	body, charged, err := readFrameBudgeted(r, tagReplicatedRequest, budget)
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
	request.Fence = decodeReplicatedFence(&d)
	switch request.Operation {
	case ReplicatedPropose:
		request.Command = d.slice()
	case ReplicatedReadLeader, ReplicatedReadFollower:
		request.Relation = replication.RelationID(d.u8())
		request.MinimumApplied = d.u64()
		request.MaxValueBytes = d.u32()
		request.Key = d.slice()
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

// EncodeReplicatedResponse emits one canonical typed native response.
func EncodeReplicatedResponse(w io.Writer, response *ReplicatedResponse) error {
	if w == nil || !validReplicatedResponse(response) {
		return ErrReplicatedWire
	}
	e := newFrameEncoder(len(response.Completion))
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
	e.bytes(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing {
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
	body, err := readFrame(r, tagReplicatedResponse)
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
	response.Completion = d.bytesCopy()
	response.Outcome.CompletionBytes = len(response.Completion)
	if response.Kind == ReplicatedReadFound || response.Kind == ReplicatedReadMissing {
		response.ReadApplied = d.u64()
		response.Value = d.bytesCopy()
	}
	if err := d.end(); err != nil {
		return nil, err
	}
	if !validReplicatedResponse(response) {
		return nil, ErrReplicatedWire
	}
	return response, nil
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
	switch request.Operation {
	case ReplicatedProbe:
		return validReplicatedFence(request.Fence, false) && len(request.Command) == 0 &&
			request.Relation == 0 && len(request.Key) == 0 && request.MinimumApplied == 0 &&
			request.MaxValueBytes == 0
	case ReplicatedPropose:
		if !validReplicatedFence(request.Fence, true) || len(request.Command) == 0 ||
			len(request.Command) > replication.MaxCommandBytes {
			return false
		}
		command, err := replication.OpenCommand(request.Command)
		return err == nil && command.ClusterID == request.Fence.Group.ClusterID &&
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
	case ReplicatedReadLeader, ReplicatedReadFollower:
		return validReplicatedFence(request.Fence, true) && len(request.Command) == 0 &&
			request.Relation != 0 && request.Relation <= replication.MaxRelationID &&
			len(request.Key) != 0 && len(request.Key) <= replication.MaxMutationKeyBytes &&
			request.MinimumApplied != 0 && request.MaxValueBytes != 0 &&
			request.MaxValueBytes <= replication.MaxMutationValueBytes
	default:
		return false
	}
}

func validReplicatedMemberState(state ReplicatedMemberState) bool {
	return validReplicatedFence(state.Fence, true) &&
		state.Commit >= state.Applied && state.Applied >= state.CheckpointApplied
}

func validReplicatedResponse(response *ReplicatedResponse) bool {
	if response == nil || response.HasState != (response.State != ReplicatedMemberState{}) ||
		(response.HasState && !validReplicatedMemberState(response.State)) ||
		len(response.Completion) > replication.MaxEmptyResultCompletionEnvelopeBytes ||
		response.Outcome.CompletionBytes != len(response.Completion) ||
		len(response.Value) > replication.MaxMutationValueBytes {
		return false
	}
	switch response.Kind {
	case ReplicatedHandshake:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedCompletion:
		completion, err := replication.OpenCompletion(response.Completion)
		return err == nil && completion.AppliedSequence == response.Outcome.CompletionAppliedSequence &&
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
			response.Outcome.Code == raftserve.OutcomeCompletion &&
			response.Outcome.AppliedIndex != 0 &&
			response.State.Applied >= response.Outcome.AppliedIndex &&
			len(response.Completion) != 0 && response.ReadApplied == 0 && len(response.Value) == 0
	case ReplicatedNotLeader, ReplicatedOutcomeUnknown:
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
				response.Outcome.Code < raftserve.OutcomeProposalRefused &&
				response.Outcome.AppliedIndex != 0 &&
				response.State.Applied >= response.Outcome.AppliedIndex &&
				response.Outcome.CompletionAppliedSequence == 0 &&
				response.Outcome.CompletionBytes == 0
		}
		return response.Outcome == (raftserve.Outcome{}) &&
			(response.HasState || response.Refusal == ReplicatedRefusalUnavailable) &&
			response.Refusal <= ReplicatedRefusalReadBehind
	case ReplicatedReadFound:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied
	case ReplicatedReadMissing:
		return response.HasState && response.Refusal == ReplicatedRefusalNone &&
			response.Outcome == (raftserve.Outcome{}) && len(response.Completion) == 0 &&
			response.ReadApplied != 0 && response.State.Applied >= response.ReadApplied &&
			len(response.Value) == 0
	default:
		return false
	}
}
