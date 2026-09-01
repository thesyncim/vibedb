package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var directMutationIdentityDomain = []byte("vibedb/direct-mutation/request\x00")
var directMutationAuthorityDomain = []byte("vibedb/direct-mutation/authority\x00")
var directMutationFingerprintDomain = []byte("vibedb/direct-mutation/fingerprint\x00")

// ReplicatedDirectMutation is the single-group write contract. The caller has
// already routed and lowered its complete logical operation to canonical native
// relation batches. RequestKey carries a serialized issuer-lane sequence;
// RequestDigest binds that sequence to the exact caller request.
type ReplicatedDirectMutation struct {
	Key           requestledger.RequestKey
	RequestDigest replication.Digest
	Tenant        []byte
	Participant   ReplicatedTransactionParticipant
}

// ReplicatedDirectMutationResult is the exact retained terminal result from the
// participant group. One successful invocation consumes one Raft proposal; an
// exact retry of the lane's latest request returns the original Applied index
// and affected-row count.
type ReplicatedDirectMutationResult struct {
	ID           distributedtxn.ID
	Applied      uint64
	Commit       uint64
	Checkpoint   uint64
	Committed    bool
	AffectedRows int64
	ResultCode   uint32
	Retries      int
	Duplicate    bool
}

// DirectMutate applies and deduplicates one complete single-group mutation in
// one consensus entry. It deliberately has no request-ledger, execution-pin,
// route-gate, coordinator, intent, or session-open preflight.
func (executor *ReplicatedExecutor) DirectMutate(
	ctx context.Context,
	request ReplicatedDirectMutation,
) (ReplicatedDirectMutationResult, error) {
	command, control, err := appendReplicatedDirectMutationCommand(nil, request)
	if err != nil {
		return ReplicatedDirectMutationResult{}, err
	}
	proposal, err := executor.Propose(ctx, request.Participant.Route, command)
	if err != nil {
		return ReplicatedDirectMutationResult{}, err
	}
	code, completion, err := durableDistributedCompletion(command, control, proposal.Completion)
	revisionMatches := completion.Revision == request.Key.IssuerSequence
	if code == replicatedstate.ResultTransactionConflict {
		revisionMatches = completion.Revision >= request.Key.IssuerSequence
	}
	if err != nil || !completion.RevisionValid || !revisionMatches {
		return ReplicatedDirectMutationResult{}, errors.Join(err, ErrReplicatedTransaction)
	}
	result := ReplicatedDirectMutationResult{
		ID: control.ID, Applied: proposal.Outcome.CompletionAppliedSequence,
		Commit: proposal.State.Commit, Checkpoint: proposal.State.CheckpointApplied,
		Committed:  code == replicatedstate.ResultApplied,
		ResultCode: code, Retries: proposal.Retries,
		Duplicate: proposal.Outcome.AppliedIndex != proposal.Outcome.CompletionAppliedSequence,
	}
	if result.Committed {
		if !completion.AffectedRowsValid || completion.AffectedRows < 0 {
			return ReplicatedDirectMutationResult{}, ErrReplicatedTransaction
		}
		result.AffectedRows = completion.AffectedRows
		return result, nil
	}
	if code != replicatedstate.ResultIndexConflict && code != replicatedstate.ResultWrongShard &&
		code != replicatedstate.ResultTransactionConflict {
		return ReplicatedDirectMutationResult{}, ErrReplicatedTransaction
	}
	return result, ErrReplicatedTransactionConflict
}

func appendReplicatedDirectMutationCommand(
	dst []byte,
	request ReplicatedDirectMutation,
) ([]byte, distributedtxn.ReplicatedCommand, error) {
	participant := request.Participant
	if !request.Key.Valid() || request.Key.IssuerEpoch == 0 ||
		request.Key.IssuerSequence == 0 || request.Key.IssuerLane == (requestledger.IssuerLane{}) ||
		request.RequestDigest == (replication.Digest{}) ||
		len(request.Tenant) == 0 || len(request.Tenant) > replication.MaxIdentityBytes ||
		requestledger.Digest(sha256.Sum256(request.Tenant)) != request.Key.TenantDigest ||
		!validReplicatedRoute(participant.Route) ||
		!distributedtxn.ValidateIntentScopes(participant.IntentScopes, participant.BucketBits) ||
		len(participant.Batches) == 0 {
		return dst, distributedtxn.ReplicatedCommand{}, ErrReplicatedTransaction
	}
	keyDigest, err := requestledger.KeyDigest(request.Key)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, errors.Join(err, ErrReplicatedTransaction)
	}
	laneHome, err := requestledger.Home(request.Key)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, errors.Join(err, ErrReplicatedTransaction)
	}
	mutationDigest, err := replication.TransactionMutationDigest(participant.Batches)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, errors.Join(err, ErrReplicatedTransaction)
	}
	id := directMutationTransactionID(requestledger.Digest(laneHome))
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedApplySingleParticipant,
		ID:        id, ExpectedRevision: request.Key.IssuerSequence,
		PayloadKind:        distributedtxn.ReplicatedPayloadParticipantStage,
		ControllerEpoch:    request.Key.IssuerEpoch,
		ExecutionPinDigest: directMutationAuthorityDigest(requestledger.Digest(laneHome)),
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(participant.Route.Group.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(participant.Route.Group.ShardIncarnation),
			CoordinatorAllocation:       participant.Route.AllocationGeneration,
			BucketBits:                  participant.BucketBits, IntentScopes: participant.IntentScopes,
			MutationDigest: mutationDigest,
		},
	}
	controlBytes, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, fmt.Errorf("gateway: encode direct control: %w", errors.Join(err, ErrReplicatedTransaction))
	}
	sequence, err := replication.TransactionClientSequence(controlBytes)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, errors.Join(err, ErrReplicatedTransaction)
	}
	outer := replicatedTransactionCommandHeader(
		participant.Route, request.Tenant,
		durableRequestRetryHome(replication.Digest(keyDigest), id),
		replication.ID128(id), uint64(control.Role), sequence,
	)
	outer.Kind = replication.CommandTransaction
	outer.AuthorityClass = replication.CommandAuthorityMembershipStableData
	outer.Transaction = controlBytes
	outer.Batches = participant.Batches
	outer.Fingerprint = directMutationFingerprint(keyDigest, request.RequestDigest, controlBytes)
	dst, err = replication.AppendCommand(dst, outer)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, fmt.Errorf("gateway: encode direct command: %w", errors.Join(err, ErrReplicatedTransaction))
	}
	return dst, control, nil
}

func directMutationTransactionID(key requestledger.Digest) distributedtxn.ID {
	hash := sha256.New()
	_, _ = hash.Write(directMutationIdentityDomain)
	_, _ = hash.Write(key[:])
	sum := hash.Sum(nil)
	var id distributedtxn.ID
	copy(id[:], sum)
	return id
}

func directMutationAuthorityDigest(
	lane requestledger.Digest,
) distributedtxn.Digest {
	hash := sha256.New()
	_, _ = hash.Write(directMutationAuthorityDomain)
	_, _ = hash.Write(lane[:])
	var digest distributedtxn.Digest
	hash.Sum(digest[:0])
	return digest
}

func directMutationFingerprint(
	key requestledger.Digest,
	request replication.Digest,
	control []byte,
) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(directMutationFingerprintDomain)
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(request[:])
	_, _ = hash.Write(control)
	var digest replication.Digest
	hash.Sum(digest[:0])
	return digest
}
