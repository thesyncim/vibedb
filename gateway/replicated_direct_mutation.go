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
	"github.com/thesyncim/vibejson/x/byteview"
)

const directMutationIdentityDomain = "vibedb/direct-mutation/request\x00"
const directMutationAuthorityDomain = "vibedb/direct-mutation/authority\x00"
const directMutationFingerprintDomain = "vibedb/direct-mutation/fingerprint\x00"

// ReplicatedDirectMutation is the single-group write contract. The caller has
// already routed and lowered its complete logical operation to canonical native
// relation batches. RequestKey carries a serialized issuer-lane sequence;
// RequestDigest binds that sequence to the exact caller request.
type ReplicatedDirectMutation struct {
	Key           requestledger.RequestKey
	RequestDigest replication.Digest
	Tenant        []byte
	Target        ReplicatedTransactionTarget
}

// ReplicatedDirectMutationResult is the exact retained terminal result from the
// target group. One successful invocation consumes one Raft proposal; an
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

// directMutationEncodeWorkspace owns every bounded temporary needed to build
// one direct command. It is deliberately caller-owned and not concurrency-safe:
// request lanes reserve one workspace before entering the hot path.
type directMutationEncodeWorkspace struct {
	control  [distributedtxn.MaxSingleTargetControlBytes]byte
	digester replication.TransactionMutationDigester
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
	proposal, err := executor.Propose(ctx, request.Target.Route, command)
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
	return appendReplicatedDirectMutationCommandPrepared(dst, nil, request)
}

// appendReplicatedDirectMutationCommandPrepared encodes one direct mutation
// using caller-owned output and fixed control storage. A warmed workspace and
// enough capacity in dst make the successful path allocation-free.
func appendReplicatedDirectMutationCommandPrepared(
	dst []byte,
	workspace *directMutationEncodeWorkspace,
	request ReplicatedDirectMutation,
) ([]byte, distributedtxn.ReplicatedCommand, error) {
	target := request.Target
	if !request.Key.Valid() || request.Key.IssuerEpoch == 0 ||
		request.Key.IssuerSequence == 0 || request.Key.IssuerLane == (requestledger.IssuerLane{}) ||
		request.RequestDigest == (replication.Digest{}) ||
		len(request.Tenant) == 0 || len(request.Tenant) > replication.MaxIdentityBytes ||
		requestledger.Digest(sha256.Sum256(request.Tenant)) != request.Key.TenantDigest ||
		!validReplicatedRoute(target.Route) ||
		!distributedtxn.ValidateIntentScopes(target.IntentScopes, target.BucketBits) ||
		len(target.Batches) == 0 {
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
	var mutationDigest distributedtxn.Digest
	if workspace == nil {
		mutationDigest, err = replication.TransactionMutationDigest(target.Batches)
	} else {
		mutationDigest, err = workspace.digester.Digest(target.Batches)
	}
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, errors.Join(err, ErrReplicatedTransaction)
	}
	id := directMutationTransactionID(requestledger.Digest(laneHome))
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleTarget,
		Operation: distributedtxn.ReplicatedApplySingleTarget,
		ID:        id, ExpectedRevision: request.Key.IssuerSequence,
		PayloadKind:        distributedtxn.ReplicatedPayloadTargetStage,
		ControllerEpoch:    request.Key.IssuerEpoch,
		ExecutionPinDigest: directMutationAuthorityDigest(requestledger.Digest(laneHome)),
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(target.Route.Group.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(target.Route.Group.ShardIncarnation),
			CoordinatorAllocation:       target.Route.AllocationGeneration,
			BucketBits:                  target.BucketBits, IntentScopes: target.IntentScopes,
			MutationDigest: mutationDigest,
		},
	}
	var controlDst []byte
	if workspace != nil {
		controlDst = workspace.control[:0]
	}
	controlBytes, err := distributedtxn.AppendReplicatedCommand(controlDst, control)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, fmt.Errorf("gateway: encode direct control: %w", errors.Join(err, ErrReplicatedTransaction))
	}
	outer := replicatedTransactionCommandHeader(
		target.Route, request.Tenant,
		durableRequestRetryHome(replication.Digest(keyDigest), id),
		replication.ID128(id), uint64(control.Role), request.Key.IssuerSequence,
	)
	outer.Kind = replication.CommandTransaction
	outer.AuthorityClass = replication.CommandAuthorityMembershipStableData
	outer.Transaction = controlBytes
	outer.Batches = target.Batches
	outer.Fingerprint = directMutationFingerprint(keyDigest, request.RequestDigest, controlBytes)
	dst, err = replication.AppendCommand(dst, outer)
	if err != nil {
		return dst, distributedtxn.ReplicatedCommand{}, fmt.Errorf("gateway: encode direct command: %w", errors.Join(err, ErrReplicatedTransaction))
	}
	return dst, control, nil
}

func directMutationTransactionID(key requestledger.Digest) distributedtxn.ID {
	var framed [len(directMutationIdentityDomain) + sha256.Size]byte
	at := copy(framed[:], directMutationIdentityDomain)
	copy(framed[at:], key[:])
	sum := sha256.Sum256(framed[:])
	var id distributedtxn.ID
	copy(id[:], sum[:])
	return id
}

func directMutationAuthorityDigest(
	lane requestledger.Digest,
) distributedtxn.Digest {
	var framed [len(directMutationAuthorityDomain) + sha256.Size]byte
	at := copy(framed[:], directMutationAuthorityDomain)
	copy(framed[at:], lane[:])
	return distributedtxn.Digest(sha256.Sum256(framed[:]))
}

func directMutationFingerprint(
	key requestledger.Digest,
	request replication.Digest,
	control []byte,
) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(directMutationFingerprintDomain))
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(request[:])
	_, _ = hash.Write(control)
	var digest replication.Digest
	hash.Sum(digest[:0])
	return digest
}
