package gateway

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

const transactionInitialRevision = 1

var (
	// ErrBatchEmpty reports an empty atomic write batch.
	ErrBatchEmpty = errors.New("gateway: atomic write batch is empty")
	// ErrBatchClassMismatch reports a batch whose statements request different
	// operational deadline/concurrency profiles.
	ErrBatchClassMismatch = errors.New("gateway: atomic write batch mixes operation classes")
	// ErrTransactionParticipantLimit reports a batch spanning more durable
	// participants than the bounded transaction record permits.
	ErrTransactionParticipantLimit = errors.New("gateway: distributed transaction exceeds the participant limit")
	// ErrTransactionCommitted reports a durably committed decision whose apply
	// or release cleanup did not finish before this request returned.
	ErrTransactionCommitted = errors.New("gateway: distributed transaction is committed and requires recovery")
)

// CommittedTransactionError preserves the raw transaction identity when the
// commit point is durable but synchronous publication or cleanup is incomplete.
// Retrying the original SQL as a new transaction is unsafe; recovery must use
// ID and the retained participant records.
type CommittedTransactionError struct {
	ID    distributedtxn.ID
	Cause error
}

// TransactionOutcomeUnknownError preserves the identity required to resolve a
// coordinator decision when the gateway cannot prove whether commit won.
type TransactionOutcomeUnknownError struct {
	ID    distributedtxn.ID
	Cause error
}

func (e *TransactionOutcomeUnknownError) Error() string {
	return fmt.Sprintf("%v: %x: %v", ErrCommitOutcomeUnknown, e.ID, e.Cause)
}

func (e *TransactionOutcomeUnknownError) Unwrap() []error {
	return []error{ErrCommitOutcomeUnknown, e.Cause}
}

func (e *CommittedTransactionError) Error() string {
	return fmt.Sprintf("%v: %x: %v", ErrTransactionCommitted, e.ID, e.Cause)
}

func (e *CommittedTransactionError) Unwrap() []error {
	return []error{ErrTransactionCommitted, e.Cause}
}

type transactionParticipant struct {
	call       shardCall
	statements []shardservice.MutationStatement
	bucketBits uint8
	scopes     []distributedtxn.IntentScope
	mutation   []byte
	digest     distributedtxn.Digest
	record     []byte
}

// ExecBatch executes multiple routed mutations atomically. Every statement is
// prepared and routed against one pinned catalog generation before any network
// I/O. The ordinary Exec path remains the lower-latency single-statement fast
// lane; a one-statement batch delegates to it.
func (e *Executor) ExecBatch(ctx context.Context, queries []Query) (*Result, error) {
	if len(queries) == 0 {
		return nil, ErrBatchEmpty
	}
	if len(queries) == 1 {
		return e.Exec(ctx, queries[0])
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	class := queries[0].Class
	for i := 1; i < len(queries); i++ {
		if queries[i].Class != class {
			return nil, ErrBatchClassMismatch
		}
	}
	profile := e.profileFor(class)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	snapshot, err := e.pin(opctx, 0, 0)
	if err != nil {
		return nil, err
	}
	participants, err := e.planTransaction(opctx, snapshot, queries, profile)
	if err != nil {
		return nil, err
	}
	if len(participants) == 0 {
		return &Result{
			Kind: shardservice.ResponseCompletion, RouteKind: distribution.RouteEmpty,
			Generation: snapshot.Generation(),
		}, nil
	}
	if len(participants) > distributedtxn.MaxParticipants {
		return nil, ErrTransactionParticipantLimit
	}
	return e.executeTransaction(opctx, snapshot, participants, profile)
}

func (e *Executor) planTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
) ([]transactionParticipant, error) {
	participants := make([]transactionParticipant, 0, min(len(queries), distributedtxn.MaxParticipants))
	for i := range queries {
		query := &queries[i]
		args, err := queryRuntimeArgs(query.Params)
		if err != nil {
			return nil, err
		}
		prepared, err := snapshot.Prepare(ctx, query.SQL)
		if err != nil {
			return nil, err
		}
		if prepared.statement.Kind == sqlast.KindSelect {
			return nil, ErrExecRequiresMutation
		}
		bound, err := prepared.BindWrite(args)
		if err != nil {
			return nil, err
		}
		call, _, _, err := e.routeWrite(snapshot, query, bound, profile)
		if err != nil {
			return nil, err
		}
		if call == nil {
			continue
		}
		participants, err = appendBoundWriteParticipants(
			participants, *call, query, bound, profile,
		)
		if err != nil {
			return nil, err
		}
	}
	sortTransactionParticipants(participants)
	return participants, nil
}

func appendBoundWriteParticipants(
	participants []transactionParticipant,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
) ([]transactionParticipant, error) {
	var err error
	participants, err = appendTransactionStatement(
		participants, baseCall,
		shardservice.MutationStatement{SQL: query.SQL, Params: query.Params},
	)
	if err != nil {
		return nil, err
	}
	for i := range bound.globalIndexes {
		index := &bound.globalIndexes[i]
		call := shardCall{
			target: index.target, address: index.address,
			req: &shardservice.ShardRequest{
				Distribution: index.distribution, Shard: index.target.Shard,
				AllocationGeneration: index.target.AllocationGeneration,
				RoutingVersion:       index.routingVersion,
				OwnershipEpoch:       index.target.OwnershipEpoch,
				ReadPolicy:           profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadWrite,
				Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
				MaxResultBytes: profile.PerShardBytes,
				BucketBits:     index.bucketBits,
				AccessScopes:   []distributedtxn.IntentScope{index.scope},
			},
		}
		entryKey := bound.globalIndexArena[index.entryStart:index.entryEnd]
		value := bound.globalIndexArena[index.valueStart:index.valueEnd]
		participants, err = appendTransactionStatement(
			participants, call, shardservice.MutationStatement{
				Kind:     shardservice.MutationGlobalIndexPut,
				Relation: index.metadata.Relation,
				IndexID:  index.metadata.IndexID, Incarnation: index.metadata.Incarnation,
				EntryKey: entryKey, Value: value,
				LocatorCount: index.metadata.LocatorCount,
				Unique:       index.metadata.Flags&IndexUnique != 0,
			},
		)
		if err != nil {
			return nil, err
		}
	}
	return participants, nil
}

func appendTransactionStatement(
	participants []transactionParticipant,
	call shardCall,
	statement shardservice.MutationStatement,
) ([]transactionParticipant, error) {
	participantIndex := -1
	for i := range participants {
		if sameTransactionTarget(participants[i].call.req, call.req) {
			participantIndex = i
			break
		}
	}
	if participantIndex < 0 {
		participants = append(participants, transactionParticipant{
			call: call, bucketBits: call.req.BucketBits,
			scopes: call.req.AccessScopes,
		})
		participantIndex = len(participants) - 1
		if len(participants) > distributedtxn.MaxParticipants {
			return nil, ErrTransactionParticipantLimit
		}
	}
	participant := &participants[participantIndex]
	if len(participant.statements) != 0 {
		mergeParticipantScopes(participant, call.req.BucketBits, call.req.AccessScopes)
	}
	participant.statements = append(participant.statements, statement)
	return participants, nil
}

func sortTransactionParticipants(participants []transactionParticipant) {
	slices.SortFunc(participants, func(a, b transactionParticipant) int {
		if a.call.req.Distribution < b.call.req.Distribution {
			return -1
		}
		if a.call.req.Distribution > b.call.req.Distribution {
			return 1
		}
		if a.call.req.Shard < b.call.req.Shard {
			return -1
		}
		if a.call.req.Shard > b.call.req.Shard {
			return 1
		}
		return 0
	})
}

func mergeParticipantScopes(
	participant *transactionParticipant,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) {
	if participant.bucketBits == 0 || bucketBits == 0 || participant.bucketBits != bucketBits {
		participant.bucketBits = 0
		participant.scopes = nil
		return
	}
	participant.scopes = append(participant.scopes, scopes...)
	participant.scopes = coalesceIntentScopes(participant.scopes)
	if len(participant.scopes) > distributedtxn.MaxIntentScopes {
		participant.bucketBits = 0
		participant.scopes = nil
	}
}

func sameTransactionTarget(a, b *shardservice.ShardRequest) bool {
	return a.Distribution == b.Distribution && a.Shard == b.Shard &&
		a.AllocationGeneration == b.AllocationGeneration &&
		a.RoutingVersion == b.RoutingVersion && a.OwnershipEpoch == b.OwnershipEpoch
}

func (e *Executor) executeTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	participants []transactionParticipant,
	profile Profile,
) (*Result, error) {
	id, err := newTransactionID(cryptorand.Reader)
	if err != nil {
		return nil, err
	}
	coordinator := &participants[0]
	refs := make([]distributedtxn.ParticipantRef, len(participants))
	for i := range participants {
		participant := &participants[i]
		participant.mutation, err = shardservice.AppendMutationBatch(nil, participant.statements)
		if err != nil {
			return nil, err
		}
		participant.digest = distributedtxn.ParticipantDigest(
			participant.bucketBits, participant.scopes, participant.mutation,
		)
		request := participant.call.req
		refs[i] = distributedtxn.ParticipantRef{
			Distribution: []byte(request.Distribution), Shard: []byte(request.Shard),
			RoutingVersion:       uint64(request.RoutingVersion),
			AllocationGeneration: uint64(request.AllocationGeneration),
			OwnershipEpoch:       uint64(request.OwnershipEpoch), MutationDigest: participant.digest,
			State: distributedtxn.ParticipantStaged,
		}
	}
	coordinatorRecord, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: transactionInitialRevision,
		CatalogGeneration: snapshot.Generation(),
		RecoveryDeadline:  time.Now().Add(profile.GlobalDeadline).UnixNano(),
		Participants:      refs,
	})
	if err != nil {
		return nil, err
	}
	coordinatorRequest := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionStageCoordinator,
		id, 0, coordinatorRecord,
	)
	if _, err := e.transactionRoundTrip(ctx, coordinator.call.address, coordinatorRequest, profile); err != nil {
		lookup := transactionRequest(
			coordinator.call.req, profile, shardservice.TransactionLookupCoordinator, id, 0, nil,
		)
		response, lookupErr := e.transactionRoundTrip(ctx, coordinator.call.address, lookup, profile)
		if errors.Is(lookupErr, ErrTransactionNotFound) {
			return nil, err
		}
		if lookupErr != nil || response.Transaction.CoordinatorState != distributedtxn.CoordinatorStaging {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, lookupErr)}
		}
		abort := transactionRequest(
			coordinator.call.req, profile, shardservice.TransactionAbortCoordinator,
			id, response.Transaction.Revision, nil,
		)
		if _, abortErr := e.transactionRoundTrip(ctx, coordinator.call.address, abort, profile); abortErr != nil {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, abortErr)}
		}
		return nil, err
	}
	for i := range participants {
		participant := &participants[i]
		participant.record, err = distributedtxn.AppendParticipant(nil, distributedtxn.ParticipantRecord{
			ID: id, State: distributedtxn.ParticipantStaged, Revision: transactionInitialRevision,
			RoutingVersion:            uint64(participant.call.req.RoutingVersion),
			AllocationGeneration:      uint64(participant.call.req.AllocationGeneration),
			OwnershipEpoch:            uint64(participant.call.req.OwnershipEpoch),
			CoordinatorDistribution:   []byte(coordinator.call.req.Distribution),
			CoordinatorShard:          []byte(coordinator.call.req.Shard),
			CoordinatorAllocation:     uint64(coordinator.call.req.AllocationGeneration),
			CoordinatorRoutingVersion: uint64(coordinator.call.req.RoutingVersion),
			CoordinatorOwnershipEpoch: uint64(coordinator.call.req.OwnershipEpoch),
			BucketBits:                participant.bucketBits, IntentScopes: participant.scopes,
			MutationDigest: participant.digest, Mutation: participant.mutation,
		})
		if err != nil {
			abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
			if abortErr != nil {
				return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, abortErr)}
			}
			return nil, err
		}
	}
	if _, err := e.participantPhase(ctx, id, participants, profile, shardservice.TransactionStageParticipant, 0); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
		if abortErr != nil {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, abortErr)}
		}
		return nil, err
	}
	if _, err := e.participantPhase(
		ctx, id, participants, profile, shardservice.TransactionPrepareParticipant,
		transactionInitialRevision,
	); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
		if abortErr != nil {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, abortErr)}
		}
		return nil, err
	}
	if err := e.commitCoordinator(ctx, id, coordinator, profile); err != nil {
		// A failed commit is resolved by lookup. commitCoordinator returns only
		// when it proved no committed decision or could not resolve the outcome.
		if errors.Is(err, ErrCommitOutcomeUnknown) {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: err}
		}
		return nil, err
	}
	responses, applyErr := e.participantPhase(
		ctx, id, participants, profile, shardservice.TransactionApplyParticipant,
		transactionInitialRevision+1,
	)
	if applyErr != nil {
		return nil, &CommittedTransactionError{ID: id, Cause: applyErr}
	}
	var affected int64
	for i := range responses {
		if responses[i] == nil || responses[i].RowsAffected < 0 ||
			responses[i].RowsAffected > math.MaxInt64-affected {
			return nil, &CommittedTransactionError{ID: id, Cause: ErrTransactionConflict}
		}
		affected += responses[i].RowsAffected
	}
	if _, releaseErr := e.participantPhase(
		ctx, id, participants, profile, shardservice.TransactionReleaseParticipant,
		transactionInitialRevision+2,
	); releaseErr != nil {
		return nil, &CommittedTransactionError{ID: id, Cause: releaseErr}
	}
	retire := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionRetireCoordinator,
		id, transactionInitialRevision+1, nil,
	)
	if _, err := e.transactionRoundTrip(ctx, coordinator.call.address, retire, profile); err != nil {
		return nil, &CommittedTransactionError{ID: id, Cause: err}
	}
	kind := distribution.RouteSingle
	if len(participants) > 1 {
		kind = distribution.RouteTargeted
	}
	e.metrics.observeRoute(kind, len(participants), ScatterNone)
	return &Result{
		Kind: shardservice.ResponseCompletion, RowsAffected: affected,
		RouteKind: kind, Generation: snapshot.Generation(), ShardsFanned: len(participants),
	}, nil
}

func newTransactionID(reader io.Reader) (distributedtxn.ID, error) {
	for {
		var id distributedtxn.ID
		if _, err := io.ReadFull(reader, id[:]); err != nil {
			return distributedtxn.ID{}, fmt.Errorf("gateway: generate transaction identity: %w", err)
		}
		if !id.IsZero() {
			return id, nil
		}
	}
}

func transactionRequest(
	template *shardservice.ShardRequest,
	profile Profile,
	operation shardservice.TransactionOperation,
	id distributedtxn.ID,
	revision uint64,
	record []byte,
) *shardservice.ShardRequest {
	transaction := shardservice.TransactionRequest{Operation: operation}
	if operation == shardservice.TransactionStageCoordinator ||
		operation == shardservice.TransactionStageParticipant {
		transaction.Record = record
	} else {
		transaction.ID = id
		transaction.Revision = revision
	}
	return &shardservice.ShardRequest{
		Distribution: template.Distribution, Shard: template.Shard,
		AllocationGeneration: template.AllocationGeneration,
		RoutingVersion:       template.RoutingVersion, OwnershipEpoch: template.OwnershipEpoch,
		ReadPolicy: profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadWrite,
		Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
		MaxResultBytes: profile.PerShardBytes,
		Transaction:    transaction,
	}
}

func (e *Executor) transactionRoundTrip(
	ctx context.Context,
	address string,
	request *shardservice.ShardRequest,
	profile Profile,
) (*shardservice.ShardResponse, error) {
	requestContext, cancel := context.WithTimeout(ctx, profile.PerShardDeadline)
	defer cancel()
	return e.client.Do(requestContext, address, request)
}

func (e *Executor) participantPhase(
	ctx context.Context,
	id distributedtxn.ID,
	participants []transactionParticipant,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) ([]*shardservice.ShardResponse, error) {
	responses := make([]*shardservice.ShardResponse, len(participants))
	errs := make([]error, len(participants))
	jobs := make(chan int)
	workers := min(max(1, profile.MaxConcurrency), len(participants))
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for i := range jobs {
				participant := &participants[i]
				var record []byte
				if operation == shardservice.TransactionStageParticipant {
					record = participant.record
				}
				request := transactionRequest(
					participant.call.req, profile, operation, id, revision, record,
				)
				responses[i], errs[i] = e.transactionRoundTrip(
					ctx, participant.call.address, request, profile,
				)
			}
		})
	}
	for i := range participants {
		select {
		case jobs <- i:
		case <-ctx.Done():
			errs[i] = ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	return responses, errors.Join(errs...)
}

func (e *Executor) commitCoordinator(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionParticipant,
	profile Profile,
) error {
	commit := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionCommitCoordinator,
		id, transactionInitialRevision, nil,
	)
	if response, err := e.transactionRoundTrip(ctx, coordinator.call.address, commit, profile); err == nil {
		if response.Transaction.CoordinatorState == distributedtxn.CoordinatorCommitted {
			return nil
		}
		return ErrTransactionConflict
	}
	lookup := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionLookupCoordinator, id, 0, nil,
	)
	response, lookupErr := e.transactionRoundTrip(ctx, coordinator.call.address, lookup, profile)
	if lookupErr != nil {
		return errors.Join(ErrCommitOutcomeUnknown, lookupErr)
	}
	switch response.Transaction.CoordinatorState {
	case distributedtxn.CoordinatorCommitted:
		return nil
	case distributedtxn.CoordinatorStaging:
		response, err := e.transactionRoundTrip(ctx, coordinator.call.address, commit, profile)
		if err == nil && response.Transaction.CoordinatorState == distributedtxn.CoordinatorCommitted {
			return nil
		}
		return errors.Join(ErrCommitOutcomeUnknown, err)
	default:
		return ErrTransactionConflict
	}
}

func (e *Executor) abortTransaction(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionParticipant,
	participants []transactionParticipant,
	profile Profile,
) error {
	participantErrors := make([]error, len(participants))
	for i := range participants {
		participant := &participants[i]
		lookup := transactionRequest(
			participant.call.req, profile, shardservice.TransactionLookupParticipant,
			id, 0, nil,
		)
		response, err := e.transactionRoundTrip(ctx, participant.call.address, lookup, profile)
		if errors.Is(err, ErrTransactionNotFound) {
			abort := transactionRequest(
				participant.call.req, profile, shardservice.TransactionAbortParticipant,
				id, transactionInitialRevision, nil,
			)
			_, participantErrors[i] = e.transactionRoundTrip(
				ctx, participant.call.address, abort, profile,
			)
			continue
		}
		if err != nil {
			participantErrors[i] = err
			continue
		}
		state := response.Transaction.ParticipantState
		if state == distributedtxn.ParticipantAborted || state == distributedtxn.ParticipantReleased {
			continue
		}
		if state != distributedtxn.ParticipantStaged && state != distributedtxn.ParticipantPrepared {
			participantErrors[i] = ErrTransactionConflict
			continue
		}
		abort := transactionRequest(
			participant.call.req, profile, shardservice.TransactionAbortParticipant,
			id, response.Transaction.Revision, nil,
		)
		_, participantErrors[i] = e.transactionRoundTrip(ctx, participant.call.address, abort, profile)
	}
	abort := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionAbortCoordinator,
		id, transactionInitialRevision, nil,
	)
	_, coordinatorErr := e.transactionRoundTrip(ctx, coordinator.call.address, abort, profile)
	return errors.Join(errors.Join(participantErrors...), coordinatorErr)
}
