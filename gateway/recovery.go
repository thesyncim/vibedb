package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

var ErrRecoveryNotReady = errors.New("gateway: distributed transaction recovery deadline has not elapsed")

// RecoveryResult describes one transaction resolved entirely from shard-owned
// durable state.
type RecoveryResult struct {
	ID           distributedtxn.ID
	State        distributedtxn.CoordinatorState
	Participants int
	RowsAffected int64
}

type recoveryCoordinator struct {
	call     shardCall
	response *shardservice.ShardResponse
}

// RecoverTransaction locates id's coordinator in the pinned catalog and
// redrives its monotone state machine. A missing participant may force abort
// only after the coordinator's recovery deadline and before a commit decision.
func (e *Executor) RecoverTransaction(ctx context.Context, id distributedtxn.ID) (RecoveryResult, error) {
	if id.IsZero() {
		return RecoveryResult{}, ErrTransactionNotFound
	}
	if ctx == nil {
		ctx = context.Background()
	}
	profile := e.profileFor(ClassAdmin)
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	snapshot, lease, err := e.pin(opctx, 0, 0)
	if err != nil {
		return RecoveryResult{}, err
	}
	defer lease.release()
	coordinator, err := e.findRecoveryCoordinator(opctx, snapshot, id, profile)
	if err != nil {
		return RecoveryResult{}, err
	}
	return e.recoverCoordinator(opctx, snapshot, coordinator, profile)
}

func (e *Executor) findRecoveryCoordinator(
	ctx context.Context,
	snapshot *Snapshot,
	id distributedtxn.ID,
	profile Profile,
) (recoveryCoordinator, error) {
	calls, err := transactionCatalogCalls(snapshot, profile)
	if err != nil {
		return recoveryCoordinator{}, err
	}
	found := make([]recoveryCoordinator, len(calls))
	errs := make([]error, len(calls))
	jobs := make(chan int)
	workers := min(max(1, profile.MaxConcurrency), len(calls))
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for i := range jobs {
				request := transactionRequest(
					calls[i].req, profile, shardservice.TransactionLookupCoordinator, id, 0, nil,
				)
				response, err := e.transactionRoundTrip(ctx, calls[i].address, request, profile)
				if errors.Is(err, ErrTransactionNotFound) {
					continue
				}
				if err != nil {
					errs[i] = err
					continue
				}
				found[i] = recoveryCoordinator{call: calls[i], response: response}
			}
		})
	}
	for i := range calls {
		select {
		case jobs <- i:
		case <-ctx.Done():
			errs[i] = ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	if err := errors.Join(errs...); err != nil {
		return recoveryCoordinator{}, err
	}
	var coordinator recoveryCoordinator
	count := 0
	for i := range found {
		if found[i].response != nil {
			coordinator = found[i]
			count++
		}
	}
	if count == 0 {
		return recoveryCoordinator{}, ErrTransactionNotFound
	}
	if count != 1 {
		return recoveryCoordinator{}, ErrTransactionConflict
	}
	return coordinator, nil
}

func transactionCatalogCalls(snapshot *Snapshot, profile Profile) ([]shardCall, error) {
	if snapshot == nil {
		return nil, ErrNoCatalog
	}
	count := 0
	for _, manifest := range snapshot.config.Manifests {
		count += manifest.ShardCount()
	}
	calls := make([]shardCall, 0, count)
	for _, manifest := range snapshot.config.Manifests {
		for i := 0; i < manifest.ShardCount(); i++ {
			shard, ok := manifest.ShardInfo(i)
			if !ok || len(shard.Leaders) == 0 {
				return nil, ErrTransactionConflict
			}
			address, err := snapshot.Address(shard.Leaders[0])
			if err != nil {
				return nil, err
			}
			request := &shardservice.ShardRequest{
				Distribution: manifest.Distribution(), Shard: shard.ID,
				AllocationGeneration: shard.AllocationGeneration,
				RoutingVersion:       manifest.Version(), OwnershipEpoch: shard.Epoch,
				ReadPolicy: profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadWrite,
				Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
				MaxResultBytes: profile.PerShardBytes,
			}
			calls = append(calls, shardCall{address: address, req: request})
		}
	}
	return calls, nil
}

func (e *Executor) recoverCoordinator(
	ctx context.Context,
	snapshot *Snapshot,
	coordinator recoveryCoordinator,
	profile Profile,
) (RecoveryResult, error) {
	reply := coordinator.response.Transaction
	var arena [distributedtxn.MaxParticipants]distributedtxn.ParticipantRef
	record, err := distributedtxn.OpenCoordinatorInto(reply.Record, arena[:])
	if err != nil || record.ID != reply.ID {
		return RecoveryResult{}, errors.Join(ErrTransactionConflict, err)
	}
	participants, err := resolveRecoveryParticipants(snapshot, record.Participants, profile)
	if err != nil {
		return RecoveryResult{}, err
	}
	result := RecoveryResult{ID: record.ID, State: reply.CoordinatorState, Participants: len(participants)}
	participantReplies, missing, err := e.readRecoveryParticipants(ctx, record, participants, profile)
	if err != nil {
		return RecoveryResult{}, err
	}

	switch reply.CoordinatorState {
	case distributedtxn.CoordinatorStaging:
		if missing != 0 {
			if time.Now().UnixNano() < record.RecoveryDeadline {
				return result, ErrRecoveryNotReady
			}
			if err := e.abortTransaction(ctx, record.ID, &participants[0], participants, profile); err != nil {
				return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: err}
			}
			return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
		}
		for i := range participantReplies {
			state := participantReplies[i].Transaction.ParticipantState
			if state == distributedtxn.ParticipantAborted {
				if err := e.abortTransaction(ctx, record.ID, &participants[0], participants, profile); err != nil {
					return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: err}
				}
				return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
			}
			if state != distributedtxn.ParticipantStaged && state != distributedtxn.ParticipantPrepared {
				return result, ErrTransactionConflict
			}
		}
		if _, err := e.participantPhase(
			ctx, record.ID, participants, profile, shardservice.TransactionPrepareParticipant, 1,
		); err != nil {
			abortErr := e.abortTransaction(ctx, record.ID, &participants[0], participants, profile)
			if abortErr != nil {
				return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: errors.Join(err, abortErr)}
			}
			return result, err
		}
		if err := e.commitCoordinator(ctx, record.ID, &participants[0], profile); err != nil {
			if errors.Is(err, ErrCommitOutcomeUnknown) {
				return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: err}
			}
			return result, err
		}
		result.State = distributedtxn.CoordinatorCommitted
	case distributedtxn.CoordinatorCommitted:
		if missing != 0 {
			return result, ErrTransactionConflict
		}
	case distributedtxn.CoordinatorAborted:
		if err := e.abortTransaction(ctx, record.ID, &participants[0], participants, profile); err != nil {
			return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: err}
		}
		return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, reply.Revision, result, profile)
	case distributedtxn.CoordinatorRetired:
		return result, nil
	default:
		return result, ErrTransactionConflict
	}

	responses, err := e.participantPhase(
		ctx, record.ID, participants, profile, shardservice.TransactionApplyParticipant, 2,
	)
	if err != nil {
		return result, &CommittedTransactionError{ID: record.ID, Cause: err}
	}
	for i := range responses {
		if responses[i] == nil || responses[i].RowsAffected < 0 {
			return result, ErrTransactionConflict
		}
		result.RowsAffected += responses[i].RowsAffected
	}
	if _, err := e.participantPhase(
		ctx, record.ID, participants, profile, shardservice.TransactionReleaseParticipant, 3,
	); err != nil {
		return result, &CommittedTransactionError{ID: record.ID, Cause: err}
	}
	return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
}

func resolveRecoveryParticipants(
	snapshot *Snapshot,
	refs []distributedtxn.ParticipantRef,
	profile Profile,
) ([]transactionParticipant, error) {
	participants := make([]transactionParticipant, len(refs))
	for i := range refs {
		ref := &refs[i]
		var matched *distribution.Manifest
		for _, manifest := range snapshot.config.Manifests {
			if vibejson.BytesEqualString(ref.Distribution, string(manifest.Distribution())) {
				matched = manifest
				break
			}
		}
		if matched == nil || uint64(matched.Version()) != ref.RoutingVersion {
			return nil, ErrTransactionConflict
		}
		found := false
		for shardIndex := 0; shardIndex < matched.ShardCount(); shardIndex++ {
			shard, ok := matched.ShardInfo(shardIndex)
			if !ok || !vibejson.BytesEqualString(ref.Shard, string(shard.ID)) {
				continue
			}
			if uint64(shard.AllocationGeneration) != ref.AllocationGeneration ||
				uint64(shard.Epoch) != ref.OwnershipEpoch || len(shard.Leaders) == 0 {
				return nil, ErrTransactionConflict
			}
			address, err := snapshot.Address(shard.Leaders[0])
			if err != nil {
				return nil, err
			}
			request := &shardservice.ShardRequest{
				Distribution: matched.Distribution(), Shard: shard.ID,
				AllocationGeneration: shard.AllocationGeneration,
				RoutingVersion:       matched.Version(), OwnershipEpoch: shard.Epoch,
				ReadPolicy: profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadWrite,
				Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
				MaxResultBytes: profile.PerShardBytes,
			}
			participants[i].call = shardCall{address: address, req: request}
			found = true
			break
		}
		if !found {
			return nil, ErrTransactionConflict
		}
	}
	return participants, nil
}

func (e *Executor) readRecoveryParticipants(
	ctx context.Context,
	coordinator distributedtxn.CoordinatorRecord,
	participants []transactionParticipant,
	profile Profile,
) ([]*shardservice.ShardResponse, int, error) {
	responses := make([]*shardservice.ShardResponse, len(participants))
	missing := 0
	for i := range participants {
		request := transactionRequest(
			participants[i].call.req, profile, shardservice.TransactionReadParticipant,
			coordinator.ID, 0, nil,
		)
		response, err := e.transactionRoundTrip(ctx, participants[i].call.address, request, profile)
		if errors.Is(err, ErrTransactionNotFound) {
			missing++
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		if response.Transaction.ParticipantState == distributedtxn.ParticipantAborted &&
			len(response.Transaction.Record) == 0 {
			responses[i] = response
			continue
		}
		record, err := distributedtxn.OpenParticipant(response.Transaction.Record)
		if err != nil || record.ID != coordinator.ID ||
			record.MutationDigest != coordinator.Participants[i].MutationDigest ||
			record.RoutingVersion != coordinator.Participants[i].RoutingVersion ||
			record.AllocationGeneration != coordinator.Participants[i].AllocationGeneration ||
			record.OwnershipEpoch != coordinator.Participants[i].OwnershipEpoch {
			return nil, 0, errors.Join(ErrTransactionConflict, err)
		}
		responses[i] = response
	}
	return responses, missing, nil
}

func (e *Executor) retireRecoveredCoordinator(
	ctx context.Context,
	coordinator recoveryCoordinator,
	id distributedtxn.ID,
	revision uint64,
	result RecoveryResult,
	profile Profile,
) (RecoveryResult, error) {
	retire := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionRetireCoordinator,
		id, revision, nil,
	)
	response, err := e.transactionRoundTrip(ctx, coordinator.call.address, retire, profile)
	if err != nil {
		return result, err
	}
	result.State = response.Transaction.CoordinatorState
	return result, nil
}

// RecoverAll scans every current shard's bounded coordinator index and redrives
// each active identity once. The first error is joined after independent work
// continues; not-yet-expired incomplete transactions are left for a later pass.
func (e *Executor) RecoverAll(ctx context.Context) ([]RecoveryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	profile := e.profileFor(ClassAdmin)
	snapshot, lease, err := e.pin(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	defer lease.release()
	calls, err := transactionCatalogCalls(snapshot, profile)
	if err != nil {
		return nil, err
	}
	seen := make(map[distributedtxn.ID]struct{})
	var (
		results []RecoveryResult
		errs    []error
	)
	for i := range calls {
		var cursor distributedtxn.ID
		for {
			request := transactionRequest(
				calls[i].req, profile, shardservice.TransactionScanCoordinator, cursor, 0, nil,
			)
			response, err := e.transactionRoundTrip(ctx, calls[i].address, request, profile)
			if err != nil {
				errs = append(errs, err)
				break
			}
			if response.Transaction.Role == shardservice.TransactionRoleNone {
				break
			}
			cursor = response.Transaction.ID
			if _, ok := seen[cursor]; ok {
				continue
			}
			seen[cursor] = struct{}{}
			result, err := e.recoverCoordinator(ctx, snapshot,
				recoveryCoordinator{call: calls[i], response: response}, profile)
			if errors.Is(err, ErrRecoveryNotReady) {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("recover %x: %w", cursor, err))
				continue
			}
			results = append(results, result)
		}
	}
	return results, errors.Join(errs...)
}

// RunRecovery periodically scans and redrives active coordinators until ctx is
// canceled. report receives completed passes and failures; it may be nil.
func (e *Executor) RunRecovery(
	ctx context.Context,
	interval time.Duration,
	report func([]RecoveryResult, error),
) {
	ctx = e.recoveryContext(ctx)
	if interval <= 0 {
		interval = 5 * time.Second
	}
	run := func() {
		results, err := e.RecoverAll(ctx)
		if report != nil && (len(results) != 0 || err != nil) {
			report(results, err)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (e *Executor) recoveryContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, present := serviceauthz.FromContext(ctx); present || e == nil || !e.internalAuthority.Valid() {
		return ctx
	}
	authorized, err := serviceauthz.WithAuthority(ctx, e.internalAuthority)
	if err != nil {
		return ctx
	}
	return authorized
}
