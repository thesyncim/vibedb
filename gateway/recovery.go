package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	ErrRecoveryNotReady           = errors.New("gateway: distributed transaction logical recovery lease has not advanced")
	errRecoveryManifestIncomplete = errors.New("gateway: distributed transaction manifest is incomplete")
)

// RecoveryResult describes one transaction resolved entirely from shard-owned
// durable state.
type RecoveryResult struct {
	ID           distributedtxn.ID
	State        distributedtxn.CoordinatorState
	Targets      int
	RowsAffected int64
}

type recoveryCoordinator struct {
	call     shardCall
	response *shardservice.ShardResponse
}

// RecoverTransaction locates id's coordinator in the pinned catalog and
// redrives its monotone state machine. A missing target may force abort
// only after the coordinator's bounded replicated pulse lease and before a
// commit decision.
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
			// This scanner speaks only the legacy shard-local journal protocol.
			// RF3 recovery reads its replicated hidden relations through the native
			// durable-request runner. A catalog may contain both authorities; never
			// send a legacy SQL-service request to a native RF3 endpoint.
			if _, replicated := snapshot.replicatedShardAt(manifest.Distribution(), shard.ID); replicated {
				continue
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
	switch reply.RecordKind {
	case shardservice.TransactionRecordInlineCoordinator:
		return e.recoverInlineCoordinator(ctx, snapshot, coordinator, profile)
	case shardservice.TransactionRecordManifestCoordinator:
		return e.recoverManifestCoordinator(ctx, snapshot, coordinator, profile)
	default:
		return RecoveryResult{}, ErrTransactionConflict
	}
}

// recoverInlineCoordinator is deliberately the original VTC1 recovery path.
// Keeping it separate prevents the paged VTCM lane from changing the fast
// path's fixed arena, request ordering, or durable transition semantics.
func (e *Executor) recoverInlineCoordinator(
	ctx context.Context,
	snapshot *Snapshot,
	coordinator recoveryCoordinator,
	profile Profile,
) (RecoveryResult, error) {
	reply := coordinator.response.Transaction
	var arena [distributedtxn.MaxInlineTargets]distributedtxn.TransactionTargetRef
	record, err := distributedtxn.OpenCoordinatorInto(reply.Record, arena[:])
	if err != nil || record.ID != reply.ID {
		return RecoveryResult{}, errors.Join(ErrTransactionConflict, err)
	}
	targets, err := resolveRecoveryTargets(snapshot, record.Targets, profile)
	if err != nil {
		return RecoveryResult{}, err
	}
	result := RecoveryResult{ID: record.ID, State: reply.CoordinatorState, Targets: len(targets)}
	targetReplies, missing, err := e.readRecoveryTargets(ctx, record, targets, profile)
	if err != nil {
		return RecoveryResult{}, err
	}

	switch reply.CoordinatorState {
	case distributedtxn.CoordinatorStaging:
		if missing != 0 {
			ready, pulseErr := e.advanceRecoveryPulse(
				ctx, coordinator, record.ID, reply.Revision, reply.RecoveryPulse,
				record.RecoveryDeadline, profile,
			)
			if pulseErr != nil || !ready {
				return result, errors.Join(pulseErr, ErrRecoveryNotReady)
			}
			if err := e.abortTransaction(ctx, record.ID, &targets[0], targets, profile); err != nil {
				return result, transactionAbortFailure(record.ID, nil, err)
			}
			return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
		}
		for i := range targetReplies {
			state := targetReplies[i].Transaction.TargetState
			if state == distributedtxn.TargetAborted {
				if err := e.abortTransaction(ctx, record.ID, &targets[0], targets, profile); err != nil {
					return result, transactionAbortFailure(record.ID, nil, err)
				}
				return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
			}
			if state != distributedtxn.TargetStaged && state != distributedtxn.TargetPrepared {
				return result, ErrTransactionConflict
			}
		}
		if _, err := e.targetPhase(
			ctx, record.ID, targets, profile, shardservice.TransactionPrepareTarget, 1,
		); err != nil {
			abortErr := e.abortTransaction(ctx, record.ID, &targets[0], targets, profile)
			if abortErr != nil {
				return result, transactionAbortFailure(record.ID, err, abortErr)
			}
			return result, err
		}
		if err := e.commitCoordinator(ctx, record.ID, &targets[0], profile); err != nil {
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
		if err := e.abortTransaction(ctx, record.ID, &targets[0], targets, profile); err != nil {
			return result, transactionAbortFailure(record.ID, nil, err)
		}
		return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, reply.Revision, result, profile)
	case distributedtxn.CoordinatorRetired:
		return result, nil
	default:
		return result, ErrTransactionConflict
	}

	responses, err := e.targetPhase(
		ctx, record.ID, targets, profile, shardservice.TransactionApplyTarget, 2,
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
	if _, err := e.targetPhase(
		ctx, record.ID, targets, profile, shardservice.TransactionReleaseTarget, 3,
	); err != nil {
		return result, &CommittedTransactionError{ID: record.ID, Cause: err}
	}
	return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
}

type manifestRecoveryArena struct {
	refs          [distributedtxn.MaxManifestPageTargets]distributedtxn.TransactionTargetRef
	identities    [distributedtxn.MaxManifestPageTargets * distributedtxn.MaxShardIdentityBytes * 2]byte
	targets       [distributedtxn.MaxManifestPageTargets]transactionTarget
	windowResults []manifestRecoveryWindowResult
}

type manifestRecoveryWindowResult struct {
	response *shardservice.ShardResponse
	err      error
	scopes   [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
}

type recoveryRouteKey struct {
	distribution string
	shard        string
}

type recoveryRouteIndex map[recoveryRouteKey]shardCall

type manifestRecoverySummary struct {
	missing      uint64
	aborted      bool
	rowsAffected int64
}

type manifestRecoveryPageFunc func(
	refs []distributedtxn.TransactionTargetRef, targets []transactionTarget,
) error

func newManifestRecoveryArena(profile Profile) *manifestRecoveryArena {
	width := max(1, profile.MaxConcurrency)
	if width > distributedtxn.MaxManifestPageTargets {
		width = distributedtxn.MaxManifestPageTargets
	}
	return &manifestRecoveryArena{windowResults: make([]manifestRecoveryWindowResult, width)}
}

func (e *Executor) recoverManifestCoordinator(
	ctx context.Context,
	snapshot *Snapshot,
	coordinator recoveryCoordinator,
	profile Profile,
) (RecoveryResult, error) {
	reply := coordinator.response.Transaction
	record, err := distributedtxn.OpenManifestCoordinator(reply.Record)
	if err != nil || record.ID != reply.ID {
		return RecoveryResult{}, errors.Join(ErrTransactionConflict, err)
	}
	result := RecoveryResult{
		ID: record.ID, State: reply.CoordinatorState,
		Targets: manifestTargetCount(record.Manifest),
	}
	if reply.CoordinatorState == distributedtxn.CoordinatorRetired {
		return result, nil
	}
	arena := newManifestRecoveryArena(profile)
	routes, err := buildRecoveryRouteIndex(snapshot, profile)
	if err != nil {
		return RecoveryResult{}, err
	}
	var coordinatorTarget transactionTarget
	// Prove the complete ordered root before contacting any participant. A
	// gateway may crash after the descriptor and page zero are durable; that
	// recoverable staging prefix must not fan out thousands of guaranteed-miss
	// participant reads or issue a write from an unsealed page stream.
	err = e.walkRecoveryManifest(
		ctx, routes, coordinator, record.ID, record.Manifest, profile, arena,
		func(_ []distributedtxn.TransactionTargetRef, targets []transactionTarget) error {
			if coordinatorTarget.call.req == nil {
				coordinatorTarget = targets[0]
			}
			return nil
		},
	)
	if err != nil {
		if errors.Is(err, errRecoveryManifestIncomplete) &&
			reply.CoordinatorState == distributedtxn.CoordinatorStaging {
			ready, pulseErr := e.advanceRecoveryPulse(
				ctx, coordinator, record.ID, reply.Revision, reply.RecoveryPulse,
				record.RecoveryDeadline, profile,
			)
			if pulseErr != nil || !ready {
				return result, errors.Join(pulseErr, ErrRecoveryNotReady)
			}
			if abortErr := e.abortIncompleteRecoveryManifest(
				ctx, coordinator, record.ID, profile,
			); abortErr != nil {
				return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: abortErr}
			}
			return e.retireRecoveredCoordinator(
				ctx, coordinator, record.ID, 2, result, profile,
			)
		}
		if errors.Is(err, errRecoveryManifestIncomplete) &&
			reply.CoordinatorState == distributedtxn.CoordinatorAborted {
			return e.retireRecoveredCoordinator(
				ctx, coordinator, record.ID, reply.Revision, result, profile,
			)
		}
		if errors.Is(err, errRecoveryManifestIncomplete) {
			return RecoveryResult{}, errors.Join(ErrTransactionConflict, err)
		}
		return RecoveryResult{}, err
	}
	if coordinatorTarget.call.req == nil {
		return RecoveryResult{}, ErrTransactionConflict
	}
	readSummary := manifestRecoverySummary{}
	err = e.walkRecoveryManifest(
		ctx, routes, coordinator, record.ID, record.Manifest, profile, arena,
		func(refs []distributedtxn.TransactionTargetRef, targets []transactionTarget) error {
			summary, phaseErr := e.readRecoveryManifestWindow(
				ctx, record.ID, reply.CoordinatorState, refs, targets,
				profile, arena.windowResults,
			)
			readSummary.missing += summary.missing
			readSummary.aborted = readSummary.aborted || summary.aborted
			return phaseErr
		},
	)
	if err != nil {
		return RecoveryResult{}, err
	}

	switch reply.CoordinatorState {
	case distributedtxn.CoordinatorStaging:
		if readSummary.missing != 0 || readSummary.aborted {
			if readSummary.missing != 0 {
				ready, pulseErr := e.advanceRecoveryPulse(
					ctx, coordinator, record.ID, reply.Revision, reply.RecoveryPulse,
					record.RecoveryDeadline, profile,
				)
				if pulseErr != nil || !ready {
					return result, errors.Join(pulseErr, ErrRecoveryNotReady)
				}
			}
			if err := e.abortRecoveryManifest(
				ctx, routes, coordinator, record, profile, arena, &coordinatorTarget,
			); err != nil {
				return result, transactionAbortFailure(record.ID, nil, err)
			}
			return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
		}
		if _, err := e.runRecoveryManifestPhase(
			ctx, routes, coordinator, record, profile, arena,
			shardservice.TransactionPrepareTarget, 1,
		); err != nil {
			abortErr := e.abortRecoveryManifest(
				ctx, routes, coordinator, record, profile, arena, &coordinatorTarget,
			)
			if abortErr != nil {
				return result, transactionAbortFailure(record.ID, err, abortErr)
			}
			return result, err
		}
		if err := e.commitCoordinator(ctx, record.ID, &coordinatorTarget, profile); err != nil {
			if errors.Is(err, ErrCommitOutcomeUnknown) {
				return result, &TransactionOutcomeUnknownError{ID: record.ID, Cause: err}
			}
			return result, err
		}
		result.State = distributedtxn.CoordinatorCommitted
	case distributedtxn.CoordinatorCommitted:
		if readSummary.missing != 0 {
			return result, ErrTransactionConflict
		}
	case distributedtxn.CoordinatorAborted:
		if err := e.abortRecoveryManifest(
			ctx, routes, coordinator, record, profile, arena, &coordinatorTarget,
		); err != nil {
			return result, transactionAbortFailure(record.ID, nil, err)
		}
		return e.retireRecoveredCoordinator(
			ctx, coordinator, record.ID, reply.Revision, result, profile,
		)
	case distributedtxn.CoordinatorRetired:
		return result, nil
	default:
		return result, ErrTransactionConflict
	}

	apply, err := e.runRecoveryManifestPhase(
		ctx, routes, coordinator, record, profile, arena,
		shardservice.TransactionApplyTarget, 2,
	)
	if err != nil {
		return result, &CommittedTransactionError{ID: record.ID, Cause: err}
	}
	result.RowsAffected = apply.rowsAffected
	if _, err := e.runRecoveryManifestPhase(
		ctx, routes, coordinator, record, profile, arena,
		shardservice.TransactionReleaseTarget, 3,
	); err != nil {
		return result, &CommittedTransactionError{ID: record.ID, Cause: err}
	}
	return e.retireRecoveredCoordinator(ctx, coordinator, record.ID, 2, result, profile)
}

func (e *Executor) advanceRecoveryPulse(
	ctx context.Context,
	coordinator recoveryCoordinator,
	id distributedtxn.ID,
	revision uint64,
	observed uint8,
	limitValue int64,
	profile Profile,
) (bool, error) {
	if limitValue <= 0 || limitValue > int64(distributedtxn.MaxRecoveryPulses) ||
		observed > uint8(limitValue) {
		return false, ErrTransactionConflict
	}
	limit := uint8(limitValue)
	if observed >= limit {
		return true, nil
	}
	next := observed + 1
	request := transactionRequest(
		coordinator.call.req, profile, shardservice.TransactionPulseCoordinator,
		id, revision, nil,
	)
	request.Transaction.RecoveryPulse = next
	response, err := e.transactionRoundTrip(ctx, coordinator.call.address, request, profile)
	if err != nil {
		return false, err
	}
	if response.Transaction.ID != id || response.Transaction.Revision != revision ||
		response.Transaction.CoordinatorState != distributedtxn.CoordinatorStaging ||
		response.Transaction.RecoveryPulse < next || response.Transaction.RecoveryPulse > limit {
		return false, ErrTransactionConflict
	}
	return response.Transaction.RecoveryPulse >= limit, nil
}

func manifestTargetCount(descriptor distributedtxn.ManifestDescriptor) int {
	if descriptor.TargetCount > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(descriptor.TargetCount)
}

func buildRecoveryRouteIndex(
	snapshot *Snapshot,
	profile Profile,
) (recoveryRouteIndex, error) {
	if snapshot == nil {
		return nil, ErrNoCatalog
	}
	count := 0
	for _, manifest := range snapshot.config.Manifests {
		count += manifest.ShardCount()
	}
	routes := make(recoveryRouteIndex, count)
	for _, manifest := range snapshot.config.Manifests {
		for index := 0; index < manifest.ShardCount(); index++ {
			shard, ok := manifest.ShardInfo(index)
			if !ok || len(shard.Leaders) == 0 {
				return nil, ErrTransactionConflict
			}
			address, err := snapshot.Address(shard.Leaders[0])
			if err != nil {
				return nil, err
			}
			key := recoveryRouteKey{
				distribution: string(manifest.Distribution()),
				shard:        string(shard.ID),
			}
			if _, duplicate := routes[key]; duplicate {
				return nil, ErrTransactionConflict
			}
			routes[key] = shardCall{
				address: address,
				req: &shardservice.ShardRequest{
					Distribution: manifest.Distribution(), Shard: shard.ID,
					AllocationGeneration: shard.AllocationGeneration,
					RoutingVersion:       manifest.Version(), OwnershipEpoch: shard.Epoch,
					ReadPolicy: profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadWrite,
					Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
					MaxResultBytes: profile.PerShardBytes,
				},
			}
		}
	}
	return routes, nil
}

func resolveRecoveryTargetsFromIndex(
	routes recoveryRouteIndex,
	refs []distributedtxn.TransactionTargetRef, targets []transactionTarget,
) error {
	if len(refs) != len(targets) {
		return ErrTransactionConflict
	}
	for index := range refs {
		ref := &refs[index]
		call, ok := routes[recoveryRouteKey{
			distribution: byteview.String(ref.Distribution),
			shard:        byteview.String(ref.Shard),
		}]
		if !ok || call.req == nil ||
			uint64(call.req.RoutingVersion) != ref.RoutingVersion ||
			uint64(call.req.AllocationGeneration) != ref.AllocationGeneration ||
			uint64(call.req.OwnershipEpoch) != ref.OwnershipEpoch {
			return ErrTransactionConflict
		}
		targets[index].call = call
	}
	return nil
}

func (e *Executor) walkRecoveryManifest(
	ctx context.Context,
	routes recoveryRouteIndex,
	coordinator recoveryCoordinator,
	id distributedtxn.ID,
	descriptor distributedtxn.ManifestDescriptor,
	profile Profile,
	arena *manifestRecoveryArena,
	visit manifestRecoveryPageFunc,
) error {
	reader, err := distributedtxn.NewManifestReader(descriptor)
	if err != nil {
		return errors.Join(ErrTransactionConflict, err)
	}
	for index := uint32(0); index < descriptor.SegmentCount; index++ {
		request := transactionRequest(
			coordinator.call.req, profile,
			shardservice.TransactionReadManifestSegment, id, 0, nil,
		)
		request.Transaction.SegmentIndex = index
		response, err := e.transactionRoundTrip(ctx, coordinator.call.address, request, profile)
		if err != nil {
			if errors.Is(err, ErrTransactionNotFound) {
				return errors.Join(errRecoveryManifestIncomplete, err)
			}
			return err
		}
		reply := response.Transaction
		if reply.Role != shardservice.TransactionRoleCoordinator || reply.ID != id ||
			reply.RecordKind != shardservice.TransactionRecordManifestSegment ||
			reply.SegmentIndex != index || len(reply.Record) == 0 {
			return ErrTransactionConflict
		}
		page, err := reader.OpenNext(reply.Record, arena.refs[:], arena.identities[:])
		if err != nil || page.Segment.Index != index {
			return errors.Join(ErrTransactionConflict, err)
		}
		targets := arena.targets[:len(page.Targets)]
		clear(targets)
		if err := resolveRecoveryTargetsFromIndex(
			routes, page.Targets, targets,
		); err != nil {
			return err
		}
		if visit != nil {
			if err := visit(page.Targets, targets); err != nil {
				return err
			}
		}
	}
	if err := reader.Seal(); err != nil {
		return errors.Join(ErrTransactionConflict, err)
	}
	return nil
}

func (e *Executor) readRecoveryManifestWindow(
	ctx context.Context,
	id distributedtxn.ID,
	coordinatorState distributedtxn.CoordinatorState,
	refs []distributedtxn.TransactionTargetRef, targets []transactionTarget,
	profile Profile,
	window []manifestRecoveryWindowResult,
) (manifestRecoverySummary, error) {
	var summary manifestRecoverySummary
	err := e.forEachRecoveryManifestWindow(
		ctx, targets, window,
		func(target *transactionTarget) (*shardservice.ShardResponse, error) {
			request := transactionRequest(
				target.call.req, profile, shardservice.TransactionReadTarget,
				id, 0, nil,
			)
			return e.transactionRoundTrip(ctx, target.call.address, request, profile)
		},
		func(index int, response *shardservice.ShardResponse, err error) error {
			if errors.Is(err, ErrTransactionNotFound) {
				summary.missing++
				return nil
			}
			if err != nil {
				return err
			}
			if response == nil || response.Transaction.Role != shardservice.TransactionRoleTarget ||
				response.Transaction.ID != id {
				return ErrTransactionConflict
			}
			if response.Transaction.TargetState == distributedtxn.TargetAborted &&
				len(response.Transaction.Record) == 0 {
				summary.aborted = true
				return nil
			}
			if response.Transaction.RecordKind != shardservice.TransactionRecordTarget {
				return ErrTransactionConflict
			}
			slot := index % len(window)
			record, openErr := distributedtxn.OpenTargetInto(
				response.Transaction.Record, window[slot].scopes[:],
			)
			ref := &refs[index]
			if openErr != nil || record.ID != id ||
				record.MutationDigest != ref.MutationDigest ||
				record.RoutingVersion != ref.RoutingVersion ||
				record.AllocationGeneration != ref.AllocationGeneration ||
				record.OwnershipEpoch != ref.OwnershipEpoch {
				return errors.Join(ErrTransactionConflict, openErr)
			}
			if response.Transaction.TargetState == distributedtxn.TargetAborted {
				summary.aborted = true
				return nil
			}
			if coordinatorState == distributedtxn.CoordinatorStaging &&
				response.Transaction.TargetState != distributedtxn.TargetStaged &&
				response.Transaction.TargetState != distributedtxn.TargetPrepared {
				return ErrTransactionConflict
			}
			return nil
		},
	)
	return summary, err
}

func (e *Executor) runRecoveryManifestPhase(
	ctx context.Context,
	routes recoveryRouteIndex,
	coordinator recoveryCoordinator,
	record distributedtxn.ManifestCoordinatorRecord,
	profile Profile,
	arena *manifestRecoveryArena,
	operation shardservice.TransactionOperation,
	revision uint64,
) (manifestRecoverySummary, error) {
	var summary manifestRecoverySummary
	err := e.walkRecoveryManifest(
		ctx, routes, coordinator, record.ID, record.Manifest, profile, arena,
		func(_ []distributedtxn.TransactionTargetRef, targets []transactionTarget) error {
			page, err := e.runRecoveryManifestWindow(
				ctx, record.ID, targets, profile, arena.windowResults, operation, revision,
			)
			if page.rowsAffected < 0 || page.rowsAffected > math.MaxInt64-summary.rowsAffected {
				return ErrTransactionConflict
			}
			summary.rowsAffected += page.rowsAffected
			return err
		},
	)
	return summary, err
}

func (e *Executor) runRecoveryManifestWindow(
	ctx context.Context,
	id distributedtxn.ID,
	targets []transactionTarget,
	profile Profile,
	window []manifestRecoveryWindowResult,
	operation shardservice.TransactionOperation,
	revision uint64,
) (manifestRecoverySummary, error) {
	var summary manifestRecoverySummary
	err := e.forEachRecoveryManifestWindow(
		ctx, targets, window,
		func(target *transactionTarget) (*shardservice.ShardResponse, error) {
			request := transactionRequest(
				target.call.req, profile, operation, id, revision, nil,
			)
			return e.transactionRoundTrip(ctx, target.call.address, request, profile)
		},
		func(_ int, response *shardservice.ShardResponse, err error) error {
			if err != nil {
				return err
			}
			if response == nil {
				return ErrTransactionConflict
			}
			if operation == shardservice.TransactionApplyTarget {
				if response.RowsAffected < 0 ||
					response.RowsAffected > math.MaxInt64-summary.rowsAffected {
					return ErrTransactionConflict
				}
				summary.rowsAffected += response.RowsAffected
			}
			return nil
		},
	)
	return summary, err
}

func (e *Executor) forEachRecoveryManifestWindow(
	ctx context.Context,
	targets []transactionTarget,
	window []manifestRecoveryWindowResult,
	dispatch func(*transactionTarget) (*shardservice.ShardResponse, error),
	consume func(int, *shardservice.ShardResponse, error) error,
) error {
	if len(window) == 0 {
		return ErrTransactionConflict
	}
	for base := 0; base < len(targets); base += len(window) {
		count := min(len(window), len(targets)-base)
		results := window[:count]
		clear(results)
		var wait sync.WaitGroup
		for slot := range results {
			wait.Go(func() {
				results[slot].response, results[slot].err = dispatch(&targets[base+slot])
			})
		}
		wait.Wait()
		for slot := range results {
			if err := consume(base+slot, results[slot].response, results[slot].err); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) abortRecoveryManifest(
	ctx context.Context,
	routes recoveryRouteIndex,
	coordinator recoveryCoordinator,
	record distributedtxn.ManifestCoordinatorRecord,
	profile Profile,
	arena *manifestRecoveryArena,
	coordinatorTarget *transactionTarget,
) error {
	// The coordinator decision is the sole commit/abort authority. Never mutate
	// a participant until abort has durably won or an idempotent lookup proves it
	// already won; a concurrent committed winner must retain all participant
	// state for committed recovery.
	if err := e.abortCoordinator(ctx, record.ID, coordinatorTarget, profile); err != nil {
		return err
	}
	var firstErr error
	walkErr := e.walkRecoveryManifest(
		ctx, routes, coordinator, record.ID, record.Manifest, profile, arena,
		func(_ []distributedtxn.TransactionTargetRef, targets []transactionTarget) error {
			err := e.forEachRecoveryManifestWindow(
				ctx, targets, arena.windowResults,
				func(target *transactionTarget) (*shardservice.ShardResponse, error) {
					return nil, e.abortRecoveryManifestTarget(ctx, record.ID, target, profile)
				},
				func(_ int, _ *shardservice.ShardResponse, err error) error {
					if err != nil && firstErr == nil {
						firstErr = err
					}
					return nil
				},
			)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			return nil
		},
	)
	if walkErr != nil && firstErr == nil {
		firstErr = walkErr
	}
	return firstErr
}

func (e *Executor) abortIncompleteRecoveryManifest(
	ctx context.Context,
	coordinator recoveryCoordinator,
	id distributedtxn.ID,
	profile Profile,
) error {
	target := transactionTarget{call: coordinator.call}
	return e.abortCoordinator(ctx, id, &target, profile)
}

func (e *Executor) abortRecoveryManifestTarget(
	ctx context.Context,
	id distributedtxn.ID,
	target *transactionTarget,
	profile Profile,
) error {
	lookup := transactionRequest(
		target.call.req, profile,
		shardservice.TransactionLookupTarget, id, 0, nil,
	)
	response, err := e.transactionRoundTrip(ctx, target.call.address, lookup, profile)
	if errors.Is(err, ErrTransactionNotFound) {
		abort := transactionRequest(
			target.call.req, profile,
			shardservice.TransactionAbortTarget, id, 1, nil,
		)
		_, err = e.transactionRoundTrip(ctx, target.call.address, abort, profile)
		return err
	}
	if err != nil {
		return err
	}
	state := response.Transaction.TargetState
	if state == distributedtxn.TargetAborted || state == distributedtxn.TargetReleased {
		return nil
	}
	if state != distributedtxn.TargetStaged && state != distributedtxn.TargetPrepared {
		return ErrTransactionConflict
	}
	abort := transactionRequest(
		target.call.req, profile,
		shardservice.TransactionAbortTarget, id, response.Transaction.Revision, nil,
	)
	_, err = e.transactionRoundTrip(ctx, target.call.address, abort, profile)
	return err
}

func resolveRecoveryTargets(
	snapshot *Snapshot,
	refs []distributedtxn.TransactionTargetRef, profile Profile,
) ([]transactionTarget, error) {
	targets := make([]transactionTarget, len(refs))
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
			targets[i].call = shardCall{address: address, req: request}
			found = true
			break
		}
		if !found {
			return nil, ErrTransactionConflict
		}
	}
	return targets, nil
}

func (e *Executor) readRecoveryTargets(
	ctx context.Context,
	coordinator distributedtxn.CoordinatorRecord,
	targets []transactionTarget,
	profile Profile,
) ([]*shardservice.ShardResponse, int, error) {
	responses := make([]*shardservice.ShardResponse, len(targets))
	missing := 0
	for i := range targets {
		request := transactionRequest(
			targets[i].call.req, profile, shardservice.TransactionReadTarget,
			coordinator.ID, 0, nil,
		)
		response, err := e.transactionRoundTrip(ctx, targets[i].call.address, request, profile)
		if errors.Is(err, ErrTransactionNotFound) {
			missing++
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		if response.Transaction.TargetState == distributedtxn.TargetAborted &&
			len(response.Transaction.Record) == 0 {
			responses[i] = response
			continue
		}
		record, err := distributedtxn.OpenTarget(response.Transaction.Record)
		if err != nil || record.ID != coordinator.ID ||
			record.MutationDigest != coordinator.Targets[i].MutationDigest ||
			record.RoutingVersion != coordinator.Targets[i].RoutingVersion ||
			record.AllocationGeneration != coordinator.Targets[i].AllocationGeneration ||
			record.OwnershipEpoch != coordinator.Targets[i].OwnershipEpoch {
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

// RecoverAll scans each legacy shard's bounded coordinator index and redrives
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
