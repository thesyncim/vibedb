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
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

const transactionInitialRevision = 1

var (
	// ErrBatchEmpty reports an empty atomic write batch.
	ErrBatchEmpty = errors.New("gateway: atomic write batch is empty")
	// ErrBatchClassMismatch reports a batch whose statements request different
	// operational deadline/concurrency profiles.
	ErrBatchClassMismatch = errors.New("gateway: atomic write batch mixes operation classes")
	// ErrTransactionMutationLimit reports an atomic write whose exact mutation
	// count exceeds its operational profile before any target is staged.
	ErrTransactionMutationLimit = errors.New("gateway: distributed transaction exceeds the mutation bound")
	// ErrTransactionByteLimit reports an atomic write whose canonical mutation
	// bytes exceed its operational profile before any target is staged.
	ErrTransactionByteLimit = errors.New("gateway: distributed transaction exceeds the byte bound")
	// ErrBatchRequestIdentityUnsupported prevents a caller request ID from being
	// silently ignored by the non-replicated transaction path.
	ErrBatchRequestIdentityUnsupported = errors.New(
		"gateway: request identity requires a replicated transaction batch",
	)
	// ErrTransactionCommitted reports a durably committed decision whose apply
	// or release cleanup did not finish before this request returned.
	ErrTransactionCommitted = errors.New("gateway: distributed transaction is committed and requires recovery")
)

// CommittedTransactionError preserves the raw transaction identity when the
// commit point is durable but synchronous publication or cleanup is incomplete.
// Retrying the original SQL as a new transaction is unsafe; recovery must use
// ID and the retained target records.
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

type transactionTarget struct {
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
	if len(queries) == 1 {
		if ctx == nil {
			ctx = context.Background()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateQueryBatchAdmission(
			queries, e.profileFor(queries[0].Class),
		); err != nil {
			return nil, err
		}
		return e.Exec(ctx, queries[0])
	}
	return e.execBatch(ctx, queries)
}

func (e *Executor) execBatch(
	ctx context.Context,
	queries []Query,
) (*Result, error) {
	if len(queries) == 0 {
		return nil, ErrBatchEmpty
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
	if err := validateQueryBatchAdmission(queries, profile); err != nil {
		return nil, err
	}
	if err := validateTypedQueries(ctx, queries); err != nil {
		return nil, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	snapshot, lease, err := e.pin(opctx, 0, 0)
	if err != nil {
		return nil, err
	}
	if len(queries) == 1 {
		lease.release()
		return e.Exec(opctx, queries[0])
	}
	defer lease.release()
	targets, err := e.planTransaction(opctx, snapshot, queries, profile)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return &Result{
			Kind: shardservice.ResponseCompletion, RouteKind: distribution.RouteEmpty,
			Generation: snapshot.Generation(),
		}, nil
	}
	return e.executeTransaction(opctx, snapshot, targets, profile)
}

func (e *Executor) planTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
) ([]transactionTarget, error) {
	capacity := transactionTargetCapacity(snapshot, len(queries), profile)
	targets := make([]transactionTarget, 0, capacity)
	// Preserve the allocation-minimal inline lane: at most 64 statements use
	// the original tiny linear grouping. Potentially wide plans promote lazily
	// only after they prove they contain enough distinct targets.
	var targetIndex map[transactionTargetKey]int
	budget := transactionPlanBudget{profile: profile}
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
		if err := rejectReplicatedGlobalIndexSQLTargets(snapshot, bound); err != nil {
			return nil, err
		}
		if len(prepared.writeGlobalIndexes) != 0 &&
			(bound.kind == sqlast.KindUpdate || bound.kind == sqlast.KindDelete) {
			if err := e.captureIndexedMutation(ctx, prepared, bound, *call, profile); err != nil {
				return nil, err
			}
			if err := rejectReplicatedGlobalIndexSQLTargets(snapshot, bound); err != nil {
				return nil, err
			}
		}
		targets, err = appendBoundWriteTargetsBudgeted(
			targets, targetIndex, *call, query, bound, profile, &budget,
		)
		if err != nil {
			return nil, err
		}
		if targetIndex == nil && len(queries) > distributedtxn.MaxInlineTargets &&
			len(targets) >= 16 {
			targetIndex = make(map[transactionTargetKey]int, max(capacity, len(targets)))
			for target := range targets {
				targetIndex[transactionKey(targets[target].call.req)] = target
			}
		}
	}
	sortTransactionTargets(targets)
	return targets, nil
}

// transactionTargetCapacity keeps the common inline lane compact even
// when a caller supplies a huge same-shard batch. It is only an allocation
// hint: the target slice and exact-key index still grow for wide catalogs.
func transactionTargetCapacity(snapshot *Snapshot, queries int, profile Profile) int {
	if queries <= 0 {
		return 0
	}
	capacity := min(queries, 8)
	if profile.MaxTransactionMutations < uint64(capacity) {
		capacity = int(profile.MaxTransactionMutations)
	}
	if snapshot == nil {
		return max(1, capacity)
	}
	targets := 0
	for _, manifest := range snapshot.config.Manifests {
		if manifest != nil {
			targets += manifest.ShardCount()
		}
	}
	if targets != 0 {
		capacity = min(capacity, targets)
	}
	return max(1, capacity)
}

type transactionPlanBudget struct {
	profile   Profile
	mutations uint64
	bytes     uint64
}

func (b *transactionPlanBudget) admit(statement *shardservice.MutationStatement, newTarget bool) error {
	if b == nil {
		return nil
	}
	if b.mutations >= b.profile.MaxTransactionMutations {
		return ErrTransactionMutationLimit
	}
	encoded, ok := transactionStatementBytes(statement)
	if !ok {
		// The canonical encoder remains the semantic validator. Overflow is
		// necessarily beyond every supported transaction byte profile.
		return ErrTransactionByteLimit
	}
	if newTarget {
		encoded += 8 // canonical VMB1 header, once per participant
	}
	if b.bytes > b.profile.MaxTransactionBytes ||
		encoded > b.profile.MaxTransactionBytes-b.bytes {
		return ErrTransactionByteLimit
	}
	b.mutations++
	b.bytes += encoded
	return nil
}

// transactionStatementBytes returns the exact number of bytes contributed by
// one statement to AppendMutationBatch, excluding its one-per-target
// header. It performs checked arithmetic only; the encoder still validates the
// statement fields once after planning.
func transactionStatementBytes(statement *shardservice.MutationStatement) (uint64, bool) {
	add := func(total *uint64, value int) bool {
		if value < 0 || uint64(value) > math.MaxUint64-*total {
			return false
		}
		*total += uint64(value)
		return true
	}
	var total uint64
	switch statement.Kind {
	case shardservice.MutationSQL:
		if !add(&total, 8+len(statement.SQL)) {
			return 0, false
		}
		if !add(&total, len(statement.ParamTypes)) {
			return 0, false
		}
		for i := range statement.Params {
			param := &statement.Params[i]
			if !add(&total, 1) {
				return 0, false
			}
			switch param.Kind {
			case shardservice.ParamBool:
				if !add(&total, 1) {
					return 0, false
				}
			case shardservice.ParamNumber, shardservice.ParamString, shardservice.ParamDocument:
				if !add(&total, 4+len(param.Bytes)) {
					return 0, false
				}
			}
		}
	case shardservice.MutationPrimaryPrecondition, shardservice.MutationPrimaryCheck:
		if !add(&total, 20+len(statement.Relation)+len(statement.PrimaryPath)) {
			return 0, false
		}
		for i := range statement.ExpectedKeys {
			if !add(&total, 4+len(statement.ExpectedKeys[i])+32) {
				return 0, false
			}
		}
	default:
		if !add(&total, 36+len(statement.Relation)+len(statement.EntryKey)+len(statement.Value)) {
			return 0, false
		}
	}
	return total, true
}

func rejectReplicatedGlobalIndexSQLTargets(
	snapshot *Snapshot,
	bound *BoundWritePlan,
) error {
	if snapshot == nil || bound == nil {
		return ErrReplicatedSQLWriteUnavailable
	}
	for index := range bound.globalIndexes {
		mutation := &bound.globalIndexes[index]
		if _, replicated := snapshot.replicatedShardAt(
			mutation.distribution, mutation.target.Shard,
		); replicated {
			return ErrReplicatedSQLWriteUnavailable
		}
	}
	return nil
}

func appendBoundWriteTargets(
	targets []transactionTarget,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
) ([]transactionTarget, error) {
	return appendBoundWriteTargetsIndexed(targets, nil, baseCall, query, bound, profile)
}

func appendBoundWriteTargetsIndexed(
	targets []transactionTarget,
	targetIndex map[transactionTargetKey]int,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
) ([]transactionTarget, error) {
	return appendBoundWriteTargetsBudgeted(
		targets, targetIndex, baseCall, query, bound, profile, nil,
	)
}

func appendBoundWriteTargetsBudgeted(
	targets []transactionTarget,
	targetIndex map[transactionTargetKey]int,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
	budget *transactionPlanBudget,
) ([]transactionTarget, error) {
	var err error
	if len(bound.primaryPath) != 0 {
		targets, err = appendTransactionStatementBudgeted(
			targets, targetIndex, baseCall,
			shardservice.MutationStatement{
				Kind:     shardservice.MutationPrimaryPrecondition,
				Relation: bound.table, PrimaryPath: bound.primaryPath,
				ExpectedKeys: bound.expectedKeys, ExpectedDigests: bound.expectedDigests,
			}, budget,
		)
		if err != nil {
			return nil, err
		}
	}
	targets, err = appendTransactionStatementBudgeted(
		targets, targetIndex, baseCall,
		shardservice.MutationStatement{
			SQL: query.SQL, Params: query.Params, ParamTypes: query.ParamTypes,
		}, budget,
	)
	if err != nil {
		return nil, err
	}
	if len(bound.postimageKeys) != 0 {
		targets, err = appendTransactionStatementBudgeted(
			targets, targetIndex, baseCall,
			shardservice.MutationStatement{
				Kind:     shardservice.MutationPrimaryCheck,
				Relation: bound.table, PrimaryPath: bound.primaryPath,
				ExpectedKeys: bound.postimageKeys, ExpectedDigests: bound.postimageDigests,
			}, budget,
		)
		if err != nil {
			return nil, err
		}
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
		targets, err = appendTransactionStatementBudgeted(
			targets, targetIndex, call, shardservice.MutationStatement{
				Kind:     index.kind,
				Relation: index.metadata.Relation,
				IndexID:  index.metadata.IndexID, Incarnation: index.metadata.Incarnation,
				EntryKey: entryKey, Value: value,
				LocatorCount: index.metadata.LocatorCount,
				Unique: index.kind == shardservice.MutationGlobalIndexPut &&
					index.metadata.Flags&IndexUnique != 0,
			}, budget,
		)
		if err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func appendTransactionStatement(
	targets []transactionTarget,
	call shardCall,
	statement shardservice.MutationStatement,
) ([]transactionTarget, error) {
	return appendTransactionStatementIndexed(targets, nil, call, statement)
}

type transactionTargetKey struct {
	distribution         distribution.DistributionName
	shard                distribution.ShardID
	routingVersion       distribution.RoutingVersion
	allocationGeneration distribution.ShardAllocationGeneration
	ownershipEpoch       distribution.OwnershipEpoch
}

func transactionKey(request *shardservice.ShardRequest) transactionTargetKey {
	return transactionTargetKey{
		distribution: request.Distribution, shard: request.Shard,
		routingVersion:       request.RoutingVersion,
		allocationGeneration: request.AllocationGeneration,
		ownershipEpoch:       request.OwnershipEpoch,
	}
}

func appendTransactionStatementIndexed(
	targets []transactionTarget,
	byTarget map[transactionTargetKey]int,
	call shardCall,
	statement shardservice.MutationStatement,
) ([]transactionTarget, error) {
	return appendTransactionStatementBudgeted(targets, byTarget, call, statement, nil)
}

func appendTransactionStatementBudgeted(
	targets []transactionTarget,
	byTarget map[transactionTargetKey]int,
	call shardCall,
	statement shardservice.MutationStatement,
	budget *transactionPlanBudget,
) ([]transactionTarget, error) {
	targetIndex := -1
	key := transactionKey(call.req)
	if byTarget != nil {
		if found, ok := byTarget[key]; ok {
			targetIndex = found
		}
	} else {
		for i := range targets {
			if sameTransactionTarget(targets[i].call.req, call.req) {
				targetIndex = i
				break
			}
		}
	}
	newTarget := targetIndex < 0
	if !newTarget && len(targets[targetIndex].statements) >= shardservice.MaxMutationStatements {
		return targets, ErrTransactionMutationLimit
	}
	if err := budget.admit(&statement, newTarget); err != nil {
		return targets, err
	}
	if targetIndex < 0 {
		targets = append(targets, transactionTarget{
			call: call, bucketBits: call.req.BucketBits,
			scopes: call.req.AccessScopes,
		})
		targetIndex = len(targets) - 1
		if byTarget != nil {
			byTarget[key] = targetIndex
		}
	}
	target := &targets[targetIndex]
	if len(target.statements) != 0 {
		mergeTargetScopes(target, call.req.BucketBits, call.req.AccessScopes)
	}
	target.statements = append(target.statements, statement)
	return targets, nil
}

func sortTransactionTargets(targets []transactionTarget) {
	// Binding preserves per-index delete/put adjacency because the RF3 lowerer
	// recognizes same-key replacements as adjacent pairs. The static transaction
	// lane has a different requirement: after every statement in an atomic batch
	// has joined its final participant, release all old index claims before any
	// replacement put. Stable ranking also keeps base preconditions, SQL, and
	// postimage checks in authored order ahead of index maintenance when a catalog
	// maps both relations onto the same physical transaction target.
	for i := range targets {
		slices.SortStableFunc(
			targets[i].statements,
			func(a, b shardservice.MutationStatement) int {
				return transactionStatementRank(a.Kind) - transactionStatementRank(b.Kind)
			},
		)
	}
	slices.SortFunc(targets, func(a, b transactionTarget) int {
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

func transactionStatementRank(kind shardservice.MutationKind) int {
	switch kind {
	case shardservice.MutationGlobalIndexDelete:
		return 1
	case shardservice.MutationGlobalIndexPut:
		return 2
	default:
		return 0
	}
}

func mergeTargetScopes(
	target *transactionTarget,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) {
	if target.bucketBits == 0 || bucketBits == 0 || target.bucketBits != bucketBits {
		target.bucketBits = 0
		target.scopes = nil
		return
	}
	target.scopes = append(target.scopes, scopes...)
	target.scopes = coalesceIntentScopes(target.scopes)
	if len(target.scopes) > distributedtxn.MaxIntentScopes {
		target.bucketBits = 0
		target.scopes = nil
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
	targets []transactionTarget,
	profile Profile,
) (*Result, error) {
	id, err := newTransactionID(cryptorand.Reader)
	if err != nil {
		return nil, err
	}
	coordinator := &targets[0]
	refs := make([]distributedtxn.TransactionTargetRef, len(targets))
	var mutationCount, mutationBytes uint64
	for i := range targets {
		target := &targets[i]
		statements := uint64(len(target.statements))
		if profile.MaxTransactionMutations == 0 ||
			mutationCount > profile.MaxTransactionMutations ||
			statements > profile.MaxTransactionMutations-mutationCount {
			return nil, ErrTransactionMutationLimit
		}
		mutationCount += statements
		target.mutation, err = shardservice.AppendMutationBatch(nil, target.statements)
		if err != nil {
			return nil, err
		}
		encodedBytes := uint64(len(target.mutation))
		if profile.MaxTransactionBytes == 0 || mutationBytes > profile.MaxTransactionBytes ||
			encodedBytes > profile.MaxTransactionBytes-mutationBytes {
			return nil, ErrTransactionByteLimit
		}
		mutationBytes += encodedBytes
		target.digest = distributedtxn.TargetDigest(
			target.bucketBits, target.scopes, target.mutation,
		)
		request := target.call.req
		refs[i] = distributedtxn.TransactionTargetRef{
			// The route strings are retained by participants through both immediate
			// coordinator passes; byteview avoids two identity allocations per shard.
			Distribution:         byteview.Bytes(string(request.Distribution)),
			Shard:                byteview.Bytes(string(request.Shard)),
			RoutingVersion:       uint64(request.RoutingVersion),
			AllocationGeneration: uint64(request.AllocationGeneration),
			OwnershipEpoch:       uint64(request.OwnershipEpoch), MutationDigest: target.digest,
			State: distributedtxn.TargetStaged,
		}
	}
	coordinatorRecord := distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: transactionInitialRevision,
		CatalogGeneration: snapshot.Generation(),
		RecoveryDeadline:  int64(distributedtxn.MaxRecoveryPulses),
		Targets:           refs,
	}
	stager := gatewayCoordinatorStager{
		executor: e, ctx: ctx, coordinator: coordinator, profile: profile,
	}
	if _, err := stageTransactionCoordinator(coordinatorRecord, &stager); err != nil {
		// A lost begin response is outcome-unknown until lookup proves whether
		// the coordinator exists. Do not introduce a new abort write merely to
		// discover whether the begin itself crossed durability.
		lookup := transactionRequest(
			coordinator.call.req, profile, shardservice.TransactionLookupCoordinator,
			id, 0, nil,
		)
		response, lookupErr := e.transactionRoundTrip(ctx, coordinator.call.address, lookup, profile)
		if errors.Is(lookupErr, ErrTransactionNotFound) {
			return nil, err
		}
		if lookupErr != nil {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, lookupErr)}
		}
		state, stateErr := coordinatorReplyState(response, id)
		if stateErr != nil {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, stateErr)}
		}
		if state == distributedtxn.CoordinatorCommitted {
			return nil, &CommittedTransactionError{ID: id, Cause: err}
		}
		if state != distributedtxn.CoordinatorStaging && state != distributedtxn.CoordinatorAborted {
			return nil, &TransactionOutcomeUnknownError{ID: id, Cause: errors.Join(err, ErrTransactionConflict)}
		}
		abortErr := e.abortCoordinator(ctx, id, coordinator, profile)
		if abortErr != nil {
			return nil, transactionAbortFailure(id, err, abortErr)
		}
		return nil, err
	}
	for i := range targets {
		target := &targets[i]
		target.record, err = distributedtxn.AppendTarget(nil, distributedtxn.TargetRecord{
			ID: id, State: distributedtxn.TargetStaged, Revision: transactionInitialRevision,
			RoutingVersion:            uint64(target.call.req.RoutingVersion),
			AllocationGeneration:      uint64(target.call.req.AllocationGeneration),
			OwnershipEpoch:            uint64(target.call.req.OwnershipEpoch),
			CoordinatorDistribution:   byteview.Bytes(string(coordinator.call.req.Distribution)),
			CoordinatorShard:          byteview.Bytes(string(coordinator.call.req.Shard)),
			CoordinatorAllocation:     uint64(coordinator.call.req.AllocationGeneration),
			CoordinatorRoutingVersion: uint64(coordinator.call.req.RoutingVersion),
			CoordinatorOwnershipEpoch: uint64(coordinator.call.req.OwnershipEpoch),
			BucketBits:                target.bucketBits, IntentScopes: target.scopes,
			MutationDigest: target.digest, Mutation: target.mutation,
		})
		if err != nil {
			abortErr := e.abortTransaction(ctx, id, coordinator, targets, profile)
			if abortErr != nil {
				return nil, transactionAbortFailure(id, err, abortErr)
			}
			return nil, err
		}
	}
	if err := e.targetCompletionPhase(ctx, id, targets, profile, shardservice.TransactionStageTarget, 0); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, targets, profile)
		if abortErr != nil {
			return nil, transactionAbortFailure(id, err, abortErr)
		}
		return nil, err
	}
	if err := e.targetCompletionPhase(
		ctx, id, targets, profile, shardservice.TransactionPrepareTarget,
		transactionInitialRevision,
	); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, targets, profile)
		if abortErr != nil {
			return nil, transactionAbortFailure(id, err, abortErr)
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
	affected, applyErr := e.targetApplyPhase(
		ctx, id, targets, profile, shardservice.TransactionApplyTarget,
		transactionInitialRevision+1,
	)
	if applyErr != nil {
		return nil, &CommittedTransactionError{ID: id, Cause: applyErr}
	}
	if releaseErr := e.targetCompletionPhase(
		ctx, id, targets, profile, shardservice.TransactionReleaseTarget,
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
	if len(targets) > 1 {
		kind = distribution.RouteTargeted
	}
	e.metrics.observeRoute(kind, len(targets), ScatterNone)
	return &Result{
		Kind: shardservice.ResponseCompletion, RowsAffected: affected,
		RouteKind: kind, Generation: snapshot.Generation(), ShardsFanned: len(targets),
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
	switch operation {
	case shardservice.TransactionStageCoordinator,
		shardservice.TransactionStageTarget,
		shardservice.TransactionStageManifestCoordinator:
		transaction.Record = record
	case shardservice.TransactionStageManifestSegment:
		transaction.ID = id
		transaction.ManifestSegment = record
	default:
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

func (e *Executor) targetPhase(
	ctx context.Context,
	id distributedtxn.ID,
	targets []transactionTarget,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) ([]*shardservice.ShardResponse, error) {
	if operation != shardservice.TransactionApplyTarget {
		return nil, e.targetCompletionPhase(
			ctx, id, targets, profile, operation, revision,
		)
	}
	responses := make([]*shardservice.ShardResponse, len(targets))
	errs := make([]error, len(targets))
	jobs := make(chan int)
	workers := min(max(1, profile.MaxConcurrency), len(targets))
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for i := range jobs {
				target := &targets[i]
				var record []byte
				if operation == shardservice.TransactionStageTarget {
					record = target.record
				}
				request := transactionRequest(
					target.call.req, profile, operation, id, revision, record,
				)
				responses[i], errs[i] = e.transactionRoundTrip(
					ctx, target.call.address, request, profile,
				)
			}
		})
	}
	for i := range targets {
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

type transactionPhaseResult struct {
	response *shardservice.ShardResponse
	err      error
}

// visitTargetPhase retains only a worker-sized result window. Wide
// transactions therefore do not allocate response and error slices merely to
// learn that a completion phase succeeded or to sum applied rows.
func (e *Executor) visitTargetPhase(
	ctx context.Context,
	id distributedtxn.ID,
	targets []transactionTarget,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
	visit func(*shardservice.ShardResponse) error,
) error {
	if len(targets) == 0 {
		return nil
	}
	workers := min(max(1, profile.MaxConcurrency), len(targets))
	results := make(chan transactionPhaseResult, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(targets) {
					return
				}
				target := &targets[i]
				var record []byte
				if operation == shardservice.TransactionStageTarget {
					record = target.record
				}
				request := transactionRequest(
					target.call.req, profile, operation, id, revision, record,
				)
				response, err := e.transactionRoundTrip(
					ctx, target.call.address, request, profile,
				)
				results <- transactionPhaseResult{response: response, err: err}
			}
		})
	}
	var firstErr error
	for range targets {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		if visit != nil {
			if err := visit(result.response); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	wait.Wait()
	return firstErr
}

func (e *Executor) targetCompletionPhase(
	ctx context.Context,
	id distributedtxn.ID,
	targets []transactionTarget,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) error {
	return e.visitTargetPhase(ctx, id, targets, profile, operation, revision, nil)
}

func (e *Executor) targetApplyPhase(
	ctx context.Context,
	id distributedtxn.ID,
	targets []transactionTarget,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) (int64, error) {
	var affected int64
	err := e.visitTargetPhase(ctx, id, targets, profile, operation, revision,
		func(response *shardservice.ShardResponse) error {
			if response == nil || response.RowsAffected < 0 ||
				response.RowsAffected > math.MaxInt64-affected {
				return ErrTransactionConflict
			}
			affected += response.RowsAffected
			return nil
		})
	return affected, err
}

func (e *Executor) commitCoordinator(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionTarget,
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

func transactionAbortFailure(id distributedtxn.ID, cause, abortErr error) error {
	joined := errors.Join(cause, abortErr)
	if errors.Is(abortErr, ErrTransactionCommitted) {
		return &CommittedTransactionError{ID: id, Cause: joined}
	}
	if errors.Is(abortErr, ErrCommitOutcomeUnknown) ||
		errors.Is(abortErr, ErrTransactionNotFound) {
		return &TransactionOutcomeUnknownError{ID: id, Cause: joined}
	}
	// The abort decision is already durable. Participant tombstone cleanup may
	// still need recovery, but the transaction outcome itself is not unknown.
	return joined
}

// abortCoordinator first wins or resolves the only durable decision. Callers
// must not mutate target state until this returns nil. A lost race to
// commit is reported as a known committed outcome; an unresolvable response is
// outcome-unknown and leaves every target untouched.
func (e *Executor) abortCoordinator(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionTarget,
	profile Profile,
) error {
	expected := uint64(transactionInitialRevision)
	var priorErr error
	for range 2 {
		abort := transactionRequest(
			coordinator.call.req, profile, shardservice.TransactionAbortCoordinator,
			id, expected, nil,
		)
		response, err := e.transactionRoundTrip(ctx, coordinator.call.address, abort, profile)
		if err == nil {
			state, stateErr := coordinatorReplyState(response, id)
			switch state {
			case distributedtxn.CoordinatorAborted:
				return nil
			case distributedtxn.CoordinatorCommitted:
				return ErrTransactionCommitted
			}
			priorErr = errors.Join(priorErr, stateErr, ErrTransactionConflict)
		} else {
			priorErr = errors.Join(priorErr, err)
		}

		lookup := transactionRequest(
			coordinator.call.req, profile, shardservice.TransactionLookupCoordinator,
			id, 0, nil,
		)
		response, lookupErr := e.transactionRoundTrip(ctx, coordinator.call.address, lookup, profile)
		if lookupErr != nil {
			if errors.Is(lookupErr, ErrTransactionNotFound) {
				return ErrTransactionNotFound
			}
			return errors.Join(ErrCommitOutcomeUnknown, priorErr, lookupErr)
		}
		state, stateErr := coordinatorReplyState(response, id)
		if stateErr != nil {
			return errors.Join(ErrCommitOutcomeUnknown, priorErr, stateErr)
		}
		switch state {
		case distributedtxn.CoordinatorAborted:
			return nil
		case distributedtxn.CoordinatorCommitted:
			return ErrTransactionCommitted
		case distributedtxn.CoordinatorStaging:
			expected = response.Transaction.Revision
			continue
		default:
			// Retired does not retain whether commit or abort won, so it is a
			// resolved terminal state but not a safe license to mutate participants.
			return errors.Join(ErrCommitOutcomeUnknown, priorErr, ErrTransactionConflict)
		}
	}
	return errors.Join(ErrCommitOutcomeUnknown, priorErr)
}

func coordinatorReplyState(
	response *shardservice.ShardResponse,
	id distributedtxn.ID,
) (distributedtxn.CoordinatorState, error) {
	if response == nil || response.Transaction.Role != shardservice.TransactionRoleCoordinator ||
		response.Transaction.ID != id {
		return 0, ErrTransactionConflict
	}
	return response.Transaction.CoordinatorState, nil
}

func (e *Executor) abortTransaction(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionTarget,
	targets []transactionTarget,
	profile Profile,
) error {
	if err := e.abortCoordinator(ctx, id, coordinator, profile); err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	workers := min(max(1, profile.MaxConcurrency), len(targets))
	results := make(chan error, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(targets) {
					return
				}
				results <- e.abortTransactionTarget(ctx, id, &targets[i], profile)
			}
		})
	}
	var firstErr error
	for range targets {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	wait.Wait()
	return firstErr
}

func (e *Executor) abortTransactionTarget(
	ctx context.Context,
	id distributedtxn.ID,
	target *transactionTarget,
	profile Profile,
) error {
	lookup := transactionRequest(
		target.call.req, profile, shardservice.TransactionLookupTarget,
		id, 0, nil,
	)
	response, err := e.transactionRoundTrip(ctx, target.call.address, lookup, profile)
	if errors.Is(err, ErrTransactionNotFound) {
		abort := transactionRequest(
			target.call.req, profile, shardservice.TransactionAbortTarget,
			id, transactionInitialRevision, nil,
		)
		_, err = e.transactionRoundTrip(
			ctx, target.call.address, abort, profile,
		)
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
		target.call.req, profile, shardservice.TransactionAbortTarget,
		id, response.Transaction.Revision, nil,
	)
	_, err = e.transactionRoundTrip(ctx, target.call.address, abort, profile)
	return err
}
