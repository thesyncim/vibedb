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
	"time"

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
	// count exceeds its operational profile before any participant is staged.
	ErrTransactionMutationLimit = errors.New("gateway: distributed transaction exceeds the mutation bound")
	// ErrTransactionByteLimit reports an atomic write whose canonical mutation
	// bytes exceed its operational profile before any participant is staged.
	ErrTransactionByteLimit = errors.New("gateway: distributed transaction exceeds the byte bound")
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
	if uint64(len(queries)) > profile.MaxTransactionMutations {
		return nil, ErrTransactionMutationLimit
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	snapshot, lease, err := e.pin(opctx, 0, 0)
	if err != nil {
		return nil, err
	}
	defer lease.release()
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
	return e.executeTransaction(opctx, snapshot, participants, profile)
}

func (e *Executor) planTransaction(
	ctx context.Context,
	snapshot *Snapshot,
	queries []Query,
	profile Profile,
) ([]transactionParticipant, error) {
	capacity := transactionParticipantCapacity(snapshot, len(queries), profile)
	participants := make([]transactionParticipant, 0, capacity)
	// Preserve the allocation-minimal inline lane: at most 64 statements use
	// the original tiny linear grouping. Potentially wide plans promote lazily
	// only after they prove they contain enough distinct targets.
	var participantIndex map[transactionTargetKey]int
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
		participants, err = appendBoundWriteParticipantsBudgeted(
			participants, participantIndex, *call, query, bound, profile, &budget,
		)
		if err != nil {
			return nil, err
		}
		if participantIndex == nil && len(queries) > distributedtxn.MaxInlineParticipants &&
			len(participants) >= 16 {
			participantIndex = make(map[transactionTargetKey]int, max(capacity, len(participants)))
			for participant := range participants {
				participantIndex[transactionKey(participants[participant].call.req)] = participant
			}
		}
	}
	sortTransactionParticipants(participants)
	return participants, nil
}

// transactionParticipantCapacity keeps the common inline lane compact even
// when a caller supplies a huge same-shard batch. It is only an allocation
// hint: the participant slice and exact-key index still grow for wide catalogs.
func transactionParticipantCapacity(snapshot *Snapshot, queries int, profile Profile) int {
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

func (b *transactionPlanBudget) admit(statement *shardservice.MutationStatement, newParticipant bool) error {
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
	if newParticipant {
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
// one statement to AppendMutationBatch, excluding its one-per-participant
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

func appendBoundWriteParticipants(
	participants []transactionParticipant,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
) ([]transactionParticipant, error) {
	return appendBoundWriteParticipantsIndexed(participants, nil, baseCall, query, bound, profile)
}

func appendBoundWriteParticipantsIndexed(
	participants []transactionParticipant,
	participantIndex map[transactionTargetKey]int,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
) ([]transactionParticipant, error) {
	return appendBoundWriteParticipantsBudgeted(
		participants, participantIndex, baseCall, query, bound, profile, nil,
	)
}

func appendBoundWriteParticipantsBudgeted(
	participants []transactionParticipant,
	participantIndex map[transactionTargetKey]int,
	baseCall shardCall,
	query *Query,
	bound *BoundWritePlan,
	profile Profile,
	budget *transactionPlanBudget,
) ([]transactionParticipant, error) {
	var err error
	if len(bound.primaryPath) != 0 {
		participants, err = appendTransactionStatementBudgeted(
			participants, participantIndex, baseCall,
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
	participants, err = appendTransactionStatementBudgeted(
		participants, participantIndex, baseCall,
		shardservice.MutationStatement{SQL: query.SQL, Params: query.Params}, budget,
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
		participants, err = appendTransactionStatementBudgeted(
			participants, participantIndex, call, shardservice.MutationStatement{
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
	return participants, nil
}

func appendTransactionStatement(
	participants []transactionParticipant,
	call shardCall,
	statement shardservice.MutationStatement,
) ([]transactionParticipant, error) {
	return appendTransactionStatementIndexed(participants, nil, call, statement)
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
	participants []transactionParticipant,
	byTarget map[transactionTargetKey]int,
	call shardCall,
	statement shardservice.MutationStatement,
) ([]transactionParticipant, error) {
	return appendTransactionStatementBudgeted(participants, byTarget, call, statement, nil)
}

func appendTransactionStatementBudgeted(
	participants []transactionParticipant,
	byTarget map[transactionTargetKey]int,
	call shardCall,
	statement shardservice.MutationStatement,
	budget *transactionPlanBudget,
) ([]transactionParticipant, error) {
	participantIndex := -1
	key := transactionKey(call.req)
	if byTarget != nil {
		if found, ok := byTarget[key]; ok {
			participantIndex = found
		}
	} else {
		for i := range participants {
			if sameTransactionTarget(participants[i].call.req, call.req) {
				participantIndex = i
				break
			}
		}
	}
	newParticipant := participantIndex < 0
	if !newParticipant && len(participants[participantIndex].statements) >= shardservice.MaxMutationStatements {
		return participants, ErrTransactionMutationLimit
	}
	if err := budget.admit(&statement, newParticipant); err != nil {
		return participants, err
	}
	if participantIndex < 0 {
		participants = append(participants, transactionParticipant{
			call: call, bucketBits: call.req.BucketBits,
			scopes: call.req.AccessScopes,
		})
		participantIndex = len(participants) - 1
		if byTarget != nil {
			byTarget[key] = participantIndex
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
	var mutationCount, mutationBytes uint64
	for i := range participants {
		participant := &participants[i]
		statements := uint64(len(participant.statements))
		if profile.MaxTransactionMutations == 0 ||
			mutationCount > profile.MaxTransactionMutations ||
			statements > profile.MaxTransactionMutations-mutationCount {
			return nil, ErrTransactionMutationLimit
		}
		mutationCount += statements
		participant.mutation, err = shardservice.AppendMutationBatch(nil, participant.statements)
		if err != nil {
			return nil, err
		}
		encodedBytes := uint64(len(participant.mutation))
		if profile.MaxTransactionBytes == 0 || mutationBytes > profile.MaxTransactionBytes ||
			encodedBytes > profile.MaxTransactionBytes-mutationBytes {
			return nil, ErrTransactionByteLimit
		}
		mutationBytes += encodedBytes
		participant.digest = distributedtxn.ParticipantDigest(
			participant.bucketBits, participant.scopes, participant.mutation,
		)
		request := participant.call.req
		refs[i] = distributedtxn.ParticipantRef{
			// The route strings are retained by participants through both immediate
			// coordinator passes; byteview avoids two identity allocations per shard.
			Distribution:         byteview.Bytes(string(request.Distribution)),
			Shard:                byteview.Bytes(string(request.Shard)),
			RoutingVersion:       uint64(request.RoutingVersion),
			AllocationGeneration: uint64(request.AllocationGeneration),
			OwnershipEpoch:       uint64(request.OwnershipEpoch), MutationDigest: participant.digest,
			State: distributedtxn.ParticipantStaged,
		}
	}
	coordinatorRecord := distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: transactionInitialRevision,
		CatalogGeneration: snapshot.Generation(),
		RecoveryDeadline:  time.Now().Add(profile.GlobalDeadline).UnixNano(),
		Participants:      refs,
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
	for i := range participants {
		participant := &participants[i]
		participant.record, err = distributedtxn.AppendParticipant(nil, distributedtxn.ParticipantRecord{
			ID: id, State: distributedtxn.ParticipantStaged, Revision: transactionInitialRevision,
			RoutingVersion:            uint64(participant.call.req.RoutingVersion),
			AllocationGeneration:      uint64(participant.call.req.AllocationGeneration),
			OwnershipEpoch:            uint64(participant.call.req.OwnershipEpoch),
			CoordinatorDistribution:   byteview.Bytes(string(coordinator.call.req.Distribution)),
			CoordinatorShard:          byteview.Bytes(string(coordinator.call.req.Shard)),
			CoordinatorAllocation:     uint64(coordinator.call.req.AllocationGeneration),
			CoordinatorRoutingVersion: uint64(coordinator.call.req.RoutingVersion),
			CoordinatorOwnershipEpoch: uint64(coordinator.call.req.OwnershipEpoch),
			BucketBits:                participant.bucketBits, IntentScopes: participant.scopes,
			MutationDigest: participant.digest, Mutation: participant.mutation,
		})
		if err != nil {
			abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
			if abortErr != nil {
				return nil, transactionAbortFailure(id, err, abortErr)
			}
			return nil, err
		}
	}
	if err := e.participantCompletionPhase(ctx, id, participants, profile, shardservice.TransactionStageParticipant, 0); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
		if abortErr != nil {
			return nil, transactionAbortFailure(id, err, abortErr)
		}
		return nil, err
	}
	if err := e.participantCompletionPhase(
		ctx, id, participants, profile, shardservice.TransactionPrepareParticipant,
		transactionInitialRevision,
	); err != nil {
		abortErr := e.abortTransaction(ctx, id, coordinator, participants, profile)
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
	affected, applyErr := e.participantApplyPhase(
		ctx, id, participants, profile, shardservice.TransactionApplyParticipant,
		transactionInitialRevision+1,
	)
	if applyErr != nil {
		return nil, &CommittedTransactionError{ID: id, Cause: applyErr}
	}
	if releaseErr := e.participantCompletionPhase(
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
	switch operation {
	case shardservice.TransactionStageCoordinator,
		shardservice.TransactionStageParticipant,
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

func (e *Executor) participantPhase(
	ctx context.Context,
	id distributedtxn.ID,
	participants []transactionParticipant,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) ([]*shardservice.ShardResponse, error) {
	if operation != shardservice.TransactionApplyParticipant {
		return nil, e.participantCompletionPhase(
			ctx, id, participants, profile, operation, revision,
		)
	}
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

type transactionPhaseResult struct {
	response *shardservice.ShardResponse
	err      error
}

// visitParticipantPhase retains only a worker-sized result window. Wide
// transactions therefore do not allocate response and error slices merely to
// learn that a completion phase succeeded or to sum applied rows.
func (e *Executor) visitParticipantPhase(
	ctx context.Context,
	id distributedtxn.ID,
	participants []transactionParticipant,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
	visit func(*shardservice.ShardResponse) error,
) error {
	if len(participants) == 0 {
		return nil
	}
	workers := min(max(1, profile.MaxConcurrency), len(participants))
	results := make(chan transactionPhaseResult, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(participants) {
					return
				}
				participant := &participants[i]
				var record []byte
				if operation == shardservice.TransactionStageParticipant {
					record = participant.record
				}
				request := transactionRequest(
					participant.call.req, profile, operation, id, revision, record,
				)
				response, err := e.transactionRoundTrip(
					ctx, participant.call.address, request, profile,
				)
				results <- transactionPhaseResult{response: response, err: err}
			}
		})
	}
	var firstErr error
	for range participants {
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

func (e *Executor) participantCompletionPhase(
	ctx context.Context,
	id distributedtxn.ID,
	participants []transactionParticipant,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) error {
	return e.visitParticipantPhase(ctx, id, participants, profile, operation, revision, nil)
}

func (e *Executor) participantApplyPhase(
	ctx context.Context,
	id distributedtxn.ID,
	participants []transactionParticipant,
	profile Profile,
	operation shardservice.TransactionOperation,
	revision uint64,
) (int64, error) {
	var affected int64
	err := e.visitParticipantPhase(ctx, id, participants, profile, operation, revision,
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
// must not mutate participant state until this returns nil. A lost race to
// commit is reported as a known committed outcome; an unresolvable response is
// outcome-unknown and leaves every participant untouched.
func (e *Executor) abortCoordinator(
	ctx context.Context,
	id distributedtxn.ID,
	coordinator *transactionParticipant,
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
	coordinator *transactionParticipant,
	participants []transactionParticipant,
	profile Profile,
) error {
	if err := e.abortCoordinator(ctx, id, coordinator, profile); err != nil {
		return err
	}
	if len(participants) == 0 {
		return nil
	}
	workers := min(max(1, profile.MaxConcurrency), len(participants))
	results := make(chan error, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(participants) {
					return
				}
				results <- e.abortTransactionParticipant(ctx, id, &participants[i], profile)
			}
		})
	}
	var firstErr error
	for range participants {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	wait.Wait()
	return firstErr
}

func (e *Executor) abortTransactionParticipant(
	ctx context.Context,
	id distributedtxn.ID,
	participant *transactionParticipant,
	profile Profile,
) error {
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
		_, err = e.transactionRoundTrip(
			ctx, participant.call.address, abort, profile,
		)
		return err
	}
	if err != nil {
		return err
	}
	state := response.Transaction.ParticipantState
	if state == distributedtxn.ParticipantAborted || state == distributedtxn.ParticipantReleased {
		return nil
	}
	if state != distributedtxn.ParticipantStaged && state != distributedtxn.ParticipantPrepared {
		return ErrTransactionConflict
	}
	abort := transactionRequest(
		participant.call.req, profile, shardservice.TransactionAbortParticipant,
		id, response.Transaction.Revision, nil,
	)
	_, err = e.transactionRoundTrip(ctx, participant.call.address, abort, profile)
	return err
}
