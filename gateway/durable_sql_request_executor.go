package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrDurableSQLRequest = errors.New("gateway: durable RF3 SQL request is unavailable")

// ErrDurableSQLNotAdmitted proves this invocation did not attempt ledger
// admission. It is not proof that an earlier invocation with the same identity
// was never admitted; recovery must always retain that earlier identity.
var ErrDurableSQLNotAdmitted = errors.New("gateway: SQL request was not admitted by this invocation")
var ErrDurableSQLAborted = errors.New("gateway: durable SQL transaction aborted")

// DurableSQLExecutionMode fixes the consensus protocol for one durable issuer
// lane. Direct and coordinated lanes require independent sequence counters.
type DurableSQLExecutionMode uint8

const (
	// DurableSQLLegacyAuto is only for recovery of pre-mode durable outboxes.
	DurableSQLLegacyAuto DurableSQLExecutionMode = iota
	DurableSQLDirectOnly
	DurableSQLCoordinated
)

// ErrDurableSQLDirectIneligible accompanies ErrDurableSQLNotAdmitted when a
// direct-only request cannot be lowered without entering the request ledger.
var ErrDurableSQLDirectIneligible = errors.New("gateway: SQL request requires coordinated execution")

type DurableSQLRequestExecutorOptions struct {
	Planner            *Executor
	ReplicatedData     *ReplicatedExecutor
	Requests           *DurableRequestService
	RecoveryPulseLimit uint8
	PlanningLeaseSpan  uint64
	// SingleParticipantFastPath enables explicit direct execution and prepared
	// recipes. Execute itself always uses the coordinated issuer domain.
	SingleParticipantFastPath bool
}

// DurableSQLRequestExecutor is the production composition boundary from one
// authenticated structured request to the fused request-ledger Create and the
// typed distributed runner. It has no legacy orchestrator or local registry.
type DurableSQLRequestExecutor struct {
	planner           *Executor
	data              *ReplicatedExecutor
	requests          *DurableRequestService
	recoveryPulses    uint8
	planningLeaseSpan uint64
	singleFast        bool
}

type DurableSQLRequestResult struct {
	Key              DurableRequestLedgerKey
	Result           *Result
	TerminalRevision uint64
	ResultDigest     replication.Digest
	AckToken         DurableRequestAckToken
	// Direct is terminal in the participant group itself and therefore has no
	// request-ledger ACK capability or terminal-ledger revision.
	Direct bool
}

func NewDurableRequestLedgerKey(
	key requestledger.RequestKey,
	requestDigest replication.Digest,
) (DurableRequestLedgerKey, error) {
	result := DurableRequestLedgerKey{RequestKey: key, Digest: requestDigest}
	if !validDurableRequestLedgerKey(result) {
		return DurableRequestLedgerKey{}, ErrDurableSQLRequest
	}
	return result, nil
}

func NewDurableSQLRequestExecutor(
	options DurableSQLRequestExecutorOptions,
) (*DurableSQLRequestExecutor, error) {
	if options.Planner == nil || options.Planner.catalog == nil ||
		options.ReplicatedData == nil || options.Requests == nil ||
		options.RecoveryPulseLimit == 0 ||
		options.RecoveryPulseLimit > distributedtxn.MaxRecoveryPulses ||
		options.PlanningLeaseSpan == 0 ||
		options.PlanningLeaseSpan > requestledger.MaxPlanningLeaseSpan {
		return nil, ErrDurableSQLRequest
	}
	return &DurableSQLRequestExecutor{
		planner: options.Planner, data: options.ReplicatedData, requests: options.Requests,
		recoveryPulses: options.RecoveryPulseLimit, planningLeaseSpan: options.PlanningLeaseSpan,
		singleFast: options.SingleParticipantFastPath,
	}, nil
}

func (executor *DurableSQLRequestExecutor) Execute(
	ctx context.Context,
	requestKey requestledger.RequestKey,
	tenant []byte,
	queries []Query,
) (DurableSQLRequestResult, error) {
	// An unqualified issuer may mix SQL shapes. Keep all its sequences in the
	// ledger; direct callers must explicitly own an independent issuer lane.
	return executor.ExecuteMode(ctx, requestKey, tenant, queries, DurableSQLCoordinated)
}

// ExecuteMode never silently switches a direct-only request into the ledger.
// Persist mode with the caller's identity before its first invocation. An
// unknown outcome must be recovered in that original mode.
func (executor *DurableSQLRequestExecutor) ExecuteMode(
	ctx context.Context, requestKey requestledger.RequestKey, tenant []byte,
	queries []Query, mode DurableSQLExecutionMode,
) (result DurableSQLRequestResult, err error) {
	admitted := false
	defer func() {
		if err != nil && !admitted {
			err = errors.Join(ErrDurableSQLNotAdmitted, err)
		}
	}()
	if executor == nil || ctx == nil || executor.planner == nil || executor.data == nil ||
		executor.requests == nil || mode > DurableSQLCoordinated || !requestKey.Valid() || len(tenant) == 0 || len(queries) == 0 ||
		requestledger.Digest(sha256.Sum256(tenant)) != requestKey.TenantDigest {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	if err := validateQueryBatchAdmission(queries, profile); err != nil {
		return DurableSQLRequestResult{}, err
	}
	if err := validateTypedQueries(ctx, queries); err != nil {
		return DurableSQLRequestResult{}, err
	}
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	var lease catalogLease
	var home DurableRequestLedgerHome
	var participants []ReplicatedTransactionParticipant
	var handled bool
	refreshedMiss := false
	for {
		lease = executor.planner.catalog.pinCurrent()
		if lease.snapshot == nil || lease.generation == 0 {
			lease.release()
			return DurableSQLRequestResult{}, ErrNoCatalog
		}
		home, err = executor.requests.home(key)
		if err != nil {
			lease.release()
			return DurableSQLRequestResult{}, err
		}
		if home.TopologyGeneration != lease.generation {
			lease.release()
			return DurableSQLRequestResult{}, fmt.Errorf("gateway: SQL catalog generation %d differs from ledger topology %d: %w",
				lease.generation, home.TopologyGeneration, ErrDurableRequestConflict)
		}
		participants, handled, err = executor.planner.planReplicatedSQLTransactionWithData(
			opctx, lease.snapshot, queries, profile, executor.data,
		)
		if !errors.Is(err, ErrTableNotPlaced) || refreshedMiss {
			break
		}
		refreshedMiss = true
		staleGeneration := lease.generation
		lease.release()
		if refreshErr := executor.planner.refreshAfterCatalogMiss(opctx, staleGeneration); refreshErr != nil {
			// Keep the existing lowering-failure replay path intact. The
			// request may already have a retained terminal recipe even when
			// this process still has stale table metadata; refresh failure
			// must never turn that recoverable identity into a new refusal.
			err = preserveCatalogMiss(err, refreshErr)
			break
		}
	}
	defer lease.release()
	if err != nil || !handled || len(participants) == 0 {
		if err != nil {
			// Planning happens before the fused ledger Create. An exact retry can
			// therefore observe its own prepared intent or a committed row whose
			// computed UPDATE now evaluates differently. Recover the authenticated
			// retained recipe on every lowering failure; no matching record leaves
			// the original refusal authoritative.
			result, found, replayErr := executor.Replay(opctx, key)
			if found {
				admitted = true
				return result, replayErr
			}
			err = errors.Join(err, replayErr)
		}
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: durable SQL lowering: %w", errors.Join(err, ErrDurableSQLRequest))
	}
	if mode != DurableSQLCoordinated && executor.singleFast && key.IssuerSequence != 0 &&
		directSQLMutationEligible(queries, participants) {
		admitted = true
		direct, directErr := executor.executeDirect(
			opctx, key, tenant, lease.generation, participants[0],
		)
		if direct.Result != nil && !direct.duplicate {
			executor.observeMutationPressure(lease.snapshot, participants)
		}
		if directErr != nil {
			if errors.Is(directErr, ErrReplicatedTransactionConflict) {
				return direct.DurableSQLRequestResult, fmt.Errorf(
					"gateway: direct SQL execution: %w", ErrDurableSQLAborted,
				)
			}
			return DurableSQLRequestResult{}, fmt.Errorf("gateway: direct SQL execution: %w", directErr)
		}
		return direct.DurableSQLRequestResult, nil
	}
	if mode == DurableSQLDirectOnly {
		return DurableSQLRequestResult{}, ErrDurableSQLDirectIneligible
	}
	program, err := BuildDurableRequestLogicalProgram(DurableRequestLogicalProgramBuild{
		Home: home, Key: key, Tenant: tenant, CatalogGeneration: lease.generation,
		RecoveryDeadline:        int64(executor.recoveryPulses),
		PlanningLeaseSpan:       executor.planningLeaseSpan,
		PlanningLeaseGeneration: home.TopologyGeneration,
		PinEpoch:                home.TopologyGeneration, Participants: participants,
		MembershipStable: true,
	})
	if err != nil {
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: durable SQL program construction: %w", err)
	}
	request := DurableRequest{Key: key, Program: program}
	admitted = true // Even an unsuccessful Begin can have an unknown outcome.
	begin, err := executor.requests.Begin(opctx, request)
	if err != nil {
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: durable SQL admission: %w", err)
	}
	var outcome DurableRequestOutcome
	if begin.ProgramMatches {
		// The fused Create is the one logical admission point for a caller
		// request. Sample its already-grouped shard participants only for the
		// successful creator: request retries and transaction recovery waves must
		// not amplify hot-shard pressure.
		if begin.Created {
			executor.observeMutationPressure(lease.snapshot, participants)
		}
		outcome, err = executor.requests.ExecuteBegun(opctx, request, begin)
	} else {
		var found bool
		outcome, found, err = executor.requests.Replay(opctx, key)
		if err == nil && !found {
			err = ErrDurableRequestUnresolved
		}
	}
	if err != nil {
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: durable SQL execution (matching plan=%t): %w", begin.ProgramMatches, err)
	}
	return executor.result(key, outcome)
}

type directSQLRequestResult struct {
	DurableSQLRequestResult
	duplicate bool
}

func (executor *DurableSQLRequestExecutor) executeDirect(
	ctx context.Context,
	key DurableRequestLedgerKey,
	tenant []byte,
	catalogGeneration uint64,
	participant ReplicatedTransactionParticipant,
) (directSQLRequestResult, error) {
	direct, err := executor.data.DirectMutate(ctx, ReplicatedDirectMutation{
		Key: key.RequestKey, RequestDigest: key.Digest, Tenant: tenant, Participant: participant,
	})
	if direct.ID == (distributedtxn.ID{}) {
		return directSQLRequestResult{}, err
	}
	result := directSQLRequestResult{
		DurableSQLRequestResult: DurableSQLRequestResult{
			Key: key, Direct: true,
			Result: &Result{
				Kind: shardservice.ResponseCompletion, RowsAffected: direct.AffectedRows,
				TransactionID: replication.ID128(direct.ID), RouteKind: distribution.RouteTargeted,
				Generation: catalogGeneration, ShardsFanned: 1, Retries: direct.Retries,
			},
		},
		duplicate: direct.Duplicate,
	}
	if executor.planner != nil {
		executor.planner.metrics.observeRoute(distribution.RouteTargeted, 1, ScatterNone)
	}
	return result, err
}

func directSQLMutationEligible(
	queries []Query,
	participants []ReplicatedTransactionParticipant,
) bool {
	if len(queries) != 1 || len(participants) != 1 || len(participants[0].Batches) != 1 {
		return false
	}
	mutations := participants[0].Batches[0].Mutations
	if len(mutations) == 0 {
		return false
	}
	for index := range mutations {
		switch mutations[index].Kind {
		case replication.MutationPut, replication.MutationPutAbsentOrEqual,
			replication.MutationDelete, replication.MutationPutAbsent:
		default:
			return false
		}
	}
	return true
}

// ReplayRequest resolves retained caller bytes before any new planning. A
// committed INSERT may now conflict with its own row; replay must not replan it.
func (executor *DurableSQLRequestExecutor) ReplayRequest(ctx context.Context, requestKey requestledger.RequestKey, queries []Query) (DurableSQLRequestResult, bool, error) {
	if executor == nil || ctx == nil || !requestKey.Valid() {
		return DurableSQLRequestResult{}, false, ErrDurableSQLRequest
	}
	if err := validateDurableSQLReplayAdmission(queries); err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	return executor.Replay(ctx, key)
}

// ReplayRequestWithTenant reconstructs the terminal one-group command after a
// gateway or client-outbox restart. The direct command is a pure function of
// the authenticated request identity, caller digest, route, and replay-stable
// canonical mutation, so no gateway-local session state is required.
func (executor *DurableSQLRequestExecutor) ReplayRequestWithTenant(
	ctx context.Context,
	requestKey requestledger.RequestKey,
	tenant []byte,
	queries []Query,
) (DurableSQLRequestResult, bool, error) {
	if executor == nil || ctx == nil || !executor.singleFast || !requestKey.Valid() ||
		len(tenant) == 0 || requestledger.Digest(sha256.Sum256(tenant)) != requestKey.TenantDigest {
		return executor.ReplayRequest(ctx, requestKey, queries)
	}
	if err := validateDurableSQLReplayAdmission(queries); err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	if err = validateQueryBatchAdmission(queries, profile); err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	lease := executor.planner.catalog.pinCurrent()
	if lease.snapshot == nil || lease.generation == 0 {
		return DurableSQLRequestResult{}, false, ErrNoCatalog
	}
	defer lease.release()
	participants, handled, planErr := executor.planner.planReplicatedSQLTransactionWithData(
		opctx, lease.snapshot, queries, profile, executor.data,
	)
	if planErr != nil || !handled || key.IssuerSequence == 0 ||
		!directSQLMutationEligible(queries, participants) {
		return executor.Replay(opctx, key)
	}
	direct, directErr := executor.executeDirect(
		opctx, key, tenant, lease.generation, participants[0],
	)
	if direct.Result == nil || direct.Result.TransactionID == (replication.ID128{}) {
		return DurableSQLRequestResult{}, true, directErr
	}
	if !direct.duplicate {
		executor.observeMutationPressure(lease.snapshot, participants)
	}
	if errors.Is(directErr, ErrReplicatedTransactionConflict) {
		return direct.DurableSQLRequestResult, true, ErrDurableSQLAborted
	}
	return direct.DurableSQLRequestResult, true, directErr
}

// observeMutationPressure samples one logical write per routed participant.
// Participant scopes were canonicalized by SQL lowering, so this boundary
// preserves exact bucket locality without counting every statement, relation
// batch, global-index side effect, or distributed-transaction retry again.
func (executor *DurableSQLRequestExecutor) observeMutationPressure(
	snapshot *Snapshot,
	participants []ReplicatedTransactionParticipant,
) {
	if executor == nil || executor.planner == nil || executor.planner.pressure == nil ||
		snapshot == nil {
		return
	}
	for index := range participants {
		participant := &participants[index]
		source := replicatedDataPressureSource(snapshot, participant.Route)
		if source == (autosplit.SourceIdentity{}) {
			continue
		}
		executor.planner.pressure.ObservePressure(PressureObservation{
			Source: source, AccessScopes: participant.IntentScopes, Write: true,
		})
	}
}

func (executor *DurableSQLRequestExecutor) Replay(
	ctx context.Context,
	key DurableRequestLedgerKey,
) (DurableSQLRequestResult, bool, error) {
	if executor == nil || ctx == nil || executor.requests == nil ||
		!validDurableRequestLedgerKey(key) {
		return DurableSQLRequestResult{}, false, ErrDurableSQLRequest
	}
	outcome, found, err := executor.requests.Replay(ctx, key)
	if err != nil || !found {
		return DurableSQLRequestResult{}, found, err
	}
	result, err := executor.result(key, outcome)
	return result, true, err
}

func (executor *DurableSQLRequestExecutor) Acknowledge(
	ctx context.Context,
	key DurableRequestLedgerKey,
	terminalRevision uint64,
	resultDigest replication.Digest,
	token DurableRequestAckToken,
) (DurableRequestAckResult, error) {
	if executor == nil || executor.requests == nil {
		return DurableRequestAckResult{}, ErrDurableSQLRequest
	}
	return executor.requests.Acknowledge(ctx, key, terminalRevision, resultDigest, token)
}

func (executor *DurableSQLRequestExecutor) profile(queries []Query) (Profile, error) {
	if len(queries) == 0 {
		return Profile{}, ErrDurableSQLRequest
	}
	class := queries[0].Class
	profile, ok := executor.planner.profiles[class]
	if !ok {
		return Profile{}, ErrDurableSQLRequest
	}
	if profile.MaxTransactionMutations == 0 || profile.MaxTransactionBytes == 0 {
		return Profile{}, ErrDurableSQLRequest
	}
	if uint64(len(queries)) > profile.MaxTransactionMutations {
		return Profile{}, ErrTransactionMutationLimit
	}
	for index := 1; index < len(queries); index++ {
		if queries[index].Class != class {
			return Profile{}, ErrBatchClassMismatch
		}
	}
	return profile, nil
}

func (executor *DurableSQLRequestExecutor) result(
	key DurableRequestLedgerKey,
	outcome DurableRequestOutcome,
) (DurableSQLRequestResult, error) {
	if outcome.CatalogGeneration == 0 ||
		outcome.ShardsFanned <= 0 || outcome.TerminalRevision == 0 ||
		outcome.ResultDigest == (replication.Digest{}) || outcome.AckToken == (DurableRequestAckToken{}) {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	if executor.planner != nil {
		executor.planner.metrics.observeRoute(distribution.RouteTargeted, outcome.ShardsFanned, ScatterNone)
	}
	result := DurableSQLRequestResult{
		Key: key,
		Result: &Result{
			Kind: shardservice.ResponseCompletion, RowsAffected: outcome.AffectedRows,
			TransactionID: replication.ID128(outcome.ID), RouteKind: distribution.RouteTargeted,
			Generation: outcome.CatalogGeneration, ShardsFanned: outcome.ShardsFanned,
		},
		TerminalRevision: outcome.TerminalRevision,
		ResultDigest:     outcome.ResultDigest, AckToken: outcome.AckToken,
	}
	if !outcome.Committed {
		return result, ErrDurableSQLAborted
	}
	return result, nil
}
