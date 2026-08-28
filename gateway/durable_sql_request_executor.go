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

type DurableSQLRequestExecutorOptions struct {
	Planner            *Executor
	ReplicatedData     *ReplicatedExecutor
	Requests           *DurableRequestService
	RecoveryPulseLimit uint8
	PlanningLeaseSpan  uint64
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
}

type DurableSQLRequestResult struct {
	Key              DurableRequestLedgerKey
	Result           *Result
	TerminalRevision uint64
	ResultDigest     replication.Digest
	AckToken         DurableRequestAckToken
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
	}, nil
}

func (executor *DurableSQLRequestExecutor) Execute(
	ctx context.Context,
	requestKey requestledger.RequestKey,
	tenant []byte,
	queries []Query,
) (result DurableSQLRequestResult, err error) {
	admitted := false
	defer func() {
		if err != nil && !admitted {
			err = errors.Join(ErrDurableSQLNotAdmitted, err)
		}
	}()
	if executor == nil || ctx == nil || executor.planner == nil || executor.data == nil ||
		executor.requests == nil || !requestKey.Valid() || len(tenant) == 0 || len(queries) == 0 ||
		requestledger.Digest(sha256.Sum256(tenant)) != requestKey.TenantDigest {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	profile, err := executor.profile(queries)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	opctx, cancel := context.WithTimeout(ctx, profile.GlobalDeadline)
	defer cancel()
	lease := executor.planner.catalog.pinCurrent()
	if lease.snapshot == nil || lease.generation == 0 {
		return DurableSQLRequestResult{}, ErrNoCatalog
	}
	defer lease.release()
	home, err := executor.requests.home(key)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	if home.TopologyGeneration != lease.generation {
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: SQL catalog generation %d differs from ledger topology %d: %w",
			lease.generation, home.TopologyGeneration, ErrDurableRequestConflict)
	}
	participants, handled, err := executor.planner.planReplicatedSQLTransactionWithData(
		opctx, lease.snapshot, queries, profile, executor.data,
	)
	if err != nil || !handled || len(participants) == 0 {
		if errors.Is(err, ErrReplicatedReadIntentActive) {
			// An exact retry may encounter its own already-prepared writes.
			// Recover the authenticated retained recipe instead of requiring
			// a new row image through that intent. No record means this is
			// another request's intent and the original refusal still stands.
			result, found, replayErr := executor.Replay(opctx, key)
			if found {
				admitted = true
				return result, replayErr
			}
			err = errors.Join(err, replayErr)
		}
		return DurableSQLRequestResult{}, fmt.Errorf("gateway: durable SQL lowering: %w", errors.Join(err, ErrDurableSQLRequest))
	}
	program, err := BuildDurableRequestLogicalProgram(DurableRequestLogicalProgramBuild{
		Home: home, Key: key, Tenant: tenant, CatalogGeneration: lease.generation,
		RecoveryDeadline:        int64(executor.recoveryPulses),
		PlanningLeaseSpan:       executor.planningLeaseSpan,
		PlanningLeaseGeneration: home.TopologyGeneration,
		PinEpoch:                home.TopologyGeneration, Participants: participants,
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

// ReplayRequest resolves retained caller bytes before any new planning. A
// committed INSERT may now conflict with its own row; replay must not replan it.
func (executor *DurableSQLRequestExecutor) ReplayRequest(ctx context.Context, requestKey requestledger.RequestKey, queries []Query) (DurableSQLRequestResult, bool, error) {
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		return DurableSQLRequestResult{}, false, err
	}
	return executor.Replay(ctx, key)
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
	for index := 1; index < len(queries); index++ {
		if queries[index].Class != class {
			return Profile{}, ErrBatchClassMismatch
		}
	}
	profile, ok := executor.planner.profiles[class]
	if !ok {
		return Profile{}, ErrDurableSQLRequest
	}
	if profile.MaxTransactionMutations == 0 || profile.MaxTransactionBytes == 0 {
		return Profile{}, ErrDurableSQLRequest
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
	executor.planner.metrics.observeRoute(distribution.RouteTargeted, outcome.ShardsFanned, ScatterNone)
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
