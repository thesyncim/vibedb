package gateway

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrDurableSQLRequest = errors.New("gateway: durable RF3 SQL request is unavailable")

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
) (DurableSQLRequestResult, error) {
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
	participants, handled, err := executor.planner.planReplicatedSQLTransactionWithData(
		opctx, lease.snapshot, queries, profile, executor.data,
	)
	if err != nil || !handled || len(participants) == 0 {
		return DurableSQLRequestResult{}, errors.Join(err, ErrDurableSQLRequest)
	}
	program, err := BuildDurableRequestLogicalProgram(DurableRequestLogicalProgramBuild{
		Home: home, Key: key, Tenant: tenant, CatalogGeneration: lease.generation,
		RecoveryDeadline:        int64(executor.recoveryPulses),
		PlanningLeaseSpan:       executor.planningLeaseSpan,
		PlanningLeaseGeneration: home.TopologyGeneration,
		PinEpoch:                home.TopologyGeneration, Participants: participants,
	})
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	request := DurableRequest{Key: key, Program: program}
	begin, err := executor.requests.Begin(opctx, request)
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	var outcome DurableRequestOutcome
	if begin.ProgramMatches {
		outcome, err = executor.requests.ExecuteBegun(opctx, request, begin)
	} else {
		var found bool
		outcome, found, err = executor.requests.Replay(opctx, key)
		if err == nil && !found {
			err = ErrDurableRequestUnresolved
		}
	}
	if err != nil {
		return DurableSQLRequestResult{}, err
	}
	return executor.result(key, outcome)
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
	if !outcome.Committed || outcome.Recovery != nil || outcome.CatalogGeneration == 0 ||
		outcome.ShardsFanned <= 0 || outcome.TerminalRevision == 0 ||
		outcome.ResultDigest == (replication.Digest{}) || outcome.AckToken == (DurableRequestAckToken{}) {
		return DurableSQLRequestResult{}, ErrDurableSQLRequest
	}
	executor.planner.metrics.observeRoute(distribution.RouteTargeted, outcome.ShardsFanned, ScatterNone)
	return DurableSQLRequestResult{
		Key: key,
		Result: &Result{
			Kind: shardservice.ResponseCompletion, RowsAffected: outcome.AffectedRows,
			TransactionID: replication.ID128(outcome.ID), RouteKind: distribution.RouteTargeted,
			Generation: outcome.CatalogGeneration, ShardsFanned: outcome.ShardsFanned,
		},
		TerminalRevision: outcome.TerminalRevision,
		ResultDigest:     outcome.ResultDigest, AckToken: outcome.AckToken,
	}, nil
}
