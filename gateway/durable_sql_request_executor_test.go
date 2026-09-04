package gateway

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestDurableSQLRequestExecutorFusesLoweringCreateAndTypedExecution(t *testing.T) {
	_, planner := replicatedSQLTransactionFixture(t, true)
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL: `INSERT INTO messages VALUES (?)`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1","n":1}`)},
	}}
	tenant := []byte("durable-sql-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x41},
		Request: requestledger.RequestID{0x42}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x43}, IssuerSequence: 9,
	}
	key, err := NewDurableRequestLedgerKey(requestKey, replicatedSQLTransactionRequestDigest(queries))
	if err != nil {
		t.Fatal(err)
	}
	homeTargets := durableFaultTargets(t)
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	topology := durableFaultTopology(t, homeTargets)
	current := topology.Current()
	current.Generation = 7
	if err = topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		topology, ledger, typedServiceRunnerStop{}, pins,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(err, errTypedServicePin) {
		t.Fatalf("execute error=%v", err)
	}
	if !strings.Contains(err.Error(), "durable SQL execution") || strings.Contains(err.Error(), "message-1") {
		t.Fatalf("execution diagnostic lost stage or included document contents: %v", err)
	}
	if ledger.applies != 1 || ledger.reads != 0 || pins.called != 1 || ledger.head.PlanningLeaseSpan != 64 ||
		ledger.head.PlanningLeaseExpiryIndex != 66 ||
		ledger.head.RequestDigest != requestledger.Digest(key.Digest) {
		t.Fatalf("applies=%d reads=%d pins=%d head=%+v", ledger.applies, ledger.reads, pins.called, ledger.head)
	}
}

func TestDurableSQLRequestExecutorAdmitsAtomicMultiRowCrossShardInsert(t *testing.T) {
	_, planner, keys := replicatedSQLSplitTransactionFixture(t)
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL: `INSERT INTO messages VALUES (?),(?)`, Class: ClassInteractive,
		Params: []shardservice.Param{
			shardservice.DocumentParam(`{"id":"` + keys[0] + `","n":1}`),
			shardservice.DocumentParam(`{"id":"` + keys[1] + `","n":2}`),
		},
	}}
	tenant := []byte("durable-multi-row-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x71},
		Request: requestledger.RequestID{0x72}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x73}, IssuerSequence: 1,
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	topology := durableFaultTopology(t, durableFaultTargets(t))
	current := topology.Current()
	current.Generation = 7
	if err = topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		topology, ledger, typedServiceRunnerStop{}, pins,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(
		executeErr, errTypedServicePin,
	) {
		t.Fatalf("execute error=%v", executeErr)
	}
	if ledger.applies != 1 || ledger.reads != 0 || pins.called != 1 {
		t.Fatalf("admission applies=%d reads=%d pins=%d", ledger.applies, ledger.reads, pins.called)
	}
}

func TestDurableSQLRequestExecutorReplaysOwnPreparedIntent(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true, true, true)
	client, data := attachReplicatedSQLIndexedReadClient(t, snapshot,
		[]byte(`{"id":"message-1","email":"old@example.test","region":"old"}`))
	queries := []Query{{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1","email":"new@example.test","region":"new"}`), shardservice.StringParam("message-1")}}}
	tenant := []byte("prepared-intent-retry")
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2},
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 7,
		IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1}
	ledger, pins := new(typedServiceLedger), new(typedServicePinStop)
	topology := durableFaultTopology(t, durableFaultTargets(t))
	current := topology.Current()
	current.Generation = 7
	if err := topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service, RecoveryPulseLimit: 3, PlanningLeaseSpan: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, errTypedServicePin) {
		t.Fatalf("initial admission: %v", err)
	}
	client.refusal = shardservice.ReplicatedRefusalReadIntentActive
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, errTypedServicePin) || pins.called != 2 || ledger.applies != 1 {
		t.Fatalf("own intent did not resume exact recipe: pins=%d creates=%d err=%v", pins.called, ledger.applies, err)
	}
	queries[0].Params[0] = shardservice.DocumentParam(`{"id":"message-1","email":"changed@example.test","region":"new"}`)
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, ErrDurableRequestConflict) || pins.called != 2 {
		t.Fatalf("different request bytes resumed the retained recipe: %v", err)
	}
	key.IssuerSequence++
	ledger.head = requestledger.HeadRecord{}
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, ErrReplicatedReadIntentActive) || pins.called != 2 {
		t.Fatalf("foreign intent was treated as our continuation: %v", err)
	}
}

func TestDurableSQLComputedUpdateRetryRecoversRetainedProgramAfterReevaluationError(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	client, data := attachReplicatedSQLIndexedReadClient(
		t, snapshot, []byte(`{"divisor":2,"id":"message-1","n":10}`),
	)
	queries := []Query{{
		SQL: `UPDATE messages SET n = n / divisor WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}}
	tenant := []byte("computed-update-retained-retry")
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{11},
		Request:      requestledger.RequestID{12},
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 7,
		IssuerLane: requestledger.IssuerLane{13}, IssuerSequence: 1,
	}
	ledger, pins := new(typedServiceLedger), new(typedServicePinStop)
	topology := durableFaultTopology(t, durableFaultTargets(t))
	current := topology.Current()
	current.Generation = 7
	if err := topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		topology, ledger, typedServiceRunnerStop{}, pins,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, errTypedServicePin) {
		t.Fatalf("initial computed admission: %v", err)
	}
	client.value = []byte(`{"divisor":0,"id":"message-1","n":5}`)
	if _, err = executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, errTypedServicePin) {
		t.Fatalf("retry did not recover retained computed program: %v", err)
	}
	if client.reads != 2 || ledger.applies != 1 || pins.called != 2 {
		t.Fatalf("reads=%d creates=%d pins=%d", client.reads, ledger.applies, pins.called)
	}
}

func TestDurableSQLRequestExecutorAdmitsAtomicFiniteCrossShardDelete(t *testing.T) {
	_, planner, keys := replicatedSQLSplitTransactionFixture(t)
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL: `DELETE FROM messages WHERE id IN (?, ?)`, Class: ClassInteractive,
		Params: []shardservice.Param{
			shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1]),
		},
	}}
	tenant := []byte("durable-finite-delete-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x74},
		Request: requestledger.RequestID{0x75}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x76}, IssuerSequence: 1,
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	topology := durableFaultTopology(t, durableFaultTargets(t))
	current := topology.Current()
	current.Generation = 7
	if err = topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		topology, ledger, typedServiceRunnerStop{}, pins,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(
		executeErr, errTypedServicePin,
	) {
		t.Fatalf("execute error=%v", executeErr)
	}
	if ledger.applies != 1 || ledger.reads != 0 || pins.called != 1 {
		t.Fatalf("admission applies=%d reads=%d pins=%d", ledger.applies, ledger.reads, pins.called)
	}
}

func TestDurableSQLRequestExecutorRejectsTenantMismatchBeforeAdmission(t *testing.T) {
	queries := []Query{{SQL: `DELETE FROM accounts WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("account-1")}}}
	tenant := []byte("durable-sql-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256([]byte("other"))),
		IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1,
	}
	executor := &DurableSQLRequestExecutor{
		planner: new(Executor), data: new(ReplicatedExecutor), requests: new(DurableRequestService),
	}
	if _, err := executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(err, ErrDurableSQLRequest) {
		t.Fatalf("tenant mismatch error=%v", err)
	}
}

func TestDurableSQLReplayRejectsUnboundedParameterTypesBeforeDigest(t *testing.T) {
	planner := NewExecutor(nil, nil, Options{})
	executor := &DurableSQLRequestExecutor{planner: planner}
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest{3},
		IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{4}, IssuerSequence: 1,
	}
	query := Query{
		SQL:        "DELETE FROM messages WHERE id = ?",
		Class:      ClassInteractive,
		Params:     make([]shardservice.Param, maxGatewaySQLParameters+1),
		ParamTypes: make([]sqldriver.ParamType, maxGatewaySQLParameters+1),
	}
	if _, _, err := executor.ReplayRequest(t.Context(), key, []Query{query}); !errors.Is(err, ErrPlanParameters) {
		t.Fatalf("ReplayRequest error = %v, want bounded admission refusal", err)
	}
}

func TestDurableSQLExecuteAdmitsEveryItemBeforeSemanticPreparation(t *testing.T) {
	tenant := []byte("durable-batch-admission")
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1,
	}
	executor := &DurableSQLRequestExecutor{
		planner:  NewExecutor(nil, nil, Options{}),
		data:     new(ReplicatedExecutor),
		requests: new(DurableRequestService),
	}
	queries := []Query{
		{
			SQL: "DELETE FROM messages WHERE id IN (SELECT BOOL 't' UNION ALL SELECT ?)",
			Params: []shardservice.Param{
				shardservice.NullParam(),
			},
			ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeText},
			Class:      ClassInteractive,
		},
		{
			SQL:    "DELETE FROM messages WHERE id = ?",
			Params: []shardservice.Param{shardservice.NullParam()},
			ParamTypes: []sqldriver.ParamType{
				sqldriver.ParamTypeInvalid,
			},
			Class: ClassInteractive,
		},
	}
	if _, err := executor.Execute(t.Context(), key, tenant, queries); !errors.Is(err, ErrPlanParameters) {
		t.Fatalf("Execute error = %v, want later metadata refusal before parse/pin", err)
	}
}

func TestDurableSQLReplayUsesRetainedBytesWithoutCurrentPlannerSemantics(t *testing.T) {
	targets := durableFaultTargets(t)
	base := durableFaultRequest(t, targets)
	queries := []Query{{
		SQL: "DELETE FROM messages WHERE id IN (SELECT BOOL 't' UNION ALL SELECT ?)",
		Params: []shardservice.Param{
			shardservice.NullParam(),
		},
		ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeText},
		Class:      ClassAdmin,
	}}
	if err := validateTypedQueries(t.Context(), queries); err == nil {
		t.Fatal("test query unexpectedly passes current semantic analysis")
	}
	key, err := NewDurableRequestLedgerKey(
		base.Key.RequestKey, replicatedSQLTransactionRequestDigest(queries),
	)
	if err != nil {
		t.Fatal(err)
	}
	resultRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: 2, Transaction: base.Program.Identity.ID,
		CatalogGeneration:       base.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(base.Program.Targets)),
		TransitionTag:           base.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     base.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  base.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: base.Program.Contract.RetirementWitnessDigest,
		Payload:                 []byte("retained-result"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &typedServiceLedger{
		head: requestledger.HeadRecord{
			Key: base.Key.RequestKey, RequestDigest: requestledger.Digest(key.Digest),
			PlanRoot: requestledger.Digest{2}, Revision: 9, Phase: requestledger.PhaseTerminal,
		},
		terminal: requestledger.TerminalRecord{
			Revision: 9, Result: resultRaw, ResultDigest: requestledger.ResultDigest(resultRaw),
			RequestDigest:          requestledger.Digest(key.Digest),
			PlanRoot:               requestledger.Digest{2},
			CatalogGeneration:      base.Program.Identity.CatalogGeneration,
			TerminalContractDigest: requestledger.Digest(base.Program.Contract.TerminalContractDigest),
			AckToken:               requestledger.AckToken{1},
		},
	}
	ledger.terminal.KeyDigest, err = requestledger.KeyDigest(base.Key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		durableFaultTopology(t, targets), ledger,
		typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	reducedPlanner := NewExecutor(nil, nil, Options{Profiles: map[OperationClass]Profile{
		ClassAdmin: {MaxTransactionMutations: 1, MaxTransactionBytes: 1},
	}})
	if admissionErr := validateQueryBatchAdmission(
		queries, reducedPlanner.profileFor(ClassAdmin),
	); !errors.Is(admissionErr, ErrTransactionByteLimit) {
		t.Fatalf("test profile unexpectedly admits retained bytes: %v", admissionErr)
	}
	tests := []struct {
		name    string
		planner *Executor
	}{
		{name: "planner unavailable"},
		{name: "profile reduced", planner: reducedPlanner},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &DurableSQLRequestExecutor{planner: test.planner, requests: service}
			result, found, replayErr := executor.ReplayRequest(
				t.Context(), base.Key.RequestKey, queries,
			)
			if replayErr != nil || !found || result.Key != key || result.Result == nil ||
				result.Result.RowsAffected != 2 {
				t.Fatalf("result=%+v found=%v error=%v", result, found, replayErr)
			}
		})
	}
}

func TestDurableSQLRequestExecutorRejectsCatalogLedgerGenerationMixBeforeAdmission(t *testing.T) {
	_, planner := replicatedSQLTransactionFixture(t, true)
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL: `INSERT INTO messages VALUES (?)`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1"}`)},
	}}
	tenant := []byte("durable-sql-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x51},
		Request: requestledger.RequestID{0x52}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x53}, IssuerSequence: 1,
	}
	targets := durableFaultTargets(t)
	ledger := new(typedServiceLedger)
	service, err := newDurableRequestService(
		durableFaultTopology(t, targets), ledger, typedServiceRunnerStop{}, new(typedServicePinStop),
	)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("mixed generation error=%v", err)
	}
	if ledger.applies != 0 || ledger.reads != 0 {
		t.Fatalf("mixed generation reached admission: applies=%d reads=%d", ledger.applies, ledger.reads)
	}
}

func TestNewDurableRequestLedgerKeyRejectsIncompleteIssuerTuple(t *testing.T) {
	if _, err := NewDurableRequestLedgerKey(requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest{3},
	}, replication.Digest{4}); !errors.Is(err, ErrDurableSQLRequest) {
		t.Fatalf("incomplete key error=%v", err)
	}
}
