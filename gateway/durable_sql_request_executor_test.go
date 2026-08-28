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
	homeParticipants := durableFaultParticipants(t)
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	topology := durableFaultTopology(t, homeParticipants)
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
	topology := durableFaultTopology(t, durableFaultParticipants(t))
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
	topology := durableFaultTopology(t, durableFaultParticipants(t))
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
	topology := durableFaultTopology(t, durableFaultParticipants(t))
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
	participants := durableFaultParticipants(t)
	ledger := new(typedServiceLedger)
	service, err := newDurableRequestService(
		durableFaultTopology(t, participants), ledger, typedServiceRunnerStop{}, new(typedServicePinStop),
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
