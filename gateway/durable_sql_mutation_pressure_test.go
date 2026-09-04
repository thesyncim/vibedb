package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
)

type durableSQLPressureObserver struct {
	calls        int
	observations [4]PressureObservation
}

func (observer *durableSQLPressureObserver) ObservePressure(observation PressureObservation) {
	if observer.calls < len(observer.observations) {
		observer.observations[observer.calls] = observation
	}
	observer.calls++
}

func TestDurableSQLMutationPressureCoversSingleBatchAndMultiTable(t *testing.T) {
	tests := []struct {
		name    string
		queries []Query
		want    int
	}{
		{name: "single-insert", want: 1, queries: []Query{{
			SQL: `INSERT INTO messages VALUES (?)`, Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1"}`)},
		}}},
		{name: "single-delete", want: 1, queries: []Query{{
			SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringParam("message-2")},
		}}},
		{name: "same-shard-batch", want: 1, queries: []Query{
			{SQL: `DELETE FROM accounts WHERE id = ?`, Class: ClassInteractive,
				Params: []shardservice.Param{shardservice.StringParam("account-1")}},
			{SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
				Params: []shardservice.Param{shardservice.StringParam("message-3")}},
		}},
		{name: "multi-table-multi-shard", want: 2, queries: []Query{
			{SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
				Params: []shardservice.Param{shardservice.StringParam("message-4")}},
			{SQL: `DELETE FROM logs WHERE id = ?`, Class: ClassInteractive,
				Params: []shardservice.Param{shardservice.StringParam("log-1")}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot, planner := replicatedSQLTransactionFixture(t, true)
			observer := new(durableSQLPressureObserver)
			if !planner.InstallPressureObserver(observer) {
				t.Fatal("install pressure observer")
			}
			targets, handled, err := planner.planReplicatedSQLTransactionWithData(
				context.Background(), snapshot, test.queries,
				planner.profileFor(ClassInteractive), nil,
			)
			if err != nil || !handled || len(targets) != test.want {
				t.Fatalf("participants=%d handled=%v err=%v", len(targets), handled, err)
			}
			executor := &DurableSQLRequestExecutor{planner: planner}
			executor.observeMutationPressure(snapshot, targets)
			if observer.calls != test.want {
				t.Fatalf("pressure calls=%d want=%d", observer.calls, test.want)
			}
			for index := range targets {
				observation := observer.observations[index]
				wantSource := replicatedDataPressureSource(snapshot, targets[index].Route)
				if !observation.Write || observation.Source != wantSource ||
					len(observation.AccessScopes) != len(targets[index].IntentScopes) {
					t.Fatalf("observation[%d]=%+v participant=%+v", index, observation, targets[index])
				}
				for scope := range observation.AccessScopes {
					if observation.AccessScopes[scope] != targets[index].IntentScopes[scope] {
						t.Fatalf("observation[%d] scopes=%+v want=%+v", index,
							observation.AccessScopes, targets[index].IntentScopes)
					}
				}
			}
		})
	}
}

func TestDurableSQLMutationPressureCountsFusedAdmissionOnceAcrossRestartedRetry(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	observer := new(durableSQLPressureObserver)
	if !planner.InstallPressureObserver(observer) {
		t.Fatal("install pressure observer")
	}
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.StringParam("message-retry")},
	}}
	tenant := []byte("durable-pressure-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x61},
		Request: requestledger.RequestID{0x62}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x63}, IssuerSequence: 1,
	}
	topology := durableFaultTopology(t, durableFaultTargets(t))
	current := topology.Current()
	current.Generation = 7
	if err = topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(
		topology, new(typedServiceLedger), typedServiceRunnerStop{}, new(typedServicePinStop),
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
	if _, executeErr := executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(executeErr, errTypedServicePin) {
		t.Fatalf("initial execute error=%v", executeErr)
	}
	// Rebuild both stateless gateway execution layers while retaining only the
	// replicated request service. The recovered exact admission must not turn
	// into a second pressure sample.
	restartedPlanner := NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	if !restartedPlanner.InstallPressureObserver(observer) {
		t.Fatal("install restarted pressure observer")
	}
	executor, err = NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: restartedPlanner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := executor.Execute(t.Context(), requestKey, tenant, queries); !errors.Is(executeErr, errTypedServicePin) {
		t.Fatalf("restarted execute error=%v", executeErr)
	}
	if observer.calls != 1 {
		t.Fatalf("retry pressure calls=%d want=1", observer.calls)
	}
}

func TestDurableSQLMutationPressureForegroundGate(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	observer := new(durableSQLPressureObserver)
	planner.pressure = observer
	targets, handled, err := planner.planReplicatedSQLTransactionWithData(
		context.Background(), snapshot, []Query{{
			SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringParam("message-hot")},
		}}, planner.profileFor(ClassInteractive), nil,
	)
	if err != nil || !handled || len(targets) != 1 {
		t.Fatalf("participants=%d handled=%v err=%v", len(targets), handled, err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner}
	if allocations := testing.AllocsPerRun(2_000, func() {
		executor.observeMutationPressure(snapshot, targets)
	}); allocations != 0 {
		t.Fatalf("pressure allocations/op=%f want=0", allocations)
	}

	const samples, batch = 256, 256
	latencies := make([]time.Duration, samples)
	for sample := range latencies {
		started := time.Now()
		for range batch {
			executor.observeMutationPressure(snapshot, targets)
		}
		latencies[sample] = time.Since(started) / batch
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	const maximumP99 = 25 * time.Microsecond
	if p99 > maximumP99 {
		t.Fatalf("mutation pressure p99=%s exceeds %s", p99, maximumP99)
	}
	t.Logf("durable mutation pressure p99=%s allocations/op=0", p99)
}

func BenchmarkDurableSQLMutationPressure(b *testing.B) {
	snapshot, planner := replicatedSQLTransactionFixture(b, true)
	planner.pressure = new(durableSQLPressureObserver)
	targets, handled, err := planner.planReplicatedSQLTransactionWithData(
		context.Background(), snapshot, []Query{{
			SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringParam("message-hot")},
		}}, planner.profileFor(ClassInteractive), nil,
	)
	if err != nil || !handled || len(targets) != 1 {
		b.Fatal(err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner}
	b.ReportAllocs()
	for b.Loop() {
		executor.observeMutationPressure(snapshot, targets)
	}
}
