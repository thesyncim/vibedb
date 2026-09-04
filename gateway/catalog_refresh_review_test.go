package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/shardservice"
)

type reviewCatalogWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *reviewCatalogWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func TestCatalogRefreshReviewSharedHolderRespectsGenerationFloor(t *testing.T) {
	for _, floor := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "same-floor", 2: "newer-floor"}[floor], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			holder := NewCatalogHolder(testSnapshot(t, 1))
			second, third := testSnapshot(t, 2), testSnapshot(t, 3)
			entered, release := make(chan struct{}), make(chan struct{})
			releaseOwner := sync.OnceFunc(func() { close(release) })
			defer releaseOwner()
			var ownerCalls, waiterCalls atomic.Int32
			executor := NewExecutor(nil, holder, Options{Refresh: func(ctx context.Context, stale uint64) (*Snapshot, error) {
				ownerCalls.Add(1)
				if stale != 1 {
					return nil, ErrStaleGeneration
				}
				close(entered)
				select {
				case <-release:
					return second, nil
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
			}})
			reader := &ReplicatedDataReader{catalog: holder, refresh: func(_ context.Context, stale uint64) (*Snapshot, error) {
				waiterCalls.Add(1)
				if stale != 2 {
					return nil, ErrStaleGeneration
				}
				return third, nil
			}}
			owner, waiter := make(chan error, 1), make(chan error, 1)
			go func() { owner <- executor.refreshAfterCatalogMiss(ctx, 1) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("refresh owner did not enter")
			}
			waiterContext := &reviewCatalogWaitContext{Context: ctx, entered: make(chan struct{})}
			go func() { waiter <- reader.refreshAfterFence(waiterContext, floor) }()
			select {
			case <-waiterContext.entered:
			case <-ctx.Done():
				t.Fatal("refresh waiter did not join the shared holder")
			}
			releaseOwner()
			for _, result := range []<-chan error{owner, waiter} {
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("refresh did not complete")
				}
			}
			if ownerCalls.Load() != 1 || waiterCalls.Load() != int32(floor-1) || holder.Current().Generation() != floor+1 {
				t.Fatalf("owner=%d waiter=%d generation=%d floor=%d", ownerCalls.Load(), waiterCalls.Load(), holder.Current().Generation(), floor)
			}
		})
	}
}

func TestCatalogRefreshReviewCanceledWaiterKeepsOwnerAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	holder := NewCatalogHolder(testSnapshot(t, 1))
	second := testSnapshot(t, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseOwner := sync.OnceFunc(func() { close(release) })
	defer releaseOwner()
	var calls atomic.Int32
	refresh := func(ownerContext context.Context, _ uint64) (*Snapshot, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
			return second, ownerContext.Err()
		case <-ownerContext.Done():
			return nil, context.Cause(ownerContext)
		}
	}
	executor := NewExecutor(nil, holder, Options{Refresh: refresh})
	reader := &ReplicatedDataReader{catalog: holder, refresh: refresh}
	owner, waiter := make(chan error, 1), make(chan error, 1)
	go func() { owner <- reader.refreshAfterFence(ctx, 1) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("refresh owner did not enter")
	}
	waitContext, cancelWaiter := context.WithCancel(ctx)
	defer cancelWaiter()
	signaled := &reviewCatalogWaitContext{Context: waitContext, entered: make(chan struct{})}
	go func() { waiter <- executor.refreshAfterCatalogMiss(signaled, 1) }()
	select {
	case <-signaled.entered:
	case <-ctx.Done():
		t.Fatal("waiter did not join")
	}
	cancelWaiter()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("waiter ignored its cancellation")
	}
	select {
	case err := <-owner:
		t.Fatalf("waiter cancellation terminated owner: %v", err)
	default:
	}
	releaseOwner()
	select {
	case err := <-owner:
		if err != nil {
			t.Fatalf("owner error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("owner did not complete")
	}
	if calls.Load() != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("calls=%d generation=%d", calls.Load(), holder.Current().Generation())
	}
}

func TestCatalogRefreshReviewFailureReplaysExactRetainedRequest(t *testing.T) {
	for _, refreshErr := range []error{ErrStaleGeneration, ErrReplicatedUnauthorized} {
		t.Run(refreshErr.Error(), func(t *testing.T) {
			participants := durableFaultParticipants(t)
			base := durableFaultRequest(t, participants)
			queries := []Query{{SQL: `INSERT INTO absent VALUES (?)`, Class: ClassInteractive,
				Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"retained"}`)}}}
			key, err := NewDurableRequestLedgerKey(base.Key.RequestKey, replicatedSQLTransactionRequestDigest(queries))
			if err != nil {
				t.Fatal(err)
			}
			resultRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
				Committed: true, AffectedRows: 2, Transaction: base.Program.Identity.ID,
				CatalogGeneration:       base.Program.Identity.CatalogGeneration,
				ShardsFanned:            uint64(len(base.Program.Participants)),
				TransitionTag:           base.Program.Contract.CommitTransitionTag,
				TerminalStateDigest:     base.Program.Contract.CommitTerminalStateDigest,
				TerminalContractDigest:  base.Program.Contract.TerminalContractDigest,
				RetirementWitnessDigest: base.Program.Contract.RetirementWitnessDigest,
			})
			if err != nil {
				t.Fatal(err)
			}
			ledger := &typedServiceLedger{
				head: requestledger.HeadRecord{Key: key.RequestKey, RequestDigest: requestledger.Digest(key.Digest),
					PlanRoot: requestledger.Digest{2}, Revision: 9, Phase: requestledger.PhaseTerminal},
				terminal: requestledger.TerminalRecord{
					Revision: 9, Result: resultRaw, ResultDigest: requestledger.ResultDigest(resultRaw),
					RequestDigest: requestledger.Digest(key.Digest), PlanRoot: requestledger.Digest{2},
					CatalogGeneration:      base.Program.Identity.CatalogGeneration,
					TerminalContractDigest: requestledger.Digest(base.Program.Contract.TerminalContractDigest),
					AckToken:               requestledger.AckToken{1},
				},
			}
			ledger.terminal.KeyDigest, err = requestledger.KeyDigest(key.RequestKey)
			if err != nil {
				t.Fatal(err)
			}
			pins := new(typedServicePinStop)
			service, err := newDurableRequestService(durableFaultTopology(t, participants), ledger, typedServiceRunnerStop{}, pins)
			if err != nil {
				t.Fatal(err)
			}
			holder := NewCatalogHolder(testSnapshot(t, 1))
			var refreshes int
			planner := NewExecutor(nil, holder, Options{Refresh: func(context.Context, uint64) (*Snapshot, error) {
				refreshes++
				holder.leaseMu.Lock()
				leased := len(holder.activeLeases)
				holder.leaseMu.Unlock()
				if leased != 0 {
					t.Error("missing-table planning lease survived into refresh")
				}
				return nil, refreshErr
			}})
			executor := &DurableSQLRequestExecutor{planner: planner, data: &ReplicatedExecutor{}, requests: service}
			result, err := executor.Execute(t.Context(), key.RequestKey, []byte("tenant-fault"), queries)
			if err != nil || result.Key != key || result.Result == nil || result.Result.RowsAffected != 2 ||
				result.Result.Generation != base.Program.Identity.CatalogGeneration {
				t.Fatalf("exact retained result lost after refresh failure: result=%+v err=%v", result, err)
			}
			if refreshes != 1 || ledger.reads != 2 || ledger.applies != 0 || pins.called != 0 {
				t.Fatalf("refreshes=%d reads=%d applies=%d pins=%d", refreshes, ledger.reads, ledger.applies, pins.called)
			}
			queries[0].Params[0] = shardservice.DocumentParam(`{"id":"changed"}`)
			if result, err = executor.Execute(t.Context(), key.RequestKey, []byte("tenant-fault"), queries); result.Result != nil ||
				!errors.Is(err, ErrDurableRequestConflict) || ledger.applies != 0 || pins.called != 0 {
				t.Fatalf("changed request resumed retained outcome: result=%+v err=%v", result, err)
			}
		})
	}
}

func reviewCatalogMissingMessages(t *testing.T) (scatterCatalogFixture, ReplicatedSQLBatchReadRequest, *Snapshot) {
	t.Helper()
	fixture, request := sameGroupSQLReadFixture(t)
	config := fixture.config
	config.Placements = config.Placements[:1]
	stale, err := NewSnapshotWithReplicatedTableMetadata(config, fixture.endpoints, 4, nil, nil,
		fixture.descriptors, fixture.profiles[:1])
	if err != nil {
		t.Fatal(err)
	}
	return fixture, request, stale
}

func TestCatalogRefreshReviewBatchMissResolvesWholeRequestBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"native-batch", "native-scatter", "sql-batch"} {
		t.Run(mode, func(t *testing.T) {
			fixture, sqlRequest, stale := reviewCatalogMissingMessages(t)
			client := &scatterReadClient{}
			refreshes := 0
			reader := newScatterReader(t, fixture, client, func(context.Context, uint64) (*Snapshot, error) {
				refreshes++
				client.mu.Lock()
				defer client.mu.Unlock()
				if len(client.reads) != 0 {
					t.Error("a partial batch reached data before catalog refresh")
				}
				return fixture.snapshot, nil
			}, 2)
			reader.catalog = NewCatalogHolder(stale)
			for attempt := 0; attempt < 2; attempt++ {
				switch mode {
				case "native-batch":
					result, err := reader.ReadBatch(t.Context(), fixture.request)
					if err != nil || result.Count() != 2 {
						result.Release()
						t.Fatalf("batch count=%d err=%v", result.Count(), err)
					}
					result.Release()
				default:
					var result ReplicatedTableScatterReadResult
					var err error
					if mode == "native-scatter" {
						result, err = reader.ReadScatterBatch(t.Context(), fixture.request)
					} else {
						result, err = reader.ReadSQLBatch(t.Context(), sqlRequest)
					}
					if err != nil || result.Count() != 2 || len(result.Observations) != 1 {
						result.Release()
						t.Fatalf("scatter count=%d observations=%d err=%v", result.Count(), len(result.Observations), err)
					}
					result.Release()
				}
			}
			if refreshes != 1 || reader.catalog.Current().Generation() != 5 {
				t.Fatalf("refreshes=%d generation=%d", refreshes, reader.catalog.Current().Generation())
			}
		})
	}
}

func TestCatalogRefreshReviewPostgresPrepareRefreshesOnlyMissingTables(t *testing.T) {
	for _, text := range []string{`SELECT id FROM messages WHERE id = ?`, `INSERT INTO messages VALUES (?)`} {
		t.Run(text, func(t *testing.T) {
			fixture, _, stale := reviewCatalogMissingMessages(t)
			refreshes := 0
			executor := NewExecutor(nil, NewCatalogHolder(stale), Options{Refresh: func(context.Context, uint64) (*Snapshot, error) {
				refreshes++
				return fixture.snapshot, nil
			}})
			backend := &PostgreSQLBackend{Executor: executor,
				Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
					return serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}, nil
				},
				Write: func(context.Context, serviceauthz.Authority, Query) (*Result, error) {
					t.Fatal("prepare dispatched a mutation")
					return nil, nil
				},
			}
			session, err := backend.NewSession(t.Context(), pgwire.SessionIdentity{})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			known, err := session.Prepare(t.Context(), `SELECT id FROM accounts WHERE id = ?`)
			if err != nil || refreshes != 0 {
				t.Fatalf("known table invoked refresh: refreshes=%d err=%v", refreshes, err)
			}
			known.Close()
			for attempt := 0; attempt < 2; attempt++ {
				statement, err := session.Prepare(t.Context(), text)
				if err != nil {
					t.Fatal(err)
				}
				if read, ok := statement.(*postgresStatement); ok &&
					read.catalogGeneration != fixture.snapshot.Generation() {
					t.Fatalf("read cache generation=%d, want validated generation=%d",
						read.catalogGeneration, fixture.snapshot.Generation())
				}
				statement.Close()
			}
			if refreshes != 1 || executor.catalog.Current().Generation() != 5 {
				t.Fatalf("refreshes=%d generation=%d", refreshes, executor.catalog.Current().Generation())
			}
		})
	}
}

func TestCatalogRefreshReviewPrepareAndExplainBoundColdRefresh(t *testing.T) {
	for _, explain := range []bool{false, true} {
		executor := NewExecutor(nil, NewCatalogHolder(testSnapshot(t, 1)), Options{
			Profiles: map[OperationClass]Profile{ClassInteractive: {GlobalDeadline: 10 * time.Millisecond}},
			Refresh: func(ctx context.Context, _ uint64) (*Snapshot, error) {
				if _, bounded := ctx.Deadline(); !bounded {
					return nil, errors.New("catalog refresh has no operation deadline")
				}
				<-ctx.Done()
				return nil, context.Cause(ctx)
			},
		})
		var err error
		if explain {
			_, err = executor.Explain(context.Background(), Query{SQL: `SELECT id FROM absent`, Class: ClassInteractive})
		} else {
			_, err = executor.validateCatalogPrepare(context.Background(), `SELECT id FROM absent`)
		}
		if !errors.Is(err, ErrTableNotPlaced) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("explain=%t lost deadline or missing-table refusal: %v", explain, err)
		}
	}
}

func TestCatalogRefreshReviewExecMissRefreshesOnceBeforeAdmission(t *testing.T) {
	holder := NewCatalogHolder(testSnapshot(t, 1))
	refreshes := 0
	executor := NewExecutor(nil, holder, Options{
		Profiles: map[OperationClass]Profile{ClassInteractive: {GlobalDeadline: 100 * time.Millisecond}},
		Refresh: func(_ context.Context, stale uint64) (*Snapshot, error) {
			refreshes++
			return testSnapshot(t, stale+1), nil
		},
	})
	result, err := executor.Exec(t.Context(), Query{SQL: `INSERT INTO absent VALUES (?)`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"new"}`)}})
	if result != nil || !errors.Is(err, ErrTableNotPlaced) || refreshes != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("result=%+v refreshes=%d generation=%d err=%v", result, refreshes, holder.Current().Generation(), err)
	}
}
