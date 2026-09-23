package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type directPoolService struct {
	postgresWriteServiceStub // OpenIssuer is stateless; other inherited methods are unused.
	prepare                  func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error)
	execute                  func(context.Context, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error)
	executeUnknown           func(context.Context, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan, bool) (durableExecBatchExecuteResult, error)
}

func (s *directPoolService) PrepareDirectBatch(ctx context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
	if s.prepare != nil {
		return s.prepare(ctx, id, q)
	}
	return &gateway.DurableSQLDirectPlan{Key: requestledger.RequestKey{Request: requestledger.RequestID(id.RequestID), IssuerSequence: id.IssuerSequence}}, nil
}
func (s *directPoolService) ExecutePreparedDirectBatch(ctx context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query, plan *gateway.DurableSQLDirectPlan, priorUnknown bool) (durableExecBatchExecuteResult, error) {
	if s.executeUnknown != nil {
		return s.executeUnknown(ctx, id, q, plan, priorUnknown)
	}
	if s.execute != nil {
		return s.execute(ctx, id, q, plan)
	}
	return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
}

var directPoolAuthority = serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}

func testDirectPool(t *testing.T, s *directPoolService) *postgresDirectPool {
	t.Helper()
	p, err := openPostgresDirectPool(filepath.Join(t.TempDir(), "direct"), directPoolAuthority, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func TestPostgreSQLDirectQueryOwnershipDetachesBuffers(t *testing.T) {
	params := []shardservice.Param{
		shardservice.StringBytesParam([]byte("row-a")),
		shardservice.NumberBytesParam([]byte("17")),
	}
	types := []driver.ParamType{driver.ParamTypeText, driver.ParamTypeOther}
	query := gateway.Query{
		SQL: `UPDATE docs SET n = ? WHERE id = ?`, Params: params,
		ParamTypes: types, Class: gateway.ClassInteractive,
	}
	owned, err := ownPostgresWriteQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := vibejson.Marshal(&query)
	if err != nil {
		t.Fatal(err)
	}
	var jsonOwned gateway.Query
	if err := vibejson.Unmarshal(raw, &jsonOwned); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(owned, jsonOwned) {
		t.Fatalf("typed ownership differs from JSON ownership: clone=%+v json=%+v", owned, jsonOwned)
	}
	params[0].Bytes[0] = 'X'
	params[1].Bytes[0] = '9'
	types[0] = driver.ParamTypeBool
	query.Params[0].Bytes[1] = 'X'
	if got := string(owned.Params[0].Bytes); got != "row-a" {
		t.Fatalf("owned string parameter changed to %q", got)
	}
	if got := string(owned.Params[1].Bytes); got != "17" {
		t.Fatalf("owned number parameter changed to %q", got)
	}
	if owned.ParamTypes[0] != driver.ParamTypeText || owned.Class != gateway.ClassInteractive {
		t.Fatalf("owned metadata changed: %+v", owned)
	}
}

func TestPostgreSQLDirectQueryOwnershipPreservesEmptySliceNormalization(t *testing.T) {
	cases := []gateway.Query{
		{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"},
		{SQL: "UPDATE docs SET n=n+1 WHERE id='a'", Params: []shardservice.Param{}},
		{SQL: "UPDATE docs SET n=n+1 WHERE id='a'", ParamTypes: []driver.ParamType{}},
		{SQL: "UPDATE docs SET n=n+1 WHERE id='a'", Params: []shardservice.Param{}, ParamTypes: []driver.ParamType{}},
	}
	for index, query := range cases {
		owned, err := ownPostgresWriteQuery(query)
		if err != nil {
			t.Fatalf("case %d clone: %v", index, err)
		}
		raw, err := vibejson.Marshal(&query)
		if err != nil {
			t.Fatalf("case %d marshal: %v", index, err)
		}
		var jsonOwned gateway.Query
		if err := vibejson.Unmarshal(raw, &jsonOwned); err != nil {
			t.Fatalf("case %d unmarshal: %v", index, err)
		}
		if !reflect.DeepEqual(owned, jsonOwned) {
			t.Fatalf("case %d differs: clone=%+v json=%+v", index, owned, jsonOwned)
		}
	}
}

func TestPostgreSQLDirectQueryOwnershipPreservesJournalSizeLimit(t *testing.T) {
	prepared := 0
	s := &directPoolService{}
	s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		prepared++
		return nil, errors.New("oversized query reached preparation")
	}
	p := testDirectPool(t, s)
	query := gateway.Query{SQL: strings.Repeat("\x00", 400_000)}
	raw, err := vibejson.Marshal(&query)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= maxPostgreSQLWriteJournalBytes/2 || !postgresWriteQueryNeedsSizeCheck(query) {
		t.Fatalf("oversized query preflight: bytes=%d needs-check=%t", len(raw), postgresWriteQueryNeedsSizeCheck(query))
	}
	if _, handled, err := p.Write(t.Context(), query); !handled || !errors.Is(err, gateway.ErrTransactionByteLimit) {
		t.Fatalf("Write oversized query: handled=%t err=%v", handled, err)
	}
	if prepared != 0 {
		t.Fatalf("oversized query reached preparation: %d", prepared)
	}
}

func BenchmarkPostgreSQLDirectQueryOwnership(b *testing.B) {
	query := gateway.Query{
		SQL: `UPDATE docs SET n = ? WHERE id = ?`,
		Params: []shardservice.Param{
			shardservice.NumberParam("17"), shardservice.StringParam("row-a"),
		},
		ParamTypes: []driver.ParamType{driver.ParamTypeOther, driver.ParamTypeText},
		Class:      gateway.ClassInteractive,
	}
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			raw, err := vibejson.Marshal(&query)
			if err != nil {
				b.Fatal(err)
			}
			var owned gateway.Query
			if err := vibejson.Unmarshal(raw, &owned); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("clone", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := ownPostgresWriteQuery(query); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestPostgreSQLDirectReservationsRestartAndWarmWrites(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	before, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(p.path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2*postgresDirectLanes; i++ {
		if _, handled, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); err != nil || !handled {
			t.Fatal(handled, err)
		}
	}
	after, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(p.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !os.SameFile(info, afterInfo) || info.ModTime() != afterInfo.ModTime() {
		t.Fatal("per-request reservation persistence")
	}
	if _, err := openPostgresDirectPool(p.path, directPoolAuthority, s); err == nil {
		t.Fatal("duplicate allocator opened")
	}
	old := p.record
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openPostgresDirectPool(p.path, directPoolAuthority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < postgresDirectLanes; i++ {
		slot := <-reopened.slots
		id, err := reopened.identity(t.Context(), slot)
		if err != nil {
			t.Fatal(err)
		}
		if id.Reference.Installation != old.Lanes[slot.index].Installation || id.IssuerSequence != postgresDirectReservation+1 {
			t.Fatal(id)
		}
	}
}
func TestPostgreSQLDirectReservationRejectsInvalidRecords(t *testing.T) {
	cases := map[string]func(*postgresDirectReservationRecord){
		"version":   func(r *postgresDirectReservationRecord) { r.Version++ },
		"authority": func(r *postgresDirectReservationRecord) { r.Authority.Generation++ },
		"missing":   func(r *postgresDirectReservationRecord) { r.Lanes = r.Lanes[:15] },
		"extra":     func(r *postgresDirectReservationRecord) { r.Lanes = append(r.Lanes, r.Lanes[0]) },
		"duplicate": func(r *postgresDirectReservationRecord) { r.Lanes[1].Installation = r.Lanes[0].Installation },
		"zero":      func(r *postgresDirectReservationRecord) { r.Lanes[0].Installation = replication.ID128{} },
		"overflow":  func(r *postgresDirectReservationRecord) { r.Lanes[0].ReservedThrough = math.MaxUint64 },
		"unaligned": func(r *postgresDirectReservationRecord) { r.Lanes[0].ReservedThrough++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := testDirectPool(t, &directPoolService{})
			mutate(&p.record)
			raw, err := vibejson.Marshal(&p.record)
			if err != nil {
				t.Fatal(err)
			}
			path := p.path
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			writePostgresJournalFixture(t, path, raw)
			if bad, err := openPostgresDirectPool(path, directPoolAuthority, &directPoolService{}); err == nil {
				bad.Close()
				t.Fatal("accepted invalid reservation")
			}
		})
	}
}
func TestPostgreSQLDirectReservationFailurePoisonsAllocation(t *testing.T) {
	p := testDirectPool(t, &directPoolService{})
	slot := <-p.slots
	slot.next = slot.limit + 1
	if err := os.Mkdir(p.path+".pending", 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := p.identity(t.Context(), slot); err == nil {
		t.Fatal("issued an unreserved identity")
	}
	if err := os.Remove(p.path + ".pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.identity(t.Context(), slot); err == nil {
		t.Fatal("allocation resumed after uncertain persistence")
	}
	other := <-p.slots
	if _, err := p.identity(t.Context(), other); err == nil {
		t.Fatal("poison not global")
	}
}
func TestPostgreSQLDirectReservationExtendsBeforeIssue(t *testing.T) {
	p := testDirectPool(t, &directPoolService{})
	slot := <-p.slots
	slot.next = slot.limit + 1
	id, err := p.identity(t.Context(), slot)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	var record postgresDirectReservationRecord
	if err := vibejson.Unmarshal(raw[32:], &record); err != nil {
		t.Fatal(err)
	}
	if id.IssuerSequence != postgresDirectReservation+1 || record.Lanes[slot.index].ReservedThrough != 2*postgresDirectReservation {
		t.Fatal(id, record)
	}
}
func TestPostgreSQLDirectUnknownRetainsExactCommand(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	// Restrict availability to one slot so the next request must recover it.
	for i := 1; i < postgresDirectLanes; i++ {
		<-p.slots
	}
	var first durableExecBatchIdentity
	var original []byte
	prepared, executed := 0, 0
	s.prepare = func(_ context.Context, id durableExecBatchIdentity, q []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		prepared++
		return &gateway.DurableSQLDirectPlan{Key: requestledger.RequestKey{IssuerSequence: id.IssuerSequence}, CatalogGeneration: uint64(prepared)}, nil
	}
	s.execute = func(_ context.Context, id durableExecBatchIdentity, q []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		executed++
		raw, err := vibejson.Marshal(&struct {
			Q []gateway.Query
			P *gateway.DurableSQLDirectPlan
		}{q, plan})
		if err != nil {
			t.Fatal(err)
		}
		if executed == 1 {
			first = id
			original = raw
			return durableExecBatchExecuteResult{}, gateway.ErrReplicatedLeader
		}
		if executed == 2 && (id != first || !bytes.Equal(raw, original) || prepared != 1) {
			t.Fatal("unknown request was replanned or changed")
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='b'"}); err != nil {
		t.Fatal(err)
	}
	if prepared != 2 || executed != 3 {
		t.Fatal(prepared, executed)
	}
}

func TestPostgreSQLDirectPreAdmissionRefusalDoesNotPoisonLane(t *testing.T) {
	for _, priorUnknown := range []bool{false, true} {
		t.Run(fmt.Sprint(priorUnknown), func(t *testing.T) {
			s := &directPoolService{}
			p := testDirectPool(t, s)
			for i := 1; i < postgresDirectLanes; i++ {
				<-p.slots
			}
			var first durableExecBatchIdentity
			var firstPlan *gateway.DurableSQLDirectPlan
			calls := 0
			s.execute = func(_ context.Context, id durableExecBatchIdentity, q []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
				calls++
				if calls == 1 {
					first, firstPlan = id, plan
					if priorUnknown {
						return durableExecBatchExecuteResult{}, errors.New("lost reply")
					}
				}
				if priorUnknown && calls <= 3 && (id != first || plan != firstPlan || q[0].SQL != "UPDATE docs SET n=n+1 WHERE id='a'") {
					t.Fatal("unresolved recipe changed")
				}
				if calls == 1 || priorUnknown && calls == 2 {
					return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted,
						&gateway.ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable})
				}
				return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
			}
			_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
			if errors.Is(err, durable.ErrCommitOutcomeUnknown) != priorUnknown {
				t.Fatalf("first err=%v", err)
			}
			_, _, err = p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='b'"})
			if priorUnknown {
				if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
					t.Fatalf("earlier unknown was discarded: %v", err)
				}
				_, _, err = p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='b'"})
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgreSQLDirectPriorUnknownReachesPreparedExecutor(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	for i := 1; i < postgresDirectLanes; i++ {
		<-p.slots
	}
	type invocation struct {
		identity durableExecBatchIdentity
		query    string
		plan     *gateway.DurableSQLDirectPlan
		unknown  bool
	}
	var calls []invocation
	s.executeUnknown = func(_ context.Context, id durableExecBatchIdentity, queries []gateway.Query, plan *gateway.DurableSQLDirectPlan, priorUnknown bool) (durableExecBatchExecuteResult, error) {
		calls = append(calls, invocation{identity: id, query: queries[0].SQL, plan: plan, unknown: priorUnknown})
		if len(calls) == 1 {
			return durableExecBatchExecuteResult{}, raftservice.ErrOutcomeUnknown
		}
		return durableExecBatchExecuteResult{
			Direct: true, Result: &gateway.Result{RowsAffected: 1},
		}, nil
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("first write error=%v", err)
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='b'"}); err != nil {
		t.Fatalf("write after recovery error=%v", err)
	}
	if len(calls) != 3 || calls[0].unknown || !calls[1].unknown || calls[0].identity != calls[1].identity ||
		calls[0].plan != calls[1].plan || calls[0].query != calls[1].query || calls[2].unknown ||
		calls[2].identity == calls[1].identity || calls[2].query != "UPDATE docs SET n=n+1 WHERE id='b'" {
		t.Fatalf("prepared recovery calls=%+v", calls)
	}
}

func TestPostgreSQLDirectAbortRetriesWithNewIdentity(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	var ids []durableExecBatchIdentity
	s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		ids = append(ids, id)
		if len(ids) < 3 {
			return durableExecBatchExecuteResult{}, &gateway.DurableSQLAbortError{
				ResultCode: replicatedstate.ResultTransactionConflict,
			}
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i].IssuerSequence != ids[i-1].IssuerSequence+1 || ids[i].RequestID == ids[i-1].RequestID || ids[i].Reference != ids[0].Reference {
			t.Fatal(ids)
		}
	}
}

func TestPostgreSQLDirectRetryBudgetsAndTerminalErrorStayTyped(t *testing.T) {
	t.Run("independent abort budget", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		var ids []durableExecBatchIdentity
		s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
			ids = append(ids, id)
			if len(ids) == 1 {
				return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
			}
			return durableExecBatchExecuteResult{}, &gateway.DurableSQLAbortError{
				ResultCode: replicatedstate.ResultTransactionConflict,
			}
		}
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		if !errors.Is(err, gateway.ErrDurableSQLAborted) || errors.Is(err, gateway.ErrDurableSQLNotAdmitted) {
			t.Fatalf("terminal aborted write error=%v", err)
		}
		if len(ids) != 9 {
			t.Fatalf("execute calls=%d, want one admission refusal and eight durable aborts", len(ids))
		}
		for i := 1; i < len(ids); i++ {
			if ids[i].IssuerSequence != ids[i-1].IssuerSequence+1 || ids[i].RequestID == ids[i-1].RequestID || ids[i].Reference != ids[0].Reference {
				t.Fatalf("retry did not use the next identity: %+v", ids)
			}
		}
	})

	t.Run("admission exhaustion", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		const window = 300 * time.Millisecond
		p.admissionWindow = window
		calls := 0
		s.execute = func(context.Context, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
			calls++
			return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
		}
		started := time.Now()
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		elapsed := time.Since(started)
		if !errors.Is(err, gateway.ErrDurableSQLNotAdmitted) || errors.Is(err, gateway.ErrDurableSQLAborted) {
			t.Fatalf("terminal admission refusal error=%v", err)
		}
		// Refusals persist until the catalog converges, so they are retried for
		// the whole window with capped backoff, then surface typed.
		if elapsed < window || elapsed > window+time.Second || calls < 3 {
			t.Fatalf("admission retry elapsed=%s calls=%d, want the %s convergence window", elapsed, calls, window)
		}
	})

	t.Run("permanent row conflict executes once", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		var ids []durableExecBatchIdentity
		s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
			ids = append(ids, id)
			return durableExecBatchExecuteResult{}, &gateway.DurableSQLAbortError{
				ResultCode: replicatedstate.ResultIndexConflict,
			}
		}
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "INSERT INTO docs VALUES ('stable-id')"})
		code, typed := gateway.DurableSQLAbortResultCode(err)
		if !errors.Is(err, gateway.ErrDurableSQLAborted) || !typed ||
			code != replicatedstate.ResultIndexConflict || len(ids) != 1 {
			t.Fatalf("permanent conflict code=%d typed=%t calls=%d err=%v", code, typed, len(ids), err)
		}
	})

	t.Run("intent busy retries with fresh identity", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		var ids []durableExecBatchIdentity
		s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
			ids = append(ids, id)
			if len(ids) == 1 {
				return durableExecBatchExecuteResult{}, &gateway.DurableSQLAbortError{
					ResultCode: replicatedstate.ResultIntentBusy,
				}
			}
			return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
		}
		if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "INSERT INTO docs VALUES ('stable-id')"}); err != nil {
			t.Fatal(err)
		}
		if len(ids) != 2 || ids[0].IssuerSequence+1 != ids[1].IssuerSequence ||
			ids[0].RequestID == ids[1].RequestID || ids[0].Reference != ids[1].Reference {
			t.Fatalf("intent retry identities=%+v", ids)
		}
	})
}

func TestPostgreSQLDirectStalePlanRetriesAfterDefiniteRefusal(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	var first durableExecBatchIdentity
	var firstPlan *gateway.DurableSQLDirectPlan
	calls := 0
	s.execute = func(_ context.Context, id durableExecBatchIdentity, q []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		calls++
		if calls == 1 {
			first, firstPlan = id, plan
			return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
		}
		if calls != 2 || id.RequestID == first.RequestID || id.IssuerSequence != first.IssuerSequence+1 || plan == firstPlan ||
			q[0].SQL != "UPDATE docs SET n=n+1 WHERE id='a'" {
			t.Fatal("definite stale plan did not acquire a fresh recipe for the same statement")
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
func TestPostgreSQLDirectSameTableOverlapsAndCloseCancels(t *testing.T) {
	entered := make(chan struct{})
	s := &directPoolService{}
	s.execute = func(ctx context.Context, _ durableExecBatchIdentity, q []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		if q[0].SQL == "UPDATE documents SET n=n+1 WHERE id='a'" {
			close(entered)
			<-ctx.Done()
			return durableExecBatchExecuteResult{}, ctx.Err()
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	w, err := openPostgresTableWriters(filepath.Join(t.TempDir(), "outbox"), directPoolAuthority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := w.Write(ctx, directPoolAuthority, gateway.Query{SQL: "UPDATE documents SET n=n+1 WHERE id='a'"})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := w.Write(ctx, directPoolAuthority, gateway.Query{SQL: "UPDATE documents SET n=n+1 WHERE id='b'"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
}
func TestPostgreSQLTableGateExclusiveAndCancellation(t *testing.T) {
	var g postgresTableGate
	release, err := g.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	waiting := make(chan error, 1)
	go func() {
		unlock, err := g.acquire(ctx, true)
		if unlock != nil {
			unlock()
		}
		waiting <- err
	}()
	// Wait for registration, not a scheduling delay.
	deadline := time.Now().Add(time.Second)
	for {
		g.mu.Lock()
		n := g.waiting
		g.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not register")
		}
		time.Sleep(time.Millisecond)
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := g.acquire(cancelled, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cancel()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release2, err := g.acquire(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	release2()
	release()
	exclusive, err := g.acquire(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	exclusive()
}
func TestPostgreSQLDirectParallelIdentityUniqueness(t *testing.T) {
	var mu sync.Mutex
	identities := map[durableExecBatchIdentity]bool{}
	sequences := map[gateway.ReplicatedIssuerReference]map[uint64]bool{}
	s := &directPoolService{}
	s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if identities[id] {
			t.Error("identity reused")
		}
		identities[id] = true
		if sequences[id.Reference] == nil {
			sequences[id.Reference] = map[uint64]bool{}
		}
		if sequences[id.Reference][id.IssuerSequence] {
			t.Error("sequence reused")
		}
		sequences[id.Reference][id.IssuerSequence] = true
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	p := testDirectPool(t, s)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(identities) != 64 {
		t.Fatal(len(identities))
	}
}
func TestPostgreSQLDirectReservationSurvivesProcessExit(t *testing.T) {
	// Exit without Close after issuing an identity, then check OS lock release
	// and the durable reservation fence in a fresh allocator.
	if path := os.Getenv("VIBEDB_TEST_DIRECT_RESERVATION_CHILD"); path != "" {
		p, err := openPostgresDirectPool(path, directPoolAuthority, &directPoolService{})
		if err != nil {
			t.Fatal(err)
		}
		slot := <-p.slots
		if _, err := p.identity(t.Context(), slot); err != nil {
			t.Fatal(err)
		}
		os.Exit(23)
	}
	path := filepath.Join(t.TempDir(), "direct")
	cmd := exec.Command(os.Args[0], "-test.run=^TestPostgreSQLDirectReservationSurvivesProcessExit$")
	cmd.Env = append(os.Environ(), "VIBEDB_TEST_DIRECT_RESERVATION_CHILD="+path)
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var old postgresDirectReservationRecord
	if err := vibejson.Unmarshal(raw[32:], &old); err != nil {
		t.Fatal(err)
	}
	p, err := openPostgresDirectPool(path, directPoolAuthority, &directPoolService{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := range old.Lanes {
		if !reflect.DeepEqual(old.Lanes[i].Installation, p.record.Lanes[i].Installation) || p.record.Lanes[i].ReservedThrough != 2*postgresDirectReservation {
			t.Fatal(p.record)
		}
	}
}

type legacyAndDirectService struct {
	postgresTableServiceStub
	prepared int
}

func (s *legacyAndDirectService) PrepareDirectBatch(ctx context.Context, authority serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
	s.mu.Lock()
	s.prepared++
	s.mu.Unlock()
	return (&directPoolService{}).PrepareDirectBatch(ctx, authority, id, q)
}
func (s *legacyAndDirectService) ExecutePreparedDirectBatch(ctx context.Context, authority serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query, p *gateway.DurableSQLDirectPlan, priorUnknown bool) (durableExecBatchExecuteResult, error) {
	return (&directPoolService{}).ExecutePreparedDirectBatch(ctx, authority, id, q, p, priorUnknown)
}
func TestPostgreSQLDirectWaitsForLegacyPendingTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox")
	s := &legacyAndDirectService{}
	s.blocked = "documents"
	legacy, err := openPostgresDurableWriter(path, directPoolAuthority, &s.postgresTableServiceStub, "documents")
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Write(t.Context(), directPoolAuthority, gateway.Query{SQL: "INSERT INTO documents (id) VALUES ('old')"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
	identity := legacy.record.Identity
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := openPostgresTableWriters(path, directPoolAuthority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Write(t.Context(), directPoolAuthority, gateway.Query{SQL: "UPDATE documents SET n=n+1 WHERE id='new'"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
	if _, err := w.Write(t.Context(), directPoolAuthority, gateway.Query{SQL: "UPDATE employees SET n=n+1 WHERE id='new'"}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prepared != 1 || s.writesByTable["documents"] != 1 {
		t.Fatal("new work overtook retained outbox", s.prepared, s.writesByTable)
	}
	if _, ok := s.requests[identity]; !ok {
		t.Fatal("lost legacy identity")
	}
}

func TestPostgreSQLDirectPreparationBackpressureDoesNotProposeOrRenumber(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	attempts, executions := 0, 0
	var original durableExecBatchIdentity
	s.prepare = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		attempts++
		if attempts == 1 {
			original = id
		}
		if id != original || executions != 0 {
			t.Fatal("preparation admitted or renumbered a write")
		}
		if attempts < 3 {
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, gateway.ErrReplicatedLeader, &gateway.ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalAdmissionBound})
		}
		return &gateway.DurableSQLDirectPlan{CatalogGeneration: 1}, nil
	}
	s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		executions++
		if id != original {
			t.Fatal("prepared identity changed")
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || executions != 1 {
		t.Fatal(attempts, executions)
	}
}

func TestPostgreSQLDirectPreparationRetriesServingFenceWithStableIdentity(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	var original durableExecBatchIdentity
	attempts, executions := 0, 0
	s.prepare = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		attempts++
		if attempts == 1 {
			original = id
		} else if id != original {
			t.Fatalf("prepare identity changed across stale fence: first=%+v got=%+v", original, id)
		}
		if attempts <= 9 {
			return nil, errors.Join(
				gateway.ErrDurableSQLNotAdmitted,
				gateway.ErrReplicatedRoute,
				raftservice.ErrServingFence,
			)
		}
		return &gateway.DurableSQLDirectPlan{CatalogGeneration: 2}, nil
	}
	s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		executions++
		if id != original || plan.CatalogGeneration != 2 || attempts != 10 {
			t.Fatalf("executed before current route prepared: id=%+v plan=%+v attempts=%d", id, plan, attempts)
		}
		return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
	}
	if _, _, err := p.Write(t.Context(), gateway.Query{SQL: "INSERT INTO docs(id) VALUES ('a')"}); err != nil {
		t.Fatal(err)
	}
	if attempts != 10 || executions != 1 {
		t.Fatalf("prepare attempts=%d execute calls=%d, want 10 and 1", attempts, executions)
	}
}

func TestPostgreSQLDirectPreparationServingFenceRetryIsTypedAndBounded(t *testing.T) {
	t.Run("generic route does not enter long retry", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, gateway.ErrReplicatedRoute, gateway.ErrReplicatedLeader)
		}
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		if !errors.Is(err, gateway.ErrReplicatedRoute) || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
	})

	t.Run("unauthorized stale fence does not retry", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence, gateway.ErrReplicatedUnauthorized)
		}
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		if !errors.Is(err, gateway.ErrReplicatedUnauthorized) || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
	})

	t.Run("unknown outcome does not enter long retry", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence, gateway.ErrReplicatedRoute, durable.ErrCommitOutcomeUnknown)
		}
		_, _, err := p.Write(t.Context(), gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
	})

	t.Run("canceled context stops after current prepare", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			cancel()
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
		}
		_, _, err := p.Write(ctx, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
		if !errors.Is(err, context.Canceled) || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
	})

	t.Run("short injected budget bounds retries", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
		}
		started := time.Now()
		_, err := p.prepareWithServingFenceWindow(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}}, 225*time.Millisecond)
		elapsed := time.Since(started)
		if !errors.Is(err, raftservice.ErrServingFence) || attempts < 2 || attempts > 3 || elapsed < 220*time.Millisecond || elapsed > 450*time.Millisecond {
			t.Fatalf("attempts=%d elapsed=%s err=%v", attempts, elapsed, err)
		}
	})

	t.Run("deadline error stops after current prepare", func(t *testing.T) {
		s := &directPoolService{}
		p := testDirectPool(t, s)
		attempts := 0
		s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
			attempts++
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence, context.DeadlineExceeded)
		}
		_, err := p.prepareWithServingFenceWindow(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"}}, time.Second)
		if !errors.Is(err, context.DeadlineExceeded) || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
	})
}

func TestPostgreSQLDirectPreparationCancellationAndRetryBound(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			s := &directPoolService{}
			p := testDirectPool(t, s)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			attempts := 0
			s.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
				attempts++
				if cancelled {
					cancel()
				}
				return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, gateway.ErrReplicatedLeader)
			}
			s.execute = func(context.Context, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
				t.Fatal("proposed failed preparation")
				return durableExecBatchExecuteResult{}, nil
			}
			_, _, err := p.Write(ctx, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
			if err == nil || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				t.Fatal(err)
			}
			if cancelled && attempts != 1 || !cancelled && attempts != 8 {
				t.Fatal(attempts)
			}
		})
	}
}

// A write refused before admission may still have been admitted by a racing
// path. While the logical route is unchanged the pool must re-drive the same
// request identity through recovery, never mint a new one: a new identity
// would turn an admitted attempt into a duplicate insert.
func TestPostgreSQLDirectAdmissionRetryKeepsIdentityOnSameRoute(t *testing.T) {
	route := catalogRouteSeedRoute(t, catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101"))
	advanced := route
	advanced.Command.OwnershipEpoch++
	advanced.Command.RoutingVersion++
	advanced.Command.RouteGeneration++
	split := advanced
	split.Group.GroupID[0] ^= 0xff
	for _, test := range []struct {
		name         string
		next         gateway.ReplicatedRoute
		sameIdentity bool
	}{
		{name: "ownership advance keeps identity", next: advanced, sameIdentity: true},
		{name: "group change mints identity", next: split, sameIdentity: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &directPoolService{}
			prepares := 0
			s.prepare = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
				prepares++
				plan := &gateway.DurableSQLDirectPlan{Key: requestledger.RequestKey{
					Request: requestledger.RequestID(id.RequestID), IssuerSequence: id.IssuerSequence}}
				plan.Target.Route = route
				if prepares > 1 {
					plan.Target.Route = test.next
				}
				return plan, nil
			}
			type call struct {
				id           durableExecBatchIdentity
				priorUnknown bool
			}
			var calls []call
			s.executeUnknown = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan, priorUnknown bool) (durableExecBatchExecuteResult, error) {
				calls = append(calls, call{id: id, priorUnknown: priorUnknown})
				if len(calls) == 1 {
					return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted, raftservice.ErrServingFence)
				}
				return durableExecBatchExecuteResult{Direct: true, Result: &gateway.Result{RowsAffected: 1}}, nil
			}
			p := testDirectPool(t, s)
			p.admissionWindow = time.Second
			result, _, err := p.Write(t.Context(), gateway.Query{SQL: "INSERT INTO docs VALUES ('a')"})
			if err != nil || result == nil || result.RowsAffected != 1 || len(calls) != 2 {
				t.Fatalf("result=%+v calls=%+v err=%v", result, calls, err)
			}
			same := calls[1].id.RequestID == calls[0].id.RequestID &&
				calls[1].id.IssuerSequence == calls[0].id.IssuerSequence
			if same != test.sameIdentity || calls[1].priorUnknown != test.sameIdentity {
				t.Fatalf("retry identity same=%t priorUnknown=%t, want %t", same, calls[1].priorUnknown, test.sameIdentity)
			}
		})
	}
}
