package main

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
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type directPoolService struct {
	postgresWriteServiceStub // OpenIssuer is stateless; other inherited methods are unused.
	prepare                  func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error)
	execute                  func(context.Context, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error)
}

func (s *directPoolService) PrepareDirectBatch(ctx context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
	if s.prepare != nil {
		return s.prepare(ctx, id, q)
	}
	return &gateway.DurableSQLDirectPlan{Key: requestledger.RequestKey{Request: requestledger.RequestID(id.RequestID), IssuerSequence: id.IssuerSequence}}, nil
}
func (s *directPoolService) ExecutePreparedDirectBatch(ctx context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
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
func TestPostgreSQLDirectAbortRetriesWithNewIdentity(t *testing.T) {
	s := &directPoolService{}
	p := testDirectPool(t, s)
	var ids []durableExecBatchIdentity
	s.execute = func(_ context.Context, id durableExecBatchIdentity, _ []gateway.Query, _ *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
		ids = append(ids, id)
		if len(ids) < 3 {
			return durableExecBatchExecuteResult{}, gateway.ErrDurableSQLAborted
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
func (s *legacyAndDirectService) ExecutePreparedDirectBatch(ctx context.Context, authority serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query, p *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
	return (&directPoolService{}).ExecutePreparedDirectBatch(ctx, authority, id, q, p)
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
