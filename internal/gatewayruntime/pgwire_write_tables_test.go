package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/store/durable"
)

type postgresTableServiceStub struct {
	postgresWriteServiceStub
	mu            sync.Mutex
	blocked       string
	requests      map[durableExecBatchIdentity][]gateway.Query
	writesByTable map[string]int
}

type postgresBlockingTableService struct {
	postgresTableServiceStub
	entered, release chan struct{}
}

func TestPostgreSQLPendingWriteDoesNotClaimNewStatementExecuted(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresTableServiceStub{blocked: "employees"}
	w, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "writes"), authority, s, "employees")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('1')"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
	id := w.record.Identity
	_, err = w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('2')"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, gateway.ErrDurableRequestUnresolved) ||
		!strings.Contains(err.Error(), "this statement was not executed") ||
		!strings.Contains(err.Error(), fmt.Sprintf("%x", id.RequestID)) || strings.Contains(err.Error(), "reopen required") ||
		s.writesByTable["employees"] != 1 || w.record.Identity != id {
		t.Fatalf("pending request misclassified or replaced: %v", err)
	}
}

func (s *postgresBlockingTableService) ExecBatch(ctx context.Context, authority serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, error) {
	table, _ := postgresWriteTable(q[0])
	if table == "documents" {
		close(s.entered)
		select {
		case <-ctx.Done():
			return durableExecBatchExecuteResult{}, ctx.Err()
		case <-s.release:
		}
	}
	return s.postgresTableServiceStub.ExecBatch(ctx, authority, id, q)
}

func TestPostgreSQLTableWritesProgressWhileOtherTableWaits(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresBlockingTableService{entered: make(chan struct{}), release: make(chan struct{})}
	w, err := openPostgresTableWriters(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer close(s.release)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := w.Write(ctx, authority, gateway.Query{SQL: "INSERT INTO documents (id) VALUES ('1')"})
		done <- err
	}()
	select {
	case <-s.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := w.Write(ctx, authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('1')"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("blocked table unexpectedly finished: %v", err)
	default:
	}
}

func TestPostgreSQLTableWritersAreBounded(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresTableServiceStub{}
	w, err := openPostgresTableWriters(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 1; i < maxPostgresTableWriters; i++ {
		if _, err := w.Write(t.Context(), authority, gateway.Query{SQL: fmt.Sprintf("INSERT INTO t%d (id) VALUES ('1')", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO overflow (id) VALUES ('1')"}); !errors.Is(err, gateway.ErrTransactionByteLimit) {
		t.Fatalf("unbounded lanes: %v", err)
	}
	if len(w.writers) != maxPostgresTableWriters {
		t.Fatal(len(w.writers))
	}
}

func (s *postgresTableServiceStub) ExecBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	table, err := postgresWriteTable(q[0])
	if err != nil {
		return durableExecBatchExecuteResult{}, err
	}
	if s.requests == nil {
		s.requests = make(map[durableExecBatchIdentity][]gateway.Query)
		s.writesByTable = make(map[string]int)
	}
	s.requests[id] = q
	s.writesByTable[table]++
	if table == s.blocked {
		return durableExecBatchExecuteResult{}, gateway.ErrDurableRequestUnresolved
	}
	s.identity = id
	return s.result(), nil
}

func (s *postgresTableServiceStub) ReplayBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.requests[id], q) {
		return durableExecBatchExecuteResult{}, false, errors.New("recovery changed request")
	}
	table, err := postgresWriteTable(q[0])
	if err != nil {
		return durableExecBatchExecuteResult{}, false, err
	}
	if table == s.blocked {
		return durableExecBatchExecuteResult{}, true, gateway.ErrDurableRequestUnresolved
	}
	s.identity = id
	return s.result(), true, nil
}

func (s *postgresTableServiceStub) AckExecBatch(_ context.Context, _ serviceauthz.Authority, ack durableExecBatchAckWireRequest) (durableExecBatchAckWireResponse, error) {
	return durableExecBatchAckWireResponse{durableExecBatchAckWireRequest: ack}, nil
}

func TestPostgreSQLTableWritesIsolateAndRecoverLegacyUnknown(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresTableServiceStub{blocked: "documents"}
	old, err := openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	q := gateway.Query{SQL: `UPDATE documents SET "$doc"='{"id":"pending"}' WHERE id='pending'`}
	if _, err = old.Write(t.Context(), authority, q); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatal(err)
	}
	identity := old.record.Identity
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := openPostgresTableWriters(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	for cycle := 0; cycle < 2; cycle++ {
		if _, err = w.Write(t.Context(), authority, gateway.Query{SQL: `INSERT INTO employees (id,name) VALUES ('1','Alex')`}); err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(t.Context(), authority, gateway.Query{SQL: `DELETE FROM documents WHERE id='new'`}); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("overtook unknown: %v", err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		w, err = openPostgresTableWriters(path, authority, s)
		if err != nil {
			t.Fatal(err)
		}
		lane := w.writers["documents"]
		<-lane.gate
		retained := lane.record.Identity == identity && reflect.DeepEqual(*lane.record.Query, q)
		lane.gate <- struct{}{}
		if !retained {
			t.Fatal("legacy request changed")
		}
	}
	s.mu.Lock()
	s.blocked = ""
	s.mu.Unlock()
	if _, err = w.Write(t.Context(), authority, gateway.Query{SQL: `DELETE FROM documents WHERE id='new'`}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writesByTable["documents"] != 2 || s.writesByTable["employees"] != 2 {
		t.Fatalf("unexpected executions: %v", s.writesByTable)
	}
}

func TestPostgreSQLTableJournalRejectsWrongBinding(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresTableServiceStub{}
	w, err := openPostgresTableWriters(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write(t.Context(), authority, gateway.Query{SQL: `INSERT INTO employees (id) VALUES ('1')`}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(filepath.Join(path+".tables", postgresTableJournalName("employees")), filepath.Join(path+".tables", postgresTableJournalName("other"))); err != nil {
		t.Fatal(err)
	}
	if bad, err := openPostgresTableWriters(path, authority, s); err == nil {
		_ = bad.Close()
		t.Fatal("accepted misbound journal")
	}
}
