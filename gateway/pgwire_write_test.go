package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	driver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestPostgreSQLWriteUsesDurableCallbackAndDocumentParameters(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	writes := 0
	b := &PostgreSQLBackend{Executor: executor, Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil }, Write: func(_ context.Context, got serviceauthz.Authority, q Query) (*Result, error) {
		writes++
		if got != authority || len(q.Params) != 2 || q.Params[0].Kind != shardservice.ParamDocument || string(q.Params[1].Bytes) != "a" {
			t.Fatalf("lost write authority or parameters: %+v", q)
		}
		return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
	}}
	s, err := b.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(t.Context(), `UPDATE messages SET "$doc" = ? WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.ParamKind(0) != driver.ParamDocument || p.ParamKind(1) != driver.ParamScalar {
		t.Fatal("wrong bind roles")
	}
	doc, id := `{"id":"a","value":"after"}`, "a"
	result, err := p.Exec(t.Context(), []any{&doc, &id})
	if err != nil || result.RowsAffected != 1 || writes != 1 {
		t.Fatalf("result=%+v err=%v writes=%d", result, err, writes)
	}
	if err = s.Begin(t.Context(), driver.TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(t.Context(), []any{&doc, &id}); err == nil || writes != 1 {
		t.Fatal("explicit transaction mutated")
	}
	s.Rollback(t.Context())
	var flag query.CancelFlag
	flag.Cancel()
	s.SetCancelFlag(&flag)
	if _, err = p.Exec(t.Context(), []any{&doc, &id}); !errors.Is(err, query.ErrCanceled) || writes != 1 {
		t.Fatal("canceled write dispatched")
	}
}

func TestPostgreSQLWritePointerBindings(t *testing.T) {
	str, bytes, boolean, integer, decimal, number := "hello", []byte("bytes"), true, int64(42), 1.5, query.Number("123.4")
	for _, v := range []any{&str, &bytes, &boolean, &integer, &decimal, &number, (*string)(nil), (*int64)(nil)} {
		if p, err := postgresWriteParam(v, false); err != nil || !p.Valid() {
			t.Fatalf("%T: %+v %v", v, p, err)
		}
	}
	if _, err := postgresWriteParam(&str, true); err == nil {
		t.Fatal("invalid document accepted")
	}
}

func TestPostgreSQLWriteUnknownOutcomeDominatesNestedRefusal(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	b := &PostgreSQLBackend{Executor: executor, Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) { return authority, nil }, Write: func(context.Context, serviceauthz.Authority, Query) (*Result, error) {
		return nil, errors.Join(durable.ErrCommitOutcomeUnknown, ErrReplicatedSQLTransactionUnsupported, query.ErrCanceled)
	}}
	s, err := b.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(t.Context(), `DELETE FROM messages WHERE id = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(t.Context(), nil); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown outcome lost: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if !p.(*postgresWriteStatement).closed || p.(*postgresWriteStatement).text != "" {
		t.Fatal("session close retained write statement")
	}
}
