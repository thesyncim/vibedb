package gateway

import (
	"context"
	"errors"
	"strings"
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
	insert, err := s.Prepare(t.Context(), `INSERT INTO messages VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	insertWrite := insert.(*postgresWriteStatement)
	if insertWrite.compiled != nil || insertWrite.paramTypes != nil ||
		insert.(pgwire.BackendStatementParamTyper).ParamType(0) != driver.ParamTypeUnspecified ||
		insert.(pgwire.BackendStatementParamTypePositioner).ParamTypePosition(0) != -1 {
		insert.Close()
		t.Fatal("ordinary insert retained optional typed DML analysis")
	}
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := s.Prepare(t.Context(), `UPDATE messages SET "$doc" = ? WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.(*postgresWriteStatement).compiled != nil || p.(*postgresWriteStatement).paramTypes != nil {
		t.Fatal("ordinary write retained optional typed DML analysis")
	}
	if p.ParamKind(0) != driver.ParamDocument || p.ParamKind(1) != driver.ParamScalar {
		t.Fatal("wrong bind roles")
	}
	typed := p.(pgwire.BackendStatementParamTyper)
	if typed.ParamType(0) != driver.ParamTypeUnspecified ||
		typed.ParamType(1) != driver.ParamTypeUnspecified ||
		typed.ParamType(2) != driver.ParamTypeInvalid ||
		p.(pgwire.BackendStatementParamTypePositioner).ParamTypePosition(0) != -1 {
		t.Fatalf("untyped write metadata = %v, %v, %v",
			typed.ParamType(0), typed.ParamType(1), typed.ParamType(2))
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

func TestPostgreSQLWriteTypedPreparationSuppressesDocumentHints(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	var written Query
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, query Query) (*Result, error) {
			written = query
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
		},
	}
	session, err := backend.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	statement, err := preparer.PrepareWithParameterTypes(
		t.Context(),
		`UPDATE messages SET "$doc" = ? WHERE id IN (SELECT ? UNION ALL SELECT ?)`,
		[]driver.ParamType{
			driver.ParamTypeOther, driver.ParamTypeBool, driver.ParamTypeUnspecified,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	typed := statement.(pgwire.BackendStatementParamTyper)
	if statement.ParamKind(0) != driver.ParamDocument ||
		typed.ParamType(0) != driver.ParamTypeUnspecified ||
		typed.ParamType(1) != driver.ParamTypeBool ||
		typed.ParamType(2) != driver.ParamTypeBool {
		t.Fatalf("write metadata roles=%v/%v/%v types=%v/%v/%v",
			statement.ParamKind(0), statement.ParamKind(1), statement.ParamKind(2),
			typed.ParamType(0), typed.ParamType(1), typed.ParamType(2))
	}
	document := `{"id":"a"}`
	if _, err := statement.Exec(t.Context(), []any{&document, nil, nil}); err != nil {
		t.Fatal(err)
	}
	if len(written.ParamTypes) != 3 ||
		written.ParamTypes[0] != driver.ParamTypeUnspecified ||
		written.ParamTypes[1] != driver.ParamTypeBool ||
		written.ParamTypes[2] != driver.ParamTypeBool {
		t.Fatalf("durable write parameter types = %v", written.ParamTypes)
	}

	_, err = preparer.PrepareWithParameterTypes(
		t.Context(),
		`DELETE FROM messages WHERE id IN (SELECT ? UNION ALL SELECT ?)`,
		[]driver.ParamType{driver.ParamTypeOther, driver.ParamTypeBool},
	)
	if err == nil {
		t.Fatal("scalar Other did not reach typed DML analysis")
	}
}

func TestPostgreSQLWriteInfersSetParameterTypeWithoutClientOID(t *testing.T) {
	executor, _ := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	var written Query
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
		Write: func(_ context.Context, _ serviceauthz.Authority, query Query) (*Result, error) {
			written = query
			return &Result{Kind: shardservice.ResponseCompletion, RowsAffected: 1}, nil
		},
	}
	session, err := backend.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	const source = `DELETE FROM messages WHERE id IN (` +
		`SELECT TEXT 'x' UNION ALL SELECT ?)`
	statement, err := session.Prepare(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	prepared := statement.(*postgresWriteStatement)
	if prepared.compiled == nil ||
		len(prepared.paramTypes) != 1 ||
		prepared.paramTypes[0] != driver.ParamTypeText {
		t.Fatalf("inferred write metadata = compiled %v, types %v",
			prepared.compiled != nil, prepared.paramTypes)
	}
	typed := statement.(pgwire.BackendStatementParamTyper)
	positioned := statement.(pgwire.BackendStatementParamTypePositioner)
	if typed.ParamType(0) != driver.ParamTypeText ||
		positioned.ParamTypePosition(0) != strings.Index(source, "?") {
		t.Fatalf("inferred parameter = %v at %d, want text at %d",
			typed.ParamType(0), positioned.ParamTypePosition(0),
			strings.Index(source, "?"))
	}

	value := "y"
	result, err := statement.Exec(t.Context(), []any{&value})
	if err != nil || result.RowsAffected != 1 {
		t.Fatalf("execute inferred write = %+v, %v", result, err)
	}
	if len(written.ParamTypes) != 1 ||
		written.ParamTypes[0] != driver.ParamTypeText {
		t.Fatalf("durable inferred parameter types = %v", written.ParamTypes)
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
