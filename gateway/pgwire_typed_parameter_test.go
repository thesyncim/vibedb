package gateway

import (
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var (
	_ pgwire.BackendSessionParameterPreparer     = (*postgresSession)(nil)
	_ pgwire.BackendStatementParamTyper          = (*postgresStatement)(nil)
	_ pgwire.BackendStatementParamTypePositioner = (*postgresStatement)(nil)
	_ pgwire.BackendStatementParamTyper          = (*postgresWriteStatement)(nil)
	_ pgwire.BackendStatementParamTypePositioner = (*postgresWriteStatement)(nil)
)

func newTypedPostgreSQLSession(t *testing.T) (pgwire.BackendSession, *sqlRF3TestTransport) {
	t.Helper()
	executor, transport := newSQLRF3TestExecutor(t)
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	backend := &PostgreSQLBackend{
		Executor: executor,
		Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
			return authority, nil
		},
	}
	session, err := backend.NewSession(t.Context(), pgwire.SessionIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, transport
}

func TestPostgreSQLBackendTypedPrepareMetadata(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	tests := []struct {
		name  string
		sql   string
		hints []sqldriver.ParamType
		want  []sqldriver.ParamType
	}{
		{
			name: "inferred_bool",
			sql:  "SELECT CASE BOOL 't' WHEN ? THEN TEXT 'yes' ELSE TEXT 'no' END",
			want: []sqldriver.ParamType{sqldriver.ParamTypeBool},
		},
		{
			name: "inferred_text",
			sql:  "SELECT CASE TEXT 'x' WHEN ? THEN TEXT 'yes' ELSE TEXT 'no' END",
			want: []sqldriver.ParamType{sqldriver.ParamTypeText},
		},
		{
			name: "declared_bool_drives_set_common_type",
			sql: "SELECT CASE BOOL 't' WHEN ? THEN BOOL 't' ELSE BOOL 'f' END " +
				"UNION ALL SELECT ?",
			hints: []sqldriver.ParamType{
				sqldriver.ParamTypeBool, sqldriver.ParamTypeUnspecified,
			},
			want: []sqldriver.ParamType{
				sqldriver.ParamTypeBool, sqldriver.ParamTypeBool,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := preparer.PrepareWithParameterTypes(
				t.Context(), test.sql, test.hints,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Close()
			typed := statement.(pgwire.BackendStatementParamTyper)
			got := make([]sqldriver.ParamType, statement.NumParams())
			for index := range got {
				got[index] = typed.ParamType(index)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("parameter types = %v, want %v", got, test.want)
			}
			positioner := statement.(pgwire.BackendStatementParamTypePositioner)
			if position := positioner.ParamTypePosition(0); position < 0 ||
				position >= len(test.sql) || test.sql[position] != '?' {
				t.Fatalf("parameter type position = %d for %q", position, test.sql)
			}
		})
	}
}

func TestPostgreSQLBackendCarriesTypedNULLThroughRF3(t *testing.T) {
	session, transport := newTypedPostgreSQLSession(t)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	statement, err := preparer.PrepareWithParameterTypes(
		t.Context(), "SELECT id FROM messages WHERE CASE BOOL 't' WHEN ? THEN 1 ELSE 0 END = 1",
		[]sqldriver.ParamType{sqldriver.ParamTypeBool},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	var rows pgwire.BackendRows
	defer rows.Close()
	if err := statement.QueryInto(t.Context(), []any{nil}, &rows); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.lastParams) != 1 || transport.lastParams[0].Kind != shardservice.ParamNull ||
		!slices.Equal(transport.lastTypes, []sqldriver.ParamType{sqldriver.ParamTypeBool}) {
		t.Fatalf("RF3 inputs = params %+v types %v", transport.lastParams, transport.lastTypes)
	}
}

func TestPostgresParameterTypeConversionsPreserveExactStringDomains(t *testing.T) {
	tests := []struct {
		query  query.ParameterType
		driver sqldriver.ParamType
	}{
		{query.ParameterTypeBool, sqldriver.ParamTypeBool},
		{query.ParameterTypeText, sqldriver.ParamTypeText},
		{query.ParameterTypeVarchar, sqldriver.ParamTypeVarchar},
		{query.ParameterTypeName, sqldriver.ParamTypeName},
		{query.ParameterTypeBPChar, sqldriver.ParamTypeBPChar},
		{query.ParameterTypeOther, sqldriver.ParamTypeOther},
	}
	for _, test := range tests {
		if got := postgresParameterType(test.query); got != test.driver {
			t.Errorf("postgresParameterType(%v) = %v, want %v", test.query, got, test.driver)
		}
		resolved, err := postgresQueryParameterTypes([]sqldriver.ParamType{test.driver}, 1)
		if err != nil || len(resolved) != 1 || resolved[0] != test.query {
			t.Errorf("postgresQueryParameterTypes(%v) = %v, %v", test.driver, resolved, err)
		}
	}
	if _, err := postgresQueryParameterTypes(
		[]sqldriver.ParamType{sqldriver.ParamTypeInvalid}, 1,
	); err == nil || !strings.Contains(err.Error(), "invalid parameter type hint") {
		t.Fatalf("invalid type = %v", err)
	}
}
