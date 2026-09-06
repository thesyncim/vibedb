package gateway

import (
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var (
	_ pgwire.BackendSessionParameterPreparer     = (*postgresSession)(nil)
	_ pgwire.BackendStatementParseReuser         = (*postgresStatement)(nil)
	_ pgwire.BackendStatementParamTyper          = (*postgresStatement)(nil)
	_ pgwire.BackendStatementParamTypePositioner = (*postgresStatement)(nil)
	_ pgwire.BackendStatementParamTyper          = (*postgresWriteStatement)(nil)
	_ pgwire.BackendStatementParamTypePositioner = (*postgresWriteStatement)(nil)
)

func TestPostgreSQLStatementParseReuseFollowsCatalogGeneration(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	statement, err := session.Prepare(
		t.Context(), "SELECT id FROM messages WHERE id = ? ORDER BY id LIMIT 32",
	)
	if err != nil {
		t.Fatal(err)
	}
	p := statement.(*postgresStatement)
	if !p.ReusableForParse() {
		t.Fatal("current distributed statement declined exact Parse reuse")
	}
	p.session.state = sqldriver.SessionFailedTransaction
	if p.ReusableForParse() {
		t.Fatal("statement in a failed transaction allowed Parse reuse")
	}
	p.session.state = sqldriver.SessionIdle
	p.catalogGeneration++
	if p.ReusableForParse() {
		t.Fatal("statement from a stale catalog generation allowed Parse reuse")
	}
}

func newTypedPostgreSQLSession(t testing.TB) (pgwire.BackendSession, *sqlRF3TestTransport) {
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

func BenchmarkPostgreSQLBackendDistributedPrepare(b *testing.B) {
	session, _ := newTypedPostgreSQLSession(b)
	s := session.(*postgresSession)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	const sql = "SELECT id FROM messages WHERE id = ? ORDER BY id LIMIT 256"
	hints := []sqldriver.ParamType{sqldriver.ParamTypeText}

	b.Run("cold", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.releaseReadCache()
			statement, err := preparer.PrepareWithParameterTypes(b.Context(), sql, hints)
			if err != nil {
				b.Fatal(err)
			}
			if err := statement.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cached", func(b *testing.B) {
		statement, err := preparer.PrepareWithParameterTypes(b.Context(), sql, hints)
		if err != nil {
			b.Fatal(err)
		}
		if err := statement.Close(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			statement, err := preparer.PrepareWithParameterTypes(b.Context(), sql, hints)
			if err != nil {
				b.Fatal(err)
			}
			if err := statement.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
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

func TestPostgreSQLBackendReusesClosedDistributedReadPrepare(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	s := session.(*postgresSession)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	const sql = "SELECT id FROM messages WHERE id = ? ORDER BY id LIMIT 32"
	hints := []sqldriver.ParamType{sqldriver.ParamTypeText}

	first, err := preparer.PrepareWithParameterTypes(t.Context(), sql, hints)
	if err != nil {
		t.Fatal(err)
	}
	firstCompiled := first.(*postgresStatement).compiled
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if s.readCache.compiled != firstCompiled {
		t.Fatal("closed distributed read was not retained")
	}

	second, err := preparer.PrepareWithParameterTypes(t.Context(), sql, hints)
	if err != nil {
		t.Fatal(err)
	}
	if second.(*postgresStatement).compiled != firstCompiled {
		t.Fatal("exact SQL and parameter types did not reuse compiled statement")
	}
	if s.readCache.compiled != nil {
		t.Fatal("cache retained a second owner after reuse")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLBackendReadPrepareCacheFollowsCatalogGeneration(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	s := session.(*postgresSession)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	const sql = "SELECT id FROM messages WHERE id = ? ORDER BY id LIMIT 64"
	hints := []sqldriver.ParamType{sqldriver.ParamTypeText}

	first, err := preparer.PrepareWithParameterTypes(t.Context(), sql, hints)
	if err != nil {
		t.Fatal(err)
	}
	firstCompiled := first.(*postgresStatement).compiled
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	s.readCache.catalogGeneration++

	second, err := preparer.PrepareWithParameterTypes(t.Context(), sql, hints)
	if err != nil {
		t.Fatal(err)
	}
	if second.(*postgresStatement).compiled == firstCompiled {
		t.Fatal("stale catalog generation reused compiled statement")
	}
	if s.readCache.compiled != firstCompiled {
		t.Fatal("generation miss mutated the prior cache entry before replacement")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLBackendReadPrepareCacheRequiresExactTypeHints(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	s := session.(*postgresSession)
	preparer := session.(pgwire.BackendSessionParameterPreparer)
	const sql = "SELECT ? FROM messages LIMIT 1"

	first, err := preparer.PrepareWithParameterTypes(
		t.Context(), sql, []sqldriver.ParamType{sqldriver.ParamTypeBool},
	)
	if err != nil {
		t.Fatal(err)
	}
	firstCompiled := first.(*postgresStatement).compiled
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := preparer.PrepareWithParameterTypes(
		t.Context(), sql, []sqldriver.ParamType{sqldriver.ParamTypeText},
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.(*postgresStatement).compiled == firstCompiled {
		t.Fatal("different parameter type hints reused compiled statement")
	}
	if s.readCache.compiled != firstCompiled {
		t.Fatal("type mismatch mutated the prior cache entry before replacement")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLBackendReadPrepareCacheIsBoundedAndReleased(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	s := session.(*postgresSession)

	local, err := session.Prepare(t.Context(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	if s.readCache.compiled != nil {
		t.Fatal("local statement entered distributed read cache")
	}
	oversized, err := session.Prepare(
		t.Context(), "SELECT id FROM messages"+
			strings.Repeat(" ", maxPostgresReadCacheSQLBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := oversized.Close(); err != nil {
		t.Fatal(err)
	}
	if s.readCache.compiled != nil {
		t.Fatal("oversized statement entered bounded read cache")
	}

	distributed, err := session.Prepare(
		t.Context(), "SELECT id FROM messages ORDER BY id LIMIT 256",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := distributed.Close(); err != nil {
		t.Fatal(err)
	}
	if s.readCache.compiled == nil {
		t.Fatal("eligible distributed statement was not retained")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if s.readCache.compiled != nil || s.readCache.text != "" ||
		len(s.readCache.parameterTypes) != 0 {
		t.Fatal("session close retained read cache ownership")
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

func TestPostgreSQLReadCacheOwnsBorrowedRequestSQL(t *testing.T) {
	session, _ := newTypedPostgreSQLSession(t)
	const original = "SELECT id FROM messages WHERE id = 'first'"
	const next = "SELECT id FROM messages WHERE id = 'other'"
	buffer := []byte(original)
	borrowed := unsafe.String(unsafe.SliceData(buffer), len(buffer))
	first, err := session.Prepare(t.Context(), borrowed)
	if err != nil {
		t.Fatal(err)
	}
	compiled := first.(*postgresStatement).compiled
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// The wire reader reuses its request storage after a simple query completes.
	copy(buffer, next)
	s := session.(*postgresSession)
	if s.readCache.text != original || compiled.SQL() != original {
		t.Fatal("reused request storage changed the cached SQL identity")
	}
	second, err := session.Prepare(t.Context(), borrowed)
	if err != nil {
		t.Fatal(err)
	}
	if second.(*postgresStatement).compiled == compiled {
		t.Fatal("changed SQL reused routing compiled for the previous key")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}
