package driver

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

var correlatedValueOuterDocuments = []string{
	`{"id":"a_match","tenant":"t1","region":"r1","probe":10.00}`,
	`{"id":"b_no_match_with_null","tenant":"t1","region":"r1","probe":12}`,
	`{"id":"c_null_only","tenant":"t2","region":"r2","probe":50}`,
	`{"id":"d_null_probe","tenant":"t1","region":"r1","probe":null}`,
	`{"id":"e_missing_probe","tenant":"t1","region":"r1"}`,
	`{"id":"f_empty_null_probe","tenant":"t3","region":"r3","probe":null}`,
	`{"id":"g_missing_corr","region":"r1","probe":null}`,
	`{"id":"h_null_corr","tenant":null,"region":"r1","probe":null}`,
	`{"id":"i_known_no_match","tenant":"t5","region":"r5","probe":9}`,
	`{"id":"j_object","tenant":"t4","region":"r4","probe":{"b":2,"a":1}}`,
	`{"id":"k_array","tenant":"t6","region":"r6","probe":[1,2]}`,
	`{"id":"l_array_order","tenant":"t6","region":"r6","probe":[2,1]}`,
	`{"id":"m_decimal","tenant":"t7","region":"r7","probe":9007199254740993.000}`,
	`{"id":"n_bool","tenant":"t8","region":"r8","probe":true}`,
}

var correlatedValueInnerDocuments = []string{
	`{"id":"i_t1_match","tenant":"t1","region":"r1","value":10,"enabled":true}`,
	`{"id":"i_t1_duplicate","tenant":"t1","region":"r1","value":10.0,"enabled":true}`,
	`{"id":"i_t1_null","tenant":"t1","region":"r1","value":null,"enabled":true}`,
	`{"id":"i_t2_null","tenant":"t2","region":"r2","value":null,"enabled":true}`,
	`{"id":"i_t4_object","tenant":"t4","region":"r4","value":{"a":1,"b":2},"enabled":true}`,
	`{"id":"i_t5_known","tenant":"t5","region":"r5","value":8,"enabled":true}`,
	`{"id":"i_t6_array","tenant":"t6","region":"r6","value":[1,2],"enabled":true}`,
	`{"id":"i_t7_decimal","tenant":"t7","region":"r7","value":9007199254740993,"enabled":true}`,
	`{"id":"i_t8_bool","tenant":"t8","region":"r8","value":true,"enabled":true}`,
}

func correlatedValueSeedPair(t testing.TB, db *stdsql.DB, outer, inner string, indexed bool) {
	t.Helper()
	statements := []string{
		fmt.Sprintf(`CREATE TABLE %s (`+
			`id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`, outer),
		fmt.Sprintf(`CREATE TABLE %s (`+
			`id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY, enabled BOOL)`, inner),
	}
	if indexed {
		statements = append(statements,
			fmt.Sprintf(`CREATE INDEX %s_lookup ON %s (tenant, region, probe)`, outer, outer))
	}
	statements = append(statements,
		fmt.Sprintf(`INSERT INTO %s VALUES %s`, outer,
			correlatedValueDocumentValues(correlatedValueOuterDocuments)),
		fmt.Sprintf(`INSERT INTO %s VALUES %s`, inner,
			correlatedValueDocumentValues(correlatedValueInnerDocuments)),
	)
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func correlatedValueDocumentValues(documents []string) string {
	var out strings.Builder
	for i, document := range documents {
		if i != 0 {
			out.WriteByte(',')
		}
		out.WriteString("('")
		out.WriteString(document)
		out.WriteString("')")
	}
	return out.String()
}

func correlatedValueIDs(
	t testing.TB,
	q interface {
		Query(query string, args ...any) (*stdsql.Rows, error)
	},
	statement string,
	args ...any,
) []string {
	t.Helper()
	rows, err := q.Query(statement, args...)
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func correlatedValuePreparedIDs(t testing.TB, statement *stdsql.Stmt, args ...any) []string {
	t.Helper()
	rows, err := statement.Query(args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func correlatedValueCompositeExistsSQL(outer, inner string, anti, directNot bool) string {
	prefix, suffix := "", ""
	if anti {
		prefix = "NOT "
	}
	if directNot {
		prefix = "NOT ("
		suffix = ")"
	}
	return fmt.Sprintf(`SELECT o.id FROM %s AS o WHERE %sEXISTS (`+
		`SELECT 1 FROM %s AS i WHERE i.tenant = o.tenant AND i.region = o.region`+
		`)%s ORDER BY o.id`, outer, prefix, inner, suffix)
}

func correlatedValueInSQL(outer, inner string, anti, directNot bool) string {
	prefix, infix, suffix := "", "IN", ""
	if anti {
		infix = "NOT IN"
	}
	if directNot {
		prefix, suffix = "NOT (", ")"
	}
	return fmt.Sprintf(`SELECT o.id FROM %s AS o WHERE %so.probe %s (`+
		`SELECT i.value FROM %s AS i `+
		`WHERE i.tenant = o.tenant AND i.region = o.region`+
		`)%s ORDER BY o.id`, outer, prefix, infix, inner, suffix)
}

func TestCorrelatedValueCompositeExistsAndNullAwareMembership(t *testing.T) {
	db := openTestDB(t)
	correlatedValueSeedPair(t, db, "cv_plain_outer", "cv_plain_inner", false)
	correlatedValueSeedPair(t, db, "cv_indexed_outer", "cv_indexed_inner", true)

	for _, tables := range []struct {
		name         string
		outer, inner string
	}{
		{"unindexed", "cv_plain_outer", "cv_plain_inner"},
		{"indexed", "cv_indexed_outer", "cv_indexed_inner"},
	} {
		t.Run(tables.name, func(t *testing.T) {
			for _, test := range []struct {
				name      string
				statement string
				want      []string
			}{
				{
					"composite exists",
					correlatedValueCompositeExistsSQL(tables.outer, tables.inner, false, false),
					[]string{"a_match", "b_no_match_with_null", "c_null_only", "d_null_probe", "e_missing_probe", "i_known_no_match", "j_object", "k_array", "l_array_order", "m_decimal", "n_bool"},
				},
				{
					"composite not exists",
					correlatedValueCompositeExistsSQL(tables.outer, tables.inner, true, false),
					[]string{"f_empty_null_probe", "g_missing_corr", "h_null_corr"},
				},
				{
					"direct not composite exists",
					correlatedValueCompositeExistsSQL(tables.outer, tables.inner, false, true),
					[]string{"f_empty_null_probe", "g_missing_corr", "h_null_corr"},
				},
				{
					"correlated in",
					correlatedValueInSQL(tables.outer, tables.inner, false, false),
					[]string{"a_match", "j_object", "k_array", "m_decimal", "n_bool"},
				},
				{
					"correlated not in",
					correlatedValueInSQL(tables.outer, tables.inner, true, false),
					[]string{"f_empty_null_probe", "g_missing_corr", "h_null_corr", "i_known_no_match", "l_array_order"},
				},
				{
					"direct not correlated in",
					correlatedValueInSQL(tables.outer, tables.inner, false, true),
					[]string{"f_empty_null_probe", "g_missing_corr", "h_null_corr", "i_known_no_match", "l_array_order"},
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					if got := correlatedValueIDs(t, db, test.statement); !slices.Equal(got, test.want) {
						t.Fatalf("ids = %v, want %v", got, test.want)
					}
				})
			}

			preparedSQL := fmt.Sprintf(`SELECT o.id FROM %s AS o WHERE o.probe IN (`+
				`SELECT i.value FROM %s AS i WHERE i.tenant = o.tenant `+
				`AND i.region = o.region AND i.enabled = ?) ORDER BY o.id`,
				tables.outer, tables.inner)
			prepared, err := db.Prepare(preparedSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			for _, run := range []struct {
				enabled bool
				want    []string
			}{
				{true, []string{"a_match", "j_object", "k_array", "m_decimal", "n_bool"}},
				{false, nil},
				{true, []string{"a_match", "j_object", "k_array", "m_decimal", "n_bool"}},
			} {
				if got := correlatedValuePreparedIDs(t, prepared, run.enabled); !slices.Equal(got, run.want) {
					t.Fatalf("prepared enabled=%t ids = %v, want %v", run.enabled, got, run.want)
				}
			}
		})
	}
}

func TestCorrelatedValueHeapDurableAndIndexDifferential(t *testing.T) {
	const statementSQL = `SELECT o.id FROM cv_heap_outer AS o WHERE o.probe IN (` +
		`SELECT i.value FROM cv_heap_inner AS i ` +
		`WHERE i.tenant = o.tenant AND i.region = o.region) ORDER BY o.id`
	want := []string{"a_match", "j_object", "k_array", "m_decimal", "n_bool"}

	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("heap/indexed=%t", indexed), func(t *testing.T) {
			database := &store.Database{}
			outer, err := database.CreateCollection("cv_heap_outer", store.Options{ChunkDocuments: 2})
			if err != nil {
				t.Fatal(err)
			}
			inner, err := database.CreateCollection("cv_heap_inner", store.Options{ChunkDocuments: 2})
			if err != nil {
				t.Fatal(err)
			}
			for i, document := range correlatedValueOuterDocuments {
				if _, err := outer.Put(fmt.Sprintf("o%02d", i), []byte(document)); err != nil {
					t.Fatal(err)
				}
			}
			for i, document := range correlatedValueInnerDocuments {
				if _, err := inner.Put(fmt.Sprintf("i%02d", i), []byte(document)); err != nil {
					t.Fatal(err)
				}
			}
			if indexed {
				if _, err := outer.CreateIndex(store.IndexDefinition{
					Name: "cv_heap_lookup", Paths: []string{"/tenant", "/region", "/probe"},
				}); err != nil {
					t.Fatal(err)
				}
				if info, err := outer.BackfillIndex("cv_heap_lookup", 0); err != nil || info.State != store.IndexReady {
					t.Fatalf("backfill = (%+v, %v)", info, err)
				}
			}
			prepared, err := query.PrepareStatement(statementSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Release()
			var exec query.Exec
			got := correlatedValueQueryIDs(t, prepared,
				query.FromDatabase(database.Snapshot(), prepared.Collection()), &exec)
			defer exec.Release()
			if !slices.Equal(got, want) {
				t.Fatalf("heap ids = %v, want %v; stats=%+v", got, want, exec.Stats)
			}
		})

		t.Run(fmt.Sprintf("durable/indexed=%t", indexed), func(t *testing.T) {
			database, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var indexes []store.IndexDefinition
			if indexed {
				indexes = []store.IndexDefinition{{
					Name: "cv_file_lookup", Paths: []string{"/tenant", "/region", "/probe"},
				}}
			}
			outer, err := database.CreateCollection("cv_heap_outer", durable.Options{Indexes: indexes})
			if err != nil {
				t.Fatal(err)
			}
			inner, err := database.CreateCollection("cv_heap_inner", durable.Options{})
			if err != nil {
				t.Fatal(err)
			}
			for i, document := range correlatedValueOuterDocuments {
				if _, err := outer.Put([]byte(fmt.Sprintf("o%02d", i)), []byte(document)); err != nil {
					t.Fatal(err)
				}
			}
			for i, document := range correlatedValueInnerDocuments {
				if _, err := inner.Put([]byte(fmt.Sprintf("i%02d", i)), []byte(document)); err != nil {
					t.Fatal(err)
				}
			}
			catalog, err := database.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			prepared, err := query.PrepareStatement(statementSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Release()
			var exec query.Exec
			got := correlatedValueQueryIDs(t, prepared,
				query.FromFileDatabase(catalog, prepared.Collection()), &exec)
			defer exec.Release()
			if !slices.Equal(got, want) {
				t.Fatalf("durable ids = %v, want %v; stats=%+v", got, want, exec.Stats)
			}
		})
	}
}

func correlatedValueQueryIDs(
	t testing.TB,
	statement *query.Statement,
	source query.Source,
	exec *query.Exec,
) []string {
	t.Helper()
	cursor, err := statement.RunInto(exec, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatalf("id = %s, want text", cursor.Cell(0).JSON())
		}
		ids = append(ids, id)
	}
	return ids
}

func TestCorrelatedScalarCardinalityIsPerProbedGroupAndRecovers(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE cv_scalar_outer (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`,
		`CREATE TABLE cv_scalar_inner (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY)`,
		`INSERT INTO cv_scalar_outer VALUES ` +
			`('{"id":"a_one","tenant":"one","region":"r","probe":5}'),` +
			`('{"id":"b_empty","tenant":"empty","region":"r","probe":1}'),` +
			`('{"id":"c_null_one","tenant":"null-one","region":"r","probe":1}'),` +
			`('{"id":"d_dup_equal","tenant":"dup-equal","region":"r","probe":7}'),` +
			`('{"id":"e_dup_null","tenant":"dup-null","region":"r","probe":null}'),` +
			`('{"id":"f_compare","tenant":"compare","region":"r","probe":9007199254740992}'),` +
			`('{"id":"z_filtered_bad","tenant":"filtered","region":"r","probe":9}')`,
		`INSERT INTO cv_scalar_inner VALUES ` +
			`('{"id":"one","tenant":"one","region":"r","value":5.0}'),` +
			`('{"id":"null-one","tenant":"null-one","region":"r","value":null}'),` +
			`('{"id":"dup-equal-a","tenant":"dup-equal","region":"r","value":7}'),` +
			`('{"id":"dup-equal-b","tenant":"dup-equal","region":"r","value":7.00}'),` +
			`('{"id":"dup-null-a","tenant":"dup-null","region":"r","value":null}'),` +
			`('{"id":"dup-null-b","tenant":"dup-null","region":"r"}'),` +
			`('{"id":"compare","tenant":"compare","region":"r","value":9007199254740993.000}'),` +
			`('{"id":"filtered-a","tenant":"filtered","region":"r","value":9}'),` +
			`('{"id":"filtered-b","tenant":"filtered","region":"r","value":10}'),` +
			`('{"id":"unprobed-a","tenant":"unprobed","region":"r","value":1}'),` +
			`('{"id":"unprobed-b","tenant":"unprobed","region":"r","value":2}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	comparison := func(id, op string) string {
		return fmt.Sprintf(`SELECT o.id FROM cv_scalar_outer AS o `+
			`WHERE o.id = '%s' AND o.probe %s (`+
			`SELECT i.value FROM cv_scalar_inner AS i `+
			`WHERE i.tenant = o.tenant AND i.region = o.region)`, id, op)
	}
	if got := correlatedValueIDs(t, db, comparison("a_one", "=")); !slices.Equal(got, []string{"a_one"}) {
		t.Fatalf("one-row scalar ids = %v", got)
	}
	if got := correlatedValueIDs(t, db, comparison("b_empty", "=")); len(got) != 0 {
		t.Fatalf("empty scalar ids = %v, want none", got)
	}
	if got := correlatedValueIDs(t, db, comparison("c_null_one", "=")); len(got) != 0 {
		t.Fatalf("one-NULL scalar ids = %v, want none", got)
	}
	for _, operator := range []struct {
		op    string
		match bool
	}{
		{"=", false}, {"!=", true}, {"<", true},
		{"<=", true}, {">", false}, {">=", false},
	} {
		got := correlatedValueIDs(t, db, comparison("f_compare", operator.op))
		if matched := slices.Equal(got, []string{"f_compare"}); matched != operator.match {
			t.Fatalf("exact decimal scalar %s ids = %v, match=%t want %t",
				operator.op, got, matched, operator.match)
		}
	}
	// z_filtered_bad and the inner-only unprobed group both contain two rows.
	// The authored outer predicate excludes them before the scalar mark is
	// probed, so eager build-time cardinality failure would be observably wrong.
	if got := correlatedValueIDs(t, db, comparison("a_one", "=")); !slices.Equal(got, []string{"a_one"}) {
		t.Fatalf("unprobed multi-row groups poisoned valid group: %v", got)
	}

	for _, id := range []string{"d_dup_equal", "e_dup_null", "z_filtered_bad"} {
		t.Run(id, func(t *testing.T) {
			rows, err := db.Query(comparison(id, "="))
			if rows != nil {
				_ = rows.Close()
				t.Fatal("cardinality violation published rows")
			}
			if !errors.Is(err, query.ErrCardinalityViolation) {
				t.Fatalf("error = %T %v, want ErrCardinalityViolation", err, err)
			}
			if got := correlatedValueIDs(t, db, comparison("a_one", "=")); !slices.Equal(got, []string{"a_one"}) {
				t.Fatalf("recovery ids = %v, want [a_one]", got)
			}
		})
	}

	directNot := `SELECT o.id FROM cv_scalar_outer AS o WHERE o.id = 'a_one' AND NOT (` +
		`o.probe = (SELECT i.value FROM cv_scalar_inner AS i ` +
		`WHERE i.tenant = o.tenant AND i.region = o.region))`
	if got := correlatedValueIDs(t, db, directNot); len(got) != 0 {
		t.Fatalf("direct NOT scalar ids = %v, want none", got)
	}
}

func TestCorrelatedValueEmptyCatalogSelfCorrelationAndDependencyDrop(t *testing.T) {
	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE cv_empty_outer (id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`,
		`CREATE TABLE cv_empty_inner (id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY)`,
		`INSERT INTO cv_empty_outer VALUES ` +
			`('{"id":"null","tenant":"none","region":"r","probe":null}'),` +
			`('{"id":"missing","tenant":"none","region":"r"}')`,
		`CREATE TABLE cv_self (` +
			`id STRING PRIMARY KEY, role STRING, tenant STRING, region STRING, probe ANY)`,
		`INSERT INTO cv_self VALUES ` +
			`('{"id":"outer-match","role":"outer","tenant":"t","region":"r","probe":{"b":2,"a":1}}'),` +
			`('{"id":"outer-empty","role":"outer","tenant":"x","region":"r","probe":null}'),` +
			`('{"id":"inner-match","role":"inner","tenant":"t","region":"r","probe":{"a":1,"b":2}}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	emptyNotIn := correlatedValueInSQL("cv_empty_outer", "cv_empty_inner", true, false)
	if got, want := correlatedValueIDs(t, db, emptyNotIn), []string{"missing", "null"}; !slices.Equal(got, want) {
		t.Fatalf("empty cataloged NOT IN ids = %v, want %v", got, want)
	}
	self := `SELECT o.id FROM cv_self AS o WHERE o.role = 'outer' AND o.probe IN (` +
		`SELECT i.probe FROM cv_self AS i WHERE i.tenant = o.tenant ` +
		`AND i.region = o.region AND i.role = 'inner') ORDER BY o.id`
	prepared, err := db.Prepare(self)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if got := correlatedValuePreparedIDs(t, prepared); !slices.Equal(got, []string{"outer-match"}) {
		t.Fatalf("self-correlated ids = %v, want [outer-match]", got)
	}

	dependencySQL := `SELECT o.id FROM cv_empty_outer AS o WHERE o.probe IN (` +
		`SELECT i.value FROM cv_empty_inner AS i WHERE i.tenant = o.tenant AND i.region = o.region)`
	dependency, err := db.Prepare(dependencySQL)
	if err != nil {
		t.Fatal(err)
	}
	defer dependency.Close()
	warmRows, err := dependency.Query()
	if err != nil {
		t.Fatal(err)
	}
	if err := warmRows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE cv_empty_inner`); err != nil {
		t.Fatal(err)
	}
	rows, err := dependency.Query()
	if rows != nil {
		_ = rows.Close()
		t.Fatal("dropped correlated dependency published rows")
	}
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("dropped dependency = %T %v, want ErrTableNotFound", err, err)
	}
}

func TestCorrelatedValueTransactionSnapshotPendingWritesAndPreparedReuse(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(4)
	for _, statement := range []string{
		`CREATE TABLE cv_tx_outer (id STRING PRIMARY KEY, tenant STRING, region STRING, probe STRING)`,
		`CREATE TABLE cv_tx_inner (id STRING PRIMARY KEY, tenant STRING, region STRING, value STRING)`,
		`INSERT INTO cv_tx_outer VALUES ` +
			`('{"id":"a_initial","tenant":"x","region":"r","probe":"yes"}'),` +
			`('{"id":"b_pending","tenant":"y","region":"r","probe":"later"}')`,
		`INSERT INTO cv_tx_inner VALUES ` +
			`('{"id":"initial","tenant":"x","region":"r","value":"yes"}')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	const source = `SELECT o.id FROM cv_tx_outer AS o WHERE o.probe IN (` +
		`SELECT i.value FROM cv_tx_inner AS i WHERE i.tenant = o.tenant ` +
		`AND i.region = o.region) ORDER BY o.id`
	prepared, err := db.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	tx, err := db.BeginTx(context.Background(), &stdsql.TxOptions{
		Isolation: stdsql.LevelRepeatableRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := db.Exec(`INSERT INTO cv_tx_inner VALUES (` +
		`'{"id":"outside-inner","tenant":"y","region":"r","value":"later"}')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO cv_tx_outer VALUES (` +
		`'{"id":"c_outside","tenant":"x","region":"r","probe":"yes"}')`); err != nil {
		t.Fatal(err)
	}
	txPrepared := tx.Stmt(prepared)
	defer txPrepared.Close()
	if got := correlatedValuePreparedIDs(t, txPrepared); !slices.Equal(got, []string{"a_initial"}) {
		t.Fatalf("BEGIN snapshot ids = %v, want [a_initial]", got)
	}
	if _, err := tx.Exec(`INSERT INTO cv_tx_inner VALUES (` +
		`'{"id":"pending-inner","tenant":"y","region":"r","value":"later"}')`); err != nil {
		t.Fatal(err)
	}
	if got, want := correlatedValuePreparedIDs(t, txPrepared),
		[]string{"a_initial", "b_pending"}; !slices.Equal(got, want) {
		t.Fatalf("read-your-writes ids = %v, want %v", got, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got, want := correlatedValuePreparedIDs(t, prepared),
		[]string{"a_initial", "b_pending", "c_outside"}; !slices.Equal(got, want) {
		t.Fatalf("post-rollback autocommit ids = %v, want %v", got, want)
	}
}

func TestCorrelatedValueCancellationAndExactMemoryFailureAreAtomic(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE cv_budget_outer (id STRING PRIMARY KEY, tenant STRING, region STRING, probe STRING)`,
		`CREATE TABLE cv_budget_inner (id STRING PRIMARY KEY, tenant STRING, region STRING, value STRING)`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	const padding = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	const fixtureRows = 512
	for base := 0; base < fixtureRows; base += 64 {
		var outer, inner strings.Builder
		outer.WriteString(`INSERT INTO cv_budget_outer VALUES `)
		inner.WriteString(`INSERT INTO cv_budget_inner VALUES `)
		for i := base; i < base+64; i++ {
			if i != base {
				outer.WriteByte(',')
				inner.WriteByte(',')
			}
			value := fmt.Sprintf("value-%04d-%s", i, padding)
			_, _ = fmt.Fprintf(&outer,
				`('{"id":"o-%04d","tenant":"t-%04d","region":"r","probe":"%s"}')`,
				i, i, value)
			_, _ = fmt.Fprintf(&inner,
				`('{"id":"i-%04d","tenant":"t-%04d","region":"r","value":"%s"}')`,
				i, i, value)
		}
		correlatedExistsRuntimeExec(t, session, outer.String(), nil)
		correlatedExistsRuntimeExec(t, session, inner.String(), nil)
	}
	prepared := correlatedExistsRuntimePrepare(t, session,
		`SELECT o.id FROM cv_budget_outer AS o WHERE o.probe IN (`+
			`SELECT i.value FROM cv_budget_inner AS i `+
			`WHERE i.tenant = o.tenant AND i.region = o.region)`)

	var cancel query.CancelFlag
	if err := session.SetCancelFlag(&cancel); err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	cursor, err := prepared.Query(context.Background(), nil)
	if cursor != nil {
		_ = cursor.Close()
		t.Fatal("canceled correlated IN published a cursor")
	}
	if !errors.Is(err, query.ErrCanceled) || session.State() != SessionIdle {
		t.Fatalf("canceled query = (%v, %s), want ErrCanceled and idle", err, session.State())
	}
	cancel.Reset()
	if err := session.SetCancelFlag(nil); err != nil {
		t.Fatal(err)
	}

	attempt := func(limit int64) (bool, *query.WorkBudgetError) {
		if err := session.SetMemoryLimit(limit); err != nil {
			t.Fatalf("SetMemoryLimit(%d): %v", limit, err)
		}
		cursor, err := prepared.Query(context.Background(), nil)
		if err == nil {
			rows := 0
			for cursor.Next() {
				rows++
			}
			if closeErr := cursor.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if rows != fixtureRows {
				t.Fatalf("rows = %d, want %d", rows, fixtureRows)
			}
			return true, nil
		}
		if cursor != nil {
			_ = cursor.Close()
			t.Fatal("work-budget failure published a cursor")
		}
		var budgetErr *query.WorkBudgetError
		if !errors.Is(err, query.ErrWorkBudget) || !errors.As(err, &budgetErr) {
			t.Fatalf("budget error = %T %v, want WorkBudgetError", err, err)
		}
		// Nested durable operators report the exact admission's remaining
		// sub-budget, not the session-wide ceiling. The binary search below is
		// over the public ceiling itself; the typed local error must still prove
		// its requested bytes exceed its own admitted remainder.
		if budgetErr.Bytes <= budgetErr.Limit || session.State() != SessionIdle {
			t.Fatalf("budget error = %+v state=%s limit=%d", budgetErr, session.State(), limit)
		}
		return false, budgetErr
	}

	low, high := int64(64<<10), int64(64<<20)
	if ok, _ := attempt(low); ok {
		t.Fatalf("grouped mark fixture fit minimum work budget %d", low)
	}
	if ok, failure := attempt(high); !ok {
		t.Fatalf("grouped mark fixture exceeded calibration ceiling: %+v", failure)
	}
	for high-low > 1 {
		mid := low + (high-low)/2
		if ok, failure := attempt(mid); ok {
			high = mid
		} else {
			low = mid
			if proved := failure.Bytes - 1; proved > low && proved < high {
				low = proved
			}
		}
	}
	if ok, failure := attempt(high - 1); ok || failure == nil {
		t.Fatalf("one-byte-short result = (%t, %+v), want typed refusal below %d",
			ok, failure, high)
	}
	if ok, failure := attempt(high); !ok {
		t.Fatalf("exact admitted budget failed after recovery: %+v", failure)
	}
}

func TestCorrelatedValueDirectDurableWarmExecutionIsAllocationFree(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, statement := range []string{
		`CREATE TABLE cv_alloc_outer (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, probe ANY)`,
		`CREATE INDEX cv_alloc_lookup ON cv_alloc_outer (tenant, region, probe)`,
		`CREATE TABLE cv_alloc_inner (` +
			`id STRING PRIMARY KEY, tenant STRING, region STRING, value ANY)`,
		`INSERT INTO cv_alloc_outer VALUES ` +
			`('{"id":"a","tenant":"t1","region":"r","probe":5}'),` +
			`('{"id":"b","tenant":"t2","region":"r","probe":8}'),` +
			`('{"id":"c","tenant":"empty","region":"r","probe":null}')`,
		`INSERT INTO cv_alloc_inner VALUES ` +
			`('{"id":"i1","tenant":"t1","region":"r","value":5.0}'),` +
			`('{"id":"i2","tenant":"t2","region":"r","value":7}')`,
	} {
		correlatedExistsRuntimeExec(t, session, statement, nil)
	}
	for _, test := range []struct {
		name, predicate string
		rows            int
	}{
		{
			"composite exists",
			`EXISTS (SELECT 1 FROM cv_alloc_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)`,
			2,
		},
		{
			"composite not exists",
			`NOT EXISTS (SELECT 1 FROM cv_alloc_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)`,
			1,
		},
		{
			"correlated in",
			`o.probe IN (SELECT i.value FROM cv_alloc_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)`,
			1,
		},
		{
			"correlated not in",
			`o.probe NOT IN (SELECT i.value FROM cv_alloc_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)`,
			2,
		},
		{
			"correlated scalar",
			`o.probe = (SELECT i.value FROM cv_alloc_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)`,
			1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := correlatedExistsRuntimePrepare(t, session,
				`SELECT o.id FROM cv_alloc_outer AS o WHERE `+test.predicate)
			if !prepared.statement.usesDirectDurableCatalog() {
				t.Fatal("grouped correlated statement did not select coherent direct durable catalog")
			}
			ctx := context.Background()
			var cursor Cursor
			run := func() {
				if err := prepared.QueryInto(ctx, nil, &cursor); err != nil {
					panic(err)
				}
				rows := 0
				for cursor.Next() {
					rows++
				}
				if rows != test.rows {
					panic(fmt.Sprintf("rows = %d, want %d", rows, test.rows))
				}
				if err := cursor.Close(); err != nil {
					panic(err)
				}
			}
			run()
			if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
				t.Fatalf("warmed grouped correlated execution allocated %.2f times, want zero", allocs)
			}
		})
	}
}

func TestCorrelatedValueGeneralizedDirectNotRemainsPositionedRefusal(t *testing.T) {
	db := openTestDB(t)
	correlatedValueSeedPair(t, db, "cv_refuse_outer", "cv_refuse_inner", false)
	for _, test := range []struct {
		name, source, marker string
	}{
		{
			"NOT over OR",
			`SELECT o.id FROM cv_refuse_outer AS o WHERE NOT (` +
				`o.probe IN (SELECT i.value FROM cv_refuse_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region) OR o.id = 'none')`,
			"o.probe",
		},
		{
			"nested double NOT",
			`SELECT o.id FROM cv_refuse_outer AS o WHERE NOT (NOT (` +
				`o.probe IN (SELECT i.value FROM cv_refuse_inner AS i ` +
				`WHERE i.tenant = o.tenant AND i.region = o.region)))`,
			"o.probe",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := db.Prepare(test.source)
			if prepared != nil {
				_ = prepared.Close()
				t.Fatal("unsupported generalized NOT prepared")
			}
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want FeatureNotSupportedError", err, err)
			}
			if unsupported.Pos != strings.Index(test.source, test.marker) {
				t.Fatalf("position = %d, want %d: %v",
					unsupported.Pos, strings.Index(test.source, test.marker), unsupported)
			}
		})
	}
}
