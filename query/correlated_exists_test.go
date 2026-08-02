package query

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

var correlatedExistsOuterDocs = []string{
	`{"id":"bool_true","k":true,"enabled":true}`,
	`{"id":"bool_false","k":false,"enabled":true}`,
	`{"id":"decimal","k":1,"enabled":true}`,
	`{"id":"wide","k":9007199254740993,"enabled":true}`,
	`{"id":"string","k":"a\u0062","enabled":true}`,
	`{"id":"null","k":null,"enabled":true}`,
	`{"id":"missing","enabled":true}`,
	`{"id":"unmatched","k":"none","enabled":true}`,
}

var correlatedExistsInnerDocs = []string{
	`{"id":"i-bool-true","k":true,"active":true}`,
	`{"id":"i-bool-false","k":false,"active":false}`,
	`{"id":"i-decimal-a","k":1.0,"active":true}`,
	`{"id":"i-decimal-b","k":10e-1,"active":true}`,
	`{"id":"i-wide","k":9007199254740993.0,"active":true}`,
	`{"id":"i-string","k":"ab","active":true}`,
	`{"id":"i-null","k":null,"active":true}`,
}

func correlatedExistsHeapDatabase(
	t testing.TB,
	outerDocs, innerDocs []string,
	indexed bool,
) *store.Database {
	t.Helper()
	db := &store.Database{}
	outer, err := db.CreateCollection("ce_outer", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put(fmt.Sprintf("o%03d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if indexed {
		if _, err := outer.CreateIndex(store.IndexDefinition{
			Name: "outer_k", Paths: []string{"/k"},
		}); err != nil {
			t.Fatal(err)
		}
		if info, err := outer.BackfillIndex("outer_k", 0); err != nil ||
			info.State != store.IndexReady {
			t.Fatalf("BackfillIndex = (%+v, %v)", info, err)
		}
	}
	inner, err := db.CreateCollection("ce_inner", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range innerDocs {
		if _, err := inner.Put(fmt.Sprintf("i%03d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func correlatedExistsDurableDatabase(
	t testing.TB,
	outerDocs, innerDocs []string,
	indexed bool,
) *durable.Database {
	t.Helper()
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	outerOptions := durableJoinOptions()
	if indexed {
		outerOptions = durableJoinOptions(store.IndexDefinition{
			Name: "outer_k", Paths: []string{"/k"},
		})
	}
	outer, err := db.CreateCollection("ce_outer", outerOptions)
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put([]byte(fmt.Sprintf("o%03d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("ce_inner", durableJoinOptions())
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range innerDocs {
		if _, err := inner.Put([]byte(fmt.Sprintf("i%03d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func correlatedExistsSQL(anti bool, parameterized bool) string {
	prefix := ""
	if anti {
		prefix = "NOT "
	}
	active := "TRUE"
	outer := ""
	if parameterized {
		outer = "o.enabled = ? AND "
		active = "?"
	}
	return `SELECT o.id FROM ce_outer AS o WHERE ` + outer + prefix + `EXISTS (` +
		`SELECT 1 FROM ce_inner AS i WHERE i.k = o.k AND i.active = ` + active +
		`) ORDER BY o.id`
}

func correlatedStatementIDs(
	t testing.TB,
	statement *Statement,
	source Source,
	exec *Exec,
	args ...any,
) []string {
	t.Helper()
	cursor, err := statement.RunInto(exec, source, args)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatalf("id cell = %s, want string", cursor.Cell(0).JSON())
		}
		ids = append(ids, id)
	}
	return ids
}

func correlatedExistsOracle(
	t testing.TB,
	outerDocs, innerDocs []string,
	active, anti bool,
) []string {
	t.Helper()
	outer := decodeDocs(t, asBytes(outerDocs))
	inner := decodeDocs(t, asBytes(innerDocs))
	ids := make([]string, 0, len(outer))
	for _, outerDoc := range outer {
		outerKey := refClassify(refResolve("k", outerDoc))
		matched := false
		if outerKey.kind != kindNull {
			for _, innerDoc := range inner {
				if !refEval(Cmp("active", Eq, active), innerDoc) {
					continue
				}
				innerKey := refClassify(refResolve("k", innerDoc))
				if innerKey.kind != kindNull && refCompare(outerKey, innerKey) == 0 {
					matched = true
					break
				}
			}
		}
		if matched == anti {
			continue
		}
		id, _ := refResolve("id", outerDoc)
		ids = append(ids, id.(string))
	}
	slices.Sort(ids)
	return ids
}

func TestSQLCorrelatedExistsDifferentialExactScalarsAndNoFanOut(t *testing.T) {
	variants := [][]string{
		nil,
		correlatedExistsInnerDocs[:1],
		correlatedExistsInnerDocs[:2],
		correlatedExistsInnerDocs[2:4],
		correlatedExistsInnerDocs[4:6],
		correlatedExistsInnerDocs,
	}
	for variant, innerDocs := range variants {
		for _, indexed := range []bool{false, true} {
			db := correlatedExistsHeapDatabase(
				t, correlatedExistsOuterDocs, innerDocs, indexed,
			)
			catalog := db.Snapshot()
			for _, anti := range []bool{false, true} {
				statement, err := PrepareStatement(correlatedExistsSQL(anti, false))
				if err != nil {
					t.Fatal(err)
				}
				for _, membershipMax := range []int{1, 1 << 20} {
					exec := Exec{Options: ExecOptions{JoinMembershipMax: membershipMax}}
					got := correlatedStatementIDs(
						t, statement,
						FromDatabase(catalog, statement.Collection()), &exec,
					)
					want := correlatedExistsOracle(
						t, correlatedExistsOuterDocs, innerDocs, true, anti,
					)
					if !slices.Equal(got, want) {
						t.Fatalf("variant=%d indexed=%t anti=%t max=%d ids=%v want=%v",
							variant, indexed, anti, membershipMax, got, want)
					}
					if exec.Stats.JoinBuilds != 0 || exec.Stats.JoinPairs != 0 ||
						exec.Stats.JoinMemberships != 1 {
						t.Fatalf("decorrelated plan fanned out or did not bind once: %+v", exec.Stats)
					}
					exec.Release()
				}
				statement.Release()
			}
		}
	}
}

func TestSQLCorrelatedExistsDurableDifferentialExactScalars(t *testing.T) {
	for _, innerDocs := range [][]string{nil, correlatedExistsInnerDocs} {
		for _, indexed := range []bool{false, true} {
			database := correlatedExistsDurableDatabase(
				t, correlatedExistsOuterDocs, innerDocs, indexed,
			)
			catalog, err := database.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, anti := range []bool{false, true} {
				statement, err := PrepareStatement(correlatedExistsSQL(anti, false))
				if err != nil {
					t.Fatal(err)
				}
				var exec Exec
				got := correlatedStatementIDs(
					t, statement, FromFileDatabase(catalog, "ce_outer"), &exec,
				)
				want := correlatedExistsOracle(
					t, correlatedExistsOuterDocs, innerDocs, true, anti,
				)
				if !slices.Equal(got, want) {
					t.Fatalf("inner=%d indexed=%t anti=%t rows=%v want=%v stats=%+v",
						len(innerDocs), indexed, anti, got, want, exec.Stats)
				}
				if exec.Stats.JoinBuilds != 0 || exec.Stats.JoinPairs != 0 ||
					exec.Stats.JoinMemberships != 1 {
					t.Fatalf("durable decorrelation fanned out or rebound: %+v", exec.Stats)
				}
				statement.Release()
				exec.Release()
			}
			if err := catalog.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSQLCorrelatedNotExistsIsTwoValuedForNullMissingAndEmpty(t *testing.T) {
	for _, innerDocs := range [][]string{nil, correlatedExistsInnerDocs} {
		db := correlatedExistsHeapDatabase(t, correlatedExistsOuterDocs, innerDocs, true)
		statement, err := PrepareStatement(correlatedExistsSQL(true, false))
		if err != nil {
			t.Fatal(err)
		}
		var exec Exec
		got := correlatedStatementIDs(
			t, statement, FromDatabase(db.Snapshot(), statement.Collection()), &exec,
		)
		want := correlatedExistsOracle(t, correlatedExistsOuterDocs, innerDocs, true, true)
		if !slices.Equal(got, want) || !slices.Contains(got, "null") ||
			!slices.Contains(got, "missing") {
			t.Fatalf("inner=%d anti ids=%v want=%v", len(innerDocs), got, want)
		}
		p, err := statement.q.compiled()
		if err != nil {
			t.Fatal(err)
		}
		if p.where == nil || p.where.kind != predAntiBound {
			t.Fatalf("anti predicate = %+v, want dedicated predAntiBound", p.where)
		}
		statement.Release()
		exec.Release()
	}
}

func TestSQLCorrelatedExistsPreparedBudgetCancellationAndReuse(t *testing.T) {
	outerDocs := make([]string, 0, 512)
	innerDocs := make([]string, 0, 1024)
	for i := range 512 {
		key := fmt.Sprintf("key-%04d-%s", i, strings.Repeat("x", 96))
		outerDocs = append(outerDocs, fmt.Sprintf(
			`{"id":"outer-%04d","k":%q,"enabled":true}`,
			i, key,
		))
	}
	for i := range 1024 {
		key := fmt.Sprintf("key-%04d-%s", i, strings.Repeat("x", 96))
		innerDocs = append(innerDocs, fmt.Sprintf(
			`{"id":"inner-%04d","k":%q,"active":true}`,
			i, key,
		))
	}
	db := correlatedExistsHeapDatabase(
		t, outerDocs, innerDocs, false,
	)
	statement, err := PrepareStatement(correlatedExistsSQL(false, true))
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	defer exec.Release()
	args := []any{true, true}
	var stale Cursor
	for warm := range 2 {
		cursor, runErr := statement.RunInto(&exec, source, args)
		if runErr != nil || !cursor.Next() {
			t.Fatalf("warm run %d: row=%d error=%v", warm, cursor.Row(), runErr)
		}
		stale = cursor
	}
	exact := exec.Workspace.heapWorkBudget.used.Load()
	if exact <= minimumHeapMemoryBytes || !stale.Next() {
		t.Fatalf("steady exact work limit = %d, cursor row = %d",
			exact, stale.Row())
	}

	exec.Options.MemoryBytes = exact - 1
	_, err = statement.RunInto(&exec, source, args)
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) ||
		budgetErr.Bytes != exact || budgetErr.Limit != exact-1 ||
		exec.Result.RowCount != 0 || stale.Next() {
		t.Fatalf("budget run = %T %v rows=%d stale=%d",
			err, err, exec.Result.RowCount, stale.Row())
	}
	exec.Options.MemoryBytes = exact
	if got, want := correlatedStatementIDs(t, statement, source, &exec, args...),
		correlatedExistsOracle(t, outerDocs, innerDocs, true, false); !slices.Equal(got, want) {
		t.Fatalf("exact-budget recovery ids=%v want=%v", got, want)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	_, err = statement.RunInto(&exec, source, args)
	if !errors.Is(err, ErrCanceled) || exec.Result.RowCount != 0 {
		t.Fatalf("cancel run = %v rows=%d", err, exec.Result.RowCount)
	}
	cancel.Reset()
	if got, want := correlatedStatementIDs(t, statement, source, &exec, args...),
		correlatedExistsOracle(t, outerDocs, innerDocs, true, false); !slices.Equal(got, want) {
		t.Fatalf("recovered ids=%v want=%v", got, want)
	}
	args[1] = false
	if got, want := correlatedStatementIDs(t, statement, source, &exec, args...),
		correlatedExistsOracle(t, outerDocs, innerDocs, false, false); !slices.Equal(got, want) {
		t.Fatalf("rebound ids=%v want=%v", got, want)
	}
}

func TestSQLCorrelatedExistsExplainAndDirectCatalogContract(t *testing.T) {
	for _, anti := range []bool{false, true} {
		statement, err := PrepareStatement(correlatedExistsSQL(anti, false))
		if err != nil {
			t.Fatal(err)
		}
		if !statement.RequiresCatalog() || !statement.UsesDirectCatalogExecution() ||
			statement.NumJoins() != 1 {
			t.Fatalf("catalog/direct/joins = %t/%t/%d",
				statement.RequiresCatalog(), statement.UsesDirectCatalogExecution(), statement.NumJoins())
		}
		explained, err := statement.Explain()
		if err != nil {
			t.Fatal(err)
		}
		want := "decorrelated-exists-semi"
		if anti {
			want = "decorrelated-exists-anti"
		}
		if !strings.Contains(explained, want) ||
			strings.Contains(explained, "hash-build-and-probe") {
			t.Fatalf("EXPLAIN = %s, want %q without fan-out", explained, want)
		}
		statement.Release()
	}
}

func TestSQLCorrelatedExistsCachesOnlyProofBackedParameterFreeLowering(t *testing.T) {
	statement, err := PrepareStatement(correlatedExistsSQL(false, false))
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if !statement.cached || statement.q.built == nil ||
		!statement.nested.onlyDecorrelatedExists() {
		t.Fatalf("decorrelated cache state = cached %t built %p nested %+v",
			statement.cached, statement.q.built, statement.nested)
	}
	built, plan := statement.q.built, statement.q.built.plan
	db := correlatedExistsHeapDatabase(
		t, correlatedExistsOuterDocs, correlatedExistsInnerDocs, false,
	)
	source := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	defer exec.Release()
	for range 2 {
		if _, err := statement.RunInto(&exec, source, nil); err != nil {
			t.Fatal(err)
		}
		if statement.q.built != built || statement.q.built.plan != plan {
			t.Fatal("parameter-free decorrelated lowering was rebuilt")
		}
	}

	for _, source := range []string{
		`SELECT o.id FROM ce_outer o WHERE EXISTS (` +
			`SELECT i.id FROM ce_inner i WHERE i.active = TRUE)`,
		`SELECT d.id FROM (SELECT id FROM ce_outer) d`,
	} {
		other, err := PrepareStatement(source)
		if err != nil {
			t.Fatal(err)
		}
		if other.cached {
			other.Release()
			t.Fatalf("unrelated nested statement became cached: %s", source)
		}
		other.Release()
	}
}

func TestSQLMultipleCorrelatedExistsComposeWithoutFanOutOrWarmAllocations(t *testing.T) {
	database := &store.Database{}
	outer, err := database.CreateCollection("ce_multi_outer", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, document := range []string{
		`{"id":"a","k":"x"}`,
		`{"id":"b","k":"y"}`,
		`{"id":"c","k":"z"}`,
		`{"id":"missing"}`,
	} {
		if _, err := outer.Put(fmt.Sprintf("o%d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	allow, err := database.CreateCollection("ce_multi_allow", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, document := range []string{
		`{"id":"a1","k":"x"}`,
		`{"id":"a2","k":"x"}`,
		`{"id":"b1","k":"y"}`,
	} {
		if _, err := allow.Put(fmt.Sprintf("a%d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	block, err := database.CreateCollection("ce_multi_block", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := block.Put("b0", []byte(`{"id":"blocked","k":"y"}`)); err != nil {
		t.Fatal(err)
	}

	statement, err := PrepareStatement(`
		SELECT o.id FROM ce_multi_outer AS o
		WHERE EXISTS (
			SELECT 1 FROM ce_multi_allow AS a WHERE a.k = o.k
		) AND NOT EXISTS (
			SELECT 1 FROM ce_multi_block AS b WHERE b.k = o.k
		)
		ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.NumJoins() != 2 || !statement.RequiresCatalog() ||
		!statement.UsesDirectCatalogExecution() {
		t.Fatalf("capabilities = joins:%d catalog:%t direct:%t, want 2/true/true",
			statement.NumJoins(), statement.RequiresCatalog(),
			statement.UsesDirectCatalogExecution())
	}

	source := FromDatabase(database.Snapshot(), statement.Collection())
	var exec Exec
	defer exec.Release()
	run := func() {
		cursor, runErr := statement.RunInto(&exec, source, nil)
		if runErr != nil {
			panic(runErr)
		}
		rows := 0
		for cursor.Next() {
			id, ok := cursor.Cell(0).Text()
			if !ok || id != "a" || rows != 0 {
				panic(fmt.Sprintf("row %d = %q/%t, want a/true", rows, id, ok))
			}
			rows++
		}
		if rows != 1 {
			panic(fmt.Sprintf("rows = %d, want 1", rows))
		}
		if exec.Stats.JoinMemberships != 2 || exec.Stats.JoinBuilds != 0 ||
			exec.Stats.JoinPairs != 0 {
			panic(fmt.Sprintf("join stats = %+v", exec.Stats))
		}
	}
	run()
	if allocs := testing.AllocsPerRun(200, run); allocs != 0 {
		t.Fatalf("warmed two-predicate decorrelation allocated %.2f times, want zero", allocs)
	}
}

func TestSQLCorrelatedExistsCanonicalContainerEqualityIsIndexIndependent(t *testing.T) {
	tests := []struct {
		name          string
		outer         []string
		inner         []string
		exists        []string
		anti          []string
		indexCanBound bool
	}{
		{
			name: "inner container disables scalar index pruning",
			outer: []string{
				`{"id":"scalar","k":"x"}`,
				`{"id":"container_exact","k":[1,2]}`,
				// SQL storage canonicalizes object key order before the query
				// layer sees it, so an authored {"b":2,"a":1} arrives in this
				// canonical stored representation and compares equal.
				`{"id":"object_order_variant","k":{"a":1,"b":2}}`,
				`{"id":"object_other_value","k":{"a":1,"b":3}}`,
				`{"id":"array_other_order","k":[2,1]}`,
				`{"id":"null","k":null}`,
				`{"id":"missing"}`,
			},
			inner: []string{
				`{"id":"i-scalar","k":"x","active":true}`,
				`{"id":"i-container","k":[1,2],"active":true}`,
				`{"id":"i-object","k":{"a":1,"b":2},"active":true}`,
			},
			exists: []string{"container_exact", "object_order_variant", "scalar"},
			anti: []string{
				"array_other_order", "missing", "null", "object_other_value",
			},
		},
		{
			name: "scalar inner permits sound pruning of outer containers",
			outer: []string{
				`{"id":"scalar","k":"x"}`,
				`{"id":"container","k":[1,2]}`,
				`{"id":"null","k":null}`,
				`{"id":"missing"}`,
			},
			inner:         []string{`{"id":"i","k":"x","active":true}`},
			exists:        []string{"scalar"},
			anti:          []string{"container", "missing", "null"},
			indexCanBound: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.indexCanBound {
				for i := range 256 {
					id := fmt.Sprintf("unmatched-%03d", i)
					test.outer = append(test.outer, fmt.Sprintf(
						`{"id":%q,"k":"absent-%03d"}`, id, i,
					))
					test.anti = append(test.anti, id)
				}
				slices.Sort(test.anti)
			}
			check := func(t *testing.T, source Source, indexed, durable bool) {
				t.Helper()
				for _, anti := range []bool{false, true} {
					statement, err := PrepareStatement(correlatedExistsSQL(anti, false))
					if err != nil {
						t.Fatal(err)
					}
					var exec Exec
					got := correlatedStatementIDs(t, statement, source, &exec)
					want := test.exists
					if anti {
						want = test.anti
					}
					if !slices.Equal(got, want) {
						t.Fatalf("indexed=%t anti=%t rows=%v want=%v stats=%+v",
							indexed, anti, got, want, exec.Stats)
					}
					bounded := exec.Workspace.storeIndexProbes != 0
					lookups := exec.Workspace.storeIndexProbes
					if durable {
						bounded = exec.Stats.IndexBounded
						lookups = exec.Stats.IndexLookups
					}
					expectBounded := test.indexCanBound && (!anti || !durable)
					if indexed && bounded != expectBounded {
						t.Fatalf("indexed existential leaf bounded=%t want=%t anti=%t durable=%t stats=%+v heap_probes=%d",
							bounded, expectBounded, anti, durable, exec.Stats,
							exec.Workspace.storeIndexProbes)
					}
					if (!indexed || !expectBounded) && lookups != 0 {
						t.Fatalf("unsound/unconfigured index probes=%d durable=%t stats=%+v",
							lookups, durable, exec.Stats)
					}
					statement.Release()
					exec.Release()
				}
			}

			for _, indexed := range []bool{false, true} {
				heap := correlatedExistsHeapDatabase(t, test.outer, test.inner, indexed)
				check(t, FromDatabase(heap.Snapshot(), "ce_outer"), indexed, false)
			}
			for _, indexed := range []bool{false, true} {
				file := correlatedExistsDurableDatabase(t, test.outer, test.inner, indexed)
				catalog, err := file.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				check(t, FromFileDatabase(catalog, "ce_outer"), indexed, true)
				_ = catalog.Close()
			}
		})
	}
}

func TestCorrelatedAntiBoundMembershipLookupHeapAndDurable(t *testing.T) {
	outer := []string{
		`{"id":"matched","ref":"k1"}`,
		`{"id":"unmatched","ref":"gone"}`,
		`{"id":"null","ref":null}`,
		`{"id":"missing"}`,
	}
	inner := []string{
		`{"id":1}`,
		`{"id":2}`,
	}
	build := func() *Query {
		join := JoinOn("ce_inner", "ref", JoinKey)
		join.anti = true
		join.origin = joinOriginDecorrelatedExists
		return Select(Path("id")).Join(join).OrderBy("id", Asc)
	}
	heap := correlatedExistsHeapDatabase(t, outer, nil, true)
	heapInner, _ := heap.Collection("ce_inner")
	for i, key := range []string{"k1", "k2"} {
		if _, err := heapInner.Put(key, []byte(inner[i])); err != nil {
			t.Fatal(err)
		}
	}
	file := correlatedExistsDurableDatabase(t, outer, nil, false)
	fileInner, _ := file.Collection("ce_inner")
	for i, key := range [][]byte{[]byte("k1"), []byte("k2")} {
		if _, err := fileInner.Put(key, []byte(inner[i])); err != nil {
			t.Fatal(err)
		}
	}
	fileCatalog, err := file.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fileCatalog.Close() }()

	for _, backend := range []struct {
		name   string
		source Source
	}{
		{"heap", FromDatabase(heap.Snapshot(), "ce_outer")},
		{"durable", FromFileDatabase(fileCatalog, "ce_outer")},
	} {
		for _, strategy := range []struct {
			name string
			max  int
		}{
			{"membership", 1 << 20},
			{"lookup", 1},
		} {
			t.Run(backend.name+"/"+strategy.name, func(t *testing.T) {
				q := build()
				var exec Exec
				exec.Options.JoinMembershipMax = strategy.max
				if err := q.RunInto(&exec, backend.source); err != nil {
					t.Fatal(err)
				}
				got := make([]string, 0, exec.Result.RowCount)
				for _, cell := range exec.Result.Columns[0].Cells {
					text, _ := cell.Text()
					got = append(got, text)
				}
				want := []string{"missing", "null", "unmatched"}
				if !slices.Equal(got, want) {
					t.Fatalf("anti rows=%v want=%v stats=%+v", got, want, exec.Stats)
				}
				if strategy.name == "membership" && exec.Stats.JoinMemberships != 1 ||
					strategy.name == "lookup" && exec.Stats.JoinLookups != 1 {
					t.Fatalf("strategy did not engage: %+v", exec.Stats)
				}
				exec.Release()
			})
		}
	}
}

func TestCorrelatedExistsLeavesOrdinaryBuilderJoinPlanUnchanged(t *testing.T) {
	q := Select(Path("id")).Join(JoinOn("ce_inner", "k", "k"))
	p, err := q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	if len(p.joins) != 1 || p.joins[0].anti ||
		p.joins[0].origin != joinOriginAuthored {
		t.Fatalf("ordinary join metadata changed: %+v", p.joins)
	}
	if p.where == nil || p.where.kind != predInBound || p.where.slot != 0 {
		t.Fatalf("ordinary join predicate = %+v, want predInBound slot zero", p.where)
	}
}

func TestSQLCorrelatedExistsUnsupportedProofShapesArePositioned(t *testing.T) {
	tests := []struct {
		source string
		at     string
	}{
		{
			`SELECT o.id FROM ce_outer o WHERE o.enabled = TRUE OR EXISTS (` +
				`SELECT 1 FROM ce_inner i WHERE i.k = o.k)`,
			"EXISTS",
		},
		{
			`SELECT o.id FROM ce_outer o WHERE EXISTS (` +
				`SELECT 1 FROM ce_inner i WHERE i.k = o.k AND i.id = o.id)`,
			"i.id",
		},
		{
			`SELECT o.id FROM ce_outer o WHERE EXISTS (` +
				`SELECT 1 FROM ce_inner i WHERE i.k > o.k)`,
			"i.k",
		},
	}
	for _, test := range tests {
		_, err := PrepareStatement(test.source)
		var unsupported *sqlast.FeatureNotSupportedError
		if !errors.As(err, &unsupported) || unsupported.Pos != strings.Index(test.source, test.at) {
			t.Fatalf("%s: error=%T %v position=%d want=%d",
				test.source, err, err, unsupportedPosition(unsupported), strings.Index(test.source, test.at))
		}
	}
}

func unsupportedPosition(err *sqlast.FeatureNotSupportedError) int {
	if err == nil {
		return -1
	}
	return err.Pos
}

func TestSQLCorrelatedExistsWarmExecutionIsAllocationFree(t *testing.T) {
	db := correlatedExistsHeapDatabase(
		t, correlatedExistsOuterDocs, correlatedExistsInnerDocs, true,
	)
	statement, err := PrepareStatement(correlatedExistsSQL(false, true))
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1}}
	defer exec.Release()
	active := true
	args := []any{&active, &active}
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			panic(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed correlated EXISTS allocations = %.2f, want 0", got)
	}
}
