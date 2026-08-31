package query

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

var markOuterDocs = []string{
	`{"id":"ax-hit","tenant":"a","bucket":"x","wanted":1}`,
	`{"id":"ax-null-poison","tenant":"a","bucket":"x","wanted":2}`,
	`{"id":"ay-hit","tenant":"a","bucket":"y","wanted":"v"}`,
	`{"id":"empty-null-probe","tenant":"b","bucket":"x","wanted":null}`,
	`{"id":"empty-missing-probe","tenant":"b","bucket":"x"}`,
	`{"id":"null-key","tenant":null,"bucket":"x","wanted":null}`,
	`{"id":"missing-key","bucket":"x","wanted":9}`,
}

var markInnerDocs = []string{
	`{"tenant":"a","bucket":"x","value":1,"active":true}`,
	`{"tenant":"a","bucket":"x","value":1.0,"active":true}`,
	`{"tenant":"a","bucket":"x","value":null,"active":true}`,
	`{"tenant":"a","bucket":"x","value":2,"active":false}`,
	`{"tenant":"a","bucket":"y","value":"v","active":true}`,
	`{"tenant":null,"bucket":"x","value":9,"active":true}`,
}

type randomMarkRow struct {
	key, value int
	active     bool
}

func markHeapDatabase(t testing.TB, outerDocs, innerDocs []string) *store.Database {
	t.Helper()
	db := &store.Database{}
	outer, err := db.CreateCollection("mark_outer", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put(fmt.Sprintf("o%03d", i), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("mark_inner", store.Options{ChunkDocuments: 2})
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

func markDurableDatabase(t testing.TB, outerDocs, innerDocs []string) *durable.Database {
	t.Helper()
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	outer, err := db.CreateCollection("mark_outer", durableJoinOptions())
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range outerDocs {
		if _, err := outer.Put([]byte(fmt.Sprintf("o%03d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("mark_inner", durableJoinOptions())
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

func markSQL(predicate string) string {
	return `SELECT o.id FROM mark_outer o WHERE ` + predicate + ` ORDER BY o.id`
}

const markChild = `(SELECT i.value FROM mark_inner i WHERE ` +
	`i.tenant = o.tenant AND i.bucket = o.bucket AND i.active = TRUE)`

func markRunIDs(t testing.TB, stmt *Statement, src Source, exec *Exec) []string {
	t.Helper()
	cursor, err := stmt.RunInto(exec, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatalf("id = %s, want string", cursor.Cell(0).JSON())
		}
		ids = append(ids, id)
	}
	return ids
}

func TestGroupedCorrelatedMarkNullTruthTablesHeapAndDurable(t *testing.T) {
	tests := []struct {
		name      string
		predicate string
		want      []string
	}{
		{
			"composite exists",
			`EXISTS ` + markChild,
			[]string{"ax-hit", "ax-null-poison", "ay-hit"},
		},
		{
			"composite not exists",
			`NOT EXISTS ` + markChild,
			[]string{"empty-missing-probe", "empty-null-probe", "missing-key", "null-key"},
		},
		{
			"in",
			`o.wanted IN ` + markChild,
			[]string{"ax-hit", "ay-hit"},
		},
		{
			"not in",
			`o.wanted NOT IN ` + markChild,
			[]string{"empty-missing-probe", "empty-null-probe", "missing-key", "null-key"},
		},
	}

	heap := markHeapDatabase(t, markOuterDocs, markInnerDocs)
	heapCatalog := heap.Snapshot()
	durableDB := markDurableDatabase(t, markOuterDocs, markInnerDocs)
	fileCatalog, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fileCatalog.Close() }()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := PrepareStatement(markSQL(test.predicate))
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Release()
			for _, backend := range []struct {
				name string
				src  Source
			}{
				{"heap", FromDatabase(heapCatalog, "mark_outer")},
				{"durable", FromFileDatabase(fileCatalog, "mark_outer")},
			} {
				t.Run(backend.name, func(t *testing.T) {
					var exec Exec
					got := markRunIDs(t, stmt, backend.src, &exec)
					if !slices.Equal(got, test.want) {
						t.Fatalf("ids = %v, want %v", got, test.want)
					}
					plan, compileErr := stmt.q.compiled()
					if compileErr != nil || len(plan.marks) != 1 || plan.marks[0].slot != 0 {
						t.Fatalf("compiled marks/error = %+v/%v, want one fixed slot", plan, compileErr)
					}
					exec.Release()
				})
			}
		})
	}
}

func TestGroupedCorrelatedMarkRejectsContainerEquality(t *testing.T) {
	outer := []string{
		`{"id":"object","tenant":"o","bucket":"x","wanted":{"b":2,"a":{"z":1,"y":2}}}`,
		`{"id":"array","tenant":"a","bucket":"x","wanted":[{"b":2,"a":1},3]}`,
		`{"id":"array-order","tenant":"a","bucket":"x","wanted":[3,{"a":1,"b":2}]}`,
		`{"id":"object-value","tenant":"o","bucket":"x","wanted":{"a":{"y":9,"z":1},"b":2}}`,
	}
	inner := []string{
		`{"tenant":"o","bucket":"x","value":{"a":{"y":2,"z":1},"b":2},"active":true}`,
		`{"tenant":"a","bucket":"x","value":[{"a":1,"b":2},3],"active":true}`,
	}
	heap := markHeapDatabase(t, outer, inner)
	durableDB := markDurableDatabase(t, outer, inner)
	files, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	stmt, err := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	for _, backend := range []struct {
		name string
		src  Source
	}{
		{"heap", FromDatabase(heap.Snapshot(), "mark_outer")},
		{"durable", FromFileDatabase(files, "mark_outer")},
	} {
		t.Run(backend.name, func(t *testing.T) {
			var exec Exec
			_, runErr := stmt.RunInto(&exec, backend.src, nil)
			var undefined *sqlast.UndefinedOperatorError
			if !errors.As(runErr, &undefined) || undefined.Left != "json" ||
				undefined.Operator != "=" || undefined.Right != "json" {
				t.Fatalf("container comparison error = %T %v", runErr, runErr)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("failed container comparison published %d rows", exec.Result.RowCount)
			}
		})
	}
}

func TestGroupedCorrelatedMarkSQLComparisonDomainsHeapAndDurable(t *testing.T) {
	tests := []struct {
		name      string
		predicate string
		outer     string
		inner     string
		marker    string
		left      string
		op        string
		right     string
	}{
		{
			name:      "correlation inner-left",
			predicate: `EXISTS ` + markChild,
			outer:     `{"id":"bad","tenant":1,"bucket":"x","wanted":1}`,
			inner:     `{"tenant":"1","bucket":"x","value":1,"active":true}`,
			marker:    "i.tenant = o.tenant", left: "text", op: "=", right: "numeric",
		},
		{
			name: "correlation outer-left",
			predicate: `EXISTS (SELECT i.value FROM mark_inner i WHERE ` +
				`o.tenant = i.tenant AND i.bucket = o.bucket AND i.active = TRUE)`,
			outer:  `{"id":"bad","tenant":1,"bucket":"x","wanted":1}`,
			inner:  `{"tenant":"1","bucket":"x","value":1,"active":true}`,
			marker: "o.tenant = i.tenant", left: "numeric", op: "=", right: "text",
		},
		{
			name:      "correlation container",
			predicate: `EXISTS ` + markChild,
			outer:     `{"id":"bad","tenant":{"a":1},"bucket":"x","wanted":1}`,
			inner:     `{"tenant":{"a":1},"bucket":"x","value":1,"active":true}`,
			marker:    "i.tenant = o.tenant", left: "json", op: "=", right: "json",
		},
		{
			name:      "IN projection",
			predicate: `o.wanted IN ` + markChild,
			outer:     `{"id":"bad","tenant":"a","bucket":"x","wanted":1}`,
			inner:     `{"tenant":"a","bucket":"x","value":"1","active":true}`,
			marker:    " IN ", left: "numeric", op: "=", right: "text",
		},
		{
			name:      "scalar projection",
			predicate: `o.wanted < ` + markChild,
			outer:     `{"id":"bad","tenant":"a","bucket":"x","wanted":1}`,
			inner:     `{"tenant":"a","bucket":"x","value":"1","active":true}`,
			marker:    " < ", left: "numeric", op: "<", right: "text",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := markSQL(test.predicate)
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			heap := markHeapDatabase(t, []string{test.outer}, []string{test.inner})
			durableDB := markDurableDatabase(t, []string{test.outer}, []string{test.inner})
			files, err := durableDB.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = files.Close() }()
			for _, backend := range []struct {
				name string
				src  Source
			}{
				{"heap", FromDatabase(heap.Snapshot(), "mark_outer")},
				{"durable", FromFileDatabase(files, "mark_outer")},
			} {
				t.Run(backend.name, func(t *testing.T) {
					var exec Exec
					exec.Options.Workers = 4
					_, runErr := statement.RunInto(&exec, backend.src, nil)
					var undefined *sqlast.UndefinedOperatorError
					if !errors.As(runErr, &undefined) || undefined.Unpositioned ||
						undefined.Left != test.left || undefined.Operator != test.op ||
						undefined.Right != test.right {
						t.Fatalf("error = %T %+v", runErr, undefined)
					}
					wantPos := strings.Index(source, test.marker)
					if strings.HasPrefix(test.marker, " ") {
						wantPos++
					} else {
						wantPos += strings.Index(test.marker, "=")
					}
					if undefined.Pos != wantPos {
						t.Fatalf("position = %d, want %d in %q", undefined.Pos, wantPos, source)
					}
					if exec.Result.RowCount != 0 {
						t.Fatalf("failed comparison published %d rows", exec.Result.RowCount)
					}
				})
			}
		})
	}
}

func TestGroupedCorrelatedMarkValidatesLiveKeysAfterNullHeapAndDurable(t *testing.T) {
	outer := []string{`{"id":"bad","tenant":null,"bucket":1,"wanted":1}`}
	inner := []string{`{"tenant":"x","bucket":"1","value":"1"}`}
	child := `(SELECT i.value FROM mark_inner i WHERE ` +
		`i.tenant = o.tenant AND i.bucket = o.bucket)`
	heap := markHeapDatabase(t, outer, inner)
	durableDB := markDurableDatabase(t, outer, inner)
	files, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	for _, predicate := range []string{
		`EXISTS ` + child,
		`o.wanted IN ` + child,
		`o.wanted = ` + child,
	} {
		t.Run(predicate, func(t *testing.T) {
			source := markSQL(predicate)
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			for _, backend := range []struct {
				name string
				src  Source
			}{
				{"heap", FromDatabase(heap.Snapshot(), "mark_outer")},
				{"durable", FromFileDatabase(files, "mark_outer")},
			} {
				t.Run(backend.name, func(t *testing.T) {
					var exec Exec
					_, runErr := statement.RunInto(&exec, backend.src, nil)
					var undefined *sqlast.UndefinedOperatorError
					if !errors.As(runErr, &undefined) || undefined.Left != "text" ||
						undefined.Operator != "=" || undefined.Right != "numeric" ||
						undefined.Pos != strings.LastIndex(source, "=") {
						t.Fatalf("error = %T %+v", runErr, undefined)
					}
					if exec.Result.RowCount != 0 {
						t.Fatalf("failed comparison published %d rows", exec.Result.RowCount)
					}
				})
			}
		})
	}
}

func TestGroupedCorrelatedScalarOperatorResolutionPrecedesCardinality(t *testing.T) {
	child := `(SELECT i.value FROM mark_inner i WHERE ` +
		`i.tenant = o.tenant AND i.bucket = o.bucket)`
	tests := []struct {
		name      string
		outer     string
		inner     []string
		undefined bool
	}{
		{
			name:      "undefined operator before cardinality",
			outer:     `{"id":"bad","tenant":"a","bucket":"x","wanted":1}`,
			inner:     []string{`{"tenant":"a","bucket":"x","value":"1"}`, `{"tenant":"a","bucket":"x","value":"2"}`},
			undefined: true,
		},
		{
			name:  "compatible cardinality before null",
			outer: `{"id":"bad","tenant":"a","bucket":"x","wanted":null}`,
			inner: []string{`{"tenant":"a","bucket":"x","value":1}`, `{"tenant":"a","bucket":"x","value":2}`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			heap := markHeapDatabase(t, []string{test.outer}, test.inner)
			durableDB := markDurableDatabase(t, []string{test.outer}, test.inner)
			files, err := durableDB.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = files.Close() }()
			source := markSQL(`o.wanted = ` + child)
			statement, err := PrepareStatement(source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			for _, backend := range []struct {
				name string
				src  Source
			}{
				{"heap", FromDatabase(heap.Snapshot(), "mark_outer")},
				{"durable", FromFileDatabase(files, "mark_outer")},
			} {
				t.Run(backend.name, func(t *testing.T) {
					var exec Exec
					_, runErr := statement.RunInto(&exec, backend.src, nil)
					if test.undefined {
						var undefined *sqlast.UndefinedOperatorError
						if !errors.As(runErr, &undefined) || undefined.Left != "numeric" ||
							undefined.Operator != "=" || undefined.Right != "text" ||
							undefined.Pos != strings.Index(source, "=") {
							t.Fatalf("error = %T %+v", runErr, undefined)
						}
					} else {
						var cardinality *CardinalityViolationError
						if !errors.As(runErr, &cardinality) {
							t.Fatalf("error = %T %v, want cardinality", runErr, runErr)
						}
					}
					if exec.Result.RowCount != 0 {
						t.Fatalf("failed scalar comparison published %d rows", exec.Result.RowCount)
					}
				})
			}
		})
	}
}

func TestGroupedCorrelatedMarkMultiworkerDurableFirstComparisonError(t *testing.T) {
	outer := make([]string, 128)
	for i := range outer {
		outer[i] = fmt.Sprintf(
			`{"id":"o%03d","tenant":%d,"bucket":"x","wanted":1}`,
			i, i,
		)
	}
	inner := []string{
		`{"tenant":"0","bucket":"x","value":1,"active":true}`,
	}
	durableDB := markDurableDatabase(t, outer, inner)
	files, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	source := markSQL(`EXISTS ` + markChild)
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	for run := 0; run < 8; run++ {
		var exec Exec
		exec.Options.Workers = 4
		_, runErr := statement.RunInto(
			&exec, FromFileDatabase(files, "mark_outer"), nil,
		)
		var undefined *sqlast.UndefinedOperatorError
		if !errors.As(runErr, &undefined) || undefined.Left != "text" ||
			undefined.Operator != "=" || undefined.Right != "numeric" ||
			undefined.Pos != strings.Index(source, "i.tenant = o.tenant")+
				strings.Index("i.tenant = o.tenant", "=") {
			t.Fatalf("run %d error = %T %+v", run, runErr, undefined)
		}
		if exec.Result.RowCount != 0 {
			t.Fatalf("run %d published %d rows", run, exec.Result.RowCount)
		}
		exec.Release()
	}
}

func TestGroupedCorrelatedScalarCardinalityIsRowCountAndProbeLazy(t *testing.T) {
	inner := []string{
		`{"tenant":"one","bucket":"x","value":5,"active":true}`,
		`{"tenant":"duplicate","bucket":"x","value":5,"active":true}`,
		`{"tenant":"duplicate","bucket":"x","value":5.0,"active":true}`,
		`{"tenant":"nulls","bucket":"x","value":null,"active":true}`,
		`{"tenant":"nulls","bucket":"x","active":true}`,
	}
	// Multi-row groups that no outer row probes must not fail the statement.
	db := markHeapDatabase(t, []string{
		`{"id":"one","tenant":"one","bucket":"x","wanted":6}`,
		`{"id":"empty","tenant":"empty","bucket":"x","wanted":6}`,
	}, inner)
	stmt, err := PrepareStatement(markSQL(`o.wanted > ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	if got := markRunIDs(t, stmt, FromDatabase(db.Snapshot(), "mark_outer"), &exec); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("unprobed multi-row groups: ids = %v, want [one]", got)
	}

	for _, test := range []struct {
		name  string
		outer string
	}{
		{"duplicate values", `{"id":"bad","tenant":"duplicate","bucket":"x","wanted":null}`},
		{"two null projections", `{"id":"bad","tenant":"nulls","bucket":"x","wanted":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := markHeapDatabase(t, []string{test.outer}, inner)
			_, err := stmt.RunInto(&exec, FromDatabase(bad.Snapshot(), "mark_outer"), nil)
			var violation *CardinalityViolationError
			if !errors.As(err, &violation) || !errors.Is(err, ErrCardinalityViolation) {
				t.Fatalf("error = %T %v, want cardinality violation", err, err)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("failed result retained %d rows", exec.Result.RowCount)
			}
		})
	}
}

func TestGroupedCorrelatedMarkCatalogedEmptyOuterValidatesDependency(t *testing.T) {
	db := &store.Database{}
	if _, err := db.CreateCollection("mark_outer", store.Options{}); err != nil {
		t.Fatal(err)
	}
	stmt, err := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	_, err = stmt.RunInto(&exec, FromDatabase(db.Snapshot(), "mark_outer"), nil)
	if err == nil || exec.Result.RowCount != 0 {
		t.Fatalf("missing empty-outer dependency error/result = %v/%d", err, exec.Result.RowCount)
	}
	if _, createErr := db.CreateCollection("mark_inner", store.Options{}); createErr != nil {
		t.Fatal(createErr)
	}
	cursor, err := stmt.RunInto(&exec, FromDatabase(db.Snapshot(), "mark_outer"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next() || exec.Result.RowCount != 0 {
		t.Fatal("cataloged-empty outer produced a row")
	}
}

func TestGroupedCorrelatedMarkCatalogedEmptyInnerAndSelfCorrelation(t *testing.T) {
	outerDocs := []string{
		`{"id":"a1","tenant":"a","bucket":"x","wanted":null,"active":true}`,
		`{"id":"a2","tenant":"a","bucket":"x","wanted":2,"active":false}`,
		`{"id":"null","tenant":null,"bucket":"x","wanted":null,"active":true}`,
	}
	durableDB := markDurableDatabase(t, outerDocs, nil)
	files, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	stmt, err := PrepareStatement(markSQL(`o.wanted NOT IN ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var exec Exec
	if got := markRunIDs(t, stmt, FromFileDatabase(files, "mark_outer"), &exec); !slices.Equal(got, []string{"a1", "a2", "null"}) {
		t.Fatalf("cataloged-empty inner ids = %v", got)
	}

	heap := markHeapDatabase(t, outerDocs, nil)
	selfSQL := `SELECT o.id FROM mark_outer o WHERE EXISTS (` +
		`SELECT 1 FROM mark_outer i WHERE i.tenant = o.tenant AND ` +
		`i.bucket = o.bucket AND i.active = TRUE) ORDER BY o.id`
	self, err := PrepareStatement(selfSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer self.Release()
	if got := markRunIDs(t, self, FromDatabase(heap.Snapshot(), "mark_outer"), &exec); !slices.Equal(got, []string{"a1", "a2"}) {
		t.Fatalf("self-correlated ids = %v, want [a1 a2]", got)
	}
}

func TestGroupedCorrelatedMarkWorkBudgetExactBoundary(t *testing.T) {
	key := scalar{kind: kindString, sval: strings.Repeat("k", 257)}
	value := scalar{kind: kindString, sval: strings.Repeat("v", 509)}
	cols := [][]scalar{{key}, {value}}
	plan := planMark{
		innerKeys: []int{0}, value: 1, kind: correlatedMarkIn,
	}
	build := func(limit int64) (*markBinding, *heapWorkBudget, error) {
		binding := &markBinding{plan: plan}
		binding.ensureSeed()
		var budget heapWorkBudget
		budget.begin(limit)
		err := binding.addInnerRow(cols, 0, &budget)
		return binding, &budget, err
	}
	_, measured, err := build(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	required := measured.used.Load()
	short, _, err := build(required - 1)
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("one-byte-short error = %T %v, want WorkBudgetError", err, err)
	}
	if len(short.values) != 0 {
		t.Fatalf("rejected build published %d membership values", len(short.values))
	}
	exact, _, err := build(required)
	if err != nil || len(exact.groups) != 1 || len(exact.values) != 1 {
		t.Fatalf("exact boundary build = groups=%d values=%d err=%v",
			len(exact.groups), len(exact.values), err)
	}
}

func TestGroupedCorrelatedMarkInitialDirectoryReservationCoversCapacity(t *testing.T) {
	var budget heapWorkBudget
	budget.begin(1 << 20)
	binding := markBinding{}
	if err := binding.growGroups(1, &budget); err != nil {
		t.Fatal(err)
	}
	groupBytes := int64(cap(binding.groupBuckets)) * int64(unsafe.Sizeof(int32(0)))
	if binding.groupReserved < groupBytes {
		t.Fatalf("group reservation = %d, physical capacity = %d", binding.groupReserved, groupBytes)
	}
	if err := binding.growValues(1, &budget); err != nil {
		t.Fatal(err)
	}
	valueBytes := int64(cap(binding.valueBuckets)) * int64(unsafe.Sizeof(int32(0)))
	if binding.valueReserved < valueBytes {
		t.Fatalf("value reservation = %d, physical capacity = %d", binding.valueReserved, valueBytes)
	}
}

func TestGroupedCorrelatedMarkRetainedArenaRebasesEveryView(t *testing.T) {
	check := func(t *testing.T, binding *markBinding) {
		t.Helper()
		for i := range binding.keys {
			if !markScalarPointsIntoArena(binding.keys[i], binding.text) {
				t.Fatalf("key %d views an abandoned arena generation", i)
			}
		}
		for i := range binding.values {
			if !markScalarPointsIntoArena(binding.values[i].value, binding.text) {
				t.Fatalf("value %d views an abandoned arena generation", i)
			}
		}
		for i := range binding.groups {
			if !markScalarPointsIntoArena(binding.groups[i].first, binding.text) {
				t.Fatalf("scalar group %d views an abandoned arena generation", i)
			}
		}
	}

	for _, mode := range []correlatedMarkKind{correlatedMarkIn, correlatedMarkScalar} {
		binding := markBinding{plan: planMark{
			innerKeys: []int{0}, value: 1, kind: mode,
		}}
		binding.ensureSeed()
		var budget heapWorkBudget
		budget.begin(1 << 30)
		for i := range 96 {
			key := fmt.Sprintf("key-%03d-%s", i, strings.Repeat("k", i%23+1))
			value := fmt.Sprintf("value-%03d-%s", i, strings.Repeat("v", i%29+1))
			cols := [][]scalar{
				{{kind: kindString, sval: key}},
				{{kind: kindString, sval: value}},
			}
			if err := binding.addInnerRow(cols, 0, &budget); err != nil {
				t.Fatalf("mode=%d row=%d: %v", mode, i, err)
			}
		}
		check(t, &binding)
	}
}

func markScalarPointsIntoArena(value scalar, arena []byte) bool {
	n := markScalarPayloadBytes(value)
	if n == 0 {
		return true
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(arena)))
	end := base + uintptr(len(arena))
	var address uintptr
	switch value.kind {
	case kindNumber:
		address = uintptr(unsafe.Pointer(unsafe.SliceData(value.num)))
	case kindString:
		address = uintptr(unsafe.Pointer(unsafe.StringData(value.sval)))
	case kindContainer:
		address = uintptr(unsafe.Pointer(unsafe.SliceData(value.raw)))
	default:
		return true
	}
	return address >= base && address+uintptr(n) <= end
}

func TestGroupedCorrelatedMarkRandomizedDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	statements := make([]*Statement, 0, 4)
	for _, predicate := range []string{
		`EXISTS ` + markChild,
		`NOT EXISTS ` + markChild,
		`o.wanted IN ` + markChild,
		`o.wanted NOT IN ` + markChild,
	} {
		stmt, err := PrepareStatement(markSQL(predicate))
		if err != nil {
			t.Fatal(err)
		}
		statements = append(statements, stmt)
		defer stmt.Release()
	}
	for iteration := range 32 {
		outerRows := make([]randomMarkRow, rng.Intn(8)+1)
		innerRows := make([]randomMarkRow, rng.Intn(10))
		outerDocs := make([]string, len(outerRows))
		innerDocs := make([]string, len(innerRows))
		for i := range outerRows {
			outerRows[i] = randomMarkRow{key: rng.Intn(4) - 1, value: rng.Intn(4) - 1}
			outerDocs[i] = randomMarkOuterDoc(i, outerRows[i].key, outerRows[i].value)
		}
		for i := range innerRows {
			innerRows[i] = randomMarkRow{key: rng.Intn(4) - 1, value: rng.Intn(4) - 1, active: rng.Intn(3) != 0}
			innerDocs[i] = randomMarkInnerDoc(innerRows[i].key, innerRows[i].value, innerRows[i].active)
		}
		db := markHeapDatabase(t, outerDocs, innerDocs)
		source := FromDatabase(db.Snapshot(), "mark_outer")
		for mode, stmt := range statements {
			var exec Exec
			got := markRunIDs(t, stmt, source, &exec)
			want := randomMarkOracle(outerRows, innerRows, mode)
			if !slices.Equal(got, want) {
				t.Fatalf("iteration=%d mode=%d got=%v want=%v outer=%v inner=%v",
					iteration, mode, got, want, outerRows, innerRows)
			}
		}
	}
}

func randomMarkOuterDoc(id, key, value int) string {
	doc := fmt.Sprintf(`{"id":"r%02d","bucket":"x"`, id)
	if key < 0 {
		doc += `,"tenant":null`
	} else {
		doc += fmt.Sprintf(`,"tenant":%d`, key)
	}
	if value < 0 {
		doc += `,"wanted":null`
	} else {
		doc += fmt.Sprintf(`,"wanted":%d`, value)
	}
	return doc + `}`
}

func randomMarkInnerDoc(key, value int, active bool) string {
	doc := `{"bucket":"x"`
	if key < 0 {
		doc += `,"tenant":null`
	} else {
		doc += fmt.Sprintf(`,"tenant":%d`, key)
	}
	if value < 0 {
		doc += `,"value":null`
	} else {
		doc += fmt.Sprintf(`,"value":%d`, value)
	}
	return fmt.Sprintf(`%s,"active":%t}`, doc, active)
}

func randomMarkOracle(outer, inner []randomMarkRow, mode int) []string {
	ids := make([]string, 0, len(outer))
	for i, probe := range outer {
		rows, matched, hasNull := 0, false, false
		if probe.key >= 0 {
			for _, candidate := range inner {
				if !candidate.active || candidate.key < 0 || candidate.key != probe.key {
					continue
				}
				rows++
				if candidate.value < 0 {
					hasNull = true
				} else if probe.value >= 0 && candidate.value == probe.value {
					matched = true
				}
			}
		}
		keep := false
		switch mode {
		case 0:
			keep = rows != 0
		case 1:
			keep = rows == 0
		case 2:
			keep = matched
		case 3:
			keep = rows == 0 || probe.value >= 0 && !matched && !hasNull
		}
		if keep {
			ids = append(ids, fmt.Sprintf("r%02d", i))
		}
	}
	slices.Sort(ids)
	return ids
}

func TestGroupedCorrelatedMarkConcurrentPreparedExecution(t *testing.T) {
	db := markHeapDatabase(t, markOuterDocs, markInnerDocs)
	source := FromDatabase(db.Snapshot(), "mark_outer")
	const goroutines = 8
	var wait sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wait.Add(1)
		go func() {
			defer wait.Done()
			stmt, prepareErr := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
			if prepareErr != nil {
				errs <- prepareErr
				return
			}
			defer stmt.Release()
			var exec Exec
			for range 20 {
				cursor, runErr := stmt.RunInto(&exec, source, nil)
				if runErr != nil {
					errs <- runErr
					return
				}
				for cursor.Next() {
				}
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestGroupedCorrelatedMarkCancellationRecovery(t *testing.T) {
	db := markHeapDatabase(t, markOuterDocs, markInnerDocs)
	stmt, err := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	var cancel CancelFlag
	cancel.Cancel()
	exec := Exec{Options: ExecOptions{Cancel: &cancel}}
	if _, err := stmt.RunInto(&exec, FromDatabase(db.Snapshot(), "mark_outer"), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled mark error = %v, want ErrCanceled", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled mark exposed %d rows", exec.Result.RowCount)
	}
	cancel.Reset()
	if got := markRunIDs(t, stmt, FromDatabase(db.Snapshot(), "mark_outer"), &exec); !slices.Equal(got, []string{"ax-hit", "ay-hit"}) {
		t.Fatalf("recovered mark ids = %v", got)
	}
}

func TestGroupedCorrelatedMarkWarmHeapAllocations(t *testing.T) {
	stmt, err := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Release()
	heap := markHeapDatabase(t, markOuterDocs, markInnerDocs)
	durableDB := markDurableDatabase(t, markOuterDocs, markInnerDocs)
	files, err := durableDB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	for _, backend := range []struct {
		name string
		src  Source
	}{
		{"heap", FromDatabase(heap.Snapshot(), "mark_outer")},
		{"durable", FromFileDatabase(files, "mark_outer")},
	} {
		t.Run(backend.name, func(t *testing.T) {
			var exec Exec
			if _, err := stmt.RunInto(&exec, backend.src, nil); err != nil {
				t.Fatal(err)
			}
			allocs := testing.AllocsPerRun(100, func() {
				if _, runErr := stmt.RunInto(&exec, backend.src, nil); runErr != nil {
					panic(runErr)
				}
			})
			if allocs != 0 {
				t.Fatalf("warm grouped mark allocated %.2f times, want 0", allocs)
			}
			exec.Release()
		})
	}
}

func BenchmarkGroupedCorrelatedMark(b *testing.B) {
	db := markHeapDatabase(b, markOuterDocs, markInnerDocs)
	stmt, err := PrepareStatement(markSQL(`o.wanted IN ` + markChild))
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Release()
	source := FromDatabase(db.Snapshot(), "mark_outer")
	var exec Exec
	if _, err := stmt.RunInto(&exec, source, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := stmt.RunInto(&exec, source, nil); err != nil {
			b.Fatal(err)
		}
	}
}
