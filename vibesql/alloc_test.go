package vibesql

import (
	"database/sql"
	"fmt"

	"testing"
)

// What the driver allocates, and what it cannot help allocating.
//
// driver.Rows.Next fills a []driver.Value, and driver.Value is any. Putting a
// []byte or an int64 into an interface allocates a box, because an interface
// word cannot hold three words or an arbitrary eight-byte integer, and an
// interface value is immutable so the box cannot be reused across rows. Every
// database/sql driver pays exactly that, and no driver can avoid it: it is the
// signature, not the implementation.
//
// What this package can promise is that it adds nothing else per row. The test
// below measures that as a slope rather than an intercept — the per-row cost is
// the difference between a hundred rows and two hundred, which is immune to
// whatever constant database/sql spends setting a query up, and to that constant
// changing between Go releases.

// allocsPerRowFixture builds a collection of n documents with two string
// fields, so every projected cell boxes identically and the arithmetic below
// has no special cases.
func allocsPerRowFixture(t *testing.T, name string, n int) *sql.DB {
	t.Helper()
	docs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, fmt.Sprintf(`{"a":"value-%d","b":"other-%d"}`, i, i))
	}
	return memoryDatabase(t, map[string][]string{name: docs})
}

// TestDriverPreparedRowAllocSlope asserts a prepared statement allocates
// exactly one interface box per projected cell and nothing else per row.
//
// The destinations are sql.RawBytes, the one kind database/sql does not copy
// into, so what is left is what reaches the driver. Two corpora of different
// sizes are measured and the difference divided by the extra rows: a driver
// that copied a cell, formatted a number, or grew a buffer per row would show
// up as a slope above the column count, and one that leaked a per-row object
// would show up however large the setup constant happened to be.
func TestDriverPreparedRowAllocSlope(t *testing.T) {
	const (
		small   = 100
		large   = 300
		columns = 2
	)
	measure := func(rows int) float64 {
		db := allocsPerRowFixture(t, "docs", rows)
		db.SetMaxOpenConns(1)
		stmt, err := db.Prepare(`SELECT a, b FROM docs WHERE a IS NOT MISSING`)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		drainOnce := func() {
			result, err := stmt.Query()
			if err != nil {
				t.Fatal(err)
			}
			cells := make([]sql.RawBytes, columns)
			into := make([]any, columns)
			for i := range cells {
				into[i] = &cells[i]
			}
			seen := 0
			for result.Next() {
				if err := result.Scan(into...); err != nil {
					t.Fatal(err)
				}
				seen++
			}
			if err := result.Close(); err != nil {
				t.Fatal(err)
			}
			if seen != rows {
				t.Fatalf("read %d rows, want %d", seen, rows)
			}
		}
		drainOnce() // warm the connection's Exec and the statement's arenas
		return testing.AllocsPerRun(20, drainOnce)
	}
	slope := (measure(large) - measure(small)) / float64(large-small)
	if slope > columns+0.05 {
		t.Fatalf("a prepared statement allocates %.3f times per row over %d columns; "+
			"one interface box per cell is the driver.Value floor and anything above it "+
			"is this package's", slope, columns)
	}
	t.Logf("prepared: %.3f allocations per row over %d columns", slope, columns)
}

// TestDriverPreparedExecutionAllocIsBounded asserts the per-execution cost of a
// prepared statement does not grow, which is what "the plan is reused" means in
// allocation terms.
//
// The bound is generous on purpose. Most of it is database/sql's own — the
// pooled connection handoff, the *sql.Rows, the argument conversion — and this
// test exists to catch a regression that made it grow, not to pin a number that
// belongs to the standard library. The interesting comparison is against a
// statement with no placeholder, which re-lowers nothing at all.
func TestDriverPreparedExecutionAllocIsBounded(t *testing.T) {
	db := allocsPerRowFixture(t, "docs", 200)
	db.SetMaxOpenConns(1)
	for _, tc := range []struct {
		name  string
		src   string
		args  []any
		bound float64
	}{
		{"literal", `SELECT a FROM docs WHERE a = 'value-7'`, nil, 20},
		{"placeholder", `SELECT a FROM docs WHERE a = ?`, []any{"value-7"}, 24},
	} {
		stmt, err := db.Prepare(tc.src)
		if err != nil {
			t.Fatal(err)
		}
		run := func() {
			result, err := stmt.Query(tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			var cell sql.RawBytes
			for result.Next() {
				if err := result.Scan(&cell); err != nil {
					t.Fatal(err)
				}
			}
			if err := result.Close(); err != nil {
				t.Fatal(err)
			}
		}
		run()
		got := testing.AllocsPerRun(50, run)
		if got > tc.bound {
			t.Fatalf("%s: %.0f allocations per execution, want at most %.0f",
				tc.name, got, tc.bound)
		}
		t.Logf("%s: %.0f allocations per execution", tc.name, got)
		_ = stmt.Close()
	}
}

// TestDriverAdHocAllocsExceedPrepared is the honesty check on the
// package documentation. It asserts the direction of the claim rather than a
// magnitude: an unprepared query parses and compiles on every call, so it must
// measurably cost more, and a change that made preparation pointless should
// fail here rather than quietly make the documentation wrong.
func TestDriverAdHocAllocsExceedPrepared(t *testing.T) {
	db := allocsPerRowFixture(t, "docs", 50)
	db.SetMaxOpenConns(1)
	const src = `SELECT a FROM docs WHERE a = ?`
	stmt, err := db.Prepare(src)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	consume := func(result *sql.Rows, err error) {
		if err != nil {
			t.Fatal(err)
		}
		var cell sql.RawBytes
		for result.Next() {
			if err := result.Scan(&cell); err != nil {
				t.Fatal(err)
			}
		}
		if err := result.Close(); err != nil {
			t.Fatal(err)
		}
	}
	prepared := func() { consume(stmt.Query("value-7")) }
	adhoc := func() { consume(db.Query(src, "value-7")) }
	prepared()
	adhoc()
	preparedAllocs := testing.AllocsPerRun(50, prepared)
	adhocAllocs := testing.AllocsPerRun(50, adhoc)
	if adhocAllocs <= preparedAllocs {
		t.Fatalf("ad-hoc allocated %.0f and prepared %.0f; the parse has to show up somewhere",
			adhocAllocs, preparedAllocs)
	}
	t.Logf("prepared %.0f allocations, ad-hoc %.0f: the parse and compile costs %.0f",
		preparedAllocs, adhocAllocs, adhocAllocs-preparedAllocs)
}
