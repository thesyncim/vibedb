package sql

import "testing"

// The allocation contract.
//
// Parsing is on the prepare path, not the row path, so it does not have to be
// free — but a driver that prepares statements per request is a normal shape,
// and the repository's standard for reusable storage is zero steady-state
// allocation. These tests pin that: after one warm-up, a Parser writing into
// its own recycled arenas must allocate nothing at all.
//
// They also guard something a benchmark would not notice. The parser is full
// of deferred closures — the depth counter, the scratch-stack unwind — and a
// closure that stops being open-coded starts allocating once per predicate
// node. That regression is invisible in a wall-clock number and obvious here.

// TestWarmParseIsAllocationFree is the headline contract.
func TestWarmParseIsAllocationFree(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{"simple", benchSimple},
		{"filtered", benchFiltered},
		{"join", benchJoin},
		{"left join", benchLeftJoin},
		{"subqueries", `SELECT id FROM orders WHERE customer IN (` +
			`SELECT id FROM customers WHERE tier = ?) AND ` +
			`EXISTS (SELECT 1 FROM regions WHERE active = TRUE)`},
		{"grouped aggregate", benchGrouped},
		{"containment and membership", benchRich},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p Parser
			var stmt SelectStmt
			// Two warm-ups: the first grows the arenas, the second confirms
			// the high-water chunk set covers this statement.
			for i := 0; i < 2; i++ {
				if err := p.Parse(&stmt, tc.src); err != nil {
					t.Fatalf("warm-up parse: %v", err)
				}
			}
			allocs := testing.AllocsPerRun(200, func() {
				if err := p.Parse(&stmt, tc.src); err != nil {
					t.Fatalf("Parse: %v", err)
				}
			})
			if allocs != 0 {
				t.Fatalf("warmed Parse allocated %.1f times per run, want 0", allocs)
			}
		})
	}
}

// TestWarmParseOfMixedShapesIsAllocationFree checks that the steady state
// survives alternating statement shapes, which is what a real prepared-statement
// cache does. A per-shape arena that only worked when the same statement
// repeated would pass the test above and fail here.
func TestWarmParseOfMixedShapesIsAllocationFree(t *testing.T) {
	sources := []string{
		benchSimple, benchFiltered, benchJoin, benchLeftJoin, benchGrouped, benchRich,
	}
	var p Parser
	var stmt SelectStmt
	for i := 0; i < 3; i++ {
		for _, src := range sources {
			if err := p.Parse(&stmt, src); err != nil {
				t.Fatalf("warm-up parse of %q: %v", src, err)
			}
		}
	}
	next := 0
	allocs := testing.AllocsPerRun(200, func() {
		src := sources[next%len(sources)]
		next++
		if err := p.Parse(&stmt, src); err != nil {
			t.Fatalf("Parse(%q): %v", src, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed Parse of mixed shapes allocated %.1f times per run, want 0", allocs)
	}
}

func TestWarmParseStatementIsAllocationFree(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{"insert document", benchInsertDocument},
		{"insert flat", benchInsertFlat},
		{"insert returning", `INSERT INTO docs VALUES (?) RETURNING id, *`},
		{"update", benchUpdate},
		{"delete", benchDelete},
		{"create table", benchCreateTable},
		{"create index", benchCreateIndex},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p Parser
			var stmt Statement
			for i := 0; i < 2; i++ {
				if err := p.ParseStatement(&stmt, tc.src); err != nil {
					t.Fatalf("warm-up ParseStatement: %v", err)
				}
			}
			allocs := testing.AllocsPerRun(200, func() {
				if err := p.ParseStatement(&stmt, tc.src); err != nil {
					t.Fatalf("ParseStatement: %v", err)
				}
			})
			if allocs != 0 {
				t.Fatalf(
					"warmed ParseStatement allocated %.1f times per run, want 0",
					allocs,
				)
			}
		})
	}
}

// TestWarmParseStatementOfMixedShapesIsAllocationFree covers the adapter
// workload: one Parser alternates SELECT, DML, and DDL rather than repeatedly
// parsing only the shape that established its current high-water mark.
func TestWarmParseStatementOfMixedShapesIsAllocationFree(t *testing.T) {
	sources := []string{
		benchSimple,
		benchInsertDocument,
		benchInsertFlat,
		`INSERT INTO docs VALUES (?) RETURNING id, *`,
		benchUpdate,
		benchDelete,
		benchCreateTable,
		benchCreateIndex,
	}
	var p Parser
	var stmt Statement
	for i := 0; i < 3; i++ {
		for _, src := range sources {
			if err := p.ParseStatement(&stmt, src); err != nil {
				t.Fatalf("warm-up ParseStatement of %q: %v", src, err)
			}
		}
	}
	next := 0
	allocs := testing.AllocsPerRun(200, func() {
		src := sources[next%len(sources)]
		next++
		if err := p.ParseStatement(&stmt, src); err != nil {
			t.Fatalf("ParseStatement(%q): %v", src, err)
		}
	})
	if allocs != 0 {
		t.Fatalf(
			"warmed ParseStatement of mixed shapes allocated %.1f times per run, want 0",
			allocs,
		)
	}
}

// TestParseRejectionAllocatesOnlyItsError checks that the rejection path does
// not allocate parse state on its way out. A parser that allocated per
// rejection would be a denial-of-service surface for a driver fed bad SQL in a
// loop, so the only allocation a refusal is allowed is the *ParseError itself.
func TestParseRejectionAllocatesOnlyItsError(t *testing.T) {
	const src = `SELECT a FROM t WHERE b SIMILAR TO 'x'`
	var p Parser
	var stmt SelectStmt
	for i := 0; i < 2; i++ {
		if err := p.Parse(&stmt, src); err == nil {
			t.Fatal("Parse of an unsupported statement succeeded")
		}
	}
	allocs := testing.AllocsPerRun(200, func() {
		if err := p.Parse(&stmt, src); err == nil {
			t.Fatal("Parse of an unsupported statement succeeded")
		}
	})
	// One for the *ParseError. Its message is a constant, so nothing is
	// formatted; a message that needed formatting would cost one more.
	if allocs > 1 {
		t.Fatalf("a rejection allocated %.1f times per run, want at most 1", allocs)
	}
}
