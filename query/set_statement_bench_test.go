package query

import (
	"fmt"
	"testing"
)

type setStatementBenchmarkCase struct {
	runtime setStatementRuntime
	plain   *Statement
	parent  Exec
	plainEx Exec
	source  Source
	args    []any
	frame   statementFrame
	cursor  Cursor
	err     error
}

func newSetStatementBenchmarkCase(tb testing.TB) *setStatementBenchmarkCase {
	tb.Helper()
	docs := make([]string, 32)
	for row := range docs {
		docs[row] = fmt.Sprintf(`{"v":%d,"tag":"row-%02d"}`, row, row)
	}
	segment := mustSegment(tb, docs...)
	left, err := PrepareStatement(
		`SELECT v AS value FROM docs WHERE v >= ? ORDER BY v`,
	)
	if err != nil {
		tb.Fatal(err)
	}
	right, err := PrepareStatement(
		`SELECT v AS ignored FROM docs WHERE v <= ? ORDER BY v`,
	)
	if err != nil {
		left.Release()
		tb.Fatal(err)
	}
	plain, err := PrepareStatement(
		`SELECT v AS value FROM docs WHERE v >= ? ORDER BY v`,
	)
	if err != nil {
		left.Release()
		right.Release()
		tb.Fatal(err)
	}
	descriptor := prepareBinarySetStatement(
		tb, left, right, SetTreeUnionDistinct, 0, 1, 2,
	)
	c := &setStatementBenchmarkCase{plain: plain, source: FromSegment(segment)}
	if err := c.runtime.prepare(descriptor); err != nil {
		left.Release()
		right.Release()
		plain.Release()
		tb.Fatal(err)
	}
	low, high := int64(8), int64(16)
	c.args = []any{&low, &high}
	c.parent.Options.Workers = 1
	c.plainEx.Options.Workers = 1
	tb.Cleanup(func() {
		c.runtime.Release()
		left.Release()
		right.Release()
		plain.Release()
		c.parent.Release()
		c.plainEx.Release()
	})
	return c
}

func (c *setStatementBenchmarkCase) runSet() {
	c.err = c.frame.begin(c.parent.Options)
	if c.err != nil {
		return
	}
	c.frame.args = c.args
	c.cursor, c.err = c.runtime.runIntoFrame(
		&c.parent, c.source, c.args, &c.frame,
	)
}

func (c *setStatementBenchmarkCase) runPlain() {
	c.cursor, c.err = c.plain.RunInto(&c.plainEx, c.source, c.args[:1])
}

func TestSetStatementWarmExecutionAllocationsAndEagerRelease(t *testing.T) {
	c := newSetStatementBenchmarkCase(t)
	c.runSet()
	if c.err != nil || c.parent.Result.RowCount != 32 || c.frame.intermediate.used != 0 {
		t.Fatalf("warm run = rows %d bytes %d err %v",
			c.parent.Result.RowCount, c.frame.intermediate.used, c.err)
	}
	allocations := testing.AllocsPerRun(100, c.runSet)
	if c.err != nil || c.parent.Result.RowCount != 32 || c.frame.intermediate.used != 0 {
		t.Fatalf("allocation run = rows %d bytes %d err %v",
			c.parent.Result.RowCount, c.frame.intermediate.used, c.err)
	}
	if allocations != 0 {
		t.Fatalf("warmed set statement allocations = %.2f, want 0", allocations)
	}
	for leaf := range c.runtime.execs {
		if c.runtime.execs[leaf].Result.RowCount != 0 ||
			c.runtime.execs[leaf].Result.resultBytesUsed != 0 {
			t.Fatalf("leaf %d retained result rows/bytes = %d/%d", leaf,
				c.runtime.execs[leaf].Result.RowCount,
				c.runtime.execs[leaf].Result.resultBytesUsed)
		}
	}
	for slot := range c.runtime.tree.slots {
		if c.runtime.tree.slots[slot].active {
			t.Fatalf("set-tree slot %d remained active after publication", slot)
		}
	}

	// The ordinary prepared path has no descriptor lookup, runtime pointer, or
	// allocation when the feature is absent.
	c.runPlain()
	if c.err != nil || c.plainEx.Result.RowCount != 24 {
		t.Fatalf("plain warm run = rows %d err %v", c.plainEx.Result.RowCount, c.err)
	}
	plainAllocations := testing.AllocsPerRun(100, c.runPlain)
	if c.err != nil || c.plainEx.Result.RowCount != 24 {
		t.Fatalf("plain allocation run = rows %d err %v", c.plainEx.Result.RowCount, c.err)
	}
	if plainAllocations != 0 {
		t.Fatalf("set-absent prepared allocations = %.2f, want 0", plainAllocations)
	}
}

func BenchmarkSetStatementRuntimeWarm(b *testing.B) {
	c := newSetStatementBenchmarkCase(b)
	c.runSet()
	if c.err != nil {
		b.Fatal(c.err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.runSet()
		if c.err != nil || c.parent.Result.RowCount != 32 {
			b.Fatalf("rows=%d err=%v", c.parent.Result.RowCount, c.err)
		}
	}
}

func BenchmarkPreparedStatementSetAbsentWarm(b *testing.B) {
	c := newSetStatementBenchmarkCase(b)
	c.runPlain()
	if c.err != nil {
		b.Fatal(c.err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.runPlain()
		if c.err != nil || c.plainEx.Result.RowCount != 24 {
			b.Fatalf("rows=%d err=%v", c.plainEx.Result.RowCount, c.err)
		}
	}
}
