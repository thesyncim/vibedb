package query

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFileSmallIndexedPages(t *testing.T) {
	const count = 4096
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	docs := &store.Segment{}
	for i := range count {
		key := fmt.Sprintf("%05d", i)
		// Secondary ordering deliberately disagrees with physical key order.
		raw := fmt.Appendf(nil, `{"id":%q,"score":%d,"active":%t,"text":%q}`, key, (i*7919)%count, i%7 == 0, strings.Repeat("a\"é", i%23))
		if err := builder.Append(key, raw); err != nil {
			t.Fatal(err)
		}
		if _, err := docs.Append(raw); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "pages")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	opts := durable.Options{Indexes: []store.IndexDefinition{{Name: "score", Paths: []string{"/score"}}}}
	if _, err := durable.CreateFromPrimary(built, f, opts); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snap, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	var e Exec
	defer e.Release()
	for _, n := range []int{1, 32, 64, 256} {
		for _, residual := range []bool{false, true} {
			t.Run(fmt.Sprintf("primary/%d/residual=%t", n, residual), func(t *testing.T) {
				pred := And(Cmp("/id", Gt, "00500"), Cmp("/id", Lt, "04000"))
				if residual {
					pred = And(pred, Cmp("/active", Eq, true))
				}
				q := Select(Path("/id"), Path("/text")).Where(pred).OrderBy("/id", Asc).Limit(n)
				want, err := q.Run(FromSegment(docs))
				if err != nil {
					t.Fatal(err)
				}
				span := NewFileRangeSource([]byte("00500"), []byte("04000"), true)
				span.BindPrimaryOrder("/id")
				// Multiple batches, including a final partially filled page.
				e.Options = ExecOptions{BatchRows: 11, BatchBytes: 512}
				for range 3 {
					if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
						t.Fatal(err)
					}
					if resultKey(e.Result) != resultKey(want) {
						t.Fatalf("got %s want %s", resultKey(e.Result), resultKey(want))
					}
					if e.Stats.Workers != 1 || !e.Stats.PrimaryRangeBounded {
						t.Fatalf("stats %+v", e.Stats)
					}
					if !residual && e.Stats.RowsScanned != uint64(n) {
						t.Fatalf("overread: %+v", e.Stats)
					}
				}
			})
		}
		t.Run(fmt.Sprintf("secondary/%d", n), func(t *testing.T) {
			q := Select(Path("/id"), Path("/score"), Path("/text")).Where(And(Cmp("/score", Ge, 1000), Cmp("/score", Lt, 1000+n))).OrderBy("/score", Desc).Limit(n)
			want, err := q.Run(FromSegment(docs))
			if err != nil {
				t.Fatal(err)
			}
			for _, bytes := range []int64{1 << 20, 1, 1 << 20} {
				e.Options = ExecOptions{BatchBytes: bytes}
				if err := q.RunInto(&e, FromFile(snap)); err != nil {
					t.Fatal(err)
				}
				if resultKey(e.Result) != resultKey(want) {
					t.Fatalf("got %s want %s", resultKey(e.Result), resultKey(want))
				}
				if !e.Stats.IndexBounded || e.Stats.RowsScanned != uint64(n) {
					t.Fatalf("stats %+v", e.Stats)
				}
			}
		})
	}
	q := Select(Path("/id"), Path("/text")).OrderBy("/id", Asc).Limit(32)
	span := NewFileRangeSource([]byte("00500"), nil, false)
	span.BindPrimaryOrder("/id")
	for _, opts := range []ExecOptions{{ResultRows: 1}, {ResultBytes: 128}} {
		e.Options = opts
		if err := q.RunInto(&e, FromFileRange(snap, &span)); !errors.Is(err, ErrResultBudget) {
			t.Fatalf("budget error: %v", err)
		}
	}
	var flag CancelFlag
	flag.Cancel()
	e.Options = ExecOptions{Cancel: &flag}
	if err := q.RunInto(&e, FromFileRange(snap, &span)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel: %v", err)
	}
	flag.Reset()
	if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
		t.Fatal(err)
	}
	if e.Result.RowCount != 32 {
		t.Fatalf("reuse rows %d", e.Result.RowCount)
	}
	e.Options = ExecOptions{}
	if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm page allocations %g", allocs)
	}
	// Materialized variable-width cells outlive the snapshot and its leases.
	want := resultKey(e.Result)
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	if resultKey(e.Result) != want {
		t.Fatal("result lost ownership")
	}
}

func TestValidatedRawPointMatchesSegment(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		q    *Query
	}{
		{
			name: "projection and residual",
			raw:  `{"id":"first","score":7,"active":true,"id":"last"}`,
			q: Select(Path("/id"), Path("/score")).
				Where(And(Cmp("/id", Eq, "last"), Cmp("/active", Eq, true))),
		},
		{
			name: "escaped unrelated key falls back",
			raw:  `{"id":"a","we\\u0069rd":1,"score":9}`,
			q:    Select(Path("/id"), Path("/score")).Where(Cmp("/id", Eq, "a")),
		},
		{
			name: "nested pointer falls back",
			raw:  `{"id":"a","nested":{"score":11}}`,
			q:    Select(Path("/id"), Path("/nested/score")),
		},
		{
			name: "array root preserves pointer semantics",
			raw:  `["a",13]`,
			q:    Select(Path("/0"), Path("/1")),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var docs store.Segment
			if _, err := docs.Append([]byte(test.raw)); err != nil {
				t.Fatal(err)
			}
			want, err := test.q.Run(FromSegment(&docs))
			if err != nil {
				t.Fatal(err)
			}
			var source ValidatedRawSource
			source.Bind([]byte(test.raw))
			var exec Exec
			defer exec.Release()
			if err := test.q.RunInto(&exec, FromValidatedRaw(&source)); err != nil {
				t.Fatal(err)
			}
			if got := resultKey(exec.Result); got != resultKey(want) {
				t.Fatalf("got %s want %s", got, resultKey(want))
			}
		})
	}

	raw := []byte(`{"id":"a","score":7,"active":true}`)
	q := Select(Path("/id"), Path("/score")).Where(Cmp("/active", Eq, true))
	var source ValidatedRawSource
	source.Bind(raw)
	var exec Exec
	defer exec.Release()
	if err := q.RunInto(&exec, FromValidatedRaw(&source)); err != nil {
		t.Fatal(err)
	}
	want := resultKey(exec.Result)
	allocs := testing.AllocsPerRun(200, func() {
		if err := q.RunInto(&exec, FromValidatedRaw(&source)); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm validated raw allocations %g", allocs)
	}
	clear(raw)
	if got := resultKey(exec.Result); got != want {
		t.Fatalf("result retained borrowed bytes: got %s want %s", got, want)
	}
}

func TestPrimaryOrderedLimitSurvivesSpills(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "spill-limit")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4096 {
		key := fmt.Sprintf("%05d", i)
		if err := builder.Append(key, fmt.Appendf(nil, `{"id":%q,"padding":%q}`, key, strings.Repeat("x", 256))); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.CreateFromPrimary(built, file, durable.Options{}); err != nil {
		t.Fatal(err)
	}
	fs, err := durable.Open(file, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	snap, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	q := Select(Path("/id"), Path("/padding")).OrderBy("/id", Asc).Limit(512)
	span := NewFileRangeSource([]byte("00000"), nil, false)
	span.BindPrimaryOrder("/id")
	e := Exec{Options: ExecOptions{Workers: 1, MemoryBytes: 64 << 10, BatchRows: 32, SpillDirectory: t.TempDir()}}
	defer e.Release()
	for range 2 {
		if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
			t.Fatal(err)
		}
		if e.Result.RowCount != 512 || e.Stats.SpillRuns == 0 || e.Stats.RowsScanned > 576 {
			t.Fatalf("limit lost across spills: rows=%d stats=%+v", e.Result.RowCount, e.Stats)
		}
		for i, cell := range e.Result.Columns[0].Cells {
			if got := string(cell.AppendJSON(nil)); got != fmt.Sprintf("%q", fmt.Sprintf("%05d", i)) {
				t.Fatalf("row %d: %s", i, got)
			}
		}
	}
}
