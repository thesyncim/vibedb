package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func matchingReviewSnapshot(t *testing.T, rows int, options durable.Options, document func(int, string) []byte) (*durable.Snapshot, *store.Segment) {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var docs store.Segment
	var overflowKeys []string
	var overflowDocs [][]byte
	for row := range rows {
		key := fmt.Sprintf("%05d", row)
		raw := document(row, key)
		initial := raw
		if options.InlineValueBytes > 0 && len(raw) > options.InlineValueBytes {
			initial = fmt.Appendf(nil, `{"id":%q}`, key)
			overflowKeys = append(overflowKeys, key)
			overflowDocs = append(overflowDocs, raw)
		}
		if err := builder.Append(key, initial); err != nil {
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
	file, err := os.CreateTemp(t.TempDir(), "matching-review-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	if _, err := durable.CreateFromPrimary(built, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { collection.Close() })
	for index, key := range overflowKeys {
		if _, err := collection.Put([]byte(key), overflowDocs[index]); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { snapshot.Close() })
	return snapshot, &docs
}

func TestProjectedMatchingEscapedArenasReview(t *testing.T) {
	// Thousands of rejected rows reuse the filter arena. Its reservation must
	// follow live capacity, while output-only escaped text has its own arena.
	filterText := strings.Repeat("filter\n", 64)
	outputText := strings.Repeat("output\t", 128)
	snapshot, docs := matchingReviewSnapshot(t, 2048, durable.Options{InlineValueBytes: 4096}, func(row int, key string) []byte {
		return fmt.Appendf(nil, `{"id":%q,"n":%d,"filter":%q,"output":%q}`, key, row%8, filterText, outputText)
	})
	q := Select(Path("/n"), Path("/filter"), Path("/output"), Path("/filter")).
		Where(And(Cmp("/filter", Eq, filterText), Cmp("/n", Eq, 7))).OrderBy("/id", Asc).Limit(256)
	span := NewFileRangeSource([]byte("00000"), []byte("02048"), false)
	span.BindPrimaryOrder("/id")
	want, err := q.Run(FromSegment(docs))
	if err != nil {
		t.Fatal(err)
	}
	var execution Exec
	defer execution.Release()
	execution.Options = ExecOptions{Workers: 1, MemoryBytes: 64 << 10}
	for attempt := range 3 {
		if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		projectedReviewEqual(t, execution.Result, want)
		if execution.Result.resultBytesUsed != want.resultBytesUsed {
			t.Fatalf("logical result budget = %d, want generic %d even when duplicate payload is shared", execution.Result.resultBytesUsed, want.resultBytesUsed)
		}
		for row := range execution.Result.RowCount {
			first := execution.Result.Columns[1].Cells[row]
			duplicate := execution.Result.Columns[3].Cells[row]
			if len(first.raw) == 0 || len(duplicate.raw) == 0 || &first.raw[0] != &duplicate.raw[0] {
				t.Fatalf("row %d copied the same selected field payload twice", row)
			}
		}
		if execution.Stats.ProjectedRows != 256 || execution.Stats.RowsScanned != 2048 {
			t.Fatalf("attempt %d: lost native late matching: %+v", attempt, execution.Stats)
		}
		if len(execution.file.small.projectionPaths) != 3 {
			t.Fatalf("selected %v, want three distinct required fields", execution.file.small.projectionPaths)
		}
	}
}

func TestProjectedMatchingFallbackContinuationReview(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(fmt.Sprintf("overflow=%t", overflow), func(t *testing.T) {
			snapshot, docs := matchingReviewSnapshot(t, 512, durable.Options{InlineValueBytes: 4096}, func(row int, key string) []byte {
				if overflow && row == 260 {
					return fmt.Appendf(nil, `{"id":%q,"n":%d,"wide":{"value":%d}}`, key, row, row)
				}
				if row == 257 {
					if overflow {
						return fmt.Appendf(nil, `{"id":%q,"n":%d,"wide":%d,"extra":%q}`, key, row, row, strings.Repeat("overflow", 1024))
					}
					// A selected container with duplicate keys forces generic extraction;
					// LAST semantics and earlier native cells must both survive the row.
					return fmt.Appendf(nil, `{"id":%q,"n":-1,"n":%d,"wide":0,"wide":{"value":%d}}`, key, row, row)
				}
				return fmt.Appendf(nil, `{"id":%q,"n":%d,"wide":%d}`, key, row, row)
			})
			q := Select(Path("/n"), Path("/wide")).Where(Cmp("/n", Ge, 128)).OrderBy("/id", Asc).Limit(256)
			span := NewFileRangeSource([]byte("00128"), []byte("00512"), false)
			span.BindPrimaryOrder("/id")
			want, err := q.Run(FromSegment(docs))
			if err != nil {
				t.Fatal(err)
			}
			var execution Exec
			defer execution.Release()
			execution.Options = ExecOptions{Workers: 1, MemoryBytes: 128 << 10}
			if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
				t.Fatal(err)
			}
			projectedReviewEqual(t, execution.Result, want)
			nativeRows := uint64(255)
			if overflow {
				nativeRows--
			}
			if execution.Stats.ProjectedRows != nativeRows || execution.Stats.RowsScanned != 256 {
				t.Fatalf("fallback lost native prefix/suffix or counted reconstruction as native: %+v", execution.Stats)
			}
			if execution.file.small.docs.Len() != 0 {
				t.Fatal("fallback rebuilt a generic batch")
			}
			if overflow && cap(execution.file.small.projectionFallback) < 8192 {
				t.Fatal("small inline fallback discarded the larger shared overflow buffer")
			}
			if allocations := testing.AllocsPerRun(10, func() {
				if err := q.RunInto(&execution, FromFileRange(snapshot, &span)); err != nil {
					t.Fatal(err)
				}
			}); allocations != 0 {
				t.Fatalf("warm same-cursor fallback allocated %v times", allocations)
			}
			projectedReviewEqual(t, execution.Result, want)
		})
	}
}

// Retained arenas are live even when a different plan does not consume them.
// A rejection must happen before any projection allocation or snapshot access,
// and must reset reservations so generic fallback cannot inherit fake credit.
func TestProjectedMatchingRetainedArenaAdmissionReview(t *testing.T) {
	p, err := Select(Path("/n")).OrderBy("/id", Asc).Limit(1).compiled()
	if err != nil {
		t.Fatal(err)
	}
	span := NewFileRangeSource(nil, nil, false)
	span.BindPrimaryOrder("/id")
	span.BindPrimaryPredicate("/id")
	for _, arena := range []string{"filter-text", "late-text", "fallback", "tape"} {
		t.Run(arena, func(t *testing.T) {
			scan := fileSmallScan{p: p, ordered: true}
			switch arena {
			case "filter-text":
				scan.work.text = make([]byte, 0, 128<<10)
			case "late-text":
				scan.work.lateText = make([]byte, 0, 128<<10)
			case "fallback":
				scan.projectionFallback = make([]byte, 0, 128<<10)
			case "tape":
				scan.work.eval.entries = make([]vibejson.IndexEntry, 1<<14)
			}
			var stats ExecStats
			allocations := testing.AllocsPerRun(10, func() {
				scan.work.heapWorkBudget.begin(64 << 10)
				handled, err := scan.tryFileProjected(nil, &span, normalizedFileOptions{batchBytes: 4096}, &stats)
				if handled || err != nil {
					t.Fatalf("retained %s was admitted: handled=%t err=%v", arena, handled, err)
				}
				if scan.work.heapWorkBudget.used.Load() != 0 || scan.projectionTextReserved != 0 || scan.projectionLateTextReserved != 0 || scan.projectionFallbackReserved != 0 || scan.work.eval.entriesReserved != 0 {
					t.Fatal("declined projection left stale budget reservations")
				}
			})
			if allocations != 0 || scan.projection != nil || cap(scan.projectionShapes) != 0 || cap(scan.projectionPaths) != 0 {
				t.Fatalf("retained %s admission allocated before rejection: %v", arena, allocations)
			}
		})
	}
}
