package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// The split scan's allocation contract, kept in its own file because the race
// and checkptr CI jobs skip allocation assertions by file and test name: an
// instrumented runtime allocates where the plain one does not, so measuring it
// there reports a failure that says nothing about this code.

// TestParallelFilterSteadyAllocs closes the allocation contract over the split
// scan, which is the one place in this package where the obvious
// implementation cannot meet it: `go worker.filter(plan, ctx, rows)` boxes its
// arguments into a fresh closure on every start, so a warmed execution would
// allocate once per worker forever. The workers therefore carry their job on
// themselves and are started through func values built once, which is what this
// measures — and it measures the snapshot backend too, whose workers expand
// their own row addresses and could as easily have allocated to do it.
func TestParallelFilterSteadyAllocs(t *testing.T) {
	const rows = 24000
	docs := parallelCorpus(rows)
	set := buildSegment(t, docs, storageMode{"hashed+shaped", true, true})

	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range docs {
		if err := builder.Append(fmt.Sprintf("k%08d", i), doc); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	sources := []struct {
		name string
		src  Source
	}{
		{"segment", FromSegment(set)},
		{"snapshot", FromSnapshot(snapshot)},
	}
	queries := map[string]*Query{
		"projection": Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 50)),
		"nested":     Select(Path("id")).Where(Cmp("nested.bucket", Eq, 7)),
		"escaped":    Select(Path("id"), Path("tag")).Where(Cmp("tag", Ge, "tB")).OrderBy("tag", Asc).Limit(64),
		"grouped":    Select(Path("nested.bucket"), Count(), Sum("score")).GroupBy("nested.bucket").Where(Cmp("sel", Lt, 300)),
		"aggregate":  Select(Count(), Sum("score")).Where(Cmp("sel", Lt, 250)),
	}
	for _, source := range sources {
		for name, q := range queries {
			for _, workers := range []int{2, 4, 8} {
				t.Run(fmt.Sprintf("%s/%s/w=%d", source.name, name, workers), func(t *testing.T) {
					e := Exec{Options: ExecOptions{Workers: workers}}
					// Two warm-up executions: the first grows every buffer, the
					// second builds the worker start closures for this width.
					for i := 0; i < 2; i++ {
						if err := q.RunInto(&e, source.src); err != nil {
							t.Fatal(err)
						}
					}
					allocs := testing.AllocsPerRun(25, func() {
						if err := q.RunInto(&e, source.src); err != nil {
							panic(err)
						}
					})
					if allocs != 0 {
						t.Fatalf("warmed parallel RunInto allocated %.2f times, want 0", allocs)
					}
				})
			}
		}
	}
}
