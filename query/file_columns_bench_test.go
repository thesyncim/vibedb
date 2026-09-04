package query

import (
	"fmt"
	"testing"
)

// Measure complete unindexed durable scans, including batching, extraction,
// predicates, reduction and result ownership, over a wide document corpus.
func BenchmarkFileColumnScan(b *testing.B) {
	snapshot := durableScanCorpus(b, 8192)
	for _, workers := range []int{1, 4} {
		for _, tc := range []struct {
			name string
			q    *Query
		}{
			{"filter", Select(Count()).Where(Cmp("bucket", Lt, 2))},
			{"group", Select(Path("bucket"), Count(), Sum("score")).GroupBy("bucket").OrderBy("bucket", Asc)},
			{"project", Select(Path("id"), Path("label"), Path("score")).Where(Cmp("active", Eq, true)).OrderBy("id", Asc).Limit(64)},
		} {
			b.Run(fmt.Sprintf("%s/workers=%d", tc.name, workers), func(b *testing.B) {
				e := Exec{Options: ExecOptions{Workers: workers}}
				defer e.Release()
				for range 16 {
					if err := tc.q.RunInto(&e, FromFile(snapshot)); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := tc.q.RunInto(&e, FromFile(snapshot)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
