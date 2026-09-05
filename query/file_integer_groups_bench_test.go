package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	fileIntegerGroupsBenchRows  = 8192
	fileIntegerGroupsBenchCount = 16
)

// keepAllFileRows deliberately routes through FromFileFiltered. The filter
// does not remove a row; the filtered-source adapter is the baseline-compatible
// way to force the ordinary batched executor instead of a storage-native direct
// covering/group lane.
type keepAllFileRows struct{}

func (keepAllFileRows) Keep([]byte) bool { return true }

func fileIntegerGroupsBenchSnapshot(tb testing.TB) *durable.Snapshot {
	tb.Helper()
	payload := strings.Repeat("p", 256)
	snapshot, _ := filePackedIntegerExtremaSnapshot(
		tb,
		fileIntegerGroupsBenchRows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			return fmt.Appendf(nil,
				`{"id":%d,"bucket":%d,"score":%d,"payload":%q}`,
				row, row%fileIntegerGroupsBenchCount, row, payload)
		},
	)
	return snapshot
}

func verifyFileIntegerGroupsBenchResult(tb testing.TB, execution *Exec) {
	tb.Helper()
	if execution.Result.RowCount != fileIntegerGroupsBenchCount || len(execution.Result.Columns) != 3 {
		tb.Fatalf("result rows/columns=%d/%d, want %d/3",
			execution.Result.RowCount, len(execution.Result.Columns), fileIntegerGroupsBenchCount)
	}
	wantCount := int64(fileIntegerGroupsBenchRows / fileIntegerGroupsBenchCount)
	for bucket := 0; bucket < fileIntegerGroupsBenchCount; bucket++ {
		gotBucket, bucketOK := execution.Result.Columns[0].Cells[bucket].Int64()
		gotCount, countOK := execution.Result.Columns[1].Cells[bucket].Int64()
		gotSum, sumOK := execution.Result.Columns[2].Cells[bucket].Int64()
		wantSum := int64(0)
		for row := bucket; row < fileIntegerGroupsBenchRows; row += fileIntegerGroupsBenchCount {
			wantSum += int64(row)
		}
		if !bucketOK || !countOK || !sumOK || gotBucket != int64(bucket) ||
			gotCount != wantCount || gotSum != wantSum {
			tb.Fatalf("bucket %d=(%d,%d,%d,%t,%t,%t), want (%d,%d,%d)",
				bucket, gotBucket, gotCount, gotSum, bucketOK, countOK, sumOK,
				bucket, wantCount, wantSum)
		}
	}
}

func BenchmarkFileIntegerGroupsFreshAndWarm(b *testing.B) {
	snapshot := fileIntegerGroupsBenchSnapshot(b)
	defer snapshot.Close()
	query := Select(Path("bucket"), Count(), Sum("score")).
		GroupBy("bucket").OrderBy("bucket", Asc)

	var genericFilter = NewFileFilterSource(keepAllFileRows{})
	cases := []struct {
		name   string
		source Source
		fresh  bool
	}{
		{name: "typed/fresh_exec", source: FromFile(snapshot), fresh: true},
		{name: "typed/warm_exec", source: FromFile(snapshot)},
		{name: "generic/fresh_exec", source: FromFileFiltered(snapshot, &genericFilter), fresh: true},
		{name: "generic/warm_exec", source: FromFileFiltered(snapshot, &genericFilter)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var execution Exec
			var cancel CancelFlag
			if !tc.fresh {
				execution.Options.Workers = 1
				execution.Options.Cancel = &cancel
				defer execution.Release()
			}
			b.ResetTimer()
			for range b.N {
				if tc.fresh {
					execution = Exec{Options: ExecOptions{
						Workers: 1,
						Cancel:  &cancel,
					}}
				}
				if err := query.RunInto(&execution, tc.source); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				verifyFileIntegerGroupsBenchResult(b, &execution)
				if tc.fresh {
					execution.Release()
				}
				b.StartTimer()
			}
		})
	}
}
