package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// Large peer groups expose repeated exclusion scans. All rows share an order
// key; COUNT(value) FILTER keeps two of every six rows (nulls also occur).
func BenchmarkWindowCountPeers(b *testing.B) {
	for _, rows := range []int{1024, 4096, 16384} {
		data := make([][]string, rows)
		for i := range data {
			data[i] = []string{`1`, []string{`null`, `7`, `9`}[i%3], []string{`true`, `false`}[i%2]}
		}
		input := buildSetTestSpool(b, data)
		for _, exclusion := range []struct {
			name string
			kind windowFrameExclusion
		}{
			{"none", windowExcludeNoOthers}, {"group", windowExcludeGroup}, {"ties", windowExcludeTies},
		} {
			for _, filtered := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows=%d/%s/filtered=%t", rows, exclusion.name, filtered), func(b *testing.B) {
					function := windowFunctionSpec{kind: windowCount, column: -1, frame: windowRowsFrame{
						start: windowFrameBound{kind: windowUnboundedPreceding},
						end:   windowFrameBound{kind: windowUnboundedFollowing}, exclusion: exclusion.kind,
					}}
					if filtered {
						function.column = 1
						function.hasFilter = true
						function.filterColumn = 2
					}
					plan := windowPlan{order: []windowOrderKey{{column: 0}}, functions: []windowFunctionSpec{function}}
					benchmarkWindowPlan(b, &input, &plan)
				})
			}
		}
	}
}

// Includes prepared SQL execution, input/output materialization, and cursor consumption.
func BenchmarkSQLWindowCountPeers(b *testing.B) {
	const rows = 4096
	input := &store.Segment{}
	for i := range rows {
		if _, err := input.Append(fmt.Appendf(nil, `{"score":1,"value":%s,"ok":%t}`, []string{`null`, `7`, `9`}[i%3], i%2 == 0)); err != nil {
			b.Fatal(err)
		}
	}
	statement, err := PrepareStatement(`SELECT COUNT(value) OVER (
  ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES
 ) AS n FROM events`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromSegment(input)
	var execution Exec
	run := func() {
		cursor, err := statement.RunInto(&execution, source, nil)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for cursor.Next() {
			count++
		}
		if count != rows {
			b.Fatalf("got %d rows, want %d", count, rows)
		}
	}
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		run()
	}
}
