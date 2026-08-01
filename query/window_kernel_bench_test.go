package query

import (
	"fmt"
	"testing"
)

func BenchmarkWindowKernel(b *testing.B) {
	input := buildWindowRows(b, 2048)
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 16},
		end:   windowFrameBound{kind: windowFollowing, offset: 8},
	}
	groupsFrame := windowRowsFrame{
		unit:  windowFrameGroups,
		start: windowFrameBound{kind: windowPreceding, offset: 2},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	excludedFrame := frameSpec
	excludedFrame.exclusion = windowExcludeGroup
	rangeBytes := []byte(`2.5`)
	rangeOffset := scalar{kind: kindNumber, num: rangeBytes, raw: rangeBytes}
	rangeFrame := windowRowsFrame{
		unit: windowFrameRange,
		start: windowFrameBound{
			kind: windowPreceding, rangeOffset: rangeOffset,
		},
		end: windowFrameBound{
			kind: windowFollowing, rangeOffset: rangeOffset,
		},
	}
	functions := []struct {
		name string
		spec windowFunctionSpec
	}{
		{"row_number", windowFunctionSpec{kind: windowRowNumber, column: -1}},
		{"rank", windowFunctionSpec{kind: windowRank, column: -1}},
		{"dense_rank", windowFunctionSpec{kind: windowDenseRank, column: -1}},
		{"ntile", windowFunctionSpec{kind: windowNTile, column: -1, buckets: 13}},
		{"percent_rank", windowFunctionSpec{kind: windowPercentRank, column: -1}},
		{"cume_dist", windowFunctionSpec{kind: windowCumeDist, column: -1}},
		{"lag", windowFunctionSpec{kind: windowLag, column: 2, offset: 3}},
		{"lead", windowFunctionSpec{kind: windowLead, column: 2, offset: 3}},
		{"lag_ignore_nulls", windowFunctionSpec{kind: windowLag, column: 2, offset: 3,
			nullTreatment: windowIgnoreNulls}},
		{"lead_ignore_nulls", windowFunctionSpec{kind: windowLead, column: 2, offset: 3,
			nullTreatment: windowIgnoreNulls}},
		{"count", windowFunctionSpec{kind: windowCount, column: 2, frame: frameSpec}},
		{"sum", windowFunctionSpec{kind: windowSum, column: 2, frame: frameSpec}},
		{"avg", windowFunctionSpec{kind: windowAvg, column: 2, frame: frameSpec}},
		{"count_filter", windowFunctionSpec{kind: windowCount, column: 2, frame: frameSpec,
			hasFilter: true, filterColumn: 3}},
		{"count_distinct_filter", windowFunctionSpec{kind: windowCount, column: 2,
			frame: frameSpec, hasFilter: true, filterColumn: 3, distinct: true}},
		{"sum_distinct_filter", windowFunctionSpec{kind: windowSum, column: 2,
			frame: frameSpec, hasFilter: true, filterColumn: 3, distinct: true}},
		{"min", windowFunctionSpec{kind: windowMin, column: 2, frame: frameSpec}},
		{"max", windowFunctionSpec{kind: windowMax, column: 2, frame: frameSpec}},
		{"first_value", windowFunctionSpec{kind: windowFirstValue, column: 2, frame: frameSpec}},
		{"last_value", windowFunctionSpec{kind: windowLastValue, column: 2, frame: frameSpec}},
		{"nth_value", windowFunctionSpec{kind: windowNthValue, column: 2, nth: 4, frame: frameSpec}},
		{"first_value_ignore_nulls", windowFunctionSpec{kind: windowFirstValue, column: 2,
			frame: frameSpec, nullTreatment: windowIgnoreNulls}},
		{"nth_value_ignore_nulls_from_last", windowFunctionSpec{kind: windowNthValue, column: 2,
			nth: 4, frame: frameSpec, nullTreatment: windowIgnoreNulls, fromLast: true}},
		{"groups_sum", windowFunctionSpec{kind: windowSum, column: 2, frame: groupsFrame}},
		{"exclude_group_sum", windowFunctionSpec{kind: windowSum, column: 2, frame: excludedFrame}},
		{"exclude_group_max", windowFunctionSpec{kind: windowMax, column: 2, frame: excludedFrame}},
		{"range_sum", windowFunctionSpec{kind: windowSum, column: 2, frame: rangeFrame}},
		{"range_first_value", windowFunctionSpec{kind: windowFirstValue, column: 2, frame: rangeFrame}},
	}
	for _, function := range functions {
		b.Run(function.name, func(b *testing.B) {
			plan := windowPlan{
				partition: []int{0},
				order: []windowOrderKey{{
					column: 1, descending: true, nulls: windowNullsLast,
				}},
				functions: []windowFunctionSpec{function.spec},
			}
			benchmarkWindowPlan(b, &input, &plan)
		})
	}
	b.Run("combined", func(b *testing.B) {
		plan := windowStressPlan()
		benchmarkWindowPlan(b, &input, &plan)
	})
}

func benchmarkWindowPlan(b *testing.B, input *relationSpool, plan *windowPlan) {
	b.Helper()
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		b.Fatal(err)
	}
	charge, err := executor.execute(input, plan, &frame, nil)
	if err != nil {
		b.Fatal(err)
	}
	frame.intermediate.release(charge)
	b.ReportAllocs()
	b.SetBytes(int64(len(input.data)))
	b.ResetTimer()
	for range b.N {
		charge, err = executor.execute(input, plan, &frame, nil)
		if err != nil {
			b.Fatal(err)
		}
		windowSink += executor.result.rows
		frame.intermediate.release(charge)
	}
}

func BenchmarkWindowKernelWideRange(b *testing.B) {
	rows := make([][]string, 256)
	for row := range rows {
		rows[row] = []string{fmt.Sprintf("10000000000000000000000000000000%03d", row)}
	}
	input := buildSetTestSpool(b, rows)
	offsetBytes := []byte(`1.5`)
	offset := scalar{kind: kindNumber, num: offsetBytes, raw: offsetBytes}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			unit: windowFrameRange,
			start: windowFrameBound{
				kind: windowPreceding, rangeOffset: offset,
			},
			end: windowFrameBound{
				kind: windowFollowing, rangeOffset: offset,
			},
		}}},
	}
	benchmarkWindowPlan(b, &input, &plan)
}
