package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestWindowKernelRankingOffsetsStableOrderAndPartitions(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`"b"`, `2`, `"x"`, `20`},
		{`"a"`, `1`, `"z"`, `10`},
		{`"a"`, `1.0`, `"z"`, `15`},
		{`"a"`, `null`, `"n"`, `5`},
		{`"b"`, `1`, `"x"`, `null`},
		{`"a"`, `2`, `"a"`, `25`},
		{`"a"`, `2.00`, `"b"`, `30`},
	})
	defaultValue := windowTestScalar(t, `-1`)
	plan := windowPlan{
		partition: []int{0},
		order: []windowOrderKey{
			{column: 1, nulls: windowNullsLast},
			{column: 2, descending: true, nulls: windowNullsFirst},
		},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowLag, column: 3, offset: 1, defaultVal: defaultValue, hasDefault: true},
			{kind: windowLead, column: 3, offset: 1, defaultVal: defaultValue, hasDefault: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `2`, `null`, `-1`},
		{`1`, `1`, `1`, `-1`, `15`},
		{`2`, `1`, `1`, `10`, `30`},
		{`5`, `5`, `4`, `25`, `-1`},
		{`1`, `1`, `1`, `-1`, `20`},
		{`4`, `4`, `3`, `30`, `5`},
		{`3`, `3`, `2`, `15`, `25`},
	})
}

func TestWindowKernelExplicitNullOrderingAndStablePeers(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{setTestMissing, `10`},
		{`null`, `20`},
		{`1`, `30`},
	})
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, descending: true, nulls: windowNullsLast}},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowLag, column: 1, offset: 1},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `2`, `30`},
		{`3`, `2`, `2`, `10`},
		{`1`, `1`, `1`, `null`},
	})
}

func TestWindowKernelDistributionFunctions(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`}, {`1.0`}, {`2`}, {`3`}, {`3.00`}, {`3e0`}, {`4`},
	})
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowNTile, column: -1, buckets: 3},
			{kind: windowPercentRank, column: -1},
			{kind: windowCumeDist, column: -1},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`1`, `0`, `0.2857142857142857142857142857142857`},
		{`1`, `0`, `0.2857142857142857142857142857142857`},
		{`1`, `0.3333333333333333333333333333333333`, `0.4285714285714285714285714285714286`},
		{`2`, `0.5`, `0.8571428571428571428571428571428571`},
		{`2`, `0.5`, `0.8571428571428571428571428571428571`},
		{`3`, `0.5`, `0.8571428571428571428571428571428571`},
		{`3`, `1`, `1`},
	})

	short := buildSetTestSpool(t, [][]string{{`1`}, {`2`}, {`3`}})
	shortPlan := windowPlan{
		order:     plan.order,
		functions: []windowFunctionSpec{{kind: windowNTile, column: -1, buckets: 8}},
	}
	assertSetRows(t, runWindowTest(t, &short, &shortPlan), [][]string{{`1`}, {`2`}, {`3`}})

	singletonPlan := windowPlan{functions: []windowFunctionSpec{
		{kind: windowPercentRank, column: -1},
		{kind: windowCumeDist, column: -1},
	}}
	assertSetRows(t, runWindowTest(t, &relationSpool{rows: 1}, &singletonPlan), [][]string{{`0`, `1`}})
}

func TestWindowKernelFrameValueFunctionsPreserveNullAndMissing(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, setTestMissing},
		{`2`, `"b"`},
		{`3`, `null`},
		{`4`, `"d"`},
	})
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowFirstValue, column: 1, frame: frameSpec},
			{kind: windowLastValue, column: 1, frame: frameSpec},
			{kind: windowNthValue, column: 1, nth: 2, frame: frameSpec},
			{kind: windowNthValue, column: 1, nth: 5, frame: frameSpec},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{setTestMissing, `"b"`, `"b"`, `null`},
		{setTestMissing, `null`, `"b"`, `null`},
		{`"b"`, `"d"`, `null`, `null`},
		{`null`, `"d"`, `"d"`, `null`},
	})
	if result.columns[0][0].raw != nil || result.columns[0][1].raw != nil {
		t.Fatal("FIRST_VALUE did not preserve missing source ownership")
	}
}

func TestWindowKernelNullTreatmentOffsetsAndZeroOffset(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`4`, `"d"`},
		{`1`, setTestMissing},
		{`3`, `null`},
		{`2`, `"b"`},
		{`5`, `"e"`},
	})
	fallback := windowTestScalar(t, `"fallback"`)
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowLag, column: 1, offset: 1, defaultVal: fallback, hasDefault: true},
			{kind: windowLag, column: 1, offset: 1, defaultVal: fallback, hasDefault: true,
				nullTreatment: windowIgnoreNulls},
			{kind: windowLead, column: 1, offset: 1, defaultVal: fallback, hasDefault: true,
				nullTreatment: windowIgnoreNulls},
			{kind: windowLag, column: 1, offset: 2, defaultVal: fallback, hasDefault: true,
				nullTreatment: windowIgnoreNulls},
			{kind: windowLead, column: 1, offset: 0, nullTreatment: windowIgnoreNulls},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`null`, `"b"`, `"e"`, `"fallback"`, `"d"`},
		{`"fallback"`, `"fallback"`, `"b"`, `"fallback"`, setTestMissing},
		{`"b"`, `"b"`, `"d"`, `"fallback"`, `null`},
		{setTestMissing, `"fallback"`, `"d"`, `"fallback"`, `"b"`},
		{`"d"`, `"d"`, `"fallback"`, `"b"`, `"e"`},
	})
	if result.columns[4][1].kind != kindNull || result.columns[4][1].raw != nil {
		t.Fatal("IGNORE NULLS offset zero did not preserve selected missing value")
	}
}

func TestWindowKernelNullTreatmentFrameValuesFromFirstAndLast(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`4`, `"d"`},
		{`1`, setTestMissing},
		{`3`, `null`},
		{`2`, `"b"`},
		{`5`, `"e"`},
	})
	full := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowFirstValue, column: 1, frame: full},
			{kind: windowFirstValue, column: 1, frame: full, nullTreatment: windowIgnoreNulls},
			{kind: windowLastValue, column: 1, frame: full, nullTreatment: windowIgnoreNulls},
			{kind: windowNthValue, column: 1, nth: 2, frame: full},
			{kind: windowNthValue, column: 1, nth: 2, frame: full,
				nullTreatment: windowIgnoreNulls},
			{kind: windowNthValue, column: 1, nth: 3, frame: full, fromLast: true},
			{kind: windowNthValue, column: 1, nth: 3, frame: full,
				nullTreatment: windowIgnoreNulls, fromLast: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	for row, got := range setTestRows(result) {
		want := []string{setTestMissing, `"b"`, `"e"`, `"b"`, `"d"`, `null`, `"b"`}
		if !slices.Equal(got, want) {
			t.Fatalf("row %d = %v, want %v", row, got, want)
		}
	}
	if result.columns[0][0].kind != kindNull || result.columns[0][0].raw != nil {
		t.Fatal("RESPECT NULLS did not preserve selected missing value")
	}
}

func TestWindowKernelIgnoreNullsAllNullPartition(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, setTestMissing}, {`2`, `null`}, {`3`, setTestMissing},
	})
	fallback := windowTestScalar(t, `"fallback"`)
	full := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowLag, column: 1, offset: 1, defaultVal: fallback, hasDefault: true,
				nullTreatment: windowIgnoreNulls},
			{kind: windowLead, column: 1, offset: 1, defaultVal: fallback, hasDefault: true,
				nullTreatment: windowIgnoreNulls},
			{kind: windowFirstValue, column: 1, frame: full, nullTreatment: windowIgnoreNulls},
			{kind: windowLastValue, column: 1, frame: full, nullTreatment: windowIgnoreNulls},
			{kind: windowNthValue, column: 1, nth: 1, frame: full,
				nullTreatment: windowIgnoreNulls, fromLast: true},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`"fallback"`, `"fallback"`, `null`, `null`, `null`},
		{`"fallback"`, `"fallback"`, `null`, `null`, `null`},
		{`"fallback"`, `"fallback"`, `null`, `null`, `null`},
	})
}

func TestWindowKernelIgnoreNullsComposesWithExclusionAndSortedPositions(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`2`, `null`}, {`1`, `"a"`}, {`2.0`, `"b"`},
		{`3`, setTestMissing}, {`2.00`, `"c"`}, {`4`, `"d"`},
	})
	frame := windowRowsFrame{
		unit:      windowFrameGroups,
		start:     windowFrameBound{kind: windowUnboundedPreceding},
		end:       windowFrameBound{kind: windowUnboundedFollowing},
		exclusion: windowExcludeTies,
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowFirstValue, column: 1, frame: frame, nullTreatment: windowIgnoreNulls},
			{kind: windowLastValue, column: 1, frame: frame, nullTreatment: windowIgnoreNulls},
			{kind: windowNthValue, column: 1, nth: 2, frame: frame,
				nullTreatment: windowIgnoreNulls, fromLast: true},
		},
	}
	got := setTestRows(runWindowTest(t, &input, &plan))
	want := referenceWindowIntegerRows(t, &input, &plan)
	if !equalSetTestRows(got, want) {
		t.Fatalf("got=%v\nwant=%v", got, want)
	}
}

func TestWindowKernelAggregateFilterAndDistinctExactSemantics(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `1`, `true`},
		{`2`, `1.0`, `true`},
		{`3`, `2`, `false`},
		{`4`, `2.00`, `true`},
		{`5`, `null`, `true`},
		{`6`, `3`, setTestMissing},
		{`7`, `3.0`, `true`},
		{`8`, `3e0`, `true`},
	})
	full := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: -1, frame: full, hasFilter: true, filterColumn: 2},
			{kind: windowCount, column: 1, frame: full, hasFilter: true, filterColumn: 2},
			{kind: windowCount, column: 1, frame: full, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowSum, column: 1, frame: full, hasFilter: true, filterColumn: 2},
			{kind: windowSum, column: 1, frame: full, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowAvg, column: 1, frame: full, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowMin, column: 1, frame: full, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowMax, column: 1, frame: full, hasFilter: true, filterColumn: 2,
				distinct: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	for row, got := range setTestRows(result) {
		want := []string{`6`, `5`, `3`, `10`, `6`, `2`, `1`, `3.0`}
		if !slices.Equal(got, want) {
			t.Fatalf("row %d = %v, want %v", row, got, want)
		}
	}

	containers := buildSetTestSpool(t, [][]string{
		{`1`, `{"a":1}`}, {`2`, `{"a":1}`}, {`3`, `[1,2]`},
		{`4`, `null`}, {`5`, setTestMissing},
	})
	containerPlan := windowPlan{functions: []windowFunctionSpec{{
		kind: windowCount, column: 1, frame: full, distinct: true,
	}}}
	assertSetRows(t, runWindowTest(t, &containers, &containerPlan), [][]string{
		{`2`}, {`2`}, {`2`}, {`2`}, {`2`},
	})
}

func TestWindowKernelDistinctFilterSlidingExclusionDifferential(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`2`, `1`, `true`}, {`1`, `1`, `true`}, {`2.0`, `2`, `false`},
		{`3`, `2`, `true`}, {`2.00`, `3`, `true`}, {`4`, `3`, `null`},
		{`5`, `4`, `true`},
	})
	frame := windowRowsFrame{
		unit:      windowFrameGroups,
		start:     windowFrameBound{kind: windowPreceding, offset: 1},
		end:       windowFrameBound{kind: windowFollowing, offset: 1},
		exclusion: windowExcludeTies,
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: 1, frame: frame, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowSum, column: 1, frame: frame, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowMin, column: 1, frame: frame, hasFilter: true, filterColumn: 2,
				distinct: true},
			{kind: windowMax, column: 1, frame: frame, hasFilter: true, filterColumn: 2,
				distinct: true},
		},
	}
	got := setTestRows(runWindowTest(t, &input, &plan))
	want := referenceWindowIntegerRows(t, &input, &plan)
	if !equalSetTestRows(got, want) {
		t.Fatalf("got=%v\nwant=%v", got, want)
	}
}

func TestWindowKernelDistinctEmptyFrame(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`, `1`, `true`}, {`2`, `1.0`, `true`}})
	future := windowRowsFrame{
		start: windowFrameBound{kind: windowFollowing, offset: 1},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: 1, frame: future, distinct: true},
			{kind: windowSum, column: 1, frame: future, distinct: true},
			{kind: windowAvg, column: 1, frame: future, distinct: true},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`1`, `1`, `1`}, {`0`, `null`, `null`},
	})
}

func TestWindowKernelGroupsFrames(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `1`}, {`1.0`, `2`}, {`2`, `10`},
		{`3`, `100`}, {`3.0`, `200`}, {`3e0`, `300`}, {`4`, `1000`},
	})
	groupsFrame := windowRowsFrame{
		unit:  windowFrameGroups,
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowCurrentRow},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: -1, frame: groupsFrame},
			{kind: windowSum, column: 1, frame: groupsFrame},
			{kind: windowFirstValue, column: 1, frame: groupsFrame},
			{kind: windowLastValue, column: 1, frame: groupsFrame},
			{kind: windowNthValue, column: 1, nth: 2, frame: groupsFrame},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `3`, `1`, `2`, `2`},
		{`2`, `3`, `1`, `2`, `2`},
		{`3`, `13`, `1`, `10`, `2`},
		{`4`, `610`, `10`, `300`, `100`},
		{`4`, `610`, `10`, `300`, `100`},
		{`4`, `610`, `10`, `300`, `100`},
		{`4`, `1600`, `100`, `1000`, `200`},
	})
}

func TestWindowKernelGroupsFrameBounds(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`}, {`1.0`}, {`2`}, {`3`}, {`3.0`}, {`3e0`}, {`4`},
	})
	tests := []struct {
		name  string
		frame windowRowsFrame
		want  []string
	}{
		{
			name: "unbounded preceding to current group",
			frame: windowRowsFrame{unit: windowFrameGroups,
				start: windowFrameBound{kind: windowUnboundedPreceding},
				end:   windowFrameBound{kind: windowCurrentRow}},
			want: []string{`2`, `2`, `3`, `6`, `6`, `6`, `7`},
		},
		{
			name: "current group to unbounded following",
			frame: windowRowsFrame{unit: windowFrameGroups,
				start: windowFrameBound{kind: windowCurrentRow},
				end:   windowFrameBound{kind: windowUnboundedFollowing}},
			want: []string{`7`, `7`, `5`, `4`, `4`, `4`, `1`},
		},
		{
			name: "one preceding to one following",
			frame: windowRowsFrame{unit: windowFrameGroups,
				start: windowFrameBound{kind: windowPreceding, offset: 1},
				end:   windowFrameBound{kind: windowFollowing, offset: 1}},
			want: []string{`3`, `3`, `6`, `5`, `5`, `5`, `4`},
		},
		{
			name: "two following to unbounded following",
			frame: windowRowsFrame{unit: windowFrameGroups,
				start: windowFrameBound{kind: windowFollowing, offset: 2},
				end:   windowFrameBound{kind: windowUnboundedFollowing}},
			want: []string{`4`, `4`, `1`, `0`, `0`, `0`, `0`},
		},
		{
			name: "unbounded preceding to two preceding",
			frame: windowRowsFrame{unit: windowFrameGroups,
				start: windowFrameBound{kind: windowUnboundedPreceding},
				end:   windowFrameBound{kind: windowPreceding, offset: 2}},
			want: []string{`0`, `0`, `0`, `2`, `2`, `2`, `3`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := windowPlan{
				order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
				functions: []windowFunctionSpec{{
					kind: windowCount, column: -1, frame: test.frame,
				}},
			}
			want := make([][]string, len(test.want))
			for row := range test.want {
				want[row] = []string{test.want[row]}
			}
			assertSetRows(t, runWindowTest(t, &input, &plan), want)
		})
	}
}

func TestWindowKernelFrameExclusions(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{setTestMissing, `10`}, {`null`, `20`}, {`1`, `1`}, {`1.0`, `2`}, {`2`, `4`},
	})
	full := func(exclusion windowFrameExclusion) windowRowsFrame {
		return windowRowsFrame{
			start:     windowFrameBound{kind: windowUnboundedPreceding},
			end:       windowFrameBound{kind: windowUnboundedFollowing},
			exclusion: exclusion,
		}
	}
	tests := []struct {
		name      string
		exclusion windowFrameExclusion
		want      [][]string
	}{
		{
			name: "no others", exclusion: windowExcludeNoOthers,
			want: [][]string{
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
			},
		},
		{
			name: "current row", exclusion: windowExcludeCurrentRow,
			want: [][]string{
				{`4`, `27`, `1`, `20`, `20`, `4`, `1`},
				{`4`, `17`, `1`, `10`, `10`, `4`, `1`},
				{`4`, `36`, `2`, `20`, `10`, `4`, `20`},
				{`4`, `35`, `1`, `20`, `10`, `4`, `20`},
				{`4`, `33`, `1`, `20`, `10`, `2`, `20`},
			},
		},
		{
			name: "group", exclusion: windowExcludeGroup,
			want: [][]string{
				{`3`, `7`, `1`, `4`, `1`, `4`, `2`},
				{`3`, `7`, `1`, `4`, `1`, `4`, `2`},
				{`3`, `34`, `4`, `20`, `10`, `4`, `20`},
				{`3`, `34`, `4`, `20`, `10`, `4`, `20`},
				{`4`, `33`, `1`, `20`, `10`, `2`, `20`},
			},
		},
		{
			name: "ties", exclusion: windowExcludeTies,
			want: [][]string{
				{`4`, `17`, `1`, `10`, `10`, `4`, `1`},
				{`4`, `27`, `1`, `20`, `20`, `4`, `1`},
				{`4`, `35`, `1`, `20`, `10`, `4`, `20`},
				{`4`, `36`, `2`, `20`, `10`, `4`, `20`},
				{`5`, `37`, `1`, `20`, `10`, `4`, `20`},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := full(test.exclusion)
			plan := windowPlan{
				order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
				functions: []windowFunctionSpec{
					{kind: windowCount, column: -1, frame: frame},
					{kind: windowSum, column: 1, frame: frame},
					{kind: windowMin, column: 1, frame: frame},
					{kind: windowMax, column: 1, frame: frame},
					{kind: windowFirstValue, column: 1, frame: frame},
					{kind: windowLastValue, column: 1, frame: frame},
					{kind: windowNthValue, column: 1, nth: 2, frame: frame},
				},
			}
			assertSetRows(t, runWindowTest(t, &input, &plan), test.want)
		})
	}
}

func TestWindowKernelFrameExclusionsUseSortedPositionsWithUnsortedInput(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`2`, `4`}, {`null`, `20`}, {`1.0`, `2`}, {setTestMissing, `10`}, {`1`, `1`},
	})
	full := func(exclusion windowFrameExclusion) windowRowsFrame {
		return windowRowsFrame{
			start:     windowFrameBound{kind: windowUnboundedPreceding},
			end:       windowFrameBound{kind: windowUnboundedFollowing},
			exclusion: exclusion,
		}
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: full(windowExcludeCurrentRow)},
			{kind: windowMax, column: 1, frame: full(windowExcludeCurrentRow)},
			{kind: windowSum, column: 1, frame: full(windowExcludeTies)},
			{kind: windowMax, column: 1, frame: full(windowExcludeTies)},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`33`, `20`, `37`, `20`},
		{`17`, `10`, `27`, `20`},
		{`35`, `20`, `36`, `20`},
		{`27`, `20`, `17`, `10`},
		{`36`, `20`, `35`, `20`},
	})
}

func TestWindowKernelFrameExclusionAverageAndEmptyFrame(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`, `2`}, {`1.0`, `4`}, {`2`, `6`}})
	full := func(exclusion windowFrameExclusion) windowRowsFrame {
		return windowRowsFrame{
			start:     windowFrameBound{kind: windowUnboundedPreceding},
			end:       windowFrameBound{kind: windowUnboundedFollowing},
			exclusion: exclusion,
		}
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowAvg, column: 1, frame: full(windowExcludeCurrentRow)},
			{kind: windowAvg, column: 1, frame: full(windowExcludeGroup)},
			{kind: windowAvg, column: 1, frame: full(windowExcludeTies)},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`5`, `6`, `4`}, {`4`, `6`, `5`}, {`3`, `3`, `4`},
	})

	empty := windowRowsFrame{
		unit:      windowFrameRange,
		start:     windowFrameBound{kind: windowCurrentRow},
		end:       windowFrameBound{kind: windowCurrentRow},
		exclusion: windowExcludeGroup,
	}
	emptyPlan := windowPlan{functions: []windowFunctionSpec{
		{kind: windowCount, column: -1, frame: empty},
		{kind: windowSum, column: 1, frame: empty},
		{kind: windowAvg, column: 1, frame: empty},
		{kind: windowMin, column: 1, frame: empty},
		{kind: windowMax, column: 1, frame: empty},
		{kind: windowFirstValue, column: 1, frame: empty},
		{kind: windowLastValue, column: 1, frame: empty},
		{kind: windowNthValue, column: 1, nth: 1, frame: empty},
	}}
	assertSetRows(t, runWindowTest(t, &input, &emptyPlan), [][]string{
		{`0`, `null`, `null`, `null`, `null`, `null`, `null`, `null`},
		{`0`, `null`, `null`, `null`, `null`, `null`, `null`, `null`},
		{`0`, `null`, `null`, `null`, `null`, `null`, `null`, `null`},
	})
}

func TestWindowKernelRangePeerFrames(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `1`, `10`}, {`1.0`, `1.00`, `20`}, {`1`, `2`, `30`}, {`2`, `1`, `40`},
	})
	running := windowRowsFrame{
		unit:  windowFrameRange,
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowCurrentRow},
	}
	peers := windowRowsFrame{
		unit:  windowFrameRange,
		start: windowFrameBound{kind: windowCurrentRow},
		end:   windowFrameBound{kind: windowCurrentRow},
	}
	plan := windowPlan{
		order: []windowOrderKey{
			{column: 0, nulls: windowNullsFirst}, {column: 1, nulls: windowNullsFirst},
		},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: -1, frame: running},
			{kind: windowSum, column: 2, frame: running},
			{kind: windowCount, column: -1, frame: peers},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`2`, `30`, `2`}, {`2`, `30`, `2`}, {`3`, `60`, `1`}, {`4`, `100`, `1`},
	})

	withoutOrder := windowPlan{functions: []windowFunctionSpec{
		{kind: windowCount, column: -1, frame: peers},
	}}
	assertSetRows(t, runWindowTest(t, &input, &withoutOrder), [][]string{{`4`}, {`4`}, {`4`}, {`4`}})
}

func TestWindowKernelRangeNumericOffsetsExactAscendingDescendingAndNulls(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{setTestMissing, `100`}, {`null`, `200`}, {`-1`, `1`}, {`0`, `2`},
		{`1.0`, `4`}, {`1.5`, `8`}, {`2`, `16`}, {`4`, `32`},
	})
	frame := windowRowsFrame{
		unit: windowFrameRange,
		start: windowFrameBound{
			kind: windowPreceding, rangeOffset: windowTestScalar(t, `0.5`),
		},
		end: windowFrameBound{
			kind: windowFollowing, rangeOffset: windowTestScalar(t, `1`),
		},
	}
	tests := []struct {
		name string
		key  windowOrderKey
		want [][]string
	}{
		{
			name: "ascending nulls first",
			key:  windowOrderKey{column: 0, nulls: windowNullsFirst},
			want: [][]string{
				{`2`, `300`}, {`2`, `300`}, {`2`, `3`}, {`2`, `6`},
				{`3`, `28`}, {`3`, `28`}, {`2`, `24`}, {`1`, `32`},
			},
		},
		{
			name: "descending nulls last",
			key: windowOrderKey{
				column: 0, descending: true, nulls: windowNullsLast,
			},
			want: [][]string{
				{`2`, `300`}, {`2`, `300`}, {`1`, `1`}, {`2`, `3`},
				{`3`, `14`}, {`3`, `28`}, {`3`, `28`}, {`1`, `32`},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := windowPlan{
				order: []windowOrderKey{test.key},
				functions: []windowFunctionSpec{
					{kind: windowCount, column: -1, frame: frame},
					{kind: windowSum, column: 1, frame: frame},
				},
			}
			assertSetRows(t, runWindowTest(t, &input, &plan), test.want)
		})
	}

	exact := buildSetTestSpool(t, [][]string{
		{`0.1`}, {`0.1000000000000000000000000000000001`}, {`0.2`},
	})
	exactFrame := windowRowsFrame{
		unit: windowFrameRange,
		start: windowFrameBound{
			kind: windowPreceding, rangeOffset: windowTestScalar(t, `1e-34`),
		},
		end: windowFrameBound{kind: windowCurrentRow},
	}
	exactPlan := windowPlan{
		order:     []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: exactFrame}},
	}
	assertSetRows(t, runWindowTest(t, &exact, &exactPlan), [][]string{{`1`}, {`2`}, {`1`}})
}

func TestWindowKernelRangeWidePreparedReuseZeroAlloc(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`10000000000000000000000000000000000`},
		{`10000000000000000000000000000000001`},
		{`10000000000000000000000000000000002`},
		{`10000000000000000000000000000000010`},
	})
	offset := windowTestScalar(t, `1.5`)
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
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(&input, &plan, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	assertSetRows(t, &executor.result, [][]string{{`2`}, {`3`}, {`2`}, {`1`}})
	if got := testing.AllocsPerRun(20, func() {
		charge, err = executor.execute(&input, &plan, &frame, nil)
		if err != nil {
			t.Fatal(err)
		}
		frame.intermediate.release(charge)
	}); got != 0 {
		t.Fatalf("warmed wide RANGE allocated %.2f times, want 0", got)
	}
}

func TestWindowKernelSlidingRowsAggregates(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`"p"`, `1`, `1`},
		{`"p"`, `2`, `2.00`},
		{`"p"`, `3`, `null`},
		{`"p"`, `4`, `4`},
		{`"p"`, `5`, `8`},
	})
	frame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	plan := windowPlan{
		partition: []int{0},
		order:     []windowOrderKey{{column: 1, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: -1, frame: frame},
			{kind: windowCount, column: 2, frame: frame},
			{kind: windowSum, column: 2, frame: frame},
			{kind: windowAvg, column: 2, frame: frame},
			{kind: windowMin, column: 2, frame: frame},
			{kind: windowMax, column: 2, frame: frame},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `3`, `1.5`, `1`, `2.00`},
		{`3`, `2`, `3`, `1.5`, `1`, `2.00`},
		{`3`, `2`, `6`, `3`, `2.00`, `4`},
		{`3`, `2`, `12`, `6`, `4`, `8`},
		{`2`, `2`, `12`, `6`, `4`, `8`},
	})
}

func TestWindowKernelUnboundedFramesExactDecimalsAndEmptyFrame(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `9007199254740992`},
		{`2`, `2.0`},
	})
	full := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	future := windowRowsFrame{
		start: windowFrameBound{kind: windowFollowing, offset: 1},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: full},
			{kind: windowAvg, column: 1, frame: full},
			{kind: windowMin, column: 1, frame: full},
			{kind: windowMax, column: 1, frame: full},
			{kind: windowSum, column: 1, frame: future},
			{kind: windowCount, column: -1, frame: future},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`9007199254740994`, `4503599627370497`, `2.0`, `9007199254740992`, `2`, `1`},
		{`9007199254740994`, `4503599627370497`, `2.0`, `9007199254740992`, `null`, `0`},
	})
}

func TestWindowKernelLagLeadPreserveMissingAndOwnDefault(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, setTestMissing},
		{`2`, `"value"`},
	})
	defaultValue := windowTestScalar(t, `"fallback"`)
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowLag, column: 1, offset: 1, defaultVal: defaultValue, hasDefault: true},
			{kind: windowLead, column: 1, offset: 1, defaultVal: defaultValue, hasDefault: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	if result.columns[0][1].kind != kindNull || result.columns[0][1].raw != nil {
		t.Fatal("LAG did not preserve the engine's missing marker")
	}
	assertSetRows(t, result, [][]string{
		{`"fallback"`, `"value"`},
		{setTestMissing, `"fallback"`},
	})
}

func TestWindowKernelValidationAndFrames(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`}})
	validFrame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowCurrentRow},
	}
	tests := []windowPlan{
		{partition: []int{1}},
		{order: []windowOrderKey{{column: 0, nulls: windowNullOrder(9)}}},
		{functions: []windowFunctionSpec{{kind: windowFunctionKind(99), column: -1}}},
		{functions: []windowFunctionSpec{{kind: windowRowNumber, column: 0}}},
		{functions: []windowFunctionSpec{{kind: windowLag, column: 1}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: windowRowsFrame{
			start: windowFrameBound{kind: windowFollowing, offset: 2},
			end:   windowFrameBound{kind: windowFollowing, offset: 1},
		}}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, offset: 1, frame: validFrame}}},
		{functions: []windowFunctionSpec{{kind: windowNTile, column: -1}}},
		{functions: []windowFunctionSpec{{kind: windowNthValue, column: 0, frame: validFrame}}},
		{functions: []windowFunctionSpec{{kind: windowFirstValue, column: 0, nth: 1, frame: validFrame}}},
		{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1,
			nullTreatment: windowIgnoreNulls}}},
		{functions: []windowFunctionSpec{{kind: windowLag, column: 0, fromLast: true}}},
		{functions: []windowFunctionSpec{{kind: windowFirstValue, column: 0, frame: validFrame,
			fromLast: true}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: validFrame,
			nullTreatment: windowIgnoreNulls}}},
		{functions: []windowFunctionSpec{{kind: windowLead, column: 0,
			nullTreatment: windowNullTreatment(9)}}},
		{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1,
			hasFilter: true}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: validFrame,
			distinct: true}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: validFrame,
			hasFilter: true, filterColumn: 1}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: validFrame,
			hasFilter: true, filterColumn: 0}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: validFrame,
			filterColumn: 1}}},
		{functions: []windowFunctionSpec{{kind: windowFirstValue, column: 0, frame: windowRowsFrame{
			unit: windowFrameUnit(9), start: validFrame.start, end: validFrame.end,
		}}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			start: validFrame.start, end: validFrame.end,
			exclusion: windowFrameExclusion(9),
		}}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			unit: windowFrameRange,
			start: windowFrameBound{
				kind: windowPreceding, rangeOffset: windowTestScalar(t, `1`),
			},
			end: windowFrameBound{kind: windowCurrentRow},
		}}}},
		{order: []windowOrderKey{{column: 0}, {column: 0}}, functions: []windowFunctionSpec{{
			kind: windowCount, column: -1, frame: windowRowsFrame{
				unit: windowFrameRange,
				start: windowFrameBound{
					kind: windowPreceding, rangeOffset: windowTestScalar(t, `1`),
				},
				end: windowFrameBound{kind: windowCurrentRow},
			},
		}}},
		{order: []windowOrderKey{{column: 0}}, functions: []windowFunctionSpec{{
			kind: windowCount, column: -1, frame: windowRowsFrame{
				unit: windowFrameRange,
				start: windowFrameBound{
					kind: windowPreceding, rangeOffset: windowTestScalar(t, `-1`),
				},
				end: windowFrameBound{kind: windowCurrentRow},
			},
		}}},
		{order: []windowOrderKey{{column: 0}}, functions: []windowFunctionSpec{{
			kind: windowCount, column: -1, frame: windowRowsFrame{
				unit: windowFrameRange,
				start: windowFrameBound{
					kind: windowPreceding, rangeOffset: windowTestScalar(t, `"x"`),
				},
				end: windowFrameBound{kind: windowCurrentRow},
			},
		}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			start: windowFrameBound{
				kind: windowPreceding, offset: 1, rangeOffset: windowTestScalar(t, `1`),
			},
			end: windowFrameBound{kind: windowCurrentRow},
		}}}},
	}
	for at := range tests {
		var executor windowExecutor
		var frame statementFrame
		if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
			t.Fatal(err)
		}
		if _, err := executor.execute(&input, &tests[at], &frame, nil); !errors.Is(err, errWindowPlan) &&
			!errors.Is(err, errWindowFrame) {
			t.Fatalf("invalid plan %d error = %v", at, err)
		}
		if executor.result.rows != 0 || frame.intermediate.used != 0 {
			t.Fatalf("invalid plan %d published rows/bytes %d/%d",
				at, executor.result.rows, frame.intermediate.used)
		}
	}

	malformed := input
	malformed.columns[0] = nil
	plan := windowPlan{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}}}
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&malformed, &plan, &frame, nil); !errors.Is(err, errWindowInput) {
		t.Fatalf("malformed input error = %v", err)
	}

	mixed := buildSetTestSpool(t, [][]string{{`1`}, {`"x"`}})
	rangePlan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			unit: windowFrameRange,
			start: windowFrameBound{
				kind: windowPreceding, rangeOffset: windowTestScalar(t, `1`),
			},
			end: windowFrameBound{kind: windowCurrentRow},
		}}},
	}
	var mixedExecutor windowExecutor
	var mixedFrame statementFrame
	if err := mixedFrame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mixedExecutor.execute(&mixed, &rangePlan, &mixedFrame, nil); !errors.Is(err, errWindowPlan) {
		t.Fatalf("mixed RANGE ORDER BY error = %v", err)
	}
	if mixedExecutor.result.rows != 0 || mixedFrame.intermediate.used != 0 {
		t.Fatalf("mixed RANGE published rows/bytes %d/%d",
			mixedExecutor.result.rows, mixedFrame.intermediate.used)
	}
}

func TestWindowKernelAliasRejectionPreservesInput(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`2`}, {`1`}})
	plan := windowPlan{
		order:     []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}},
	}
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(&input, &plan, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	want := setTestRows(&executor.result)
	wantCaps := windowExecutorCapacities(&executor)
	if _, err := executor.execute(&executor.result, &plan, &frame, nil); !errors.Is(err, errWindowAlias) {
		t.Fatalf("alias error = %v", err)
	}
	if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
		t.Fatalf("alias rejection mutated input: got %v, want %v", got, want)
	}
	if got := windowExecutorCapacities(&executor); got != wantCaps {
		t.Fatalf("alias rejection changed high-water: got %+v, want %+v", got, wantCaps)
	}
}

func TestWindowKernelBudgetAdmissionPrecedesGrowth(t *testing.T) {
	input := buildWindowRows(t, 128)
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 4},
		end:   windowFrameBound{kind: windowFollowing, offset: 3},
	}
	excludedFrame := frameSpec
	excludedFrame.exclusion = windowExcludeGroup
	rangeOffset := windowTestScalar(t, `2.5`)
	rangeFrame := windowRowsFrame{
		unit: windowFrameRange,
		start: windowFrameBound{
			kind: windowPreceding, rangeOffset: rangeOffset,
		},
		end: windowFrameBound{
			kind: windowFollowing, rangeOffset: rangeOffset,
		},
	}
	plan := windowPlan{
		partition: []int{0},
		order:     []windowOrderKey{{column: 1, nulls: windowNullsLast}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 2, frame: frameSpec},
			{kind: windowSum, column: 2, frame: excludedFrame, distinct: true,
				hasFilter: true, filterColumn: 3},
			{kind: windowMin, column: 2, frame: excludedFrame},
			{kind: windowCount, column: -1, frame: rangeFrame},
		},
	}
	shape, err := measureWindowExecution(&input, &plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	var executor windowExecutor
	before := windowExecutorCapacities(&executor)
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: shape.workCharge - 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&input, &plan, &frame, nil); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("workspace budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if got := windowExecutorCapacities(&executor); got != before {
		t.Fatalf("failed workspace admission grew %+v to %+v", before, got)
	}
	if frame.intermediate.used != 0 || executor.result.rows != 0 {
		t.Fatalf("workspace failure retained rows/bytes %d/%d",
			executor.result.rows, frame.intermediate.used)
	}

	if err := frame.begin(ExecOptions{IntermediateBytes: shape.workCharge}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&input, &plan, &frame, nil); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("result budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if cap(executor.result.columns) != 0 || cap(executor.result.data) != 0 {
		t.Fatal("failed result admission grew result storage")
	}
	if frame.intermediate.used != 0 {
		t.Fatalf("result failure retained %d budget bytes", frame.intermediate.used)
	}
}

func TestWindowKernelRangeWideExponentBudgetFailureIsAtomic(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`}, {`2`}})
	offset := windowTestScalar(t, `1e999999999999999999999999999999999999`)
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowCount, column: -1, frame: windowRowsFrame{
			unit: windowFrameRange,
			start: windowFrameBound{
				kind: windowPreceding, rangeOffset: offset,
			},
			end: windowFrameBound{kind: windowCurrentRow},
		}}},
	}
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&input, &plan, &frame, nil); !errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("wide RANGE budget error = %v, want %v", err, ErrAggregateBudget)
	}
	if executor.result.rows != 0 || frame.intermediate.used != 0 {
		t.Fatalf("wide RANGE failure published rows/bytes %d/%d",
			executor.result.rows, frame.intermediate.used)
	}
}

func TestWindowKernelCancellationNoPartialAndReuse(t *testing.T) {
	input := buildWindowRows(t, 4096)
	plan := windowStressPlan()
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	cancel.Cancel()
	before := windowExecutorCapacities(&executor)
	if _, err := executor.execute(&input, &plan, &frame, &cancel); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-cancel error = %v", err)
	}
	if got := windowExecutorCapacities(&executor); got != before {
		t.Fatalf("pre-cancel grew %+v to %+v", before, got)
	}
	cancel.Reset()
	charge, err := executor.execute(&input, &plan, &frame, &cancel)
	if err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	frame.intermediate.release(charge)
}

func TestWindowKernelPreparedReuseZeroAllocAndRelease(t *testing.T) {
	input := buildWindowRows(t, 256)
	plan := windowStressPlan()
	var executor windowExecutor
	run := func() {
		var frame statementFrame
		if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
			panic(err)
		}
		charge, err := executor.execute(&input, &plan, &frame, nil)
		if err != nil {
			panic(err)
		}
		windowSink += executor.result.rows
		frame.intermediate.release(charge)
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(300, run); allocations != 0 {
		t.Fatalf("warmed window execution allocated %.2f times, want 0", allocations)
	}
	if windowExecutorCapacities(&executor) == (windowTestCapacities{}) {
		t.Fatal("test did not establish high-water storage")
	}
	executor.release()
	executor.release()
	if got := windowExecutorCapacities(&executor); got != (windowTestCapacities{}) {
		t.Fatalf("Release retained high-water: %+v", got)
	}
	run()
}

func TestWindowKernelWideDecimalAggregation(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `123456789012345678901234567890.125`},
		{`2`, `-99999999999999999999999999999.875`},
	})
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: frameSpec},
			{kind: windowAvg, column: 1, frame: frameSpec},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`23456789012345678901234567890.25`, `11728394506172839450617283945.125`},
		{`23456789012345678901234567890.25`, `11728394506172839450617283945.125`},
	})
}

func TestWindowKernelDecimalOutputNormalization(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`, `1.2`}, {`2`, `1.8`}})
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: frameSpec},
			{kind: windowAvg, column: 1, frame: frameSpec},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`3`, `1.5`}, {`3`, `1.5`},
	})
}

func TestWindowKernelAverageFastPathExactAndTiesToEven(t *testing.T) {
	tests := []struct {
		coefficient int64
		scale       int64
		count       int64
		want        string
	}{
		{1, 0, 2, `0.5`},
		{1, 0, 3, `0.3333333333333333333333333333333333`},
		{2, 0, 3, `0.6666666666666666666666666666666667`},
		{-1, 0, 8, `-0.125`},
		{506612146037681, 9, 84, `6031096976639059523809.52380952381`},
		{1, 0, 562949953421312, `0.000000000000001776356839400250464677810668945312`},
		{3, 0, 281474976710656, `0.00000000000001065814103640150278806686401367188`},
	}
	for iteration, test := range tests {
		if test.count > int64(math.MaxInt) {
			continue
		}
		var executor windowExecutor
		executor.numberOut = make([]byte, 0, 512)
		executor.negative = make([]byte, 0, averageDigits+2)
		executor.aggregate.n = int(test.count)
		executor.aggregate.sum = decimalSum{
			set: true, smallCoeff: test.coefficient, smallScale: test.scale,
			digits: intDigits64(test.coefficient),
		}
		got, ok, err := executor.windowAverageCellFast()
		if err != nil || !ok {
			t.Fatalf("iteration %d fast average = ok %v err %v", iteration, ok, err)
		}
		if string(got.raw) != test.want {
			t.Fatalf("iteration %d (%d*10^%d)/%d = %s, want %s",
				iteration, test.coefficient, test.scale, test.count, got.raw, test.want)
		}
	}
}

func TestWindowKernelDifferentialRandomIntegerFrames(t *testing.T) {
	random := rand.New(rand.NewSource(0x71ad0))
	for iteration := range 300 {
		rows := 1 + random.Intn(24)
		data := make([][]string, rows)
		for row := range data {
			value := fmt.Sprintf("%d", random.Intn(21)-10)
			if random.Intn(5) == 0 {
				value = `null`
			}
			order := fmt.Sprintf("%d", random.Intn(7)-3)
			if random.Intn(9) == 0 {
				order = setTestMissing
			}
			filters := [...]string{`true`, `false`, `null`, setTestMissing}
			data[row] = []string{
				fmt.Sprintf("%d", random.Intn(3)), order, value,
				filters[random.Intn(len(filters))],
			}
		}
		input := buildSetTestSpool(t, data)
		preceding, following := random.Intn(4), random.Intn(4)
		frameSpec := windowRowsFrame{exclusion: windowFrameExclusion(random.Intn(4))}
		frameSpec.unit = windowFrameUnit(random.Intn(3))
		if frameSpec.unit == windowFrameRange {
			switch random.Intn(3) {
			case 0:
				frameSpec.start.kind = windowUnboundedPreceding
				frameSpec.end.kind = windowCurrentRow
			case 1:
				frameSpec.start.kind = windowCurrentRow
				frameSpec.end.kind = windowCurrentRow
			default:
				frameSpec.start = windowFrameBound{
					kind: windowPreceding, rangeOffset: windowTestScalar(t, fmt.Sprintf("%d", preceding)),
				}
				frameSpec.end = windowFrameBound{
					kind: windowFollowing, rangeOffset: windowTestScalar(t, fmt.Sprintf("%d", following)),
				}
			}
		} else {
			frameSpec.start = windowFrameBound{kind: windowPreceding, offset: preceding}
			frameSpec.end = windowFrameBound{kind: windowFollowing, offset: following}
		}
		plan := windowPlan{
			partition: []int{0},
			order: []windowOrderKey{{
				column: 1, descending: random.Intn(2) == 0,
				nulls: windowNullOrder(random.Intn(2)),
			}},
			functions: []windowFunctionSpec{
				{kind: windowRowNumber, column: -1},
				{kind: windowRank, column: -1},
				{kind: windowDenseRank, column: -1},
				{kind: windowNTile, column: -1, buckets: 1 + random.Intn(8)},
				{kind: windowPercentRank, column: -1},
				{kind: windowCumeDist, column: -1},
				{kind: windowLag, column: 2, offset: random.Intn(4),
					nullTreatment: windowNullTreatment(random.Intn(2))},
				{kind: windowLead, column: 2, offset: random.Intn(4),
					nullTreatment: windowNullTreatment(random.Intn(2))},
				{kind: windowCount, column: -1, frame: frameSpec},
				{kind: windowCount, column: 2, frame: frameSpec},
				{kind: windowSum, column: 2, frame: frameSpec},
				{kind: windowCount, column: 2, frame: frameSpec, distinct: true,
					hasFilter: true, filterColumn: 3},
				{kind: windowSum, column: 2, frame: frameSpec, distinct: true,
					hasFilter: true, filterColumn: 3},
				{kind: windowMin, column: 2, frame: frameSpec},
				{kind: windowMax, column: 2, frame: frameSpec},
				{kind: windowFirstValue, column: 2, frame: frameSpec,
					nullTreatment: windowNullTreatment(random.Intn(2))},
				{kind: windowLastValue, column: 2, frame: frameSpec,
					nullTreatment: windowNullTreatment(random.Intn(2))},
				{kind: windowNthValue, column: 2, nth: 2, frame: frameSpec,
					nullTreatment: windowNullTreatment(random.Intn(2)),
					fromLast:      random.Intn(2) != 0},
			},
		}
		result := runWindowTest(t, &input, &plan)
		want := referenceWindowIntegerRows(t, &input, &plan)
		if got := setTestRows(result); !equalSetTestRows(got, want) {
			t.Fatalf("iteration %d\nframe=%+v\ndata=%v\ngot=%v\nwant=%v",
				iteration, frameSpec, data, got, want)
		}
	}
}

func TestWindowKernelRaceIndependentExecutorsAndConcurrentCancellation(t *testing.T) {
	input := buildWindowRows(t, 2048)
	plan := windowStressPlan()
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			var executor windowExecutor
			for range 8 {
				var frame statementFrame
				if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
					t.Error(err)
					return
				}
				charge, err := executor.execute(&input, &plan, &frame, nil)
				if err != nil {
					t.Error(err)
					return
				}
				frame.intermediate.release(charge)
			}
		})
	}
	workers.Wait()

	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	done := make(chan error, 1)
	go func() {
		charge, err := executor.execute(&input, &plan, &frame, &cancel)
		if err == nil {
			frame.intermediate.release(charge)
		}
		done <- err
	}()
	cancel.Cancel()
	err := <-done
	if err != nil && !errors.Is(err, ErrCanceled) {
		t.Fatalf("concurrent cancellation error = %v", err)
	}
	if errors.Is(err, ErrCanceled) &&
		(executor.result.rows != 0 || frame.intermediate.used != 0) {
		t.Fatalf("canceled execution retained rows/bytes %d/%d",
			executor.result.rows, frame.intermediate.used)
	}
}

func TestWindowKernelOverflowGuards(t *testing.T) {
	if _, ok := windowExtremaEntries(math.MaxInt); ok {
		t.Fatal("extrema tree admitted MaxInt rows")
	}
	if _, ok := windowScaleDifference(math.MaxInt64, math.MinInt64); ok {
		t.Fatal("RANGE scale subtraction overflow was admitted")
	}
	if _, err := windowDistinctCapacity(math.MaxInt); !errors.Is(err, errWindowSize) {
		t.Fatalf("distinct capacity overflow = %v, want %v", err, errWindowSize)
	}
	input := relationSpool{rows: math.MaxInt}
	plan := windowPlan{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}}}
	if _, err := measureWindowExecution(&input, &plan, nil); !errors.Is(err, errWindowSize) {
		t.Fatalf("row workspace overflow = %v, want %v", err, errWindowSize)
	}
	groupsPlan := windowPlan{functions: []windowFunctionSpec{{
		kind: windowCount, column: -1,
		frame: windowRowsFrame{
			unit: windowFrameGroups,
			end:  windowFrameBound{kind: windowCurrentRow},
		},
	}}}
	if _, err := measureWindowExecution(&input, &groupsPlan, nil); !errors.Is(err, errWindowSize) {
		t.Fatalf("group workspace overflow = %v, want %v", err, errWindowSize)
	}
	invalidFrames := []windowRowsFrame{
		{start: windowFrameBound{kind: windowUnboundedFollowing}, end: windowFrameBound{kind: windowUnboundedFollowing}},
		{start: windowFrameBound{kind: windowCurrentRow}, end: windowFrameBound{kind: windowUnboundedPreceding}},
		{start: windowFrameBound{kind: windowCurrentRow, offset: 1}, end: windowFrameBound{kind: windowUnboundedFollowing}},
	}
	for _, frame := range invalidFrames {
		if err := validateWindowFrame(frame); !errors.Is(err, errWindowFrame) {
			t.Fatalf("invalid frame %+v error = %v", frame, err)
		}
	}
}

var windowSink int

type windowTestCapacities struct {
	order, scratch, deque int
	groups, extrema       int
	nonNull               int
	distinctSlots         int
	distinctEntries       int
	number, negative      int
	columns, data         int
}

func windowExecutorCapacities(e *windowExecutor) windowTestCapacities {
	return windowTestCapacities{
		order: cap(e.order), scratch: cap(e.sortScratch), deque: cap(e.deque),
		groups: cap(e.groups), extrema: cap(e.extrema),
		nonNull:       cap(e.nonNull),
		distinctSlots: cap(e.distinctSlots), distinctEntries: cap(e.distinctEntries),
		number: cap(e.numberOut), negative: cap(e.negative),
		columns: cap(e.result.columns), data: cap(e.result.data),
	}
}

func runWindowTest(
	t testing.TB,
	input *relationSpool,
	plan *windowPlan,
) *relationSpool {
	t.Helper()
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(input, plan, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	return &executor.result
}

func windowTestScalar(t testing.TB, value string) scalar {
	t.Helper()
	spool := buildSetTestSpool(t, [][]string{{value}})
	return spool.columns[0][0]
}

func buildWindowRows(t testing.TB, rows int) relationSpool {
	t.Helper()
	data := make([][]string, rows)
	for row := range data {
		data[row] = []string{
			fmt.Sprintf("%d", row%7),
			fmt.Sprintf("%d.0", row%97),
			fmt.Sprintf("%d", row%31-15),
			[]string{`true`, `false`, `null`}[row%3],
		}
	}
	return buildSetTestSpool(t, data)
}

func windowStressPlan() windowPlan {
	frame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 8},
		end:   windowFrameBound{kind: windowFollowing, offset: 4},
	}
	groupsFrame := windowRowsFrame{
		unit:  windowFrameGroups,
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	excludedFrame := frame
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
		exclusion: windowExcludeTies,
	}
	return windowPlan{
		partition: []int{0},
		order: []windowOrderKey{{
			column: 1, descending: true, nulls: windowNullsLast,
		}},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowNTile, column: -1, buckets: 11},
			{kind: windowPercentRank, column: -1},
			{kind: windowCumeDist, column: -1},
			{kind: windowLag, column: 2, offset: 3},
			{kind: windowLead, column: 2, offset: 2},
			{kind: windowLag, column: 2, offset: 3, nullTreatment: windowIgnoreNulls},
			{kind: windowLead, column: 2, offset: 2, nullTreatment: windowIgnoreNulls},
			{kind: windowCount, column: -1, frame: frame},
			{kind: windowCount, column: 2, frame: frame},
			{kind: windowSum, column: 2, frame: frame},
			{kind: windowCount, column: 2, frame: excludedFrame, distinct: true},
			{kind: windowSum, column: 2, frame: excludedFrame, distinct: true,
				hasFilter: true, filterColumn: 3},
			{kind: windowAvg, column: 2, frame: frame},
			{kind: windowMin, column: 2, frame: frame},
			{kind: windowMax, column: 2, frame: frame},
			{kind: windowFirstValue, column: 2, frame: frame},
			{kind: windowLastValue, column: 2, frame: frame},
			{kind: windowNthValue, column: 2, nth: 3, frame: frame},
			{kind: windowFirstValue, column: 2, frame: excludedFrame,
				nullTreatment: windowIgnoreNulls},
			{kind: windowNthValue, column: 2, nth: 3, frame: excludedFrame,
				nullTreatment: windowIgnoreNulls, fromLast: true},
			{kind: windowCount, column: -1, frame: groupsFrame},
			{kind: windowSum, column: 2, frame: groupsFrame},
			{kind: windowCount, column: -1, frame: excludedFrame},
			{kind: windowMax, column: 2, frame: excludedFrame},
			{kind: windowSum, column: 2, frame: rangeFrame},
			{kind: windowFirstValue, column: 2, frame: rangeFrame},
		},
	}
}

func referenceWindowIntegerRows(
	t testing.TB,
	input *relationSpool,
	plan *windowPlan,
) [][]string {
	t.Helper()
	order := make([]int, input.rows)
	for row := range order {
		order[row] = row
	}
	slices.SortStableFunc(order, func(left, right int) int {
		comparison, err := compareWindowRows(input, plan, left, right, nil)
		if err != nil {
			panic(err)
		}
		return comparison
	})
	result := make([][]string, input.rows)
	for row := range result {
		result[row] = make([]string, len(plan.functions))
	}
	for partitionStart := 0; partitionStart < len(order); {
		partitionEnd := partitionStart + 1
		for partitionEnd < len(order) {
			equal, err := windowPartitionEqual(
				input, plan.partition, order[partitionStart], order[partitionEnd], nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !equal {
				break
			}
			partitionEnd++
		}
		groups := referenceWindowGroups(t, input, plan.order, order, partitionStart, partitionEnd)
		for position := partitionStart; position < partitionEnd; position++ {
			row := order[position]
			local := position - partitionStart
			rows := partitionEnd - partitionStart
			for column, function := range plan.functions {
				switch function.kind {
				case windowRowNumber:
					result[row][column] = fmt.Sprintf("%d", local+1)
				case windowRank, windowDenseRank, windowPercentRank:
					rank, dense := 1, 1
					for at := 1; at <= local; at++ {
						peer, err := windowPeers(
							input, plan.order, order[partitionStart+at-1], order[partitionStart+at], nil,
						)
						if err != nil {
							t.Fatal(err)
						}
						if !peer {
							rank = at + 1
							dense++
						}
					}
					if function.kind == windowDenseRank {
						rank = dense
					}
					if function.kind == windowPercentRank {
						result[row][column] = referenceWindowRatio(rank-1, max(rows-1, 1))
					} else {
						result[row][column] = fmt.Sprintf("%d", rank)
					}
				case windowCumeDist:
					end := local + 1
					for end < rows {
						peer, err := windowPeers(
							input, plan.order, order[position], order[partitionStart+end], nil,
						)
						if err != nil {
							t.Fatal(err)
						}
						if !peer {
							break
						}
						end++
					}
					result[row][column] = referenceWindowRatio(end, rows)
				case windowNTile:
					result[row][column] = fmt.Sprintf(
						"%d", referenceWindowTile(local, rows, function.buckets),
					)
				case windowLag, windowLead:
					target, ok := referenceWindowOffsetPosition(
						input, order, partitionStart, function, local, rows,
					)
					if !ok {
						if function.hasDefault {
							result[row][column] = windowScalarString(function.defaultVal)
						} else {
							result[row][column] = `null`
						}
					} else {
						result[row][column] = windowScalarString(
							input.columns[function.column][order[partitionStart+target]],
						)
					}
				case windowCount, windowSum, windowMin, windowMax:
					group := referenceWindowGroupAt(groups, local)
					lo, hi := referenceWindowFrame(
						input, plan.order, order, partitionStart,
						function.frame, local, rows, groups, group,
					)
					count, sum := 0, int64(0)
					var extreme scalar
					var distinct []scalar
					for at := lo; at < hi; at++ {
						if referenceWindowExcluded(function.frame, local, at, groups, group) {
							continue
						}
						row := order[partitionStart+at]
						if function.hasFilter {
							filter := input.columns[function.filterColumn][row]
							if filter.kind != kindBool || !filter.bval {
								continue
							}
						}
						if function.kind == windowCount && function.column < 0 {
							count++
							continue
						}
						value := input.columns[function.column][row]
						if value.kind == kindNull {
							continue
						}
						if function.distinct {
							duplicate := false
							for _, seen := range distinct {
								if compareScalar(seen, value) == 0 {
									duplicate = true
									break
								}
							}
							if duplicate {
								continue
							}
							distinct = append(distinct, value)
						}
						if function.kind == windowCount {
							count++
							continue
						}
						if value.kind != kindNumber || !value.isInt {
							continue
						}
						if count == 0 {
							extreme = value
						} else if function.kind == windowMin && compareScalar(value, extreme) < 0 ||
							function.kind == windowMax && compareScalar(value, extreme) > 0 {
							extreme = value
						}
						count++
						sum += value.ival
					}
					switch function.kind {
					case windowCount:
						result[row][column] = fmt.Sprintf("%d", count)
					case windowSum:
						if count == 0 {
							result[row][column] = `null`
						} else {
							result[row][column] = fmt.Sprintf("%d", sum)
						}
					default:
						if count == 0 {
							result[row][column] = `null`
						} else {
							result[row][column] = windowScalarString(extreme)
						}
					}
				case windowFirstValue, windowLastValue, windowNthValue:
					group := referenceWindowGroupAt(groups, local)
					lo, hi := referenceWindowFrame(
						input, plan.order, order, partitionStart,
						function.frame, local, rows, groups, group,
					)
					target, seen := -1, 0
					step, at, stop := 1, lo, hi
					fromLast := function.kind == windowLastValue ||
						(function.kind == windowNthValue && function.fromLast)
					if fromLast {
						step, at, stop = -1, hi-1, lo-1
					}
					for at != stop {
						if referenceWindowExcluded(function.frame, local, at, groups, group) {
							at += step
							continue
						}
						value := input.columns[function.column][order[partitionStart+at]]
						if function.nullTreatment == windowIgnoreNulls && value.kind == kindNull {
							at += step
							continue
						}
						seen++
						switch function.kind {
						case windowFirstValue, windowLastValue:
							target = at
							at = stop
						case windowNthValue:
							if seen == function.nth {
								target = at
								at = stop
							}
						}
						if at != stop {
							at += step
						}
					}
					if target < 0 {
						result[row][column] = `null`
					} else {
						result[row][column] = windowScalarString(
							input.columns[function.column][order[partitionStart+target]],
						)
					}
				}
			}
		}
		partitionStart = partitionEnd
	}
	return result
}

func referenceWindowOffsetPosition(
	input *relationSpool,
	order []int,
	partitionStart int,
	function windowFunctionSpec,
	position, rows int,
) (int, bool) {
	lead := function.kind == windowLead
	if function.nullTreatment == windowRespectNulls || function.offset == 0 {
		return windowOffsetPosition(position, rows, function.offset, lead)
	}
	step := -1
	if lead {
		step = 1
	}
	remaining := function.offset
	for at := position + step; at >= 0 && at < rows; at += step {
		value := input.columns[function.column][order[partitionStart+at]]
		if value.kind == kindNull {
			continue
		}
		remaining--
		if remaining == 0 {
			return at, true
		}
	}
	return 0, false
}

func windowScalarString(value scalar) string {
	if value.kind == kindNull && value.raw == nil {
		return setTestMissing
	}
	return string(value.raw)
}

func referenceWindowGroups(
	t testing.TB,
	input *relationSpool,
	keys []windowOrderKey,
	order []int,
	start, end int,
) []int {
	t.Helper()
	groups := []int{0}
	for position := start + 1; position < end; position++ {
		peer, err := windowPeers(input, keys, order[position-1], order[position], nil)
		if err != nil {
			t.Fatal(err)
		}
		if !peer {
			groups = append(groups, position-start)
		}
	}
	return append(groups, end-start)
}

func referenceWindowGroupAt(groups []int, position int) int {
	group := 0
	for group+1 < len(groups)-1 && position >= groups[group+1] {
		group++
	}
	return group
}

func referenceWindowTile(position, rows, buckets int) int {
	base, extra := rows/buckets, rows%buckets
	if base == 0 {
		return position + 1
	}
	largeRows := (base + 1) * extra
	if position < largeRows {
		return position/(base+1) + 1
	}
	return extra + (position-largeRows)/base + 1
}

func referenceWindowFrame(
	input *relationSpool,
	keys []windowOrderKey,
	order []int,
	partitionStart int,
	frame windowRowsFrame,
	position, rows int,
	groups []int,
	group int,
) (int, int) {
	if frame.unit == windowFrameRange {
		start := referenceWindowRangeBound(
			input, keys, order, partitionStart, frame.start,
			true, position, rows, groups, group,
		)
		end := referenceWindowRangeBound(
			input, keys, order, partitionStart, frame.end,
			false, position, rows, groups, group,
		)
		if end < start {
			end = start
		}
		return start, end
	}
	if frame.unit == windowFrameGroups {
		start := 0
		switch frame.start.kind {
		case windowPreceding:
			if frame.start.offset <= group {
				start = groups[group-frame.start.offset]
			}
		case windowCurrentRow:
			start = groups[group]
		case windowFollowing:
			if frame.start.offset >= len(groups)-1-group {
				start = rows
			} else {
				start = groups[group+frame.start.offset]
			}
		}
		end := 0
		switch frame.end.kind {
		case windowPreceding:
			if frame.end.offset <= group {
				end = groups[group-frame.end.offset+1]
			}
		case windowCurrentRow:
			end = groups[group+1]
		case windowFollowing:
			if frame.end.offset >= len(groups)-2-group {
				end = rows
			} else {
				end = groups[group+frame.end.offset+1]
			}
		case windowUnboundedFollowing:
			end = rows
		}
		if end < start {
			end = start
		}
		return start, end
	}

	start := 0
	switch frame.start.kind {
	case windowPreceding:
		if frame.start.offset <= position {
			start = position - frame.start.offset
		}
	case windowCurrentRow:
		start = position
	case windowFollowing:
		if frame.start.offset >= rows-position {
			start = rows
		} else {
			start = position + frame.start.offset
		}
	}
	end := 0
	switch frame.end.kind {
	case windowPreceding:
		if frame.end.offset <= position {
			end = position - frame.end.offset + 1
		}
	case windowCurrentRow:
		end = position + 1
	case windowFollowing:
		if frame.end.offset >= rows-1-position {
			end = rows
		} else {
			end = position + frame.end.offset + 1
		}
	case windowUnboundedFollowing:
		end = rows
	}
	if end < start {
		end = start
	}
	return start, end
}

func referenceWindowRangeBound(
	input *relationSpool,
	keys []windowOrderKey,
	order []int,
	partitionStart int,
	bound windowFrameBound,
	start bool,
	position, rows int,
	groups []int,
	group int,
) int {
	switch bound.kind {
	case windowUnboundedPreceding:
		return 0
	case windowCurrentRow:
		if start {
			return groups[group]
		}
		return groups[group+1]
	case windowUnboundedFollowing:
		return rows
	}
	key := keys[0]
	current := input.columns[key.column][order[partitionStart+position]]
	if current.kind == kindNull {
		if start {
			return groups[group]
		}
		return groups[group+1]
	}
	target := new(big.Rat).Set(ratOf(string(current.num)))
	offset := ratOf(string(bound.rangeOffset.num))
	subtract := bound.kind == windowPreceding
	if key.descending {
		subtract = !subtract
	}
	if subtract {
		target.Sub(target, offset)
	} else {
		target.Add(target, offset)
	}
	for at := 0; at < rows; at++ {
		candidate := input.columns[key.column][order[partitionStart+at]]
		comparison := referenceWindowRangeCompare(candidate, target, key)
		if comparison > 0 || start && comparison == 0 {
			return at
		}
	}
	return rows
}

func referenceWindowRangeCompare(value scalar, target *big.Rat, key windowOrderKey) int {
	if value.kind == kindNull {
		if key.nulls == windowNullsFirst {
			return -1
		}
		return 1
	}
	comparison := ratOf(string(value.num)).Cmp(target)
	if key.descending {
		comparison = -comparison
	}
	return comparison
}

func referenceWindowExcluded(
	frame windowRowsFrame,
	position, candidate int,
	groups []int,
	group int,
) bool {
	switch frame.exclusion {
	case windowExcludeCurrentRow:
		return candidate == position
	case windowExcludeGroup:
		return candidate >= groups[group] && candidate < groups[group+1]
	case windowExcludeTies:
		return candidate != position && candidate >= groups[group] && candidate < groups[group+1]
	default:
		return false
	}
}

func referenceWindowRatio(numerator, denominator int) string {
	if numerator == 0 {
		return "0"
	}
	if numerator == denominator {
		return "1"
	}
	remainder := numerator
	leading := 0
	digits := make([]byte, 0, averageDigits+1)
	for len(digits) < averageDigits+1 && remainder != 0 {
		remainder *= 10
		digit := remainder / denominator
		remainder %= denominator
		if len(digits) == 0 && digit == 0 {
			leading++
			continue
		}
		digits = append(digits, byte(digit)+'0')
	}
	if len(digits) > averageDigits {
		guard := digits[averageDigits]
		digits = digits[:averageDigits]
		if guard > '5' || guard == '5' &&
			(remainder != 0 || (digits[len(digits)-1]-'0')&1 != 0) {
			for at := len(digits) - 1; at >= 0; at-- {
				if digits[at] != '9' {
					digits[at]++
					break
				}
				digits[at] = '0'
			}
		}
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	return "0." + strings.Repeat("0", leading) + string(digits)
}
