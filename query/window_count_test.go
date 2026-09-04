package query

import (
	"fmt"
	"testing"
)

// Compare prefix counts with the row-by-row oracle across empty frames,
// clipped peer exclusions, filters, null/missing values, and reused scratch.
func TestWindowCountFrameDifferential(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `2`, `null`, `true`}, {`0`, `1`, `"x"`, `true`},
		{`1`, `2`, `7`, `false`}, {`0`, `1.0`, `0`, `true`},
		{`0`, `2`, setTestMissing, `true`}, {`1`, `3`, `false`, `null`},
		{`0`, `1e0`, `[]`, `true`}, {`1`, `null`, `9`, setTestMissing},
	})
	var executor windowExecutor
	defer executor.release()
	var execution statementFrame
	if err := execution.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []windowFrameUnit{windowFrameRows, windowFrameGroups, windowFrameRange} {
		bounds := []windowFrameBound{
			{kind: windowUnboundedPreceding}, {kind: windowPreceding, offset: 2},
			{kind: windowCurrentRow}, {kind: windowFollowing, offset: 2},
			{kind: windowUnboundedFollowing},
		}
		if unit == windowFrameRange {
			bounds[1] = windowFrameBound{kind: windowPreceding, rangeOffset: windowTestScalar(t, `2`)}
			bounds[3] = windowFrameBound{kind: windowFollowing, rangeOffset: windowTestScalar(t, `2`)}
		}
		for start := 0; start < len(bounds)-1; start++ {
			for end := max(start, 1); end < len(bounds); end++ {
				for exclusion := windowExcludeNoOthers; exclusion <= windowExcludeTies; exclusion++ {
					for _, descending := range []bool{false, true} {
						t.Run(fmt.Sprintf("unit=%d/start=%d/end=%d/exclude=%d/desc=%t", unit, start, end, exclusion, descending), func(t *testing.T) {
							frame := windowRowsFrame{unit: unit, start: bounds[start], end: bounds[end], exclusion: exclusion}
							plan := windowPlan{partition: []int{0}, order: []windowOrderKey{{column: 1, descending: descending}}, functions: []windowFunctionSpec{
								{kind: windowCount, column: 2, hasFilter: true, filterColumn: 3, frame: frame},
								{kind: windowCount, column: -1, frame: frame},
								{kind: windowCount, column: 2, frame: frame},
								{kind: windowCount, column: -1, hasFilter: true, filterColumn: 3, frame: frame},
								{kind: windowRank, column: -1},
							}}
							charge, err := executor.execute(&input, &plan, &execution, nil)
							if err != nil {
								t.Fatal(err)
							}
							execution.intermediate.release(charge)
							assertSetRows(t, executor.relation(), referenceWindowIntegerRows(t, &input, &plan))
						})
					}
				}
			}
		}
	}
}

func TestWindowOrderedInputAndLateInversion(t *testing.T) {
	for _, keys := range [][]string{
		{`1`, `1.0`, `2`, `3`, `null`}, // Already ordered, with stable exact-number peers.
		{`1`, `1.0`, `2`, `null`, `3`}, // Last pair inverted; must still sort.
		{`3`, `2`, `1`, `1.0`, `null`}, // Ordered only under DESC NULLS LAST.
	} {
		data := make([][]string, len(keys))
		for row, key := range keys {
			data[row] = []string{key, fmt.Sprint(row)}
		}
		input := buildSetTestSpool(t, data)
		for _, descending := range []bool{false, true} {
			plan := windowPlan{order: []windowOrderKey{{column: 0, descending: descending, nulls: windowNullsLast}}, functions: []windowFunctionSpec{
				{kind: windowRowNumber, column: -1}, {kind: windowRank, column: -1},
				{kind: windowLag, column: 1, offset: 1},
			}}
			assertSetRows(t, runWindowTest(t, &input, &plan), referenceWindowIntegerRows(t, &input, &plan))
		}
	}
}
