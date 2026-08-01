package query

import "testing"

func FuzzWindowKernelAgainstReference(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{8, 1, 0, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{16, 3, 2, 1, 0, 255, 127, 64, 32, 16, 8, 4, 2, 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		at := 0
		next := func() byte {
			if at == len(data) {
				return 0
			}
			value := data[at]
			at++
			return value
		}
		rows := int(next() % 17)
		partitions := [...]string{`0`, `1`, `2`, `null`}
		orders := [...]string{
			setTestMissing, `null`, `-2`, `-1`, `0`, `1`, `1.0`, `2`,
		}
		values := [...]string{`null`, `-9`, `-3`, `-1`, `0`, `2`, `4`, `8`}
		decoded := make([][]string, rows)
		for row := range decoded {
			decoded[row] = []string{
				partitions[int(next())%len(partitions)],
				orders[int(next())%len(orders)],
				values[int(next())%len(values)],
			}
		}
		input := buildSetTestSpoolColumns(t, decoded, 3)
		frameSpec := windowRowsFrame{
			start: windowFrameBound{kind: windowPreceding, offset: int(next() % 5)},
			end:   windowFrameBound{kind: windowFollowing, offset: int(next() % 5)},
		}
		plan := windowPlan{
			partition: []int{0},
			order: []windowOrderKey{{
				column: 1, descending: next()&1 != 0,
				nulls: windowNullOrder(next() & 1),
			}},
			functions: []windowFunctionSpec{
				{kind: windowRowNumber, column: -1},
				{kind: windowRank, column: -1},
				{kind: windowDenseRank, column: -1},
				{kind: windowLag, column: 2, offset: int(next() % 5)},
				{kind: windowLead, column: 2, offset: int(next() % 5)},
				{kind: windowCount, column: -1, frame: frameSpec},
				{kind: windowCount, column: 2, frame: frameSpec},
				{kind: windowSum, column: 2, frame: frameSpec},
				{kind: windowMin, column: 2, frame: frameSpec},
				{kind: windowMax, column: 2, frame: frameSpec},
			},
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
		want := referenceWindowIntegerRows(t, &input, &plan)
		if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
			t.Fatalf("rows=%v\ngot=%v\nwant=%v", decoded, got, want)
		}
	})
}
