package query

import "testing"

func FuzzSetKernelAgainstReference(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 3, 3, 0, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{3, 8, 7, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add([]byte{2, 15, 15, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5})

	f.Fuzz(func(t *testing.T, data []byte) {
		values := [...]string{
			setTestMissing, `null`, `false`, `true`, `0`, `-0.0`,
			`1`, `1.00`, `10e-1`, `9007199254740992`,
			`9007199254740993`, `"a"`, `"a\n"`, `[]`, `[1]`,
			`{"x":1}`, `{ "x" : 1 }`,
		}
		at := 0
		next := func() byte {
			if at == len(data) {
				return 0
			}
			value := data[at]
			at++
			return value
		}
		columns := 1 + int(next()%3)
		leftRows := int(next() % 17)
		rightRows := int(next() % 17)
		decode := func(rows int) [][]string {
			decoded := make([][]string, rows)
			for row := range decoded {
				decoded[row] = make([]string, columns)
				for column := range decoded[row] {
					decoded[row][column] = values[int(next())%len(values)]
				}
			}
			return decoded
		}
		leftRowsData := decode(leftRows)
		rightRowsData := decode(rightRows)
		left := buildSetTestSpoolColumns(t, leftRowsData, columns)
		right := buildSetTestSpoolColumns(t, rightRowsData, columns)

		var executor setExecutor
		for op := setUnionAll; op <= setExceptDistinct; op++ {
			var frame statementFrame
			if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
				t.Fatal(err)
			}
			charge, err := executor.execute(op, &left, &right, &frame, nil)
			if err != nil {
				t.Fatalf("op %d: %v", op, err)
			}
			frame.intermediate.release(charge)
			want := rowsForSetRefs(referenceSetRows(op, &left, &right), &left, &right)
			if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
				t.Fatalf("op %d\nleft=%v\nright=%v\ngot=%v\nwant=%v",
					op, leftRowsData, rightRowsData, got, want)
			}
		}
	})
}
