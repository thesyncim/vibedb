package query

import (
	"fmt"
	"testing"
)

func BenchmarkSetKernel(b *testing.B) {
	leftRows := make([][]string, 2048)
	for row := range leftRows {
		leftRows[row] = []string{
			fmt.Sprintf("%d.00", row%257),
			fmt.Sprintf("%q", fmt.Sprintf("group-%d", row%127)),
			fmt.Sprintf(`{"bucket":%d}`, row%31),
		}
	}
	rightRows := make([][]string, 1536)
	for row := range rightRows {
		rightRows[row] = []string{
			fmt.Sprintf("%de0", row%257),
			fmt.Sprintf("%q", fmt.Sprintf("group-%d", row%127)),
			fmt.Sprintf(`{"bucket":%d}`, row%31),
		}
	}
	left := buildSetTestSpool(b, leftRows)
	right := buildSetTestSpool(b, rightRows)
	tests := [...]struct {
		name string
		op   setOperation
	}{
		{"union_all", setUnionAll},
		{"union_distinct", setUnionDistinct},
		{"intersect_all", setIntersectAll},
		{"intersect_distinct", setIntersectDistinct},
		{"except_all", setExceptAll},
		{"except_distinct", setExceptDistinct},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			var executor setExecutor
			var frame statementFrame
			if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
				b.Fatal(err)
			}
			charge, err := executor.execute(test.op, &left, &right, &frame, nil)
			if err != nil {
				b.Fatal(err)
			}
			frame.intermediate.release(charge)
			b.ReportAllocs()
			b.SetBytes(int64(len(left.data) + len(right.data)))
			b.ResetTimer()
			for range b.N {
				charge, err = executor.execute(test.op, &left, &right, &frame, nil)
				if err != nil {
					b.Fatal(err)
				}
				setSink += executor.result.rows
				frame.intermediate.release(charge)
			}
		})
	}
}
