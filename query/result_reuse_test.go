package query

import (
	"testing"
)

func TestResultReuseScrubsFullCapacityAndEnforcesCap(t *testing.T) {
	columns := make([]ResultColumn, 2, 3)
	all := columns[:cap(columns)]
	for i := range all {
		all[i] = ResultColumn{Header: "borrowed", Cells: make([]Cell, 1, 3)}
		cells := all[i].Cells[:cap(all[i].Cells)]
		for j := range cells {
			cells[j] = Cell{raw: []byte("borrowed cell")}
		}
	}
	result := Result{Columns: columns, RowCount: 1, fileData: make([]byte, 20, 32), rootIntermediate: &intermediateBudget{}, resultBytesUsed: 99}
	size := result.ReuseCapacityBytes()
	want := 3*resultColumnBytes + 9*resultCellBytes + 32
	if size != want {
		t.Fatalf("capacity=%d want %d", size, want)
	}
	result.ResetForReuse(size)
	if len(result.Columns) != 0 || result.RowCount != 0 || result.RetainedBytes() != 0 || result.rootIntermediate != nil || result.ReuseCapacityBytes() != size {
		t.Fatal("reset lost capacity or retained execution state")
	}
	for _, col := range result.Columns[:cap(result.Columns)] {
		if col.Header != "" || len(col.Cells) != 0 {
			t.Fatal("retained borrowed metadata")
		}
		for _, cell := range col.Cells[:cap(col.Cells)] {
			if cell.raw != nil {
				t.Fatal("retained borrowed cell past length")
			}
		}
	}
	result.ResetForReuse(size - 1)
	if result.ReuseCapacityBytes() != 0 || result.Columns != nil || result.fileData != nil {
		t.Fatal("oversized result retained")
	}
}
