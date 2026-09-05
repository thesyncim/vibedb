package storeio

import (
	"fmt"
	"testing"
)

func TestCompactProjectionRejectedOutputNeverFallsBackReview(t *testing.T) {
	// An unsupported output is irrelevant until the predicate accepts its row.
	// This detects eager whole-shape rejection even when final values are right.
	records := make([]CommonPrimaryLeafRecord, 10)
	for row := range records {
		output := fmt.Sprintf(`{"nested":%d}`, row)
		if row == 9 {
			output = "123"
		}
		records[row] = CommonPrimaryLeafRecord{
			Key:   fmt.Appendf(nil, "row-%03d", row),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(nil, `{"n":%d,"output":%s}`, row, output)},
		}
	}
	view := compactProjectionTestView(t, records)
	f, err := NewUnifiedProjectionFilter([][]byte{[]byte("/n"), []byte("/output")})
	if err != nil {
		t.Fatal(err)
	}
	cursor := PrimaryGraphCursor{leaf: view, depth: 1}
	defer cursor.Close()
	cursor.spliceScratch = make([]byte, 0, 256)
	var progress UnifiedProjectionProgress
	matches, visits, fallbacks := 0, 0, 0
	supported, stopped, _, err := cursor.VisitProjectedMatch(
		f, &progress, 1,
		make([]int, view.shapeCount), make([]UnifiedProjectionShapeWorkspace, view.shapeCount),
		make([]UnifiedProjectionStreamWorkspace, 2*view.shapeCount), make([]UnifiedProjectionField, 2),
		make([]byte, 0, 256), 1,
		func(row uint64, fields []UnifiedProjectionField) (bool, error) {
			matches++
			if len(fields) != 1 || string(fields[0].AppendJSON(nil)) != fmt.Sprint(row) {
				t.Fatalf("filter at %d: %v", row, fields)
			}
			return row == 9, nil
		},
		func(row uint64, fields []UnifiedProjectionField) error {
			visits++
			if row != 9 || string(fields[1].AppendJSON(nil)) != "123" {
				t.Fatalf("output at %d: %v", row, fields)
			}
			return nil
		},
		func(row uint64, raw []byte, ref PageRef) (bool, error) {
			fallbacks++
			return row == 9, nil
		},
	)
	if err != nil || !supported || !stopped || matches != 10 || visits != 1 || fallbacks != 0 || progress.Scanned != 10 {
		t.Fatalf("supported=%t stopped=%t matches=%d visits=%d fallbacks=%d progress=%+v err=%v", supported, stopped, matches, visits, fallbacks, progress, err)
	}
}
