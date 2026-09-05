package storeio

import (
	"fmt"
	"testing"
)

func TestCompactProjectionPreparesOnlyVisitedShapesReview(t *testing.T) {
	records := make([]CommonPrimaryLeafRecord, 12)
	for row := range records {
		records[row] = CommonPrimaryLeafRecord{
			Key: fmt.Appendf(nil, "row-%03d", row),
			Value: CommonPrimaryLeafValue{Inline: fmt.Appendf(nil,
				`{"id":%d,"shape%d":null}`, row, row/4)},
		}
	}
	view := compactProjectionTestView(t, records)
	if view.shapeCount != 3 {
		t.Fatalf("shapes=%d, want three", view.shapeCount)
	}
	f, err := NewUnifiedProjectionFilter([][]byte{[]byte("/id")})
	if err != nil {
		t.Fatal(err)
	}
	for _, start := range []int{0, 5, 9} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			seen := make([]int, view.shapeCount)
			shapes := make([]UnifiedProjectionShapeWorkspace, view.shapeCount)
			streams := make([]UnifiedProjectionStreamWorkspace, view.shapeCount)
			fields := make([]UnifiedProjectionField, 1)
			scratch := make([]byte, 0, 128)
			cursor := PrimaryGraphCursor{leaf: view, row: start, depth: 1}
			defer cursor.Close()
			var progress UnifiedProjectionProgress
			calls := 0
			ok, stopped, _, err := cursor.VisitProjected(f, &progress,
				seen, shapes, streams, fields, scratch, 1,
				func(row uint64, fields []UnifiedProjectionField) error {
					calls++
					if row != 0 || string(fields[0].AppendJSON(nil)) != fmt.Sprint(start) {
						t.Fatalf("row=%d value=%s, want %d", row, fields[0].AppendJSON(nil), start)
					}
					return nil
				})
			if !ok || !stopped || err != nil || calls != 1 {
				t.Fatalf("ok=%v stopped=%v calls=%d err=%v", ok, stopped, calls, err)
			}
			prepared := 0
			for _, shape := range shapes {
				if shape.prepared {
					prepared++
				}
			}
			if prepared != 1 {
				t.Fatalf("prepared %d shapes for one visited row", prepared)
			}
		})
	}
}
