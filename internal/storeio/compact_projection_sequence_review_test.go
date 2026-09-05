package storeio

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompactProjectionSequenceReview(t *testing.T) {
	const rows = 192
	records := make([]CommonPrimaryLeafRecord, rows)
	values := make([]string, rows)
	for row := range records {
		values[row] = fmt.Sprintf("%s%d-tail", strings.Repeat("prefix-", 12), row%37)
		value := fmt.Appendf(nil, `{"text":%q,"n":%d}`, values[row], row*7919%1024)
		if row%2 != 0 {
			value = fmt.Appendf(nil, `{"n":%d,"extra":true,"text":%q}`, row*7919%1024, values[row])
		}
		records[row] = CommonPrimaryLeafRecord{Key: fmt.Appendf(nil, "%05d", row), Value: CommonPrimaryLeafValue{Inline: value}}
	}
	view := compactProjectionTestView(t, records)
	f, err := NewUnifiedProjectionFilter([][]byte{[]byte("/text"), []byte("/n")})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]int, view.shapeCount)
	shapes := make([]UnifiedProjectionShapeWorkspace, view.shapeCount)
	streams := make([]UnifiedProjectionStreamWorkspace, 2*view.shapeCount)
	fields := make([]UnifiedProjectionField, 2)
	scratch := make([]byte, 0, 256)
	// Reuse state across backward starts, restarts and interleaved shapes.
	for _, start := range []int{0, 63, 127, 1, 64, 65, 126, 128, 31, 191} {
		calls := 0
		ok, _, _, err := view.visitResolvedProjectionRange(start, rows, false,
			f.resolvers, seen, shapes, streams, fields, scratch,
			func(row int, fields []UnifiedProjectionField) error {
				calls++
				if row != start+calls-1 || string(fields[0].AppendJSON(nil)) != fmt.Sprintf("%q", values[row]) ||
					string(fields[1].AppendJSON(nil)) != fmt.Sprint(row*7919%1024) {
					t.Fatalf("incorrect sequential result at start=%d row=%d", start, row)
				}
				return nil
			})
		if !ok || err != nil || calls != rows-start {
			t.Fatalf("start=%d supported=%v calls=%d err=%v", start, ok, calls, err)
		}
		if streams[0].view.kind != compactStreamPrefixInt && streams[2].view.kind != compactStreamPrefixInt {
			t.Fatalf("fixture missed prefix encoding: %d/%d", streams[0].view.kind, streams[2].view.kind)
		}
	}
}
