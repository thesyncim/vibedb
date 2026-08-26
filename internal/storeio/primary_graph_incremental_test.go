package storeio

import (
	"bytes"
	"fmt"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

func TestPrimaryGraphLeafWindowPlannerMatchesBulkBoundary(t *testing.T) {
	pointer, err := vibejson.CompilePointer("/score")
	if err != nil {
		t.Fatal(err)
	}
	for _, placed := range []bool{false, true} {
		rows := 1200
		if placed {
			rows = CommonPrimaryLeafWideSlots
		}
		records := make([]PrimaryGraphRecord, rows)
		for row := range records {
			key := []byte(fmt.Sprintf("key-%08d", row))
			value := []byte(fmt.Sprintf(
				`{"score":%d,"label":"value-%08d-%s"}`,
				row, row, bytes.Repeat([]byte{'x'}, row%47),
			))
			records[row] = BorrowPrimaryGraphRecord(key, value)
		}
		for _, maxExtent := range []int{16 << 10, CommonPrimaryLeafMaxExtentBytes} {
			planner, err := NewPrimaryGraphLeafWindowPlanner(
				placed, []vibejson.CompiledPointer{pointer},
			)
			if err != nil {
				t.Fatal(err)
			}
			count, extent, payload, err := planner.Plan(records, maxExtent)
			if err != nil {
				t.Fatal(err)
			}
			maxRows := CompactPrimaryStripeMaxRows
			if placed {
				maxRows = CommonPrimaryLeafWideSlots
			}
			bulk, err := planCompactPrimaryLeavesSummarized(
				testStoreID, records, maxRows, maxExtent,
				[]vibejson.CompiledPointer{pointer},
			)
			if err != nil {
				t.Fatal(err)
			}
			first := bulk[0]
			if count != first.last-first.first || extent != first.extent {
				t.Fatalf(
					"placed=%v extent=%d got count/extent %d/%d, want %d/%d",
					placed, maxExtent, count, extent,
					first.last-first.first, first.extent,
				)
			}
			builder := NewUnifiedPrimaryLeafBuilder()
			if err := builder.SetCompactPrimarySummaries(
				[]vibejson.CompiledPointer{pointer},
			); err != nil {
				t.Fatal(err)
			}
			if err := prepareCompactPrimaryGraphStripe(records, placed, builder); err != nil {
				t.Fatal(err)
			}
			want, err := buildPreparedCompactPrimaryGraphStripePayload(
				records[:count], builder,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, want) {
				t.Fatalf("placed=%v extent=%d payload differs", placed, maxExtent)
			}
		}
	}
}

func TestPrimaryGraphLeafWindowPlannerWarmAllocationBound(t *testing.T) {
	records := make([]PrimaryGraphRecord, CommonPrimaryLeafWideSlots)
	for row := range records {
		records[row] = BorrowPrimaryGraphRecord(
			[]byte(fmt.Sprintf("key-%08d", row)),
			[]byte(fmt.Sprintf(`{"score":%d,"enabled":true}`, row)),
		)
	}
	planner, err := NewPrimaryGraphLeafWindowPlanner(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, _, _, err := planner.Plan(records, CommonPrimaryLeafMaxExtentBytes); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, err := planner.Plan(records, CommonPrimaryLeafMaxExtentBytes); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm planner allocs/run = %.2f, want 0", allocs)
	}
}
