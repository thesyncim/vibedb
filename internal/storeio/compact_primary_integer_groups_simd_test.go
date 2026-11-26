package storeio

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestCompactIntegerGroupsSIMDUnpackParity(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	for range 256 {
		var src16 [16]uint8
		for i := range src16 {
			src16[i] = uint8(rng.Uint32())
		}
		var got4, want4 [32]uint8
		compactUnpackPacked4x32SIMD(&got4, &src16)
		compactUnpackPacked4x32Scalar(&want4, &src16)
		if got4 != want4 {
			t.Fatalf("unpack4 mismatch:\n got=%v\nwant=%v", got4, want4)
		}
		var got7, want7 [16]uint16
		compactUnpackPacked7x16SIMD(&got7, &src16)
		compactUnpackPacked7x16Scalar(&want7, &src16)
		if got7 != want7 {
			t.Fatalf("unpack7 mismatch:\n got=%v\nwant=%v", got7, want7)
		}
		var src40 [40]uint8
		for i := range src40 {
			src40[i] = uint8(rng.Uint32())
		}
		var got10, want10 [32]uint16
		compactUnpackPacked10x32SIMD(&got10, &src40)
		compactUnpackPacked10x32Scalar(&want10, &src40)
		if got10 != want10 {
			t.Fatalf("unpack10 mismatch:\n got=%v\nwant=%v", got10, want10)
		}
	}
	for _, count := range []int{0, 1, 2, 15, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256} {
		data := make([]byte, (count+1)/2)
		for limit := 1; limit <= 16; limit++ {
			for i := range data {
				lo := uint8(rng.IntN(limit))
				hi := uint8(rng.IntN(limit))
				data[i] = lo | (hi << 4)
			}
			if count&1 != 0 {
				data[len(data)-1] &= 0x0f
			}
			got := compactPacked4MaxBelowSIMD(data, count, limit)
			want := compactPacked4MaxBelowScalar(data, count, limit)
			if got != want || !got {
				t.Fatalf("count=%d limit=%d valid: got=%t want=%t", count, limit, got, want)
			}
			if count > 0 && limit < 16 {
				pos := rng.IntN(count)
				if pos&1 == 0 {
					data[pos>>1] = (data[pos>>1] & 0xf0) | uint8(limit)
				} else {
					data[pos>>1] = (data[pos>>1] & 0x0f) | (uint8(limit) << 4)
				}
				got := compactPacked4MaxBelowSIMD(data, count, limit)
				want := compactPacked4MaxBelowScalar(data, count, limit)
				if got != want || got {
					t.Fatalf("count=%d limit=%d pos=%d invalid: got=%t want=%t", count, limit, pos, got, want)
				}
			}
		}
	}
}

func TestVisitResolvedIntegerGroupsBatchMatchesScalarAllWidths(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rows      int
		groups    int
		sumModulo int64
	}{
		{name: "dict4_for10_256", rows: 256, groups: 16, sumModulo: 1000},
		{name: "dict4_nonpow2_129", rows: 129, groups: 11, sumModulo: 120},
		{name: "dict7_for16_257", rows: 257, groups: 100, sumModulo: 50000},
		{name: "dict8_for8_256", rows: 256, groups: 200, sumModulo: 250},
		{name: "dict1_for0_64", rows: 64, groups: 1, sumModulo: 1},
		{name: "dict3_for12_95", rows: 95, groups: 7, sumModulo: 3000},
		{name: "dict2_for32_100", rows: 100, groups: 3, sumModulo: 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := make([]CommonPrimaryLeafRecord, tc.rows)
			wantGroup := make([]int64, tc.rows)
			wantSum := make([]int64, tc.rows)
			for i := range tc.rows {
				g := int64((i*7 + 3) % tc.groups)
				s := int64(1000) + int64((i*31+17)%int(tc.sumModulo))
				wantGroup[i] = g
				wantSum[i] = s
				records[i] = CommonPrimaryLeafRecord{
					Key:   []byte(fmt.Sprintf("k-%06d", i)),
					Value: CommonPrimaryLeafValue{Inline: []byte(fmt.Sprintf(`{"g":%d,"s":%d}`, g, s))},
				}
			}
			view := compactProjectionTestView(t, records)
			groupRes := &UnifiedHoleResolver{}
			sumRes := &UnifiedHoleResolver{}
			if err := groupRes.SetPath([]byte("/g")); err != nil {
				t.Fatal(err)
			}
			if err := sumRes.SetPath([]byte("/s")); err != nil {
				t.Fatal(err)
			}
			seen := make([]int, max(128, view.shapeCount))
			shapes := make([]IntegerGroupShapeWorkspace, max(1, view.shapeCount))
			var batch IntegerGroupBatch
			var gotRows int
			ok, err := view.VisitResolvedIntegerGroupsBatch(groupRes, sumRes, seen, shapes, &batch, func(startRow int, groups, sums []int64) error {
				if startRow != gotRows {
					t.Fatalf("startRow=%d want=%d", startRow, gotRows)
				}
				for i := range groups {
					if groups[i] != wantGroup[startRow+i] || sums[i] != wantSum[startRow+i] {
						t.Fatalf("row %d: got (%d,%d) want (%d,%d)", startRow+i, groups[i], sums[i], wantGroup[startRow+i], wantSum[startRow+i])
					}
				}
				gotRows += len(groups)
				return nil
			})
			if err != nil || !ok || gotRows != tc.rows {
				t.Fatalf("batch ok=%t rows=%d want=%d err=%v", ok, gotRows, tc.rows, err)
			}
			allocs := testing.AllocsPerRun(50, func() {
				ok, err := view.VisitResolvedIntegerGroupsBatch(groupRes, sumRes, seen, shapes, &batch, func(_ int, _, _ []int64) error {
					return nil
				})
				if err != nil || !ok {
					t.Fatalf("ok=%t err=%v", ok, err)
				}
			})
			if allocs != 0 {
				t.Fatalf("VisitResolvedIntegerGroupsBatch allocated %g times", allocs)
			}
		})
	}
}

func BenchmarkVisitResolvedIntegerGroups256Dict4FOR10(b *testing.B) {
	records := make([]CommonPrimaryLeafRecord, 256)
	for i := range records {
		g := (i*7 + 3) % 16
		s := 1000 + (i*31+17)%1000
		records[i] = CommonPrimaryLeafRecord{
			Key:   []byte(fmt.Sprintf("k-%06d", i)),
			Value: CommonPrimaryLeafValue{Inline: []byte(fmt.Sprintf(`{"g":%d,"s":%d}`, g, s))},
		}
	}
	view := compactProjectionTestView(b, records)
	groupRes := &UnifiedHoleResolver{}
	sumRes := &UnifiedHoleResolver{}
	_ = groupRes.SetPath([]byte("/g"))
	_ = sumRes.SetPath([]byte("/s"))
	seen := make([]int, 128)
	shapes := make([]IntegerGroupShapeWorkspace, 1)
	var batch IntegerGroupBatch
	var sink int64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ok, err := view.VisitResolvedIntegerGroupsBatch(groupRes, sumRes, seen, shapes, &batch, func(_ int, groups, sums []int64) error {
			for i := range groups {
				sink += groups[i] + sums[i]
			}
			return nil
		})
		if err != nil || !ok {
			b.Fatalf("ok=%t err=%v", ok, err)
		}
	}
	if sink == 0 {
		b.Fatal("unexpected zero sink")
	}
}

func BenchmarkVisitResolvedIntegerGroups256Dict4TrueFOR10(b *testing.B) {
	rng := rand.New(rand.NewPCG(7, 13))
	records := make([]CommonPrimaryLeafRecord, 256)
	for i := range records {
		g := rng.IntN(16)
		s := 1000 + rng.IntN(1000)
		records[i] = CommonPrimaryLeafRecord{
			Key:   []byte(fmt.Sprintf("k-%06d", i)),
			Value: CommonPrimaryLeafValue{Inline: []byte(fmt.Sprintf(`{"g":%d,"s":%d}`, g, s))},
		}
	}
	view := compactProjectionTestView(b, records)
	groupRes := &UnifiedHoleResolver{}
	sumRes := &UnifiedHoleResolver{}
	_ = groupRes.SetPath([]byte("/g"))
	_ = sumRes.SetPath([]byte("/s"))
	seen := make([]int, 128)
	shapes := make([]IntegerGroupShapeWorkspace, 1)
	var batch IntegerGroupBatch
	var sink int64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ok, err := view.VisitResolvedIntegerGroupsBatch(groupRes, sumRes, seen, shapes, &batch, func(_ int, groups, sums []int64) error {
			for i := range groups {
				sink += groups[i] + sums[i]
			}
			return nil
		})
		if err != nil || !ok {
			b.Fatalf("ok=%t err=%v", ok, err)
		}
	}
	if sink == 0 {
		b.Fatal("unexpected zero sink")
	}
}
