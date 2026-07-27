package store

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

func TestPackedFloat64ReductionMatchesReferenceBits(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	values := make([]float64, 257)
	for i := range values {
		values[i] = math.Ldexp(random.Float64()*2-1, random.Intn(160)-80)
	}
	copy(values, []float64{
		0, math.Copysign(0, -1), math.SmallestNonzeroFloat64,
		-math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64,
	})
	encoded := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(encoded[i*8:(i+1)*8], math.Float64bits(value))
	}
	for count := 0; count <= len(values); count++ {
		src := encoded[:count*8]
		got := reducePackedFloat64LE(src)
		want := reducePackedFloat64LEReference(src)
		if got.count != want.count ||
			math.Float64bits(got.sum) != math.Float64bits(want.sum) ||
			math.Float64bits(got.min) != math.Float64bits(want.min) ||
			math.Float64bits(got.max) != math.Float64bits(want.max) {
			t.Fatalf("count %d = %+v, want %+v", count, got, want)
		}
	}
}

func TestPackedUnsignedReductionMatchesFloat64ReferenceBitsAndAllocs(t *testing.T) {
	for _, width := range []int{1, 2, 4} {
		for count := 0; count <= 257; count++ {
			packed := make([]byte, count*width)
			float64LE := make([]byte, count*8)
			for index := 0; index < count; index++ {
				value := uint32(index*7919 + index%13)
				switch width {
				case 1:
					value &= math.MaxUint8
					packed[index] = byte(value)
				case 2:
					value &= math.MaxUint16
					binary.LittleEndian.PutUint16(packed[index*2:], uint16(value))
				case 4:
					binary.LittleEndian.PutUint32(packed[index*4:], value)
				}
				binary.LittleEndian.PutUint64(
					float64LE[index*8:], math.Float64bits(float64(value)),
				)
			}
			got := reducePackedUnsignedLE(packed, width)
			want := reducePackedFloat64LEReference(float64LE)
			if got.count != want.count ||
				math.Float64bits(got.sum) != math.Float64bits(want.sum) ||
				math.Float64bits(got.min) != math.Float64bits(want.min) ||
				math.Float64bits(got.max) != math.Float64bits(want.max) {
				t.Fatalf("width %d count %d = %+v, want %+v", width, count, got, want)
			}
		}
		values := make([]byte, 257*width)
		if allocs := testing.AllocsPerRun(100, func() {
			summary := reducePackedUnsignedLE(values, width)
			if summary.count != 257 {
				panic("unsigned reduction count")
			}
		}); allocs != 0 {
			t.Fatalf("width %d warm allocations = %.2f, want zero", width, allocs)
		}
	}
}
