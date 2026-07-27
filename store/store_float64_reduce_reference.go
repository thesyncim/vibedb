package store

import (
	"encoding/binary"
	"math"
)

// reducePackedFloat64LEReference defines the exact four-lane accumulation
// order used by every build. Min/max retain stable-slot order, including the
// existing signed-zero behavior. Values are finite by typed page admission.
func reducePackedFloat64LEReference(values []byte) packedFloat64Summary {
	count := len(values) / 8
	if count == 0 {
		return packedFloat64Summary{}
	}
	first := math.Float64frombits(binary.LittleEndian.Uint64(values[:8]))
	summary := packedFloat64Summary{count: count, min: first, max: first}
	for offset := 8; offset < len(values); offset += 8 {
		value := math.Float64frombits(binary.LittleEndian.Uint64(values[offset : offset+8]))
		if value < summary.min {
			summary.min = value
		}
		if value > summary.max {
			summary.max = value
		}
	}
	var lanes [4]float64
	offset := 0
	for ; offset+32 <= len(values); offset += 32 {
		lanes[0] += math.Float64frombits(binary.LittleEndian.Uint64(values[offset : offset+8]))
		lanes[1] += math.Float64frombits(binary.LittleEndian.Uint64(values[offset+8 : offset+16]))
		lanes[2] += math.Float64frombits(binary.LittleEndian.Uint64(values[offset+16 : offset+24]))
		lanes[3] += math.Float64frombits(binary.LittleEndian.Uint64(values[offset+24 : offset+32]))
	}
	summary.sum = (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
	for ; offset < len(values); offset += 8 {
		summary.sum += math.Float64frombits(binary.LittleEndian.Uint64(values[offset : offset+8]))
	}
	return summary
}
