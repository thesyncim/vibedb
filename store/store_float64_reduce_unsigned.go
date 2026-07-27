package store

import "encoding/binary"

// AddPackedFloat64Width reduces one exact dense unsigned or float64 run. The
// integer encodings preserve the same four-lane accumulation order as the
// float64 kernel, so adaptive storage cannot change aggregate result bits.
func (a *Float64Aggregate) AddPackedFloat64Width(values []byte, width int) bool {
	if width == 8 {
		return a.addPackedFloat64LE(values)
	}
	if a == nil || width != 1 && width != 2 && width != 4 || len(values)%width != 0 {
		return false
	}
	summary := reducePackedUnsignedLE(values, width)
	if summary.count == 0 {
		return true
	}
	if a.Count == 0 {
		a.Min, a.Max = summary.min, summary.max
	} else {
		if summary.min < a.Min {
			a.Min = summary.min
		}
		if summary.max > a.Max {
			a.Max = summary.max
		}
	}
	a.Count += summary.count
	a.Sum += summary.sum
	return true
}

func reducePackedUnsignedLE(values []byte, width int) packedFloat64Summary {
	switch width {
	case 1:
		return reducePackedUint8(values)
	case 2:
		if len(values)&1 == 0 {
			return reducePackedUint16LE(values)
		}
	case 4:
		if len(values)&3 == 0 {
			return reducePackedUint32LE(values)
		}
	}
	return packedFloat64Summary{}
}

func reducePackedUint8(values []byte) packedFloat64Summary {
	if len(values) == 0 {
		return packedFloat64Summary{}
	}
	var lanes [4]float64
	summary := packedFloat64Summary{
		count: len(values), min: float64(values[0]), max: float64(values[0]),
	}
	offset := 0
	for ; offset+4 <= len(values); offset += 4 {
		v0, v1 := float64(values[offset]), float64(values[offset+1])
		v2, v3 := float64(values[offset+2]), float64(values[offset+3])
		lanes[0] += v0
		lanes[1] += v1
		lanes[2] += v2
		lanes[3] += v3
		if v0 < summary.min {
			summary.min = v0
		}
		if v1 < summary.min {
			summary.min = v1
		}
		if v2 < summary.min {
			summary.min = v2
		}
		if v3 < summary.min {
			summary.min = v3
		}
		if v0 > summary.max {
			summary.max = v0
		}
		if v1 > summary.max {
			summary.max = v1
		}
		if v2 > summary.max {
			summary.max = v2
		}
		if v3 > summary.max {
			summary.max = v3
		}
	}
	summary.sum = (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
	for ; offset < len(values); offset++ {
		value := float64(values[offset])
		summary.sum += value
		if value < summary.min {
			summary.min = value
		}
		if value > summary.max {
			summary.max = value
		}
	}
	return summary
}

func reducePackedUint16LE(values []byte) packedFloat64Summary {
	if len(values) == 0 {
		return packedFloat64Summary{}
	}
	first := float64(binary.LittleEndian.Uint16(values))
	var lanes [4]float64
	summary := packedFloat64Summary{count: len(values) / 2, min: first, max: first}
	offset := 0
	for ; offset+8 <= len(values); offset += 8 {
		v0 := float64(binary.LittleEndian.Uint16(values[offset : offset+2]))
		v1 := float64(binary.LittleEndian.Uint16(values[offset+2 : offset+4]))
		v2 := float64(binary.LittleEndian.Uint16(values[offset+4 : offset+6]))
		v3 := float64(binary.LittleEndian.Uint16(values[offset+6 : offset+8]))
		lanes[0] += v0
		lanes[1] += v1
		lanes[2] += v2
		lanes[3] += v3
		if v0 < summary.min {
			summary.min = v0
		}
		if v1 < summary.min {
			summary.min = v1
		}
		if v2 < summary.min {
			summary.min = v2
		}
		if v3 < summary.min {
			summary.min = v3
		}
		if v0 > summary.max {
			summary.max = v0
		}
		if v1 > summary.max {
			summary.max = v1
		}
		if v2 > summary.max {
			summary.max = v2
		}
		if v3 > summary.max {
			summary.max = v3
		}
	}
	summary.sum = (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
	for ; offset < len(values); offset += 2 {
		value := float64(binary.LittleEndian.Uint16(values[offset : offset+2]))
		summary.sum += value
		if value < summary.min {
			summary.min = value
		}
		if value > summary.max {
			summary.max = value
		}
	}
	return summary
}

func reducePackedUint32LE(values []byte) packedFloat64Summary {
	if len(values) == 0 {
		return packedFloat64Summary{}
	}
	first := float64(binary.LittleEndian.Uint32(values))
	var lanes [4]float64
	summary := packedFloat64Summary{count: len(values) / 4, min: first, max: first}
	offset := 0
	for ; offset+16 <= len(values); offset += 16 {
		v0 := float64(binary.LittleEndian.Uint32(values[offset : offset+4]))
		v1 := float64(binary.LittleEndian.Uint32(values[offset+4 : offset+8]))
		v2 := float64(binary.LittleEndian.Uint32(values[offset+8 : offset+12]))
		v3 := float64(binary.LittleEndian.Uint32(values[offset+12 : offset+16]))
		lanes[0] += v0
		lanes[1] += v1
		lanes[2] += v2
		lanes[3] += v3
		if v0 < summary.min {
			summary.min = v0
		}
		if v1 < summary.min {
			summary.min = v1
		}
		if v2 < summary.min {
			summary.min = v2
		}
		if v3 < summary.min {
			summary.min = v3
		}
		if v0 > summary.max {
			summary.max = v0
		}
		if v1 > summary.max {
			summary.max = v1
		}
		if v2 > summary.max {
			summary.max = v2
		}
		if v3 > summary.max {
			summary.max = v3
		}
	}
	summary.sum = (lanes[0] + lanes[1]) + (lanes[2] + lanes[3])
	for ; offset < len(values); offset += 4 {
		value := float64(binary.LittleEndian.Uint32(values[offset : offset+4]))
		summary.sum += value
		if value < summary.min {
			summary.min = value
		}
		if value > summary.max {
			summary.max = value
		}
	}
	return summary
}
