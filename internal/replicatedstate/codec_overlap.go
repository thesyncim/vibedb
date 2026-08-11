package replicatedstate

import "unsafe"

func writableAppendRegion(dst []byte, count int) []byte {
	if count <= 0 || count > cap(dst)-len(dst) {
		return nil
	}
	return dst[len(dst) : len(dst)+count]
}

func byteSlicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	return addressRangesOverlap(leftStart, uintptr(len(left)), rightStart, uintptr(len(right)))
}

func byteStringOverlap(left []byte, right string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.StringData(right)))
	return addressRangesOverlap(leftStart, uintptr(len(left)), rightStart, uintptr(len(right)))
}

func addressRangesOverlap(left, leftBytes, right, rightBytes uintptr) bool {
	if left <= right {
		return right-left < leftBytes
	}
	return left-right < rightBytes
}
