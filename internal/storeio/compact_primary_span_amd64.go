//go:build amd64

package storeio

import "unsafe"

// compactPrimaryDictionarySpan loads adjacent little-endian fragment bounds
// together. amd64 permits the two-byte-aligned 32-bit load used here.
func compactPrimaryDictionarySpan(
	bounds *[compactPrimaryScanDictionaryBounds]uint16,
	at int,
) (int, int) {
	span := *(*uint32)(unsafe.Pointer(&bounds[at]))
	return int(uint16(span)), int(span >> 16)
}
