//go:build !amd64

package storeio

func compactPrimaryDictionarySpan(
	bounds *[compactPrimaryScanDictionaryBounds]uint16,
	at int,
) (int, int) {
	return int(bounds[at]), int(bounds[at+1])
}
