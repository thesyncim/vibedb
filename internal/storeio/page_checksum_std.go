//go:build !go1.27 || go1.28 || !goexperiment.simd || (!amd64 && !arm64) || (arm64 && !darwin)

package storeio

import "hash/crc32"

func pageChecksum(data []byte) uint32 {
	return crc32.Checksum(data, pageChecksumTable)
}
