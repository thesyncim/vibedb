package requestledger

import (
	"encoding/binary"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendChecksum(dst []byte, start int) []byte {
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

func checksumOK(raw []byte) bool {
	return len(raw) >= checksumBytes &&
		crc32.Checksum(raw[:len(raw)-checksumBytes], castagnoli) ==
			binary.LittleEndian.Uint32(raw[len(raw)-checksumBytes:])
}

func exactLength(base int, lengths ...uint64) (int, bool) {
	total := uint64(base)
	for _, length := range lengths {
		if length > uint64(MaxCommandBytes) || total > uint64(MaxCommandBytes)-length {
			return 0, false
		}
		total += length
	}
	return int(total), true
}

func putDigest(out []byte, digest Digest) { copy(out, digest[:]) }

func readDigest(raw []byte) (digest Digest) {
	copy(digest[:], raw[:len(digest)])
	return digest
}

func magicOK(raw []byte, magic [4]byte) bool {
	return len(raw) >= 4 && raw[0] == magic[0] && raw[1] == magic[1] &&
		raw[2] == magic[2] && raw[3] == magic[3]
}
