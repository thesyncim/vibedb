package vnext

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
)

const (
	FingerprintPageSize          = Quantum
	FingerprintPayloadHeaderSize = 32
	FingerprintBucketCount       = 256
	FingerprintBucketBytes       = (FingerprintBucketCount + 1) * 2
	FingerprintEntrySize         = 11
	FingerprintPageCapacity      = (FingerprintPageSize - FrameHeaderSize - FrameTrailerSize -
		FingerprintPayloadHeaderSize - FingerprintBucketBytes) / FingerprintEntrySize

	fingerprintVersion = uint32(0)
)

// Location is a stable row address. Six low bits store the stable slot and the
// remaining 26 bits store a block ID, addressing 67 million blocks without a
// physical offset in every primary-directory entry.
type Location struct {
	BlockID uint32
	Slot    uint8
}

// FingerprintEntry is one transient encoder input. Equal hashes are explicit
// and legal; Location breaks their ordering and full-key verification decides
// the lookup result.
type FingerprintEntry struct {
	Hash     uint64
	Location Location
}

// FingerprintView is a checksum- and structure-verified borrowed leaf view.
type FingerprintView struct {
	identity Identity
	payload  []byte
	count    uint16
	minHash  uint64
	maxHash  uint64
}

// EncodeFingerprintPage writes one immutable hash-range leaf. Entries must be
// ordered by (Hash, Location), with no duplicate location for one hash.
func EncodeFingerprintPage(
	dst []byte,
	identity Identity,
	entries []FingerprintEntry,
) ([]byte, error) {
	if len(dst) != FingerprintPageSize || len(entries) > FingerprintPageCapacity {
		return nil, ErrInvalidFrame
	}
	previousHash, previousLocation := uint64(0), uint32(0)
	for i, entry := range entries {
		location, ok := packLocation(entry.Location)
		if !ok || i != 0 && (entry.Hash < previousHash ||
			entry.Hash == previousHash && location <= previousLocation) {
			return nil, fmt.Errorf("%w: unordered fingerprint entry", ErrInvalidFrame)
		}
		previousHash, previousLocation = entry.Hash, location
	}
	payloadLength := FingerprintPayloadHeaderSize + FingerprintBucketBytes +
		len(entries)*FingerprintEntrySize
	payload, err := initFrame(dst, identity, frameFingerprint, payloadLength)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(payload[0:4], fingerprintVersion)
	binary.LittleEndian.PutUint16(payload[4:6], uint16(len(entries)))
	binary.LittleEndian.PutUint16(payload[6:8], FingerprintBucketBytes)
	binary.LittleEndian.PutUint16(payload[8:10], FingerprintEntrySize)
	if len(entries) != 0 {
		binary.LittleEndian.PutUint64(payload[12:20], entries[0].Hash)
		binary.LittleEndian.PutUint64(payload[20:28], entries[len(entries)-1].Hash)
	}
	directory := payload[FingerprintPayloadHeaderSize:]
	index := 0
	for bucket := 0; bucket <= FingerprintBucketCount; bucket++ {
		for index < len(entries) && int(entries[index].Hash>>56) < bucket {
			index++
		}
		binary.LittleEndian.PutUint16(directory[bucket*2:bucket*2+2], uint16(index))
	}
	records := directory[FingerprintBucketBytes:]
	for i, entry := range entries {
		record := records[i*FingerprintEntrySize:]
		putUint56(record[0:7], entry.Hash)
		location, _ := packLocation(entry.Location)
		binary.LittleEndian.PutUint32(record[7:11], location)
	}
	if err := sealFrame(dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// OpenFingerprintPage validates one complete leaf once. Lookup then performs no
// allocation and trusts no fingerprint as proof of key identity.
func OpenFingerprintPage(src []byte) (FingerprintView, error) {
	header, payload, err := openFrame(src, frameFingerprint)
	if err != nil {
		return FingerprintView{}, err
	}
	if header.span != FingerprintPageSize || len(payload) < FingerprintPayloadHeaderSize+FingerprintBucketBytes ||
		binary.LittleEndian.Uint32(payload[0:4]) != fingerprintVersion ||
		binary.LittleEndian.Uint16(payload[6:8]) != FingerprintBucketBytes ||
		binary.LittleEndian.Uint16(payload[8:10]) != FingerprintEntrySize ||
		!allZero(payload[10:12]) || !allZero(payload[28:32]) {
		return FingerprintView{}, corrupt("fingerprint header")
	}
	count := binary.LittleEndian.Uint16(payload[4:6])
	wantLength := FingerprintPayloadHeaderSize + FingerprintBucketBytes +
		int(count)*FingerprintEntrySize
	if int(count) > FingerprintPageCapacity || len(payload) != wantLength {
		return FingerprintView{}, corrupt("fingerprint length")
	}
	minHash := binary.LittleEndian.Uint64(payload[12:20])
	maxHash := binary.LittleEndian.Uint64(payload[20:28])
	if count == 0 && (minHash != 0 || maxHash != 0) || count != 0 && minHash > maxHash {
		return FingerprintView{}, corrupt("fingerprint range")
	}
	directory := payload[FingerprintPayloadHeaderSize:]
	previous := uint16(0)
	for bucket := 0; bucket <= FingerprintBucketCount; bucket++ {
		current := binary.LittleEndian.Uint16(directory[bucket*2 : bucket*2+2])
		if current < previous || current > count ||
			bucket == 0 && current != 0 ||
			bucket == FingerprintBucketCount && current != count {
			return FingerprintView{}, corrupt("fingerprint buckets")
		}
		previous = current
	}
	records := directory[FingerprintBucketBytes:]
	firstHash, previousHash, previousLocation := uint64(0), uint64(0), uint32(0)
	for bucket := 0; bucket < FingerprintBucketCount; bucket++ {
		begin := int(binary.LittleEndian.Uint16(directory[bucket*2 : bucket*2+2]))
		end := int(binary.LittleEndian.Uint16(directory[bucket*2+2 : bucket*2+4]))
		for i := begin; i < end; i++ {
			record := records[i*FingerprintEntrySize:]
			hash := uint64(bucket)<<56 | getUint56(record[0:7])
			location := binary.LittleEndian.Uint32(record[7:11])
			if _, ok := unpackLocation(location); !ok ||
				i != 0 && (hash < previousHash ||
					hash == previousHash && location <= previousLocation) {
				return FingerprintView{}, corrupt("fingerprint ordering")
			}
			if i == 0 {
				firstHash = hash
			}
			previousHash, previousLocation = hash, location
		}
	}
	if count != 0 && (firstHash != minHash || previousHash != maxHash) {
		return FingerprintView{}, corrupt("fingerprint range mismatch")
	}
	return FingerprintView{
		identity: header.identity,
		payload:  payload,
		count:    count,
		minHash:  minHash,
		maxHash:  maxHash,
	}, nil
}

// Lookup probes only equal-fingerprint candidates, then requires verify to
// compare the complete key in the addressed record block. A nil verifier never
// returns a hit.
func (v FingerprintView) Lookup(hash uint64, verify func(Location) bool) (Location, bool) {
	if verify == nil || v.count == 0 || hash < v.minHash || hash > v.maxHash {
		return Location{}, false
	}
	directory := v.payload[FingerprintPayloadHeaderSize:]
	bucket := int(hash >> 56)
	begin := int(binary.LittleEndian.Uint16(directory[bucket*2 : bucket*2+2]))
	end := int(binary.LittleEndian.Uint16(directory[bucket*2+2 : bucket*2+4]))
	records := directory[FingerprintBucketBytes:]
	suffix := hash & (uint64(1)<<56 - 1)
	index := sort.Search(end-begin, func(i int) bool {
		record := records[(begin+i)*FingerprintEntrySize:]
		return getUint56(record[0:7]) >= suffix
	}) + begin
	for ; index < end; index++ {
		record := records[index*FingerprintEntrySize:]
		if getUint56(record[0:7]) != suffix {
			break
		}
		location, _ := unpackLocation(binary.LittleEndian.Uint32(record[7:11]))
		if verify(location) {
			return location, true
		}
	}
	return Location{}, false
}

func packLocation(location Location) (uint32, bool) {
	if location.BlockID == 0 || location.BlockID >= 1<<26 || location.Slot >= 64 {
		return 0, false
	}
	return location.BlockID<<6 | uint32(location.Slot), true
}

func unpackLocation(packed uint32) (Location, bool) {
	location := Location{BlockID: packed >> 6, Slot: uint8(packed & 63)}
	return location, location.BlockID != 0
}

func putUint56(dst []byte, value uint64) {
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
	dst[3] = byte(value >> 24)
	dst[4] = byte(value >> 32)
	dst[5] = byte(value >> 40)
	dst[6] = byte(value >> 48)
}

func getUint56(src []byte) uint64 {
	return uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40 |
		uint64(src[6])<<48
}

// KeyedFingerprint is SipHash-2-4 over key. The seed is persisted in the
// collection root. Hashes route candidates only; Lookup still verifies full
// key bytes, so collision probability is never part of correctness.
func KeyedFingerprint(seed [16]byte, key string) uint64 {
	k0 := binary.LittleEndian.Uint64(seed[0:8])
	k1 := binary.LittleEndian.Uint64(seed[8:16])
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573
	end := len(key) &^ 7
	for offset := 0; offset < end; offset += 8 {
		message := uint64(key[offset]) |
			uint64(key[offset+1])<<8 |
			uint64(key[offset+2])<<16 |
			uint64(key[offset+3])<<24 |
			uint64(key[offset+4])<<32 |
			uint64(key[offset+5])<<40 |
			uint64(key[offset+6])<<48 |
			uint64(key[offset+7])<<56
		v3 ^= message
		sipRounds(&v0, &v1, &v2, &v3, 2)
		v0 ^= message
	}
	tail := uint64(len(key)) << 56
	for i := len(key) - end - 1; i >= 0; i-- {
		tail |= uint64(key[end+i]) << (8 * i)
	}
	v3 ^= tail
	sipRounds(&v0, &v1, &v2, &v3, 2)
	v0 ^= tail
	v2 ^= 0xff
	sipRounds(&v0, &v1, &v2, &v3, 4)
	return v0 ^ v1 ^ v2 ^ v3
}

func sipRounds(v0, v1, v2, v3 *uint64, count int) {
	for range count {
		*v0 += *v1
		*v1 = bits.RotateLeft64(*v1, 13)
		*v1 ^= *v0
		*v0 = bits.RotateLeft64(*v0, 32)
		*v2 += *v3
		*v3 = bits.RotateLeft64(*v3, 16)
		*v3 ^= *v2
		*v0 += *v3
		*v3 = bits.RotateLeft64(*v3, 21)
		*v3 ^= *v0
		*v2 += *v1
		*v1 = bits.RotateLeft64(*v1, 17)
		*v1 ^= *v2
		*v2 = bits.RotateLeft64(*v2, 32)
	}
}
