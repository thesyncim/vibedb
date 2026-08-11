package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"unsafe"
)

const (
	// CommandFormatV1 and CompletionFormatV1 are frozen byte grammars. Unknown
	// versions are rejected; a decoder never guesses a compatible layout.
	CommandFormatV1    = uint16(1)
	CompletionFormatV1 = uint16(1)

	// MaxCommandBytes is the intentionally narrower replicated-admission limit
	// for one complete command, including its header and checksum. Admission must
	// enforce it before proposal or transport enqueue. A future consensus
	// transport may use a smaller batching target, but must still carry one
	// admitted command plus framing.
	MaxCommandBytes = 16 << 20
	// MaxCompletionEnvelopeBytes bounds retained inline completion metadata.
	MaxCompletionEnvelopeBytes = 128 << 10
	// MaxCompletionResultBytes is the largest exact response a digest reference
	// may name. Small results have one canonical inline representation.
	MaxCompletionResultBytes = 16 << 20
	MaxInlineCompletionBytes = 64 << 10

	MaxIdentityBytes      = 255
	MaxCollectionBytes    = 1<<16 - 1
	MaxMutations          = 1 << 16
	MaxMutationKeyBytes   = 256
	MaxMutationValueBytes = 4 << 20

	commandHeaderBytes       = 256
	completionHeaderBytes    = 288
	envelopeChecksumBytes    = 8
	mutationHeaderBytes      = 8
	commandKindMutationBatch = uint8(1)
)

// ID128 is one opaque, byte-canonical 128-bit identity. The codec assigns no
// UUID text grammar; every nonzero bit pattern is valid and equality is exact.
type ID128 [16]byte

// Digest is a SHA-256 request fingerprint or result digest, depending on the
// field carrying it. The all-zero value is never a valid persisted digest.
type Digest [32]byte

// RetryHome is the stable, fixed-width keyspace point that owns completion
// state across route changes and range movement. Zero is a valid keyspace point.
type RetryHome [8]byte

// MutationKind selects one logical collection mutation.
type MutationKind uint8

const (
	MutationPut    MutationKind = 1
	MutationDelete MutationKind = 2
)

// Mutation is one caller-owned command mutation. Key and Value are borrowed
// only for AppendCommandV1. Its ordinal is part of the command's identity:
// order and duplicate keys are preserved exactly and are never normalized.
type Mutation struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
}

// CompletionStorage selects the only two completion representations. Results
// through MaxInlineCompletionBytes are inline; larger results are named by
// their digest and must be supplied by a future durable blob store.
type CompletionStorage uint8

const (
	CompletionInline          CompletionStorage = 1
	CompletionDigestReference CompletionStorage = 2
)

var (
	ErrUnsupportedFormat = errors.New("replication: unsupported envelope format")
	ErrEnvelopeTooLarge  = errors.New("replication: envelope exceeds its bounded format")
	ErrEnvelopeCorrupt   = errors.New("replication: corrupt envelope")
	ErrEnvelopeSemantic  = errors.New("replication: invalid envelope semantics")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func corrupt(reason string) error {
	return fmt.Errorf("%w: %s", ErrEnvelopeCorrupt, reason)
}

func semantic(reason string) error {
	return fmt.Errorf("%w: %s", ErrEnvelopeSemantic, reason)
}

func unsupported(kind string, version uint16) error {
	return fmt.Errorf("%w: %s version %d", ErrUnsupportedFormat, kind, version)
}

func nonzero128(id ID128) bool    { return id != (ID128{}) }
func nonzeroDigest(d Digest) bool { return d != (Digest{}) }

func checkedAdd(total, addition, limit uint64) (uint64, bool) {
	if total > limit || addition > limit-total {
		return 0, false
	}
	return total + addition, true
}

// writableAppendRegion returns the exact bytes Append* will overwrite in dst's
// current backing array. A nil result means slices.Grow must allocate a distinct
// backing array, so aliases into the old dst backing are safe.
func writableAppendRegion(dst []byte, count int) []byte {
	if count <= 0 || count > cap(dst)-len(dst) {
		return nil
	}
	end := len(dst) + count
	return dst[len(dst):end:end]
}

func byteSlicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	return addressRangesOverlap(
		uintptr(unsafe.Pointer(unsafe.SliceData(left))), uintptr(len(left)),
		uintptr(unsafe.Pointer(unsafe.SliceData(right))), uintptr(len(right)),
	)
}

func byteSliceStringOverlap(left []byte, right string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	return addressRangesOverlap(
		uintptr(unsafe.Pointer(unsafe.SliceData(left))), uintptr(len(left)),
		uintptr(unsafe.Pointer(unsafe.StringData(right))), uintptr(len(right)),
	)
}

func addressRangesOverlap(left, leftBytes, right, rightBytes uintptr) bool {
	if left <= right {
		return right-left < leftBytes
	}
	return left-right < rightBytes
}

// extendZeroed grows dst by count bytes, preserving the original prefix and
// clearing the extension. With sufficient caller capacity this allocates zero.
func extendZeroed(dst []byte, count int) []byte {
	old := len(dst)
	dst = slices.Grow(dst, count)
	dst = dst[:old+count]
	clear(dst[old:])
	return dst
}

func sealEnvelope(buf []byte) {
	trailer := len(buf) - envelopeChecksumBytes
	checksum := crc32.Checksum(buf[:trailer], castagnoliTable)
	binary.LittleEndian.PutUint32(buf[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(buf[trailer+4:], ^checksum)
}

func verifyEnvelopeChecksum(src []byte) error {
	if len(src) < envelopeChecksumBytes {
		return corrupt("short checksum trailer")
	}
	trailer := len(src) - envelopeChecksumBytes
	stored := binary.LittleEndian.Uint32(src[trailer : trailer+4])
	if binary.LittleEndian.Uint32(src[trailer+4:]) != ^stored ||
		crc32.Checksum(src[:trailer], castagnoliTable) != stored {
		return corrupt("checksum")
	}
	return nil
}

func appendU16(dst []byte, at int, value uint16) {
	binary.LittleEndian.PutUint16(dst[at:at+2], value)
}

func appendU32(dst []byte, at int, value uint32) {
	binary.LittleEndian.PutUint32(dst[at:at+4], value)
}

func appendU64(dst []byte, at int, value uint64) {
	binary.LittleEndian.PutUint64(dst[at:at+8], value)
}
