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
	// The command wire header retains one private zero sentinel so corrupt,
	// foreign, or pre-grammar envelopes fail closed. It is not a version: there
	// is exactly one supported command grammar and no compatibility decoder.
	commandCodecSentinel  = uint16(0)
	completionCodecFormat = uint16(1)

	// MaxCommandBytes is the intentionally narrower replicated-admission limit
	// for one complete command, including its header and checksum. Admission must
	// enforce it before proposal or transport enqueue. A future consensus
	// transport may use a smaller batching target, but must still carry one
	// admitted command plus framing.
	MaxCommandBytes = 16 << 20
	// MaxCompletionEnvelopeBytes bounds retained inline completion metadata.
	MaxCompletionEnvelopeBytes = 128 << 10
	// MaxEmptyResultCompletionEnvelopeBytes is the largest canonical completion
	// carrying no inline result bytes. Replicated session mutations currently
	// use this shape, so a serving result sink can reserve exact bounded scratch
	// before proposal admission.
	MaxEmptyResultCompletionEnvelopeBytes = completionHeaderBytes + envelopeChecksumBytes + 3*MaxIdentityBytes
	// MaxCompletionResultBytes is the largest exact response a digest reference
	// may name. Small results have one canonical inline representation.
	MaxCompletionResultBytes = 16 << 20
	MaxInlineCompletionBytes = 64 << 10

	MaxIdentityBytes   = 255
	MaxCollectionBytes = 1<<16 - 1
	// MaxRelationsPerBundle is the fixed dense user-relation slot count for one
	// replicated shard group. Relation zero is reserved for hidden state; the
	// durable checkpoint certificate has room for that state plus 59 relations.
	// Slot reuse or remapping is legal only behind SchemaGeneration and the
	// authenticated bundle manifest; RelationID is deliberately not a sparse,
	// long-lived catalog identity.
	MaxRelationsPerBundle = 59
	// MaxRelationBatches bounds the number of distinct relation slots touched by
	// one command.
	MaxRelationBatches = MaxRelationsPerBundle
	// MaxRelationID is the largest dense physical bundle ordinal.
	MaxRelationID = MaxRelationsPerBundle
	// MaxMutations bounds the sum across every relation batch in one command.
	MaxMutations          = 1 << 16
	MaxMutationKeyBytes   = 256
	MaxMutationValueBytes = 4 << 20

	commandHeaderBytes         = 256
	completionHeaderBytes      = 288
	envelopeChecksumBytes      = 8
	mutationHeaderBytes        = 8
	MutationDigestCompareBytes = 8 + 32
	mutationDigestCompareBytes = MutationDigestCompareBytes
	relationBatchHeaderBytes   = 8
	commandWireMutationBatch   = uint8(1)
	commandWireSessionRetire   = uint8(2)
	commandWireSessionRelease  = uint8(3)
	commandWireSessionOpen     = uint8(4)
	commandWireSessionRenew    = uint8(5)
	commandWireSessionRevoke   = uint8(6)
	sessionLeaseBodyBytes      = 16
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

// RelationID is one compact, dense bundle-local physical slot ordinal in
// [1, MaxRelationID]. Zero is reserved for the replicated state machine's
// hidden system collection and is never legal in a client mutation command.
// It is interpreted only under the command's SchemaGeneration.
type RelationID uint16

// CommandAuthorityClass is authenticated command identity, not a transport
// hint. Data is the zero value so the sole unreleased grammar keeps ordinary
// command bytes compact; topology commands carry the one nonzero class bit.
type CommandAuthorityClass uint8

const (
	CommandAuthorityData CommandAuthorityClass = iota
	CommandAuthorityTopology
)

// CommandKind selects the command's state-machine operation. The zero value is
// the ordinary mutation batch so command producers remain explicit for session
// lifecycle operations.
type CommandKind uint8

const (
	// CommandMutationBatch applies one or more ordered nonempty relation batches.
	CommandMutationBatch CommandKind = iota
	// CommandSessionRetire seals the command's client epoch and carries no
	// mutations. The compact identity high-water remains durable so delayed
	// commands from a retired epoch can never become new again.
	CommandSessionRetire
	// CommandSessionRelease reclaims the bounded retry state for an already
	// retired client epoch and carries no mutations.
	CommandSessionRelease
	// CommandSessionOpen allocates the next shard-issued client epoch and carries
	// no mutations. Its request header uses epoch zero, sequence one, and no
	// acknowledgement because the allocated epoch is returned by the apply path.
	// Its lease body is (0, NextDeadlineUnixNano), with a positive deadline.
	CommandSessionOpen
	// CommandSessionRenew conditionally advances an active session's replicated
	// lease deadline. ExpectedDeadlineUnixNano fences delayed renewals.
	CommandSessionRenew
	// CommandSessionRevoke conditionally clears an active session's replicated
	// lease deadline. ExpectedDeadlineUnixNano fences delayed revocations.
	CommandSessionRevoke
)

// MutationKind selects one logical relation mutation.
type MutationKind uint8

const (
	MutationPut               MutationKind = 1
	MutationDelete            MutationKind = 2
	MutationPutAbsentOrEqual  MutationKind = 3
	MutationDeleteDigestEqual MutationKind = 4
	MutationPutDigestEqual    MutationKind = 5
)

// Mutation is one caller-owned command mutation. Key and Value are borrowed
// only for AppendCommand. Its ordinal is part of the command's identity:
// order and duplicate keys are preserved exactly and are never normalized.
type Mutation struct {
	Kind  MutationKind
	Key   []byte
	Value []byte

	// ExpectedValueLength and ExpectedValueDigest are populated only by the
	// digest-equal mutations. The closed compare operation makes stale deletes
	// and replacements deterministic without carrying or decoding the prior
	// JSON value. Ordinary Put/Delete wire bytes remain unchanged.
	ExpectedValueLength uint64
	ExpectedValueDigest Digest
}

// RelationMutationBatch is one nonempty, caller-owned mutation sequence for a
// single relation. Command batches are strictly ordered by Relation and may
// not repeat an identity. Mutation order and duplicate keys remain semantic.
type RelationMutationBatch struct {
	Relation  RelationID
	Mutations []Mutation
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

func unsupported(kind string, format uint16) error {
	return fmt.Errorf("%w: %s format %d", ErrUnsupportedFormat, kind, format)
}

func unsupportedCommandSentinel(sentinel uint16) error {
	return fmt.Errorf(
		"%w: command grammar sentinel %d", ErrUnsupportedFormat, sentinel,
	)
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

func typedSliceOverlapsBytes[T any](left []byte, right []T) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	var element T
	return addressRangesOverlap(
		uintptr(unsafe.Pointer(unsafe.SliceData(left))), uintptr(len(left)),
		uintptr(unsafe.Pointer(unsafe.SliceData(right))),
		uintptr(len(right))*unsafe.Sizeof(element),
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
