package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
)

const (
	childStageCursorFormat = uint16(1)
	childStageCursorBytes  = 480
	// ChildStageCursorEncodedBytes is the exact canonical persisted/wire size.
	ChildStageCursorEncodedBytes = childStageCursorBytes
)

var (
	childStageCursorMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'S', 'T', 0}
	childStageCursorDomain = []byte("vibedb/range-split/child-stage-cursor\x00")
)

// ChildStagePhase identifies the durable progress represented by a cursor.
type ChildStagePhase uint8

const (
	// ChildStageArtifact means that a verified artifact prefix is durable.
	ChildStageArtifact ChildStagePhase = iota + 1
	// ChildStageTail means that the complete artifact is validated and the
	// cursor can advance through translated source entries.
	ChildStageTail
	// ChildStageSealed means that the terminal ownership-fence entry is durable.
	// No later tail entry is accepted.
	ChildStageSealed
)

// ChildStageCursor is a constant-size authenticated destination progress
// record. Its fields are private so progress can only come from verified stage
// work or strict decoding.
type ChildStageCursor struct {
	phase ChildStagePhase
	child uint8

	artifactChunks  uint64
	artifactRows    uint64
	artifactPayload uint64
	artifactOffset  uint64
	applied         uint64
	term            uint64
	routeGeneration uint64
	// These carry the incremental image accumulator through artifact and tail
	// phases. At seal imageDigest becomes the context-bound terminal proof.
	// Cursor durability is the accumulator's crash-consistency boundary.
	imageRows  uint64
	imageBytes uint64

	planDigest      [sha256.Size]byte
	placementDigest [sha256.Size]byte
	artifactDigest  [sha256.Size]byte
	headerDigest    [sha256.Size]byte
	lastChunkDigest [sha256.Size]byte
	dataChainDigest [sha256.Size]byte
	baseDigest      [sha256.Size]byte
	entryDigest     [sha256.Size]byte
	lastBatchDigest [sha256.Size]byte
	imageDigest     [sha256.Size]byte
	// pendingBatchDigest authenticates a verified preimage before any tail
	// writes. All other fields still describe the preceding completed entry.
	pendingBatchDigest [sha256.Size]byte
}

// ChildStageCursorWorkspace retains one SHA-256 state for allocation-free
// warmed cursor encoding. Reuse it serially.
type ChildStageCursorWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
}

// Phase returns the durable destination phase.
func (c ChildStageCursor) Phase() ChildStagePhase { return c.phase }

// Child returns the split child ordinal.
func (c ChildStageCursor) Child() uint8 { return c.child }

// PlanDigest returns the exact split plan bound to this durable cursor.
func (c ChildStageCursor) PlanDigest() [sha256.Size]byte { return c.planDigest }

// PlacementDigest returns the exact compiled placement program bound to this
// durable cursor.
func (c ChildStageCursor) PlacementDigest() [sha256.Size]byte {
	return c.placementDigest
}

// SourceCut returns the exact translated source prefix.
func (c ChildStageCursor) SourceCut() ChildArtifactSourceCut {
	return ChildArtifactSourceCut{
		DataChainDigest: c.dataChainDigest, BaseDigest: c.baseDigest,
		EntryDigest: c.entryDigest, Applied: c.applied, Term: c.term,
		RouteGeneration: c.routeGeneration,
	}
}

// ArtifactDigest returns the complete expected child artifact identity.
func (c ChildStageCursor) ArtifactDigest() [sha256.Size]byte {
	return c.artifactDigest
}

// ArtifactProgress returns the verified durable artifact prefix. nextSequence
// is the first chunk that is not present in that prefix.
func (c ChildStageCursor) ArtifactProgress() (
	nextSequence, rows, payloadBytes, endOffset uint64,
	lastDigest [sha256.Size]byte,
) {
	return c.artifactChunks, c.artifactRows, c.artifactPayload,
		c.artifactOffset, c.lastChunkDigest
}

// LastBatchDigest returns the latest durable tail batch identity. It is zero
// before the first tail entry.
func (c ChildStageCursor) LastBatchDigest() [sha256.Size]byte {
	return c.lastBatchDigest
}

// ResumesTailBatch reports whether c is the exact durable preimage receipt for
// batch and before. It grants no authority to replay a different entry.
func (c ChildStageCursor) ResumesTailBatch(before ChildStageCursor, batch TailBatch) bool {
	if c.phase != ChildStageTail || before.pendingBatchDigest != ([sha256.Size]byte{}) ||
		c.pendingBatchDigest != batch.Digest || batch.Digest == ([sha256.Size]byte{}) {
		return false
	}
	c.pendingBatchDigest = [sha256.Size]byte{}
	return c == before && cursorImmediatelyPrecedesBatch(before, batch)
}

// ImageProof returns the exact order-independent final child image commitment
// recorded at seal.
func (c ChildStageCursor) ImageProof() (
	rows, bytes uint64,
	digest [sha256.Size]byte,
	ok bool,
) {
	if c.phase != ChildStageSealed {
		return 0, 0, [sha256.Size]byte{}, false
	}
	return c.imageRows, c.imageBytes, c.imageDigest, true
}

// AppendChildStageCursor appends one strict, constant-size cursor. On error,
// dst is unchanged.
func AppendChildStageCursor(dst []byte, cursor *ChildStageCursor) ([]byte, error) {
	return AppendChildStageCursorWithWorkspace(dst, cursor, &ChildStageCursorWorkspace{})
}

// AppendChildStageCursorWithWorkspace is AppendChildStageCursor with reusable
// hash state.
func AppendChildStageCursorWithWorkspace(
	dst []byte,
	cursor *ChildStageCursor,
	workspace *ChildStageCursorWorkspace,
) ([]byte, error) {
	if err := validateChildStageCursor(cursor); err != nil {
		return dst, err
	}
	if workspace == nil {
		return dst, fmt.Errorf("%w: nil cursor workspace", ErrChildStage)
	}
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	start := len(dst)
	dst = append(dst, make([]byte, childStageCursorBytes)...)
	frame := dst[start:]
	clear(frame)
	copy(frame[0:8], childStageCursorMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], childStageCursorFormat)
	binary.LittleEndian.PutUint16(frame[10:12], childStageCursorBytes)
	binary.LittleEndian.PutUint32(frame[12:16], childStageCursorBytes)
	frame[16] = byte(cursor.phase)
	frame[17] = cursor.child
	binary.LittleEndian.PutUint64(frame[24:32], cursor.artifactChunks)
	binary.LittleEndian.PutUint64(frame[32:40], cursor.artifactRows)
	binary.LittleEndian.PutUint64(frame[40:48], cursor.artifactPayload)
	binary.LittleEndian.PutUint64(frame[48:56], cursor.artifactOffset)
	binary.LittleEndian.PutUint64(frame[56:64], cursor.applied)
	binary.LittleEndian.PutUint64(frame[64:72], cursor.term)
	binary.LittleEndian.PutUint64(frame[72:80], cursor.routeGeneration)
	binary.LittleEndian.PutUint64(frame[80:88], cursor.imageRows)
	binary.LittleEndian.PutUint64(frame[88:96], cursor.imageBytes)
	copy(frame[96:128], cursor.planDigest[:])
	copy(frame[128:160], cursor.placementDigest[:])
	copy(frame[160:192], cursor.artifactDigest[:])
	copy(frame[192:224], cursor.headerDigest[:])
	copy(frame[224:256], cursor.lastChunkDigest[:])
	copy(frame[256:288], cursor.dataChainDigest[:])
	copy(frame[288:320], cursor.baseDigest[:])
	copy(frame[320:352], cursor.entryDigest[:])
	copy(frame[352:384], cursor.lastBatchDigest[:])
	copy(frame[384:416], cursor.imageDigest[:])
	copy(frame[416:448], cursor.pendingBatchDigest[:])
	childArtifactDigestPartsInto(
		workspace.hasher, &workspace.digest, childStageCursorDomain, frame[:448], nil,
	)
	copy(frame[448:480], workspace.digest[:])
	return dst, nil
}

// OpenChildStageCursor strictly decodes one complete cursor.
func OpenChildStageCursor(src []byte) (*ChildStageCursor, error) {
	var workspace ChildStageCursorWorkspace
	cursor, err := decodeChildStageCursor(src, &workspace)
	if err != nil {
		return nil, err
	}
	return &cursor, nil
}

func decodeChildStageCursor(
	src []byte,
	workspace *ChildStageCursorWorkspace,
) (ChildStageCursor, error) {
	if len(src) != childStageCursorBytes ||
		!bytes.Equal(src[0:8], childStageCursorMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != childStageCursorFormat ||
		binary.LittleEndian.Uint16(src[10:12]) != childStageCursorBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != childStageCursorBytes ||
		!allChildArtifactZero(src[18:24]) {
		return ChildStageCursor{}, fmt.Errorf("%w: stage cursor header", ErrChildStage)
	}
	if workspace == nil {
		return ChildStageCursor{}, fmt.Errorf("%w: nil cursor workspace", ErrChildStage)
	}
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	childArtifactDigestPartsInto(
		workspace.hasher, &workspace.digest, childStageCursorDomain, src[:448], nil,
	)
	var stored [sha256.Size]byte
	copy(stored[:], src[448:480])
	if stored != workspace.digest {
		return ChildStageCursor{}, fmt.Errorf("%w: stage cursor digest", ErrChildStage)
	}
	cursor := ChildStageCursor{
		phase:           ChildStagePhase(src[16]),
		child:           src[17],
		artifactChunks:  binary.LittleEndian.Uint64(src[24:32]),
		artifactRows:    binary.LittleEndian.Uint64(src[32:40]),
		artifactPayload: binary.LittleEndian.Uint64(src[40:48]),
		artifactOffset:  binary.LittleEndian.Uint64(src[48:56]),
		applied:         binary.LittleEndian.Uint64(src[56:64]),
		term:            binary.LittleEndian.Uint64(src[64:72]),
		routeGeneration: binary.LittleEndian.Uint64(src[72:80]),
		imageRows:       binary.LittleEndian.Uint64(src[80:88]),
		imageBytes:      binary.LittleEndian.Uint64(src[88:96]),
	}
	copy(cursor.planDigest[:], src[96:128])
	copy(cursor.placementDigest[:], src[128:160])
	copy(cursor.artifactDigest[:], src[160:192])
	copy(cursor.headerDigest[:], src[192:224])
	copy(cursor.lastChunkDigest[:], src[224:256])
	copy(cursor.dataChainDigest[:], src[256:288])
	copy(cursor.baseDigest[:], src[288:320])
	copy(cursor.entryDigest[:], src[320:352])
	copy(cursor.lastBatchDigest[:], src[352:384])
	copy(cursor.imageDigest[:], src[384:416])
	copy(cursor.pendingBatchDigest[:], src[416:448])
	if err := validateChildStageCursor(&cursor); err != nil {
		return ChildStageCursor{}, err
	}
	return cursor, nil
}

func validateChildStageCursor(cursor *ChildStageCursor) error {
	if cursor == nil || cursor.child >= autosplit.MaxSplitChildren ||
		cursor.applied == 0 || cursor.applied == math.MaxUint64 ||
		cursor.term == 0 || cursor.term == math.MaxUint64 ||
		cursor.routeGeneration == 0 ||
		cursor.planDigest == ([sha256.Size]byte{}) ||
		cursor.placementDigest == ([sha256.Size]byte{}) ||
		cursor.artifactDigest == ([sha256.Size]byte{}) ||
		cursor.headerDigest == ([sha256.Size]byte{}) ||
		cursor.lastChunkDigest == ([sha256.Size]byte{}) ||
		cursor.dataChainDigest == ([sha256.Size]byte{}) ||
		cursor.baseDigest == ([sha256.Size]byte{}) ||
		cursor.entryDigest == ([sha256.Size]byte{}) ||
		cursor.phase != ChildStageTail && cursor.pendingBatchDigest != ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid stage cursor", ErrChildStage)
	}
	switch cursor.phase {
	case ChildStageArtifact:
		if cursor.artifactChunks == 0 || cursor.artifactRows == 0 ||
			cursor.artifactPayload == 0 || cursor.artifactOffset == 0 ||
			cursor.lastBatchDigest != ([sha256.Size]byte{}) ||
			cursor.imageRows != cursor.artifactRows || cursor.imageBytes == 0 ||
			cursor.imageDigest == ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: artifact cursor", ErrChildStage)
		}
	case ChildStageTail, ChildStageSealed:
		if cursor.artifactOffset == 0 {
			return fmt.Errorf("%w: tail cursor", ErrChildStage)
		}
		if cursor.phase == ChildStageTail &&
			((cursor.imageRows == 0) != (cursor.imageBytes == 0) ||
				cursor.imageRows != 0 && cursor.imageDigest == ([sha256.Size]byte{})) {
			return fmt.Errorf("%w: tail image accumulator", ErrChildStage)
		}
		if cursor.phase == ChildStageSealed &&
			(cursor.lastBatchDigest == ([sha256.Size]byte{}) ||
				cursor.imageDigest == ([sha256.Size]byte{}) ||
				(cursor.imageRows == 0) != (cursor.imageBytes == 0)) {
			return fmt.Errorf("%w: sealed cursor", ErrChildStage)
		}
	default:
		return fmt.Errorf("%w: stage cursor phase", ErrChildStage)
	}
	return nil
}
