package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	retainedPruneCursorFormat   = uint16(1)
	retainedPruneCursorHeader   = 400
	MaxRetainedPruneCursorBytes = retainedPruneCursorHeader +
		2*replication.MaxMutationKeyBytes + sha256.Size
)

var (
	retainedPruneCursorMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'R', 'N', 0}
	retainedPruneCursorDomain = []byte("vibedb/range-split/retained-prune-cursor\x00")
)

// RetainedPrunePhase identifies resumable retained-source cleanup progress.
type RetainedPrunePhase uint8

const (
	RetainedPruneScan RetainedPrunePhase = iota + 1
	RetainedPruneAwaitingApply
	RetainedPruneComplete
)

type RetainedPruneCursor struct {
	phase    RetainedPrunePhase
	retained uint8

	applied            uint64
	term               uint64
	ownershipEpoch     uint64
	routingVersion     uint64
	routeGeneration    uint64
	scannedRows        uint64
	scannedBytes       uint64
	deletedRows        uint64
	deletedKeyBytes    uint64
	pendingCount       uint64
	pendingKeyBytes    uint64
	remainingRows      uint64
	remainingBytes     uint64
	snapshotGeneration uint64

	plan           [sha256.Size]byte
	placement      [sha256.Size]byte
	cutover        [sha256.Size]byte
	logical        [sha256.Size]byte
	base           [sha256.Size]byte
	entry          [sha256.Size]byte
	pending        [sha256.Size]byte
	retainedDigest [sha256.Size]byte
	scanAfter      []byte
	resumeAfter    []byte
}

type RetainedPruneCursorWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
}

func (c RetainedPruneCursor) Phase() RetainedPrunePhase { return c.phase }

func (c RetainedPruneCursor) SourceCut() ChildArtifactSourceCut {
	return ChildArtifactSourceCut{
		LogicalDigest: c.logical, BaseDigest: c.base, EntryDigest: c.entry,
		Applied: c.applied, Term: c.term, RouteGeneration: c.routeGeneration,
	}
}

func (c RetainedPruneCursor) SourceCoordinates() TailSourceCoordinates {
	return TailSourceCoordinates{
		OwnershipEpoch: c.ownershipEpoch, RoutingVersion: c.routingVersion,
		RouteGeneration: c.routeGeneration,
	}
}

func (c RetainedPruneCursor) Progress() (
	scannedRows, scannedBytes, deletedRows, deletedKeyBytes uint64,
) {
	return c.scannedRows, c.scannedBytes, c.deletedRows, c.deletedKeyBytes
}

func (c RetainedPruneCursor) RetainedProof() (
	rows, bytes, snapshotGeneration uint64,
	digest [sha256.Size]byte,
	ok bool,
) {
	if c.phase != RetainedPruneComplete {
		return 0, 0, 0, [sha256.Size]byte{}, false
	}
	return c.remainingRows, c.remainingBytes, c.snapshotGeneration,
		c.retainedDigest, true
}

func AppendRetainedPruneCursor(
	dst []byte,
	cursor *RetainedPruneCursor,
) ([]byte, error) {
	return AppendRetainedPruneCursorWithWorkspace(
		dst, cursor, &RetainedPruneCursorWorkspace{},
	)
}

func AppendRetainedPruneCursorWithWorkspace(
	dst []byte,
	cursor *RetainedPruneCursor,
	workspace *RetainedPruneCursorWorkspace,
) ([]byte, error) {
	if workspace == nil || !validRetainedPruneCursor(cursor) {
		return dst, ErrRetainedPrune
	}
	total := retainedPruneCursorHeader + len(cursor.scanAfter) +
		len(cursor.resumeAfter) + sha256.Size
	if total > MaxRetainedPruneCursorBytes {
		return dst, ErrRetainedPrune
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], retainedPruneCursorMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], retainedPruneCursorFormat)
	binary.LittleEndian.PutUint16(frame[10:12], retainedPruneCursorHeader)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	frame[16], frame[17] = byte(cursor.phase), cursor.retained
	values := [...]uint64{
		cursor.applied, cursor.term, cursor.ownershipEpoch, cursor.routingVersion,
		cursor.routeGeneration, cursor.scannedRows, cursor.scannedBytes,
		cursor.deletedRows, cursor.deletedKeyBytes, cursor.pendingCount,
		cursor.pendingKeyBytes, cursor.remainingRows, cursor.remainingBytes,
		cursor.snapshotGeneration,
	}
	for index, value := range values {
		binary.LittleEndian.PutUint64(frame[24+index*8:32+index*8], value)
	}
	digests := [...][sha256.Size]byte{
		cursor.plan, cursor.placement, cursor.cutover, cursor.logical,
		cursor.base, cursor.entry, cursor.pending, cursor.retainedDigest,
	}
	for index := range digests {
		copy(frame[136+index*32:168+index*32], digests[index][:])
	}
	binary.LittleEndian.PutUint32(frame[392:396], uint32(len(cursor.scanAfter)))
	binary.LittleEndian.PutUint32(frame[396:400], uint32(len(cursor.resumeAfter)))
	at := retainedPruneCursorHeader
	at += copy(frame[at:], cursor.scanAfter)
	at += copy(frame[at:], cursor.resumeAfter)
	if at != total-sha256.Size {
		panic("rangesplit: retained prune cursor size diverged")
	}
	retainedPruneCursorDigestInto(workspace, frame[:at])
	copy(frame[at:], workspace.digest[:])
	return dst, nil
}

func OpenRetainedPruneCursor(raw []byte) (*RetainedPruneCursor, error) {
	if len(raw) < retainedPruneCursorHeader+sha256.Size ||
		len(raw) > MaxRetainedPruneCursorBytes ||
		!bytes.Equal(raw[0:8], retainedPruneCursorMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != retainedPruneCursorFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != retainedPruneCursorHeader ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) ||
		!allChildArtifactZero(raw[18:24]) {
		return nil, ErrRetainedPrune
	}
	scanBytes := int(binary.LittleEndian.Uint32(raw[392:396]))
	resumeBytes := int(binary.LittleEndian.Uint32(raw[396:400]))
	if scanBytes > replication.MaxMutationKeyBytes ||
		resumeBytes > replication.MaxMutationKeyBytes ||
		retainedPruneCursorHeader+scanBytes+resumeBytes+sha256.Size != len(raw) {
		return nil, ErrRetainedPrune
	}
	var workspace RetainedPruneCursorWorkspace
	retainedPruneCursorDigestInto(&workspace, raw[:len(raw)-sha256.Size])
	if !bytes.Equal(workspace.digest[:], raw[len(raw)-sha256.Size:]) {
		return nil, ErrRetainedPrune
	}
	values := [14]uint64{}
	for index := range values {
		values[index] = binary.LittleEndian.Uint64(raw[24+index*8 : 32+index*8])
	}
	cursor := &RetainedPruneCursor{
		phase: RetainedPrunePhase(raw[16]), retained: raw[17],
		applied: values[0], term: values[1], ownershipEpoch: values[2],
		routingVersion: values[3], routeGeneration: values[4],
		scannedRows: values[5], scannedBytes: values[6],
		deletedRows: values[7], deletedKeyBytes: values[8],
		pendingCount: values[9], pendingKeyBytes: values[10],
		remainingRows: values[11], remainingBytes: values[12],
		snapshotGeneration: values[13],
	}
	digests := []*[sha256.Size]byte{
		&cursor.plan, &cursor.placement, &cursor.cutover, &cursor.logical,
		&cursor.base, &cursor.entry, &cursor.pending, &cursor.retainedDigest,
	}
	for index := range digests {
		copy(digests[index][:], raw[136+index*32:168+index*32])
	}
	at := retainedPruneCursorHeader
	cursor.scanAfter = bytes.Clone(raw[at : at+scanBytes])
	at += scanBytes
	cursor.resumeAfter = bytes.Clone(raw[at : at+resumeBytes])
	if !validRetainedPruneCursor(cursor) {
		return nil, ErrRetainedPrune
	}
	return cursor, nil
}

func retainedPruneCursorDigestInto(
	workspace *RetainedPruneCursorWorkspace,
	frame []byte,
) {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	_, _ = workspace.hasher.Write(retainedPruneCursorDomain)
	_, _ = workspace.hasher.Write(frame)
	_ = workspace.hasher.Sum(workspace.digest[:0])
}

func validRetainedPruneCursor(cursor *RetainedPruneCursor) bool {
	if cursor == nil || cursor.retained >= 3 || cursor.applied == 0 ||
		cursor.applied == math.MaxUint64 || cursor.term == 0 || cursor.term == math.MaxUint64 ||
		cursor.ownershipEpoch == 0 || cursor.routingVersion == 0 ||
		cursor.routeGeneration == 0 || cursor.plan == ([sha256.Size]byte{}) ||
		cursor.placement == ([sha256.Size]byte{}) || cursor.cutover == ([sha256.Size]byte{}) ||
		cursor.logical == ([sha256.Size]byte{}) || cursor.base == ([sha256.Size]byte{}) ||
		cursor.entry == ([sha256.Size]byte{}) ||
		len(cursor.scanAfter) > replication.MaxMutationKeyBytes ||
		len(cursor.resumeAfter) > replication.MaxMutationKeyBytes {
		return false
	}
	switch cursor.phase {
	case RetainedPruneScan:
		return cursor.pendingCount == 0 && cursor.pendingKeyBytes == 0 &&
			cursor.pending == ([sha256.Size]byte{}) && len(cursor.resumeAfter) == 0 &&
			cursor.remainingRows == 0 && cursor.remainingBytes == 0 &&
			cursor.snapshotGeneration == 0 && cursor.retainedDigest == ([sha256.Size]byte{})
	case RetainedPruneAwaitingApply:
		return cursor.pendingCount != 0 && cursor.pendingCount <= replication.MaxMutations &&
			cursor.pendingKeyBytes != 0 && cursor.pending != ([sha256.Size]byte{}) &&
			len(cursor.resumeAfter) != 0 && cursor.remainingRows == 0 &&
			cursor.remainingBytes == 0 && cursor.snapshotGeneration == 0 &&
			cursor.retainedDigest == ([sha256.Size]byte{})
	case RetainedPruneComplete:
		return cursor.pendingCount == 0 && cursor.pendingKeyBytes == 0 &&
			cursor.pending == ([sha256.Size]byte{}) && len(cursor.resumeAfter) == 0 &&
			cursor.snapshotGeneration != 0 && cursor.retainedDigest != ([sha256.Size]byte{})
	default:
		return false
	}
}

var errRetainedPruneScanStop = errors.New("rangesplit: retained prune scan stop")
