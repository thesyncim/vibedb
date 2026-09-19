package replicatedstate

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	// MaxPointReadBatchBytes is the physical request and result ceiling. Batch
	// cardinality is therefore byte-derived rather than a small product limit.
	MaxPointReadBatchBytes   = replication.MaxCommandBytes
	pointReadBatchCountBytes = 4
	pointReadBatchEntryBytes = 1 + 4
)

var ErrPointReadBatch = errors.New("replicatedstate: invalid point-read batch")

// PointRead identifies one dense relation/key lookup. Key is borrowed while
// AppendPointReadBatch runs.
type PointRead struct {
	Relation replication.RelationID
	Key      []byte
}

// PointReadBatchRequest is one canonical packed positional request. Its
// grammar is count:u32 followed by count relation:u8,key-length:u32,key tuples.
// The byte ceiling implies the count ceiling; there is no 64-item boundary.
type PointReadBatchRequest struct {
	raw   []byte
	count uint32
}

func (request PointReadBatchRequest) Count() int { return int(request.count) }

// AppendPointReadBatch appends the sole packed request grammar.
func AppendPointReadBatch(dst []byte, reads []PointRead) ([]byte, error) {
	if len(reads) == 0 || uint64(len(reads)) > uint64(^uint32(0)) {
		return dst, ErrPointReadBatch
	}
	start := len(dst)
	total := uint64(pointReadBatchCountBytes)
	for index := range reads {
		read := reads[index]
		if read.Relation == 0 || read.Relation > replication.MaxRelationID ||
			len(read.Key) == 0 || len(read.Key) > replication.MaxMutationKeyBytes {
			return dst, ErrPointReadBatch
		}
		total += pointReadBatchEntryBytes + uint64(len(read.Key))
		if total > MaxPointReadBatchBytes {
			return dst, ErrPointReadBatch
		}
	}
	dst = slices.Grow(dst, int(total))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(reads)))
	for index := range reads {
		dst = append(dst, byte(reads[index].Relation))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(reads[index].Key)))
		dst = append(dst, reads[index].Key...)
	}
	if len(dst)-start != int(total) {
		panic("replicatedstate: point-read batch size invariant")
	}
	return dst, nil
}

// OpenPointReadBatch validates one exact packed request without allocation.
func OpenPointReadBatch(src []byte) (PointReadBatchRequest, error) {
	if len(src) < pointReadBatchCountBytes || len(src) > MaxPointReadBatchBytes {
		return PointReadBatchRequest{}, ErrPointReadBatch
	}
	count := binary.LittleEndian.Uint32(src)
	if count == 0 || uint64(count) > uint64(len(src)-pointReadBatchCountBytes)/
		pointReadBatchEntryBytes {
		return PointReadBatchRequest{}, ErrPointReadBatch
	}
	offset := pointReadBatchCountBytes
	for ordinal := uint32(0); ordinal < count; ordinal++ {
		if len(src)-offset < pointReadBatchEntryBytes {
			return PointReadBatchRequest{}, ErrPointReadBatch
		}
		relation := replication.RelationID(src[offset])
		keyBytes := binary.LittleEndian.Uint32(src[offset+1 : offset+5])
		offset += pointReadBatchEntryBytes
		if relation == 0 || relation > replication.MaxRelationID || keyBytes == 0 ||
			keyBytes > replication.MaxMutationKeyBytes || uint64(keyBytes) > uint64(len(src)-offset) {
			return PointReadBatchRequest{}, ErrPointReadBatch
		}
		offset += int(keyBytes)
	}
	if offset != len(src) {
		return PointReadBatchRequest{}, ErrPointReadBatch
	}
	return PointReadBatchRequest{raw: src, count: count}, nil
}

func (request PointReadBatchRequest) each(fn func(uint32, replication.RelationID, []byte) error) error {
	if request.count == 0 || len(request.raw) < pointReadBatchCountBytes {
		return ErrPointReadBatch
	}
	offset := pointReadBatchCountBytes
	for ordinal := uint32(0); ordinal < request.count; ordinal++ {
		relation := replication.RelationID(request.raw[offset])
		keyBytes := int(binary.LittleEndian.Uint32(request.raw[offset+1 : offset+5]))
		offset += pointReadBatchEntryBytes
		key := request.raw[offset : offset+keyBytes : offset+keyBytes]
		offset += keyBytes
		if err := fn(ordinal, relation, key); err != nil {
			return err
		}
	}
	return nil
}

// PointReadBatchResult owns one packed positional value. Data is count:u32,
// ceil(count/8) found bits, count raw u32 value lengths, then concatenated raw
// values. Empty found values remain distinct from misses through the bitmap.
type PointReadBatchResult struct {
	Fence SnapshotFence
	Data  []byte
}

// PointReadBatchValue is an allocation-free opened result.
type PointReadBatchValue struct {
	count   uint32
	found   []byte
	lengths []byte
	payload []byte
}

func pointReadBatchFixedBytes(count uint32) (int, bool) {
	bitmap := (uint64(count) + 7) / 8
	total := uint64(pointReadBatchCountBytes) + bitmap + uint64(count)*4
	return int(total), count != 0 && total <= MaxPointReadBatchBytes
}

func OpenPointReadBatchValue(src []byte) (PointReadBatchValue, error) {
	if len(src) < pointReadBatchCountBytes || len(src) > MaxPointReadBatchBytes {
		return PointReadBatchValue{}, ErrPointReadBatch
	}
	count := binary.LittleEndian.Uint32(src)
	fixed, ok := pointReadBatchFixedBytes(count)
	if !ok || fixed > len(src) {
		return PointReadBatchValue{}, ErrPointReadBatch
	}
	bitmapBytes := (int(count) + 7) / 8
	view := PointReadBatchValue{count: count, found: src[4 : 4+bitmapBytes]}
	view.lengths = src[4+bitmapBytes : fixed]
	view.payload = src[fixed:]
	if count&7 != 0 && view.found[len(view.found)-1]>>uint(count&7) != 0 {
		return PointReadBatchValue{}, ErrPointReadBatch
	}
	payloadBytes := uint64(0)
	for ordinal := uint32(0); ordinal < count; ordinal++ {
		length := binary.LittleEndian.Uint32(view.lengths[ordinal*4:])
		found := view.found[ordinal/8]&(1<<uint(ordinal&7)) != 0
		if !found && length != 0 {
			return PointReadBatchValue{}, ErrPointReadBatch
		}
		payloadBytes += uint64(length)
		if payloadBytes > uint64(len(view.payload)) {
			return PointReadBatchValue{}, ErrPointReadBatch
		}
	}
	if payloadBytes != uint64(len(view.payload)) {
		return PointReadBatchValue{}, ErrPointReadBatch
	}
	return view, nil
}

func (value PointReadBatchValue) Count() int { return int(value.count) }

// PointReadBatchCursor streams every position in O(count) total time. Prefer
// it when consuming the complete result; Lookup is intended for sparse probes.
type PointReadBatchCursor struct {
	value   PointReadBatchValue
	ordinal uint32
	offset  uint64
}

func (value PointReadBatchValue) Cursor() PointReadBatchCursor {
	return PointReadBatchCursor{value: value}
}

func (cursor *PointReadBatchCursor) Next() (raw []byte, found bool, ok bool) {
	if cursor == nil || cursor.ordinal >= cursor.value.count {
		return nil, false, false
	}
	ordinal := cursor.ordinal
	length := uint64(binary.LittleEndian.Uint32(cursor.value.lengths[ordinal*4:]))
	found = cursor.value.found[ordinal/8]&(1<<uint(ordinal&7)) != 0
	raw = cursor.value.payload[cursor.offset : cursor.offset+length : cursor.offset+length]
	cursor.offset += length
	cursor.ordinal++
	return raw, found, true
}

// Lookup returns one raw positional value. The returned bytes alias the packed
// result and remain valid for the result owner's lifetime.
func (value PointReadBatchValue) Lookup(index int) (raw []byte, found bool, ok bool) {
	if index < 0 || uint64(index) >= uint64(value.count) {
		return nil, false, false
	}
	offset := uint64(0)
	for ordinal := 0; ordinal < index; ordinal++ {
		offset += uint64(binary.LittleEndian.Uint32(value.lengths[ordinal*4:]))
	}
	length := uint64(binary.LittleEndian.Uint32(value.lengths[index*4:]))
	found = value.found[index/8]&(1<<uint(index&7)) != 0
	return value.payload[offset : offset+length : offset+length], found, true
}

// PointReadBatchInto captures one all-relation generation, rejects the whole
// batch if any key is covered by an active intent, then materializes the
// positional result. It never returns a partial batch.
func (m *Machine) PointReadBatchInto(
	packed []byte,
	minimumApplied uint64,
	maxResultBytes int,
	dst []byte,
) (PointReadBatchResult, error) {
	request, err := OpenPointReadBatch(packed)
	if err != nil || m == nil || minimumApplied == 0 || maxResultBytes <= 0 ||
		maxResultBytes > MaxPointReadBatchBytes {
		return PointReadBatchResult{}, ErrPointReadBatch
	}
	fixed, ok := pointReadBatchFixedBytes(request.count)
	if !ok || fixed > maxResultBytes {
		return PointReadBatchResult{}, ErrReadBufferBound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return PointReadBatchResult{}, err
	}
	err = request.each(func(_ uint32, relation replication.RelationID, key []byte) error {
		if int(relation) > len(m.relations) {
			return ErrInvalidCollection
		}
		selected := m.relations[int(relation)-1]
		if selected.id != relation || len(key) > selected.target.Limits.MaxKeyBytes {
			return ErrInvalidCollection
		}
		if !relationOwnsPoint(selected, key, m.state.Binding.OwnedRange) {
			return ErrWrongBinding
		}
		return nil
	})
	if err != nil {
		return PointReadBatchResult{}, err
	}
	if m.publication.Applied < minimumApplied {
		return PointReadBatchResult{}, ErrReadBehind
	}
	if err := durable.SnapshotCollectionsInto(&m.applyCut, m.members); err != nil {
		return PointReadBatchResult{}, m.fail(err)
	}
	closeCut := func(cause error) (PointReadBatchResult, error) {
		if closeErr := m.applyCut.Close(); closeErr != nil {
			return PointReadBatchResult{}, errors.Join(cause, closeErr)
		}
		return PointReadBatchResult{}, cause
	}
	systemSnapshot, ok := m.applyCut.CollectionHandle(m.system.Collection)
	if !ok || systemSnapshot == nil {
		_, closeErr := closeCut(ErrInconsistentSnapshot)
		return PointReadBatchResult{}, m.fail(closeErr)
	}
	// This complete first pass is the atomic visibility gate. No user value is
	// copied until every point is proven free of an active transaction intent.
	err = request.each(func(_ uint32, relation replication.RelationID, key []byte) error {
		_, blocked, lookupErr := lookupTransactionIntentOwner(
			pointSnapshot{value: systemSnapshot}, relation, key,
		)
		if lookupErr != nil {
			return lookupErr
		}
		if blocked {
			return ErrTransactionIntentActive
		}
		return nil
	})
	if err != nil {
		_, closeErr := closeCut(err)
		if errors.Is(err, ErrTransactionIntentActive) {
			return PointReadBatchResult{}, closeErr
		}
		return PointReadBatchResult{}, m.fail(closeErr)
	}
	dst = slices.Grow(dst[:0], maxResultBytes)
	dst = binary.LittleEndian.AppendUint32(dst, request.count)
	bitmapStart := len(dst)
	bitmapBytes := (int(request.count) + 7) / 8
	dst = append(dst, make([]byte, bitmapBytes+int(request.count)*4)...)
	lengthStart := bitmapStart + bitmapBytes
	var scratch []byte
	err = request.each(func(ordinal uint32, relation replication.RelationID, key []byte) error {
		selected := m.relations[int(relation)-1]
		snapshot, exists := m.applyCut.CollectionHandle(selected.target.Collection)
		if !exists || snapshot == nil {
			return ErrInconsistentSnapshot
		}
		start := len(dst)
		maximum := selected.target.Limits.MaxDocumentBytes
		direct := cap(dst)-len(dst) >= maximum
		readDst := dst
		if !direct {
			// AppendRaw grows to the actual document size. The schema maximum
			// is an admission bound, not a scratch allocation requirement.
			readDst = scratch[:0]
		}
		value, found, readErr := snapshot.AppendRaw(readDst, key)
		if readErr != nil {
			return readErr
		}
		valueBytes := len(value)
		if direct {
			valueBytes -= start
			dst = value
		} else {
			scratch = value[:0]
			if valueBytes <= maxResultBytes-len(dst) {
				dst = append(dst, value...)
			}
		}
		if valueBytes > maxResultBytes-start || len(dst) > maxResultBytes ||
			len(dst)-start > replication.MaxMutationValueBytes {
			return ErrReadBufferBound
		}
		if found {
			dst[bitmapStart+int(ordinal)/8] |= 1 << uint(ordinal&7)
		}
		binary.LittleEndian.PutUint32(dst[lengthStart+int(ordinal)*4:], uint32(len(dst)-start))
		return nil
	})
	if err != nil {
		_, closeErr := closeCut(err)
		if errors.Is(err, ErrReadBufferBound) {
			return PointReadBatchResult{}, closeErr
		}
		return PointReadBatchResult{}, m.fail(closeErr)
	}
	result := PointReadBatchResult{Fence: SnapshotFence{
		Binding: m.state.Binding, RelationManifestDigest: m.manifestDigest,
		ReplicaSetVersion: m.publication.ReplicaSetVersion,
		Applied:           m.state.Applied, LastTerm: m.state.LastTerm,
		LastEntryDigest: m.state.LastEntryDigest, DataChainDigest: m.state.DataChainDigest,
		SnapshotBaseDigest: m.state.SnapshotBaseDigest,
	}, Data: dst}
	if err := m.applyCut.Close(); err != nil {
		return PointReadBatchResult{}, m.fail(err)
	}
	return result, nil
}
