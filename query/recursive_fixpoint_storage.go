package query

import (
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
	"unsafe"

	"github.com/thesyncim/vibejson/x/byteview"
)

type recursiveCellStorageFlag uint8

const (
	recursiveCellHasRaw recursiveCellStorageFlag = 1 << iota
	recursiveCellHasText
)

// recursiveCell stores offsets rather than slices. The packed arena may grow
// between callbacks without invalidating any earlier cell, which is the key
// property an incrementally growing fixpoint result needs.
type recursiveCell struct {
	rawOffset  int
	rawLength  int
	textOffset int
	textLength int
	word       uint64
	kind       ValueType
	cellFlags  cellFlag
	storeFlags recursiveCellStorageFlag
}

// recursiveSpool is a reusable row-major physical spool. ends records the
// packed-data end after each row, making DISTINCT rollback O(columns) with no
// scan and letting publication copy one contiguous arena.
type recursiveSpool struct {
	cells   []recursiveCell
	data    []byte
	ends    []int
	rows    int
	columns int
}

func (s *recursiveSpool) reset() {
	if s == nil {
		return
	}
	s.cells = s.cells[:0]
	s.data = s.data[:0]
	s.ends = s.ends[:0]
	s.rows = 0
}

func (s *recursiveSpool) release() {
	if s == nil {
		return
	}
	*s = recursiveSpool{}
}

func (s *recursiveSpool) retainedBytes() int64 {
	if s == nil {
		return 0
	}
	return saturatedBytes(
		saturatedProduct(int64(len(s.cells)), int64(unsafe.Sizeof(recursiveCell{}))),
		saturatedBytes(
			int64(len(s.data)),
			saturatedProduct(int64(len(s.ends)), int64(unsafe.Sizeof(int(0)))),
		),
	)
}

func recursiveRowRetainedBytes(columns int, payload int64) int64 {
	if columns <= 0 || payload < 0 {
		return math.MaxInt64
	}
	return saturatedBytes(
		saturatedProduct(int64(columns), int64(unsafe.Sizeof(recursiveCell{}))),
		saturatedBytes(payload, int64(unsafe.Sizeof(int(0)))),
	)
}

func measureRecursiveRow(row []Cell, cancel *CancelFlag) (int64, error) {
	payload := int64(0)
	for column := range row {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return 0, err
		}
		if row[column].kind < TypeNull || row[column].kind > TypeJSON {
			return 0, fmt.Errorf(
				"query: recursive row column %d has invalid value type %d: %w",
				column, row[column].kind, ErrRecursiveConfig,
			)
		}
		bytes, err := relationCellOwnedBytesCancelable(row[column], cancel)
		if err != nil {
			return 0, err
		}
		payload = saturatedBytes(payload, int64(bytes))
		if payload == math.MaxInt64 {
			return 0, ErrRecursiveSize
		}
	}
	return payload, cancellationError(cancel)
}

func (s *recursiveSpool) appendMeasuredRow(
	row []Cell,
	payload int64,
	cancel *CancelFlag,
) error {
	if s == nil || len(row) != s.columns || payload < 0 || payload > int64(math.MaxInt) {
		return ErrRecursiveSize
	}
	cellCount, ok := checkedRecursiveAdd(len(s.cells), s.columns)
	if !ok || len(s.data) > math.MaxInt-int(payload) {
		return ErrRecursiveSize
	}
	dataNeed := len(s.data) + int(payload)
	rowCount, ok := checkedRecursiveAdd(s.rows, 1)
	if !ok {
		return ErrRecursiveSize
	}
	if err := s.reserve(cellCount, dataNeed, rowCount); err != nil {
		return err
	}
	cellStart, dataStart := len(s.cells), len(s.data)
	for column := range row {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			s.cells = s.cells[:cellStart]
			s.data = s.data[:dataStart]
			return err
		}
		stored, err := s.appendCell(row[column], cancel)
		if err != nil {
			s.cells = s.cells[:cellStart]
			s.data = s.data[:dataStart]
			return err
		}
		s.cells = append(s.cells, stored)
	}
	if len(s.data)-dataStart != int(payload) {
		s.cells = s.cells[:cellStart]
		s.data = s.data[:dataStart]
		return fmt.Errorf(
			"query: recursive row copied %d payload bytes after measuring %d: %w",
			len(s.data)-dataStart, payload, ErrRecursiveSize,
		)
	}
	s.ends = append(s.ends, len(s.data))
	s.rows++
	return cancellationError(cancel)
}

func (s *recursiveSpool) reserve(cells, data, rows int) error {
	if cells < len(s.cells) || data < len(s.data) || rows < len(s.ends) {
		return ErrRecursiveSize
	}
	var err error
	s.cells, err = reserveRecursiveSlice(s.cells, cells)
	if err != nil {
		return err
	}
	s.data, err = reserveRecursiveSlice(s.data, data)
	if err != nil {
		return err
	}
	s.ends, err = reserveRecursiveSlice(s.ends, rows)
	return err
}

// reserveRecursiveSlice reserves capacity without changing length. Keeping the
// append length unchanged lets a failed row restore its two marks exactly.
func reserveRecursiveSlice[T any](slice []T, need int) ([]T, error) {
	if need < 0 {
		return slice, ErrRecursiveSize
	}
	if cap(slice) >= need {
		return slice, nil
	}
	capacity := cap(slice)
	if capacity < 8 {
		capacity = 8
	}
	for capacity < need {
		if capacity > math.MaxInt/2 {
			capacity = need
			break
		}
		capacity *= 2
	}
	if capacity < need {
		return slice, ErrRecursiveSize
	}
	next := make([]T, len(slice), capacity)
	copy(next, slice)
	return next, nil
}

func (s *recursiveSpool) appendCell(cell Cell, cancel *CancelFlag) (recursiveCell, error) {
	stored := recursiveCell{
		word:      cell.word,
		kind:      cell.kind,
		cellFlags: cell.flag,
	}
	if cell.kind == TypeNull && cell.flag&cellMissing != 0 {
		return stored, cancellationError(cancel)
	}

	raw := cell.raw
	if raw != nil {
		stored.rawOffset = len(s.data)
		stored.rawLength = len(raw)
		stored.storeFlags |= recursiveCellHasRaw
		s.data = append(s.data, raw...)
		if cell.kind == TypeString {
			escaped, err := relationJSONStringEscapedCancelable(raw, cancel)
			if err != nil {
				return recursiveCell{}, err
			}
			if escaped {
				text, _ := cell.Text()
				stored.textOffset = len(s.data)
				stored.textLength = len(text)
				stored.storeFlags |= recursiveCellHasText
				s.data = append(s.data, text...)
			}
		}
		return stored, cancellationError(cancel)
	}

	switch cell.kind {
	case TypeNull, TypeBool, TypeJSON:
		return stored, cancellationError(cancel)
	case TypeNumber:
		stored.rawOffset = len(s.data)
		stored.storeFlags |= recursiveCellHasRaw
		s.data = cell.AppendJSON(s.data)
		stored.rawLength = len(s.data) - stored.rawOffset
		if cell.flag&cellInteger == 0 {
			stored.cellFlags |= cellNumberRaw
		}
		return stored, cancellationError(cancel)
	case TypeString:
		text, _ := cell.Text()
		stored.rawOffset = len(s.data)
		stored.storeFlags |= recursiveCellHasRaw
		var escaped bool
		var err error
		s.data, escaped, err = appendRecursiveJSONString(s.data, text, cancel)
		if err != nil {
			return recursiveCell{}, err
		}
		stored.rawLength = len(s.data) - stored.rawOffset
		if escaped {
			stored.textOffset = len(s.data)
			stored.textLength = len(text)
			stored.storeFlags |= recursiveCellHasText
			s.data = append(s.data, text...)
		}
		return stored, cancellationError(cancel)
	default:
		return recursiveCell{}, ErrRecursiveConfig
	}
}

func appendRecursiveJSONString(
	dst []byte,
	text string,
	cancel *CancelFlag,
) ([]byte, bool, error) {
	dst = append(dst, '"')
	escaped := false
	const hex = "0123456789abcdef"
	for i := 0; i < len(text); i++ {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return dst, escaped, err
		}
		switch b := text[i]; b {
		case '"', '\\':
			dst = append(dst, '\\', b)
			escaped = true
		case '\b':
			dst = append(dst, '\\', 'b')
			escaped = true
		case '\f':
			dst = append(dst, '\\', 'f')
			escaped = true
		case '\n':
			dst = append(dst, '\\', 'n')
			escaped = true
		case '\r':
			dst = append(dst, '\\', 'r')
			escaped = true
		case '\t':
			dst = append(dst, '\\', 't')
			escaped = true
		default:
			if b < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xf])
				escaped = true
			} else {
				dst = append(dst, b)
			}
		}
	}
	return append(dst, '"'), escaped, cancellationError(cancel)
}

func (s *recursiveSpool) rollbackLastRow() {
	if s == nil || s.rows == 0 {
		return
	}
	previousData := 0
	if s.rows > 1 {
		previousData = s.ends[s.rows-2]
	}
	s.cells = s.cells[:len(s.cells)-s.columns]
	s.data = s.data[:previousData]
	s.ends = s.ends[:s.rows-1]
	s.rows--
}

func (s *recursiveSpool) appendSpool(src *recursiveSpool, cancel *CancelFlag) error {
	if s == nil || src == nil || s.columns != src.columns || src.rows == 0 {
		if src != nil && src.rows == 0 {
			return cancellationError(cancel)
		}
		return ErrRecursiveConfig
	}
	addCells := len(src.cells)
	cellNeed, ok := checkedRecursiveAdd(len(s.cells), addCells)
	if !ok {
		return ErrRecursiveSize
	}
	dataNeed, ok := checkedRecursiveAdd(len(s.data), len(src.data))
	if !ok {
		return ErrRecursiveSize
	}
	rowNeed, ok := checkedRecursiveAdd(s.rows, src.rows)
	if !ok {
		return ErrRecursiveSize
	}
	if err := s.reserve(cellNeed, dataNeed, rowNeed); err != nil {
		return err
	}
	cellStart, dataStart, endStart, rowStart := len(s.cells), len(s.data), len(s.ends), s.rows
	const copyChunk = 32 << 10
	remaining := src.data
	for len(remaining) != 0 {
		if err := cancellationError(cancel); err != nil {
			s.cells = s.cells[:cellStart]
			s.data = s.data[:dataStart]
			s.ends = s.ends[:endStart]
			s.rows = rowStart
			return err
		}
		n := min(len(remaining), copyChunk)
		s.data = append(s.data, remaining[:n]...)
		remaining = remaining[n:]
	}
	for index, source := range src.cells {
		if err := cancellationCheckpoint(cancel, index); err != nil {
			s.cells = s.cells[:cellStart]
			s.data = s.data[:dataStart]
			s.ends = s.ends[:endStart]
			s.rows = rowStart
			return err
		}
		cell := source
		if cell.storeFlags&recursiveCellHasRaw != 0 {
			cell.rawOffset += dataStart
		}
		if cell.storeFlags&recursiveCellHasText != 0 {
			cell.textOffset += dataStart
		}
		s.cells = append(s.cells, cell)
	}
	for row, end := range src.ends {
		if err := cancellationCheckpoint(cancel, row); err != nil {
			s.cells = s.cells[:cellStart]
			s.data = s.data[:dataStart]
			s.ends = s.ends[:endStart]
			s.rows = rowStart
			return err
		}
		s.ends = append(s.ends, dataStart+end)
	}
	s.rows = rowNeed
	return cancellationError(cancel)
}

func (s *recursiveSpool) stored(row, column int) recursiveCell {
	return s.cells[row*s.columns+column]
}

func (s *recursiveSpool) cell(row, column int) Cell {
	stored := s.stored(row, column)
	raw := []byte(nil)
	if stored.storeFlags&recursiveCellHasRaw != 0 {
		raw = s.data[stored.rawOffset : stored.rawOffset+stored.rawLength : stored.rawOffset+stored.rawLength]
	}
	switch stored.kind {
	case TypeNull:
		if stored.cellFlags&cellMissing != 0 {
			return Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
		}
		if raw == nil {
			raw = nullBytes
		}
		return Cell{kind: TypeNull, raw: raw}
	case TypeBool:
		if raw == nil {
			raw = falseBytes
			if stored.cellFlags&cellTrue != 0 {
				raw = trueBytes
			}
		}
		return Cell{kind: TypeBool, flag: stored.cellFlags & cellTrue, raw: raw}
	case TypeNumber:
		return Cell{
			kind: TypeNumber, flag: stored.cellFlags, word: stored.word, raw: raw,
		}
	case TypeString:
		text := ""
		if stored.storeFlags&recursiveCellHasText != 0 {
			text = byteview.String(
				s.data[stored.textOffset : stored.textOffset+stored.textLength : stored.textOffset+stored.textLength],
			)
		} else if len(raw) >= 2 {
			text = byteview.String(raw[1 : len(raw)-1 : len(raw)-1])
		}
		return Cell{kind: TypeString, text: text, raw: raw}
	default:
		return Cell{kind: TypeJSON, raw: raw}
	}
}

func (s *recursiveSpool) scalar(row, column int) scalar {
	stored := s.stored(row, column)
	raw := []byte(nil)
	if stored.storeFlags&recursiveCellHasRaw != 0 {
		raw = s.data[stored.rawOffset : stored.rawOffset+stored.rawLength : stored.rawOffset+stored.rawLength]
	}
	switch stored.kind {
	case TypeNull:
		if stored.cellFlags&cellMissing != 0 {
			return scalar{kind: kindNull}
		}
		if raw == nil {
			raw = nullBytes
		}
		return scalar{kind: kindNull, raw: raw}
	case TypeBool:
		return scalar{kind: kindBool, bval: stored.cellFlags&cellTrue != 0, raw: raw}
	case TypeNumber:
		return scalar{
			kind: kindNumber, num: raw, raw: raw,
			isInt: stored.cellFlags&cellInteger != 0,
			ival:  int64(stored.word),
		}
	case TypeString:
		text := ""
		if stored.storeFlags&recursiveCellHasText != 0 {
			text = byteview.String(
				s.data[stored.textOffset : stored.textOffset+stored.textLength : stored.textOffset+stored.textLength],
			)
		} else if len(raw) >= 2 {
			text = byteview.String(raw[1 : len(raw)-1 : len(raw)-1])
		}
		return scalar{kind: kindString, sval: text, raw: raw}
	default:
		return scalar{kind: kindContainer, raw: raw}
	}
}

func (s *recursiveSpool) missing(row, column int) bool {
	return s.stored(row, column).cellFlags&cellMissing != 0
}

type recursiveIdentityEntry struct {
	hash    uint64
	row     int
	working bool
}

// recursiveIdentity owns the reusable UNION DISTINCT first-occurrence index.
// slots contain entry index + 1 so zero is the empty marker.
type recursiveIdentity struct {
	seed      maphash.Seed
	seedReady bool
	slots     []uint32
	entries   []recursiveIdentityEntry
}

func (i *recursiveIdentity) begin() {
	if !i.seedReady {
		i.seed = maphash.MakeSeed()
		i.seedReady = true
	}
	i.reset()
}

func (i *recursiveIdentity) reset() {
	if i == nil {
		return
	}
	i.slots = i.slots[:0]
	i.entries = i.entries[:0]
}

func (i *recursiveIdentity) release() {
	if i == nil {
		return
	}
	*i = recursiveIdentity{}
}

func (i *recursiveIdentity) retainedBytes() int64 {
	if i == nil {
		return 0
	}
	return recursiveIdentityRetainedBytes(len(i.slots), len(i.entries))
}

func recursiveIdentityRetainedBytes(slots, entries int) int64 {
	if slots < 0 || entries < 0 {
		return math.MaxInt64
	}
	return saturatedBytes(
		saturatedProduct(int64(slots), int64(unsafe.Sizeof(uint32(0)))),
		saturatedProduct(int64(entries), int64(unsafe.Sizeof(recursiveIdentityEntry{}))),
	)
}

func (i *recursiveIdentity) retainedBytesForInsert() (int64, error) {
	entries, ok := checkedRecursiveAdd(len(i.entries), 1)
	if !ok || uint64(entries) >= uint64(math.MaxUint32) {
		return 0, ErrRecursiveSize
	}
	slots, err := recursiveIdentityTableCapacity(entries)
	if err != nil {
		return 0, err
	}
	return recursiveIdentityRetainedBytes(slots, entries), nil
}

func recursiveIdentityTableCapacity(entries int) (int, error) {
	if entries < 0 || uint64(entries) >= uint64(math.MaxUint32) || entries > math.MaxInt/2 {
		return 0, ErrRecursiveSize
	}
	if entries == 0 {
		return 0, nil
	}
	need := entries * 2
	capacity := 8
	for capacity < need {
		if capacity > math.MaxInt/2 {
			return 0, ErrRecursiveSize
		}
		capacity <<= 1
	}
	return capacity, nil
}

func (i *recursiveIdentity) ensureTable(entries int) error {
	capacity, err := recursiveIdentityTableCapacity(entries)
	if err != nil {
		return err
	}
	if len(i.slots) >= capacity {
		return nil
	}
	if cap(i.slots) < capacity {
		i.slots = make([]uint32, capacity)
	} else {
		i.slots = i.slots[:capacity]
		clear(i.slots)
	}
	mask := uint64(capacity - 1)
	for entry := range i.entries {
		slot := i.entries[entry].hash & mask
		for i.slots[slot] != 0 {
			slot = (slot + 1) & mask
		}
		i.slots[slot] = uint32(entry + 1)
	}
	return nil
}

func (i *recursiveIdentity) find(
	hash uint64,
	candidate *recursiveSpool,
	row int,
	result, working *recursiveSpool,
	columns int,
	cancel *CancelFlag,
) (*recursiveIdentityEntry, bool, error) {
	if len(i.slots) == 0 {
		return nil, false, cancellationError(cancel)
	}
	mask := uint64(len(i.slots) - 1)
	for slot, probes := hash&mask, 0; probes < len(i.slots); probes++ {
		if err := cancellationCheckpoint(cancel, probes); err != nil {
			return nil, false, err
		}
		stored := i.slots[slot]
		if stored == 0 {
			return nil, false, nil
		}
		entry := &i.entries[stored-1]
		if entry.hash == hash {
			relation := result
			if entry.working {
				relation = working
			}
			equal, err := recursiveRowsEqual(
				relation, entry.row, candidate, row, columns, cancel,
			)
			if err != nil {
				return nil, false, err
			}
			if equal {
				return entry, true, nil
			}
		}
		slot = (slot + 1) & mask
	}
	return nil, false, ErrRecursiveSize
}

func (i *recursiveIdentity) insert(
	hash uint64,
	row int,
	working bool,
	cancel *CancelFlag,
) error {
	entries, ok := checkedRecursiveAdd(len(i.entries), 1)
	if !ok {
		return ErrRecursiveSize
	}
	if err := i.ensureTable(entries); err != nil {
		return err
	}
	if err := cancellationError(cancel); err != nil {
		return err
	}
	var reserveErr error
	i.entries, reserveErr = reserveRecursiveSlice(i.entries, entries)
	if reserveErr != nil {
		return reserveErr
	}
	entry := len(i.entries)
	i.entries = append(i.entries, recursiveIdentityEntry{
		hash: hash, row: row, working: working,
	})
	mask := uint64(len(i.slots) - 1)
	for slot, probes := hash&mask, 0; probes < len(i.slots); probes++ {
		if err := cancellationCheckpoint(cancel, probes); err != nil {
			i.entries = i.entries[:entry]
			return err
		}
		if i.slots[slot] == 0 {
			i.slots[slot] = uint32(entry + 1)
			return nil
		}
		slot = (slot + 1) & mask
	}
	i.entries = i.entries[:entry]
	return ErrRecursiveSize
}

func (i *recursiveIdentity) promoteWorking(base int) {
	// Working entries are the contiguous tail appended during this level.
	// Stop at the first prior result entry so promotion stays O(delta), not
	// O(result) per breadth-first level.
	for entry := len(i.entries) - 1; entry >= 0 && i.entries[entry].working; entry-- {
		i.entries[entry].row += base
		i.entries[entry].working = false
	}
}

func recursiveRowsEqual(
	left *recursiveSpool,
	leftRow int,
	right *recursiveSpool,
	rightRow int,
	columns int,
	cancel *CancelFlag,
) (bool, error) {
	for column := 0; column < columns; column++ {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return false, err
		}
		if compareScalar(left.scalar(leftRow, column), right.scalar(rightRow, column)) != 0 {
			return false, nil
		}
	}
	return true, cancellationError(cancel)
}

func hashRecursiveRow(
	seed maphash.Seed,
	relation *recursiveSpool,
	row, columns int,
	cancel *CancelFlag,
) (uint64, error) {
	hash := uint64(0x243f6a8885a308d3) ^ uint64(columns)
	for column := 0; column < columns; column++ {
		if err := cancellationCheckpoint(cancel, column); err != nil {
			return 0, err
		}
		value := relation.scalar(row, column)
		valueHash := uint64(0x9e3779b97f4a7c15)
		if value.kind != kindNull {
			valueHash = hashJoinValue(seed, value)
		}
		valueHash ^= uint64(value.kind) * 0xbf58476d1ce4e5b9
		hash ^= valueHash + 0x9e3779b97f4a7c15 + uint64(column)
		hash = bits.RotateLeft64(hash, 27)*0x94d049bb133111eb + 0x52dce729
	}
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ (hash >> 31), cancellationError(cancel)
}

func checkedRecursiveAdd(left, right int) (int, bool) {
	if left < 0 || right < 0 || left > math.MaxInt-right {
		return 0, false
	}
	return left + right, true
}
