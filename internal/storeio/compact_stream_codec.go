package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/bits"
	"slices"
	"strconv"

	"github.com/thesyncim/vibejson"
)

// Compact scalar streams are the independently decodable columns inside the
// production VCS1 primary-stripe format. The codec is schema-free: the caller
// supplies exact canonical scalar spellings in row order, and the planner
// chooses the smallest reversible representation. Every sequential codec has
// bounded restart points so a point read never replays an unbounded stripe prefix.
const (
	compactStreamRestart = 64
	compactStreamHeader  = 12

	compactStreamDictionary uint8 = iota
	compactStreamFront
	compactStreamFOR
	compactStreamDelta
	compactStreamDate
	compactStreamPrefixInt
	compactStreamDeltaPack
	compactStreamAlphabet
	compactStreamKindLimit
	compactDictionaryHashThreshold = 16
	compactDictionaryScanPreferred = 128
)

// UnifiedIntegerOrder names the four exact signed-integer predicates accepted
// by the storage-native ordered COUNT lane. It is intentionally shared with
// durable and kept separate from query's parser operators.
type UnifiedIntegerOrder uint8

const (
	UnifiedIntegerLess UnifiedIntegerOrder = iota
	UnifiedIntegerLessEqual
	UnifiedIntegerGreater
	UnifiedIntegerGreaterEqual
)

func validUnifiedIntegerOrder(op UnifiedIntegerOrder) bool {
	return op <= UnifiedIntegerGreaterEqual
}

// UnifiedIntegerInterval is the normalized form of two ordered integer
// comparisons. Lower is inclusive; Upper is exclusive when UpperUnbounded is
// false. An unbounded upper endpoint represents values through MaxInt64.
// Keeping the normalized form in the storage layer avoids doing endpoint
// arithmetic while a compact stream is being scanned.
type UnifiedIntegerInterval struct {
	Lower          int64
	Upper          int64
	UpperUnbounded bool
}

type compactStreamEncoding struct {
	kind  uint8
	width uint8
	count int
	data  []byte
	dict  [][]byte
}

type compactAlphabetPlan struct {
	width      int
	prefix     int
	suffix     int
	totalBytes int
	encoded    int
}

// compactStreamScratch retains every candidate's backing storage while the
// planner chooses the smallest representation. A stripe emits the winner
// before reusing this workspace for the next column, so one fixed candidate
// set serves every stream without per-column allocation churn.
type compactStreamScratch struct {
	candidates      [8]compactStreamEncoding
	data            [8][]byte
	dict            [8][][]byte
	alphabet        [8][]byte
	integers        []int64
	dates           []int32
	parsed          []uint64
	dictionaryTable []uint32
	dictionaryStamp []uint32
	dictionaryEpoch uint32
	dictionarySeed  maphash.Seed
	dictionaryReady bool
}

func (e compactStreamEncoding) encodedBytes() int {
	n := compactStreamHeader + 2*len(e.dict) + len(e.data)
	for _, value := range e.dict {
		n += len(value)
	}
	return n
}

// appendBinary appends one self-delimiting stream. Dictionary ends are u32 so
// arbitrary canonical scalar spellings remain representable; compact codecs
// are selected only when the complete stream fits the enclosing page.
func (e compactStreamEncoding) appendBinary(dst []byte) ([]byte, error) {
	if e.kind >= compactStreamKindLimit || e.count < 0 || len(e.dict) > int(^uint16(0)) {
		return dst, fmt.Errorf("%w: compact stream encoding", ErrInvalidWrite)
	}
	if len(e.data) > int(^uint16(0)) {
		return dst, ErrCommonPrimaryLeafFull
	}
	start := len(dst)
	dst = append(dst, make([]byte, compactStreamHeader+2*len(e.dict))...)
	dst[start] = e.kind
	dst[start+1] = e.width
	binary.LittleEndian.PutUint16(dst[start+2:], uint16(len(e.dict)))
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(e.count))
	dictionaryBytes := 0
	for id, value := range e.dict {
		dictionaryBytes += len(value)
		if dictionaryBytes > int(^uint16(0)) {
			return dst[:start], ErrCommonPrimaryLeafFull
		}
		binary.LittleEndian.PutUint16(
			dst[start+compactStreamHeader+id*2:], uint16(dictionaryBytes),
		)
	}
	binary.LittleEndian.PutUint16(dst[start+8:], uint16(dictionaryBytes))
	binary.LittleEndian.PutUint16(dst[start+10:], uint16(len(e.data)))
	for _, value := range e.dict {
		dst = append(dst, value...)
	}
	dst = append(dst, e.data...)
	return dst, nil
}

func encodeCompactScalarStream(values [][]byte) compactStreamEncoding {
	var scratch compactStreamScratch
	return scratch.encode(values)
}

func (s *compactStreamScratch) encode(values [][]byte) compactStreamEncoding {
	if len(values) == 0 {
		return compactStreamEncoding{kind: compactStreamDictionary}
	}
	dictionaryBytes := s.measureDictionary(values)
	frontBytes := measureCompactFront(values)
	n := 3
	alphabetLimit := min(
		dictionaryBytes,
		frontBytes,
	)
	alphabet, hasAlphabet := s.measureAlphabet(2, values, alphabetLimit)
	s.integers = slices.Grow(s.integers[:0], len(values))[:len(values)]
	allIntegers := true
	for i := range values {
		s.integers[i], allIntegers = CanonicalIntValue(values[i])
		if !allIntegers {
			break
		}
	}
	if allIntegers {
		s.candidates[n] = s.encodeFOR(n, s.integers)
		n++
		s.candidates[n] = s.encodeDelta(n, s.integers)
		n++
		s.candidates[n] = s.encodeDeltaPack(n, s.integers)
		n++
	}
	s.dates = slices.Grow(s.dates[:0], len(values))[:len(values)]
	allDates := true
	for i := range values {
		s.dates[i], allDates = compactDateOrdinal(values[i])
		if !allDates {
			break
		}
	}
	if allDates {
		s.candidates[n] = s.encodeDate(n, s.dates)
		n++
	}
	if numeric, ok := s.encodePrefixInt(n, values); ok {
		s.candidates[n] = numeric
		n++
	}
	bestBytes := frontBytes
	bestSlot := -1
	if hasAlphabet && alphabet.encoded < bestBytes {
		bestBytes = alphabet.encoded
		bestSlot = 2
	}
	for slot := 3; slot < n; slot++ {
		candidate := s.candidates[slot]
		if candidate.encodedBytes() < bestBytes {
			bestBytes = candidate.encodedBytes()
			bestSlot = slot
		}
	}
	// Dictionary was historically the first candidate, so an exact-size tie
	// must still select it. Defer its sort and row-ID stream until that work can
	// affect the result; high-cardinality columns normally choose alphabet or
	// front coding and otherwise paid for a discarded O(rows log rows) build.
	// A small dictionary scans packed IDs much faster than spelling-oriented
	// codecs. Keep it when its exact size is within 25% of the byte minimum;
	// this is a representation choice, not a posting index or row-skipping path.
	preferDictionaryScan := len(s.dict[0]) <= compactDictionaryScanPreferred &&
		dictionaryBytes <= bestBytes+bestBytes/4
	if dictionaryBytes <= bestBytes || preferDictionaryScan {
		return s.finishDictionary(values)
	}
	if bestSlot < 0 {
		return s.encodeFront(1, values)
	}
	if bestSlot == 2 {
		return s.finishAlphabet(2, values, alphabet)
	}
	return s.candidates[bestSlot]
}

func (s *compactStreamScratch) measureDictionary(values [][]byte) int {
	dictionary := slices.Grow(s.dict[0][:0], len(values))[:0]
	s.dict[0] = dictionary
	dictionaryBytes := 0
	hashed := false
	for _, value := range values {
		if hashed {
			if _, found := s.dictionaryLookup(value); found {
				continue
			}
		} else {
			found := false
			for _, existing := range dictionary {
				if bytes.Equal(existing, value) {
					found = true
					break
				}
			}
			if found {
				continue
			}
		}
		dictionary = append(dictionary, value)
		s.dict[0] = dictionary
		dictionaryBytes += len(value)
		if hashed {
			s.dictionaryInsert(value, uint32(len(dictionary)-1))
		} else if len(dictionary) == compactDictionaryHashThreshold {
			s.resetDictionaryTable(len(values))
			for id, existing := range dictionary {
				s.dictionaryInsert(existing, uint32(id))
			}
			hashed = true
		}
	}
	width := bits.Len(uint(max(0, len(dictionary)-1)))
	return compactStreamHeader + 2*len(dictionary) + dictionaryBytes +
		(len(values)*width+7)/8
}

func (s *compactStreamScratch) finishDictionary(values [][]byte) compactStreamEncoding {
	dictionary := s.dict[0]
	slices.SortFunc(dictionary, bytes.Compare)
	width := bits.Len(uint(max(0, len(dictionary)-1)))
	dataBytes := (len(values)*width + 7) / 8
	data := slices.Grow(s.data[0][:0], dataBytes)[:dataBytes]
	clear(data)
	if len(dictionary) < compactDictionaryHashThreshold {
		for row, value := range values {
			id, found := slices.BinarySearchFunc(dictionary, value, bytes.Compare)
			if !found {
				panic("compact dictionary planner lost value")
			}
			compactPutBits(data, row*width, width, uint64(id))
		}
	} else {
		s.resetDictionaryTable(len(dictionary))
		for id, value := range dictionary {
			s.dictionaryInsert(value, uint32(id))
		}
		for row, value := range values {
			id, found := s.dictionaryLookup(value)
			if !found {
				panic("compact dictionary planner lost value")
			}
			compactPutBits(data, row*width, width, uint64(id))
		}
	}
	s.data[0] = data
	return compactStreamEncoding{
		kind: compactStreamDictionary, width: uint8(width), count: len(values),
		data: data, dict: dictionary,
	}
}

func (s *compactStreamScratch) resetDictionaryTable(entries int) {
	size := 2
	for size < entries*2 {
		size <<= 1
	}
	if len(s.dictionaryTable) < size {
		s.dictionaryTable = make([]uint32, size)
		s.dictionaryStamp = make([]uint32, size)
		s.dictionaryEpoch = 1
	} else {
		s.dictionaryEpoch++
		if s.dictionaryEpoch == 0 {
			clear(s.dictionaryStamp)
			s.dictionaryEpoch = 1
		}
	}
	if !s.dictionaryReady {
		s.dictionarySeed = maphash.MakeSeed()
		s.dictionaryReady = true
	}
}

func (s *compactStreamScratch) dictionaryLookup(value []byte) (uint32, bool) {
	mask := uint64(len(s.dictionaryTable) - 1)
	at := maphash.Bytes(s.dictionarySeed, value) & mask
	for s.dictionaryStamp[at] == s.dictionaryEpoch {
		id := s.dictionaryTable[at]
		if bytes.Equal(s.dict[0][id], value) {
			return id, true
		}
		at = (at + 1) & mask
	}
	return 0, false
}

func (s *compactStreamScratch) dictionaryInsert(value []byte, id uint32) {
	mask := uint64(len(s.dictionaryTable) - 1)
	at := maphash.Bytes(s.dictionarySeed, value) & mask
	for s.dictionaryStamp[at] == s.dictionaryEpoch {
		at = (at + 1) & mask
	}
	s.dictionaryTable[at] = id
	s.dictionaryStamp[at] = s.dictionaryEpoch
}

func (s *compactStreamScratch) encodeFront(slot int, values [][]byte) compactStreamEncoding {
	restarts := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	data := slices.Grow(s.data[slot][:0], 4*restarts)[:4*restarts]
	clear(data)
	var previous []byte
	for row, value := range values {
		if row%compactStreamRestart == 0 {
			binary.LittleEndian.PutUint32(data[(row/compactStreamRestart)*4:], uint32(len(data)))
			data = appendCompactUvarint(data, uint64(len(value)))
			data = append(data, value...)
		} else {
			prefix := compactCommonPrefix(previous, value)
			suffix := len(value) - prefix
			if prefix < 15 && suffix < 15 {
				data = append(data, byte(prefix<<4|suffix))
			} else {
				data = append(data, 0xff)
				data = appendCompactUvarint(data, uint64(prefix))
				data = appendCompactUvarint(data, uint64(suffix))
			}
			data = append(data, value[prefix:]...)
		}
		previous = value
	}
	s.data[slot] = data
	s.dict[slot] = s.dict[slot][:0]
	return compactStreamEncoding{kind: compactStreamFront, count: len(values), data: data}
}

func measureCompactFront(values [][]byte) int {
	dataBytes := 4 * ((len(values) + compactStreamRestart - 1) / compactStreamRestart)
	var previous []byte
	for row, value := range values {
		if row%compactStreamRestart == 0 {
			dataBytes += compactUvarintLen(uint64(len(value))) + len(value)
		} else {
			prefix := compactCommonPrefix(previous, value)
			suffix := len(value) - prefix
			dataBytes += 1 + suffix
			if prefix >= 15 || suffix >= 15 {
				dataBytes += compactUvarintLen(uint64(prefix)) +
					compactUvarintLen(uint64(suffix))
			}
		}
		previous = value
	}
	return compactStreamHeader + dataBytes
}

func compactUvarintLen(value uint64) int {
	n := 1
	for value >= 0x80 {
		value >>= 7
		n++
	}
	return n
}

func (s *compactStreamScratch) encodeAlphabet(
	slot int,
	values [][]byte,
	limit int,
) (compactStreamEncoding, bool) {
	plan, ok := s.measureAlphabet(slot, values, limit)
	if !ok {
		return compactStreamEncoding{}, false
	}
	return s.finishAlphabet(slot, values, plan), true
}

func (s *compactStreamScratch) measureAlphabet(
	slot int,
	values [][]byte,
	limit int,
) (compactAlphabetPlan, bool) {
	blocks := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	prefix, suffix := compactSharedAffixes(values)
	var present [256]bool
	count := 0
	for _, value := range values {
		middle := value[prefix : len(value)-suffix]
		for _, b := range middle {
			if !present[b] {
				present[b] = true
				count++
			}
		}
	}
	if count == 0 || count > 64 {
		return compactAlphabetPlan{}, false
	}
	width := bits.Len(uint(count - 1))
	totalBytes := 4 * blocks
	for first := 0; first < len(values); first += compactStreamRestart {
		last := min(first+compactStreamRestart, len(values))
		lo, hi, characters := 0, 0, 0
		for row, value := range values[first:last] {
			middle := len(value) - prefix - suffix
			if row == 0 {
				lo, hi = middle, middle
			} else {
				lo, hi = min(lo, middle), max(hi, middle)
			}
			characters += middle
		}
		lengthWidth := bits.Len(uint(hi - lo))
		totalBytes += compactUvarintLen(uint64(lo)) + 1 +
			((last-first)*lengthWidth+7)/8
		totalBytes += (characters*width + 7) / 8
	}
	dictionaryEntries, affixBytes := 1, 0
	if prefix != 0 || suffix != 0 {
		dictionaryEntries = 3
		affixBytes = prefix + suffix
	}
	encodedBytes := compactStreamHeader + 2*dictionaryEntries + count +
		affixBytes + totalBytes
	if limit > 0 && encodedBytes >= limit {
		return compactAlphabetPlan{}, false
	}
	alphabet := slices.Grow(s.alphabet[slot][:0], count)[:0]
	for b := range present {
		if present[b] {
			alphabet = append(alphabet, byte(b))
		}
	}
	s.alphabet[slot] = alphabet
	return compactAlphabetPlan{
		width: width, prefix: prefix, suffix: suffix,
		totalBytes: totalBytes, encoded: encodedBytes,
	}, true
}

func compactSharedAffixes(values [][]byte) (prefix, suffix int) {
	if len(values) < 2 {
		return 0, 0
	}
	prefix = len(values[0])
	for _, value := range values[1:] {
		prefix = min(prefix, compactCommonPrefix(values[0], value))
	}
	suffix = len(values[0]) - prefix
	for _, value := range values[1:] {
		limit := min(len(values[0])-prefix, len(value)-prefix)
		shared := 0
		for shared < limit &&
			values[0][len(values[0])-1-shared] == value[len(value)-1-shared] {
			shared++
		}
		suffix = min(suffix, shared)
	}
	return prefix, suffix
}

func (s *compactStreamScratch) finishAlphabet(
	slot int,
	values [][]byte,
	plan compactAlphabetPlan,
) compactStreamEncoding {
	alphabet := s.alphabet[slot]
	var code [256]uint8
	for id, b := range alphabet {
		code[b] = uint8(id)
	}
	blocks := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	width, totalBytes := plan.width, plan.totalBytes
	data := slices.Grow(s.data[slot][:0], totalBytes)[:4*blocks]
	clear(data)
	for first := 0; first < len(values); first += compactStreamRestart {
		last := min(first+compactStreamRestart, len(values))
		block := first / compactStreamRestart
		binary.LittleEndian.PutUint32(data[block*4:], uint32(len(data)))
		lo, hi, characters := 0, 0, 0
		for row, value := range values[first:last] {
			middle := len(value) - plan.prefix - plan.suffix
			if row == 0 {
				lo, hi = middle, middle
			} else {
				lo, hi = min(lo, middle), max(hi, middle)
			}
			characters += middle
		}
		lengthWidth := bits.Len(uint(hi - lo))
		data = appendCompactUvarint(data, uint64(lo))
		data = append(data, byte(lengthWidth))
		lengthStart := len(data)
		data = append(data, make([]byte, ((last-first)*lengthWidth+7)/8)...)
		for row, value := range values[first:last] {
			middle := len(value) - plan.prefix - plan.suffix
			compactPutBits(
				data[lengthStart:], row*lengthWidth, lengthWidth,
				uint64(middle-lo),
			)
		}
		start := len(data)
		data = append(data, make([]byte, (characters*width+7)/8)...)
		bit := 0
		for _, value := range values[first:last] {
			middle := value[plan.prefix : len(value)-plan.suffix]
			for _, b := range middle {
				compactPutBits(data[start:], bit, width, uint64(code[b]))
				bit += width
			}
		}
	}
	if len(data) != totalBytes {
		panic("compact alphabet sizing drift")
	}
	dictionaryEntries := 1
	if plan.prefix != 0 || plan.suffix != 0 {
		dictionaryEntries = 3
	}
	dictionary := slices.Grow(s.dict[slot][:0], dictionaryEntries)[:dictionaryEntries]
	dictionary[0] = alphabet
	if dictionaryEntries == 3 {
		dictionary[1] = values[0][:plan.prefix]
		dictionary[2] = values[0][len(values[0])-plan.suffix:]
	}
	s.data[slot], s.dict[slot] = data, dictionary
	return compactStreamEncoding{
		kind: compactStreamAlphabet, width: uint8(width), count: len(values),
		data: data, dict: dictionary,
	}
}

func (s *compactStreamScratch) encodeFOR(slot int, values []int64) compactStreamEncoding {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len64(uint64(hi - lo))
	dataBytes := 8 + (len(values)*width+7)/8
	data := slices.Grow(s.data[slot][:0], dataBytes)[:dataBytes]
	clear(data)
	binary.LittleEndian.PutUint64(data, uint64(lo))
	for row, value := range values {
		compactPutBits(data[8:], row*width, width, uint64(value-lo))
	}
	s.data[slot] = data
	s.dict[slot] = s.dict[slot][:0]
	return compactStreamEncoding{
		kind: compactStreamFOR, width: uint8(width), count: len(values), data: data,
	}
}

func (s *compactStreamScratch) encodeDelta(slot int, values []int64) compactStreamEncoding {
	restarts := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	data := slices.Grow(s.data[slot][:0], 4*restarts)[:4*restarts]
	clear(data)
	var previous int64
	for row, value := range values {
		if row%compactStreamRestart == 0 {
			binary.LittleEndian.PutUint32(data[(row/compactStreamRestart)*4:], uint32(len(data)))
			var fixed [8]byte
			binary.LittleEndian.PutUint64(fixed[:], uint64(value))
			data = append(data, fixed[:]...)
		} else {
			delta := value - previous
			data = appendCompactUvarint(data, uint64(delta<<1)^uint64(delta>>63))
		}
		previous = value
	}
	s.data[slot] = data
	s.dict[slot] = s.dict[slot][:0]
	return compactStreamEncoding{kind: compactStreamDelta, count: len(values), data: data}
}

func (s *compactStreamScratch) encodeDeltaPack(slot int, values []int64) compactStreamEncoding {
	blocks := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	totalBytes := 4 * blocks
	for first := 0; first < len(values); first += compactStreamRestart {
		last := min(first+compactStreamRestart, len(values))
		width := 0
		previous := values[first]
		for row := first + 1; row < last; row++ {
			delta := values[row] - previous
			zigzag := uint64(delta<<1) ^ uint64(delta>>63)
			width = max(width, bits.Len64(zigzag))
			previous = values[row]
		}
		totalBytes += 9 + ((last-first-1)*width+7)/8
	}
	data := slices.Grow(s.data[slot][:0], totalBytes)[:totalBytes]
	clear(data)
	cursor := 4 * blocks
	for first := 0; first < len(values); first += compactStreamRestart {
		last := min(first+compactStreamRestart, len(values))
		width := 0
		previous := values[first]
		for row := first + 1; row < last; row++ {
			delta := values[row] - previous
			zigzag := uint64(delta<<1) ^ uint64(delta>>63)
			width = max(width, bits.Len64(zigzag))
			previous = values[row]
		}
		block := first / compactStreamRestart
		binary.LittleEndian.PutUint32(data[block*4:], uint32(cursor))
		blockStart := cursor
		packedBytes := ((last-first-1)*width + 7) / 8
		cursor += 9 + packedBytes
		binary.LittleEndian.PutUint64(data[blockStart:], uint64(values[first]))
		data[blockStart+8] = byte(width)
		previous = values[first]
		for row := first + 1; row < last; row++ {
			delta := values[row] - previous
			zigzag := uint64(delta<<1) ^ uint64(delta>>63)
			compactPutBits(data[blockStart+9:], (row-first-1)*width, width, zigzag)
			previous = values[row]
		}
	}
	if cursor != len(data) {
		panic("compact packed delta sizing drift")
	}
	s.data[slot] = data
	s.dict[slot] = s.dict[slot][:0]
	return compactStreamEncoding{kind: compactStreamDeltaPack, count: len(values), data: data}
}

func (s *compactStreamScratch) encodeDate(slot int, values []int32) compactStreamEncoding {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len32(uint32(hi - lo))
	dataBytes := 4 + (len(values)*width+7)/8
	data := slices.Grow(s.data[slot][:0], dataBytes)[:dataBytes]
	clear(data)
	binary.LittleEndian.PutUint32(data, uint32(lo))
	for row, value := range values {
		compactPutBits(data[4:], row*width, width, uint64(value-lo))
	}
	s.data[slot] = data
	s.dict[slot] = s.dict[slot][:0]
	return compactStreamEncoding{
		kind: compactStreamDate, width: uint8(width), count: len(values), data: data,
	}
}

func encodeCompactDictionary(values [][]byte) compactStreamEncoding {
	unique := make(map[string]struct{}, min(len(values), 256))
	for _, value := range values {
		unique[string(value)] = struct{}{}
	}
	dictionary := make([][]byte, 0, len(unique))
	for value := range unique {
		dictionary = append(dictionary, []byte(value))
	}
	slices.SortFunc(dictionary, bytes.Compare)
	width := bits.Len(uint(max(0, len(dictionary)-1)))
	data := make([]byte, (len(values)*width+7)/8)
	ids := make(map[string]int, len(dictionary))
	for id := range dictionary {
		ids[string(dictionary[id])] = id
	}
	for row, value := range values {
		compactPutBits(data, row*width, width, uint64(ids[string(value)]))
	}
	return compactStreamEncoding{
		kind: compactStreamDictionary, width: uint8(width), count: len(values),
		data: data, dict: dictionary,
	}
}

func encodeCompactFront(values [][]byte) compactStreamEncoding {
	restarts := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	data := make([]byte, 4*restarts)
	var previous []byte
	for row, value := range values {
		if row%compactStreamRestart == 0 {
			binary.LittleEndian.PutUint32(data[(row/compactStreamRestart)*4:], uint32(len(data)))
			data = appendCompactUvarint(data, uint64(len(value)))
			data = append(data, value...)
		} else {
			prefix := compactCommonPrefix(previous, value)
			suffix := len(value) - prefix
			if prefix < 15 && suffix < 15 {
				data = append(data, byte(prefix<<4|suffix))
			} else {
				data = append(data, 0xff)
				data = appendCompactUvarint(data, uint64(prefix))
				data = appendCompactUvarint(data, uint64(suffix))
			}
			data = append(data, value[prefix:]...)
		}
		previous = value
	}
	return compactStreamEncoding{kind: compactStreamFront, count: len(values), data: data}
}

func encodeCompactFOR(values []int64) compactStreamEncoding {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len64(uint64(hi - lo))
	data := make([]byte, 8+(len(values)*width+7)/8)
	binary.LittleEndian.PutUint64(data, uint64(lo))
	for row, value := range values {
		compactPutBits(data[8:], row*width, width, uint64(value-lo))
	}
	return compactStreamEncoding{
		kind: compactStreamFOR, width: uint8(width), count: len(values), data: data,
	}
}

func encodeCompactDelta(values []int64) compactStreamEncoding {
	restarts := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	data := make([]byte, 4*restarts)
	var previous int64
	for row, value := range values {
		if row%compactStreamRestart == 0 {
			binary.LittleEndian.PutUint32(data[(row/compactStreamRestart)*4:], uint32(len(data)))
			var fixed [8]byte
			binary.LittleEndian.PutUint64(fixed[:], uint64(value))
			data = append(data, fixed[:]...)
		} else {
			delta := value - previous
			data = appendCompactUvarint(data, uint64(delta<<1)^uint64(delta>>63))
		}
		previous = value
	}
	return compactStreamEncoding{kind: compactStreamDelta, count: len(values), data: data}
}

func encodeCompactDate(values []int32) compactStreamEncoding {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len32(uint32(hi - lo))
	data := make([]byte, 4+(len(values)*width+7)/8)
	binary.LittleEndian.PutUint32(data, uint32(lo))
	for row, value := range values {
		compactPutBits(data[4:], row*width, width, uint64(value-lo))
	}
	return compactStreamEncoding{
		kind: compactStreamDate, width: uint8(width), count: len(values), data: data,
	}
}

type compactPrefixIntValue struct {
	prefix, suffix []byte
	value          uint64
	width          int
	canonical      bool
}

func parseCompactPrefixInt(src []byte) (compactPrefixIntValue, bool) {
	start := 0
	for start < len(src) && (src[start] < '0' || src[start] > '9') {
		start++
	}
	if start == len(src) {
		return compactPrefixIntValue{}, false
	}
	end := start
	for end < len(src) && src[end] >= '0' && src[end] <= '9' {
		end++
	}
	for at := end; at < len(src); at++ {
		if src[at] >= '0' && src[at] <= '9' {
			return compactPrefixIntValue{}, false
		}
	}
	var value uint64
	for _, digit := range src[start:end] {
		next := value*10 + uint64(digit-'0')
		if next < value || next > uint64(^uint64(0)>>1) {
			return compactPrefixIntValue{}, false
		}
		value = next
	}
	width := end - start
	return compactPrefixIntValue{
		prefix: src[:start], suffix: src[end:], value: value, width: width,
		canonical: width == 1 || src[start] != '0',
	}, true
}

func encodeCompactPrefixInt(values [][]byte) (compactStreamEncoding, bool) {
	var scratch compactStreamScratch
	return scratch.encodePrefixInt(0, values)
}

func (s *compactStreamScratch) encodePrefixInt(
	slot int,
	values [][]byte,
) (compactStreamEncoding, bool) {
	first, ok := parseCompactPrefixInt(values[0])
	if !ok {
		return compactStreamEncoding{}, false
	}
	s.parsed = slices.Grow(s.parsed[:0], len(values))[:len(values)]
	parsed := s.parsed
	parsed[0] = first.value
	allCanonical, fixedWidth := first.canonical, true
	for row := 1; row < len(values); row++ {
		value, ok := parseCompactPrefixInt(values[row])
		if !ok || !bytes.Equal(value.prefix, first.prefix) ||
			!bytes.Equal(value.suffix, first.suffix) {
			return compactStreamEncoding{}, false
		}
		parsed[row] = value.value
		allCanonical = allCanonical && value.canonical
		fixedWidth = fixedWidth && value.width == first.width
	}
	if !allCanonical && !fixedWidth {
		return compactStreamEncoding{}, false
	}
	linear := true
	delta := int64(0)
	if len(parsed) > 1 {
		delta = int64(parsed[1] - parsed[0])
		for row := 2; row < len(parsed); row++ {
			if int64(parsed[row]-parsed[row-1]) != delta {
				linear = false
				break
			}
		}
	}
	if linear {
		data := slices.Grow(s.data[slot][:0], 18)[:18]
		clear(data)
		if fixedWidth {
			if first.width > int(^uint8(0)) {
				return compactStreamEncoding{}, false
			}
			data[0], data[1] = 1, byte(first.width)
		}
		data[0] |= 2
		binary.LittleEndian.PutUint64(data[2:], parsed[0])
		binary.LittleEndian.PutUint64(data[10:], uint64(delta))
		dictionary := slices.Grow(s.dict[slot][:0], 2)[:2]
		dictionary[0], dictionary[1] = first.prefix, first.suffix
		s.data[slot], s.dict[slot] = data, dictionary
		return compactStreamEncoding{
			kind: compactStreamPrefixInt, count: len(values), data: data,
			dict: dictionary,
		}, true
	}
	restarts := (len(values) + compactStreamRestart - 1) / compactStreamRestart
	if fixedWidth {
		if first.width > int(^uint8(0)) {
			return compactStreamEncoding{}, false
		}
	}
	headerBytes := 2 + 4*restarts
	varintBytes, packedBytes := headerBytes, headerBytes
	for blockFirst := 0; blockFirst < len(parsed); blockFirst += compactStreamRestart {
		last := min(blockFirst+compactStreamRestart, len(parsed))
		width := 0
		previous := parsed[blockFirst]
		varintBytes += 8
		for row := blockFirst + 1; row < last; row++ {
			delta := int64(parsed[row] - previous)
			zigzag := uint64(delta<<1) ^ uint64(delta>>63)
			varintBytes += (bits.Len64(zigzag|1) + 6) / 7
			width = max(width, bits.Len64(zigzag))
			previous = parsed[row]
		}
		packedBytes += 9 + ((last-blockFirst-1)*width+7)/8
	}
	var data []byte
	if packedBytes < varintBytes {
		data = slices.Grow(s.data[slot][:0], packedBytes)[:packedBytes]
		clear(data)
		if fixedWidth {
			data[0], data[1] = 1, byte(first.width)
		}
		data[0] |= 4
		cursor := headerBytes
		for blockFirst := 0; blockFirst < len(parsed); blockFirst += compactStreamRestart {
			last := min(blockFirst+compactStreamRestart, len(parsed))
			width := 0
			previous := parsed[blockFirst]
			for row := blockFirst + 1; row < last; row++ {
				delta := int64(parsed[row] - previous)
				zigzag := uint64(delta<<1) ^ uint64(delta>>63)
				width = max(width, bits.Len64(zigzag))
				previous = parsed[row]
			}
			block := blockFirst / compactStreamRestart
			binary.LittleEndian.PutUint32(data[2+block*4:], uint32(cursor))
			blockStart := cursor
			cursor += 9 + ((last-blockFirst-1)*width+7)/8
			binary.LittleEndian.PutUint64(data[blockStart:], parsed[blockFirst])
			data[blockStart+8] = byte(width)
			previous = parsed[blockFirst]
			for row := blockFirst + 1; row < last; row++ {
				delta := int64(parsed[row] - previous)
				zigzag := uint64(delta<<1) ^ uint64(delta>>63)
				compactPutBits(data[blockStart+9:], (row-blockFirst-1)*width, width, zigzag)
				previous = parsed[row]
			}
		}
	} else {
		data = slices.Grow(s.data[slot][:0], varintBytes)[:varintBytes]
		clear(data[:headerBytes])
		if fixedWidth {
			data[0], data[1] = 1, byte(first.width)
		}
		cursor := headerBytes
		for blockFirst := 0; blockFirst < len(parsed); blockFirst += compactStreamRestart {
			last := min(blockFirst+compactStreamRestart, len(parsed))
			block := blockFirst / compactStreamRestart
			binary.LittleEndian.PutUint32(data[2+block*4:], uint32(cursor))
			previous := parsed[blockFirst]
			binary.LittleEndian.PutUint64(data[cursor:], previous)
			cursor += 8
			for row := blockFirst + 1; row < last; row++ {
				delta := int64(parsed[row] - previous)
				zigzag := uint64(delta<<1) ^ uint64(delta>>63)
				for zigzag >= 0x80 {
					data[cursor] = byte(zigzag) | 0x80
					cursor++
					zigzag >>= 7
				}
				data[cursor] = byte(zigzag)
				cursor++
				previous = parsed[row]
			}
		}
	}
	dictionary := slices.Grow(s.dict[slot][:0], 2)[:2]
	dictionary[0], dictionary[1] = first.prefix, first.suffix
	s.data[slot], s.dict[slot] = data, dictionary
	return compactStreamEncoding{
		kind: compactStreamPrefixInt, count: len(values), data: data,
		dict: dictionary,
	}, true
}

func compactCommonPrefix(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 || a[0] != b[0] {
		return 0
	}
	return compactCommonPrefixEqualHead(a, b)
}

// Keep the immediate mismatch case in the small, inlinable caller.
func compactCommonPrefixEqualHead(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	// Canonical scalar strings often share long payloads. Compare whole words
	// without reading beyond either slice; little-endian bit order identifies
	// the first different byte on every architecture, including unaligned data.
	for n-i >= 32 {
		x0 := binary.LittleEndian.Uint64(a[i:]) ^ binary.LittleEndian.Uint64(b[i:])
		x1 := binary.LittleEndian.Uint64(a[i+8:]) ^ binary.LittleEndian.Uint64(b[i+8:])
		x2 := binary.LittleEndian.Uint64(a[i+16:]) ^ binary.LittleEndian.Uint64(b[i+16:])
		x3 := binary.LittleEndian.Uint64(a[i+24:]) ^ binary.LittleEndian.Uint64(b[i+24:])
		if x0|x1|x2|x3 != 0 {
			switch {
			case x0 != 0:
				return i + bits.TrailingZeros64(x0)/8
			case x1 != 0:
				return i + 8 + bits.TrailingZeros64(x1)/8
			case x2 != 0:
				return i + 16 + bits.TrailingZeros64(x2)/8
			default:
				return i + 24 + bits.TrailingZeros64(x3)/8
			}
		}
		i += 32
	}
	for n-i >= 8 {
		different := binary.LittleEndian.Uint64(a[i:]) ^ binary.LittleEndian.Uint64(b[i:])
		if different != 0 {
			return i + bits.TrailingZeros64(different)/8
		}
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func appendCompactUvarint(dst []byte, value uint64) []byte {
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func readCompactUvarint(src []byte) (uint64, int, bool) {
	var value uint64
	for i := 0; i < min(len(src), 10); i++ {
		b := src[i]
		if i == 9 && b > 1 {
			return 0, 0, false
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, i + 1, true
		}
	}
	return 0, 0, false
}

func compactPutBits(dst []byte, bit, width int, value uint64) {
	for width > 0 {
		byteIndex := bit >> 3
		shift := bit & 7
		take := min(width, 8-shift)
		mask := uint64(1<<take) - 1
		dst[byteIndex] |= byte(value&mask) << shift
		value >>= take
		bit += take
		width -= take
	}
}

func compactReadBits(src []byte, bit, width int) uint64 {
	var value uint64
	written := 0
	for width > 0 {
		byteIndex := bit >> 3
		shift := bit & 7
		take := min(width, 8-shift)
		mask := byte(1<<take) - 1
		value |= uint64((src[byteIndex]>>shift)&mask) << written
		written += take
		bit += take
		width -= take
	}
	return value
}

func compactDateOrdinal(value []byte) (int32, bool) {
	if len(value) != 12 || value[0] != '"' || value[11] != '"' ||
		value[5] != '-' || value[8] != '-' {
		return 0, false
	}
	digit := func(at int) (int, bool) {
		if value[at] < '0' || value[at] > '9' {
			return 0, false
		}
		return int(value[at] - '0'), true
	}
	read2 := func(at int) (int, bool) {
		a, ok := digit(at)
		if !ok {
			return 0, false
		}
		b, ok := digit(at + 1)
		return a*10 + b, ok
	}
	y0, ok0 := read2(1)
	y1, ok1 := read2(3)
	month, ok2 := read2(6)
	day, ok3 := read2(9)
	if !ok0 || !ok1 || !ok2 || !ok3 {
		return 0, false
	}
	year := y0*100 + y1
	if month < 1 || month > 12 {
		return 0, false
	}
	monthDays := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	leap := year%4 == 0 && (year%100 != 0 || year%400 == 0)
	limit := monthDays[month-1]
	if month == 2 && leap {
		limit++
	}
	if day < 1 || day > limit {
		return 0, false
	}
	days := compactDaysBeforeYear(year)
	for m := 1; m < month; m++ {
		days += monthDays[m-1]
		if m == 2 && leap {
			days++
		}
	}
	return int32(days + day - 1), true
}

func compactDaysBeforeYear(year int) int {
	return year*365 + (year+3)/4 - (year+99)/100 + (year+399)/400
}

func appendCompactDateArithmetic(dst []byte, ordinal int32) []byte {
	// Shift the ordinal's 0000-01-01 origin to a March-based 400-year era.
	// March makes leap day the final day of a year, allowing direct division
	// instead of a year search and month loop. The negative-era branch covers
	// only 0000-01-01 through 0000-02-29 under Go's truncating division.
	z := int64(ordinal) - 60
	era := z / 146097
	if z < 0 {
		era = (z - 146096) / 146097
	}
	dayOfEra := z - era*146097
	yearOfEra := (dayOfEra - dayOfEra/1460 + dayOfEra/36524 - dayOfEra/146096) / 365
	year := yearOfEra + era*400
	dayOfYear := dayOfEra - (365*yearOfEra + yearOfEra/4 - yearOfEra/100)
	monthPrime := (5*dayOfYear + 2) / 153
	day := dayOfYear - (153*monthPrime+2)/5 + 1
	month := monthPrime + 3
	if monthPrime >= 10 {
		month -= 12
	}
	if month <= 2 {
		year++
	}
	return append(dst,
		'"', byte('0'+year/1000), byte('0'+year/100%10),
		byte('0'+year/10%10), byte('0'+year%10), '-',
		byte('0'+month/10), byte('0'+month%10), '-',
		byte('0'+day/10), byte('0'+day%10), '"',
	)
}

type compactStreamView struct {
	kind      uint8
	width     uint8
	count     int
	dictCount int
	dictDir   []byte
	dictData  []byte
	data      []byte
	encoded   int
}

func openCompactStream(src []byte) (compactStreamView, error) {
	corrupt := func(what string) (compactStreamView, error) {
		return compactStreamView{}, fmt.Errorf("%w: compact stream %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	if len(src) < compactStreamHeader {
		return corrupt("header")
	}
	kind := src[0]
	width := src[1]
	if kind >= compactStreamKindLimit {
		return corrupt("kind")
	}
	count := int(binary.LittleEndian.Uint32(src[4:]))
	dictCount := int(binary.LittleEndian.Uint16(src[2:]))
	dictBytes := int(binary.LittleEndian.Uint16(src[8:]))
	dataBytes := int(binary.LittleEndian.Uint16(src[10:]))
	dirBytes := 2 * dictCount
	total64 := uint64(compactStreamHeader) + uint64(dirBytes) +
		uint64(dictBytes) + uint64(dataBytes)
	if total64 > uint64(len(src)) {
		return corrupt("section bounds")
	}
	total := int(total64)
	v := compactStreamView{
		kind: kind, width: width, count: count, dictCount: dictCount,
		dictDir:  src[compactStreamHeader : compactStreamHeader+dirBytes],
		dictData: src[compactStreamHeader+dirBytes : compactStreamHeader+dirBytes+dictBytes],
		data:     src[compactStreamHeader+dirBytes+dictBytes : total], encoded: total,
	}
	previous := uint32(0)
	for id := 0; id < dictCount; id++ {
		end := uint32(binary.LittleEndian.Uint16(v.dictDir[id*2:]))
		if end < previous || uint64(end) > uint64(len(v.dictData)) {
			return corrupt("dictionary directory")
		}
		previous = end
	}
	if dictCount == 0 && len(v.dictData) != 0 ||
		dictCount != 0 && int(previous) != len(v.dictData) {
		return corrupt("dictionary length")
	}
	if err := v.validate(); err != nil {
		return compactStreamView{}, err
	}
	return v, nil
}

// admittedCompactStream reconstructs a stream whose complete grammar was
// already proven by CompactPrimaryStripe admission. It repeats only bounded
// section arithmetic so point reads and scans do not re-walk the stream.
func admittedCompactStream(src []byte) (compactStreamView, bool) {
	if len(src) < compactStreamHeader {
		return compactStreamView{}, false
	}
	dictCount := int(binary.LittleEndian.Uint16(src[2:]))
	dictBytes := int(binary.LittleEndian.Uint16(src[8:]))
	dataBytes := int(binary.LittleEndian.Uint16(src[10:]))
	dirBytes := 2 * dictCount
	total64 := uint64(compactStreamHeader) + uint64(dirBytes) +
		uint64(dictBytes) + uint64(dataBytes)
	if total64 > uint64(len(src)) {
		return compactStreamView{}, false
	}
	total := int(total64)
	return compactStreamView{
		kind: src[0], width: src[1],
		count: int(binary.LittleEndian.Uint32(src[4:])), dictCount: dictCount,
		dictDir:  src[compactStreamHeader : compactStreamHeader+dirBytes],
		dictData: src[compactStreamHeader+dirBytes : compactStreamHeader+dirBytes+dictBytes],
		data:     src[compactStreamHeader+dirBytes+dictBytes : total], encoded: total,
	}, true
}

func (v *compactStreamView) dictionaryEntry(id int) ([]byte, bool) {
	if id < 0 || id >= v.dictCount {
		return nil, false
	}
	start := uint32(0)
	if id != 0 {
		start = uint32(binary.LittleEndian.Uint16(v.dictDir[(id-1)*2:]))
	}
	end := uint32(binary.LittleEndian.Uint16(v.dictDir[id*2:]))
	if end < start || uint64(end) > uint64(len(v.dictData)) {
		return nil, false
	}
	return v.dictData[start:end:end], true
}

func (v compactStreamView) validate() error {
	corrupt := func(what string) error {
		return fmt.Errorf("%w: compact stream %s", ErrCommonPrimaryLeafCorrupt, what)
	}
	bitsBytes := func(prefix, width int) (int, bool) {
		if width < 0 || width > 64 {
			return 0, false
		}
		n := uint64(prefix) + (uint64(v.count)*uint64(width)+7)/8
		return int(n), n <= uint64(^uint(0)>>1)
	}
	switch v.kind {
	case compactStreamDictionary:
		if v.dictCount == 0 && v.count != 0 ||
			v.width != uint8(bits.Len(uint(max(0, v.dictCount-1)))) {
			return corrupt("dictionary geometry")
		}
		need, ok := bitsBytes(0, int(v.width))
		if !ok || need != len(v.data) {
			return corrupt("dictionary ids")
		}
		for row := 0; row < v.count; row++ {
			if compactReadBits(v.data, row*int(v.width), int(v.width)) >= uint64(v.dictCount) {
				return corrupt("dictionary id")
			}
		}
	case compactStreamFront:
		if v.width != 0 || v.dictCount != 0 {
			return corrupt("front metadata")
		}
		restarts := (v.count + compactStreamRestart - 1) / compactStreamRestart
		cursor := 4 * restarts
		if cursor > len(v.data) {
			return corrupt("front restart table")
		}
		previousLength := 0
		for row := 0; row < v.count; row++ {
			if row%compactStreamRestart == 0 {
				if int(binary.LittleEndian.Uint32(v.data[(row/compactStreamRestart)*4:])) != cursor {
					return corrupt("front restart offset")
				}
				length, n, ok := readCompactUvarint(v.data[cursor:])
				if !ok || length > uint64(len(v.data)-cursor-n) {
					return corrupt("front restart value")
				}
				cursor += n + int(length)
				previousLength = int(length)
				continue
			}
			if cursor >= len(v.data) {
				return corrupt("front tuple")
			}
			packed := v.data[cursor]
			cursor++
			prefix, suffix := int(packed>>4), int(packed&15)
			if packed == 0xff {
				p, n, ok := readCompactUvarint(v.data[cursor:])
				if !ok {
					return corrupt("front prefix")
				}
				cursor += n
				s, n, ok := readCompactUvarint(v.data[cursor:])
				if !ok {
					return corrupt("front suffix")
				}
				cursor += n
				if p > uint64(^uint(0)>>1) || s > uint64(^uint(0)>>1) {
					return corrupt("front tuple width")
				}
				prefix, suffix = int(p), int(s)
			}
			if prefix > previousLength || suffix > len(v.data)-cursor {
				return corrupt("front value bounds")
			}
			cursor += suffix
			previousLength = prefix + suffix
		}
		if cursor != len(v.data) {
			return corrupt("front trailing bytes")
		}
	case compactStreamFOR:
		if v.dictCount != 0 {
			return corrupt("FOR dictionary")
		}
		need, ok := bitsBytes(8, int(v.width))
		if !ok || need != len(v.data) {
			return corrupt("FOR data")
		}
	case compactStreamDelta:
		if v.width != 0 || v.dictCount != 0 || !validateCompactRestartVarints(v.data, v.count, 0) {
			return corrupt("delta data")
		}
	case compactStreamDeltaPack:
		if v.width != 0 || v.dictCount != 0 || !validateCompactPackedDeltas(v.data, v.count, 0) {
			return corrupt("packed delta data")
		}
	case compactStreamDate:
		if v.dictCount != 0 || v.width > 32 {
			return corrupt("date metadata")
		}
		need, ok := bitsBytes(4, int(v.width))
		if !ok || need != len(v.data) {
			return corrupt("date data")
		}
		if v.count != 0 {
			base := uint64(binary.LittleEndian.Uint32(v.data))
			maxDelta := uint64(0)
			if v.width != 0 {
				maxDelta = uint64(1)<<v.width - 1
			}
			if base+maxDelta >= uint64(compactDaysBeforeYear(10_000)) {
				return corrupt("date range")
			}
		}
	case compactStreamPrefixInt:
		if v.width != 0 || v.dictCount != 2 || len(v.data) < 2 ||
			v.data[0] > 7 || v.data[0]&6 == 6 ||
			v.data[0]&1 == 0 && v.data[1] != 0 ||
			v.data[0]&1 != 0 && v.data[1] == 0 {
			return corrupt("prefix-integer data")
		}
		if v.data[0]&2 != 0 {
			if len(v.data) != 18 {
				return corrupt("linear prefix-integer data")
			}
		} else if v.data[0]&4 != 0 {
			if !validateCompactPackedDeltas(v.data, v.count, 2) {
				return corrupt("packed prefix-integer data")
			}
		} else if !validateCompactRestartVarints(v.data, v.count, 2) {
			return corrupt("prefix-integer restart data")
		}
	case compactStreamAlphabet:
		alphabet, _, _, ok := v.alphabetParts()
		if !ok || len(alphabet) == 0 || len(alphabet) > 64 ||
			v.width != uint8(bits.Len(uint(len(alphabet)-1))) {
			return corrupt("alphabet metadata")
		}
		for at := 1; at < len(alphabet); at++ {
			if alphabet[at-1] >= alphabet[at] {
				return corrupt("alphabet order")
			}
		}
		if !validateCompactAlphabet(v.data, v.count, int(v.width), len(alphabet)) {
			return corrupt("alphabet data")
		}
	default:
		return corrupt("kind")
	}
	return nil
}

func (v compactStreamView) alphabetParts() (
	alphabet, prefix, suffix []byte,
	ok bool,
) {
	if v.kind != compactStreamAlphabet || v.dictCount != 1 && v.dictCount != 3 {
		return nil, nil, nil, false
	}
	alphabetEnd := int(binary.LittleEndian.Uint16(v.dictDir))
	alphabet = v.dictData[:alphabetEnd:alphabetEnd]
	if v.dictCount == 3 {
		prefixEnd := int(binary.LittleEndian.Uint16(v.dictDir[2:]))
		prefix = v.dictData[alphabetEnd:prefixEnd:prefixEnd]
		suffix = v.dictData[prefixEnd:len(v.dictData):len(v.dictData)]
	}
	return alphabet, prefix, suffix, true
}

func (v compactStreamView) alphabetBlock(
	block int,
) (base uint64, lengths []byte, packedBit, rows, width int) {
	cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
	base, n, _ := readCompactUvarint(v.data[cursor:])
	cursor += n
	width = int(v.data[cursor])
	cursor++
	rows = min(compactStreamRestart, v.count-block*compactStreamRestart)
	lengthBytes := (rows*width + 7) / 8
	lengths = v.data[cursor : cursor+lengthBytes]
	packedBit = (cursor + lengthBytes) * 8
	return base, lengths, packedBit, rows, width
}

func validateCompactAlphabet(data []byte, count, width, alphabet int) bool {
	blocks := (count + compactStreamRestart - 1) / compactStreamRestart
	cursor := 4 * blocks
	if cursor > len(data) {
		return false
	}
	for block := 0; block < blocks; block++ {
		if int(binary.LittleEndian.Uint32(data[block*4:])) != cursor {
			return false
		}
		base, n, ok := readCompactUvarint(data[cursor:])
		if !ok || base > CommonPrimaryLeafMaxExtentBytes {
			return false
		}
		cursor += n
		if cursor >= len(data) {
			return false
		}
		lengthWidth := int(data[cursor])
		cursor++
		rows := min(compactStreamRestart, count-block*compactStreamRestart)
		lengthBytes := (rows*lengthWidth + 7) / 8
		if lengthWidth > 64 || lengthBytes > len(data)-cursor {
			return false
		}
		lengths := data[cursor : cursor+lengthBytes]
		cursor += lengthBytes
		characters := 0
		for row := range rows {
			length := base + compactReadBits(lengths, row*lengthWidth, lengthWidth)
			if length > CommonPrimaryLeafMaxExtentBytes ||
				length > uint64(^uint(0)>>1)-uint64(characters) {
				return false
			}
			characters += int(length)
		}
		packed := (characters*width + 7) / 8
		if packed > len(data)-cursor {
			return false
		}
		for at := 0; at < characters; at++ {
			if compactReadBits(data[cursor:cursor+packed], at*width, width) >= uint64(alphabet) {
				return false
			}
		}
		cursor += packed
	}
	return cursor == len(data)
}

func validateCompactPackedDeltas(data []byte, count, prefix int) bool {
	blocks := (count + compactStreamRestart - 1) / compactStreamRestart
	cursor := prefix + 4*blocks
	if cursor > len(data) {
		return false
	}
	for block := 0; block < blocks; block++ {
		if int(binary.LittleEndian.Uint32(data[prefix+block*4:])) != cursor || len(data)-cursor < 9 {
			return false
		}
		width := int(data[cursor+8])
		if width > 64 {
			return false
		}
		rows := min(compactStreamRestart, count-block*compactStreamRestart)
		cursor += 9 + ((rows-1)*width+7)/8
		if cursor > len(data) {
			return false
		}
	}
	return cursor == len(data)
}

func validateCompactRestartVarints(data []byte, count, prefix int) bool {
	restarts := (count + compactStreamRestart - 1) / compactStreamRestart
	cursor := prefix + 4*restarts
	if cursor > len(data) {
		return false
	}
	for row := 0; row < count; row++ {
		if row%compactStreamRestart == 0 {
			if int(binary.LittleEndian.Uint32(data[prefix+(row/compactStreamRestart)*4:])) != cursor ||
				len(data)-cursor < 8 {
				return false
			}
			cursor += 8
			continue
		}
		_, n, ok := readCompactUvarint(data[cursor:])
		if !ok {
			return false
		}
		cursor += n
	}
	return cursor == len(data)
}

func (v compactStreamView) appendValue(dst []byte, row int) ([]byte, bool) {
	if row < 0 || row >= v.count {
		return dst, false
	}
	start := len(dst)
	switch v.kind {
	case compactStreamDictionary:
		id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
		value, ok := v.dictionaryEntry(id)
		if !ok {
			return dst, false
		}
		return append(dst, value...), true
	case compactStreamFront:
		block := row / compactStreamRestart
		cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
		length, n, _ := readCompactUvarint(v.data[cursor:])
		cursor += n
		dst = append(dst, v.data[cursor:cursor+int(length)]...)
		cursor += int(length)
		for at := block * compactStreamRestart; at < row; at++ {
			packed := v.data[cursor]
			cursor++
			prefix, suffix := int(packed>>4), int(packed&15)
			if packed == 0xff {
				p, n, _ := readCompactUvarint(v.data[cursor:])
				cursor += n
				s, n, _ := readCompactUvarint(v.data[cursor:])
				cursor += n
				prefix, suffix = int(p), int(s)
			}
			dst = dst[:start+prefix]
			dst = append(dst, v.data[cursor:cursor+suffix]...)
			cursor += suffix
		}
		return dst, true
	case compactStreamFOR:
		base := int64(binary.LittleEndian.Uint64(v.data))
		value := base + int64(compactReadBits(v.data[8:], row*int(v.width), int(v.width)))
		return AppendCanonicalInt(dst, value), true
	case compactStreamDelta:
		value := int64(0)
		if decoded, ok := v.restartInteger(row, 0); ok {
			value = decoded
		} else {
			return dst, false
		}
		return AppendCanonicalInt(dst, value), true
	case compactStreamDeltaPack:
		value, ok := v.packedDeltaInteger(row)
		if !ok {
			return dst, false
		}
		return AppendCanonicalInt(dst, value), true
	case compactStreamDate:
		base := int32(binary.LittleEndian.Uint32(v.data))
		value := base + int32(compactReadBits(v.data[4:], row*int(v.width), int(v.width)))
		return appendCompactDate(dst, value), true
	case compactStreamPrefixInt:
		value, ok := v.prefixInteger(row)
		if !ok || value < 0 {
			return dst, false
		}
		prefix, _ := v.dictionaryEntry(0)
		suffix, _ := v.dictionaryEntry(1)
		dst = append(dst, prefix...)
		digits := len(dst)
		dst = strconv.AppendUint(dst, uint64(value), 10)
		if v.data[0]&1 != 0 {
			width := int(v.data[1])
			n := len(dst) - digits
			if n > width {
				return dst[:start], false
			}
			if n < width {
				gap := width - n
				for range gap {
					dst = append(dst, 0)
				}
				copy(dst[digits+gap:], dst[digits:digits+n])
				for at := digits; at < digits+gap; at++ {
					dst[at] = '0'
				}
			}
		}
		return append(dst, suffix...), true
	case compactStreamAlphabet:
		return v.appendAlphabetValue(dst, row)
	}
	return dst, false
}

func (v compactStreamView) appendAlphabetValue(dst []byte, row int) ([]byte, bool) {
	if v.kind != compactStreamAlphabet || row < 0 || row >= v.count {
		return dst, false
	}
	if _, _, _, ok := v.alphabetParts(); !ok {
		return dst, false
	}
	// Prime only the lengths preceding this row, then share the bounded
	// word/SIMD renderer with scans. A failed seek leaves the sentinel intact;
	// never re-enter random decoding through the sequential fallback.
	state := compactStreamSequentialState{next: -1}
	state.seek(&v, row)
	if state.next != row {
		return dst, false
	}
	return state.appendAlphabet(dst, &v, row)
}

func (v compactStreamView) prefixInteger(row int) (int64, bool) {
	if v.kind != compactStreamPrefixInt || row < 0 || row >= v.count || len(v.data) < 2 {
		return 0, false
	}
	if v.data[0]&2 == 0 {
		if v.data[0]&4 != 0 {
			return v.packedDeltaIntegerAt(row, 2)
		}
		return v.restartInteger(row, 2)
	}
	if len(v.data) != 18 {
		return 0, false
	}
	first := int64(binary.LittleEndian.Uint64(v.data[2:]))
	delta := int64(binary.LittleEndian.Uint64(v.data[10:]))
	return first + int64(row)*delta, true
}

func (v compactStreamView) findPrefixInteger(needle []byte) (int, bool) {
	if v.kind != compactStreamPrefixInt || v.count == 0 {
		return 0, false
	}
	parsed, ok := parseCompactPrefixInt(needle)
	if !ok {
		return 0, false
	}
	prefix, _ := v.dictionaryEntry(0)
	suffix, _ := v.dictionaryEntry(1)
	if !bytes.Equal(parsed.prefix, prefix) || !bytes.Equal(parsed.suffix, suffix) ||
		v.data[0]&1 != 0 && parsed.width != int(v.data[1]) ||
		v.data[0]&1 == 0 && !parsed.canonical {
		return 0, false
	}
	if v.data[0]&2 != 0 {
		first := int64(binary.LittleEndian.Uint64(v.data[2:]))
		delta := int64(binary.LittleEndian.Uint64(v.data[10:]))
		value := int64(parsed.value)
		if delta == 0 {
			return 0, value == first
		}
		difference := value - first
		if difference%delta != 0 {
			return 0, false
		}
		row := difference / delta
		return int(row), row >= 0 && row < int64(v.count)
	}
	lo, hi := 0, v.count
	want := int64(parsed.value)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		value, ok := v.prefixInteger(mid)
		if !ok {
			return 0, false
		}
		if value < want {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == v.count {
		return 0, false
	}
	value, ok := v.prefixInteger(lo)
	return lo, ok && value == want
}

func (v compactStreamView) restartInteger(row, prefix int) (int64, bool) {
	block := row / compactStreamRestart
	cursor := int(binary.LittleEndian.Uint32(v.data[prefix+block*4:]))
	if len(v.data)-cursor < 8 {
		return 0, false
	}
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	cursor += 8
	for at := block * compactStreamRestart; at < row; at++ {
		u, n, ok := readCompactUvarint(v.data[cursor:])
		if !ok {
			return 0, false
		}
		cursor += n
		value += int64(u>>1) ^ -int64(u&1)
	}
	return value, true
}

func (v compactStreamView) packedDeltaInteger(row int) (int64, bool) {
	if v.kind != compactStreamDeltaPack || row < 0 || row >= v.count {
		return 0, false
	}
	return v.packedDeltaIntegerAt(row, 0)
}

func (v compactStreamView) packedDeltaIntegerAt(row, prefix int) (int64, bool) {
	if row < 0 || row >= v.count {
		return 0, false
	}
	block := row / compactStreamRestart
	cursor := int(binary.LittleEndian.Uint32(v.data[prefix+block*4:]))
	value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
	width := int(v.data[cursor+8])
	packed := v.data[cursor+9:]
	for at := block * compactStreamRestart; at < row; at++ {
		u := compactReadBits(packed, (at-block*compactStreamRestart)*width, width)
		value += int64(u>>1) ^ -int64(u&1)
	}
	return value, true
}

// countIntegerEqual performs a complete scan of an integer stream without
// formatting each value back into JSON. FOR compares the packed offsets and
// delta streams consume every restart and varint in row order.
func (v compactStreamView) countIntegerEqual(needle int64) (matched int, supported bool) {
	switch v.kind {
	case compactStreamFOR:
		base := int64(binary.LittleEndian.Uint64(v.data))
		delta := uint64(needle) - uint64(base)
		if v.width < 64 {
			limit := uint64(1) << v.width
			if needle < base || delta >= limit {
				// An impossible packed value preserves the full encoded-row scan
				// for an out-of-range predicate without a separate branch per row.
				delta = limit
			}
		} else if needle < base {
			// A 64-bit packed lane has no impossible sentinel. Keep this rare
			// boundary on the general path rather than claiming a scan we did
			// not perform.
			return 0, false
		}
		return countCompactPackedEqual(
			v.data[8:], v.count, int(v.width), delta,
		), true
	case compactStreamDelta:
		restarts := (v.count + compactStreamRestart - 1) / compactStreamRestart
		cursor := 4 * restarts
		var value int64
		for row := 0; row < v.count; row++ {
			if row%compactStreamRestart == 0 {
				value = int64(binary.LittleEndian.Uint64(v.data[cursor:]))
				cursor += 8
			} else {
				u, n, _ := readCompactUvarint(v.data[cursor:])
				cursor += n
				value += int64(u>>1) ^ -int64(u&1)
			}
			if value == needle {
				matched++
			}
		}
		return matched, true
	case compactStreamDeltaPack:
		blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
		for block := 0; block < blocks; block++ {
			cursor := int(binary.LittleEndian.Uint32(v.data[block*4:]))
			value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
			width := int(v.data[cursor+8])
			packed := v.data[cursor+9:]
			rows := min(compactStreamRestart, v.count-block*compactStreamRestart)
			if value == needle {
				matched++
			}
			for row := 1; row < rows; row++ {
				u := compactReadBits(packed, (row-1)*width, width)
				value += int64(u>>1) ^ -int64(u&1)
				if value == needle {
					matched++
				}
			}
		}
		return matched, true
	default:
		return 0, false
	}
}

func (v compactStreamView) validIntegerFORData() bool {
	if v.kind != compactStreamFOR || v.count < 0 || v.width > 64 || len(v.data) < 8 {
		return false
	}
	packedBytes := (uint64(v.count)*uint64(v.width) + 7) / 8
	return packedBytes == uint64(len(v.data)-8)
}

// countIntegerExtrema scans one signed integer FOR stream and returns its
// exact extrema. A width-64 stream, or a sub-64 stream whose base plus the
// maximum representable delta would cross MaxInt64, is declined: unsigned
// lane ordering is then no longer signed ordering and the native aggregate
// must leave the complete query to the generic executor.
func (v compactStreamView) countIntegerExtrema() (
	minimum, maximum int64, found, supported bool,
) {
	if !v.validIntegerFORData() || v.width == 64 || v.dictCount != 0 ||
		len(v.dictDir) != 0 || len(v.dictData) != 0 {
		return 0, 0, false, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	if v.width > 0 {
		maxDelta := (uint64(1) << v.width) - 1
		const maxInt64Value = uint64(1<<63 - 1)
		if base >= 0 && maxDelta > maxInt64Value-uint64(base) {
			return 0, 0, false, false
		}
	}
	minDelta, maxDelta, packedFound, packedSupported :=
		countCompactPackedExtrema(v.data[8:], v.count, int(v.width))
	if !packedSupported || !packedFound {
		return 0, 0, packedFound, packedSupported
	}
	return int64(uint64(base) + minDelta),
		int64(uint64(base) + maxDelta), true, true
}

// countIntegerLess scans one exact FOR stream for values below an exclusive
// signed threshold. For widths below 64 the signed ordering lets us compare
// packed unsigned deltas directly; a threshold outside the representable
// range becomes an all-zero or all-one result without touching the data. A
// full-width or structurally valid wrapping FOR stream reconstructs each
// signed value in the scalar bounded reader.
func (v compactStreamView) countIntegerLess(needle int64) (matched int, supported bool) {
	if !v.validIntegerFORData() {
		return 0, false
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	// Writer-produced FOR streams choose the signed minimum as base, so a
	// sub-64-bit span cannot wrap. The admitted binary grammar does not depend
	// on writer provenance, though: reconstruct a structurally valid wrapped
	// span exactly instead of applying the ordinary unsigned-delta shortcut.
	limit := uint64(0)
	if v.width < 64 {
		limit = uint64(1) << v.width
	}
	wrapped := v.width > 0 && v.width < 64 &&
		base > int64(1<<63-1)-int64(limit-1)
	if v.width == 64 || wrapped {
		for row := 0; row < v.count; row++ {
			delta := compactReadBits(v.data[8:], row*int(v.width), int(v.width))
			value := int64(uint64(base) + delta)
			if value < needle {
				matched++
			}
		}
		return matched, true
	}
	if needle <= base {
		return 0, true
	}
	delta := uint64(needle) - uint64(base)
	if delta >= limit {
		return v.count, true
	}
	return countCompactPackedLess(v.data[8:], v.count, int(v.width), delta), true
}

// countIntegerOrdered derives every ordered predicate from the one exclusive
// less-than scan. The boundary checks avoid overflowing MinInt64/MaxInt64 and
// complements are taken only after a complete exact scan.
func (v compactStreamView) countIntegerOrdered(
	needle int64, op UnifiedIntegerOrder,
) (matched int, supported bool) {
	if !validUnifiedIntegerOrder(op) {
		return 0, false
	}
	const maxInt64Value = int64(1<<63 - 1)
	switch op {
	case UnifiedIntegerLess:
		return v.countIntegerLess(needle)
	case UnifiedIntegerLessEqual:
		if needle == maxInt64Value {
			if !v.validIntegerFORData() {
				return 0, false
			}
			return v.count, true
		}
		return v.countIntegerLess(needle + 1)
	case UnifiedIntegerGreater:
		if needle == maxInt64Value {
			if !v.validIntegerFORData() {
				return 0, false
			}
			return 0, true
		}
		matched, supported = v.countIntegerLess(needle + 1)
		return v.count - matched, supported
	case UnifiedIntegerGreaterEqual:
		matched, supported = v.countIntegerLess(needle)
		return v.count - matched, supported
	default:
		return 0, false
	}
}

// countIntegerInterval scans one exact FOR stream for the normalized signed
// interval [interval.Lower, interval.Upper). The packed lane is decoded once
// and both bounds are intersected by countCompactPackedBetween. Full-width or
// wrapping FOR streams use exact wrapped reconstruction because their
// unsigned deltas do not preserve signed ordering.
func (v compactStreamView) countIntegerInterval(
	interval UnifiedIntegerInterval,
) (matched int, supported bool) {
	if !v.validIntegerFORData() {
		return 0, false
	}
	if !interval.UpperUnbounded && interval.Upper <= interval.Lower {
		return 0, true
	}
	base := int64(binary.LittleEndian.Uint64(v.data))
	limit := uint64(0)
	if v.width < 64 {
		limit = uint64(1) << v.width
	}
	wrapped := v.width > 0 && v.width < 64 &&
		base > int64(1<<63-1)-int64(limit-1)
	if v.width == 64 || wrapped {
		for row := 0; row < v.count; row++ {
			delta := compactReadBits(v.data[8:], row*int(v.width), int(v.width))
			value := int64(uint64(base) + delta)
			if value >= interval.Lower &&
				(interval.UpperUnbounded || value < interval.Upper) {
				matched++
			}
		}
		return matched, true
	}

	lower := uint64(0)
	if interval.Lower > base {
		lower = uint64(interval.Lower) - uint64(base)
	}
	if lower >= limit {
		return 0, true
	}
	upper := limit
	if !interval.UpperUnbounded && interval.Upper > base {
		upper = uint64(interval.Upper) - uint64(base)
		if upper > limit {
			upper = limit
		}
	} else if !interval.UpperUnbounded {
		return 0, true
	}
	if upper <= lower {
		return 0, true
	}
	return countCompactPackedBetween(v.data[8:], v.count, int(v.width), lower, upper), true
}

// countSpellingEqual scans one complete scalar stream for an exact canonical
// JSON spelling. scratch is retained by the caller for front-coded values so
// repeated scans remain allocation-free.
func (v compactStreamView) countSpellingEqual(
	needle, scratch []byte,
) (matched int, out []byte, supported bool) {
	switch v.kind {
	case compactStreamDictionary:
		matched, supported = v.countDictionaryEqual(needle)
		return matched, scratch, supported
	case compactStreamFront:
		matched, scratch = v.countFrontEqual(needle, scratch, false)
		return matched, scratch, true
	case compactStreamAlphabet:
		matched, scratch = v.countAlphabetEqual(needle, scratch)
		return matched, scratch, true
	case compactStreamFOR, compactStreamDelta, compactStreamDeltaPack:
		value, ok := CanonicalIntValue(needle)
		if !ok {
			return 0, scratch, false
		}
		matched, supported = v.countIntegerEqual(value)
		return matched, scratch, supported
	case compactStreamDate:
		ordinal, ok := compactDateOrdinal(needle)
		if !ok {
			return 0, scratch, false
		}
		base := int32(binary.LittleEndian.Uint32(v.data))
		delta := uint64(uint32(ordinal - base))
		limit := uint64(1) << v.width
		if ordinal < base || delta >= limit {
			delta = limit
		}
		return countCompactPackedEqual(
			v.data[4:], v.count, int(v.width), delta,
		), scratch, true
	case compactStreamPrefixInt:
		matched, supported = v.countPrefixIntegerEqual(needle)
		return matched, scratch, supported
	default:
		return 0, scratch, false
	}
}

// countNumberEqual scans one complete scalar stream with exact JSON decimal
// semantics. ids is reusable dictionary-match storage owned by the filter.
func (v compactStreamView) countNumberEqual(
	needle []byte,
	needleInt int64,
	needleIsInt bool,
	scratch []byte,
	ids []uint64,
) (matched int, out []byte, outIDs []uint64, supported bool) {
	switch v.kind {
	case compactStreamDictionary:
		words := (v.dictCount + 63) / 64
		ids = slices.Grow(ids[:0], words)[:words]
		clear(ids)
		for id := 0; id < v.dictCount; id++ {
			value, _ := v.dictionaryEntry(id)
			if compactJSONNumberEqual(value, needle) {
				ids[id>>6] |= uint64(1) << (id & 63)
			}
		}
		for row := 0; row < v.count; row++ {
			id := int(compactReadBits(v.data, row*int(v.width), int(v.width)))
			if ids[id>>6]&(uint64(1)<<(id&63)) != 0 {
				matched++
			}
		}
		return matched, scratch, ids, true
	case compactStreamFront:
		matched, scratch = v.countFrontEqual(needle, scratch, true)
		return matched, scratch, ids, true
	case compactStreamAlphabet:
		matched, scratch = v.countAlphabetNumberEqual(needle, scratch)
		return matched, scratch, ids, true
	case compactStreamFOR:
		if needleIsInt {
			matched, supported = v.countIntegerEqual(needleInt)
			return matched, scratch, ids, supported
		}
		base := int64(binary.LittleEndian.Uint64(v.data))
		_, supported = v.countIntegerEqual(base)
		return 0, scratch, ids, supported
	case compactStreamDelta, compactStreamDeltaPack:
		if needleIsInt {
			matched, supported = v.countIntegerEqual(needleInt)
			return matched, scratch, ids, supported
		}
		_, supported = v.countIntegerEqual(0)
		return 0, scratch, ids, supported
	default:
		return 0, scratch, ids, false
	}
}

func compactJSONNumberEqual(value, needle []byte) bool {
	if len(value) == 0 ||
		(value[0] != '-' && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	return vibejson.JSONNumberEqual(value, needle)
}

func (v compactStreamView) countAlphabetEqual(
	needle, scratch []byte,
) (matched int, out []byte) {
	alphabet, prefix, suffix, _ := v.alphabetParts()
	var codes [256]uint8
	var admitted [256]bool
	for code, b := range alphabet {
		codes[b] = uint8(code)
		admitted[b] = true
	}
	possible := len(needle) >= len(prefix)+len(suffix) &&
		bytes.HasPrefix(needle, prefix) && bytes.HasSuffix(needle, suffix)
	var middle []byte
	if possible {
		middle = needle[len(prefix) : len(needle)-len(suffix)]
	}
	width := int(v.width)
	needleBytes := (len(middle)*width + 7) / 8
	scratch = slices.Grow(scratch[:0], needleBytes)[:needleBytes]
	clear(scratch)
	for at, b := range middle {
		if !admitted[b] {
			possible = false
			continue
		}
		compactPutBits(scratch, at*width, width, uint64(codes[b]))
	}
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	for block := 0; block < blocks; block++ {
		base, lengths, bit, rows, lengthWidth := v.alphabetBlock(block)
		for row := range rows {
			length := base + compactReadBits(lengths, row*lengthWidth, lengthWidth)
			equal := possible && int(length) == len(middle) &&
				compactPackedBitsEqual(
					v.data, bit, scratch, int(length)*width,
				)
			if equal {
				matched++
			}
			bit += int(length) * width
		}
	}
	return matched, scratch
}

func compactPackedBitsEqual(data []byte, bit int, want []byte, nbits int) bool {
	wantAt := 0
	for nbits >= 64 {
		byteAt := bit >> 3
		shift := bit & 7
		got := binary.LittleEndian.Uint64(data[byteAt:]) >> shift
		if shift != 0 {
			got |= uint64(data[byteAt+8]) << (64 - shift)
		}
		if got != binary.LittleEndian.Uint64(want[wantAt:]) {
			return false
		}
		bit += 64
		wantAt += 8
		nbits -= 64
	}
	if nbits == 0 {
		return true
	}
	return compactReadBits(data, bit, nbits) ==
		compactReadBits(want[wantAt:], 0, nbits)
}

func (v compactStreamView) countAlphabetNumberEqual(
	needle, scratch []byte,
) (matched int, out []byte) {
	alphabet, prefix, suffix, _ := v.alphabetParts()
	blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
	width := int(v.width)
	for block := 0; block < blocks; block++ {
		base, lengths, bit, rows, lengthWidth := v.alphabetBlock(block)
		for row := range rows {
			length := base + compactReadBits(lengths, row*lengthWidth, lengthWidth)
			fullLength := len(prefix) + int(length) + len(suffix)
			scratch = slices.Grow(scratch[:0], fullLength)[:fullLength]
			copy(scratch, prefix)
			middle := len(prefix)
			for char := 0; char < int(length); char++ {
				scratch[middle+char] = alphabet[int(compactReadBits(
					v.data, bit+char*width, width,
				))]
			}
			copy(scratch[middle+int(length):], suffix)
			if compactJSONNumberEqual(scratch, needle) {
				matched++
			}
			bit += int(length) * width
		}
	}
	return matched, scratch
}

func (v compactStreamView) countFrontEqual(
	needle, scratch []byte,
	numbersByValue bool,
) (matched int, out []byte) {
	restarts := (v.count + compactStreamRestart - 1) / compactStreamRestart
	cursor := 4 * restarts
	for row := 0; row < v.count; row++ {
		if row%compactStreamRestart == 0 {
			length, n, _ := readCompactUvarint(v.data[cursor:])
			cursor += n
			scratch = append(scratch[:0], v.data[cursor:cursor+int(length)]...)
			cursor += int(length)
		} else {
			packed := v.data[cursor]
			cursor++
			prefix, suffix := int(packed>>4), int(packed&15)
			if packed == 0xff {
				p, n, _ := readCompactUvarint(v.data[cursor:])
				cursor += n
				s, n, _ := readCompactUvarint(v.data[cursor:])
				cursor += n
				prefix, suffix = int(p), int(s)
			}
			scratch = append(scratch[:prefix], v.data[cursor:cursor+suffix]...)
			cursor += suffix
		}
		equal := bytes.Equal(scratch, needle)
		if numbersByValue {
			equal = compactJSONNumberEqual(scratch, needle)
		}
		if equal {
			matched++
		}
	}
	return matched, scratch
}

func (v compactStreamView) countPrefixIntegerEqual(needle []byte) (matched int, supported bool) {
	parsed, ok := parseCompactPrefixInt(needle)
	if !ok {
		return 0, false
	}
	prefix, _ := v.dictionaryEntry(0)
	suffix, _ := v.dictionaryEntry(1)
	if !bytes.Equal(parsed.prefix, prefix) || !bytes.Equal(parsed.suffix, suffix) ||
		v.data[0]&1 != 0 && parsed.width != int(v.data[1]) ||
		v.data[0]&1 == 0 && !parsed.canonical {
		return 0, false
	}
	want := int64(parsed.value)
	if v.data[0]&2 != 0 {
		first := int64(binary.LittleEndian.Uint64(v.data[2:]))
		delta := int64(binary.LittleEndian.Uint64(v.data[10:]))
		for row := 0; row < v.count; row++ {
			if first+int64(row)*delta == want {
				matched++
			}
		}
		return matched, true
	}
	if v.data[0]&4 != 0 {
		blocks := (v.count + compactStreamRestart - 1) / compactStreamRestart
		for block := 0; block < blocks; block++ {
			cursor := int(binary.LittleEndian.Uint32(v.data[2+block*4:]))
			value := int64(binary.LittleEndian.Uint64(v.data[cursor:]))
			width := int(v.data[cursor+8])
			packed := v.data[cursor+9:]
			rows := min(compactStreamRestart, v.count-block*compactStreamRestart)
			if value == want {
				matched++
			}
			for row := 1; row < rows; row++ {
				u := compactReadBits(packed, (row-1)*width, width)
				value += int64(u>>1) ^ -int64(u&1)
				if value == want {
					matched++
				}
			}
		}
		return matched, true
	}
	restarts := (v.count + compactStreamRestart - 1) / compactStreamRestart
	cursor := 2 + 4*restarts
	var value int64
	for row := 0; row < v.count; row++ {
		if row%compactStreamRestart == 0 {
			value = int64(binary.LittleEndian.Uint64(v.data[cursor:]))
			cursor += 8
		} else {
			u, n, _ := readCompactUvarint(v.data[cursor:])
			cursor += n
			value += int64(u>>1) ^ -int64(u&1)
		}
		if value == want {
			matched++
		}
	}
	return matched, true
}

// countDictionaryEqual performs a complete encoded-id scan. supported is
// false for non-dictionary streams; callers may decode those streams or take a
// generic comparison path. No row index, pruning metadata, or candidate list
// participates.
func (v compactStreamView) countDictionaryEqual(needle []byte) (matched int, supported bool) {
	if v.kind != compactStreamDictionary {
		return 0, false
	}
	id := -1
	for at := 0; at < v.dictCount; at++ {
		value, _ := v.dictionaryEntry(at)
		if bytes.Equal(value, needle) {
			id = at
			break
		}
	}
	if id < 0 {
		return 0, true
	}
	return countCompactPackedEqual(
		v.data, v.count, int(v.width), uint64(id),
	), true
}

// countCompactPackedEqual consumes every fixed-width dictionary id in lexical
// row order through one little-endian bit reservoir. Unlike compactReadBits,
// which is optimized for random access, this scan carries unused bits across
// rows and avoids recomputing a bit offset and crossing bytes from scratch for
// every value. Padding after count is deliberately ignored.
func countCompactPackedEqual(
	data []byte,
	count, width int,
	want uint64,
) (matched int) {
	if count <= 0 || width < 0 || width > 64 {
		return 0
	}
	if width == 0 {
		if want == 0 {
			return count
		}
		return 0
	}
	if width == 7 {
		return countCompactPacked7EqualImpl(data, count, want)
	}
	if width == 8 {
		return countCompactPacked8EqualImpl(data, count, want)
	}
	if width == 10 {
		return countCompactPacked10EqualImpl(data, count, want)
	}
	if width == 16 {
		return countCompactPacked16EqualImpl(data, count, want)
	}
	if width > 56 {
		for row := 0; row < count; row++ {
			if compactReadBits(data, row*width, width) == want {
				matched++
			}
		}
		return matched
	}
	mask := uint64(1)<<uint(width) - 1
	var reservoir uint64
	available := 0
	cursor := 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		if reservoir&mask == want {
			matched++
		}
		reservoir >>= uint(width)
		available -= width
	}
	return matched
}

// countCompactPackedLess counts packed unsigned lanes below an exclusive
// threshold. The caller handles thresholds outside the lane's representable
// range, so the SIMD entry points only need to compare ordinary lane values.
// Keeping this operation separate from equality lets ordered FOR predicates
// derive all four SQL operators from one exact threshold scan.
func countCompactPackedLess(
	data []byte,
	count, width int,
	threshold uint64,
) (matched int) {
	if count <= 0 || width < 0 || width > 64 || threshold == 0 {
		return 0
	}
	if width == 0 {
		return count
	}
	if width < 64 && threshold >= uint64(1)<<uint(width) {
		return count
	}
	switch width {
	case 7:
		return countCompactPacked7LessImpl(data, count, threshold)
	case 8:
		return countCompactPacked8LessImpl(data, count, threshold)
	case 10:
		return countCompactPacked10LessImpl(data, count, threshold)
	case 16:
		return countCompactPacked16LessImpl(data, count, threshold)
	}
	if width > 56 {
		for row := 0; row < count; row++ {
			if compactReadBits(data, row*width, width) < threshold {
				matched++
			}
		}
		return matched
	}
	mask := uint64(1)<<uint(width) - 1
	var reservoir uint64
	available := 0
	cursor := 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		if reservoir&mask < threshold {
			matched++
		}
		reservoir >>= uint(width)
		available -= width
	}
	return matched
}

// countCompactPackedBetween counts packed unsigned lanes in the half-open
// interval [lower, upper). The caller has already normalized the signed FOR
// bounds and established that both endpoints fit the lane domain. Empty and
// whole-domain intervals are answered without touching the data; one-sided
// intervals reuse the existing SIMD less-than counter, while finite intervals
// dispatch to a fused counter that decodes each packed lane once.
func countCompactPackedBetween(
	data []byte,
	count, width int,
	lower, upper uint64,
) (matched int) {
	if count <= 0 || width < 0 || width > 64 || upper <= lower {
		return 0
	}
	if width == 0 {
		if lower == 0 && upper > 0 {
			return count
		}
		return 0
	}
	if width < 64 {
		limit := uint64(1) << uint(width)
		if lower >= limit {
			return 0
		}
		if upper > limit {
			upper = limit
		}
		if upper <= lower {
			return 0
		}
		if lower == 0 {
			return countCompactPackedLess(data, count, width, upper)
		}
		if upper == limit {
			return count - countCompactPackedLess(data, count, width, lower)
		}
	}
	if width == 64 {
		// Full-width intervals are not used by the strict packed FOR lane, but
		// retain an exact bounded fallback for direct codec callers.
		for row := 0; row < count; row++ {
			value := compactReadBits(data, row*width, width)
			if value >= lower && value < upper {
				matched++
			}
		}
		return matched
	}
	switch width {
	case 7:
		return countCompactPacked7BetweenImpl(data, count, lower, upper)
	case 8:
		return countCompactPacked8BetweenImpl(data, count, lower, upper)
	case 10:
		return countCompactPacked10BetweenImpl(data, count, lower, upper)
	case 16:
		return countCompactPacked16BetweenImpl(data, count, lower, upper)
	}
	mask := uint64(1)<<uint(width) - 1
	var reservoir uint64
	available := 0
	cursor := 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		value := reservoir & mask
		if value >= lower && value < upper {
			matched++
		}
		reservoir >>= uint(width)
		available -= width
	}
	return matched
}

// countCompactPackedExtrema returns the unsigned minimum and maximum packed
// lane values. The caller adds the FOR base after proving that reconstruction
// cannot wrap. found is false only for an empty input; malformed geometry is
// also reported as unsupported so a storage-native aggregate can decline
// atomically and leave the generic executor authoritative.
func countCompactPackedExtrema(
	data []byte, count, width int,
) (minimum, maximum uint64, found, supported bool) {
	if count < 0 || width < 0 || width > 64 {
		return 0, 0, false, false
	}
	if count == 0 {
		return 0, 0, false, true
	}
	if width == 0 {
		return 0, 0, true, true
	}
	packedBytes := (uint64(count)*uint64(width) + 7) / 8
	if uint64(len(data)) != packedBytes {
		return 0, 0, false, false
	}
	if width == 7 {
		return countCompactPacked7ExtremaImpl(data, count)
	}
	if width == 8 {
		return countCompactPacked8ExtremaImpl(data, count)
	}
	if width == 10 {
		return countCompactPacked10ExtremaImpl(data, count)
	}
	if width == 16 {
		return countCompactPacked16ExtremaImpl(data, count)
	}
	if width > 56 {
		minimum = ^uint64(0)
		for row := 0; row < count; row++ {
			value := compactReadBits(data, row*width, width)
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
		return minimum, maximum, true, true
	}
	mask := uint64(1)<<uint(width) - 1
	minimum = mask
	var reservoir uint64
	available := 0
	cursor := 0
	for range count {
		for available < width {
			reservoir |= uint64(data[cursor]) << uint(available)
			cursor++
			available += 8
		}
		value := reservoir & mask
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
		reservoir >>= uint(width)
		available -= width
	}
	return minimum, maximum, true, true
}

// Byte-aligned widths can compare values directly without a bit reservoir.
// Keep the full uint64 needle so out-of-range values scan without matching.
func countCompactPacked8EqualScalar(data []byte, count int, want uint64) (matched int) {
	for row := 0; row < count; row++ {
		if uint64(data[row]) == want {
			matched++
		}
	}
	return matched
}

func countCompactPacked16EqualScalar(data []byte, count int, want uint64) (matched int) {
	for row := 0; row < count; row++ {
		value := binary.LittleEndian.Uint16(data[row*2:])
		if uint64(value) == want {
			matched++
		}
	}
	return matched
}

func countCompactPacked8LessScalar(
	data []byte,
	count int,
	threshold uint64,
) (matched int) {
	for row := 0; row < count; row++ {
		if uint64(data[row]) < threshold {
			matched++
		}
	}
	return matched
}

func countCompactPacked16LessScalar(
	data []byte,
	count int,
	threshold uint64,
) (matched int) {
	for row := 0; row < count; row++ {
		if uint64(binary.LittleEndian.Uint16(data[row*2:])) < threshold {
			matched++
		}
	}
	return matched
}

func countCompactPacked8BetweenScalar(
	data []byte,
	count int,
	lower, upper uint64,
) (matched int) {
	for row := 0; row < count; row++ {
		value := uint64(data[row])
		if value >= lower && value < upper {
			matched++
		}
	}
	return matched
}

func countCompactPacked16BetweenScalar(
	data []byte,
	count int,
	lower, upper uint64,
) (matched int) {
	for row := 0; row < count; row++ {
		value := uint64(binary.LittleEndian.Uint16(data[row*2:]))
		if value >= lower && value < upper {
			matched++
		}
	}
	return matched
}

func countCompactPacked8ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	minimum = uint64(data[0])
	maximum = minimum
	for row := 1; row < count; row++ {
		value := uint64(data[row])
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked16ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	minimum = uint64(binary.LittleEndian.Uint16(data))
	maximum = minimum
	for row := 1; row < count; row++ {
		value := uint64(binary.LittleEndian.Uint16(data[row*2:]))
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked7ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	row, cursor := 0, 0
	minimum = 127
	for ; row+8 <= count; row, cursor = row+8, cursor+7 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32 |
			uint64(data[cursor+5])<<40 |
			uint64(data[cursor+6])<<48
		for at := 0; at < 8; at++ {
			value := packed >> uint(at*7) & 127
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*7, 7)
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

func countCompactPacked10ExtremaScalar(
	data []byte, count int,
) (minimum, maximum uint64, found, supported bool) {
	if count <= 0 {
		return 0, 0, false, true
	}
	row, cursor := 0, 0
	minimum = 1023
	for ; row+4 <= count; row, cursor = row+4, cursor+5 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32
		for at := 0; at < 4; at++ {
			value := packed >> uint(at*10) & 1023
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*10, 10)
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
	}
	return minimum, maximum, true, true
}

// Eight 7-bit ids occupy exactly seven bytes. This width is common for
// low-cardinality columns with 65-128 values, and consuming complete groups
// avoids the generic reservoir's refill branch on almost every row.
func countCompactPacked7EqualScalar(data []byte, count int, want uint64) (matched int) {
	row := 0
	cursor := 0
	for ; row+8 <= count; row, cursor = row+8, cursor+7 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32 |
			uint64(data[cursor+5])<<40 |
			uint64(data[cursor+6])<<48
		if packed&0x7f == want {
			matched++
		}
		if packed>>7&0x7f == want {
			matched++
		}
		if packed>>14&0x7f == want {
			matched++
		}
		if packed>>21&0x7f == want {
			matched++
		}
		if packed>>28&0x7f == want {
			matched++
		}
		if packed>>35&0x7f == want {
			matched++
		}
		if packed>>42&0x7f == want {
			matched++
		}
		if packed>>49&0x7f == want {
			matched++
		}
	}
	for ; row < count; row++ {
		if compactReadBits(data, row*7, 7) == want {
			matched++
		}
	}
	return matched
}

// countCompactPacked7LessScalar consumes eight 7-bit lanes per seven-byte
// group. It is also the short-input and portable oracle for the SIMD lane.
func countCompactPacked7LessScalar(
	data []byte,
	count int,
	threshold uint64,
) (matched int) {
	row := 0
	cursor := 0
	for ; row+8 <= count; row, cursor = row+8, cursor+7 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32 |
			uint64(data[cursor+5])<<40 |
			uint64(data[cursor+6])<<48
		if packed&0x7f < threshold {
			matched++
		}
		if packed>>7&0x7f < threshold {
			matched++
		}
		if packed>>14&0x7f < threshold {
			matched++
		}
		if packed>>21&0x7f < threshold {
			matched++
		}
		if packed>>28&0x7f < threshold {
			matched++
		}
		if packed>>35&0x7f < threshold {
			matched++
		}
		if packed>>42&0x7f < threshold {
			matched++
		}
		if packed>>49&0x7f < threshold {
			matched++
		}
	}
	for ; row < count; row++ {
		if compactReadBits(data, row*7, 7) < threshold {
			matched++
		}
	}
	return matched
}

// countCompactPacked7BetweenScalar consumes eight 7-bit lanes per seven-byte
// group and applies both bounds while each packed word is decoded once.
func countCompactPacked7BetweenScalar(
	data []byte,
	count int,
	lower, upper uint64,
) (matched int) {
	row := 0
	cursor := 0
	for ; row+8 <= count; row, cursor = row+8, cursor+7 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32 |
			uint64(data[cursor+5])<<40 |
			uint64(data[cursor+6])<<48
		if value := packed & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 7 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 14 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 21 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 28 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 35 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 42 & 0x7f; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 49 & 0x7f; value >= lower && value < upper {
			matched++
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*7, 7)
		if value >= lower && value < upper {
			matched++
		}
	}
	return matched
}

// Four 10-bit values occupy exactly five bytes. This is the ordinary packed
// width for integer ranges and dictionaries with 513-1024 distinct values.
func countCompactPacked10EqualScalar(data []byte, count int, want uint64) (matched int) {
	row := 0
	cursor := 0
	for ; row+4 <= count; row, cursor = row+4, cursor+5 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32
		if packed&0x3ff == want {
			matched++
		}
		if packed>>10&0x3ff == want {
			matched++
		}
		if packed>>20&0x3ff == want {
			matched++
		}
		if packed>>30&0x3ff == want {
			matched++
		}
	}
	for ; row < count; row++ {
		if compactReadBits(data, row*10, 10) == want {
			matched++
		}
	}
	return matched
}

// countCompactPacked10LessScalar consumes four 10-bit lanes per five-byte
// group. It deliberately keeps threshold as uint64 so direct tests can prove
// that an out-of-range threshold does not truncate into a false match.
func countCompactPacked10LessScalar(
	data []byte,
	count int,
	threshold uint64,
) (matched int) {
	row := 0
	cursor := 0
	for ; row+4 <= count; row, cursor = row+4, cursor+5 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32
		if packed&0x3ff < threshold {
			matched++
		}
		if packed>>10&0x3ff < threshold {
			matched++
		}
		if packed>>20&0x3ff < threshold {
			matched++
		}
		if packed>>30&0x3ff < threshold {
			matched++
		}
	}
	for ; row < count; row++ {
		if compactReadBits(data, row*10, 10) < threshold {
			matched++
		}
	}
	return matched
}

// countCompactPacked10BetweenScalar consumes four 10-bit lanes per five-byte
// group and applies both bounds to the decoded lanes.
func countCompactPacked10BetweenScalar(
	data []byte,
	count int,
	lower, upper uint64,
) (matched int) {
	row := 0
	cursor := 0
	for ; row+4 <= count; row, cursor = row+4, cursor+5 {
		packed := uint64(data[cursor]) |
			uint64(data[cursor+1])<<8 |
			uint64(data[cursor+2])<<16 |
			uint64(data[cursor+3])<<24 |
			uint64(data[cursor+4])<<32
		if value := packed & 0x3ff; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 10 & 0x3ff; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 20 & 0x3ff; value >= lower && value < upper {
			matched++
		}
		if value := packed >> 30 & 0x3ff; value >= lower && value < upper {
			matched++
		}
	}
	for ; row < count; row++ {
		value := compactReadBits(data, row*10, 10)
		if value >= lower && value < upper {
			matched++
		}
	}
	return matched
}
