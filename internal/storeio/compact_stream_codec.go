package storeio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"strconv"
)

// Compact scalar streams are the independently decodable columns inside the
// next primary-stripe format. The codec is schema-free: the caller supplies
// exact canonical scalar spellings in row order, and the planner chooses the
// smallest reversible representation. Every sequential codec has bounded
// restart points so a point read never replays an unbounded stripe prefix.
const (
	compactStreamRestart = 64
	compactStreamHeader  = 20

	compactStreamDictionary uint8 = iota
	compactStreamFront
	compactStreamFOR
	compactStreamDelta
	compactStreamDate
	compactStreamPrefixInt
	compactStreamKindLimit
)

type compactStreamEncoding struct {
	kind  uint8
	width uint8
	count int
	data  []byte
	dict  [][]byte
}

// compactStreamScratch retains every candidate's backing storage while the
// planner chooses the smallest representation. A stripe emits the winner
// before reusing this workspace for the next column, so one fixed candidate
// set serves every stream without per-column allocation churn.
type compactStreamScratch struct {
	candidates [6]compactStreamEncoding
	data       [6][]byte
	dict       [6][][]byte
	integers   []int64
	dates      []int32
	parsed     []uint64
}

func (e compactStreamEncoding) encodedBytes() int {
	n := compactStreamHeader + 4*len(e.dict) + len(e.data)
	for _, value := range e.dict {
		n += len(value)
	}
	return n
}

// appendBinary appends one self-delimiting stream. Dictionary ends are u32 so
// arbitrary canonical scalar spellings remain representable; compact codecs
// are selected only when the complete stream fits the enclosing page.
func (e compactStreamEncoding) appendBinary(dst []byte) ([]byte, error) {
	if e.kind >= compactStreamKindLimit || e.count < 0 ||
		len(e.dict) > int(^uint16(0)) || uint64(len(e.data)) > uint64(^uint32(0)) {
		return dst, fmt.Errorf("%w: compact stream encoding", ErrInvalidWrite)
	}
	start := len(dst)
	dst = append(dst, make([]byte, compactStreamHeader+4*len(e.dict))...)
	dst[start] = e.kind
	dst[start+1] = e.width
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(e.count))
	binary.LittleEndian.PutUint16(dst[start+8:], uint16(len(e.dict)))
	dictionaryBytes := 0
	for id, value := range e.dict {
		dictionaryBytes += len(value)
		if uint64(dictionaryBytes) > uint64(^uint32(0)) {
			return dst[:start], fmt.Errorf("%w: compact stream dictionary", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint32(
			dst[start+compactStreamHeader+id*4:], uint32(dictionaryBytes),
		)
	}
	binary.LittleEndian.PutUint32(dst[start+12:], uint32(dictionaryBytes))
	binary.LittleEndian.PutUint32(dst[start+16:], uint32(len(e.data)))
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
	n := 0
	s.candidates[n] = s.encodeDictionary(n, values)
	n++
	s.candidates[n] = s.encodeFront(n, values)
	n++
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
	best := s.candidates[0]
	for _, candidate := range s.candidates[1:n] {
		if candidate.encodedBytes() < best.encodedBytes() {
			best = candidate
		}
	}
	return best
}

func (s *compactStreamScratch) encodeDictionary(slot int, values [][]byte) compactStreamEncoding {
	dictionary := slices.Grow(s.dict[slot][:0], len(values))[:len(values)]
	copy(dictionary, values)
	slices.SortFunc(dictionary, bytes.Compare)
	unique := 0
	for _, value := range dictionary {
		if unique == 0 || !bytes.Equal(dictionary[unique-1], value) {
			dictionary[unique] = value
			unique++
		}
	}
	dictionary = dictionary[:unique]
	s.dict[slot] = dictionary
	width := bits.Len(uint(max(0, len(dictionary)-1)))
	dataBytes := (len(values)*width + 7) / 8
	data := slices.Grow(s.data[slot][:0], dataBytes)[:dataBytes]
	clear(data)
	for row, value := range values {
		id, found := slices.BinarySearchFunc(dictionary, value, bytes.Compare)
		if !found {
			panic("compact dictionary planner lost value")
		}
		compactPutBits(data, row*width, width, uint64(id))
	}
	s.data[slot] = data
	return compactStreamEncoding{
		kind: compactStreamDictionary, width: uint8(width), count: len(values),
		data: data, dict: dictionary,
	}
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
	data := slices.Grow(s.data[slot][:0], 2+4*restarts)[:2+4*restarts]
	clear(data)
	if fixedWidth {
		if first.width > int(^uint8(0)) {
			return compactStreamEncoding{}, false
		}
		data[0], data[1] = 1, byte(first.width)
	}
	var previous uint64
	for row, value := range parsed {
		if row%compactStreamRestart == 0 {
			binary.LittleEndian.PutUint32(data[2+(row/compactStreamRestart)*4:], uint32(len(data)))
			var fixed [8]byte
			binary.LittleEndian.PutUint64(fixed[:], value)
			data = append(data, fixed[:]...)
		} else {
			delta := int64(value - previous)
			data = appendCompactUvarint(data, uint64(delta<<1)^uint64(delta>>63))
		}
		previous = value
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
	n := min(len(a), len(b))
	i := 0
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

func appendCompactDate(dst []byte, ordinal int32) []byte {
	dayNumber := int(ordinal)
	lo, hi := 0, 10_000
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if compactDaysBeforeYear(mid) <= dayNumber {
			lo = mid
		} else {
			hi = mid
		}
	}
	year := lo
	dayOfYear := dayNumber - compactDaysBeforeYear(year)
	leap := year%4 == 0 && (year%100 != 0 || year%400 == 0)
	monthDays := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	month := 1
	for month <= 12 {
		days := monthDays[month-1]
		if month == 2 && leap {
			days++
		}
		if dayOfYear < days {
			break
		}
		dayOfYear -= days
		month++
	}
	day := dayOfYear + 1
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
	if kind >= compactStreamKindLimit || src[2] != 0 || src[3] != 0 ||
		binary.LittleEndian.Uint16(src[10:]) != 0 {
		return corrupt("kind or reserved bytes")
	}
	count := int(binary.LittleEndian.Uint32(src[4:]))
	dictCount := int(binary.LittleEndian.Uint16(src[8:]))
	dictBytes := int(binary.LittleEndian.Uint32(src[12:]))
	dataBytes := int(binary.LittleEndian.Uint32(src[16:]))
	dirBytes := 4 * dictCount
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
		end := binary.LittleEndian.Uint32(v.dictDir[id*4:])
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
	dictCount := int(binary.LittleEndian.Uint16(src[8:]))
	dictBytes := int(binary.LittleEndian.Uint32(src[12:]))
	dataBytes := int(binary.LittleEndian.Uint32(src[16:]))
	dirBytes := 4 * dictCount
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

func (v compactStreamView) dictionaryEntry(id int) ([]byte, bool) {
	if id < 0 || id >= v.dictCount {
		return nil, false
	}
	start := uint32(0)
	if id != 0 {
		start = binary.LittleEndian.Uint32(v.dictDir[(id-1)*4:])
	}
	end := binary.LittleEndian.Uint32(v.dictDir[id*4:])
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
			v.data[0] > 3 || v.data[0]&1 == 0 && v.data[1] != 0 ||
			v.data[0]&1 != 0 && v.data[1] == 0 {
			return corrupt("prefix-integer data")
		}
		if v.data[0]&2 != 0 {
			if len(v.data) != 18 {
				return corrupt("linear prefix-integer data")
			}
		} else if !validateCompactRestartVarints(v.data, v.count, 2) {
			return corrupt("prefix-integer restart data")
		}
	default:
		return corrupt("kind")
	}
	return nil
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
	}
	return dst, false
}

func (v compactStreamView) prefixInteger(row int) (int64, bool) {
	if v.kind != compactStreamPrefixInt || row < 0 || row >= v.count || len(v.data) < 2 {
		return 0, false
	}
	if v.data[0]&2 == 0 {
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
	for row := 0; row < v.count; row++ {
		if int(compactReadBits(v.data, row*int(v.width), int(v.width))) == id {
			matched++
		}
	}
	return matched, true
}
