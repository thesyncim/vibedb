package storeio

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"slices"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
)

// This file is the executable design gate for the next durable format. It is
// intentionally not wired into the store until both size and scan gates pass:
// the prototype must encode every key and scalar spelling needed to reconstruct
// the canonical documents, charge the existing exact template representation
// byte-for-byte, and scan encoded field streams rather than consult a row
// index. Durable page framing and template emission remain integration work.

const (
	compactPrototypeRows    = 4 << 10
	compactPrototypeRestart = 64
)

const (
	compactPrototypeDict = iota
	compactPrototypeFront
	compactPrototypeFOR
	compactPrototypeDelta
	compactPrototypeDate
	compactPrototypePrefixInt
)

type compactPrototypeStream struct {
	kind  uint8
	width uint8
	count int
	data  []byte
	dict  [][]byte
}

func (s compactPrototypeStream) bytes() int {
	// kind, width, count, data length and codec payload metadata.
	n := 12 + len(s.data)
	for _, value := range s.dict {
		n += compactPrototypeUvarintLen(uint64(len(value))) + len(value)
	}
	return n
}

func (s *compactPrototypeStream) dictID(value []byte) int {
	for id := range s.dict {
		if bytes.Equal(s.dict[id], value) {
			return id
		}
	}
	return -1
}

func (s *compactPrototypeStream) countDictID(id int) int {
	if s.kind != compactPrototypeDict || id < 0 || id >= len(s.dict) {
		return 0
	}
	if s.width == 0 {
		if id == 0 {
			return s.count
		}
		return 0
	}
	matched := 0
	bit := 0
	for range s.count {
		if compactPrototypeReadBits(s.data, bit, int(s.width)) == uint64(id) {
			matched++
		}
		bit += int(s.width)
	}
	return matched
}

type compactPrototypeShape struct {
	streams           []compactPrototypeStream
	countryStream     int
	productionCountry compactStreamView
}

type compactPrototypeStripe struct {
	rows             int
	bytes            int
	productionBytes  int
	productionExtent int
	shapeCodes       []byte
	shapes           []compactPrototypeShape
}

type compactPrototypeStats struct {
	fixed, keys, shapeCodes, templates int
	codecBytes                         [6]int
	productionCodecBytes               [compactStreamKindLimit]int
	productionKeyBytes                 int
	holeBytes                          [32]int
	holeValues                         [32]int
}

func (s *compactPrototypeStripe) countCountry(needle []byte) int {
	matched := 0
	for shape := range s.shapes {
		field := s.shapes[shape].countryStream
		if field < 0 {
			continue
		}
		stream := &s.shapes[shape].streams[field]
		matched += stream.countDictID(stream.dictID(needle))
	}
	return matched
}

func (s *compactPrototypeStripe) countCountryProduction(needle []byte) int {
	matched := 0
	for shape := range s.shapes {
		count, supported := s.shapes[shape].productionCountry.countDictionaryEqual(needle)
		if supported {
			matched += count
		}
	}
	return matched
}

func buildCompactPrototype(t testing.TB, rows int, high bool) ([]compactPrototypeStripe, int, int, compactPrototypeStats) {
	t.Helper()
	corpus := benchcorpus.Corpus(rows, high)
	records := make([]CommonPrimaryLeafRecord, len(corpus))
	for i := range corpus {
		records[i] = CommonPrimaryLeafRecord{
			Key:   []byte(corpus[i].Key),
			Value: CommonPrimaryLeafValue{Inline: corpus[i].JSON},
		}
	}
	stripes := make([]compactPrototypeStripe, 0, (rows+compactPrototypeRows-1)/compactPrototypeRows)
	total := 0
	productionTotal := 0
	var stats compactPrototypeStats
	builder := NewUnifiedPrimaryLeafBuilder()
	var resolver UnifiedHoleResolver
	if err := resolver.SetPath([]byte("/country")); err != nil {
		t.Fatal(err)
	}
	for first := 0; first < len(records); first += compactPrototypeRows {
		last := min(first+compactPrototypeRows, len(records))
		window := records[first:last]
		if err := builder.extract(window); err != nil {
			t.Fatal(err)
		}
		stripe := compactPrototypeStripe{rows: len(window)}
		// Stripe header plus a front-coded exact-key stream.
		stripe.bytes = 32
		stripe.productionBytes = 32 + PageHeaderSize + PageTrailerSize
		stats.fixed += 32
		keys := make([][]byte, len(window))
		for i := range window {
			keys[i] = window[i].Key
		}
		keyStream := compactPrototypeEncode(keys)
		compactPrototypeValidateStream(t, keyStream, keys)
		keyBytes := keyStream.bytes()
		stripe.bytes += keyBytes
		stats.keys += keyBytes
		productionKey := encodeCompactScalarStream(keys)
		productionKeyView := compactCodecRoundTrip(t, productionKey, keys)
		stripe.productionBytes += productionKeyView.encoded
		stats.productionKeyBytes += productionKeyView.encoded

		shapeWidth := bits.Len(uint(max(0, len(builder.shapes)-1)))
		stripe.shapeCodes = make([]byte, (len(window)*shapeWidth+7)/8)
		shapeRows := make([][]int, len(builder.shapes))
		for row := range builder.rows {
			shape := int(builder.rows[row].shape)
			if shape < 0 {
				t.Fatal("compact prototype does not admit overflow rows")
			}
			compactPrototypePutBits(stripe.shapeCodes, row*shapeWidth, shapeWidth, uint64(shape))
			shapeRows[shape] = append(shapeRows[shape], row)
		}
		stripe.bytes += len(stripe.shapeCodes)
		stripe.productionBytes += len(stripe.shapeCodes)
		stats.shapeCodes += len(stripe.shapeCodes)
		stripe.shapes = make([]compactPrototypeShape, len(builder.shapes))
		for shape := range builder.shapes {
			shapePlan := &builder.shapes[shape]
			// Directory end plus the existing exact skeleton representation.
			stripe.bytes += 4 + shapePlan.entryBytes
			stripe.productionBytes += 4 + shapePlan.entryBytes
			stats.templates += 4 + shapePlan.entryBytes
			out := &stripe.shapes[shape]
			out.countryStream = -1
			out.streams = make([]compactPrototypeStream, shapePlan.holes)
			columns := make([][][]byte, shapePlan.holes)
			for _, rowIndex := range shapeRows[shape] {
				row := &builder.rows[rowIndex]
				canonical := builder.canonicalOf(rowIndex)
				for hole, span := range builder.spans[row.spanStart:row.spanEnd] {
					columns[hole] = append(columns[hole], canonical[span.Start:span.End])
				}
			}
			if len(shapeRows[shape]) != 0 {
				rowIndex := shapeRows[shape][0]
				canonical := builder.canonicalOf(rowIndex)
				start, end, found, err := resolver.PathSpanOf(canonical)
				if err != nil {
					t.Fatal(err)
				}
				if found {
					row := &builder.rows[rowIndex]
					for hole, span := range builder.spans[row.spanStart:row.spanEnd] {
						if span.Start == start && span.End == end {
							out.countryStream = hole
							break
						}
					}
				}
			}
			for hole := range columns {
				out.streams[hole] = compactPrototypeEncode(columns[hole])
				compactPrototypeValidateStream(t, out.streams[hole], columns[hole])
				streamBytes := out.streams[hole].bytes()
				stripe.bytes += streamBytes
				stats.codecBytes[out.streams[hole].kind] += streamBytes
				stats.holeBytes[hole] += streamBytes
				stats.holeValues[hole] += len(columns[hole])
				production := encodeCompactScalarStream(columns[hole])
				productionBytes := production.encodedBytes()
				stripe.productionBytes += productionBytes
				stats.productionCodecBytes[production.kind] += productionBytes
				if productionBytes <= int(^uint16(0)) {
					productionView := compactCodecRoundTrip(t, production, columns[hole])
					if hole == out.countryStream {
						out.productionCountry = productionView
					}
				} else if hole == out.countryStream {
					t.Fatal("country production stream exceeds one leaf")
				}
			}
		}
		quantum := int(physicalPageQuantum)
		stripe.productionExtent = (stripe.productionBytes + quantum - 1) &^ (quantum - 1)
		stripes = append(stripes, stripe)
		total += stripe.bytes
		productionTotal += stripe.productionExtent
	}
	return stripes, total, productionTotal, stats
}

func compactPrototypeEncode(values [][]byte) compactPrototypeStream {
	if len(values) == 0 {
		return compactPrototypeStream{}
	}
	candidates := []compactPrototypeStream{
		compactPrototypeEncodeDict(values),
		compactPrototypeEncodeFront(values),
	}
	integers := make([]int64, len(values))
	allIntegers := true
	for i := range values {
		integers[i], allIntegers = CanonicalIntValue(values[i])
		if !allIntegers {
			break
		}
	}
	if allIntegers {
		candidates = append(candidates,
			compactPrototypeEncodeFOR(integers),
			compactPrototypeEncodeDelta(integers),
		)
	}
	dates := make([]int32, len(values))
	allDates := true
	for i := range values {
		dates[i], allDates = compactPrototypeDateOrdinal(values[i])
		if !allDates {
			break
		}
	}
	if allDates {
		candidates = append(candidates, compactPrototypeEncodeDate(dates))
	}
	if prefixInt, ok := compactPrototypeEncodePrefixInt(values); ok {
		candidates = append(candidates, prefixInt)
	}
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.bytes() < best.bytes() {
			best = candidate
		}
	}
	return best
}

func compactPrototypeEncodeDict(values [][]byte) compactPrototypeStream {
	unique := make(map[string]struct{}, min(len(values), 256))
	for _, value := range values {
		unique[string(value)] = struct{}{}
	}
	dict := make([][]byte, 0, len(unique))
	for value := range unique {
		dict = append(dict, []byte(value))
	}
	slices.SortFunc(dict, bytes.Compare)
	width := bits.Len(uint(max(0, len(dict)-1)))
	data := make([]byte, (len(values)*width+7)/8)
	ids := make(map[string]int, len(dict))
	for id := range dict {
		ids[string(dict[id])] = id
	}
	for i, value := range values {
		compactPrototypePutBits(data, i*width, width, uint64(ids[string(value)]))
	}
	return compactPrototypeStream{
		kind: compactPrototypeDict, width: uint8(width), count: len(values),
		data: data, dict: dict,
	}
}

func compactPrototypeEncodeFront(values [][]byte) compactPrototypeStream {
	data := make([]byte, 4*((len(values)+compactPrototypeRestart-1)/compactPrototypeRestart))
	var previous []byte
	for i, value := range values {
		if i%compactPrototypeRestart == 0 {
			binary.LittleEndian.PutUint32(data[(i/compactPrototypeRestart)*4:], uint32(len(data)))
			data = compactPrototypeAppendUvarint(data, uint64(len(value)))
			data = append(data, value...)
		} else {
			prefix := compactPrototypePrefix(previous, value)
			suffix := len(value) - prefix
			if prefix < 15 && suffix < 15 {
				data = append(data, byte(prefix<<4|suffix))
			} else {
				data = append(data, 0xff)
				data = compactPrototypeAppendUvarint(data, uint64(prefix))
				data = compactPrototypeAppendUvarint(data, uint64(suffix))
			}
			data = append(data, value[prefix:]...)
		}
		previous = value
	}
	return compactPrototypeStream{kind: compactPrototypeFront, count: len(values), data: data}
}

func compactPrototypeEncodeDate(values []int32) compactPrototypeStream {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len32(uint32(hi - lo))
	data := make([]byte, 4+(len(values)*width+7)/8)
	binary.LittleEndian.PutUint32(data, uint32(lo))
	for i, value := range values {
		compactPrototypePutBits(data[4:], i*width, width, uint64(value-lo))
	}
	return compactPrototypeStream{
		kind: compactPrototypeDate, width: uint8(width), count: len(values), data: data,
	}
}

func compactPrototypeEncodePrefixInt(values [][]byte) (compactPrototypeStream, bool) {
	if len(values) == 0 {
		return compactPrototypeStream{}, false
	}
	type parsed struct {
		prefix, suffix []byte
		value          uint64
		width          int
		canonical      bool
	}
	parse := func(src []byte) (parsed, bool) {
		start := 0
		for start < len(src) && (src[start] < '0' || src[start] > '9') {
			start++
		}
		if start == len(src) {
			return parsed{}, false
		}
		end := start
		for end < len(src) && src[end] >= '0' && src[end] <= '9' {
			end++
		}
		for at := end; at < len(src); at++ {
			if src[at] >= '0' && src[at] <= '9' {
				return parsed{}, false
			}
		}
		value, err := strconv.ParseUint(string(src[start:end]), 10, 63)
		if err != nil {
			return parsed{}, false
		}
		width := end - start
		return parsed{
			prefix: src[:start], suffix: src[end:], value: value, width: width,
			canonical: width == 1 || src[start] != '0',
		}, true
	}
	first, ok := parse(values[0])
	if !ok {
		return compactPrototypeStream{}, false
	}
	parsedValues := make([]uint64, len(values))
	parsedValues[0] = first.value
	allCanonical := first.canonical
	fixedWidth := true
	for i := 1; i < len(values); i++ {
		value, ok := parse(values[i])
		if !ok || !bytes.Equal(value.prefix, first.prefix) ||
			!bytes.Equal(value.suffix, first.suffix) {
			return compactPrototypeStream{}, false
		}
		parsedValues[i] = value.value
		allCanonical = allCanonical && value.canonical
		fixedWidth = fixedWidth && value.width == first.width
	}
	if !allCanonical && !fixedWidth {
		return compactPrototypeStream{}, false
	}
	data := make([]byte, 10, 10+len(values))
	if fixedWidth {
		data[0] = 1
		data[1] = byte(first.width)
	}
	binary.LittleEndian.PutUint64(data[2:], first.value)
	previous := first.value
	for _, value := range parsedValues[1:] {
		delta := int64(value - previous)
		data = compactPrototypeAppendUvarint(data, uint64(delta<<1)^uint64(delta>>63))
		previous = value
	}
	return compactPrototypeStream{
		kind: compactPrototypePrefixInt, count: len(values), data: data,
		dict: [][]byte{append([]byte(nil), first.prefix...), append([]byte(nil), first.suffix...)},
	}, true
}

func compactPrototypeDateOrdinal(value []byte) (int32, bool) {
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
	days := compactPrototypeDaysBeforeYear(year)
	for m := 1; m < month; m++ {
		days += monthDays[m-1]
		if m == 2 && leap {
			days++
		}
	}
	return int32(days + day - 1), true
}

func compactPrototypeDaysBeforeYear(year int) int {
	return year*365 + (year+3)/4 - (year+99)/100 + (year+399)/400
}

func compactPrototypeAppendDate(dst []byte, ordinal int32) []byte {
	dayNumber := int(ordinal)
	lo, hi := 0, 10_000
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if compactPrototypeDaysBeforeYear(mid) <= dayNumber {
			lo = mid
		} else {
			hi = mid
		}
	}
	year := lo
	dayOfYear := dayNumber - compactPrototypeDaysBeforeYear(year)
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

func compactPrototypeValidateStream(t testing.TB, stream compactPrototypeStream, values [][]byte) {
	t.Helper()
	if stream.count != len(values) {
		t.Fatalf("compact stream count=%d want=%d", stream.count, len(values))
	}
	scratch := make([]byte, 0, 256)
	previous := make([]byte, 0, 256)
	cursor := 0
	var integer int64
	var unsigned uint64
	switch stream.kind {
	case compactPrototypeFront:
		cursor = 4 * ((stream.count + compactPrototypeRestart - 1) / compactPrototypeRestart)
	case compactPrototypeDelta:
		if len(stream.data) < 8 {
			t.Fatal("compact delta header")
		}
		integer = int64(binary.LittleEndian.Uint64(stream.data))
		cursor = 8
	case compactPrototypePrefixInt:
		if len(stream.data) < 10 || len(stream.dict) != 2 {
			t.Fatal("compact prefix-int header")
		}
		unsigned = binary.LittleEndian.Uint64(stream.data[2:])
		cursor = 10
	}
	for i, want := range values {
		scratch = scratch[:0]
		switch stream.kind {
		case compactPrototypeDict:
			id := int(compactPrototypeReadBits(stream.data, i*int(stream.width), int(stream.width)))
			if stream.width == 0 {
				id = 0
			}
			if id >= len(stream.dict) {
				t.Fatalf("compact dictionary id=%d", id)
			}
			scratch = append(scratch, stream.dict[id]...)
		case compactPrototypeFront:
			if i%compactPrototypeRestart == 0 {
				length, n, ok := compactPrototypeReadUvarint(stream.data[cursor:])
				if !ok || length > uint64(len(stream.data)-cursor-n) {
					t.Fatal("compact front restart")
				}
				cursor += n
				scratch = append(scratch, stream.data[cursor:cursor+int(length)]...)
				cursor += int(length)
			} else {
				if cursor >= len(stream.data) {
					t.Fatal("compact front tuple")
				}
				packed := stream.data[cursor]
				cursor++
				prefix, suffix := int(packed>>4), int(packed&15)
				if packed == 0xff {
					p, n, ok := compactPrototypeReadUvarint(stream.data[cursor:])
					if !ok {
						t.Fatal("compact front prefix")
					}
					cursor += n
					s, n, ok := compactPrototypeReadUvarint(stream.data[cursor:])
					if !ok {
						t.Fatal("compact front suffix")
					}
					cursor += n
					prefix, suffix = int(p), int(s)
				}
				if prefix > len(previous) || suffix > len(stream.data)-cursor {
					t.Fatal("compact front bounds")
				}
				scratch = append(scratch, previous[:prefix]...)
				scratch = append(scratch, stream.data[cursor:cursor+suffix]...)
				cursor += suffix
			}
		case compactPrototypeFOR:
			if len(stream.data) < 8 {
				t.Fatal("compact FOR header")
			}
			base := int64(binary.LittleEndian.Uint64(stream.data))
			value := base + int64(compactPrototypeReadBits(
				stream.data[8:], i*int(stream.width), int(stream.width),
			))
			scratch = AppendCanonicalInt(scratch, value)
		case compactPrototypeDelta:
			if i != 0 {
				u, n, ok := compactPrototypeReadUvarint(stream.data[cursor:])
				if !ok {
					t.Fatal("compact delta value")
				}
				cursor += n
				integer += int64(u>>1) ^ -int64(u&1)
			}
			scratch = AppendCanonicalInt(scratch, integer)
		case compactPrototypeDate:
			if len(stream.data) < 4 {
				t.Fatal("compact date header")
			}
			base := int32(binary.LittleEndian.Uint32(stream.data))
			value := base + int32(compactPrototypeReadBits(
				stream.data[4:], i*int(stream.width), int(stream.width),
			))
			scratch = compactPrototypeAppendDate(scratch, value)
		case compactPrototypePrefixInt:
			if i != 0 {
				u, n, ok := compactPrototypeReadUvarint(stream.data[cursor:])
				if !ok {
					t.Fatal("compact prefix-int delta")
				}
				cursor += n
				unsigned = uint64(int64(unsigned) + (int64(u>>1) ^ -int64(u&1)))
			}
			scratch = append(scratch, stream.dict[0]...)
			start := len(scratch)
			scratch = strconv.AppendUint(scratch, unsigned, 10)
			if stream.data[0] != 0 {
				width := int(stream.data[1])
				digits := len(scratch) - start
				if digits < width {
					scratch = append(scratch, make([]byte, width-digits)...)
					copy(scratch[start+width-digits:], scratch[start:start+digits])
					for at := start; at < start+width-digits; at++ {
						scratch[at] = '0'
					}
				}
			}
			scratch = append(scratch, stream.dict[1]...)
		default:
			t.Fatalf("compact codec kind=%d", stream.kind)
		}
		if !bytes.Equal(scratch, want) {
			t.Fatalf("compact stream row=%d got=%q want=%q", i, scratch, want)
		}
		previous = append(previous[:0], scratch...)
	}
}

func compactPrototypeEncodeFOR(values []int64) compactPrototypeStream {
	lo, hi := values[0], values[0]
	for _, value := range values[1:] {
		lo = min(lo, value)
		hi = max(hi, value)
	}
	width := bits.Len64(uint64(hi - lo))
	data := make([]byte, 8+(len(values)*width+7)/8)
	binary.LittleEndian.PutUint64(data, uint64(lo))
	for i, value := range values {
		compactPrototypePutBits(data[8:], i*width, width, uint64(value-lo))
	}
	return compactPrototypeStream{
		kind: compactPrototypeFOR, width: uint8(width), count: len(values), data: data,
	}
}

func compactPrototypeEncodeDelta(values []int64) compactPrototypeStream {
	data := make([]byte, 8, 8+len(values))
	binary.LittleEndian.PutUint64(data, uint64(values[0]))
	previous := values[0]
	for _, value := range values[1:] {
		delta := value - previous
		data = compactPrototypeAppendUvarint(data, uint64(delta<<1)^uint64(delta>>63))
		previous = value
	}
	return compactPrototypeStream{kind: compactPrototypeDelta, count: len(values), data: data}
}

func compactPrototypePrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func compactPrototypeAppendUvarint(dst []byte, value uint64) []byte {
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func compactPrototypeUvarintLen(value uint64) int {
	n := 1
	for value >= 0x80 {
		n++
		value >>= 7
	}
	return n
}

func compactPrototypeReadUvarint(src []byte) (uint64, int, bool) {
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

func compactPrototypePutBits(dst []byte, bit, width int, value uint64) {
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

func compactPrototypeReadBits(src []byte, bit, width int) uint64 {
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

func TestCompactStreamPrototypeCompetitiveGate(t *testing.T) {
	if testing.Short() {
		t.Skip("compact prototype encodes both 100k competitive corpora")
	}
	for _, high := range []bool{false, true} {
		stripes, encoded, physical, stats := buildCompactPrototype(t, 100_000, high)
		matched := 0
		for i := range stripes {
			matched += stripes[i].countCountry([]byte(`"PT"`))
		}
		if matched != 945 {
			t.Fatalf("high=%v matched=%d, want 945", high, matched)
		}
		productionMatched := 0
		for i := range stripes {
			productionMatched += stripes[i].countCountryProduction([]byte(`"PT"`))
		}
		if productionMatched != 945 {
			t.Fatalf("high=%v production matched=%d, want 945", high, productionMatched)
		}
		if !high && encoded*2 > 2_713_077 {
			t.Fatalf("low-cardinality compact bytes=%d do not beat ClickHouse 2x", encoded)
		}
		if !high && physical*2 > 2_713_077 {
			t.Fatalf("low-cardinality physical compact bytes=%d do not beat ClickHouse 2x", physical)
		}
		t.Logf("high=%v stripes=%d encoded=%d bytes %.2f B/doc", high, len(stripes), encoded, float64(encoded)/100_000)
		t.Logf("high=%v production physical=%d bytes %.2f B/doc", high, physical, float64(physical)/100_000)
		t.Logf("high=%v fixed=%.2f keys=%.2f shapes=%.2f templates=%.2f dict=%.2f front=%.2f FOR=%.2f delta=%.2f date=%.2f prefix-int=%.2f B/doc",
			high, float64(stats.fixed)/100_000, float64(stats.keys)/100_000,
			float64(stats.shapeCodes)/100_000, float64(stats.templates)/100_000,
			float64(stats.codecBytes[compactPrototypeDict])/100_000,
			float64(stats.codecBytes[compactPrototypeFront])/100_000,
			float64(stats.codecBytes[compactPrototypeFOR])/100_000,
			float64(stats.codecBytes[compactPrototypeDelta])/100_000,
			float64(stats.codecBytes[compactPrototypeDate])/100_000,
			float64(stats.codecBytes[compactPrototypePrefixInt])/100_000)
		t.Logf("high=%v production streams: keys=%.2f dict=%.2f front=%.2f FOR=%.2f delta=%.2f packed-delta=%.2f date=%.2f prefix-int=%.2f B/doc",
			high, float64(stats.productionKeyBytes)/100_000,
			float64(stats.productionCodecBytes[compactStreamDictionary])/100_000,
			float64(stats.productionCodecBytes[compactStreamFront])/100_000,
			float64(stats.productionCodecBytes[compactStreamFOR])/100_000,
			float64(stats.productionCodecBytes[compactStreamDelta])/100_000,
			float64(stats.productionCodecBytes[compactStreamDeltaPack])/100_000,
			float64(stats.productionCodecBytes[compactStreamDate])/100_000,
			float64(stats.productionCodecBytes[compactStreamPrefixInt])/100_000)
		for hole := range stats.holeBytes {
			if stats.holeValues[hole] != 0 {
				t.Logf("high=%v hole[%d]=%.2f B/doc values=%d", high, hole,
					float64(stats.holeBytes[hole])/100_000, stats.holeValues[hole])
			}
		}
	}
}

func BenchmarkCompactStreamPrototypeCountryScan(b *testing.B) {
	stripes, _, physical, _ := buildCompactPrototype(b, 100_000, false)
	b.ReportMetric(float64(physical)/100_000, "B/doc")
	b.ReportAllocs()
	needle := []byte(`"PT"`)
	b.ResetTimer()
	for range b.N {
		matched := 0
		for i := range stripes {
			matched += stripes[i].countCountryProduction(needle)
		}
		if matched != 945 {
			b.Fatalf("matched=%d", matched)
		}
	}
}
