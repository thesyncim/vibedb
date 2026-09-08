package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func compressedDictionaryTestValues(distinct, rows int) [][]byte {
	unique := make([][]byte, distinct)
	for id := range unique {
		token := bytes.Repeat([]byte(fmt.Sprintf("node-%02d/checkpoint/", id)), 8)
		unique[id] = append([]byte{'"'}, token...)
		unique[id] = append(unique[id], token...)
		unique[id] = append(unique[id], '"')
	}
	values := make([][]byte, rows)
	for row := range values {
		values[row] = unique[row%len(unique)]
	}
	return values
}

func TestCompactCompressedDictionaryRoundTrip(t *testing.T) {
	values := compressedDictionaryTestValues(8, 64)
	encoded := encodeCompactScalarStream(values)
	if encoded.kind != compactStreamCompressedDictionary {
		t.Fatalf("kind = %d, want compressed dictionary", encoded.kind)
	}
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range values {
		prefix := []byte(`{"value":`)
		got, ok := view.appendValue(prefix, row)
		if !ok || !bytes.Equal(got, append(append([]byte(nil), prefix...), want...)) {
			t.Fatalf("row %d append = %q, %v", row, got, ok)
		}
		length, peak, ok := compactProjectionValueLen(view, row)
		if !ok || length != len(want) || peak != len(want) {
			t.Fatalf("row %d projection length = %d/%d/%v, want %d", row, length, peak, ok, len(want))
		}
	}
	needle := values[3]
	matched, scratch, supported := view.countSpellingEqual(needle, make([]byte, 0, len(needle)))
	if !supported || matched != 8 || !bytes.Equal(scratch, needle) {
		t.Fatalf("count = %d/%v scratch=%q", matched, supported, scratch)
	}
	dst := make([]byte, 0, len(values[0])+16)
	if allocs := testing.AllocsPerRun(100, func() {
		var ok bool
		dst, ok = view.appendValue(dst[:0], 17)
		if !ok {
			panic("decode")
		}
	}); allocs != 0 {
		t.Fatalf("warm point allocations = %.2f", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := openCompactStream(binaryStream); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("validation allocations = %.2f", allocs)
	}
}

func TestCompactCompressedDictionaryRawEntry(t *testing.T) {
	value := []byte(`"raw dictionary spelling"`)
	encoded := compactStreamEncoding{
		kind: compactStreamCompressedDictionary, count: 2,
		dict: [][]byte{append([]byte{0}, value...)},
	}
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := openCompactStream(binaryStream)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := view.appendValue(nil, 1)
	if !ok || !bytes.Equal(got, value) {
		t.Fatalf("raw entry = %q, %v", got, ok)
	}
}

func TestCompactCompressedDictionaryRejectsMalformedEntries(t *testing.T) {
	values := compressedDictionaryTestValues(2, 8)
	encoded := encodeCompactScalarStream(values)
	binaryStream, err := encoded.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	dictStart := compactStreamHeader + 2*len(encoded.dict)
	tests := map[string]func([]byte){
		"tag": func(src []byte) { src[dictStart] = 2 },
		"decoded length": func(src []byte) {
			src[dictStart] = 1
			for at := dictStart + 1; at < dictStart+10; at++ {
				src[at] = 0xff
			}
		},
		"body": func(src []byte) { src[dictStart+3] ^= 0xff },
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			src := append([]byte(nil), binaryStream...)
			corrupt(src)
			if _, err := openCompactStream(src); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	oversizeEntry := append([]byte{1}, binary.AppendUvarint(nil, compactCompressedDictionaryMaxValueBytes+1)...)
	oversizeEntry = append(oversizeEntry, 0)
	oversize := compactStreamEncoding{
		kind: compactStreamCompressedDictionary, count: 1,
		dict: [][]byte{oversizeEntry},
	}
	src, err := oversize.appendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openCompactStream(src); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("oversize error = %v", err)
	}
}

func TestCompactCompressedDictionaryExcludesKeysAndLargeValues(t *testing.T) {
	values := compressedDictionaryTestValues(4, 64)
	var scratch compactStreamScratch
	keys := make([][]byte, len(values))
	for i := range values {
		keys[i] = append([]byte(nil), values[i]...)
	}
	if got := scratch.encodeKeys(keys); got.kind == compactStreamCompressedDictionary {
		t.Fatal("quoted opaque keys selected scalar compression")
	}
	if scratch.compressor != nil {
		t.Fatal("key planning allocated an LZ4 compressor")
	}
	large := bytes.Repeat([]byte("repeated-large-value"), compactCompressedDictionaryMaxValueBytes/20+2)
	large = append([]byte{'"'}, large...)
	large = append(large, '"')
	for i := range values {
		values[i] = large
	}
	if got := scratch.encode(values); got.kind == compactStreamCompressedDictionary {
		t.Fatal("value above decoded cap selected compression")
	}
	if scratch.compressor != nil {
		t.Fatal("large-value rejection allocated an LZ4 compressor")
	}
}

func TestCompactCompressedDictionaryWarmEncodeAllocations(t *testing.T) {
	accepted := compressedDictionaryTestValues(8, 64)
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_=+,.!?@#$%^&*()[]{}:;<>/|~"
	rejected := make([][]byte, 64)
	state := uint64(0xdecafbad12345678)
	for row := range rejected {
		value := make([]byte, 202)
		value[0], value[len(value)-1] = '"', '"'
		for at := 1; at < len(value)-1; at++ {
			state += 0x9e3779b97f4a7c15
			state = (state ^ state>>30) * 0xbf58476d1ce4e5b9
			state = (state ^ state>>27) * 0x94d049bb133111eb
			state ^= state >> 31
			value[at] = alphabet[state%uint64(len(alphabet))]
		}
		rejected[row] = value
	}
	for name, values := range map[string][][]byte{
		"accepted": accepted,
		"rejected": rejected,
	} {
		t.Run(name, func(t *testing.T) {
			var scratch compactStreamScratch
			first := scratch.encode(values)
			if name == "accepted" && first.kind != compactStreamCompressedDictionary {
				t.Fatalf("warmup kind = %d, want compressed", first.kind)
			}
			if name == "rejected" && first.kind == compactStreamCompressedDictionary {
				t.Fatal("incompressible warmup unexpectedly compressed")
			}
			if scratch.compressor == nil {
				t.Fatal("warmup did not exercise compression candidate")
			}
			if allocs := testing.AllocsPerRun(100, func() {
				encoded := scratch.encode(values)
				if (name == "accepted") != (encoded.kind == compactStreamCompressedDictionary) {
					panic("admission changed")
				}
			}); allocs != 0 {
				t.Fatalf("warm encode allocations = %.2f", allocs)
			}
		})
	}
}

func TestCompactCompressedDictionaryAdmissionLeavesOrdinaryValuesAlone(t *testing.T) {
	values := make([][]byte, 64)
	for row := range values {
		values[row] = []byte(fmt.Sprintf(`"user-%08d"`, row))
	}
	if got := encodeCompactScalarStream(values); got.kind == compactStreamCompressedDictionary {
		t.Fatal("ordinary short strings selected compression")
	}
}
