package storeio

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func indexTerm(kind IndexTermKind, raw string) IndexTermComponent {
	return IndexTermComponent{
		Kind: kind, Direction: IndexTermAscending, JSON: []byte(raw),
	}
}

func mustIndexTermKey(t testing.TB, components ...IndexTermComponent) []byte {
	t.Helper()
	key, ok := AppendIndexTermKey(nil, components)
	if !ok {
		t.Fatalf("AppendIndexTermKey(%q) rejected", components)
	}
	if !ValidIndexTermKey(key) {
		t.Fatalf("AppendIndexTermKey(%q) emitted invalid key %x", components, key)
	}
	return key
}

func TestIndexTermSemanticCanonicalization(t *testing.T) {
	numberSpellings := []string{
		"1", "1.0", "1e0", "1.000e+0", "0.10e1",
	}
	wantNumber := mustIndexTermKey(t, indexTerm(IndexTermNumber, numberSpellings[0]))
	for _, spelling := range numberSpellings[1:] {
		got := mustIndexTermKey(t, indexTerm(IndexTermNumber, spelling))
		if !bytes.Equal(got, wantNumber) {
			t.Fatalf("number %q = %x, want canonical %x", spelling, got, wantNumber)
		}
	}
	zero := mustIndexTermKey(t, indexTerm(IndexTermNumber, "0"))
	for _, spelling := range []string{"-0", "0.0", "-0e999"} {
		got := mustIndexTermKey(t, indexTerm(IndexTermNumber, spelling))
		if !bytes.Equal(got, zero) {
			t.Fatalf("zero %q = %x, want %x", spelling, got, zero)
		}
	}

	stringSpellings := []string{
		`"A/😀"`,
		`"\u0041\/\ud83d\ude00"`,
		`"\u0041/\uD83D\uDE00"`,
	}
	wantString := mustIndexTermKey(t, indexTerm(IndexTermString, stringSpellings[0]))
	for _, spelling := range stringSpellings[1:] {
		got := mustIndexTermKey(t, indexTerm(IndexTermString, spelling))
		if !bytes.Equal(got, wantString) {
			t.Fatalf("string %q = %x, want decoded canonical %x", spelling, got, wantString)
		}
	}
}

func TestIndexTermTypeOrderAndCompoundBoundaries(t *testing.T) {
	ordered := [][]byte{
		mustIndexTermKey(t, indexTerm(IndexTermNull, "null")),
		mustIndexTermKey(t, indexTerm(IndexTermBool, "false")),
		mustIndexTermKey(t, indexTerm(IndexTermBool, "true")),
		mustIndexTermKey(t, indexTerm(IndexTermNumber, "-1")),
		mustIndexTermKey(t, indexTerm(IndexTermNumber, "0")),
		mustIndexTermKey(t, indexTerm(IndexTermNumber, "1")),
		mustIndexTermKey(t, indexTerm(IndexTermString, `""`)),
		mustIndexTermKey(t, indexTerm(IndexTermString, `"a"`)),
	}
	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(ordered[i-1], ordered[i]) >= 0 {
			t.Fatalf("key %d (%x) does not sort before %d (%x)", i-1, ordered[i-1], i, ordered[i])
		}
	}

	short := mustIndexTermKey(t, indexTerm(IndexTermString, `"tenant"`))
	long := mustIndexTermKey(t,
		indexTerm(IndexTermString, `"tenant"`),
		indexTerm(IndexTermNumber, "7"),
	)
	if bytes.HasPrefix(long, short) {
		t.Fatalf("complete tuple %x is a prefix of %x", short, long)
	}
	if bytes.Compare(short, long) >= 0 {
		t.Fatalf("short tuple %x does not sort before its extension %x", short, long)
	}

	left := mustIndexTermKey(t,
		indexTerm(IndexTermString, `"ab"`),
		indexTerm(IndexTermString, `"c"`),
	)
	right := mustIndexTermKey(t,
		indexTerm(IndexTermString, `"a"`),
		indexTerm(IndexTermString, `"bc"`),
	)
	if bytes.Equal(left, right) {
		t.Fatalf("compound boundaries collapsed: %x", left)
	}
}

func TestIndexTermRejectsInvalidWithoutPartialAppend(t *testing.T) {
	huge := make([]byte, IndexTermMaxKeyBytes+1)
	for i := range huge {
		huge[i] = '1'
	}
	wideString := `"` + strings.Repeat("x", IndexTermMaxKeyBytes/2) + `"`
	cases := []struct {
		name       string
		components []IndexTermComponent
	}{
		{name: "empty"},
		{name: "zero direction", components: []IndexTermComponent{{Kind: IndexTermNull, JSON: []byte("null")}}},
		{name: "unknown direction", components: []IndexTermComponent{{Kind: IndexTermNull, Direction: 2, JSON: []byte("null")}}},
		{name: "invalid kind", components: []IndexTermComponent{indexTerm(IndexTermInvalid, "null")}},
		{name: "array", components: []IndexTermComponent{indexTerm(IndexTermArray, "[]")}},
		{name: "object", components: []IndexTermComponent{indexTerm(IndexTermObject, "{}")}},
		{name: "kind mismatch", components: []IndexTermComponent{indexTerm(IndexTermNumber, `"1"`)}},
		{name: "null whitespace", components: []IndexTermComponent{indexTerm(IndexTermNull, "null ")}},
		{name: "bool case", components: []IndexTermComponent{indexTerm(IndexTermBool, "TRUE")}},
		{name: "malformed number", components: []IndexTermComponent{indexTerm(IndexTermNumber, "01")}},
		{name: "malformed string", components: []IndexTermComponent{indexTerm(IndexTermString, `"\ud800"`)}},
		{name: "container as string", components: []IndexTermComponent{indexTerm(IndexTermString, "{}")}},
		{name: "huge", components: []IndexTermComponent{{
			Kind: IndexTermNumber, Direction: IndexTermAscending, JSON: huge,
		}}},
		{name: "oversized compound", components: []IndexTermComponent{
			indexTerm(IndexTermString, wideString),
			indexTerm(IndexTermString, wideString),
		}},
		{name: "too many", components: []IndexTermComponent{
			indexTerm(IndexTermNull, "null"),
			indexTerm(IndexTermNull, "null"),
			indexTerm(IndexTermNull, "null"),
			indexTerm(IndexTermNull, "null"),
			indexTerm(IndexTermNull, "null"),
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			backing := make([]byte, 3, 64)
			copy(backing, "pre")
			before := append([]byte(nil), backing...)
			got, ok := AppendIndexTermKey(backing, test.components)
			if ok || len(got) != len(backing) || &got[0] != &backing[0] ||
				!bytes.Equal(got, before) {
				t.Fatalf("failure changed dst: got=(%x,%v), want %x", got, ok, before)
			}
		})
	}
}

func TestIndexTermKeyExactSizeBoundary(t *testing.T) {
	payload := strings.Repeat("x", IndexTermMaxKeyBytes-5)
	key := mustIndexTermKey(t, indexTerm(IndexTermString, `"`+payload+`"`))
	if len(key) != IndexTermMaxKeyBytes {
		t.Fatalf("boundary key size = %d, want %d", len(key), IndexTermMaxKeyBytes)
	}
	tooLarge := indexTerm(IndexTermString, `"`+payload+"x"+`"`)
	if got, ok := AppendIndexTermKey(nil, []IndexTermComponent{tooLarge}); ok || got != nil {
		t.Fatalf("oversized canonical key = (%x,%v), want (nil,false)", got, ok)
	}
}

func TestIndexTermKeyValidationRejectsMalformed(t *testing.T) {
	valid := mustIndexTermKey(t,
		indexTerm(IndexTermString, `"tenant"`),
		indexTerm(IndexTermNumber, "7"),
	)
	number := mustIndexTermKey(t, indexTerm(IndexTermNumber, "1"))
	nonCanonicalNumber := append([]byte(nil), number[:len(number)-2]...)
	nonCanonicalNumber = append(nonCanonicalNumber, 1)
	nonCanonicalNumber = append(nonCanonicalNumber, number[len(number)-2:]...)
	cases := [][]byte{
		nil,
		{indexTermKeyVersion, indexTermKeyTerminator},
		append([]byte{2}, valid[1:]...),
		append([]byte(nil), valid[:len(valid)-1]...),
		append(append([]byte(nil), valid...), 1),
		{indexTermKeyVersion, 0xff, indexTermKeyTerminator},
		{indexTermKeyVersion, 0x10, 0x10, 0x10, 0x10, 0x10, indexTermKeyTerminator},
		nonCanonicalNumber,
	}
	for _, key := range cases {
		if ValidIndexTermKey(key) {
			t.Fatalf("malformed key accepted: %x", key)
		}
		if _, ok := OpenIndexTermKeyRecord(testStoreID, key); ok {
			t.Fatalf("malformed key record accepted: %x", key)
		}
	}
}

func TestIndexTermRouteIsKeyedButCanonicalBytesAreIdentity(t *testing.T) {
	a := mustIndexTermKey(t, indexTerm(IndexTermString, `"alpha"`))
	b := mustIndexTermKey(t, indexTerm(IndexTermString, `"beta"`))
	firstStore := [16]byte{1, 2, 3, 4}
	secondStore := [16]byte{4, 3, 2, 1}
	firstHash := IndexTermRouteHash(firstStore, a)
	if second := IndexTermRouteHash(secondStore, a); second == firstHash {
		t.Fatalf("different StoreIDs produced the same test route %#x", firstHash)
	}

	record, ok := OpenIndexTermKeyRecord(firstStore, a)
	if !ok || !record.Matches(firstHash, a) {
		t.Fatalf("record did not match its canonical key: %+v", record)
	}
	// Force a routing collision. Identity still consults the complete key.
	collision := IndexTermKeyRecord{RouteHash: record.RouteHash, Canonical: b}
	if record.SameIdentity(collision) || record.Matches(record.RouteHash, b) {
		t.Fatal("forced hash collision merged distinct canonical terms")
	}
	equal := IndexTermKeyRecord{
		RouteHash: record.RouteHash,
		Canonical: append([]byte(nil), a...),
	}
	if !record.SameIdentity(equal) {
		t.Fatal("equal canonical bytes did not match")
	}
}

func TestIndexTermIntegerSpellingProperty(t *testing.T) {
	for value := -1000; value <= 1000; value++ {
		plain := strconv.Itoa(value)
		exponent := plain + "e0"
		decimal := plain + ".000"
		want := mustIndexTermKey(t, indexTerm(IndexTermNumber, plain))
		for _, spelling := range []string{exponent, decimal} {
			got := mustIndexTermKey(t, indexTerm(IndexTermNumber, spelling))
			if !bytes.Equal(got, want) {
				t.Fatalf("%q = %x, want %q = %x", spelling, got, plain, want)
			}
		}
	}
}

func TestIndexTermWarmAppendAllocations(t *testing.T) {
	components := []IndexTermComponent{
		indexTerm(IndexTermString, `"tenant\u0000west"`),
		indexTerm(IndexTermNumber, "-123456.7500e-2"),
		indexTerm(IndexTermBool, "true"),
	}
	dst := make([]byte, 0, 128)
	want, ok := AppendIndexTermKey(dst[:0], components)
	if !ok {
		t.Fatal("warm key rejected")
	}
	wantHash := IndexTermRouteHash(testStoreID, want)
	var sink []byte
	var hashSink uint64
	allocs := testing.AllocsPerRun(1000, func() {
		var encoded bool
		sink, encoded = AppendIndexTermKey(dst[:0], components)
		if !encoded {
			panic("AppendIndexTermKey failed")
		}
		hashSink = IndexTermRouteHash(testStoreID, sink)
	})
	if allocs != 0 {
		t.Fatalf("warm append/hash allocations = %g, want 0", allocs)
	}
	if !bytes.Equal(sink, want) || hashSink != wantHash {
		t.Fatal("warm append/hash changed output")
	}
}

func FuzzAppendIndexTermKey(f *testing.F) {
	f.Add(uint8(IndexTermNumber), uint8(IndexTermAscending), "1.00e0")
	f.Add(uint8(IndexTermString), uint8(IndexTermAscending), `"\u0041"`)
	f.Add(uint8(IndexTermArray), uint8(IndexTermAscending), "[]")
	f.Add(uint8(IndexTermBool), uint8(0), "true")
	f.Fuzz(func(t *testing.T, kind, direction uint8, raw string) {
		if len(raw) > IndexTermMaxKeyBytes+32 {
			t.Skip()
		}
		component := IndexTermComponent{
			Kind: IndexTermKind(kind), Direction: IndexTermDirection(direction),
			JSON: []byte(raw),
		}
		backing := make([]byte, 4, IndexTermMaxKeyBytes+16)
		copy(backing, "seed")
		before := append([]byte(nil), backing...)
		got, ok := AppendIndexTermKey(backing, []IndexTermComponent{component})
		if !ok {
			if len(got) != len(backing) || !bytes.Equal(got, before) {
				t.Fatalf("rejected append changed dst: %x => %x", before, got)
			}
			return
		}
		key := got[len(backing):]
		if !ValidIndexTermKey(key) {
			t.Fatalf("accepted input emitted invalid key: kind=%d direction=%d raw=%q key=%x",
				kind, direction, raw, key)
		}
	})
}

func BenchmarkAppendIndexTermKeyScalar(b *testing.B) {
	components := []IndexTermComponent{indexTerm(IndexTermString, `"customer\u0000west"`)}
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(components[0].JSON)))
	for b.Loop() {
		var ok bool
		dst, ok = AppendIndexTermKey(dst[:0], components)
		if !ok {
			b.Fatal("encoding failed")
		}
	}
}

func BenchmarkAppendIndexTermKeyCompoundAndRoute(b *testing.B) {
	components := []IndexTermComponent{
		indexTerm(IndexTermString, `"tenant-west"`),
		indexTerm(IndexTermNumber, "-123456.7500e-2"),
		indexTerm(IndexTermBool, "true"),
	}
	dst := make([]byte, 0, 128)
	var route uint64
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendIndexTermKey(dst[:0], components)
		if !ok {
			b.Fatal("encoding failed")
		}
		route = IndexTermRouteHash(testStoreID, dst)
	}
	_ = route
}
