package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"testing"
)

type indexTermLeafFixture struct {
	terms    []IndexTermLeafTerm
	live     map[uint32]*[TermPostingTileChunks]uint64
	expected map[string]map[uint32][TermPostingTileChunks]uint64
}

var indexTermLeafTestSink uint64

func TestIndexTermLeafExactRangeAndAdaptivePostings(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 48, 1)
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexTermLeaf(encoded, indexTermLeafTestStoreID(), fixture.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != len(fixture.terms) {
		t.Fatalf("Len = %d, want %d", view.Len(), len(fixture.terms))
	}
	if view.PostingLen() != len(fixture.terms) {
		t.Fatalf("PostingLen = %d, want %d", view.PostingLen(), len(fixture.terms))
	}
	if view.DictionaryLen() == 0 {
		t.Fatal("repeated out-of-line dense payload was not dictionary encoded")
	}

	for i := range fixture.terms {
		key := fixture.terms[i].Key.Canonical
		match, ok := view.Lookup(key)
		if !ok {
			t.Fatalf("Lookup(%x) missed", key)
		}
		assertIndexTermLeafMatch(t, key, match, fixture.expected[string(key)])
	}
	missing := mustIndexTermLeafKey(t, "term/common/9999")
	if _, ok := view.Lookup(missing.Canonical); ok {
		t.Fatal("missing key matched")
	}

	iterator := view.Ordered()
	for i := 0; i < len(fixture.terms); i++ {
		key, match, ok := iterator.Next()
		if !ok {
			t.Fatalf("ordered iteration stopped at %d", i)
		}
		want := fixture.terms[i].Key.Canonical
		if !bytes.Equal(key, want) {
			t.Fatalf("ordered key[%d] = %x, want %x", i, key, want)
		}
		assertIndexTermLeafMatch(t, key, match, fixture.expected[string(key)])
	}
	if _, _, ok := iterator.Next(); ok {
		t.Fatal("ordered iterator exceeded leaf")
	}

	lower := fixture.terms[9].Key.Canonical
	upper := fixture.terms[23].Key.Canonical
	ranged := view.Range(lower, upper)
	for i := 9; i < 23; i++ {
		key, _, ok := ranged.Next()
		if !ok || !bytes.Equal(key, fixture.terms[i].Key.Canonical) {
			t.Fatalf("range[%d] = %x, %t", i, key, ok)
		}
	}
	if _, _, ok := ranged.Next(); ok {
		t.Fatal("range included upper bound")
	}
}

func TestIndexTermLeafTermAggregateAndTileDeltas(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 9, 4)
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexTermLeaf(encoded, indexTermLeafTestStoreID(), fixture.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if view.PostingLen() != len(fixture.terms)*4 {
		t.Fatalf("PostingLen = %d", view.PostingLen())
	}
	for i := range fixture.terms {
		key := fixture.terms[i].Key.Canonical
		match, ok := view.Lookup(key)
		if !ok || match.Len() != 4 {
			t.Fatalf("aggregate[%d] = (%d,%t)", i, match.Len(), ok)
		}
		assertIndexTermLeafMatch(t, key, match, fixture.expected[string(key)])
	}
}

func TestIndexTermLeafDeterministicDictionaryOnlyWhenSmaller(t *testing.T) {
	repeated := makeIndexTermLeafFixture(t, 16, 1)
	first, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), repeated.terms)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AppendIndexTermLeaf(
		[]byte("prefix"), indexTermLeafTestStoreID(), repeated.terms,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second[len("prefix"):]) {
		t.Fatal("encoding is not deterministic")
	}
	view, err := OpenIndexTermLeaf(first, indexTermLeafTestStoreID(), repeated.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if view.DictionaryLen() == 0 {
		t.Fatal("profitable repeated payload not shared")
	}

	unique := makeIndexTermLeafFixture(t, 6, 1)
	// Give every adaptive payload a distinct bit pattern while retaining more
	// than two non-empty chunks.
	for i := range unique.terms {
		var posting [TermPostingTileChunks]uint64
		var live [TermPostingTileChunks]uint64
		for chunk := range posting {
			live[chunk] = ^uint64(0)
			posting[chunk] = uint64(0x0101010101010101) << uint(i%7)
		}
		tileID := unique.terms[i].Postings[0].Posting.TileID
		unique.terms[i].Postings[0] = buildIndexTermLeafPosting(t, tileID, &posting, &live)
		unique.live[tileID] = unique.terms[i].Postings[0].Live
		unique.expected[string(unique.terms[i].Key.Canonical)] =
			map[uint32][TermPostingTileChunks]uint64{tileID: posting}
	}
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), unique.terms)
	if err != nil {
		t.Fatal(err)
	}
	view, err = OpenIndexTermLeaf(encoded, indexTermLeafTestStoreID(), unique.lookup)
	if err != nil {
		t.Fatal(err)
	}
	if view.DictionaryLen() != 0 {
		t.Fatalf("unique payload dictionary = %d, want zero", view.DictionaryLen())
	}
}

func TestIndexTermLeafRejectsInvalidBuilderInput(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 3, 1)
	dst := []byte("sentinel")
	badRoute := fixture.terms[0]
	badRoute.Key.RouteHash ^= 1
	cases := []struct {
		name  string
		terms []IndexTermLeafTerm
	}{
		{name: "empty"},
		{name: "duplicate", terms: []IndexTermLeafTerm{fixture.terms[0], fixture.terms[0]}},
		{name: "bad-route", terms: []IndexTermLeafTerm{badRoute}},
		{name: "bad-key", terms: []IndexTermLeafTerm{{
			Key:      IndexTermKeyRecord{Canonical: []byte("not canonical")},
			Postings: fixture.terms[0].Postings,
		}}},
		{name: "no-posting", terms: []IndexTermLeafTerm{{
			Key: fixture.terms[0].Key,
		}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := AppendIndexTermLeaf(dst, indexTermLeafTestStoreID(), test.terms)
			if err == nil || !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("error = %v", err)
			}
			if !bytes.Equal(got, dst) {
				t.Fatalf("failure changed destination: %q", got)
			}
		})
	}
}

func TestIndexTermLeafCorruptionAdmission(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 24, 1)
	good, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIndexTermLeaf(
		good, indexTermLeafTestStoreID(), fixture.lookup,
	); err != nil {
		t.Fatal(err)
	}
	mutate := func(name string, fn func([]byte)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			damaged := append([]byte(nil), good...)
			fn(damaged)
			if name != "checksum" {
				binary.LittleEndian.PutUint32(
					damaged[28:32], indexTermLeafChecksum(damaged),
				)
			}
			if _, err := OpenIndexTermLeaf(
				damaged, indexTermLeafTestStoreID(), fixture.lookup,
			); !errors.Is(err, ErrIndexTermLeafCorrupt) {
				t.Fatalf("Open error = %v", err)
			}
		})
	}
	mutate("checksum", func(encoded []byte) {
		encoded[len(encoded)-1] ^= 1
	})
	mutate("reserved", func(encoded []byte) {
		encoded[6] = 1
	})
	mutate("section", func(encoded []byte) {
		binary.LittleEndian.PutUint16(encoded[18:20], indexTermLeafHeaderBytes)
	})
	mutate("equality-index", func(encoded []byte) {
		terms := binary.LittleEndian.Uint16(encoded[12:14])
		equalityAt := indexTermLeafHeaderBytes +
			int(terms)*indexTermLeafDescriptorBytes
		binary.LittleEndian.PutUint16(encoded[equalityAt:equalityAt+2], terms)
	})
	mutate("restart-prefix", func(encoded []byte) {
		record := indexTermLeafHeaderBytes +
			IndexTermLeafRestartInterval*indexTermLeafDescriptorBytes
		binary.LittleEndian.PutUint16(encoded[record+6:record+8], 1)
	})
	mutate("term-order", func(encoded []byte) {
		record := indexTermLeafHeaderBytes + indexTermLeafDescriptorBytes
		suffix := int(binary.LittleEndian.Uint16(encoded[record : record+2]))
		keyAt := int(binary.LittleEndian.Uint16(encoded[18:20]))
		encoded[keyAt+suffix] = 0
	})
	mutate("posting-kind", func(encoded []byte) {
		postingAt := int(binary.LittleEndian.Uint16(encoded[20:22]))
		encoded[postingAt] = 0xff
	})
	mutate("dictionary-reserved", func(encoded []byte) {
		dictionaryAt := int(binary.LittleEndian.Uint16(encoded[22:24]))
		if binary.LittleEndian.Uint16(encoded[24:26]) == 0 {
			t.Fatal("fixture must contain a dictionary")
		}
		encoded[dictionaryAt+7] = 1
	})
	mutate("noncanonical-varint", func(encoded []byte) {
		postingAt := int(binary.LittleEndian.Uint16(encoded[20:22]))
		encoded[postingAt+1] = 0x80
	})
	mutate("direct-dead-slot", func(encoded []byte) {
		postingAt := int(binary.LittleEndian.Uint16(encoded[20:22]))
		// Fixture term zero is a singleton: move it outside the tile.
		binary.LittleEndian.PutUint16(
			encoded[postingAt+2:postingAt+4], TermPostingTileRows,
		)
	})
	if len(good) > IndexTermLeafMaxBytes {
		t.Fatal("fixture unexpectedly exceeds leaf bound")
	}
	if _, err := OpenIndexTermLeaf(
		good[:len(good)-1], indexTermLeafTestStoreID(), fixture.lookup,
	); !errors.Is(err, ErrIndexTermLeafCorrupt) {
		t.Fatalf("truncation error = %v", err)
	}
	if _, err := OpenIndexTermLeaf(good, indexTermLeafTestStoreID(), func(uint32) *[TermPostingTileChunks]uint64 {
		return nil
	}); !errors.Is(err, ErrIndexTermLeafCorrupt) {
		t.Fatalf("missing live tile error = %v", err)
	}
	wrongStoreID := indexTermLeafTestStoreID()
	wrongStoreID[15] ^= 1
	if _, err := OpenIndexTermLeaf(good, wrongStoreID, fixture.lookup); !errors.Is(err, ErrIndexTermLeafCorrupt) {
		t.Fatalf("wrong StoreID error = %v", err)
	}
}

func TestIndexTermLeafLongCanonicalKeys(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 2, 1)
	for i := range fixture.terms {
		value := bytes.Repeat([]byte{'x'}, 700+i)
		key := mustIndexTermLeafKey(t, string(value))
		delete(fixture.expected, string(fixture.terms[i].Key.Canonical))
		fixture.terms[i].Key = key
		fixture.expected[string(key.Canonical)] = map[uint32][TermPostingTileChunks]uint64{
			fixture.terms[i].Postings[0].Posting.TileID: func() [TermPostingTileChunks]uint64 {
				var posting, live [TermPostingTileChunks]uint64
				makeIndexTermLeafPattern(i%6, &posting, &live)
				return posting
			}(),
		}
	}
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexTermLeaf(encoded, indexTermLeafTestStoreID(), fixture.lookup)
	if err != nil {
		t.Fatal(err)
	}
	for i := range fixture.terms {
		if _, ok := view.LookupRecord(fixture.terms[i].Key); !ok {
			t.Fatalf("long key %d missed", i)
		}
	}
}

func TestIndexTermLeafGlobalDirectColumn(t *testing.T) {
	for _, pattern := range []int{0, 1} {
		t.Run(indexTermLeafPatternName(pattern), func(t *testing.T) {
			fixture := makeIndexTermLeafPatternFixture(t, 96, 1, pattern)
			encoded, err := AppendIndexTermLeaf(
				nil, indexTermLeafTestStoreID(), fixture.terms,
			)
			if err != nil {
				t.Fatal(err)
			}
			view, err := OpenIndexTermLeaf(
				encoded, indexTermLeafTestStoreID(), fixture.lookup,
			)
			if err != nil {
				t.Fatal(err)
			}
			block, ok := view.GlobalDirectBlock()
			if !ok || block.Len() != len(fixture.terms) {
				t.Fatalf("global block = (%d,%t)", block.Len(), ok)
			}
			for i := range fixture.terms {
				match, ok := view.LookupRecord(fixture.terms[i].Key)
				if !ok {
					t.Fatalf("term %d missed", i)
				}
				assertIndexTermLeafMatch(
					t, fixture.terms[i].Key.Canonical, match,
					fixture.expected[string(fixture.terms[i].Key.Canonical)],
				)
			}
			if allocs := testing.AllocsPerRun(1000, func() {
				block, ok := view.GlobalDirectBlock()
				if !ok {
					panic("global block")
				}
				switch pattern {
				case 0:
					_, row, count, ok := block.SingletonRun()
					if !ok {
						panic("global singleton run")
					}
					indexTermLeafTestSink += uint64(row) * uint64(count)
				case 1:
					_, mask, count, ok := block.OneMaskRun()
					if !ok || mask.Chunk != 3 {
						panic("global one-mask run")
					}
					indexTermLeafTestSink += mask.Bits * uint64(count)
				}
			}); allocs != 0 {
				t.Fatalf("global direct allocations = %v", allocs)
			}
		})
	}
}

func TestIndexTermLeafOneMaskWordsRejectsUnalignedLeaf(t *testing.T) {
	fixture := makeIndexTermLeafPatternFixture(t, 96, 1, 1)
	for i := range fixture.terms {
		var posting, live [TermPostingTileChunks]uint64
		for chunk := range live {
			live[chunk] = ^uint64(0)
		}
		posting[3] = uint64(3) << uint(i%62)
		tileID := fixture.terms[i].Postings[0].Posting.TileID
		input := buildIndexTermLeafPosting(t, tileID, &posting, &live)
		fixture.terms[i].Postings[0] = input
		fixture.live[tileID] = input.Live
		fixture.expected[string(fixture.terms[i].Key.Canonical)] =
			map[uint32][TermPostingTileChunks]uint64{tileID: posting}
	}
	encoded, err := AppendIndexTermLeaf(
		nil, indexTermLeafTestStoreID(), fixture.terms,
	)
	if err != nil {
		t.Fatal(err)
	}
	aligned, err := OpenIndexTermLeaf(
		encoded, indexTermLeafTestStoreID(), fixture.lookup,
	)
	if err != nil {
		t.Fatal(err)
	}
	block, ok := aligned.GlobalDirectBlock()
	if !ok {
		t.Fatal("aligned global direct block missed")
	}
	if _, _, _, ok := block.OneMaskWords(); !ok {
		t.Fatal("aligned native mask words missed")
	}

	storage := make([]byte, len(encoded)+1)
	copy(storage[1:], encoded)
	unaligned, err := OpenIndexTermLeaf(
		storage[1:], indexTermLeafTestStoreID(), fixture.lookup,
	)
	if err != nil {
		t.Fatal(err)
	}
	block, ok = unaligned.GlobalDirectBlock()
	if !ok {
		t.Fatal("unaligned global direct block missed")
	}
	if _, _, masks, ok := block.OneMaskBits(); !ok || len(masks) != 96*8 {
		t.Fatalf("portable mask bits = (%d,%t)", len(masks), ok)
	}
	if _, _, _, ok := block.OneMaskWords(); ok {
		t.Fatal("unaligned native mask words accepted")
	}
}

func TestIndexTermLeafAdmittedOperationsAllocateZero(t *testing.T) {
	fixture := makeIndexTermLeafFixture(t, 64, 3)
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenIndexTermLeaf(encoded, indexTermLeafTestStoreID(), fixture.lookup)
	if err != nil {
		t.Fatal(err)
	}
	key := fixture.terms[37].Key
	if allocs := testing.AllocsPerRun(1000, func() {
		match, ok := view.LookupRecord(key)
		if !ok {
			panic("lookup")
		}
		iterator := match.MaskIterator()
		for {
			_, _, ok := iterator.Next()
			if !ok {
				break
			}
		}
	}); allocs != 0 {
		t.Fatalf("admitted equality allocations = %v", allocs)
	}
	directFixture := makeIndexTermLeafPatternFixture(t, 1, 64, 0)
	directEncoded, err := AppendIndexTermLeaf(
		nil, indexTermLeafTestStoreID(), directFixture.terms,
	)
	if err != nil {
		t.Fatal(err)
	}
	directView, err := OpenIndexTermLeaf(
		directEncoded, indexTermLeafTestStoreID(), directFixture.lookup,
	)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		block, ok := directView.LookupDirectBlock(directFixture.terms[0].Key)
		if !ok || block.Len() != 64 {
			panic("direct block")
		}
		iterator := block.Iterator()
		for {
			if _, _, ok := iterator.Next(); !ok {
				break
			}
		}
	}); allocs != 0 {
		t.Fatalf("admitted direct-block allocations = %v", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		iterator := view.Range(
			fixture.terms[17].Key.Canonical,
			fixture.terms[29].Key.Canonical,
		)
		for {
			key, _, ok := iterator.Next()
			if !ok {
				break
			}
			if len(key) == 0 {
				panic("key")
			}
		}
	}); allocs != 0 {
		t.Fatalf("admitted range allocations = %v", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		iterator := view.Ordered()
		for {
			if _, _, ok := iterator.Next(); !ok {
				break
			}
		}
	}); allocs != 0 {
		t.Fatalf("admitted ordered allocations = %v", allocs)
	}
}

func FuzzOpenIndexTermLeaf(f *testing.F) {
	fixture := makeIndexTermLeafFixture(f, 12, 2)
	encoded, err := AppendIndexTermLeaf(nil, indexTermLeafTestStoreID(), fixture.terms)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, candidate []byte) {
		view, err := OpenIndexTermLeaf(
			candidate, indexTermLeafTestStoreID(), fixture.lookup,
		)
		if err != nil {
			return
		}
		iterator := view.Ordered()
		previous := []byte(nil)
		terms := 0
		for {
			key, match, ok := iterator.Next()
			if !ok {
				break
			}
			if !ValidIndexTermKey(key) ||
				previous != nil && bytes.Compare(previous, key) >= 0 ||
				match.Len() == 0 {
				t.Fatalf("admitted invalid term %x", key)
			}
			previous = append(previous[:0], key...)
			terms++
		}
		if terms != view.Len() {
			t.Fatalf("iteration count = %d, want %d", terms, view.Len())
		}
	})
}

func makeIndexTermLeafFixture(
	t testing.TB,
	termCount, postingsPerTerm int,
) indexTermLeafFixture {
	t.Helper()
	fixture := indexTermLeafFixture{
		terms:    make([]IndexTermLeafTerm, termCount),
		live:     make(map[uint32]*[TermPostingTileChunks]uint64),
		expected: make(map[string]map[uint32][TermPostingTileChunks]uint64),
	}
	tileID := uint32(7)
	for term := range termCount {
		key := mustIndexTermLeafKey(t, fmt.Sprintf("term/common/%04d", term))
		fixture.terms[term].Key = key
		fixture.terms[term].Postings =
			make([]IndexTermLeafPosting, postingsPerTerm)
		expected := make(map[uint32][TermPostingTileChunks]uint64)
		for postingIndex := range postingsPerTerm {
			var posting, live [TermPostingTileChunks]uint64
			makeIndexTermLeafPattern(term%6, &posting, &live)
			input := buildIndexTermLeafPosting(
				t, tileID, &posting, &live,
			)
			fixture.terms[term].Postings[postingIndex] = input
			fixture.live[tileID] = input.Live
			expected[tileID] = posting
			tileID += uint32(1 + postingIndex%2)
		}
		fixture.expected[string(key.Canonical)] = expected
	}
	return fixture
}

func mustIndexTermLeafKey(t testing.TB, value string) IndexTermKeyRecord {
	t.Helper()
	canonical, ok := AppendIndexTermKey(nil, []IndexTermComponent{{
		Kind: IndexTermString, Direction: IndexTermAscending,
		JSON: []byte(fmt.Sprintf("%q", value)),
	}})
	if !ok {
		t.Fatalf("canonical key %q rejected", value)
	}
	storeID := indexTermLeafTestStoreID()
	record, ok := OpenIndexTermKeyRecord(storeID, canonical)
	if !ok {
		t.Fatalf("canonical key %q did not reopen", value)
	}
	return record
}

func indexTermLeafTestStoreID() [16]byte {
	var storeID [16]byte
	copy(storeID[:], "term-leaf-test")
	return storeID
}

func buildIndexTermLeafPosting(
	t testing.TB,
	tileID uint32,
	posting, live *[TermPostingTileChunks]uint64,
) IndexTermLeafPosting {
	t.Helper()
	var component [TermPostingMaxPayloadBytes]byte
	record, n, err := BuildTermPosting(component[:], tileID, posting, live)
	if err != nil {
		t.Fatal(err)
	}
	result := IndexTermLeafPosting{
		Posting: record,
		Live:    new([TermPostingTileChunks]uint64),
	}
	*result.Live = *live
	if n != 0 {
		result.Component = append([]byte(nil), component[:n]...)
	}
	return result
}

func makeIndexTermLeafPattern(
	pattern int,
	posting, live *[TermPostingTileChunks]uint64,
) {
	for i := range live {
		live[i] = ^uint64(0)
	}
	switch pattern {
	case 0: // singleton/direct1
		posting[0] = 1 << 7
	case 1: // one wide mask/directN
		posting[3] = 0xf0f0f0f00f0f0f0f
	case 2: // run crossing many chunks
		for row := 11; row < 379; row++ {
			posting[row>>6] |= uint64(1) << uint(row&63)
		}
	case 3: // sparse, more than the direct-mask bound
		posting[1] = 1 << 1
		posting[17] = 1 << 9
		posting[33] = 1 << 17
		posting[61] = 1 << 25
	case 4: // dense repeated payload, dictionary candidate
		for chunk := range posting {
			posting[chunk] = 0x5555555555555555
		}
	case 5: // all live
		*posting = *live
	default:
		panic("unknown exact-term leaf pattern")
	}
}

func (f indexTermLeafFixture) lookup(
	tileID uint32,
) *[TermPostingTileChunks]uint64 {
	return f.live[tileID]
}

func assertIndexTermLeafMatch(
	t *testing.T,
	key []byte,
	match IndexTermLeafMatch,
	expected map[uint32][TermPostingTileChunks]uint64,
) {
	t.Helper()
	if match.Len() != len(expected) {
		t.Fatalf("match %x count = %d, want %d", key, match.Len(), len(expected))
	}
	seen := 0
	postings := match.Iterator()
	for {
		posting, ok := postings.Next()
		if !ok {
			break
		}
		want, ok := expected[posting.TileID()]
		if !ok {
			t.Fatalf("match %x unexpected tile %d", key, posting.TileID())
		}
		var got [TermPostingTileChunks]uint64
		rows := 0
		masks := posting.Iterator()
		for {
			mask, ok := masks.Next()
			if !ok {
				break
			}
			got[mask.Chunk] = mask.Bits
			rows += bits.OnesCount64(mask.Bits)
		}
		if got != want || rows != int(posting.Rows()) {
			t.Fatalf("match %x tile %d rows=%d/%d masks differ=%t",
				key, posting.TileID(), rows, posting.Rows(), got != want)
		}
		seen++
	}
	if seen != len(expected) {
		t.Fatalf("match %x saw %d postings, want %d", key, seen, len(expected))
	}
}
