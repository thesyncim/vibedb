package store

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The interner's contract has three load-bearing edges: identifiers are dense
// and grouped by decoded content (escaped and raw spellings of one key must
// share an identifier, or engine-side grouping breaks), returned key bytes
// stay valid and immobile as the interner grows, and the tape walk that reuses
// enrichment hashes must produce identifier sequences identical to a naive
// per-key rehash through the public iteration API. These tests close each edge
// over the adversarial key-hash corpus.

// TestKeyInternerDenseIDs pins the basic contract on a zero-value interner:
// first appearances assign 0, 1, 2, ... in order, repeats and alternate
// spellings return the existing identifier, Lookup never inserts, and Key
// round-trips every identifier.
func TestKeyInternerDenseIDs(t *testing.T) {
	var in KeyInterner
	if in.Len() != 0 {
		t.Fatalf("zero interner Len = %d, want 0", in.Len())
	}
	if _, ok := in.Lookup([]byte("absent")); ok {
		t.Fatal("Lookup on empty interner reported a hit")
	}
	keys := []string{"", "a", "ab", "abc", "abcdefgh", "abcdefghi", "héllo", "😀", "key with spaces", "\x00\x01"}
	for want, key := range keys {
		if id := in.InternString(key); id != uint32(want) {
			t.Fatalf("InternString(%q) = %d, want %d", key, id, want)
		}
	}
	if in.Len() != len(keys) {
		t.Fatalf("Len = %d, want %d", in.Len(), len(keys))
	}
	for want, key := range keys {
		if id := in.Intern([]byte(key)); id != uint32(want) {
			t.Fatalf("repeat Intern(%q) = %d, want %d", key, id, want)
		}
		if id, ok := in.LookupString(key); !ok || id != uint32(want) {
			t.Fatalf("LookupString(%q) = (%d, %v), want (%d, true)", key, id, ok, want)
		}
		if got := in.Key(uint32(want)); string(got) != key {
			t.Fatalf("Key(%d) = %q, want %q", want, got, key)
		}
	}
	if _, ok := in.Lookup([]byte("still absent")); ok || in.Len() != len(keys) {
		t.Fatalf("Lookup inserted: Len = %d, want %d", in.Len(), len(keys))
	}
}

// TestKeyInternerResetReuse exercises the interner as a reusable GROUP BY
// workspace. Reset must restart dense identifiers and retain enough table,
// slice, decode, and arena capacity for an identical pass to allocate nothing.
func TestKeyInternerResetReuse(t *testing.T) {
	keys := make([]string, 2048)
	for i := range keys {
		keys[i] = fmt.Sprintf("reset-key-%04d-%s", i, strings.Repeat("x", 64+i%31))
	}

	var in KeyInterner
	fill := func() {
		for i, key := range keys {
			if id := in.InternString(key); id != uint32(i) {
				t.Fatalf("InternString(%q) = %d, want %d", key, id, i)
			}
		}
	}
	fill()
	if len(in.chunks) < 2 {
		t.Fatalf("fixture used %d arena chunks, want multiple chunks", len(in.chunks))
	}
	in.Reset()
	if in.Len() != 0 {
		t.Fatalf("Len after Reset = %d, want 0", in.Len())
	}
	if _, ok := in.LookupString(keys[0]); ok {
		t.Fatal("Lookup found a pre-Reset key")
	}
	fill()
	if id := in.InternString(keys[0]); id != 0 {
		t.Fatalf("repeat identifier after Reset = %d, want 0", id)
	}

	allocs := testing.AllocsPerRun(20, func() {
		in.Reset()
		for _, key := range keys {
			in.InternString(key)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm Reset + InternString pass allocated %.2f times, want 0", allocs)
	}
}

// TestKeyInternerArenaStability proves the arena never moves interned bytes:
// slices returned early must keep both their content and their exact backing
// address after ten thousand further keys force chunk turnover and table
// rehashes.
func TestKeyInternerArenaStability(t *testing.T) {
	var in KeyInterner
	early := make([]string, 32)
	held := make([][]byte, 32)
	base := make([]*byte, 32)
	for i := range early {
		early[i] = fmt.Sprintf("early-key-%02d-%s", i, strings.Repeat("x", i))
		id := in.InternString(early[i])
		held[i] = in.Key(id)
		base[i] = unsafe.SliceData(held[i])
	}
	for i := 0; i < 10000; i++ {
		in.InternString(fmt.Sprintf("filler-key-%05d-%s", i, strings.Repeat("y", i%97)))
	}
	if in.Len() != 32+10000 {
		t.Fatalf("Len = %d, want %d", in.Len(), 32+10000)
	}
	for i := range early {
		if string(held[i]) != early[i] {
			t.Fatalf("held slice %d changed to %q, want %q", i, held[i], early[i])
		}
		if unsafe.SliceData(held[i]) != base[i] || unsafe.SliceData(in.Key(uint32(i))) != base[i] {
			t.Fatalf("key %d moved in the arena", i)
		}
		if id, ok := in.LookupString(early[i]); !ok || id != uint32(i) {
			t.Fatalf("LookupString(%q) after growth = (%d, %v), want (%d, true)", early[i], id, ok, i)
		}
	}
}

// TestKeyInternerCollisions drives the table through genuine hash collisions.
// A deterministic sweep finds two keys whose full 32-bit content hashes agree
// — the birthday bound makes one certain within the sweep — and they must
// still intern to distinct identifiers with correct lookups, since slots
// verify bytes after the hash. The sweep's prefix is then bulk-interned so the
// load also exercises shared low bits, probe chains, and repeated growth.
func TestKeyInternerCollisions(t *testing.T) {
	seen := make(map[uint32]string)
	var a, b string
	for i := 0; i < 1<<20; i++ {
		key := fmt.Sprintf("collision-probe-%07x", i)
		h := vibejson.HashKey(key)
		if prev, ok := seen[h]; ok {
			a, b = prev, key
			break
		}
		seen[h] = key
	}
	if a == "" {
		t.Fatal("no 32-bit hash collision within 2^20 keys; the hash cannot be uniform")
	}

	var in KeyInterner
	idA, idB := in.InternString(a), in.InternString(b)
	if idA == idB {
		t.Fatalf("colliding keys %q and %q share identifier %d", a, b, idA)
	}
	if got, ok := in.LookupString(a); !ok || got != idA {
		t.Fatalf("LookupString(%q) = (%d, %v), want (%d, true)", a, got, ok, idA)
	}
	if got, ok := in.LookupString(b); !ok || got != idB {
		t.Fatalf("LookupString(%q) = (%d, %v), want (%d, true)", b, got, ok, idB)
	}
	if string(in.Key(idA)) != a || string(in.Key(idB)) != b {
		t.Fatal("colliding keys read back wrong content")
	}

	const bulk = 1 << 17
	for i := 0; i < bulk; i++ {
		key := fmt.Sprintf("collision-probe-%07x", i)
		want, ok := in.LookupString(key)
		id := in.InternString(key)
		if ok && id != want {
			t.Fatalf("bulk key %q moved from %d to %d", key, want, id)
		}
		if got, ok := in.Lookup([]byte(key)); !ok || got != id {
			t.Fatalf("bulk Lookup(%q) = (%d, %v), want (%d, true)", key, got, ok, id)
		}
	}
	for i := 0; i < bulk; i += 4093 {
		key := fmt.Sprintf("collision-probe-%07x", i)
		id, ok := in.LookupString(key)
		if !ok || string(in.Key(id)) != key {
			t.Fatalf("bulk key %q lost after growth", key)
		}
	}
}

// TestKeyInternerDecodedSpellings pins the decoded-content contract directly:
// escaped, unicode-escaped, and surrogate-pair spellings of one key must share
// its identifier, and near-miss spellings must not.
func TestKeyInternerDecodedSpellings(t *testing.T) {
	src := []byte(`{"abc":1,"abc":2,"k\n":3,"k\u000a":4,"kn":5,"😀":6,"😀":7,"héllo":8,"héllo":9}`)
	tape, err := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, len(src)+2))
	if err != nil {
		t.Fatal(err)
	}
	var in KeyInterner
	ids := in.AppendKeyIDs(nil, tape)
	want := []uint32{0, 0, 1, 1, 2, 3, 3, 4, 4}
	if len(ids) != len(want) {
		t.Fatalf("AppendKeyIDs returned %d ids, want %d", len(ids), len(want))
	}
	for i, id := range ids {
		if id != want[i] {
			t.Fatalf("ids = %v, want %v: spelling grouping broke at key %d", ids, want, i)
		}
	}
	if id, ok := in.LookupString("k\n"); !ok || id != 1 {
		t.Fatalf(`LookupString("k\n") = (%d, %v), want (1, true)`, id, ok)
	}
	if _, ok := in.LookupString(`k\n`); ok {
		t.Fatal("raw spelling of an escaped key was interned; interning must be by decoded content")
	}
}

// refAppendKeyIDs is the naive reference for AppendKeyIDs: walk the document
// through the public iterators, decode every key with the same helper the
// eager tree uses, and intern it with a fresh hash. Objects yield each key
// before its value's subtree, so the emission order is exactly tape order.
func refAppendKeyIDs(in *KeyInterner, dst []uint32, v vibejson.Node) []uint32 {
	switch v.Kind() {
	case document.Object:
		iter, _ := v.ObjectIter()
		for {
			key, value, ok := iter.Next()
			if !ok {
				return dst
			}
			dst = append(dst, in.InternString(nodeKeyString(key)))
			dst = refAppendKeyIDs(in, dst, value)
		}
	case document.Array:
		iter, _ := v.ArrayIter()
		for {
			element, ok := iter.Next()
			if !ok {
				return dst
			}
			dst = refAppendKeyIDs(in, dst, element)
		}
	default:
		return dst
	}
}

// TestKeyInternerAppendKeyIDsDifferential is the zero-regression gate for the
// tape walk: over the adversarial corpus and wide documents, one interner fed
// by AppendKeyIDs must emit the identical identifier sequence to a second
// interner fed by the rehash reference — across every build route: unenriched,
// enriched (whose stored hashes the walk reuses), and the enriched stage-2
// machine. Feeding every document through one interner pair also proves
// cross-document grouping, the property a multi-document engine consumes.
func TestKeyInternerAppendKeyIDsDifferential(t *testing.T) {
	docs := append([]string{}, keyHashCorpus...)
	docs = append(docs,
		keyHashWideDoc(64, ""),
		keyHashWideDoc(400, "pad-value-"),
		// Large enough to take the production stage-1/stage-2 machine route.
		keyHashWideDoc(2000, strings.Repeat("pad", 12)),
		`[{"a":1},[{"b":2},{"a":3}],"s",{"c":{"a":4}}]`,
		`[1,2,3]`, `"scalar"`, `42`,
	)
	builds := []struct {
		name  string
		build func(src []byte) (vibejson.Index, bool)
	}{
		{"unenriched", func(src []byte) (vibejson.Index, bool) {
			tape, err := vibejson.BuildIndex(src, make([]vibejson.IndexEntry, len(src)+2))
			return tape, err == nil
		}},
		{"enriched", func(src []byte) (vibejson.Index, bool) {
			tape, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, len(src)+2), document.IndexOptions{HashKeys: true})
			return tape, err == nil
		}},
		{"machine", func(src []byte) (vibejson.Index, bool) {
			return buildEnrichedMachine(src, make([]vibejson.IndexEntry, 0, len(src)+2))
		}},
	}
	for _, route := range builds {
		var in, ref KeyInterner
		got := []uint32{0xdeadbeef} // a dst prefix AppendKeyIDs must preserve
		want := []uint32{0xdeadbeef}
		for _, doc := range docs {
			src := []byte(doc)
			tape, ok := route.build(src)
			if !ok {
				if route.name == "machine" {
					continue // the machine may decline shapes it routes to the fallback
				}
				t.Fatalf("%s: build declined %.60q", route.name, doc)
			}
			got = in.AppendKeyIDs(got, tape)
			want = refAppendKeyIDs(&ref, want, tape.Root())
			if len(got) != len(want) {
				t.Fatalf("%s %.60q: %d ids, reference %d", route.name, doc, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("%s %.60q: id %d = %d, reference %d", route.name, doc, i, got[i], want[i])
				}
			}
		}
		if in.Len() != ref.Len() {
			t.Fatalf("%s: interner has %d keys, reference %d", route.name, in.Len(), ref.Len())
		}
		for id := 0; id < in.Len(); id++ {
			if !bytes.Equal(in.Key(uint32(id)), ref.Key(uint32(id))) {
				t.Fatalf("%s: Key(%d) = %q, reference %q", route.name, id, in.Key(uint32(id)), ref.Key(uint32(id)))
			}
		}
	}
}
