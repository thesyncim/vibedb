package store

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestStoreMappedKeysPointerFreeBaseAndOverlay(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 4, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 17 {
		if err := builder.Append(fmt.Sprintf("key:%02d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	var image bytes.Buffer
	if _, err := collection.WriteTo(&image); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	base := reopened.state.Load()
	if base.keys != nil {
		t.Fatal("OpenStore rebuilt a per-key pointer HAMT")
	}
	if base.baseKeys == nil || base.baseKeys.count != 17 || base.baseKeys.keyRefCount() != 17 {
		t.Fatalf("mapped base = %+v", base.baseKeys)
	}
	base.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		if chunk.keys != nil || chunk.mappedKeys != base.baseKeys {
			t.Fatalf("mapped chunk retained heap string table")
		}
		if chunk.Docs.mappedDocs != base.mappedDocs || chunk.Docs.docs != nil ||
			chunk.Docs.narrow != nil || chunk.Docs.tapeRefs != nil {
			t.Fatalf("mapped chunk retained pointer-rich document tables")
		}
		return true
	})

	old, _ := reopened.Snapshot()
	if created, err := reopened.Put("key:03", []byte(`{"n":300}`)); err != nil || created {
		t.Fatalf("replace = (%v, %v)", created, err)
	}
	del31, _ := reopened.Delete("key:04")
	if !del31 {
		t.Fatal("delete mapped-base key missed")
	}
	if created, err := reopened.Put("new", []byte(`{"n":900}`)); err != nil || !created {
		t.Fatalf("insert = (%v, %v)", created, err)
	}
	current := reopened.state.Load()
	if current.baseKeys != base.baseKeys || current.keys == nil {
		t.Fatal("mutation did not preserve compact base plus bounded overlay")
	}
	if raw, ok := old.GetRaw("key:04"); !ok || string(raw.Bytes()) != `{"n":4}` {
		t.Fatalf("retained base snapshot = (%q, %v)", raw.Bytes(), ok)
	}
	if _, ok := reopened.GetRaw("key:04"); ok {
		t.Fatal("deleted mapped-base key remained visible")
	}
	if raw, ok := reopened.GetRaw("key:03"); !ok || string(raw.Bytes()) != `{"n":300}` {
		t.Fatalf("replacement = (%q, %v)", raw.Bytes(), ok)
	}
	if raw, ok := reopened.GetRaw("new"); !ok || string(raw.Bytes()) != `{"n":900}` {
		t.Fatalf("overlay insert = (%q, %v)", raw.Bytes(), ok)
	}

	// Aggressive collection cannot finalize the external table while either a
	// current or retained state can still reach it.
	for range 4 {
		runtime.GC()
		if raw, ok := old.GetRaw("key:03"); !ok || string(raw.Bytes()) != `{"n":3}` {
			t.Fatalf("old snapshot after GC = (%q, %v)", raw.Bytes(), ok)
		}
	}
}

func TestStoreMappedBaseConcurrentReadersAndWriter(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 64, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 128 {
		if err := builder.Append(fmt.Sprintf("key:%03d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	var image bytes.Buffer
	if _, err := collection.WriteTo(&image); err != nil {
		t.Fatal(err)
	}
	collection, err = Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	retained, _ := collection.Snapshot()

	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			<-start
			for range 2_000 {
				snapshot, _ := collection.Snapshot()
				raw, ok := snapshot.GetRaw("key:002")
				if !ok || len(raw.Bytes()) == 0 {
					t.Errorf("concurrent mapped read missed")
					return
				}
			}
		})
	}
	close(start)
	for i := range 500 {
		if _, err := collection.Put("key:002", []byte(fmt.Sprintf(`{"n":%d}`, 1_000+i))); err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			runtime.GC()
		}
	}
	readers.Wait()
	if raw, ok := retained.GetRaw("key:002"); !ok || string(raw.Bytes()) != `{"n":2}` {
		t.Fatalf("retained mapped snapshot = (%q, %v)", raw.Bytes(), ok)
	}
}

func TestStoreMappedKeysGroupProbeCollisionDifferential(t *testing.T) {
	const count = 257
	source := make([]byte, 0, count*12)
	mapped, err := newStoreMappedKeys(nil, count, false)
	if err != nil {
		t.Fatal(err)
	}
	defer mapped.release()
	seed := maphash.MakeSeed()
	want := make(map[string]Location, count)
	for i := range count {
		key := fmt.Sprintf("collision-key-%03d", i)
		off := len(source)
		source = append(source, key...)
		loc := Location{Chunk: uint32(i / 64), Slot: uint8(i % 64)}
		mapped.refs[i] = storeMappedKeyRef{off: uint64(off), length: uint32(len(key))}
		mapped.setLocation(uint64(i), loc)
		want[key] = loc
	}
	mapped.source = source
	for i := range count {
		key := fmt.Sprintf("collision-key-%03d", i)
		// Retain only three initial hash bits to force long, wrapping clusters;
		// exact spelling must still distinguish every key.
		hash := maphash.String(seed, key) & 7
		if !mapped.insert(hash, uint64(i)) {
			t.Fatalf("insert %q reported duplicate", key)
		}
	}
	for key, loc := range want {
		hash := maphash.String(seed, key) & 7
		if got, ok := mapped.lookup(hash, key); !ok || got != loc {
			t.Fatalf("lookup %q = (%+v, %v), want %+v", key, got, ok, loc)
		}
	}
	if _, ok := mapped.lookup(0, "absent"); ok {
		t.Fatal("absent collision lookup hit")
	}
	if mapped.insert(maphash.String(seed, "collision-key-100")&7, 100) {
		t.Fatal("duplicate insertion succeeded")
	}
}

func TestStoreMappedKeysExactRangeAndIndexKeys(t *testing.T) {
	collection, _ := buildStorePersistFixture(t)
	var image bytes.Buffer
	if _, err := collection.WriteTo(&image); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var ranged []string
	snap30, _ := reopened.Snapshot()
	snap30.Range(func(key string, _ vibejson.RawValue) bool {
		ranged = append(ranged, key)
		return true
	})
	snap29, _ := reopened.Snapshot()
	rows, err := snap29.AppendIndexRawKeys(nil, "country_status", []byte(`"PT"`), []byte(`"active"`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rows, []string{"key:00", "key:06"}) {
		t.Fatalf("index keys = %v", rows)
	}
	if uint64(len(ranged)) != reopened.Len() {
		t.Fatalf("Range keys = %d, want %d", len(ranged), reopened.Len())
	}
}
