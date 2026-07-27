package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/thesyncim/vibejson/document"
)

func buildStorePersistFixture(t testing.TB) (*Collection, map[string]string) {
	t.Helper()
	options := Options{
		ChunkDocuments: 3,
		ShapeTapes:     true,
		ValueDict:      true,
		IndexOptions:   document.IndexOptions{HashKeys: true, MaxDepth: 37},
	}
	builder, err := NewBuilder(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.CreateIndex(IndexDefinition{
		Name: "country_status", Paths: []string{"/profile/geo/country", "/status"},
	}); err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string)
	for i := 0; i < 12; i++ {
		country := []string{"PT", "US", "DE"}[i%3]
		status := []string{"active", "idle"}[i%2]
		key := fmt.Sprintf("key:%02d", i)
		doc := fmt.Sprintf(`{"id":%d,"profile":{"geo":{"country":%q}},"status":%q,"label":"shared"}`, i, country, status)
		if err := builder.Append(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
		want[key] = doc
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	info, err := collection.AddIndex("search", IndexPostings)
	if err != nil {
		t.Fatal(err)
	}
	for info.State != IndexReady {
		info, err = collection.BackfillIndex(info.Name, 2)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Remove one complete page so persistence must retain a sparse high-water
	// vector and its reusable empty id rather than compacting stable addresses.
	for i := 3; i < 6; i++ {
		key := fmt.Sprintf("key:%02d", i)
		del46, _ := collection.Delete(key)
		if !del46 {
			t.Fatalf("Delete(%q) missed", key)
		}
		delete(want, key)
	}
	return collection, want
}

func TestStorePersistRoundTripIndexesAndMutation(t *testing.T) {
	collection, want := buildStorePersistFixture(t)
	beforeStats := collection.Stats()
	var image bytes.Buffer
	n, err := collection.WriteTo(&image)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(image.Len()) {
		t.Fatalf("WriteTo bytes = %d, buffer = %d", n, image.Len())
	}

	reopened, err := Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Options != collection.Options {
		t.Fatalf("Options = %+v, want %+v", reopened.Options, collection.Options)
	}
	afterStats := reopened.Stats()
	if afterStats.MappedImageBytes != uint64(image.Len()) || afterStats.ExternalKeyBytes == 0 ||
		afterStats.ExternalDocumentBytes == 0 {
		t.Fatalf("mapped Stats = %+v, want image=%d and external key metadata", afterStats, image.Len())
	}
	afterComparable := afterStats
	afterComparable.MappedImageBytes = 0
	afterComparable.ExternalKeyBytes = 0
	afterComparable.ExternalDocumentBytes = 0
	afterComparable.ExternalIndexBytes = 0
	beforeStats.MappedImageBytes = 0
	beforeStats.ExternalKeyBytes = 0
	beforeStats.ExternalDocumentBytes = 0
	beforeStats.ExternalIndexBytes = 0
	if afterComparable != beforeStats {
		t.Fatalf("Stats = %+v, want operational fields %+v", afterStats, beforeStats)
	}
	snap44, _ := reopened.Snapshot()
	checkCollectionSnapshot(t, snap44, want)
	snap43, _ := reopened.Snapshot()
	infos := snap43.AppendIndexes(nil)
	if len(infos) != 2 || infos[0].Name != "country_status" || infos[1].Name != "search" {
		t.Fatalf("indexes = %+v", infos)
	}
	for _, info := range infos {
		if info.State != IndexReady || info.CoveredChunks != info.TotalChunks {
			t.Fatalf("index not Ready: %+v", info)
		}
	}
	snap42, _ := reopened.Snapshot()
	keys, err := snap42.AppendIndexRawKeys(nil, "country_status", []byte(`"PT"`), []byte(`"active"`))
	if err != nil || !slices.Equal(keys, []string{"key:00", "key:06"}) {
		t.Fatalf("compound lookup = (%v,%v)", keys, err)
	}
	needle := testScalarIndex(t, `"active"`)
	keys = reopened.AppendWhereContainsIndexKeys(nil, "status", needle)
	if !slices.Equal(keys, []string{"key:00", "key:02", "key:06", "key:08", "key:10"}) {
		t.Fatalf("posting lookup = %v", keys)
	}

	compiled := reopened.CompileKey("key:00")
	copyBuf := make([]byte, 3, len(want["key:00"])+3)
	copy(copyBuf, "pre")
	copyBuf, ok := reopened.AppendRawKey(copyBuf, compiled)
	if !ok || string(copyBuf) != "pre"+want["key:00"] {
		t.Fatalf("AppendRawKey = (%q,%v)", copyBuf, ok)
	}
	unchanged := len(copyBuf)
	if copyBuf, ok = reopened.AppendRaw(copyBuf, "missing"); ok || len(copyBuf) != unchanged {
		t.Fatalf("AppendRaw miss changed destination: (%d,%v)", len(copyBuf), ok)
	}

	retained, _ := reopened.Snapshot()
	if _, err := reopened.Put("key:00", []byte(`{"id":0,"profile":{"geo":{"country":"FR"}},"status":"idle"}`)); err != nil {
		t.Fatal(err)
	}
	del41, _ := reopened.Delete("key:06")
	if !del41 {
		t.Fatal("Delete after OpenStore missed")
	}
	if _, err := reopened.Put("new", []byte(`{"id":99,"profile":{"geo":{"country":"PT"}},"status":"active"}`)); err != nil {
		t.Fatal(err)
	}
	if raw, ok := retained.GetRaw("key:00"); !ok || string(raw.Bytes()) != want["key:00"] {
		t.Fatalf("mutation changed retained mapped snapshot: (%q,%v)", raw.Bytes(), ok)
	}
	snap40, _ := reopened.Snapshot()
	keys, err = snap40.AppendIndexRawKeys(nil, "country_status", []byte(`"PT"`), []byte(`"active"`))
	if err != nil || !slices.Equal(keys, []string{"new"}) {
		t.Fatalf("post-open exact maintenance = (%v,%v)", keys, err)
	}

	var second bytes.Buffer
	if _, err := reopened.WriteTo(&second); err != nil {
		t.Fatal(err)
	}
	secondCollection, err := Open(second.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if raw, ok := secondCollection.GetRaw("new"); !ok || string(raw.Bytes()) != `{"id":99,"profile":{"geo":{"country":"PT"}},"status":"active"}` {
		t.Fatalf("second generation value = (%q,%v)", raw.Bytes(), ok)
	}
}

func TestStorePersistEmptyAndBuildingIndex(t *testing.T) {
	empty := &Collection{Options: Options{ChunkDocuments: 7, IndexOptions: document.IndexOptions{MaxDepth: -1}}}
	var image bytes.Buffer
	if _, err := empty.WriteTo(&image); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(image.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 0 || reopened.Generation() != 0 || reopened.Options != (Options{
		ChunkDocuments: 7, IndexOptions: document.IndexOptions{MaxDepth: -1},
	}) {
		t.Fatalf("empty reopened collection = len %d generation %d options %+v", reopened.Len(), reopened.Generation(), reopened.Options)
	}
	if _, err := reopened.Put("first", []byte(`1`)); err != nil {
		t.Fatal(err)
	}

	building := &Collection{Options: Options{ChunkDocuments: 2}}
	_, _ = building.Put("a", []byte(`{"v":1}`))
	_, _ = building.Put("b", []byte(`{"v":2}`))
	if _, err := building.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := building.WriteTo(&bytes.Buffer{}); !errors.Is(err, ErrCheckpointIndexBuilding) {
		t.Fatalf("building-index WriteTo error = %v", err)
	}
}

func TestOpenStoreRejectsMalformedFramingAndManifest(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2, ShapeTapes: true}}
	_, _ = collection.Put("a", []byte(`{"v":1}`))
	_, _ = collection.Put("b", []byte(`{"v":2}`))
	var buf bytes.Buffer
	if _, err := collection.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	image := buf.Bytes()
	for cut := 0; cut < len(image); cut++ {
		if _, err := Open(image[:cut]); err == nil {
			t.Fatalf("truncation at %d bytes opened", cut)
		}
	}

	mutate := func(name string, fn func([]byte)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			bad := append([]byte(nil), image...)
			fn(bad)
			if _, err := Open(bad); err == nil {
				t.Fatal("malformed image opened")
			}
		})
	}
	mutate("header magic", func(b []byte) { b[0] ^= 0xff })
	mutate("header version", func(b []byte) { binary.LittleEndian.PutUint32(b[8:12], storePersistVersion+1) })
	mutate("header reserved", func(b []byte) { b[12] = 1 })
	mutate("footer magic", func(b []byte) { b[len(b)-ImageFooterLen] ^= 0xff })
	mutate("footer reserved", func(b []byte) { b[len(b)-1] = 1 })
	mutate("manifest checksum", func(b []byte) {
		off := binary.LittleEndian.Uint64(b[len(b)-ImageFooterLen+8:])
		b[off+CheckpointManifestFixed] ^= 0xff
	})
	mutate("impossible live count", func(b []byte) {
		footer := b[len(b)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:16])
		manifest := b[off : off+binary.LittleEndian.Uint64(footer[16:24])]
		binary.LittleEndian.PutUint32(manifest[44:48], 3)
		binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	})
	mutate("manifest reserved", func(b []byte) {
		footer := b[len(b)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:16])
		manifest := b[off : off+binary.LittleEndian.Uint64(footer[16:24])]
		manifest[64] = 1
		binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	})
	mutate("count allocation bomb", func(b []byte) {
		footer := b[len(b)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:16])
		manifest := b[off : off+binary.LittleEndian.Uint64(footer[16:24])]
		binary.LittleEndian.PutUint32(manifest[40:44], math.MaxUint32)
		binary.LittleEndian.PutUint32(manifest[44:48], 0)
		binary.LittleEndian.PutUint32(manifest[60:64], math.MaxUint32)
		binary.LittleEndian.PutUint64(manifest[32:40], 0)
		binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	})
	mutate("unaligned chunk", func(b []byte) {
		footer := b[len(b)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:16])
		manifest := b[off : off+binary.LittleEndian.Uint64(footer[16:24])]
		chunk := CheckpointManifestFixed
		imageOffset := binary.LittleEndian.Uint64(manifest[chunk+16 : chunk+24])
		binary.LittleEndian.PutUint64(manifest[chunk+16:chunk+24], imageOffset+1)
		binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	})
	mutate("invalid stable slots", func(b []byte) {
		footer := b[len(b)-ImageFooterLen:]
		off := binary.LittleEndian.Uint64(footer[8:16])
		manifest := b[off : off+binary.LittleEndian.Uint64(footer[16:24])]
		chunk := CheckpointManifestFixed
		binary.LittleEndian.PutUint64(manifest[chunk+8:chunk+16], 0)
		binary.LittleEndian.PutUint64(footer[24:32], ManifestChecksum(manifest))
	})
}

func TestStoreAppendRawSteadyAllocs(t *testing.T) {
	collection := &Collection{Options: Options{}}
	doc := `{"value":"caller-owned"}`
	if _, err := collection.Put("key", []byte(doc)); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := collection.Snapshot()
	key := snapshot.CompileKey("key")
	dst := make([]byte, 0, len(doc))
	if allocs := testing.AllocsPerRun(1000, func() {
		out, ok := snapshot.AppendRawKey(dst[:0], key)
		if !ok || len(out) != len(doc) {
			panic("AppendRawKey failed")
		}
	}); allocs != 0 {
		t.Fatalf("AppendRawKey allocated %.2f times, want 0", allocs)
	}
}

type storePersistShortWriter struct{}

func (storePersistShortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestStorePersistShortWrite(t *testing.T) {
	collection := &Collection{Options: Options{}}
	if _, err := collection.Put("key", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	n, err := collection.WriteTo(storePersistShortWriter{})
	if !errors.Is(err, io.ErrShortWrite) || n != ImageHeaderLen-1 {
		t.Fatalf("WriteTo short write = (%d,%v), want (%d,%v)", n, err, ImageHeaderLen-1, io.ErrShortWrite)
	}
}
