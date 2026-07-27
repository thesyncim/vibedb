package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func buildStorePageTestData(t testing.TB, rows, chunkDocuments int) (*store.Collection, map[string]string) {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: chunkDocuments, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string, rows)
	for i := range rows {
		key := fmt.Sprintf("account:%08d", i)
		doc := fmt.Sprintf(`{"id":%d,"tenant":%d,"active":%t,"name":"user-%08d"}`,
			i, i%97, i&1 == 0, i)
		if err := builder.Append(key, []byte(doc)); err != nil {
			t.Fatal(err)
		}
		want[key] = doc
	}
	fs, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return fs, want
}

func writeStorePageTestFile(t testing.TB, fs *store.Collection, options StorePageWriteOptions) (string, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.pages")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	size, writeErr := WritePageFile(fs, file, options)
	closeErr := file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return path, size
}

func TestStorePageFileRoundTripEvictionAndCompiledKey(t *testing.T) {
	fs, want := buildStorePageTestData(t, 1024, 16)
	path, size := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	if reader.Len() != uint64(len(want)) || reader.Generation() != fs.Generation() {
		t.Fatalf("reader Len/Generation = %d/%d", reader.Len(), reader.Generation())
	}
	if reader.root.FreeChunkHint != reader.root.ChunkHighWater {
		t.Fatalf("dense free-chunk hint = %d, want high-water %d",
			reader.root.FreeChunkHint, reader.root.ChunkHighWater)
	}
	for i := range len(want) {
		key := fmt.Sprintf("account:%08d", i)
		dst := make([]byte, 3, 256)
		copy(dst, "pre")
		got, ok, err := reader.AppendRawKey(dst, reader.CompileKey(key))
		if err != nil || !ok || string(got[:3]) != "pre" || string(got[3:]) != want[key] {
			t.Fatalf("read %q = (%q,%v,%v)", key, got, ok, err)
		}
	}
	if _, ok, err := reader.ViewRaw("absent"); err != nil || ok {
		t.Fatalf("absent read = (%v,%v)", ok, err)
	}
	seen := 0
	err = reader.RangeRaw(func(key, json []byte) bool {
		seen++
		wantJSON, ok := want[string(key)]
		if !ok || string(json) != wantJSON {
			t.Fatalf("RangeRaw row = (%q,%q)", key, json)
		}
		return true
	})
	if err != nil || seen != len(want) {
		t.Fatalf("RangeRaw = (%d,%v), want %d rows", seen, err, len(want))
	}
	stats := reader.Stats()
	if stats.FileBytes != uint64(size) || stats.Cache.CapacityBytes != 2*4096 ||
		stats.Cache.ResidentBytes > stats.Cache.ReservedBytes ||
		stats.Cache.ReservedBytes > stats.Cache.CapacityBytes || stats.Cache.Evictions == 0 ||
		stats.Cache.PageReads == 0 || stats.Cache.PinnedFrames != 0 {
		t.Fatalf("page stats = %+v", stats)
	}
}

func TestStorePageFileMoreThanHundredTimesResidentBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("large bounded-residency smoke")
	}
	fs, _ := buildStorePageTestData(t, 4096, 16)
	path, size := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	const resident = int64(2 * 4096)
	if size <= 100*resident {
		t.Fatalf("test image = %d bytes, need >100x %d-byte cache", size, resident)
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: resident, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, row := range []int{4095, 0, 2048, 17, 4000, 1} {
		key := fmt.Sprintf("account:%08d", row)
		value, ok, err := reader.ViewRaw(key)
		if err != nil || !ok || !bytes.Contains(value.Bytes(), []byte(fmt.Sprintf(`"id":%d`, row))) {
			t.Fatalf("pressure read %q = (%q,%v,%v)", key, value.Bytes(), ok, err)
		}
		if err := value.Close(); err != nil {
			t.Fatal(err)
		}
	}
	rows := 0
	if err := reader.RangeRaw(func(_, _ []byte) bool { rows++; return true }); err != nil || rows != 4096 {
		t.Fatalf("bounded RangeRaw = (%d,%v)", rows, err)
	}
	if stats := reader.Stats(); stats.Cache.CapacityBytes != uint64(resident) ||
		stats.Cache.ResidentBytes > uint64(resident) || stats.FileBytes <= 100*uint64(resident) {
		t.Fatalf("bounded stats = %+v", stats)
	}
}

func TestStorePageFileEmptyAndMixedDocumentExtents(t *testing.T) {
	empty, err := store.New(store.Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	emptyPath, _ := writeStorePageTestFile(t, empty, StorePageWriteOptions{MaxDocumentPageBytes: 8192})
	emptyReader, err := OpenStorePageReader(emptyPath, StorePageOpenOptions{
		ResidentBytes: 2 * 8192, MaxDocumentPageBytes: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	if emptyReader.Len() != 0 {
		t.Fatalf("empty Len = %d", emptyReader.Len())
	}
	if err := emptyReader.RangeRaw(func(_, _ []byte) bool {
		t.Fatal("empty reader visited a row")
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if err := emptyReader.Close(); err != nil {
		t.Fatal(err)
	}

	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("small", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	large := append([]byte(`{"v":"`), bytes.Repeat([]byte{'x'}, 5000)...)
	large = append(large, '"', '}')
	if err := builder.Append("large", large); err != nil {
		t.Fatal(err)
	}
	fs, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 8192})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 8192, MaxDocumentPageBytes: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, ok, err := reader.AppendRaw(nil, "large")
	if err != nil || !ok || !bytes.Equal(got, large) {
		t.Fatalf("mixed-extent read = (%d,%v,%v)", len(got), ok, err)
	}
}

func TestStorePageFileUsesExactQuantumDocumentExtents(t *testing.T) {
	for _, test := range []struct {
		required uint64
		maximum  uint32
		want     uint32
		ok       bool
	}{
		{required: 1, maximum: 16 << 10, want: 4 << 10, ok: true},
		{required: 4 << 10, maximum: 16 << 10, want: 4 << 10, ok: true},
		{required: 4<<10 + 1, maximum: 16 << 10, want: 8 << 10, ok: true},
		{required: 8<<10 + 1, maximum: 16 << 10, want: 12 << 10, ok: true},
		{required: 12 << 10, maximum: 16 << 10, want: 12 << 10, ok: true},
		{required: 16<<10 + 1, maximum: 16 << 10, ok: false},
	} {
		got, ok := storePageExtent(test.required, test.maximum)
		if got != test.want || ok != test.ok {
			t.Fatalf(
				"storePageExtent(%d,%d) = (%d,%v), want (%d,%v)",
				test.required, test.maximum, got, ok, test.want, test.ok,
			)
		}
	}

	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	value := append([]byte(`{"payload":"`), bytes.Repeat([]byte{'x'}, 9<<10)...)
	value = append(value, '"', '}')
	if err := builder.Append("exact", value); err != nil {
		t.Fatal(err)
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	path, _ := writeStorePageTestFile(t, collection, StorePageWriteOptions{
		MaxDocumentPageBytes: 16 << 10,
	})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 16 << 10, MaxDocumentPageBytes: 16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	document, ok, err := resolveStoreDocumentPage(
		reader.pages.Load(), reader.root.ChunkDirectory, 0,
	)
	if err != nil || !ok || document.Length != 12<<10 {
		t.Fatalf("exact StorePage document = (%+v,%v,%v), want 12 KiB", document, ok, err)
	}
	got, ok, err := reader.AppendRaw(nil, "exact")
	if err != nil || !ok || !bytes.Equal(got, value) {
		t.Fatalf("exact StorePage read = (%d,%v,%v)", len(got), ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorePageFilePersistsCurrentUpdatesDeletesAndReuse(t *testing.T) {
	fs, want := buildStorePageTestData(t, 96, 8)
	for i := range 8 {
		key := fmt.Sprintf("account:%08d", i)
		_sigfix16, _ := fs.Delete(key)
		if !_sigfix16 {
			t.Fatalf("Delete(%q)", key)
		}
		delete(want, key)
	}
	updated := `{"id":20,"tenant":999,"active":true,"name":"updated"}`
	if _, err := fs.Put("account:00000020", []byte(updated)); err != nil {
		t.Fatal(err)
	}
	want["account:00000020"] = updated
	_sigfix15, _ := fs.Delete("account:00000030")
	if !_sigfix15 {
		t.Fatal("Delete account:00000030")
	}
	delete(want, "account:00000030")
	reused := `{"id":1000,"tenant":7,"active":true,"name":"reused"}`
	if created, err := fs.Put("account:new", []byte(reused)); err != nil || !created {
		t.Fatalf("reused Put = (%v,%v)", created, err)
	}
	want["account:new"] = reused

	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 3 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Len() != uint64(len(want)) {
		t.Fatalf("Len = %d, want %d", reader.Len(), len(want))
	}
	for key, expected := range want {
		got, ok, err := reader.AppendRaw(nil, key)
		if err != nil || !ok || string(got) != expected {
			t.Fatalf("read %q = (%q,%v,%v), want %q", key, got, ok, err, expected)
		}
	}
	for _, key := range []string{"account:00000000", "account:00000007", "account:00000030"} {
		if _, ok, err := reader.ViewRaw(key); err != nil || ok {
			t.Fatalf("deleted %q = (%v,%v)", key, ok, err)
		}
	}
}

func TestStorePageFileWarmAppendIsZeroAllocation(t *testing.T) {
	fs, want := buildStorePageTestData(t, 16, 16)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	key := reader.CompileKey("account:00000007")
	prepared, ok, err := reader.PrepareKey("account:00000007")
	if err != nil || !ok {
		t.Fatalf("PrepareKey = (%v,%v)", ok, err)
	}
	dst := make([]byte, 0, 256)
	if dst, _, err = reader.AppendRawKey(dst, key); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var ok bool
		dst, ok, err = reader.AppendRawKey(dst[:0], key)
		if err != nil || !ok || string(dst) != want["account:00000007"] {
			panic("page lookup failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warm AppendRawKey allocated %.2f times, want zero", allocs)
	}
	allocs = testing.AllocsPerRun(100, func() {
		var ok bool
		dst, ok, err = reader.AppendRawKey(dst[:0], prepared)
		if err != nil || !ok || string(dst) != want["account:00000007"] {
			panic("prepared page lookup failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("prepared AppendRawKey allocated %.2f times, want zero", allocs)
	}
	var rows, bytesSeen int
	visit := func(key, json []byte) bool {
		rows++
		bytesSeen += len(key) + len(json)
		return true
	}
	if err := reader.RangeRaw(visit); err != nil {
		t.Fatal(err)
	}
	allocs = testing.AllocsPerRun(100, func() {
		rows, bytesSeen = 0, 0
		if err := reader.RangeRaw(visit); err != nil {
			panic(err)
		}
	})
	if allocs != 0 || rows != 16 || bytesSeen == 0 {
		t.Fatalf("warm RangeRaw = allocs %.2f rows %d bytes %d", allocs, rows, bytesSeen)
	}
}

func TestStorePageFileRejectsUnsupportedAndCorruptState(t *testing.T) {
	indexed, _ := buildStorePageTestData(t, 16, 16)
	if _, err := indexed.CreateIndex(store.IndexDefinition{Name: "tenant", Paths: []string{"/tenant"}}); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "unsupported-index")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WritePageFile(indexed, file, StorePageWriteOptions{}); !errors.Is(err, ErrStorePageUnsupported) {
		t.Fatalf("index write error = %v", err)
	}
	_ = file.Close()

	clean, _ := buildStorePageTestData(t, 16, 16)
	path, _ := writeStorePageTestFile(t, clean, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	corrupt, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0xff}
	if _, err := corrupt.WriteAt(one, int64(testMutableDataStart(4096)+100)); err != nil {
		t.Fatal(err)
	}
	if err := corrupt.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, _, err := reader.ViewRaw("account:00000001"); !errors.Is(err, ErrStorePageCorrupt) {
		t.Fatalf("corrupt document error = %v, want %v", err, ErrStorePageCorrupt)
	}
}

func TestStorePageReaderBackpressureAndClose(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 3, 1)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared := make([]StorePageKey, 3)
	for row := range prepared {
		prepared[row], _, err = reader.PrepareKey(fmt.Sprintf("account:%08d", row))
		if err != nil || !prepared[row].resolved {
			t.Fatalf("PrepareKey(%d) = (%+v,%v)", row, prepared[row], err)
		}
	}
	first, ok, err := reader.ViewRawKey(prepared[0])
	if err != nil || !ok {
		t.Fatalf("first pinned value = (%v,%v)", ok, err)
	}
	second, ok, err := reader.ViewRawKey(prepared[1])
	if err != nil || !ok {
		_ = first.Close()
		t.Fatalf("second pinned value = (%v,%v)", ok, err)
	}
	if _, _, err := reader.ViewRawKey(prepared[2]); !errors.Is(err, ErrStorePageCacheFull) {
		t.Fatalf("third pinned value error = %v, want %v", err, ErrStorePageCacheFull)
	}
	if err := errors.Join(first.Close(), second.Close()); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.ViewRaw("account:00000000"); !errors.Is(err, ErrStorePageClosed) {
		t.Fatalf("ViewRaw after Close = %v, want %v", err, ErrStorePageClosed)
	}
	if _, _, err := reader.PrepareKey("account:00000000"); !errors.Is(err, ErrStorePageClosed) {
		t.Fatalf("PrepareKey after Close = %v, want %v", err, ErrStorePageClosed)
	}
	if err := reader.RangeRaw(func(_, _ []byte) bool { return true }); !errors.Is(err, ErrStorePageClosed) {
		t.Fatalf("RangeRaw after Close = %v, want %v", err, ErrStorePageClosed)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestStorePageReaderConcurrentClose(t *testing.T) {
	fs, _ := buildStorePageTestData(t, 16, 16)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, ok, err := reader.PrepareKey("account:00000007")
	if err != nil || !ok {
		t.Fatalf("PrepareKey = (%v,%v)", ok, err)
	}
	leaseHeld := make(chan struct{})
	releaseLease := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		value, ok, viewErr := reader.ViewRawKey(key)
		if viewErr != nil || !ok {
			readDone <- fmt.Errorf("concurrent ViewRawKey = (%v,%w)", ok, viewErr)
			return
		}
		close(leaseHeld)
		<-releaseLease
		if closeErr := value.Close(); closeErr != nil {
			readDone <- closeErr
			return
		}
		if _, _, viewErr = reader.ViewRawKey(key); !errors.Is(viewErr, ErrStorePageClosed) {
			readDone <- fmt.Errorf("ViewRawKey after concurrent Close = %v, want %v", viewErr, ErrStorePageClosed)
			return
		}
		readDone <- nil
	}()
	<-leaseHeld
	closeDone := make(chan error, 1)
	go func() { closeDone <- reader.Close() }()
	for reader.pages.Load() != nil {
		runtime.Gosched()
	}
	var earlyClose error
	closedEarly := false
	select {
	case earlyClose = <-closeDone:
		closedEarly = true
	default:
	}
	close(releaseLease)
	if !closedEarly {
		earlyClose = <-closeDone
	}
	if earlyClose != nil {
		t.Fatal(earlyClose)
	}
	if closedEarly {
		t.Fatal("Close returned while a zero-copy value still pinned its cache extent")
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
}

func TestStorePageFileDirectIOMode(t *testing.T) {
	fs, want := buildStorePageTestData(t, 16, 16)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	mode := StoreDirectTry
	require := os.Getenv("VIBEJSON_REQUIRE_DIRECT_IO") == "1"
	if require {
		mode = StoreDirectRequire
	}
	reader, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 4 * 4096, MaxDocumentPageBytes: 4096, DirectIO: mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if require && !reader.DirectIO() {
		t.Fatal("direct I/O was required but is not active")
	}
	dst, ok, err := reader.AppendRaw(nil, "account:00000007")
	if err != nil || !ok || string(dst) != want["account:00000007"] {
		t.Fatalf("direct read = (%q,%v,%v)", dst, ok, err)
	}
}

func TestStorePageFileRequiredDirectIOReportsUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux support depends on the selected filesystem")
	}
	fs, _ := buildStorePageTestData(t, 1, 1)
	path, _ := writeStorePageTestFile(t, fs, StorePageWriteOptions{MaxDocumentPageBytes: 4096})
	if _, err := OpenStorePageReader(path, StorePageOpenOptions{
		ResidentBytes: 2 * 4096, MaxDocumentPageBytes: 4096, DirectIO: StoreDirectRequire,
	}); !errors.Is(err, ErrStoreDirectIOUnsupported) {
		t.Fatalf("required direct I/O error = %v, want %v", err, ErrStoreDirectIOUnsupported)
	}
}
