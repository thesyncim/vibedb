//go:build linux

package durable

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStoreRequiredDirectReads(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-required-direct-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ReadMode = ReadDirectRequire
	collection, err := Create(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) {
		t.Skipf("test filesystem has no O_DIRECT support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !collection.Stats().DirectReads {
		t.Fatal("required direct reads were not reported active")
	}
	// Large enough that the ordered-primary scan crosses several pages. That
	// scan is deliberately serial even through RangeRawReadAheadBuffer: inline
	// values arrive with the leaves the cursor already reads, so there is no
	// separate document-extent prefetch lane.
	const documents = 512
	for row := range documents {
		key := fmt.Sprintf("linux:direct:%04d", row)
		value := fmt.Appendf(nil, `{"v":%d}`, row)
		if _, err := collection.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok, err := reopened.AppendRaw(nil, "linux:direct:0001"); err != nil || !ok || string(got) != `{"v":1}` {
		t.Fatalf("required direct read = (%q,%v,%v)", got, ok, err)
	}
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	if _, err := snapshot.RangeRawReadAheadBuffer(nil, func(_, _ []byte) error {
		rows++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != documents {
		t.Fatalf("required direct read-ahead rows = %d, want %d", rows, documents)
	}
	if stats := reopened.Stats(); !stats.DirectReads || stats.PageReads == 0 ||
		stats.PrefetchQueued != 0 {
		t.Fatalf("required direct stats = %+v", stats)
	}
}

func TestFileStoreRequiredDirectWrites(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-required-direct-write-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.WriteMode = WriteDirectRequire
	collection, err := Create(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) {
		t.Skipf("test filesystem has no O_DIRECT support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if stats := collection.Stats(); !stats.DirectWrites || stats.DirectReads {
		t.Fatalf("required direct-write stats = %+v", stats)
	}
	for row := range 64 {
		key := fmt.Sprintf("linux:direct-write:%02d", row)
		value := fmt.Appendf(nil, `{"v":%d}`, row)
		if _, err := collection.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if stats := reopened.Stats(); !stats.DirectWrites || stats.DirectReads {
		t.Fatalf("reopened direct-write stats = %+v", stats)
	}
	if got, ok, err := reopened.AppendRaw(nil, "linux:direct-write:63"); err != nil || !ok || string(got) != `{"v":63}` {
		t.Fatalf("required direct write = (%q,%v,%v)", got, ok, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		t.Fatalf("caller descriptor after direct writer Close: %v", err)
	}
}

func TestFileStoreRequiredDirectReadWrite(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-required-direct-rw-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.ReadMode = ReadDirectRequire
	options.WriteMode = WriteDirectRequire
	collection, err := Create(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) {
		t.Skipf("test filesystem has no O_DIRECT support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	for row := range 64 {
		key := fmt.Sprintf("linux:direct-rw:%02d", row)
		value := fmt.Appendf(nil, `{"v":%d}`, row)
		if _, err := collection.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok, err := reopened.AppendRaw(nil, "linux:direct-rw:31"); err != nil || !ok || string(got) != `{"v":31}` {
		t.Fatalf("required direct read/write = (%q,%v,%v)", got, ok, err)
	}
	if stats := reopened.Stats(); !stats.DirectReads || !stats.DirectWrites || stats.PageReads == 0 {
		t.Fatalf("required direct read/write stats = %+v", stats)
	}
}

// pressureKey spells one fixture key. The writer, the concurrent readers, and
// the deleter below must agree exactly; they did not once, and a widened row
// count silently turned every read into a miss for a key that was never
// written.
func pressureKey(row uint32) string { return fmt.Sprintf("pressure:%06d", row) }

func TestFileStoreDirectReadWriteUnderCachePressure(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-direct-pressure-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.ReadMode = ReadDirectRequire
	options.WriteMode = WriteDirectRequire
	options.MaxSnapshotLeases = 64
	options.MaxRetiredExtents = 1 << 14
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	options.ResidentBytes = int64(2 * normalized.maxTransactionBytes)
	collection, err := Create(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) {
		t.Skipf("test filesystem has no O_DIRECT support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	// The point of this fixture is a file far larger than the page cache, so
	// that reads genuinely miss and take the direct path. Write until that is
	// true rather than assuming a record count achieves it: the file this store
	// produces for a given number of records is not fixed, and has already
	// shrunk by roughly five times as free-space reclamation improved, which
	// silently turned a fixed count into a fixture that no longer applied any
	// pressure at all.
	const (
		minimumRecords = 2048
		maximumRecords = 1 << 20
	)
	records := 0
	for row := 0; row < maximumRecords; row++ {
		key := pressureKey(uint32(row))
		value := fmt.Appendf(nil, `{"id":%d,"version":0,"payload":"%064d"}`, row, row)
		if _, err := collection.Put(key, value); err != nil {
			t.Fatal(err)
		}
		records = row + 1
		if records >= minimumRecords && records%256 == 0 &&
			collection.Stats().FileEnd > 10*collection.Stats().CapacityBytes {
			break
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	initial := collection.Stats()
	if initial.FileEnd <= 10*initial.CapacityBytes || !initial.DirectReads || !initial.DirectWrites {
		t.Fatalf("direct pressure setup after %d records = %+v", records, initial)
	}

	stop := make(chan struct{})
	failures := make(chan error, 8)
	var readers sync.WaitGroup
	for worker := range 4 {
		readers.Add(1)
		go func(worker int) {
			defer readers.Done()
			row := uint32(worker)
			dst := make([]byte, 0, 128)
			for {
				select {
				case <-stop:
					return
				default:
				}
				row = row*1664525 + 1013904223
				key := pressureKey(row % uint32(records))
				snapshot, err := collection.Snapshot()
				if err != nil {
					failures <- err
					return
				}
				value, ok, readErr := snapshot.AppendRaw(dst[:0], key)
				closeErr := snapshot.Close()
				if readErr != nil {
					failures <- readErr
					return
				}
				if closeErr != nil {
					failures <- closeErr
					return
				}
				// The writer briefly deletes one key before reinserting it.
				if ok && len(value) == 0 {
					failures <- fmt.Errorf("empty value for %s", key)
					return
				}
			}
		}(worker)
	}

	for version := 1; version <= 256; version++ {
		row := version & 63
		key := pressureKey(uint32(row))
		if version%32 == 0 {
			if deleted, err := collection.Delete(key); err != nil || !deleted {
				close(stop)
				readers.Wait()
				t.Fatalf("Delete(%s) = (%v,%v)", key, deleted, err)
			}
		}
		value := fmt.Appendf(nil, `{"id":%d,"version":%d,"payload":"%064d"}`, row, version, version)
		if _, err := collection.Put(key, value); err != nil {
			close(stop)
			readers.Wait()
			t.Fatal(err)
		}
		select {
		case err := <-failures:
			close(stop)
			readers.Wait()
			t.Fatal(err)
		default:
		}
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := collection.Stats()
	if stats.ResidentBytes > stats.CapacityBytes || stats.DirtyBytes != 0 ||
		stats.PinnedPages != 0 || stats.Evictions == 0 || stats.PageReads == 0 ||
		!stats.DirectReads || !stats.DirectWrites {
		t.Fatalf("direct pressure stats = %+v", stats)
	}
}

func TestFileStoreDirectIOUring(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-store-direct-ring-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Backend = BackendIOUring
	options.ReadMode = ReadDirectRequire
	options.WriteMode = WriteDirectRequire
	// The test stores tiny inline documents. Keep fixed-buffer registration
	// equally small so a shared CI host's memlock pressure does not decide
	// whether the reopen path can be exercised.
	options.MaxPageSize = options.PageSize
	options.MaxDocumentBytes = options.PageSize
	collection, err := Create(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) ||
		errors.Is(err, storeio.ErrUnavailable) ||
		errors.Is(err, storeio.ErrUnsupported) {
		t.Skipf("test host cannot provide direct io_uring I/O: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if stats := collection.Stats(); stats.Backend != BackendIOUring ||
		stats.ReadBackend != BackendIOUring ||
		!stats.DirectReads || !stats.DirectWrites {
		t.Fatalf("direct io_uring stats = %+v", stats)
	}
	for row := range 64 {
		key := fmt.Sprintf("ring:%02d", row)
		value := fmt.Appendf(nil, `{"v":%d}`, row)
		if _, err := collection.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if errors.Is(err, storeio.ErrDirectIOUnsupported) ||
		errors.Is(err, storeio.ErrUnavailable) ||
		errors.Is(err, storeio.ErrUnsupported) {
		t.Skipf("test host cannot reopen direct io_uring I/O: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	keys := make([]string, 16)
	for row := range keys {
		keys[row] = fmt.Sprintf("ring:%02d", row)
	}
	if queued, err := reopened.PrefetchKeys(keys); err != nil || queued == 0 {
		t.Fatalf("direct io_uring prefetch = (%d,%v)", queued, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for reopened.Stats().AsyncReadBatches == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got, ok, err := reopened.AppendRaw(nil, "ring:63"); err != nil || !ok || string(got) != `{"v":63}` {
		t.Fatalf("direct io_uring read = (%q,%v,%v)", got, ok, err)
	}
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	if _, err := snapshot.RangeRawReadAheadBuffer(nil, func(_, _ []byte) error {
		rows++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	stats := reopened.Stats()
	if rows != 64 || stats.AsyncReadBatches == 0 || stats.LargestReadBatch < 2 ||
		stats.ReadBackend != BackendIOUring {
		t.Fatalf("direct io_uring batch read = rows %d stats %+v", rows, stats)
	}
}
