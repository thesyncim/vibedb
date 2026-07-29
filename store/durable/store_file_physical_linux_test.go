//go:build linux

package durable

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"golang.org/x/sys/unix"
)

// TestFileStorePhysicalHundredXMemory is the expensive, cgroup-constrained
// larger-than-RAM proof. Unlike TestFileStoreHundredXResidentSmoke, this gate
// compares live source and allocated file blocks with the container's complete
// memory limit and peak, not only with collection's page-cache arena.
//
// Run it through scripts/run-filestore-physical-scale.sh. The script compiles
// outside the constrained cgroup, uses a Linux volume that accepts O_DIRECT,
// and defaults to a database physically larger than 100 times a 64 MiB limit.
func TestFileStorePhysicalHundredXMemory(t *testing.T) {
	if os.Getenv("VIBEJSON_FILESTORE_PHYSICAL_100X") != "1" {
		t.Skip("run scripts/run-filestore-physical-scale.sh for the physical larger-than-RAM gate")
	}
	memoryLimit, err := fileStoreCgroupValue("memory.max", "memory.limit_in_bytes")
	if err != nil || memoryLimit == 0 || memoryLimit >= 1<<60 {
		t.Fatalf("finite cgroup memory limit is required: limit=%d err=%v", memoryLimit, err)
	}
	ratio := fileStoreScaleEnvUint(t, "VIBEJSON_FILESTORE_PHYSICAL_RATIO", 100)
	payloadBytes := fileStoreScaleEnvUint(t, "VIBEJSON_FILESTORE_PHYSICAL_PAYLOAD", (3<<20)-4096)
	if ratio == 0 || ratio > 1000 || payloadBytes < 4096 || payloadBytes > (4<<20)-4096 ||
		memoryLimit > math.MaxUint64/ratio {
		t.Fatalf("invalid physical scale geometry: memory=%d ratio=%d payload=%d", memoryLimit, ratio, payloadBytes)
	}
	targetBytes := memoryLimit * ratio
	records64 := targetBytes/payloadBytes + 1
	if records64 > uint64(math.MaxInt) {
		t.Fatalf("physical scale record count overflows int: %d", records64)
	}
	records := int(records64)

	directory := t.TempDir()
	var filesystem unix.Statfs_t
	if err := unix.Statfs(directory, &filesystem); err != nil {
		t.Fatal(err)
	}
	available := uint64(filesystem.Bavail) * uint64(filesystem.Bsize)
	requiredDisk := targetBytes + targetBytes/2
	if available < requiredDisk {
		t.Fatalf("physical scale gate needs at least %d free bytes, have %d", requiredDisk, available)
	}
	file, err := os.CreateTemp(directory, "file-store-physical-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	maxDocumentBytes := int(payloadBytes) + 512
	overflowPayload := (64 << 10) - storeio.PageHeaderSize - storeio.PageTrailerSize -
		storeio.OverflowPagePayloadHeaderSize
	maxTransactionPages := 1 + (maxDocumentBytes-1)/overflowPayload + 48 + 24
	bufferCount := 1
	for bufferCount <= maxTransactionPages {
		bufferCount <<= 1
	}
	options := Options{
		Collection: store.Options{ChunkDocuments: 64},
		Indexes: []store.IndexDefinition{
			{Name: "nested_group", Paths: []string{"/nested/group"}},
		},
		PageSize: 4096, MaxPageSize: 64 << 10, ResidentBytes: 6 << 20,
		MaxDocumentBytes: maxDocumentBytes, MaxKeyBytes: 32, InlineValueBytes: 512,
		ReadConcurrency: 4, PrefetchQueue: 64, BufferCount: bufferCount,
		QueueSlots: 4, GroupLimit: 2, Backend: BackendAuto,
		ReadMode: ReadDirectRequire, WriteMode: WriteDirectRequire,
		MaxSnapshotLeases: 16, MaxRetiredExtents: 8192,
	}

	started := time.Now()
	db, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 0, 32)
	document := make([]byte, 0, int(payloadBytes)+256)
	var sourceBytes uint64
	nextProgress := uint64(1 << 30)
	for row := range records {
		key = fmt.Appendf(key[:0], "row:%08d", row)
		document = appendFileStoreScaleDocumentPayload(document[:0], row, false, int(payloadBytes))
		sourceBytes += uint64(len(key) + len(document))
		if created, putErr := db.Put(string(key), document); putErr != nil || !created {
			t.Fatalf("Put(%d) = (%v,%v)", row, created, putErr)
		}
		if sourceBytes >= nextProgress {
			t.Logf("build progress source=%d records=%d elapsed=%s", sourceBytes, row+1, time.Since(started))
			nextProgress += 1 << 30
		}
	}
	if sourceBytes <= targetBytes {
		t.Fatalf("source = %d, want more than %dx cgroup memory %d", sourceBytes, ratio, memoryLimit)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	buildStats := db.Stats()
	if !buildStats.DirectReads || !buildStats.DirectWrites {
		t.Fatalf("physical gate requires direct I/O: %+v", buildStats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Measure one live instance at a time. Close releases the anonymous arenas;
	// dropping the owner and forcing a collection releases its Go control
	// slices before the reopen, as a long-running service naturally would.
	db = nil
	debug.FreeOSMemory()

	allocatedBytes, err := fileStoreAllocatedBytes(file)
	if err != nil {
		t.Fatal(err)
	}
	if allocatedBytes <= targetBytes {
		t.Fatalf("allocated file = %d, want more than %dx cgroup memory %d", allocatedBytes, ratio, memoryLimit)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	readBuffer := make([]byte, 0, int(payloadBytes)+256)
	for _, row := range []int{
		records - 1, 0, records / 2, min(17, records-1), max(0, records-101), min(1, records-1),
	} {
		key = fmt.Appendf(key[:0], "row:%08d", row)
		want := appendFileStoreScaleDocumentPayload(document[:0], row, false, int(payloadBytes))
		got, ok, readErr := reopened.AppendRaw(readBuffer[:0], string(key))
		if readErr != nil || !ok || !bytes.Equal(got, want) {
			t.Fatalf("pressure read %d = (%d bytes,%v,%v)", row, len(got), ok, readErr)
		}
	}

	needleEntries, err := vibejson.RequiredIndexEntries([]byte("17"))
	if err != nil {
		t.Fatal(err)
	}
	needle, err := vibejson.BuildIndex([]byte("17"), make([]vibejson.IndexEntry, needleEntries))
	if err != nil {
		t.Fatal(err)
	}
	masks, err := collectionIndexMasks(reopened, nil, "nested_group", needle)
	if err != nil {
		t.Fatal(err)
	}
	indexedRows := 0
	for _, mask := range masks {
		indexedRows += bits.OnesCount64(mask.Bits)
	}
	wantIndexedRows := (records + 63 - 17) / 64
	if indexedRows != wantIndexedRows {
		t.Fatalf("nested index rows = %d, want %d", indexedRows, wantIndexedRows)
	}

	updatedRow := records / 2
	updatedKey := fmt.Sprintf("row:%08d", updatedRow)
	updated := appendFileStoreScaleDocumentPayload(document[:0], updatedRow, true, int(payloadBytes))
	if created, err := reopened.Put(updatedKey, updated); err != nil || created {
		t.Fatalf("pressure update = (%v,%v)", created, err)
	}
	deletedKey := fmt.Sprintf("row:%08d", min(17, records-1))
	if deleted, err := reopened.Delete(deletedKey); err != nil || !deleted {
		t.Fatalf("pressure delete = (%v,%v)", deleted, err)
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := reopened.Stats()
	if stats.ResidentBytes > stats.CapacityBytes || stats.Evictions == 0 ||
		stats.PinnedPages != 0 || stats.DirtyBytes != 0 {
		t.Fatalf("unbounded or dirty cache after physical pressure: %+v", stats)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened = nil
	debug.FreeOSMemory()

	final, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := final.AppendRaw(readBuffer[:0], updatedKey); err != nil || !ok || !bytes.Equal(got, updated) {
		t.Fatalf("reopened update = (%d bytes,%v,%v)", len(got), ok, err)
	}
	if _, ok, err := final.AppendRaw(readBuffer[:0], deletedKey); err != nil || ok {
		t.Fatalf("reopened delete = (%v,%v)", ok, err)
	}
	if err := final.Close(); err != nil {
		t.Fatal(err)
	}

	memoryPeak, peakErr := fileStoreCgroupValue("memory.peak", "memory.max_usage_in_bytes")
	if peakErr != nil || memoryPeak == 0 || memoryPeak > memoryLimit {
		t.Fatalf("invalid cgroup peak: peak=%d limit=%d err=%v", memoryPeak, memoryLimit, peakErr)
	}
	t.Logf("records=%d source=%d file=%d allocated=%d memory_limit=%d memory_peak=%d source_peak_ratio=%.1fx allocated_peak_ratio=%.1fx elapsed=%s reads=%d evictions=%d backend=%v",
		records, sourceBytes, stats.FileEnd, allocatedBytes, memoryLimit, memoryPeak,
		float64(sourceBytes)/float64(memoryPeak), float64(allocatedBytes)/float64(memoryPeak),
		time.Since(started), stats.PageReads, stats.Evictions, stats.Backend)
}

func fileStoreScaleEnvUint(t *testing.T, name string, fallback uint64) uint64 {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("%s = %q: %v", name, value, err)
	}
	return parsed
}

func fileStoreCgroupValue(v2, v1 string) (uint64, error) {
	paths := []string{
		filepath.Join("/sys/fs/cgroup", v2),
		filepath.Join("/sys/fs/cgroup/memory", v1),
	}
	var last error
	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			last = err
			continue
		}
		value := strings.TrimSpace(string(src))
		if value == "max" {
			return 0, nil
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			last = err
			continue
		}
		return parsed, nil
	}
	return 0, last
}

func fileStoreAllocatedBytes(file *os.File) (uint64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Blocks) * 512, nil
}
