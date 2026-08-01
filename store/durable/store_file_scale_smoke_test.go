package durable

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
)

// TestFileStoreHundredXResidentSmoke is an explicit storage-pressure gate. It
// builds live collection state whose physical image exceeds the exact cache
// budget by 100x, reopens with an empty cache, probes distant keys, then
// exercises update and delete while eviction is unavoidable.
//
//	VIBEJSON_FILESTORE_100X=1 go test . -run '^TestFileStoreHundredXResidentSmoke$' -v -count=1
func TestFileStoreHundredXResidentSmoke(t *testing.T) {
	if os.Getenv("VIBEJSON_FILESTORE_100X") != "1" {
		t.Skip("set VIBEJSON_FILESTORE_100X=1 to run the resident-pressure gate")
	}
	const records = 10_000
	file, err := os.CreateTemp(t.TempDir(), "file-store-100x-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := fileStoreScaleOptions()
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	options.ResidentBytes = int64(normalized.maxTransactionBytes)

	usageBefore := fileStoreScaleProcessUsage()
	started := time.Now()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 0, 32)
	document := make([]byte, 0, 3072)
	var sourceBytes uint64
	for row := range records {
		key = fmt.Appendf(key[:0], "row:%08d", row)
		document = appendFileStoreScaleDocument(document[:0], row, false)
		sourceBytes += uint64(len(key) + len(document))
		if created, putErr := collection.Put([]byte(string(key)), document); putErr != nil || !created {
			t.Fatalf("Put(%d) = (%v,%v)", row, created, putErr)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	buildStats := collection.Stats()
	if buildStats.FileEnd <= 100*buildStats.CapacityBytes {
		t.Fatalf("file image = %d bytes, need >100x %d-byte cache", buildStats.FileEnd, buildStats.CapacityBytes)
	}
	if sourceBytes <= 100*buildStats.CapacityBytes {
		t.Fatalf("source keys+JSON = %d bytes, need >100x %d-byte cache", sourceBytes, buildStats.CapacityBytes)
	}
	if buildStats.ResidentBytes > buildStats.ReservedBytes ||
		buildStats.ReservedBytes > buildStats.CapacityBytes || buildStats.DirtyBytes != 0 {
		t.Fatalf("unbounded or dirty cache after Flush: %+v", buildStats)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	scanStarted := time.Now()
	scanRows := 0
	var scanBytes uint64
	scanScratch := make([]byte, 0, 3072)
	scanSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	scanScratch, err = scanSnapshot.RangeRawBuffer(scanScratch, func(key, value []byte) error {
		scanRows++
		scanBytes += uint64(len(key) + len(value))
		return nil
	})
	if closeErr := scanSnapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if cap(scanScratch) < 3072 {
		t.Fatalf("read-ahead returned scratch capacity = %d, want at least 3072", cap(scanScratch))
	}
	scanElapsed := time.Since(scanStarted)
	if scanRows != records || scanBytes != sourceBytes {
		t.Fatalf("read-ahead scan = %d rows/%d bytes, want %d/%d", scanRows, scanBytes, records, sourceBytes)
	}
	scanStats := reopened.Stats()
	if scanStats.DirectReads &&
		(scanStats.PrefetchQueued == 0 || scanStats.PrefetchHits+scanStats.CoalescedReads == 0) {
		t.Fatalf("read-ahead scan performed no overlapping reads: %+v", scanStats)
	}
	if !scanStats.DirectReads && scanStats.PrefetchQueued != 0 {
		t.Fatalf("buffered scan bypassed the serial kernel-readahead lane: %+v", scanStats)
	}
	readBuffer := make([]byte, 0, 3072)
	for _, row := range []int{records - 1, 0, records / 2, 17, records - 101, 1} {
		key = fmt.Appendf(key[:0], "row:%08d", row)
		want := appendFileStoreScaleDocument(document[:0], row, false)
		got, ok, readErr := reopened.AppendRaw(readBuffer[:0], []byte(string(key)))
		if readErr != nil || !ok || !bytes.Equal(got, want) {
			t.Fatalf("pressure read %d = (%q,%v,%v)", row, got, ok, readErr)
		}
	}
	updatedRow := records / 2
	updatedKey := fmt.Sprintf("row:%08d", updatedRow)
	updated := appendFileStoreScaleDocument(document[:0], updatedRow, true)
	if created, err := reopened.Put([]byte(updatedKey), updated); err != nil || created {
		t.Fatalf("pressure update = (%v,%v)", created, err)
	}
	if deleted, err := reopened.Delete([]byte("row:00000017")); err != nil || !deleted {
		t.Fatalf("pressure delete = (%v,%v)", deleted, err)
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := reopened.AppendRaw(readBuffer[:0], []byte(updatedKey)); err != nil || !ok || !bytes.Equal(got, updated) {
		t.Fatalf("updated pressure read = (%q,%v,%v)", got, ok, err)
	}
	if _, ok, err := reopened.AppendRaw(readBuffer[:0], []byte("row:00000017")); err != nil || ok {
		t.Fatalf("deleted pressure read = (%v,%v)", ok, err)
	}
	stats := reopened.Stats()
	if stats.CapacityBytes != uint64(options.ResidentBytes) ||
		stats.ResidentBytes > stats.ReservedBytes || stats.ReservedBytes > stats.CapacityBytes ||
		stats.Evictions == 0 || stats.PageReads == 0 || stats.PinnedPages != 0 || stats.DirtyBytes != 0 {
		t.Fatalf("pressure stats = %+v", stats)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	final, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	if got, ok, err := final.AppendRaw(readBuffer[:0], []byte(updatedKey)); err != nil || !ok || !bytes.Equal(got, updated) {
		t.Fatalf("reopened update = (%q,%v,%v)", got, ok, err)
	}
	if _, ok, err := final.AppendRaw(readBuffer[:0], []byte("row:00000017")); err != nil || ok {
		t.Fatalf("reopened delete = (%v,%v)", ok, err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	usageAfter := fileStoreScaleProcessUsage()
	t.Logf("records=%d source=%d source_ratio=%.1fx file=%d cache=%d file_ratio=%.1fx elapsed=%s scan=%s scan_mib_s=%.1f heap_alloc=%d rss=%d rss_peak=%d minor_faults=%d major_faults=%d reads=%d evictions=%d direct_reads=%v direct_writes=%v",
		records, sourceBytes, float64(sourceBytes)/float64(stats.CapacityBytes), stats.FileEnd, stats.CapacityBytes,
		float64(stats.FileEnd)/float64(stats.CapacityBytes),
		time.Since(started), scanElapsed, float64(sourceBytes)/(1<<20)/scanElapsed.Seconds(),
		memory.HeapAlloc, usageAfter.rss, usageAfter.rssPeak,
		fileStoreScaleDelta(usageAfter.minorFaults, usageBefore.minorFaults),
		fileStoreScaleDelta(usageAfter.majorFaults, usageBefore.majorFaults),
		stats.PageReads, stats.Evictions, stats.DirectReads, stats.DirectWrites)
}

type fileStoreProcessUsage struct {
	rss         uint64
	rssPeak     uint64
	minorFaults uint64
	majorFaults uint64
}

func fileStoreScaleDelta(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func fileStoreScaleProcessUsage() fileStoreProcessUsage {
	if runtime.GOOS != "linux" {
		return fileStoreProcessUsage{}
	}
	var usage fileStoreProcessUsage
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil {
				continue
			}
			switch fields[0] {
			case "VmRSS:":
				usage.rss = value << 10
			case "VmHWM:":
				usage.rssPeak = value << 10
			}
		}
	}
	if stat, err := os.ReadFile("/proc/self/stat"); err == nil {
		// A process name may contain spaces and parentheses. Fields after the
		// final ')' begin with field 3 (state); minflt and majflt are 10 and 12.
		if end := strings.LastIndexByte(string(stat), ')'); end >= 0 {
			fields := strings.Fields(string(stat)[end+1:])
			if len(fields) > 9 {
				usage.minorFaults, _ = strconv.ParseUint(fields[7], 10, 64)
				usage.majorFaults, _ = strconv.ParseUint(fields[9], 10, 64)
			}
		}
	}
	return usage
}

func fileStoreScaleOptions() Options {
	return Options{
		Collection: store.Options{ChunkDocuments: 1},
		PageSize:   4096, MaxPageSize: 4096, ResidentBytes: 1 << 20,
		MaxDocumentBytes: 3072, MaxKeyBytes: 32, InlineValueBytes: 3072,
		ReadConcurrency: 4, PrefetchQueue: 64, BufferCount: 64,
		QueueSlots: 16, GroupLimit: 8, Backend: BackendPortable,
		ReadMode: ReadDirectTry, WriteMode: WriteDirectTry,
		MaxSnapshotLeases: 16,
		MaxRetiredExtents: 1 << 15,
	}
}

func appendFileStoreScaleDocument(dst []byte, row int, updated bool) []byte {
	return appendFileStoreScaleDocumentPayload(dst, row, updated, 2048)
}

func appendFileStoreScaleDocumentPayload(dst []byte, row int, updated bool, payloadBytes int) []byte {
	dst = fmt.Appendf(dst, `{"id":%d,"nested":{"group":%d,"state":"s%d"},"payload":"`, row, row%64, row%8)
	for range payloadBytes {
		dst = append(dst, byte('a'+row%26))
	}
	dst = append(dst, `","updated":`...)
	if updated {
		dst = append(dst, "true}"...)
	} else {
		dst = append(dst, "false}"...)
	}
	return dst
}
