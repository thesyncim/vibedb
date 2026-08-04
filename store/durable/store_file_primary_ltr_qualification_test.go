package durable

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

var primaryLTRScale = flag.String(
	"scale", "ci",
	"ordered-primary larger-than-cache qualification scale: ci, 8gb, or a byte size such as 16mb",
)

const (
	primaryLTRDocumentBytes = 7168
	primaryLTRLeafBytes     = int64(storeio.CommonPrimaryLeafWideBytes)
)

type primaryLTRConfig struct {
	targetBytes   int64
	residentBytes int64
	pointReads    int
	warmReads     int
	prefetchReads int
}

type primaryLTRMeasurement struct {
	elapsed    time.Duration
	operations int
	pageReads  uint64
	readBytes  uint64
	dataBytes  uint64
}

// TestFilePrimaryLargerThanCacheQualification builds the publishable
// larger-than-cache artifact and exercises its cold and steady-state read
// story. The default is the CI geometry (256 MB file, 32 MiB cache):
//
//	go test ./store/durable -run '^TestFilePrimaryLargerThanCacheQualification$' -count=1 -v
//
// The manual qualification uses an 8 GB file and a 256 MiB cache:
//
//	go test ./store/durable -run '^TestFilePrimaryLargerThanCacheQualification$' -count=1 -v -scale=8gb
//
// A smaller byte scale is accepted for focused harness development. It keeps
// the same 1:8 cache:file ratio, capped at the CI cache size.
func TestFilePrimaryLargerThanCacheQualification(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("larger-than-cache qualification builds a 256 MiB corpus; it is a geometry/throughput gate, not a race target")
	}
	config := primaryLTRConfigForScale(t, *primaryLTRScale)
	path := filepath.Join(t.TempDir(), "primary-ltr.vibe")
	records := primaryLTRBuild(t, path, config)
	debug.FreeOSMemory()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < config.targetBytes {
		t.Fatalf(
			"primary graph file = %d bytes, want at least %d",
			info.Size(), config.targetBytes,
		)
	}
	if config.residentBytes >= info.Size() {
		t.Fatalf(
			"qualification cache = %d bytes, must be smaller than %d-byte file",
			config.residentBytes, info.Size(),
		)
	}
	t.Logf(
		"scale=%s records=%d file_bytes=%d cache_bytes=%d file_cache_ratio=%.2fx",
		*primaryLTRScale, records, info.Size(), config.residentBytes,
		float64(info.Size())/float64(config.residentBytes),
	)

	order := rand.New(rand.NewPCG(3003, 8)).Perm(records)
	cold := primaryLTRPointLane(
		t, path, config, order, config.pointReads, false,
	)
	primaryLTRReport(t, "cold-random-point", cold)
	if cold.operations != 0 &&
		float64(cold.pageReads)/float64(cold.operations) > 4 {
		t.Fatalf(
			"cold random reads/op = %.3f, exceeds four-page walk bound",
			float64(cold.pageReads)/float64(cold.operations),
		)
	}

	warm := primaryLTRWarmLane(t, path, config, order)
	primaryLTRReport(t, "warm-steady-state", warm)

	scan := primaryLTRScanLane(t, path, config, records)
	primaryLTRReport(t, "ordered-scan", scan)
	t.Logf(
		"lane=ordered-scan throughput_mib_s=%.2f",
		float64(scan.dataBytes)/(1<<20)/scan.elapsed.Seconds(),
	)

	prefetchOff := primaryLTRPointLane(
		t, path, config, order, config.prefetchReads, false,
	)
	prefetchOn := primaryLTRPointLane(
		t, path, config, order, config.prefetchReads, true,
	)
	primaryLTRReport(t, "prefetch-off", prefetchOff)
	primaryLTRReport(t, "prefetch-on", prefetchOn)
	t.Logf(
		"lane=prefetch-delta ns_op_delta=%.0f speedup=%.3fx reads_op_delta=%.3f read_bytes_op_delta=%.0f",
		primaryLTRNSPerOp(prefetchOff)-primaryLTRNSPerOp(prefetchOn),
		primaryLTRNSPerOp(prefetchOff)/primaryLTRNSPerOp(prefetchOn),
		primaryLTRReadsPerOp(prefetchOff)-primaryLTRReadsPerOp(prefetchOn),
		primaryLTRReadBytesPerOp(prefetchOff)-primaryLTRReadBytesPerOp(prefetchOn),
	)
}

func primaryLTRConfigForScale(t testing.TB, scale string) primaryLTRConfig {
	t.Helper()
	config := primaryLTRConfig{
		targetBytes: 256_000_000, residentBytes: 32 << 20,
		pointReads: 8192, warmReads: 50_000, prefetchReads: 8192,
	}
	switch strings.ToLower(scale) {
	case "", "ci":
		return config
	case "8gb", "manual":
		config.targetBytes = 8_000_000_000
		config.residentBytes = 256 << 20
		config.pointReads = 65_536
		config.warmReads = 500_000
		config.prefetchReads = 65_536
		return config
	}
	bytes, err := primaryLTRParseBytes(scale)
	if err != nil || bytes < 8<<20 {
		t.Fatalf("invalid -scale %q: use ci, 8gb, or at least 8mb", scale)
	}
	config.targetBytes = bytes
	config.residentBytes = min(int64(32<<20), max(int64(4<<20), bytes/8))
	config.pointReads = min(config.pointReads, max(512, int(bytes/primaryLTRLeafBytes)))
	config.warmReads = min(config.warmReads, max(2048, config.pointReads*4))
	config.prefetchReads = config.pointReads
	return config
}

func primaryLTRParseBytes(value string) (int64, error) {
	lower := strings.ToLower(strings.TrimSpace(value))
	multiplier := int64(1)
	for suffix, factor := range map[string]int64{
		"gb":  1_000_000_000,
		"mb":  1_000_000,
		"kb":  1_000,
		"gib": 1 << 30,
		"mib": 1 << 20,
		"kib": 1 << 10,
	} {
		if strings.HasSuffix(lower, suffix) {
			multiplier = factor
			lower = strings.TrimSpace(strings.TrimSuffix(lower, suffix))
			break
		}
	}
	var count int64
	if _, err := fmt.Sscan(lower, &count); err != nil ||
		count <= 0 || count > (1<<63-1)/multiplier {
		return 0, fmt.Errorf("invalid byte count")
	}
	return count * multiplier, nil
}

func primaryLTRBuild(
	t testing.TB, path string, config primaryLTRConfig,
) int {
	t.Helper()
	// One deterministic document is deliberately too large for a narrow leaf.
	// Each row therefore contributes at least its canonical document bytes even
	// though the unified wide leaf no longer pads every row to a full 8 KiB
	// page. Size the corpus from that tighter lower bound so format space
	// improvements cannot accidentally turn this into an in-cache test.
	records := int((config.targetBytes + primaryLTRDocumentBytes - 1) /
		primaryLTRDocumentBytes)
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	document := make([]byte, 0, primaryLTRDocumentBytes)
	for row := range records {
		document = primaryLTRDocument(document[:0], row)
		if err := builder.Append(primaryLTRKey(row), document); err != nil {
			t.Fatalf("append row %d: %v", row, err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := primaryLTROptions(config)
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		_ = file.Close()
		t.Fatalf("CreateFromPrimary(%s): %v", *primaryLTRScale, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return records
}

func primaryLTROptions(config primaryLTRConfig) Options {
	return Options{
		Collection: store.Options{ChunkDocuments: 1},
		Backend:    BackendPortable, ReadMode: ReadBuffered,
		WriteMode: WriteBuffered, ResidentBytes: config.residentBytes,
		PageSize: 4096, MaxPageSize: 64 << 10,
		MaxDocumentBytes:  primaryLTRDocumentBytes,
		InlineValueBytes:  primaryLTRDocumentBytes,
		MaxBatchDocuments: 1,
		ReadConcurrency:   4, ReadQueueDepth: 64, PrefetchQueue: 64,
	}
}

func primaryLTRKey(row int) string {
	return fmt.Sprintf("ltr-key-%012d", row)
}

func primaryLTRDocument(dst []byte, row int) []byte {
	dst = fmt.Appendf(dst, `{"id":%d,"group":%d,"payload":"`, row, row%997)
	padding := primaryLTRDocumentBytes - len(dst) - len(`"}`)
	dst = appendWideJSONSafePattern(dst, padding, row*37)
	return append(dst, `"}`...)
}

func primaryLTROpen(
	t testing.TB, path string, config primaryLTRConfig,
) (*os.File, *Collection) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, primaryLTROptions(config))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	return file, collection
}

func primaryLTRClose(t testing.TB, file *os.File, collection *Collection) {
	t.Helper()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func primaryLTRPointLane(
	t testing.TB,
	path string,
	config primaryLTRConfig,
	order []int,
	operations int,
	prefetch bool,
) primaryLTRMeasurement {
	t.Helper()
	file, collection := primaryLTROpen(t, path, config)
	defer primaryLTRClose(t, file, collection)
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	operations = min(operations, len(order))
	keys := make([]string, operations)
	for at := range keys {
		keys[at] = primaryLTRKey(order[at])
	}
	buffer := make([]byte, 0, primaryLTRDocumentBytes)
	base := collection.Stats()
	started := time.Now()
	for first := 0; first < operations; first += 64 {
		last := min(first+64, operations)
		if prefetch {
			primaryLTRPrefetch(t, collection, keys[first:last])
		}
		for at := first; at < last; at++ {
			out, ok, readErr := snapshot.AppendRaw(buffer[:0], []byte(keys[at]))
			if readErr != nil || !ok || len(out) != primaryLTRDocumentBytes {
				t.Fatalf(
					"point read %d = %d bytes, ok=%v err=%v",
					at, len(out), ok, readErr,
				)
			}
			buffer = out
		}
	}
	elapsed := time.Since(started)
	stats := collection.Stats()
	return primaryLTRMeasurement{
		elapsed: elapsed, operations: operations,
		pageReads: stats.PageReads - base.PageReads,
		readBytes: stats.ReadBytes - base.ReadBytes,
		dataBytes: uint64(operations * primaryLTRDocumentBytes),
	}
}

func primaryLTRPrefetch(
	t testing.TB, collection *Collection, keys []string,
) {
	t.Helper()
	var refs [64]storeio.PageRef
	for at, key := range keys {
		route, ok := collection.primaryRouter.Load().Route([]byte(key))
		if !ok {
			t.Fatalf("prefetch route %q not found", key)
		}
		refs[at] = route.Ref
	}
	ordered := refs[:len(keys)]
	slices.SortFunc(ordered, func(a, b storeio.PageRef) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	ordered = slices.Compact(ordered)
	if _, err := collection.cache.Prefetch(ordered); err != nil {
		t.Fatal(err)
	}
}

func primaryLTRWarmLane(
	t testing.TB,
	path string,
	config primaryLTRConfig,
	order []int,
) primaryLTRMeasurement {
	t.Helper()
	file, collection := primaryLTROpen(t, path, config)
	defer primaryLTRClose(t, file, collection)
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	workingSet := min(len(order), int(config.residentBytes/primaryLTRLeafBytes/2))
	keys := make([]string, workingSet)
	buffer := make([]byte, 0, primaryLTRDocumentBytes)
	for at := range keys {
		keys[at] = primaryLTRKey(order[at])
		out, ok, readErr := snapshot.AppendRaw(buffer[:0], []byte(keys[at]))
		if readErr != nil || !ok {
			t.Fatalf("warmup read %d: ok=%v err=%v", at, ok, readErr)
		}
		buffer = out
	}
	want, err := vibejson.AppendCanonicalize(
		nil, primaryLTRDocument(nil, order[0]),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Convert the probe key once, outside the measured closure (borrowed []byte
	// key; a per-call conversion would break the 0-alloc pin).
	probe := []byte(keys[0])
	allocs := testing.AllocsPerRun(1000, func() {
		out, ok, readErr := snapshot.AppendRaw(buffer[:0], probe)
		if readErr != nil || !ok {
			panic("warm allocation probe failed")
		}
		if !bytes.Equal(out, want) {
			panic("warm allocation probe returned wrong document")
		}
	})
	if allocs != 0 {
		t.Fatalf("warm primary point allocations = %g, want 0", allocs)
	}

	base := collection.Stats()
	started := time.Now()
	for at := 0; at < config.warmReads; at++ {
		out, ok, readErr := snapshot.AppendRaw(
			buffer[:0], []byte(keys[at%len(keys)]),
		)
		if readErr != nil || !ok {
			t.Fatalf("warm read %d: ok=%v err=%v", at, ok, readErr)
		}
		buffer = out
	}
	elapsed := time.Since(started)
	stats := collection.Stats()
	return primaryLTRMeasurement{
		elapsed: elapsed, operations: config.warmReads,
		pageReads: stats.PageReads - base.PageReads,
		readBytes: stats.ReadBytes - base.ReadBytes,
		dataBytes: uint64(config.warmReads * primaryLTRDocumentBytes),
	}
}

func primaryLTRScanLane(
	t testing.TB,
	path string,
	config primaryLTRConfig,
	records int,
) primaryLTRMeasurement {
	t.Helper()
	file, collection := primaryLTROpen(t, path, config)
	defer primaryLTRClose(t, file, collection)
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	rows := 0
	var dataBytes uint64
	visit := func(key, value []byte) error {
		rows++
		dataBytes += uint64(len(key) + len(value))
		return nil
	}
	base := collection.Stats()
	started := time.Now()
	if err := snapshot.RangeRaw(visit); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	stats := collection.Stats()
	if rows != records {
		t.Fatalf("ordered scan rows = %d, want %d", rows, records)
	}

	// The allocation probe uses a one-row bounded range so it checks cursor,
	// cache-miss, eviction, and callback machinery without rereading the
	// complete qualification artifact.
	lower := []byte(primaryLTRKey(records / 2))
	upper := []byte(primaryLTRKey(records/2 + 1))
	allocs := testing.AllocsPerRun(100, func() {
		if err := snapshot.rangePrimaryGraph(lower, upper, nil, func(_, _ []byte) error {
			return nil
		}); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ordered primary scan allocations = %g, want 0", allocs)
	}
	return primaryLTRMeasurement{
		elapsed: elapsed, operations: rows,
		pageReads: stats.PageReads - base.PageReads,
		readBytes: stats.ReadBytes - base.ReadBytes,
		dataBytes: dataBytes,
	}
}

func primaryLTRReport(
	t testing.TB, lane string, measurement primaryLTRMeasurement,
) {
	t.Helper()
	t.Logf(
		"lane=%s ns/op=%.0f reads/op=%.3f read-bytes/op=%.0f operations=%d elapsed=%s",
		lane, primaryLTRNSPerOp(measurement),
		primaryLTRReadsPerOp(measurement),
		primaryLTRReadBytesPerOp(measurement),
		measurement.operations, measurement.elapsed,
	)
}

func primaryLTRNSPerOp(measurement primaryLTRMeasurement) float64 {
	if measurement.operations == 0 {
		return 0
	}
	return float64(measurement.elapsed.Nanoseconds()) /
		float64(measurement.operations)
}

func primaryLTRReadsPerOp(measurement primaryLTRMeasurement) float64 {
	if measurement.operations == 0 {
		return 0
	}
	return float64(measurement.pageReads) / float64(measurement.operations)
}

func primaryLTRReadBytesPerOp(measurement primaryLTRMeasurement) float64 {
	if measurement.operations == 0 {
		return 0
	}
	return float64(measurement.readBytes) / float64(measurement.operations)
}
