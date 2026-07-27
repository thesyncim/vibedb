package durable

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func openPrimaryScanPair(
	t testing.TB, count int,
) (*Collection, *Collection, []string, [][]byte) {
	t.Helper()
	built, keys, values := buildFilePrimaryCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
	}
	legacyFile, err := os.OpenFile(
		filepath.Join(t.TempDir(), "legacy-scan.vibe"),
		os.O_RDWR|os.O_CREATE, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacyFile.Close() })
	if _, err := CreateFrom(built, legacyFile, options); err != nil {
		t.Fatal(err)
	}
	primaryFile := createPrimaryPointFile(
		t, built, options, "primary-scan.vibe",
	)
	legacy, err := Open(legacyFile, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	primary, err := Open(primaryFile, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = primary.Close() })
	return legacy, primary, keys, values
}

func TestFilePrimaryOrderedScanDifferential100K(t *testing.T) {
	const count = 100_000
	legacy, primary, keys, values := openPrimaryScanPair(t, count)
	legacySnapshot, err := legacy.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer legacySnapshot.Close()
	primarySnapshot, err := primary.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer primarySnapshot.Close()

	verifyFull := func(label string, snapshot *Snapshot) {
		t.Helper()
		at := 0
		err := snapshot.RangeRaw(func(key, value []byte) error {
			if at >= count || string(key) != keys[at] ||
				!bytes.Equal(value, values[at]) {
				return fmt.Errorf(
					"%s row %d = %q/%q, want %q/%q",
					label, at, key, value, keys[at], values[at],
				)
			}
			at++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if at != count {
			t.Fatalf("%s rows = %d, want %d", label, at, count)
		}
	}
	verifyFull("legacy", legacySnapshot)
	verifyFull("primary", primarySnapshot)

	random := rand.New(rand.NewSource(2203))
	for sample := 0; sample < 1_000; sample++ {
		first := random.Intn(count)
		last := min(first+random.Intn(128), count)
		lower := []byte(keys[first])
		var upper []byte
		if last < count {
			upper = []byte(keys[last])
		}
		at := first
		err := primarySnapshot.rangePrimaryGraph(
			lower, upper, nil,
			func(key, value []byte) error {
				if at >= last || string(key) != keys[at] ||
					!bytes.Equal(value, values[at]) {
					return fmt.Errorf(
						"range %d row %d = %q/%q",
						sample, at, key, value,
					)
				}
				at++
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if at != last {
			t.Fatalf(
				"range %d stopped at %d, want %d", sample, at, last,
			)
		}
	}

	for sample := 0; sample < 1_000; sample++ {
		selected := random.Intn(count)
		// Dropping one or two decimal digits exercises prefixes spanning leaf
		// boundaries without turning each sample into another full scan.
		drop := 1 + random.Intn(2)
		prefix := []byte(keys[selected][:len(keys[selected])-drop])
		first := selected / 10
		width := 10
		if drop == 2 {
			first = selected / 100
			width = 100
		}
		first *= width
		last := min(first+width, count)
		at := first
		err := primarySnapshot.rangePrimaryGraph(
			nil, nil, prefix,
			func(key, value []byte) error {
				if at >= last || string(key) != keys[at] ||
					!bytes.Equal(value, values[at]) {
					return fmt.Errorf(
						"prefix %d row %d = %q/%q",
						sample, at, key, value,
					)
				}
				at++
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if at != last {
			t.Fatalf(
				"prefix %d stopped at %d, want %d", sample, at, last,
			)
		}
	}
}

func TestFilePrimaryOrderedScanAllocatesZero(t *testing.T) {
	_, primary, _, _ := openPrimaryScanPair(t, 2_000)
	snapshot, err := primary.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	visit := func(_, _ []byte) error { return nil }
	if err := snapshot.RangeRaw(visit); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := snapshot.RangeRaw(visit); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ordered primary full scan allocations = %g, want 0", allocs)
	}
	lower := []byte("primary-key-000000500")
	upper := []byte("primary-key-000001500")
	allocs = testing.AllocsPerRun(100, func() {
		if err := snapshot.rangePrimaryGraph(
			lower, upper, nil, visit,
		); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ordered primary bounded scan allocations = %g, want 0", allocs)
	}
	prefix := []byte("primary-key-000001")
	allocs = testing.AllocsPerRun(100, func() {
		if err := snapshot.rangePrimaryGraph(
			nil, nil, prefix, visit,
		); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ordered primary prefix scan allocations = %g, want 0", allocs)
	}
}

func benchmarkFilePrimaryOrderedScan(b *testing.B, allBytes bool) {
	const documents = 100_000
	built, _, _ := buildFilePrimaryCorpus(b, documents)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
	}
	file := createPrimaryPointFile(b, built, options, "scan-bench.vibe")
	collection, err := Open(file, options)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	var sink byte
	visited := 0
	visit := func(_, value []byte) error {
		if allBytes {
			sink ^= touchPrimaryScanBytes(value)
		} else if len(value) != 0 {
			sink ^= value[0]
		}
		visited++
		return nil
	}
	if err := snapshot.RangeRaw(visit); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := snapshot.RangeRaw(visit); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if visited != (b.N+1)*documents {
		b.Fatalf("visited %d documents", visited)
	}
	b.ReportMetric(
		float64(b.Elapsed().Nanoseconds())/float64(b.N*documents),
		"ns/doc",
	)
	storeScanSink ^= sink
}

var storeScanSink byte

func touchPrimaryScanBytes(value []byte) byte {
	var sink byte
	for _, valueByte := range value {
		sink ^= valueByte
	}
	return sink
}

func BenchmarkFilePrimaryOrderedScan(b *testing.B) {
	benchmarkFilePrimaryOrderedScan(b, false)
}

func BenchmarkFilePrimaryOrderedScanAllBytes(b *testing.B) {
	benchmarkFilePrimaryOrderedScan(b, true)
}
