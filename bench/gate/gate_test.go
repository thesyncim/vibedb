package main

import "testing"

// sampleOutput is representative go test -benchmem output: mixed custom metrics
// (MB/s, ns/doc), a zero-allocation line, and a non-zero line. The gate must
// read allocs/op and B/op regardless of how many custom metrics precede them.
const sampleOutput = `goos: darwin
goarch: arm64
pkg: github.com/thesyncim/vibedb/store/durable
BenchmarkFileStoreCreateFromFloor-16              	       5	  17626433 ns/op	   8.83 MB/s	     881.3 ns/doc	 1678331 B/op	     124 allocs/op
BenchmarkUnifiedScanAllBytesLowCardinality-16     	       5	  40205812 ns/op	 618.84 MB/s	     248.8 bytes/doc	     402.1 ns/doc	       0 B/op	       0 allocs/op
PASS
ok  	github.com/thesyncim/vibedb/store/durable	4.834s
`

func TestParseBenchmarksReadsAllocsAndBytesPastCustomMetrics(t *testing.T) {
	got, err := parseBenchmarks(sampleOutput)
	if err != nil {
		t.Fatalf("parseBenchmarks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("parsed %d benchmarks, want 2: %#v", len(got), got)
	}
	fs, ok := got["BenchmarkFileStoreCreateFromFloor"]
	if !ok {
		t.Fatalf("missing FileStoreCreateFromFloor; got %#v", got)
	}
	if fs.allocs != 124 || fs.bytes != 1678331 {
		t.Fatalf("FileStore = %d allocs / %d B, want 124 / 1678331", fs.allocs, fs.bytes)
	}
	scan, ok := got["BenchmarkUnifiedScanAllBytesLowCardinality"]
	if !ok {
		t.Fatalf("missing UnifiedScan; got %#v", got)
	}
	if scan.allocs != 0 || scan.bytes != 0 {
		t.Fatalf("UnifiedScan = %d allocs / %d B, want 0 / 0", scan.allocs, scan.bytes)
	}
}

func TestParseBenchmarksFailsClosedWithoutBenchmem(t *testing.T) {
	// A benchmark line missing B/op and allocs/op (no -benchmem) must be an
	// error, not a silently dropped row.
	const line = "BenchmarkNoMem-8   \t100\t  1234 ns/op\n"
	if _, err := parseBenchmarks(line); err == nil {
		t.Fatal("expected an error for a line without -benchmem metrics")
	}
}

func TestStripProcSuffix(t *testing.T) {
	cases := map[string]string{
		"BenchmarkFoo-16":       "BenchmarkFoo",
		"BenchmarkFoo-1":        "BenchmarkFoo",
		"BenchmarkFoo":          "BenchmarkFoo",
		"BenchmarkFoo/case-abc": "BenchmarkFoo/case-abc",
		"BenchmarkFoo-":         "BenchmarkFoo-",
	}
	for in, want := range cases {
		if got := stripProcSuffix(in); got != want {
			t.Errorf("stripProcSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBytesRegressedTolerance(t *testing.T) {
	cases := []struct {
		name       string
		base, head int64
		want       bool
	}{
		{"equal", 1000, 1000, false},
		{"under 5%", 1000, 1040, false},
		{"exactly 5%", 1000, 1050, false},
		{"just over 5%", 1000, 1051, true},
		{"decrease", 1000, 900, false},
		{"zero base zero head", 0, 0, false},
		{"zero base nonzero head", 0, 1, true},
	}
	for _, c := range cases {
		if got := bytesRegressed(c.base, c.head); got != c.want {
			t.Errorf("%s: bytesRegressed(%d, %d) = %v, want %v",
				c.name, c.base, c.head, got, c.want)
		}
	}
}

func TestCompareVerdicts(t *testing.T) {
	benches := []curatedBench{
		{pkg: "./a", name: "BenchmarkZeroAlloc", policy: metricPolicy{gateAllocs: true, gateBytes: true}},
		{pkg: "./a", name: "BenchmarkBytesOnly", policy: metricPolicy{gateAllocs: false, gateBytes: true}},
		{pkg: "./a", name: "BenchmarkGone", policy: metricPolicy{gateAllocs: true, gateBytes: true}},
	}

	t.Run("clean run passes", func(t *testing.T) {
		base := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		head := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			// allocs/op rises but is not gated for this benchmark; B/op holds
			// within tolerance.
			"BenchmarkBytesOnly": {allocs: 130, bytes: 1040},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		cs := compare(benches, base, head)
		if regressed(cs) {
			t.Fatalf("expected pass, got regressions: %+v", cs)
		}
	})

	t.Run("gated allocs increase fails", func(t *testing.T) {
		base := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		head := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 1, bytes: 16}, // regressed
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		cs := compare(benches, base, head)
		if !regressed(cs) {
			t.Fatal("expected a regression for the gated allocs increase")
		}
		if !cs[0].allocsFail || !cs[0].bytesFail {
			t.Fatalf("zero-alloc benchmark should fail both metrics: %+v", cs[0])
		}
	})

	t.Run("bytes-only benchmark fails on byte blowup", func(t *testing.T) {
		base := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		head := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 2000}, // +100% B/op
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		cs := compare(benches, base, head)
		if !regressed(cs) {
			t.Fatal("expected a regression for the byte blowup")
		}
	})

	t.Run("missing measurement fails", func(t *testing.T) {
		base := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			"BenchmarkGone":      {allocs: 3, bytes: 64},
		}
		head := map[string]result{
			"BenchmarkZeroAlloc": {allocs: 0, bytes: 0},
			"BenchmarkBytesOnly": {allocs: 124, bytes: 1000},
			// BenchmarkGone absent on head.
		}
		cs := compare(benches, base, head)
		if !regressed(cs) {
			t.Fatal("expected a regression when a benchmark is missing on head")
		}
		var gone comparison
		for _, c := range cs {
			if c.bench.name == "BenchmarkGone" {
				gone = c
			}
		}
		if !gone.missing() || !gone.failed() {
			t.Fatalf("BenchmarkGone should be missing and failed: %+v", gone)
		}
	})
}

// TestGroupByRunCollapsesInvocations checks that benchmarks sharing a package
// and -benchtime run in one go test invocation, so per-package fixtures build
// once.
func TestGroupByRunCollapsesInvocations(t *testing.T) {
	groups := groupByRun([]curatedBench{
		{pkg: "./store/durable", name: "A", benchtime: "5x"},
		{pkg: "./store/durable", name: "B", benchtime: "5x"},
		{pkg: "./store/durable", name: "C", benchtime: "20x"},
		{pkg: "./query", name: "D", benchtime: "20x"},
	})
	if len(groups) != 3 {
		t.Fatalf("got %d groups, want 3: %+v", len(groups), groups)
	}
	if groups[0].pkg != "./store/durable" || len(groups[0].names) != 2 {
		t.Fatalf("first group should batch the two 5x durable benchmarks: %+v", groups[0])
	}
	if got := benchPattern([]string{"A", "B"}); got != "^(A|B)$" {
		t.Fatalf("benchPattern = %q, want ^(A|B)$", got)
	}
}

// TestCuratedSetIsRunnableShape guards the curated table's own invariants: every
// entry names a Benchmark, sets a -benchtime, and gates at least one metric.
func TestCuratedSetIsRunnableShape(t *testing.T) {
	if len(curated) == 0 {
		t.Fatal("curated set is empty")
	}
	seen := map[string]bool{}
	for _, b := range curated {
		if seen[b.name] {
			t.Errorf("duplicate curated benchmark %q", b.name)
		}
		seen[b.name] = true
		if b.pkg == "" || b.benchtime == "" {
			t.Errorf("%q: pkg and benchtime are required", b.name)
		}
		if len(b.name) < len("Benchmark") || b.name[:len("Benchmark")] != "Benchmark" {
			t.Errorf("%q: curated name must start with Benchmark", b.name)
		}
		if !b.policy.gateAllocs && !b.policy.gateBytes {
			t.Errorf("%q: gates no metric", b.name)
		}
		if !b.policy.gateAllocs && b.policy.allocsNote == "" {
			t.Errorf("%q: allocs is not gated but carries no exclusion note", b.name)
		}
	}
}
