// Command gate is vibedb's allocation-regression gate. It runs a curated set of
// benchmarks on the working revision and on a base revision, then fails when a
// gated allocs/op count rises at all or a gated B/op count rises by more than
// the byte tolerance. It never reads or compares ns/op: allocation and byte
// counts are reproducible across machine load, while timing is not, so timing
// has no place in a pass/fail gate.
//
// The comparison is A/B by construction. There is no checked-in baseline file
// because allocation counts legitimately differ across GOOS (a darwin-only
// hole-punch path allocates differently than its linux fallback), so a baseline
// captured on one platform would misjudge another. Instead the gate measures
// the base revision and the working revision on the same machine, in the same
// invocation, and compares those two measurements to each other.
//
// The base revision is checked out into a throwaway git worktree, so the gate
// never stashes, resets, or otherwise disturbs uncommitted work in the primary
// tree.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// bytesToleranceNumerator/Denominator express the B/op regression threshold as
// an exact rational (5%) so the comparison never depends on float rounding of
// the tolerance itself.
const (
	bytesToleranceNumerator   = 105
	bytesToleranceDenominator = 100
)

// metricPolicy records which measured metrics a curated benchmark is gated on.
// A metric that is measured but not gated is still printed for context.
type metricPolicy struct {
	gateAllocs bool
	gateBytes  bool
	// allocsNote documents why allocs/op is reported but not gated, when it is
	// not gated. It exists so the exclusion is visible in the tool, not only in
	// prose.
	allocsNote string
}

// curatedBench is one benchmark the gate measures on both revisions.
type curatedBench struct {
	pkg       string // package path relative to the module root
	name      string // exact benchmark function name, without the -GOMAXPROCS suffix
	benchtime string // fixed -benchtime value chosen for reproducible allocs/op
	policy    metricPolicy
}

// curated is the gate's benchmark set. Every entry was verified to exist and,
// for gated allocs/op, to produce an identical count across repeated runs at
// its listed -benchtime. See README.md for the selection and exclusion record.
var curated = []curatedBench{
	{
		pkg:       "./store/durable",
		name:      "BenchmarkFileStoreCreateFromFloor",
		benchtime: "5x",
		policy: metricPolicy{
			gateAllocs: false,
			gateBytes:  true,
			allocsNote: "bulk-build allocs/op is fractional (measured 123-124 as -benchtime rises); gated on B/op only",
		},
	},
	{
		pkg:       "./store/durable",
		name:      "BenchmarkUnifiedScanAllBytesLowCardinality",
		benchtime: "5x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
	{
		pkg:       "./store/durable",
		name:      "BenchmarkUnifiedScanAllBytesHighCardinality",
		benchtime: "5x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
	{
		pkg:       "./internal/storeio",
		name:      "BenchmarkCompactPrimaryStripePointRead",
		benchtime: "20x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
	{
		pkg:       "./query",
		name:      "BenchmarkStoreQueryIndexedProjection",
		benchtime: "20x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
	{
		pkg:       "./query",
		name:      "BenchmarkStoreQueryIndexedCount",
		benchtime: "20x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
	{
		pkg:       "./query",
		name:      "BenchmarkApplyKernelLeftExactMemoizationWarm",
		benchtime: "20x",
		policy:    metricPolicy{gateAllocs: true, gateBytes: true},
	},
}

func main() {
	base := flag.String("base", envDefault("GATE_BASE", ""),
		"git revision to compare against; empty means the merge-base of HEAD and main")
	keep := flag.Bool("keep", false,
		"keep the temporary base worktree instead of removing it (for debugging)")
	flag.Parse()

	hadRegression, err := run(*base, *keep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench-gate: %v\n", err)
		os.Exit(2)
	}
	if hadRegression {
		os.Exit(1)
	}
}

func envDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// run measures the working tree and the base revision, prints the comparison
// table, and reports whether any gated metric regressed. It returns instead of
// calling os.Exit so its deferred worktree cleanup always runs, including on the
// regression path — the tool's core scenario, where an os.Exit here would skip
// the defer and leak the base checkout. main maps the outcome to a process exit
// code: a non-nil error becomes 2 (the gate could not run), a true return
// becomes 1 (it ran and found a regression), and false becomes 0 (every gated
// metric held).
func run(baseFlag string, keep bool) (bool, error) {
	root, err := gitTopLevel(".")
	if err != nil {
		return false, err
	}

	baseSHA, baseDesc, err := resolveBase(root, baseFlag)
	if err != nil {
		return false, err
	}

	fmt.Fprintf(os.Stderr, "bench-gate: GOEXPERIMENT=%s\n", benchExperiment())
	fmt.Fprintf(os.Stderr, "bench-gate: head = working tree at %s\n", root)
	fmt.Fprintf(os.Stderr, "bench-gate: base = %s (%s)\n", baseSHA, baseDesc)

	head, err := measureTree(root, "head")
	if err != nil {
		return false, fmt.Errorf("measuring working tree: %w", err)
	}

	worktree, cleanup, err := addWorktree(root, baseSHA)
	if err != nil {
		return false, err
	}
	if keep {
		fmt.Fprintf(os.Stderr, "bench-gate: keeping base worktree at %s\n", worktree)
	} else {
		defer cleanup()
	}

	baseResults, err := measureTree(worktree, "base")
	if err != nil {
		return false, fmt.Errorf("measuring base revision: %w", err)
	}

	gate := compare(curated, baseResults, head)
	printTable(os.Stdout, gate, baseSHA, baseDesc)

	return regressed(gate), nil
}

// result is the gated measurement of one benchmark on one revision.
type result struct {
	name   string
	allocs int64
	bytes  int64
}

// measureTree runs every curated benchmark group in one tree and returns the
// measurements keyed by benchmark name. label only tags progress output.
func measureTree(dir, label string) (map[string]result, error) {
	out := map[string]result{}
	for _, group := range groupByRun(curated) {
		pattern := benchPattern(group.names)
		fmt.Fprintf(os.Stderr, "bench-gate: [%s] go test %s -bench %s -benchtime=%s\n",
			label, group.pkg, pattern, group.benchtime)
		text, err := goTestBench(dir, group.pkg, pattern, group.benchtime)
		if err != nil {
			return nil, fmt.Errorf("go test %s: %w", group.pkg, err)
		}
		parsed, err := parseBenchmarks(text)
		if err != nil {
			return nil, fmt.Errorf("parsing %s output: %w", group.pkg, err)
		}
		for name, r := range parsed {
			out[name] = r
		}
	}
	return out, nil
}

// runGroup is a set of curated benchmarks that share a package and -benchtime
// and can therefore run in a single go test invocation.
type runGroup struct {
	pkg       string
	benchtime string
	names     []string
}

// groupByRun collapses the curated set into the minimum number of go test
// invocations. Grouping keeps expensive per-package fixtures from being rebuilt
// once per benchmark.
func groupByRun(benches []curatedBench) []runGroup {
	index := map[string]int{}
	var groups []runGroup
	for _, b := range benches {
		key := b.pkg + "\x00" + b.benchtime
		i, ok := index[key]
		if !ok {
			index[key] = len(groups)
			groups = append(groups, runGroup{pkg: b.pkg, benchtime: b.benchtime})
			i = len(groups) - 1
		}
		groups[i].names = append(groups[i].names, b.name)
	}
	return groups
}

// benchPattern builds an anchored alternation so go test runs exactly the named
// benchmarks and nothing that merely shares a prefix.
func benchPattern(names []string) string {
	return "^(" + strings.Join(names, "|") + ")$"
}

// benchExperiment selects the same backend for baseline and candidate builds.
func benchExperiment() string {
	if experiment := os.Getenv("GOEXPERIMENT"); experiment != "" {
		return experiment
	}
	return "simd"
}

// goTestBench runs one benchmark group and returns its stdout. It fixes
// -run '^$' (no tests), -count=1 (no result caching), and -benchmem (so B/op
// and allocs/op are always present).
func goTestBench(dir, pkg, pattern, benchtime string) (string, error) {
	cmd := exec.Command("go", "test",
		"-run", "^$",
		"-bench", pattern,
		"-benchmem",
		"-benchtime", benchtime,
		"-count=1",
		pkg,
	)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOEXPERIMENT="+benchExperiment())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w\n%s", err, out)
	}
	return string(out), nil
}

// parseBenchmarks reads go test -benchmem output and returns the allocs/op and
// B/op for every reported benchmark line. It fails closed: a benchmark line
// without both metrics is an error, not a silently dropped row.
func parseBenchmarks(text string) (map[string]result, error) {
	out := map[string]result{}
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := stripProcSuffix(fields[0])
		var (
			r          = result{name: name}
			haveAllocs bool
			haveBytes  bool
		)
		for i := 1; i < len(fields); i++ {
			switch fields[i] {
			case "allocs/op":
				v, perr := strconv.ParseInt(fields[i-1], 10, 64)
				if perr != nil {
					return nil, fmt.Errorf("benchmark %q: unparseable allocs/op %q", name, fields[i-1])
				}
				r.allocs, haveAllocs = v, true
			case "B/op":
				v, perr := strconv.ParseInt(fields[i-1], 10, 64)
				if perr != nil {
					return nil, fmt.Errorf("benchmark %q: unparseable B/op %q", name, fields[i-1])
				}
				r.bytes, haveBytes = v, true
			}
		}
		if !haveAllocs || !haveBytes {
			return nil, fmt.Errorf("benchmark %q line lacks -benchmem metrics: %q", name, line)
		}
		out[name] = r
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// stripProcSuffix removes the trailing -GOMAXPROCS suffix go test appends to
// every benchmark name. It only strips when the suffix is all digits, so a
// benchmark whose name legitimately ends in -word is left intact.
func stripProcSuffix(name string) string {
	i := strings.LastIndex(name, "-")
	if i < 0 || i == len(name)-1 {
		return name
	}
	for _, c := range name[i+1:] {
		if c < '0' || c > '9' {
			return name
		}
	}
	return name[:i]
}

// comparison is the gate verdict for one curated benchmark.
type comparison struct {
	bench      curatedBench
	base, head result
	baseFound  bool
	headFound  bool
	allocsFail bool
	bytesFail  bool
}

// missing reports whether either revision failed to produce a measurement, in
// which case the benchmark cannot be verified and the gate must fail.
func (c comparison) missing() bool { return !c.baseFound || !c.headFound }

// failed reports whether this benchmark blocks the gate.
func (c comparison) failed() bool { return c.missing() || c.allocsFail || c.bytesFail }

// compare joins base and head measurements against the curated policies. A
// benchmark absent from either side is a failure: a gate that silently ignores
// a benchmark it could not measure is worse than one that stops.
func compare(benches []curatedBench, base, head map[string]result) []comparison {
	out := make([]comparison, 0, len(benches))
	for _, b := range benches {
		c := comparison{bench: b}
		c.base, c.baseFound = base[b.name]
		c.head, c.headFound = head[b.name]
		if c.missing() {
			out = append(out, c)
			continue
		}
		if b.policy.gateAllocs && c.head.allocs > c.base.allocs {
			c.allocsFail = true
		}
		if b.policy.gateBytes && bytesRegressed(c.base.bytes, c.head.bytes) {
			c.bytesFail = true
		}
		out = append(out, c)
	}
	return out
}

// bytesRegressed reports whether head exceeds base by more than the byte
// tolerance. base==0 collapses to "any non-zero head is a regression", which is
// the correct verdict for a benchmark documented as zero-allocation.
func bytesRegressed(base, head int64) bool {
	return head*bytesToleranceDenominator > base*bytesToleranceNumerator
}

// regressed reports whether any benchmark blocks the gate.
func regressed(cs []comparison) bool {
	for _, c := range cs {
		if c.failed() {
			return true
		}
	}
	return false
}

// printTable writes the human-readable per-benchmark comparison and a final
// PASS/FAIL line. It always prints, whether or not the gate passed, so a green
// run documents exactly what was measured.
func printTable(w *os.File, cs []comparison, baseSHA, baseDesc string) {
	fmt.Fprintf(w, "\nAllocation gate: head (working tree) vs base %s (%s)\n",
		shortSHA(baseSHA), baseDesc)
	fmt.Fprintln(w, "Gated: allocs/op must not increase; B/op must not increase by more than 5%. ns/op is never gated.")
	fmt.Fprintln(w)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BENCHMARK\tALLOCS/OP base→head\tB/OP base→head\tΔB/OP\tVERDICT")
	sorted := append([]comparison(nil), cs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].bench.name < sorted[j].bench.name
	})
	for _, c := range sorted {
		fmt.Fprintln(tw, formatRow(c))
	}
	tw.Flush()

	fmt.Fprintln(w)
	if regressed(cs) {
		fmt.Fprintln(w, "RESULT: FAIL — a gated allocation or byte count regressed. See the rows marked FAIL above.")
		return
	}
	fmt.Fprintln(w, "RESULT: PASS — no gated allocation or byte-count regression.")
}

func formatRow(c comparison) string {
	name := strings.TrimPrefix(c.bench.name, "Benchmark")
	if c.missing() {
		which := "base"
		if c.baseFound {
			which = "head"
		}
		return fmt.Sprintf("%s\t—\t—\t—\tFAIL (no %s measurement)", name, which)
	}

	allocsCell := fmt.Sprintf("%d→%d", c.base.allocs, c.head.allocs)
	if !c.bench.policy.gateAllocs {
		allocsCell += " (report)"
	} else if c.allocsFail {
		allocsCell += " FAIL"
	}

	bytesCell := fmt.Sprintf("%d→%d", c.base.bytes, c.head.bytes)
	deltaCell := formatBytesDelta(c.base.bytes, c.head.bytes)
	if c.bench.policy.gateBytes && c.bytesFail {
		bytesCell += " FAIL"
	}

	verdict := "pass"
	if c.failed() {
		verdict = "FAIL"
	}
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s", name, allocsCell, bytesCell, deltaCell, verdict)
}

func formatBytesDelta(base, head int64) string {
	if base == 0 {
		if head == 0 {
			return "+0%"
		}
		return "+inf"
	}
	pct := float64(head-base) / float64(base) * 100
	return fmt.Sprintf("%+.2f%%", pct)
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
