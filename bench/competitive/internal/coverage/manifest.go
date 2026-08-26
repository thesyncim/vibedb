package coverage

import (
	"bytes"
	"fmt"
	"strings"
)

// CoverageStatus says how directly the competitive harness measures a lane.
// It describes harness coverage, not whether vibedb implements the underlying
// database feature and not whether a result has been published.
type CoverageStatus string

const (
	// CoverageImplemented means a dedicated harness records the requested case
	// with a correctness oracle and machine-readable configuration or output.
	CoverageImplemented CoverageStatus = "implemented"
	// CoverageDiagnostic means an executable probe or correctness test provides
	// partial evidence, but there is no publication-grade comparative lane.
	CoverageDiagnostic CoverageStatus = "diagnostic"
	// CoverageGap means there is no executable measurement of the requested case.
	CoverageGap CoverageStatus = "gap"
)

// CoverageTargetKind identifies an executable evidence target.
type CoverageTargetKind string

const (
	CoverageCommand   CoverageTargetKind = "command"
	CoverageTest      CoverageTargetKind = "test"
	CoverageBenchmark CoverageTargetKind = "benchmark"
)

// CoverageTarget points to a concrete executable in this repository. Package
// is repository-relative. Args are separate tokens so validation can prove
// that command-line flags still exist instead of accepting an opaque shell
// fragment that silently drifts.
type CoverageTarget struct {
	Label        string
	Kind         CoverageTargetKind
	Package      string
	Symbol       string
	Env          []string
	Args         []string
	OutputFunc   string
	OutputTokens []string
}

// CoverageLane is one required benchmark-matrix cell.
type CoverageLane struct {
	Dimension string
	Case      string
	Status    CoverageStatus
	Boundary  string
	Targets   []CoverageTarget
}

func mixedTarget(label string, args ...string) CoverageTarget {
	base := []string{
		"-engine=vibedb",
		"-workload=churn",
		"-corpus=10000",
		"-operations=20000",
		"-warmup=2000",
		"-durability=buffered-visible",
		"-checkpoint-mutations=64",
		"-clients=1",
		"-cardinality=low",
		"-header=true",
	}
	for _, override := range args {
		name := coverageFlagName(override)
		replaced := false
		for i, existing := range base {
			if coverageFlagName(existing) == name {
				base[i] = override
				replaced = true
				break
			}
		}
		if !replaced {
			base = append(base, override)
		}
	}
	return CoverageTarget{
		Label: label, Kind: CoverageCommand,
		Package:    "bench/competitive/cmd/mixed",
		Args:       base,
		OutputFunc: "printHeader",
		OutputTokens: []string{
			"durability", "checkpoint", "forced-cp", "exact-indexes", "document-shape", "clients",
			"p50-us", "p95-us", "p99-us", "p99.9-us", "max-us", "total-ops/s", "disk-MiB", "alloc-MiB",
			"durability-payload-known", "logical-write-B", "durability-payload-B", "durability-payload/logical",
		},
	}
}

func coverageFlagName(arg string) string {
	name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
	return name
}

func commandTarget(label, pkg string, args ...string) CoverageTarget {
	return CoverageTarget{Label: label, Kind: CoverageCommand, Package: pkg, Args: args}
}

func testTarget(label, pkg, symbol string) CoverageTarget {
	return CoverageTarget{Label: label, Kind: CoverageTest, Package: pkg, Symbol: symbol}
}

func benchmarkTarget(label, pkg, symbol string) CoverageTarget {
	return CoverageTarget{Label: label, Kind: CoverageBenchmark, Package: pkg, Symbol: symbol}
}

func withOutputTokens(target CoverageTarget, function string, tokens ...string) CoverageTarget {
	target.OutputFunc = function
	target.OutputTokens = tokens
	return target
}

func massiveChurnTarget(label string) CoverageTarget {
	target := testTarget(label, "bench/competitive", "TestMassiveChurnDiag")
	target.Env = []string{
		"MASSIVE_CHURN=1",
		"MASSIVE_CORPUS=1000000",
		"MASSIVE_OPERATIONS=250000",
		"MASSIVE_CACHE_MIB=64",
	}
	target.OutputFunc = "TestMassiveChurnDiag"
	target.OutputTokens = []string{"p50=", "p99=", "p99.9=", "misses=", "durabilityPayloadKnown=", "durabilityPayloadMiB="}
	return target
}

func lifecycleTarget(label, mode string) CoverageTarget {
	return withOutputTokens(commandTarget(
		label, "bench/competitive/cmd/lifecycle",
		"-engine=vibedb", "-mode="+mode, "-corpus=10000",
		"-durability=ordinary-sync", "-exact-indexes=0",
		"-cardinality=low", "-document-shape=inline",
		"-max-rss-bytes=0", "-max-physical-write-bytes=1073741824",
	), "parentRun", "cache-control", "durability", "exact-indexes",
		"open-ns", "peak-rss-bytes", "physical-write-known", "physical-write-bytes")
}

// BenchmarkCoverageManifest returns a fresh copy of the complete required
// matrix so generators and tests cannot mutate the contract accidentally.
func BenchmarkCoverageManifest() []CoverageLane {
	lanes := []CoverageLane{
		{
			Dimension: "durability", Case: "buffered-visible", Status: CoverageImplemented,
			Boundary: "The mixed harness records the resolved acknowledgement mode; checkpoint cadence zero leaves only the final in-window durability fence.",
			Targets: []CoverageTarget{mixedTarget(
				"buffered-visible mixed workload", "-checkpoint-mutations=0",
			)},
		},
		{
			Dimension: "durability", Case: "checkpointed", Status: CoverageImplemented,
			Boundary: "CP64 admission and checkpoint latency are inside the measured interval; forced checkpoints are reported.",
			Targets:  []CoverageTarget{mixedTarget("periodic checkpoint lane")},
		},
		{
			Dimension: "durability", Case: "strongest sync", Status: CoverageImplemented,
			Boundary: "This evidence command covers VibeDB's power-safe acknowledgement lane only. It proves the lane is executable, not that a matched cross-engine result has been run.",
			Targets: []CoverageTarget{mixedTarget(
				"power-safe acknowledgement lane",
				"-durability=power-safe", "-checkpoint-mutations=0",
			)},
		},

		{
			Dimension: "indexes", Case: "none", Status: CoverageImplemented,
			Boundary: "The default mixed corpus is schemaless and unindexed.",
			Targets:  []CoverageTarget{mixedTarget("unindexed mixed workload")},
		},
		{
			Dimension: "indexes", Case: "one exact", Status: CoverageImplemented,
			Boundary: "VibeDB maintains one exact country index; the mixed command checks final documents and the indexed equivalence test checks postings. This target is not cross-engine index evidence.",
			Targets: []CoverageTarget{
				mixedTarget("one exact index", "-exact-indexes=1"),
				testTarget("indexed document/posting equivalence", "bench/competitive", "TestFullEquivalenceIndexedDurable"),
			},
		},
		{
			Dimension: "indexes", Case: "several exact", Status: CoverageImplemented,
			Boundary: "The same mixed lane maintains three exact scalar indexes on VibeDB and SQLite. A byte-native parametric oracle proves country, tier, and region cardinality plus native indexed-plan engagement across update, delete, checkpoint, and restore.",
			Targets: []CoverageTarget{
				mixedTarget("three simultaneous exact indexes", "-exact-indexes=3"),
				testTarget("parametric three-index posting and plan equivalence", "bench/competitive", "TestExactIndexParametricPostingEquivalence"),
				testTarget("three-index adapter shape", "bench/competitive", "TestVibeDBExactIndexAndDocumentBounds"),
				testTarget("matched SQLite physical index count", "bench/competitive", "TestSQLiteExactIndexCountIsPhysical"),
			},
		},

		{
			Dimension: "document size", Case: "inline", Status: CoverageImplemented,
			Boundary: "The deterministic 10,000-document corpus is bounded below the durable adapter's 512-byte inline limit.",
			Targets:  []CoverageTarget{mixedTarget("inline corpus", "-document-shape=inline")},
		},
		{
			Dimension: "document size", Case: "mixed", Status: CoverageImplemented,
			Boundary: "The deterministic mixed corpus alternates inline documents with exact 4 KiB overflow documents across every engine.",
			Targets: []CoverageTarget{
				mixedTarget("mixed inline and overflow corpus", "-document-shape=mixed"),
				testTarget("mixed corpus byte shape", "bench/competitive", "TestCorpusDocumentShapesAreExactAndValid"),
			},
		},
		{
			Dimension: "document size", Case: "overflow-heavy", Status: CoverageImplemented,
			Boundary: "Seven of every eight deterministic documents are exact 16 KiB overflow values under one shared admission bound.",
			Targets: []CoverageTarget{
				mixedTarget("overflow-heavy corpus", "-document-shape=overflow-heavy"),
				testTarget("overflow-heavy corpus byte shape", "bench/competitive", "TestCorpusDocumentShapesAreExactAndValid"),
			},
		},

		{
			Dimension: "working set", Case: "fits cache", Status: CoverageImplemented,
			Boundary: "The standard corpus fits beneath the common 64 MiB read-cache budget.",
			Targets:  []CoverageTarget{mixedTarget("cache-resident standard corpus")},
		},
		{
			Dimension: "working set", Case: "larger than cache", Status: CoverageDiagnostic,
			Boundary: "The opt-in million-document VibeDB-only churn probe reports cache reads, misses, and evictions; it is not a cross-engine publication lane.",
			Targets:  []CoverageTarget{massiveChurnTarget("VibeDB-only cache-pressure diagnostic")},
		},
		{
			Dimension: "working set", Case: "larger than RAM", Status: CoverageImplemented,
			Boundary: "The streaming overflow-heavy loader admits the row only when exact logical key-plus-document bytes exceed measured host physical memory. It enforces hard loader-byte, RSS, disk-space, and Linux physical-write bounds; a cross-engine claim requires one isolated process per engine with identical durability and index flags.",
			Targets: []CoverageTarget{withOutputTokens(commandTarget(
				"bounded-memory out-of-RAM scan", "bench/competitive/cmd/outofram",
				"-engine=vibedb", "-corpus=4000000", "-durability=buffered-visible",
				"-exact-indexes=0", "-cardinality=low", "-document-shape=overflow-heavy",
				"-checkpoint-documents=4096", "-max-loader-bytes=8388608",
				"-max-rss-bytes=0", "-max-physical-write-bytes=0",
			), "run", "logical-bytes", "physical-memory-bytes", "loader-peak-bytes",
				"max-loader-bytes", "peak-rss-bytes", "max-rss-bytes",
				"physical-write-known", "physical-write-bytes", "max-physical-write-bytes")},
		},

		{
			Dimension: "cache state", Case: "hot reopen", Status: CoverageImplemented,
			Boundary: "A conditioning child fully scans and closes the populated image, then a fresh child times only Factory.Open and proves the complete corpus after the timed interval. The output calls this full-scan-close, not an in-handle warm cache.",
			Targets:  []CoverageTarget{lifecycleTarget("controlled hot reopen", "hot")},
		},
		{
			Dimension: "cache state", Case: "cold reopen", Status: CoverageImplemented,
			Boundary: "Cold mode synchronously writes Linux /proc/sys/vm/drop_caches=3 before the isolated timed child and fails closed without that global control. Darwin has no equivalent supported lane and is documented as unsupported rather than approximated with advisory eviction.",
			Targets:  []CoverageTarget{lifecycleTarget("Linux global-cache cold reopen", "cold")},
		},

		{
			Dimension: "concurrency", Case: "1", Status: CoverageImplemented,
			Boundary: "One worker owns the full deterministic trace.",
			Targets:  []CoverageTarget{mixedTarget("single-client mixed workload")},
		},
		{
			Dimension: "concurrency", Case: "8", Status: CoverageImplemented,
			Boundary: "Eight sessions share one engine handle and own disjoint mutation shards.",
			Targets:  []CoverageTarget{mixedTarget("eight-client mixed workload", "-clients=8")},
		},
		{
			Dimension: "concurrency", Case: "32", Status: CoverageImplemented,
			Boundary: "Thirty-two sessions share one engine handle and own disjoint mutation shards.",
			Targets:  []CoverageTarget{mixedTarget("thirty-two-client mixed workload", "-clients=32")},
		},
		{
			Dimension: "concurrency", Case: "saturation", Status: CoverageDiagnostic,
			Boundary: "Arbitrary client counts can be swept, but there is no machine-defined stopping rule that identifies the saturation point.",
			Targets:  []CoverageTarget{mixedTarget("manual high-client probe", "-clients=64")},
		},

		{
			Dimension: "snapshot pressure", Case: "none", Status: CoverageImplemented,
			Boundary: "The mixed harness releases cached read state before final fencing and holds no long-lived snapshot during mutation.",
			Targets:  []CoverageTarget{mixedTarget("unpressured snapshot lane")},
		},
		{
			Dimension: "snapshot pressure", Case: "long pinned", Status: CoverageDiagnostic,
			Boundary: "Durable correctness tests prove bounded backpressure under a held snapshot, but no competitive latency or storage lane holds one.",
			Targets: []CoverageTarget{testTarget(
				"held-snapshot backpressure correctness",
				"store/durable", "TestFileStoreLongHeldSnapshotCostsBoundedBackpressure",
			)},
		},

		{
			Dimension: "interfaces", Case: "native", Status: CoverageImplemented,
			Boundary: "This command drives VibeDB through the native Engine/EngineSession adapter. Other adapters implement the same harness interface, but this evidence target does not execute them.",
			Targets:  []CoverageTarget{mixedTarget("VibeDB native-adapter workload")},
		},
		{
			Dimension: "interfaces", Case: "database/sql", Status: CoverageDiagnostic,
			Boundary: "Driver point-query microbenchmarks exist, but the competitive workload, durability, storage, and latency protocol does not run through database/sql.",
			Targets: []CoverageTarget{benchmarkTarget(
				"prepared point-query microbenchmark",
				"sql/driver", "BenchmarkDriverPreparedPointQuery",
			)},
		},
		{
			Dimension: "interfaces", Case: "pgwire", Status: CoverageDiagnostic,
			Boundary: "Wire functional tests cover transaction semantics, but there is no competitive pgwire performance client.",
			Targets: []CoverageTarget{testTarget(
				"wire transaction functional coverage",
				"pgwire", "TestSQLCatalogTransactionsAndFailedState",
			)},
		},

		{
			Dimension: "lifecycle", Case: "open", Status: CoverageImplemented,
			Boundary: "A fresh child times only Factory.Open over a previously checkpointed image; process startup, corpus creation, correctness scan, and Close are outside the interval. Cache state is explicitly uncontrolled in this lane, so hot and cold claims must use their dedicated modes.",
			Targets:  []CoverageTarget{lifecycleTarget("isolated clean open", "open")},
		},
		{
			Dimension: "lifecycle", Case: "recovery", Status: CoverageImplemented,
			Boundary: "An isolated producer opens the checkpointed image, acknowledges one ordinary-sync mutation, and exits without Close. A fresh child times only Factory.Open, verifies the exact recovered canonical value and full row count afterward, and reports Linux process write_bytes when available. This is one controlled acknowledged-mutation crash shape, not an exhaustive crash-point timing claim.",
			Targets: []CoverageTarget{
				lifecycleTarget("isolated acknowledged-mutation recovery", "recovery"),
				testTarget("whole-generation crash-image recovery correctness", "store/durable", "TestFileStoreCrashImagesRecoverWholeGeneration"),
			},
		},
		{
			Dimension: "lifecycle", Case: "checkpoint", Status: CoverageImplemented,
			Boundary: "The mixed harness reports checkpoint call count and p50/p95/p99/p99.9/maximum acknowledgement latency inside elapsed throughput.",
			Targets:  []CoverageTarget{mixedTarget("checkpoint latency")},
		},
		{
			Dimension: "lifecycle", Case: "verify", Status: CoverageDiagnostic,
			Boundary: "Verify has corruption and clean-image correctness tests, but no timed lifecycle lane.",
			Targets: []CoverageTarget{testTarget(
				"clean primary verification",
				"store/durable", "TestVerifyCleanPrimaryStoreVerifiesClean",
			)},
		},
		{
			Dimension: "lifecycle", Case: "repack", Status: CoverageDiagnostic,
			Boundary: "Sustained-churn output records pre/post-repack footprint and cumulative elapsed time, not isolated repack latency or cutover cost.",
			Targets: []CoverageTarget{withOutputTokens(commandTarget(
				"sustained churn with maintenance-floor phase",
				"bench/competitive/cmd/churndisk",
				"-engine=vibedb", "-corpus=100000", "-mutations=200000",
				"-checkpoint-mutations=64", "-sample-mutations=5000",
				"-cardinality=low", "-storage-profile=intrinsic",
			), "printHeader", "maintenance-floor", "phase", "apparent-bytes", "allocated-bytes", "elapsed-seconds")},
		},

		{
			Dimension: "latency", Case: "p50", Status: CoverageImplemented,
			Boundary: "Mixed output reports per-operation and checkpoint p50 in microseconds.",
			Targets:  []CoverageTarget{mixedTarget("p50 latency")},
		},
		{
			Dimension: "latency", Case: "p95", Status: CoverageImplemented,
			Boundary: "Mixed output reports per-operation and checkpoint p95 in microseconds.",
			Targets:  []CoverageTarget{mixedTarget("p95 latency")},
		},
		{
			Dimension: "latency", Case: "p99", Status: CoverageImplemented,
			Boundary: "Mixed output reports per-operation and checkpoint p99 in microseconds.",
			Targets:  []CoverageTarget{mixedTarget("p99 latency")},
		},
		{
			Dimension: "latency", Case: "p99.9", Status: CoverageImplemented,
			Boundary: "Mixed output reports the deterministic rounded order statistic for per-operation and checkpoint p99.9 in microseconds for every engine.",
			Targets:  []CoverageTarget{mixedTarget("p99.9 latency")},
		},
		{
			Dimension: "latency", Case: "max", Status: CoverageImplemented,
			Boundary: "Mixed output reports the exact maximum per-operation and checkpoint sample for every engine.",
			Targets:  []CoverageTarget{mixedTarget("maximum latency")},
		},

		{
			Dimension: "storage", Case: "logical", Status: CoverageImplemented,
			Boundary: "The footprint tool reports key bytes, JSON bytes, and their key-inclusive logical sum separately. JSON gzip is an entropy control and explicitly excludes keys.",
			Targets: []CoverageTarget{withOutputTokens(commandTarget(
				"logical corpus bytes", "bench/competitive/cmd/footprint",
				"-corpus=100000", "-cardinality=low", "-corpus-stats=true",
			), "printCorpusStats", "document-shape=", "key-bytes=", "json-bytes=", "logical-bytes=", "json-gzip-9-bytes=")},
		},
		{
			Dimension: "storage", Case: "allocated", Status: CoverageImplemented,
			Boundary: "The VibeDB footprint command reports apparent bytes, allocated filesystem blocks, and both ratios to the key-inclusive logical payload after a durability fence.",
			Targets: []CoverageTarget{withOutputTokens(commandTarget(
				"allocated and apparent bytes", "bench/competitive/cmd/footprint",
				"-engine=vibedb", "-corpus=100000", "-cardinality=low",
				"-durability=buffered-visible", "-storage-profile=intrinsic", "-header=true",
			), "printHeader", "disk", "diskalloc", "disk-bytes", "allocated-bytes", "logical-bytes", "disk/logical", "allocated/logical")},
		},
		{
			Dimension: "storage", Case: "write amplification", Status: CoverageDiagnostic,
			Boundary: "On VibeDB's journal-backed durable acknowledgement lanes, mixed reports exact submitted mutation bytes, engine-issued durability payload bytes, and their normalized ratio. This is not OS, filesystem-metadata, or physical-media accounting. Counter regression, buffered-visible, and adapters without an equally strong native counter report durability-payload-known=false.",
			Targets: []CoverageTarget{mixedTarget(
				"normalized VibeDB durability-payload diagnostic",
				"-durability=ordinary-sync", "-checkpoint-mutations=0",
			)},
		},

		{
			Dimension: "stability", Case: "long churn", Status: CoverageDiagnostic,
			Boundary: "The deterministic 200,000-mutation disk lane is sustained and sampled, but it is a bounded run rather than a time-based soak with health thresholds.",
			Targets: []CoverageTarget{withOutputTokens(commandTarget(
				"bounded sustained-churn run", "bench/competitive/cmd/churndisk",
				"-engine=vibedb", "-corpus=100000", "-mutations=200000",
				"-checkpoint-mutations=64", "-sample-mutations=5000",
				"-cardinality=low", "-storage-profile=intrinsic",
			), "printHeader", "mutation-index", "apparent-bytes", "allocated-bytes", "elapsed-seconds", "publishable")},
		},
		{
			Dimension: "stability", Case: "periodic crashes", Status: CoverageDiagnostic,
			Boundary: "Injected crash-point sweeps prove recovery atomicity, but no long-running external kill/restart benchmark records latency, loss window, or storage growth.",
			Targets: []CoverageTarget{testTarget(
				"exhaustive commit crash sweep",
				"store/durable", "TestFileStoreExhaustiveCommitCrashSweep",
			)},
		},
	}
	return lanes
}

// RenderBenchmarkCoverageMarkdown renders the checked-in coverage document.
// The exact output is guarded by TestBenchmarkCoverageDocumentIsGenerated.
func RenderBenchmarkCoverageMarkdown() []byte {
	lanes := BenchmarkCoverageManifest()
	evidence, evidenceIDs := benchmarkCoverageEvidenceCatalog(lanes)
	counts := map[CoverageStatus]int{}
	for _, lane := range lanes {
		counts[lane.Status]++
	}

	var out bytes.Buffer
	fmt.Fprintln(&out, "# Competitive benchmark coverage")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "<!-- Code generated by go generate ./...; DO NOT EDIT. -->")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "This table is the executable benchmark-coverage contract. Status describes the harness, not product support and not a measured result:")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "- **implemented**: a dedicated harness records the requested case with a correctness oracle and machine-readable configuration or output;")
	fmt.Fprintln(&out, "- **diagnostic**: an executable probe or correctness test supplies partial evidence, but the publication-grade comparative lane is incomplete; and")
	fmt.Fprintln(&out, "- **gap**: no executable measurement covers the requested case.")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "`implemented` establishes an executable measurement shape, not a comparison. An evidence command selecting `-engine=vibedb` is VibeDB-only; a cross-engine claim additionally requires the repeated isolated `mixedsuite` publication protocol and recorded results.")
	fmt.Fprintln(&out)
	fmt.Fprintf(
		&out,
		"Current coverage: **%d implemented**, **%d diagnostic**, **%d gaps** across %d required cells. A command's presence does not imply that a result has been run or published.\n",
		counts[CoverageImplemented], counts[CoverageDiagnostic], counts[CoverageGap], len(lanes),
	)
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "Evidence commands are rendered to run from the repository root.")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "| Dimension | Case | Status | Executable evidence | Current boundary |")
	fmt.Fprintln(&out, "| --- | --- | --- | --- | --- |")
	for _, lane := range lanes {
		fmt.Fprintf(
			&out, "| %s | %s | **%s** | %s | %s |\n",
			markdownCell(lane.Dimension), markdownCell(lane.Case), lane.Status,
			coverageEvidence(lane.Targets, evidenceIDs), markdownCell(lane.Boundary),
		)
	}
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "## Executable evidence catalog")
	fmt.Fprintln(&out)
	for _, item := range evidence {
		fmt.Fprintf(&out, "### %s\n\n", item.id)
		fmt.Fprintf(&out, "```sh\n%s\n```\n\n", item.command)
	}
	fmt.Fprintln(&out, "## Regeneration and validation")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "From `bench/competitive`:")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "```sh")
	fmt.Fprintln(&out, "go generate .")
	fmt.Fprintln(&out, "go test -run '^TestBenchmarkCoverage' -count=1 ./internal/coverage")
	fmt.Fprintln(&out, "```")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "Validation fixes the required dimensions and cases, resolves every test and benchmark symbol, verifies every command package, flag, and claimed output token, rejects executable evidence on gap rows, and fails when this generated document differs from the manifest.")
	return out.Bytes()
}

type coverageEvidenceItem struct {
	id      string
	command string
}

func benchmarkCoverageEvidenceCatalog(lanes []CoverageLane) ([]coverageEvidenceItem, map[string]string) {
	ids := make(map[string]string)
	var items []coverageEvidenceItem
	for _, lane := range lanes {
		for _, target := range lane.Targets {
			command := coverageCommand(target)
			if _, exists := ids[command]; exists {
				continue
			}
			id := fmt.Sprintf("E%02d", len(items)+1)
			ids[command] = id
			items = append(items, coverageEvidenceItem{id: id, command: command})
		}
	}
	return items, ids
}

func coverageEvidence(targets []CoverageTarget, evidenceIDs map[string]string) string {
	if len(targets) == 0 {
		return "—"
	}
	parts := make([]string, len(targets))
	for i, target := range targets {
		id := evidenceIDs[coverageCommand(target)]
		parts[i] = fmt.Sprintf("[%s](#%s): %s", id, strings.ToLower(id), markdownCell(target.Label))
	}
	return strings.Join(parts, "<br>")
}

func coverageCommand(target CoverageTarget) string {
	prefix := strings.Join(target.Env, " ")
	if prefix != "" {
		prefix += " "
	}
	switch target.Kind {
	case CoverageCommand:
		pkg := strings.TrimPrefix(target.Package, "bench/competitive/")
		return fmt.Sprintf(
			"(cd bench/competitive && %sgo run ./%s %s)",
			prefix, pkg, strings.Join(target.Args, " "),
		)
	case CoverageTest, CoverageBenchmark:
		command := "go test ./" + target.Package
		if target.Package == "bench/competitive" {
			command = "(cd bench/competitive && " + prefix + "go test ."
			if target.Kind == CoverageTest {
				command += " -run '^" + target.Symbol + "$' -count=1)"
			} else {
				command += " -run '^$' -bench '^" + target.Symbol + "$' -count=1)"
			}
			return command
		}
		if target.Kind == CoverageTest {
			return prefix + command + " -run '^" + target.Symbol + "$' -count=1"
		}
		return prefix + command + " -run '^$' -bench '^" + target.Symbol + "$' -count=1"
	default:
		return "invalid target"
	}
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
