// Command vibedb-verify is the offline verify and salvage tool for vibedb store
// files. It is read-only against its input and wants a quiescent file or a copy:
// it does not take the writer lock and does not apply the in-place
// materialization rollback that opening the store would, so a concurrent writer
// may retire and reuse the extents it reads.
//
// Usage:
//
//	vibedb-verify verify  <store-file>
//	vibedb-verify salvage <store-file> <output-file>
//
// Output is one line per finding followed by a machine-parseable summary. verify
// exits non-zero on any violation; salvage exits non-zero on failure to write a
// recovered store.
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/thesyncim/vibedb/store/durable"
)

func main() {
	os.Exit(run(os.Args))
}

func run(args []string) int {
	if len(args) < 2 {
		usage()
		return 2
	}
	switch args[1] {
	case "verify":
		if len(args) != 3 {
			usage()
			return 2
		}
		return runVerify(args[2])
	case "salvage":
		if len(args) != 4 {
			usage()
			return 2
		}
		return runSalvage(args[2], args[3])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb-verify verify  <store-file>")
	fmt.Fprintln(os.Stderr, "  vibedb-verify salvage <store-file> <output-file>")
}

func runVerify(path string) int {
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", path, err)
		return 1
	}
	defer file.Close()

	report, err := durable.Verify(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error verify=%q: %v\n", path, err)
		return 1
	}
	for _, finding := range report.Findings {
		fmt.Printf(
			"finding kind=%s offset=%d logical=%d detail=%q\n",
			finding.Kind, finding.Offset, finding.LogicalID, finding.Detail,
		)
	}
	kinds := make([]string, 0, len(report.PageCounts))
	for kind := range report.PageCounts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		fmt.Printf("pages kind=%s count=%d\n", kind, report.PageCounts[kind])
	}
	fmt.Printf(
		"summary root_slot=%d generation=%d file_end=%d primary=%t documents=%d free_extents=%d findings=%d\n",
		report.RootSlot, report.Generation, report.FileEnd, report.Primary,
		report.Documents, report.FreeExtents, len(report.Findings),
	)
	if report.OK() {
		fmt.Println("result ok")
		return 0
	}
	fmt.Printf("result fail findings=%d\n", len(report.Findings))
	return 1
}

func runSalvage(srcPath, outPath string) int {
	src, err := os.Open(srcPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", srcPath, err)
		return 1
	}
	defer src.Close()

	out, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create=%q: %v\n", outPath, err)
		return 1
	}
	defer out.Close()

	report, err := durable.Salvage(src, out, durable.Options{
		Backend: durable.BackendPortable,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error salvage=%q: %v\n", srcPath, err)
		return 1
	}
	fmt.Printf(
		"salvage leaves_scanned=%d buckets=%d documents=%d overflow_skipped=%d duplicate_skipped=%d output_file_end=%d\n",
		report.LeavesScanned, report.BucketsKept, report.Documents,
		report.OverflowSkipped, report.DuplicateSkipped, report.OutputFileEnd,
	)
	fmt.Println("result ok")
	return 0
}
