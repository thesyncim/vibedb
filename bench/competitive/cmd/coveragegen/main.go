// Command coveragegen regenerates the benchmark coverage table from the
// executable manifest. It performs no benchmark work.
package main

import (
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/bench/competitive/internal/coverage"
)

func main() {
	if err := os.WriteFile("COVERAGE.md", coverage.RenderBenchmarkCoverageMarkdown(), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "coveragegen:", err)
		os.Exit(1)
	}
}
