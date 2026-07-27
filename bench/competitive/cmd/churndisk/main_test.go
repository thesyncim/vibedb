package main

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestSmokeFixedLiveSetTSV(t *testing.T) {
	for _, engine := range []string{"vibejson-durable", "bbolt"} {
		t.Run(engine, func(t *testing.T) {
			var out bytes.Buffer
			err := run([]string{
				"-engine=" + engine,
				"-corpus=1000",
				"-mutations=2000",
				"-sample-mutations=500",
				"-checkpoint-mutations=64",
			}, &out)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) < 3 {
				t.Fatalf("got %d TSV lines, want header and final rows", len(lines))
			}
			header := strings.Split(lines[0], "\t")
			index := make(map[string]int, len(header))
			for i, name := range header {
				index[name] = i
			}
			for _, required := range []string{
				"engine", "mutation-index", "phase", "apparent-bytes",
				"allocated-bytes", "forced-cp", "publishable",
			} {
				if _, ok := index[required]; !ok {
					t.Fatalf("header omits %q: %q", required, lines[0])
				}
			}
			last := -1
			phases := map[string]bool{}
			for lineNo, line := range lines[1:] {
				fields := strings.Split(line, "\t")
				if len(fields) != len(header) {
					t.Fatalf("line %d has %d fields, want %d: %q", lineNo+2, len(fields), len(header), line)
				}
				got, err := strconv.Atoi(fields[index["mutation-index"]])
				if err != nil {
					t.Fatalf("line %d mutation index: %v", lineNo+2, err)
				}
				if got < last {
					t.Fatalf("mutation index decreased from %d to %d", last, got)
				}
				last = got
				phases[fields[index["phase"]]] = true
			}
			if !phases["pre-floor"] || !phases["post-floor"] {
				t.Fatalf("final floor rows missing; phases=%v", phases)
			}
			if last != 2000 {
				t.Fatalf("final mutation index = %d, want 2000", last)
			}
		})
	}
}
