package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibedb/internal/conformance"
)

const (
	start = "<!-- capability-matrix:start -->\n"
	end   = "<!-- capability-matrix:end -->"
)

func main() {
	output := flag.String("out", "", "Markdown file containing the capability markers")
	flag.Parse()
	if *output == "" {
		fatalf("-out is required")
	}

	raw, err := os.ReadFile(*output)
	if err != nil {
		fatalf("read %s: %v", *output, err)
	}
	begin := strings.Index(string(raw), start)
	finish := strings.Index(string(raw), end)
	if begin < 0 || finish < begin {
		fatalf("%s does not contain the capability markers", *output)
	}
	begin += len(start)
	next := append([]byte(nil), raw[:begin]...)
	next = append(next, conformance.RenderMarkdown()...)
	next = append(next, raw[finish:]...)

	dir := filepath.Dir(*output)
	tmp, err := os.CreateTemp(dir, ".capabilities-*.tmp")
	if err != nil {
		fatalf("create temporary output: %v", err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o644); err != nil {
		fatalf("chmod temporary output: %v", err)
	}
	if _, err := tmp.Write(next); err != nil {
		fatalf("write temporary output: %v", err)
	}
	if err := tmp.Sync(); err != nil {
		fatalf("sync temporary output: %v", err)
	}
	if err := tmp.Close(); err != nil {
		fatalf("close temporary output: %v", err)
	}
	if err := os.Rename(name, *output); err != nil {
		fatalf("publish %s: %v", *output, err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "capabilitygen: "+format+"\n", args...)
	os.Exit(1)
}
