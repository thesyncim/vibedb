package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/featurestate"
)

func main() {
	out := flag.String("out", "", "generated Markdown output path")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "featurestategen: -out is required")
		os.Exit(2)
	}
	if err := os.WriteFile(*out, featurestate.RenderMarkdown(), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "featurestategen:", err)
		os.Exit(1)
	}
}
