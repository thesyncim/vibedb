package main

import (
	"bytes"
	"strings"
	"testing"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

func TestReportIncludesStorageProfileProvenance(t *testing.T) {
	profile, err := competitive.ResolveStorageProfile(
		"badger", competitive.StorageProfileProduction,
	)
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	printHeader(&out)
	report(&out, "badger", "buffered-visible", competitive.LowCardinality, false,
		competitive.Footprint{}, profile)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines:\n%s", len(lines), out.String())
	}
	header := strings.Fields(lines[0])
	row := strings.Fields(lines[1])
	if len(row) != len(header) {
		t.Fatalf("row has %d fields, header has %d:\n%s", len(row), len(header), out.String())
	}
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[name] = i
	}
	if row[index["storage-profile"]] != "production" {
		t.Fatalf("storage profile = %q", row[index["storage-profile"]])
	}
	if row[index["compression"]] != "snappy-sst-blocks" {
		t.Fatalf("compression = %q", row[index["compression"]])
	}
	if !strings.Contains(row[index["compression-provenance"]], "WithCompression(options.Snappy)") {
		t.Fatalf("provenance = %q", row[index["compression-provenance"]])
	}
}
