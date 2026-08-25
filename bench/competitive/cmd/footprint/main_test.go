package main

import (
	"bytes"
	"fmt"
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
	report(&out, "badger", "buffered-visible", competitive.LowCardinality,
		competitive.InlineDocuments, 0,
		100, competitive.Footprint{DiskBytes: 250, DiskAllocatedBytes: 125}, profile)

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
	for _, required := range []string{
		"git-commit", "vcs-modified", "disk-bytes", "allocated-bytes",
		"logical-bytes", "disk/logical", "allocated/logical",
	} {
		if _, ok := index[required]; !ok {
			t.Fatalf("header omits %q: %q", required, lines[0])
		}
	}
	if row[index["disk-bytes"]] != "250" || row[index["allocated-bytes"]] != "125" {
		t.Fatalf("exact byte columns = %q/%q, want 250/125",
			row[index["disk-bytes"]], row[index["allocated-bytes"]])
	}
	if row[index["logical-bytes"]] != "100" || row[index["disk/logical"]] != "2.5000" ||
		row[index["allocated/logical"]] != "1.2500" {
		t.Fatalf("logical footprint columns = %q/%q/%q, want 100/2.5000/1.2500",
			row[index["logical-bytes"]], row[index["disk/logical"]], row[index["allocated/logical"]])
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

func TestCorpusStatsReportsExactLogicalBytes(t *testing.T) {
	docs := competitive.CorpusOf(17, competitive.LowCardinality)
	counts := competitive.CorpusBytes(docs)
	jsonBytes, gzipBytes, err := competitive.CorpusRedundancy(docs)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := printCorpusStats(&out, competitive.LowCardinality, competitive.InlineDocuments, docs); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"document-shape=inline",
		"docs=17",
		fmt.Sprintf("key-bytes=%d", counts.KeyBytes),
		fmt.Sprintf("json-bytes=%d", jsonBytes),
		fmt.Sprintf("logical-bytes=%d", counts.LogicalBytes),
		fmt.Sprintf("json-gzip-9-bytes=%d", gzipBytes),
	} {
		if !strings.Contains(out.String(), required) {
			t.Fatalf("corpus stats omit %q: %q", required, out.String())
		}
	}
}

func TestCorpusStatsRejectsEmptyCorpus(t *testing.T) {
	if err := printCorpusStats(&bytes.Buffer{}, competitive.LowCardinality, competitive.InlineDocuments, nil); err == nil {
		t.Fatal("empty corpus stats unexpectedly succeeded")
	}
}
