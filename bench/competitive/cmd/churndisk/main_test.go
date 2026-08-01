package main

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestSmokeFixedLiveSetTSV(t *testing.T) {
	for _, engine := range []string{"vibedb", "bbolt"} {
		t.Run(engine, func(t *testing.T) {
			var out bytes.Buffer
			args := []string{
				"-engine=" + engine,
				"-corpus=1000",
				"-mutations=2000",
				"-sample-mutations=500",
				"-checkpoint-mutations=64",
				"-allow-diagnostic",
			}
			wantProfile := "intrinsic"
			wantCompression := "unsupported/no-op"
			if engine == "bbolt" {
				args = append(args, "-storage-profile=production")
				wantProfile = "production"
			}
			err := run(args, &out)
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
				"git-commit", "vcs-modified", "engine", "mutation-index", "phase", "apparent-bytes",
				"allocated-bytes", "forced-cp", "publishable", "storage-profile",
				"compression", "compression-provenance", "maintenance-floor",
			} {
				if _, ok := index[required]; !ok {
					t.Fatalf("header omits %q: %q", required, lines[0])
				}
			}
			last := -1
			phases := map[string]bool{}
			var preFloor, postFloor int64
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
				phase := fields[index["phase"]]
				phases[phase] = true
				apparent, err := strconv.ParseInt(
					fields[index["apparent-bytes"]], 10, 64,
				)
				if err != nil {
					t.Fatalf("line %d apparent bytes: %v", lineNo+2, err)
				}
				switch phase {
				case "pre-floor":
					preFloor = apparent
				case "post-floor":
					postFloor = apparent
				}
				if fields[index["publishable"]] != "false" {
					t.Fatalf("diagnostic line %d marked publishable: %q", lineNo+2, line)
				}
				if fields[index["storage-profile"]] != wantProfile {
					t.Fatalf("line %d storage profile = %q, want %q", lineNo+2, fields[index["storage-profile"]], wantProfile)
				}
				if fields[index["compression"]] != wantCompression {
					t.Fatalf("line %d compression = %q, want %q", lineNo+2, fields[index["compression"]], wantCompression)
				}
				if !strings.Contains(fields[index["compression-provenance"]], "profile-no-op") {
					t.Fatalf("line %d omits no-op provenance: %q", lineNo+2, line)
				}
			}
			if !phases["pre-floor"] || !phases["post-floor"] {
				t.Fatalf("final floor rows missing; phases=%v", phases)
			}
			if engine == "vibedb" {
				if postFloor > preFloor {
					t.Fatalf(
						"offline repack increased apparent bytes: pre=%d post=%d",
						preFloor, postFloor,
					)
				}
				if !strings.Contains(
					strings.Join(lines[1:], "\n"),
					"offline out-of-place durable.Repack",
				) {
					t.Fatal("vibedb floor rows omit offline Repack disclosure")
				}
			}
			if last != 2000 {
				t.Fatalf("final mutation index = %d, want 2000", last)
			}
		})
	}
}

func TestPublicationShape(t *testing.T) {
	cfg := config{
		corpusSize:          100_000,
		mutationBudget:      200_000,
		replacePercent:      80,
		sampleMutations:     5_000,
		checkpointMutations: 64,
		seed:                defaultSeed,
	}
	if !publicationShape(cfg) {
		t.Fatal("default publication shape rejected")
	}
	cfg.corpusSize--
	if publicationShape(cfg) {
		t.Fatal("custom corpus accepted as a publication shape")
	}
}
