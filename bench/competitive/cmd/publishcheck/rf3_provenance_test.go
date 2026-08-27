package main

import (
	"strings"
	"testing"
)

func TestValidateRF3ReportsExactProvenanceAndPairFailures(t *testing.T) {
	revision := strings.Repeat("a", 40)
	valid := rf3Fixture(revision, "mixed", "8")
	parse := func(raw string) table {
		var result table
		for _, row := range strings.Split(strings.TrimSpace(raw), "\n") {
			result.lines = append(result.lines, strings.Split(row, "\t"))
		}
		return result
	}
	if err := validateRF3(parse(valid), revision, "mixed", "8"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, raw, key string }{
		{"absent", strings.ReplaceAll(valid, "meta\tvcs_revision\t"+revision+"\n", ""), "vcs_revision"},
		{"unknown", strings.ReplaceAll(valid, revision, "unknown"), "vcs_revision"},
		{"mismatched revision", strings.ReplaceAll(valid, revision, strings.Repeat("b", 40)), "vcs_revision"},
		{"dirty", strings.ReplaceAll(valid, "vcs_modified\tfalse", "vcs_modified\ttrue"), "vcs_modified"},
		{"empty pair", strings.ReplaceAll(valid, "vcs_revision\t"+revision, "vcs_revision\t"), "duplicate or empty metadata \"vcs_revision\""},
		{"duplicate metadata", valid + "meta\tvcs_revision\t" + revision + "\n", "duplicate or empty metadata \"vcs_revision\""},
		{"duplicate config", valid + "config\tclients\t8\n", "duplicate or empty metadata \"clients\""},
		{"wrong workload", strings.ReplaceAll(valid, "config\tworkload\tmixed", "config\tworkload\twrite"), "workload"},
		{"wrong clients", strings.ReplaceAll(valid, "config\tclients\t8", "config\tclients\t32"), "clients"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateRF3(parse(test.raw), revision, "mixed", "8"); err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("got %v, want offending key %q", err, test.key)
			}
		})
	}
}
