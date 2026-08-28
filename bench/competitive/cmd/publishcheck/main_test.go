package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateCompleteBundleAndFailClosedCuts(t *testing.T) {
	dir := t.TempDir()
	revision := strings.Repeat("a", 40)
	writeFixture(t, dir, "metadata.tsv", "meta\tcommand_schema\tvibedb.publish-evidence/1\nmeta\trevision\t"+revision+"\nmeta\tvcs_modified\tfalse\nmeta\tgo_version\tgo1.test\nmeta\tgoos\tlinux\nmeta\tgoarch\tamd64\nmeta\tkernel\ttest\nmeta\tfilesystem\text4\n")
	for _, item := range []struct {
		name, durability, shape, exactIndexes string
		engines                               []string
	}{
		{"mixed-ordinary-sync.tsv", "ordinary-sync", "inline", "0", engines},
		{"mixed-indexed-ordinary-sync.tsv", "ordinary-sync", "inline", "3", []string{"sqlite", "vibedb"}},
		{"mixed-overflow-ordinary-sync.tsv", "ordinary-sync", "overflow-heavy", "0", engines},
		{"mixed-power-safe.tsv", "power-safe", "inline", "3", []string{"sqlite", "vibedb"}},
	} {
		writeFixture(t, dir, item.name, mixedFixture(revision, item.durability, item.shape, item.exactIndexes, item.engines))
	}
	for _, engine := range engines {
		writeFixture(t, dir, "footprint-"+engine+".tsv", "git-commit vcs-modified engine durability disk-bytes allocated-bytes logical-bytes disk/logical allocated/logical maxrss\n"+revision+" false "+engine+" ordinary-sync 2 2 1 2 2 1\n")
		writeFixture(t, dir, "churn-"+engine+".tsv", "git-commit\tvcs-modified\tengine\tdurability\tapparent-bytes\tallocated-bytes\tpeak-rss-bytes\tlogical-write-bytes\tphysical-write-known\tphysical-write-bytes\tphysical-write/logical\tpublishable\n"+revision+"\tfalse\t"+engine+"\tbuffered-visible\t2\t2\t1\t1\ttrue\t2\t2\ttrue\n")
		writeFixture(t, dir, "outofram-"+engine+".tsv", "engine\tgoos\tdurability\tdocument-shape\tlogical-bytes\tphysical-memory-bytes\tpeak-rss-bytes\tmax-rss-bytes\tphysical-write-known\tphysical-write-bytes\tapparent-bytes\tallocated-bytes\n"+engine+"\tlinux\tordinary-sync\toverflow-heavy\t3\t2\t1\t2\ttrue\t2\t3\t3\n")
	}
	for _, engine := range []string{"sqlite", "vibedb"} {
		writeFixture(t, dir, "footprint-indexed-"+engine+".tsv", "git-commit vcs-modified engine durability exact-indexes disk-bytes allocated-bytes logical-bytes disk/logical allocated/logical maxrss\n"+revision+" false "+engine+" ordinary-sync 3 2 2 1 2 2 1\n")
	}
	for run := 1; run <= minimumRepetitions; run++ {
		for _, workload := range []string{"mixed", "read", "write"} {
			for _, clients := range []string{"1", "8", "32"} {
				name := fmt.Sprintf("rf3/run-%02d/rf3-%s-clients-%s.tsv", run, workload, clients)
				writeFixture(t, dir, name, rf3Fixture(revision, workload, clients))
			}
		}
	}
	writeFixture(t, dir, "rf3-chaos.tsv", chaosFixture(revision))
	result, err := validate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result), "result\tpass\n") || !strings.Contains(string(result), "repetitions\t9\n") {
		t.Fatalf("receipt:\n%s", result)
	}

	if err := os.Remove(filepath.Join(dir, "rf3", "run-09", "rf3-write-clients-32.tsv")); err != nil {
		t.Fatal(err)
	}
	if _, err := validate(dir); err == nil {
		t.Fatal("missing RF3 repetition was accepted")
	}
}

func TestValidateRejectsDirtyMetadata(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "metadata.tsv", "meta\tcommand_schema\tvibedb.publish-evidence/1\nmeta\trevision\tx\nmeta\tvcs_modified\ttrue\nmeta\tgo_version\tgo1\nmeta\tgoos\tlinux\nmeta\tgoarch\tamd64\nmeta\tkernel\tx\nmeta\tfilesystem\tx\n")
	if _, err := validate(dir); err == nil {
		t.Fatal("dirty tree was accepted")
	}
}

func TestValidateQualificationProducesDigestReceiptAndRejectsMissingCut(t *testing.T) {
	dir := t.TempDir()
	revision := strings.Repeat("b", 40)
	writeFixture(t, dir, "metadata.tsv", "meta\tcommand_schema\tvibedb.ci-evidence/1\nmeta\trevision\t"+revision+"\nmeta\tvcs_modified\tfalse\nmeta\tgo_version\tgo1.test\nmeta\tgoos\tlinux\nmeta\tgoarch\tamd64\nmeta\tkernel\ttest\nmeta\tfilesystem\text4\nmeta\tembedded_repetitions\t9\nmeta\trf3_repetitions\t3\nmeta\tchaos_repetitions\t3\nmeta\tcorpus_documents\t256\nmeta\tmeasured_operations\t512\n")
	for _, item := range []struct {
		name, durability, exactIndexes string
		engines                        []string
	}{
		{"mixed-ordinary-sync.tsv", "ordinary-sync", "0", engines},
		{"mixed-indexed-ordinary-sync.tsv", "ordinary-sync", "3", []string{"sqlite", "vibedb"}},
		{"mixed-power-safe.tsv", "power-safe", "3", []string{"sqlite", "vibedb"}},
	} {
		writeFixture(t, dir, item.name, mixedFixture(revision, item.durability, "inline", item.exactIndexes, item.engines))
	}
	for _, engine := range engines {
		writeFixture(t, dir, "footprint-"+engine+".tsv", "git-commit vcs-modified engine durability disk-bytes allocated-bytes logical-bytes disk/logical allocated/logical maxrss\n"+revision+" false "+engine+" ordinary-sync 2 2 1 2 2 1\n")
	}
	for _, engine := range []string{"sqlite", "vibedb"} {
		writeFixture(t, dir, "footprint-indexed-"+engine+".tsv", "git-commit vcs-modified engine durability exact-indexes disk-bytes allocated-bytes logical-bytes disk/logical allocated/logical maxrss\n"+revision+" false "+engine+" ordinary-sync 3 2 2 1 2 2 1\n")
	}
	for run := 1; run <= qualificationChaosRepetitions; run++ {
		for _, workload := range []string{"mixed", "read", "write"} {
			for _, clients := range []string{"1", "8", "32"} {
				writeFixture(t, dir, fmt.Sprintf("rf3/run-%02d/rf3-%s-clients-%s.tsv", run, workload, clients), rf3Fixture(revision, workload, clients))
			}
		}
	}
	writeFixture(t, dir, "rf3-chaos.tsv", chaosFixture(revision))

	receipt, err := validateQualification(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(receipt), "schema\tvibedb.ci-validation\t1\n") ||
		!strings.Contains(string(receipt), "artifacts\t39\n") ||
		!strings.Contains(string(receipt), "artifact\trf3-chaos.tsv\t") {
		t.Fatalf("receipt:\n%s", receipt)
	}
	if err := os.Remove(filepath.Join(dir, "footprint-pebble.tsv")); err != nil {
		t.Fatal(err)
	}
	if _, err := validateQualification(dir); err == nil {
		t.Fatal("qualification accepted a missing space artifact")
	}
}

func mixedFixture(revision, durability, shape, exactIndexes string, fixtureEngines []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "meta\tgit-commit\t%s\nmeta\tgit-dirty\tfalse\nmeta\tpublishable-suite\ttrue\nmeta\trepetitions\t9\nmeta\tdurability\t%s\nmeta\tcheckpoint-mutations\t0\nmeta\tdocument-shape\t%s\nmeta\texact-indexes\t%s\nmeta\tallow-diagnostic\tfalse\n", revision, durability, shape, exactIndexes)
	metrics := []string{"p50-us", "p99-us", "p99.9-us", "max-us", "total-ops/s", "disk-MiB", "alloc-MiB", "peak-rss-MiB", "durability-payload/logical"}
	for _, engine := range fixtureEngines {
		for run := 1; run <= minimumRepetitions; run++ {
			fmt.Fprintf(&b, "raw\t%d\t1\t%s\tx\n", run, engine)
		}
		for _, metric := range metrics {
			fmt.Fprintf(&b, "summary\t%s\t%s\tycsb-a\tlow\t%s\t%s\t1\tread\ttrue\t%s\t9\n", engine, durability, shape, exactIndexes, metric)
		}
	}
	return b.String()
}

func rf3Fixture(revision, workload, clients string) string {
	logical := "counter\tworkload\tlogical_write_bytes\t0\t1\t1\n"
	if workload == "read" {
		logical = "counter\tworkload\tlogical_write_bytes\t0\t0\t0\n"
	}
	summaries := "summary\t" + workload + "\t1\t1\t1\t1\t1\t1\n"
	if workload == "mixed" {
		summaries = "summary\tread\t1\t1\t1\t1\t1\t1\nsummary\twrite\t1\t1\t1\t1\t1\t1\n"
	}
	return "schema\tvibedb.rf3.evidence\t1\nmeta\tdurability\tpower-safe\nmeta\treplicas\t3\nmeta\tvcs_modified\tfalse\nmeta\tvcs_revision\t" + revision + "\nconfig\tworkload\t" + workload + "\nconfig\tclients\t" + clients + "\nsummary_header\toperation\tsamples\tp50_ns\tp95_ns\tp99_ns\tp99.9_ns\tmax_ns\n" + summaries + "counter\tnetwork\tsent_bytes\t0\t1\t1\ncounter\tstorage\tdevice_bytes\t0\t1\t1\ncounter\tstorage\tfile_end\t0\t1\t1\n" + logical
}

func chaosFixture(revision string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "schema\tvibedb.rf3.chaos-evidence\t2\nmeta\tvcs_modified\tfalse\nmeta\tvcs_revision\t%s\nraw_header\tordinal\ttimed_out\texact_run\tqualification_exact\tpassed\twal_growth_bytes\twal_growth_bound_bytes\twaiter_rss_growth_bytes\twaiter_rss_growth_bound_bytes\n", revision)
	for run := 1; run <= minimumRepetitions; run++ {
		fmt.Fprintf(&b, "raw\t%d\tfalse\ttrue\ttrue\ttrue\t1\t2\t1\t2\n", run)
	}
	return b.String()
}

func writeFixture(t *testing.T, root, name, value string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
