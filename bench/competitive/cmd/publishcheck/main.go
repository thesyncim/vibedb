// Command publishcheck validates one complete, claim-free competitive evidence
// bundle. It never invents missing measurements: incomplete, diagnostic, dirty,
// mismatched, or malformed evidence fails closed.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	minimumRepetitions = 9
	maximumArtifact    = 1 << 30
)

var engines = []string{"badger", "bbolt", "pebble", "sqlite", "vibedb"}

type table struct {
	lines [][]string
}

func main() {
	fs := flag.NewFlagSet("publishcheck", flag.ExitOnError)
	evidence := fs.String("evidence", "", "absolute evidence directory")
	writeReceipt := fs.Bool("write-receipt", false, "atomically create VALIDATED.tsv")
	qualification := fs.Bool("qualification", false, "validate the bounded CI qualification profile instead of publication evidence")
	fs.Parse(os.Args[1:])
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "publishcheck: unexpected positional arguments")
		os.Exit(2)
	}
	var result []byte
	var err error
	if *qualification {
		result, err = validateQualification(*evidence)
	} else {
		result, err = validate(*evidence)
	}
	if err == nil && *writeReceipt {
		err = writeValidated(*evidence, result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "publishcheck:", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(result)
}

func validate(directory string) ([]byte, error) {
	if directory == "" || !filepath.IsAbs(directory) {
		return nil, errors.New("-evidence must be an absolute directory")
	}
	metadata, err := readTable(filepath.Join(directory, "metadata.tsv"))
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	meta, err := exactPairs(metadata, "meta")
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"revision", "go_version", "goos", "goarch", "kernel", "filesystem", "command_schema"} {
		if meta[key] == "" {
			return nil, fmt.Errorf("metadata omits %s", key)
		}
	}
	if meta["vcs_modified"] != "false" || meta["goos"] != "linux" || meta["command_schema"] != "vibedb.publish-evidence/1" {
		return nil, errors.New("publication requires Linux, a clean tree, and command schema 1")
	}

	artifacts := make([]string, 0, 96)
	artifacts = append(artifacts, "metadata.tsv")
	for _, item := range []struct {
		name, durability, shape, exactIndexes string
		engines                               []string
	}{
		{"mixed-ordinary-sync.tsv", "ordinary-sync", "inline", "0", engines},
		{"mixed-indexed-ordinary-sync.tsv", "ordinary-sync", "inline", "3", []string{"sqlite", "vibedb"}},
		{"mixed-overflow-ordinary-sync.tsv", "ordinary-sync", "overflow-heavy", "0", engines},
		{"mixed-power-safe.tsv", "power-safe", "inline", "3", []string{"sqlite", "vibedb"}},
	} {
		name := item.name
		path := filepath.Join(directory, name)
		t, readErr := readTable(path)
		if readErr != nil {
			return nil, fmt.Errorf("%s: %w", name, readErr)
		}
		if err = validateMixed(t, meta["revision"], item.durability, item.shape, item.exactIndexes, item.engines); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		artifacts = append(artifacts, name)
	}
	for _, engine := range engines {
		for _, prefix := range []string{"footprint", "churn", "outofram"} {
			name := prefix + "-" + engine + ".tsv"
			t, readErr := readTable(filepath.Join(directory, name))
			if readErr != nil {
				return nil, fmt.Errorf("%s: %w", name, readErr)
			}
			switch prefix {
			case "footprint":
				err = validateHeaderRows(t, engine, []string{"disk-bytes", "allocated-bytes", "logical-bytes", "disk/logical", "allocated/logical", "maxrss"})
				if err == nil {
					err = validateColumnContract(t, map[string]string{"git-commit": meta["revision"], "vcs-modified": "false", "durability": "ordinary-sync"})
				}
			case "churn":
				err = validateHeaderRows(t, engine, []string{"apparent-bytes", "allocated-bytes", "peak-rss-bytes", "logical-write-bytes", "physical-write-known", "physical-write-bytes", "physical-write/logical", "publishable"})
				if err == nil {
					err = validateColumnContract(t, map[string]string{"git-commit": meta["revision"], "vcs-modified": "false", "durability": "buffered-visible", "physical-write-known": "true", "publishable": "true"})
				}
				if err == nil && !hasColumnValue(t, "publishable", "true") {
					err = errors.New("no publishable churn row")
				}
			case "outofram":
				err = validateOutOfRAM(t, engine)
			}
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			artifacts = append(artifacts, name)
		}
	}
	for _, engine := range []string{"sqlite", "vibedb"} {
		name := "footprint-indexed-" + engine + ".tsv"
		t, readErr := readTable(filepath.Join(directory, name))
		if readErr != nil {
			return nil, fmt.Errorf("%s: %w", name, readErr)
		}
		if err = validateHeaderRows(t, engine, []string{"exact-indexes", "disk-bytes", "allocated-bytes", "logical-bytes", "disk/logical", "allocated/logical", "maxrss"}); err == nil {
			err = validateColumnContract(t, map[string]string{"git-commit": meta["revision"], "vcs-modified": "false", "durability": "ordinary-sync", "exact-indexes": "3"})
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		artifacts = append(artifacts, name)
	}

	for run := 1; run <= minimumRepetitions; run++ {
		for _, workload := range []string{"mixed", "read", "write"} {
			for _, clients := range []string{"1", "8", "32"} {
				name := fmt.Sprintf("rf3/run-%02d/rf3-%s-clients-%s.tsv", run, workload, clients)
				t, readErr := readTable(filepath.Join(directory, name))
				if readErr != nil {
					return nil, fmt.Errorf("%s: %w", name, readErr)
				}
				if err = validateRF3(t, meta["revision"], workload, clients); err != nil {
					return nil, fmt.Errorf("%s: %w", name, err)
				}
				artifacts = append(artifacts, name)
			}
		}
	}
	chaos := "rf3-chaos.tsv"
	t, err := readTable(filepath.Join(directory, chaos))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", chaos, err)
	}
	if err = validateChaos(t, meta["revision"]); err != nil {
		return nil, fmt.Errorf("%s: %w", chaos, err)
	}
	artifacts = append(artifacts, chaos)

	slices.Sort(artifacts)
	var out bytes.Buffer
	fmt.Fprintln(&out, "schema\tvibedb.publish-validation\t1")
	fmt.Fprintln(&out, "result\tpass")
	fmt.Fprintf(&out, "revision\t%s\nartifacts\t%d\nrepetitions\t%d\n", meta["revision"], len(artifacts), minimumRepetitions)
	for _, name := range artifacts {
		digest, size, digestErr := fileDigest(filepath.Join(directory, name))
		if digestErr != nil {
			return nil, digestErr
		}
		fmt.Fprintf(&out, "artifact\t%s\t%d\t%x\n", name, size, digest)
	}
	return out.Bytes(), nil
}

func readTable(path string) (table, error) {
	f, err := os.Open(path)
	if err != nil {
		return table{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maximumArtifact {
		return table{}, errors.New("artifact is empty or exceeds bound")
	}
	s := bufio.NewScanner(io.LimitReader(f, maximumArtifact+1))
	s.Buffer(make([]byte, 4096), 4<<20)
	var result table
	for s.Scan() {
		line := s.Text()
		if line == "" || strings.ContainsAny(line, "\r\x00") {
			return table{}, errors.New("noncanonical line")
		}
		fields := strings.Split(line, "\t")
		if len(fields) == 1 {
			fields = strings.Fields(line)
		}
		result.lines = append(result.lines, fields)
	}
	if err := s.Err(); err != nil {
		return table{}, err
	}
	if len(result.lines) == 0 {
		return table{}, errors.New("no records")
	}
	return result, nil
}

func exactPairs(t table, kind string) (map[string]string, error) {
	result := make(map[string]string)
	for _, fields := range t.lines {
		if len(fields) == 3 && fields[0] == kind {
			if fields[1] == "" || fields[2] == "" || result[fields[1]] != "" {
				return nil, fmt.Errorf("duplicate or empty metadata %q", fields[1])
			}
			result[fields[1]] = fields[2]
		}
	}
	return result, nil
}

func validateMixed(t table, revision, durability, shape, exactIndexes string, expectedEngines []string) error {
	meta, err := exactPairs(t, "meta")
	if err != nil {
		return err
	}
	if meta["git-commit"] != revision || meta["git-dirty"] != "false" || meta["publishable-suite"] != "true" {
		return errors.New("dirty, mismatched, or diagnostic suite")
	}
	if meta["durability"] != durability || meta["checkpoint-mutations"] != "0" || meta["document-shape"] != shape || meta["exact-indexes"] != exactIndexes || meta["allow-diagnostic"] != "false" {
		return errors.New("suite does not match the publication durability/index/shape contract")
	}
	if n, _ := strconv.Atoi(meta["repetitions"]); n < minimumRepetitions {
		return errors.New("fewer than nine repetitions")
	}
	requiredNames := []string{"p50-us", "p99-us", "p99.9-us", "max-us", "total-ops/s", "disk-MiB", "alloc-MiB", "peak-rss-MiB", "durability-payload/logical"}
	required := make(map[string]map[string]bool, len(expectedEngines))
	for _, engine := range expectedEngines {
		required[engine] = make(map[string]bool, len(requiredNames))
		for _, name := range requiredNames {
			required[engine][name] = false
		}
	}
	enginesSeen := make(map[string]bool)
	rawRuns := make(map[string]map[string]bool)
	for _, row := range t.lines {
		if len(row) >= 12 && row[0] == "summary" {
			engine := row[1]
			if strings.HasPrefix(engine, "vibedb/") {
				engine = "vibedb"
			}
			enginesSeen[engine] = true
			if metrics := required[engine]; metrics != nil {
				if _, ok := metrics[row[10]]; ok {
					samples, parseErr := strconv.Atoi(row[11])
					if parseErr != nil || samples < minimumRepetitions {
						return fmt.Errorf("%s %s has fewer than nine samples", engine, row[10])
					}
					metrics[row[10]] = true
				}
			}
		}
		if len(row) > 4 && row[0] == "raw" {
			engine := row[3]
			if rawRuns[engine] == nil {
				rawRuns[engine] = make(map[string]bool)
			}
			rawRuns[engine][row[1]] = true
		}
	}
	for _, engine := range expectedEngines {
		if !enginesSeen[engine] {
			return fmt.Errorf("missing summary for %s", engine)
		}
		if len(rawRuns[engine]) < minimumRepetitions {
			return fmt.Errorf("%s has fewer than nine raw repetitions", engine)
		}
	}
	for engine, metrics := range required {
		for field, found := range metrics {
			if !found {
				return fmt.Errorf("%s missing summary metric %s", engine, field)
			}
		}
	}
	return nil
}

func validateHeaderRows(t table, engine string, required []string) error {
	if len(t.lines) < 2 {
		return errors.New("header or data row missing")
	}
	header := t.lines[0]
	index := make(map[string]int, len(header))
	for i, name := range header {
		if name == "" {
			return errors.New("invalid header")
		}
		if _, exists := index[name]; exists {
			return errors.New("duplicate header")
		}
		index[name] = i
	}
	for _, name := range required {
		if _, ok := index[name]; !ok {
			return fmt.Errorf("missing column %s", name)
		}
	}
	engineColumn, ok := index["engine"]
	if !ok {
		return errors.New("missing engine column")
	}
	rows := 0
	for _, row := range t.lines[1:] {
		if len(row) != len(header) {
			return errors.New("ragged row")
		}
		if row[engineColumn] != engine && !(engine == "vibedb" && strings.HasPrefix(row[engineColumn], "vibedb/")) {
			return errors.New("engine mismatch")
		}
		rows++
	}
	if rows == 0 {
		return errors.New("no data rows")
	}
	return nil
}

func hasColumnValue(t table, column, value string) bool {
	if len(t.lines) < 2 {
		return false
	}
	idx := slices.Index(t.lines[0], column)
	if idx < 0 {
		return false
	}
	for _, row := range t.lines[1:] {
		if idx < len(row) && row[idx] == value {
			return true
		}
	}
	return false
}

func validateColumnContract(t table, expected map[string]string) error {
	if len(t.lines) < 2 {
		return errors.New("missing rows")
	}
	for name, want := range expected {
		at := slices.Index(t.lines[0], name)
		if at < 0 {
			return fmt.Errorf("missing contract column %s", name)
		}
		for _, row := range t.lines[1:] {
			if at >= len(row) {
				return fmt.Errorf("missing %s value", name)
			}
			if row[at] != want {
				return fmt.Errorf("%s=%q, want %q", name, row[at], want)
			}
		}
	}
	return nil
}

func validateOutOfRAM(t table, engine string) error {
	if err := validateHeaderRows(t, engine, []string{"logical-bytes", "physical-memory-bytes", "peak-rss-bytes", "max-rss-bytes", "physical-write-known", "physical-write-bytes", "apparent-bytes", "allocated-bytes"}); err != nil {
		return err
	}
	h := t.lines[0]
	row := t.lines[1]
	for name, want := range map[string]string{"goos": "linux", "durability": "ordinary-sync", "document-shape": "overflow-heavy", "physical-write-known": "true"} {
		if at := slices.Index(h, name); at < 0 || row[at] != want {
			return fmt.Errorf("%s does not match publication contract", name)
		}
	}
	value := func(name string) uint64 {
		parsed, _ := strconv.ParseUint(row[slices.Index(h, name)], 10, 64)
		return parsed
	}
	if value("logical-bytes") <= value("physical-memory-bytes") || value("peak-rss-bytes") > value("max-rss-bytes") || row[slices.Index(h, "physical-write-known")] != "true" {
		return errors.New("above-RAM or hard resource proof failed")
	}
	return nil
}

func validateRF3(t table, revision, workload, clients string) error {
	if len(t.lines) == 0 || !slices.Equal(t.lines[0], []string{"schema", "vibedb.rf3.evidence", "1"}) {
		return errors.New("wrong RF3 schema")
	}
	meta, err := exactPairs(t, "meta")
	if err != nil {
		return fmt.Errorf("RF3 meta: %w", err)
	}
	config, err := exactPairs(t, "config")
	if err != nil {
		return fmt.Errorf("RF3 config: %w", err)
	}
	for _, required := range []struct{ kind, key, got, want string }{
		{"meta", "durability", meta["durability"], "power-safe"},
		{"meta", "replicas", meta["replicas"], "3"},
		{"meta", "vcs_modified", meta["vcs_modified"], "false"},
		{"meta", "vcs_revision", meta["vcs_revision"], revision},
		{"config", "workload", config["workload"], workload},
		{"config", "clients", config["clients"], clients},
	} {
		if required.got != required.want {
			return fmt.Errorf("RF3 %s %q mismatch: got %q, want %q", required.kind, required.key, required.got, required.want)
		}
	}
	requiredSummary := map[string]bool{"p50_ns": false, "p99_ns": false, "p99.9_ns": false, "max_ns": false}
	requiredCounters := map[string]bool{"network\tsent_bytes": false, "storage\tdevice_bytes": false, "storage\tfile_end": false, "workload\tlogical_write_bytes": workload == "read"}
	requiredOperations := map[string]bool{"read": workload == "write", "write": workload == "read"}
	for _, row := range t.lines {
		if len(row) >= 7 && row[0] == "summary_header" {
			for _, name := range row[1:] {
				if _, ok := requiredSummary[name]; ok {
					requiredSummary[name] = true
				}
			}
		}
		if len(row) == 8 && row[0] == "summary" {
			if _, ok := requiredOperations[row[1]]; ok {
				for _, value := range row[2:] {
					parsed, parseErr := strconv.ParseUint(value, 10, 64)
					if parseErr != nil || parsed == 0 {
						return errors.New("invalid RF3 latency summary")
					}
				}
				requiredOperations[row[1]] = true
			}
		}
		if len(row) == 6 && row[0] == "counter" {
			before, beforeErr := strconv.ParseUint(row[3], 10, 64)
			after, afterErr := strconv.ParseUint(row[4], 10, 64)
			delta, deltaErr := strconv.ParseUint(row[5], 10, 64)
			if beforeErr != nil || afterErr != nil || deltaErr != nil || after < before || after-before != delta {
				return errors.New("invalid RF3 counter cut")
			}
			key := row[1] + "\t" + row[2]
			if _, ok := requiredCounters[key]; ok {
				requiredCounters[key] = true
			}
		}
	}
	for name, found := range requiredSummary {
		if !found {
			return fmt.Errorf("missing RF3 %s", name)
		}
	}
	for name, found := range requiredCounters {
		if !found {
			return fmt.Errorf("missing RF3 counter %s", name)
		}
	}
	for operation, found := range requiredOperations {
		if !found {
			return fmt.Errorf("missing RF3 %s summary", operation)
		}
	}
	return nil
}

func validateChaos(t table, revision string) error {
	return validateChaosAtLeast(t, revision, minimumRepetitions)
}

func validateChaosAtLeast(t table, revision string, minimum int) error {
	if !slices.Equal(t.lines[0], []string{"schema", "vibedb.rf3.chaos-evidence", "2"}) {
		return errors.New("wrong chaos schema")
	}
	meta, _ := exactPairs(t, "meta")
	if meta["vcs_modified"] != "false" || meta["vcs_revision"] != revision {
		return errors.New("dirty or mismatched chaos artifact")
	}
	rawHeader := []string(nil)
	passed := 0
	for _, row := range t.lines {
		if len(row) > 1 && row[0] == "raw_header" {
			rawHeader = row
		}
		if len(row) > 1 && row[0] == "raw" && rawHeader != nil {
			if len(row) != len(rawHeader) {
				return errors.New("ragged chaos row")
			}
			field := func(name string) string {
				at := slices.Index(rawHeader, name)
				if at < 0 {
					return ""
				}
				return row[at]
			}
			if field("passed") != "true" || field("timed_out") != "false" || field("exact_run") != "true" || field("qualification_exact") != "true" {
				return errors.New("failed or inexact chaos run")
			}
			walGrowth, e1 := strconv.ParseUint(field("wal_growth_bytes"), 10, 64)
			walBound, e2 := strconv.ParseUint(field("wal_growth_bound_bytes"), 10, 64)
			rssGrowth, e3 := strconv.ParseUint(field("waiter_rss_growth_bytes"), 10, 64)
			rssBound, e4 := strconv.ParseUint(field("waiter_rss_growth_bound_bytes"), 10, 64)
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil || walGrowth > walBound || rssGrowth > rssBound {
				return errors.New("chaos WAL/RSS bound failed")
			}
			passed++
		}
	}
	if passed < minimum {
		return fmt.Errorf("fewer than %d passing chaos runs", minimum)
	}
	return nil
}

func fileDigest(path string) ([32]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return [32]byte{}, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return [32]byte{}, 0, err
	}
	h := sha256.New()
	if _, err = io.Copy(h, io.LimitReader(f, maximumArtifact+1)); err != nil {
		return [32]byte{}, 0, err
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, info.Size(), nil
}

func writeValidated(directory string, result []byte) error {
	temporary, err := os.CreateTemp(directory, ".VALIDATED-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err = temporary.Write(result); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Link(name, filepath.Join(directory, "VALIDATED.tsv"))
}
