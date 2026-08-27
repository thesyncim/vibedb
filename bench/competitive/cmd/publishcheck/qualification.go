package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
)

const qualificationChaosRepetitions = 3

// validateQualification checks the deliberately bounded clean-Ubuntu CI cut.
// It is evidence that every real harness executed and remained within its
// contracts, not publication-grade competitive evidence or a superiority claim.
func validateQualification(directory string) ([]byte, error) {
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
	if meta["command_schema"] != "vibedb.ci-evidence/1" || meta["vcs_modified"] != "false" || meta["goos"] != "linux" {
		return nil, errors.New("qualification requires its exact schema, Linux, and a clean tree")
	}
	for name, want := range map[string]string{
		"embedded_repetitions": "9", "rf3_repetitions": "3", "chaos_repetitions": "3",
		"corpus_documents": "256", "measured_operations": "512",
	} {
		if meta[name] != want {
			return nil, fmt.Errorf("metadata %s=%q, want %q", name, meta[name], want)
		}
	}

	artifacts := []string{"metadata.tsv"}
	for _, item := range []struct {
		name, durability, exactIndexes string
		engines                        []string
	}{
		{"mixed-ordinary-sync.tsv", "ordinary-sync", "0", engines},
		{"mixed-indexed-ordinary-sync.tsv", "ordinary-sync", "3", []string{"sqlite", "vibedb"}},
		{"mixed-power-safe.tsv", "power-safe", "3", []string{"sqlite", "vibedb"}},
	} {
		t, readErr := readTable(filepath.Join(directory, item.name))
		if readErr != nil {
			return nil, fmt.Errorf("%s: %w", item.name, readErr)
		}
		if err = validateMixed(t, meta["revision"], item.durability, "inline", item.exactIndexes, item.engines); err != nil {
			return nil, fmt.Errorf("%s: %w", item.name, err)
		}
		artifacts = append(artifacts, item.name)
	}

	for _, engine := range engines {
		name := "footprint-" + engine + ".tsv"
		t, readErr := readTable(filepath.Join(directory, name))
		if readErr != nil {
			return nil, fmt.Errorf("%s: %w", name, readErr)
		}
		if err = validateHeaderRows(t, engine, []string{"disk-bytes", "allocated-bytes", "logical-bytes", "disk/logical", "allocated/logical", "maxrss"}); err == nil {
			err = validateColumnContract(t, map[string]string{"git-commit": meta["revision"], "vcs-modified": "false", "durability": "ordinary-sync"})
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		artifacts = append(artifacts, name)
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

	for run := 1; run <= qualificationChaosRepetitions; run++ {
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
	chaosName := "rf3-chaos.tsv"
	chaos, err := readTable(filepath.Join(directory, chaosName))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", chaosName, err)
	}
	if err = validateChaosAtLeast(chaos, meta["revision"], qualificationChaosRepetitions); err != nil {
		return nil, fmt.Errorf("%s: %w", chaosName, err)
	}
	artifacts = append(artifacts, chaosName)

	slices.Sort(artifacts)
	if err = validateExactQualificationArtifacts(directory, artifacts); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	fmt.Fprintln(&out, "schema\tvibedb.ci-validation\t1")
	fmt.Fprintln(&out, "result\tpass")
	fmt.Fprintf(&out, "revision\t%s\nartifacts\t%d\nembedded_repetitions\t%d\nrf3_repetitions\t%d\nchaos_repetitions\t%d\n",
		meta["revision"], len(artifacts), minimumRepetitions, qualificationChaosRepetitions, qualificationChaosRepetitions)
	for _, name := range artifacts {
		digest, size, digestErr := fileDigest(filepath.Join(directory, name))
		if digestErr != nil {
			return nil, digestErr
		}
		fmt.Fprintf(&out, "artifact\t%s\t%d\t%x\n", name, size, digest)
	}
	return out.Bytes(), nil
}

func validateExactQualificationArtifacts(directory string, expected []string) error {
	actual := make([]string, 0, len(expected))
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == directory || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("qualification artifact %s is not a regular file", path)
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		actual = append(actual, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return fmt.Errorf("artifact inventory: %w", err)
	}
	slices.Sort(actual)
	if !slices.Equal(actual, expected) {
		return fmt.Errorf("artifact inventory mismatch: got %v, want %v", actual, expected)
	}
	return nil
}
