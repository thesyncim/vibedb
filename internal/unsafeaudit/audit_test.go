package unsafeaudit

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// updateBlock rewrites the marked block in UNSAFE.md from the current source
// tree instead of asserting against it. It mirrors the -update convention used
// by golden-file tests: run once with the flag to regenerate, then commit.
var updateBlock = flag.Bool("update", false,
	"rewrite the <!-- unsafe-file-list --> block in UNSAFE.md from the current source tree")

// repoRoot is the root module directory relative to this package.
func repoRoot() string { return filepath.Join("..", "..") }

func unsafeDocPath() string { return filepath.Join(repoRoot(), "UNSAFE.md") }

// regenerateCommand is the exact invocation the failure message and UNSAFE.md
// both print. Keeping it in one place stops the two from drifting.
const regenerateCommand = "go test ./internal/unsafeaudit " +
	"-run TestUnsafeFileListMatchesSource -update"

// TestUnsafeFileListMatchesSource is the executable guard: it recomputes the
// unsafe-importing file set from the source tree and byte-compares the rendered
// block against the marked region of UNSAFE.md. With -update it rewrites that
// region instead. Modeled on conformance.TestPublicCapabilityTableMatchesExecutableManifest.
func TestUnsafeFileListMatchesSource(t *testing.T) {
	want, err := RenderBlock(repoRoot())
	if err != nil {
		t.Fatalf("recompute unsafe file list: %v", err)
	}
	docPath := unsafeDocPath()
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, BlockStart)
	end := strings.Index(text, BlockEnd)
	if start < 0 || end < start {
		t.Fatalf("%s is missing the %q / %q markers; add them around the file list",
			docPath, strings.TrimRight(BlockStart, "\n"), BlockEnd)
	}
	bodyStart := start + len(BlockStart)

	if *updateBlock {
		updated := text[:bodyStart] + want + text[end:]
		if updated == text {
			t.Logf("%s unsafe-file-list block already current", docPath)
			return
		}
		if err := os.WriteFile(docPath, []byte(updated), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("rewrote unsafe-file-list block in %s", docPath)
		return
	}

	if got := text[bodyStart:end]; got != want {
		t.Fatalf("UNSAFE.md unsafe-file-list block is stale; regenerate it with:\n\t%s\n"+
			"--- got ---\n%s\n--- want ---\n%s", regenerateCommand, got, want)
	}
}

// TestUnsafeDocPrintsRegenerateCommand keeps the command UNSAFE.md advertises in
// lockstep with the command this test actually accepts.
func TestUnsafeDocPrintsRegenerateCommand(t *testing.T) {
	raw, err := os.ReadFile(unsafeDocPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), regenerateCommand) {
		t.Fatalf("UNSAFE.md does not print the regeneration command %q", regenerateCommand)
	}
}

// TestCollectUnsafeFilesShape asserts the structural invariants the guard relies
// on: a sorted, deduplicated set that excludes test files and nested modules,
// includes a stable sentinel, and no longer names files the audit found stale.
func TestCollectUnsafeFilesShape(t *testing.T) {
	files, err := CollectUnsafeFiles(repoRoot())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected at least one unsafe-importing production file")
	}
	seen := make(map[string]bool, len(files))
	for i, file := range files {
		if seen[file] {
			t.Fatalf("duplicate entry %q", file)
		}
		seen[file] = true
		if i > 0 && files[i-1] > file {
			t.Fatalf("list is not sorted: %q precedes %q", files[i-1], file)
		}
		if strings.HasSuffix(file, "_test.go") {
			t.Fatalf("test file leaked into the inventory: %q", file)
		}
		for _, nested := range []string{
			"x/vitessroute/", "bench/competitive/", "integration/pgclient/",
		} {
			if strings.HasPrefix(file, nested) {
				t.Fatalf("nested module file leaked into the inventory: %q", file)
			}
		}
	}
	if !seen["store/store_mapped_docs.go"] {
		t.Fatal("expected sentinel store/store_mapped_docs.go in the inventory")
	}
	// Regression guards for the stale entries the audit removed: store_file.go
	// dropped its unsafe import, and the float64 SIMD reducers were deleted.
	for _, gone := range []string{
		"store/durable/store_file.go",
		"store/store_float64_reduce_simd_amd64.go",
		"store/store_float64_reduce_simd_arm64.go",
	} {
		if seen[gone] {
			t.Fatalf("removed file %q reappeared in the inventory", gone)
		}
	}
}
