package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// captureRun redirects os.Stdout and os.Stderr around one run() invocation and
// returns the exit code together with everything the command wrote. The command
// writes findings and its machine-parseable summary to os.Stdout and usage plus
// error diagnostics to os.Stderr, so both streams must be observed to assert the
// documented contract. These tests never call t.Parallel, so the process-global
// stream swap is serialized within this binary.
func captureRun(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	originalOut, originalErr := os.Stdout, os.Stderr
	outReader, outWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	errReader, errWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stdout, os.Stderr = outWriter, errWriter

	var outBuf, errBuf bytes.Buffer
	drained := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(&outBuf, outReader); drained <- struct{}{} }()
	go func() { _, _ = io.Copy(&errBuf, errReader); drained <- struct{}{} }()

	code = run(args)

	os.Stdout, os.Stderr = originalOut, originalErr
	_ = outWriter.Close()
	_ = errWriter.Close()
	<-drained
	<-drained
	_ = outReader.Close()
	_ = errReader.Close()
	return code, outBuf.String(), errBuf.String()
}

// buildFixture writes a small, cleanly closed inline-primary store file at path
// through the durable bulk-load entry point and returns its keys. The verify
// tool consumes a single store file, and repack is inline-primary only, so the
// fixture is built with CreateFromRecords rather than through vibedb.Open or the
// sql/driver (both of which produce a directory-backed catalog with schemas that
// repack rejects and that the tool's single-file <store-file> argument cannot
// address).
func buildFixture(t *testing.T, path string, count int) []string {
	t.Helper()
	records := make([]durable.PrimaryBulkRecord, count)
	keys := make([]string, count)
	for i := range records {
		key := fmt.Sprintf("%08d", i)
		keys[i] = key
		records[i] = durable.PrimaryBulkRecord{
			Key:   key,
			Value: []byte(fmt.Sprintf("%q", "doc-"+key)),
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create fixture %q: %v", path, err)
	}
	if _, err := durable.CreateFromRecords(records, file, durable.Options{
		Backend: durable.BackendPortable,
	}); err != nil {
		_ = file.Close()
		t.Fatalf("CreateFromRecords: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close fixture: %v", err)
	}
	return keys
}

// TestRunUsageErrors covers the dispatch and arity validation branches: an
// absent subcommand, an unknown subcommand, and every subcommand invoked with
// the wrong number of positional arguments all print usage to stderr and exit 2
// without touching any file.
func TestRunUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no_subcommand", []string{"vibedb-verify"}},
		{"unknown_subcommand", []string{"vibedb-verify", "wat"}},
		{"verify_missing_arg", []string{"vibedb-verify", "verify"}},
		{"verify_extra_arg", []string{"vibedb-verify", "verify", "a", "b"}},
		{"salvage_missing_arg", []string{"vibedb-verify", "salvage", "only-source"}},
		{"salvage_extra_arg", []string{"vibedb-verify", "salvage", "a", "b", "c"}},
		{"repack_missing_arg", []string{"vibedb-verify", "repack", "only-source"}},
		{"repack_extra_arg", []string{"vibedb-verify", "repack", "a", "b", "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := captureRun(t, tc.args...)
			if code != 2 {
				t.Fatalf("run(%v) = %d, want 2", tc.args, code)
			}
			if !strings.Contains(stderr, "usage:") {
				t.Fatalf("run(%v) stderr = %q, want it to contain usage:", tc.args, stderr)
			}
			if stdout != "" {
				t.Fatalf("run(%v) stdout = %q, want empty on a usage error", tc.args, stdout)
			}
		})
	}
}

// TestRunVerifyOpenFailure proves the store path reaches os.Open and that an
// input that cannot be opened is a code-1 failure distinct from a code-2 usage
// error, with the offending path named on stderr.
func TestRunVerifyOpenFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-store.vibe")
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", missing)
	if code != 1 {
		t.Fatalf("verify(missing) = %d, want 1", code)
	}
	if !strings.Contains(stderr, "error open") {
		t.Fatalf("verify(missing) stderr = %q, want it to contain error open", stderr)
	}
	if strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(missing) reported result ok on a missing file: %q", stdout)
	}
}

// TestVerifyHappyPath builds a small clean store and proves verify walks it
// without findings, exits 0, and emits the ok result plus a summary reporting
// the exact document count.
func TestVerifyHappyPath(t *testing.T) {
	const documents = 256
	path := filepath.Join(t.TempDir(), "clean.vibe")
	buildFixture(t, path, documents)

	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", path)
	if code != 0 {
		t.Fatalf("verify(clean) = %d, want 0; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if strings.Contains(stdout, "finding kind=") {
		t.Fatalf("verify(clean) reported findings on a clean store: %q", stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(clean) stdout = %q, want it to contain result ok", stdout)
	}
	if want := fmt.Sprintf("documents=%d", documents); !strings.Contains(stdout, want) {
		t.Fatalf("verify(clean) stdout = %q, want it to contain %q", stdout, want)
	}
}

// TestRepackRoundTrip repacks a fixture into a fresh destination and proves the
// repacked output itself passes verification: repack exits 0 reporting the full
// document count, and verify of the destination exits 0 with an ok result.
func TestRepackRoundTrip(t *testing.T) {
	const documents = 256
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.vibe")
	outPath := filepath.Join(dir, "repacked.vibe")
	buildFixture(t, srcPath, documents)

	code, stdout, stderr := captureRun(t, "vibedb-verify", "repack", srcPath, outPath)
	if code != 0 {
		t.Fatalf("repack = %d, want 0; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("repack stdout = %q, want it to contain result ok", stdout)
	}
	if want := fmt.Sprintf("repack documents=%d", documents); !strings.Contains(stdout, want) {
		t.Fatalf("repack stdout = %q, want it to contain %q", stdout, want)
	}

	code, stdout, stderr = captureRun(t, "vibedb-verify", "verify", outPath)
	if code != 0 {
		t.Fatalf("verify(repacked) = %d, want 0; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(repacked) stdout = %q, want it to contain result ok", stdout)
	}
	if want := fmt.Sprintf("documents=%d", documents); !strings.Contains(stdout, want) {
		t.Fatalf("verify(repacked) stdout = %q, want it to contain %q", stdout, want)
	}
}

// TestSalvageRoundTrip proves the salvage subcommand recovers a clean store's
// documents into a fresh output that itself passes verification: salvage exits 0
// reporting the full document count, and verify of the salvaged output exits 0
// with an ok result.
func TestSalvageRoundTrip(t *testing.T) {
	const documents = 256
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.vibe")
	outPath := filepath.Join(dir, "salvaged.vibe")
	buildFixture(t, srcPath, documents)

	code, stdout, stderr := captureRun(t, "vibedb-verify", "salvage", srcPath, outPath)
	if code != 0 {
		t.Fatalf("salvage = %d, want 0; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("salvage stdout = %q, want it to contain result ok", stdout)
	}
	if want := fmt.Sprintf("documents=%d", documents); !strings.Contains(stdout, want) {
		t.Fatalf("salvage stdout = %q, want it to contain %q", stdout, want)
	}

	code, stdout, stderr = captureRun(t, "vibedb-verify", "verify", outPath)
	if code != 0 {
		t.Fatalf("verify(salvaged) = %d, want 0; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(salvaged) stdout = %q, want it to contain result ok", stdout)
	}
}

// TestVerifyDetectsTruncation is the deterministic corruption gate. A CreateFrom
// Records store is written with no trailing slack, so its superblock records a
// FileEnd equal to the file size; truncating the tail below that FileEnd removes
// pages the root reaches. The same fixture verifies clean before truncation and
// fails with a non-zero exit after it, isolating truncation as the cause.
//
// Truncation is chosen over an in-region byte flip because locating a leaf page
// to corrupt deterministically requires the durable package's unexported page
// scanner, which is not reachable from this command's package; truncation needs
// only the file size and is detected regardless of which reachable page the
// removed region held.
func TestVerifyDetectsTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "victim.vibe")
	buildFixture(t, path, 256)

	if code, _, stderr := captureRun(t, "vibedb-verify", "verify", path); code != 0 {
		t.Fatalf("verify(intact) = %d, want 0; stderr=%q", code, stderr)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat fixture: %v", err)
	}
	truncated := info.Size() / 2
	if truncated <= 0 {
		t.Fatalf("fixture too small to truncate: size %d", info.Size())
	}
	if err := os.Truncate(path, truncated); err != nil {
		t.Fatalf("truncate fixture: %v", err)
	}

	code, stdout, _ := captureRun(t, "vibedb-verify", "verify", path)
	if code != 1 {
		t.Fatalf("verify(truncated) = %d, want 1", code)
	}
	if strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(truncated) reported result ok on a truncated store: %q", stdout)
	}
}
