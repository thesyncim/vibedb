//go:build darwin || linux

package gatewayruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Executables are build artifacts, not database state. Keep their independently
// owned lifetime outside the data root; never filter SQL/WAL/private files out
// of the persistent-storage measurement to compensate for build output size.
func durableRF3ExternalBinaryPaths(t testing.TB, dataRoot string) (string, string) {
	t.Helper()
	buildRoot := t.TempDir()
	relative, err := filepath.Rel(dataRoot, buildRoot)
	if err != nil || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("build output is inside persistent data root: %q relative=%q err=%v", buildRoot, relative, err)
	}
	return filepath.Join(buildRoot, "vibedb-shard"), filepath.Join(buildRoot, "vibedb-gateway")
}

func TestDurableRF3StorageCountsAllDataButSeparatesBuildArtifacts(t *testing.T) {
	dataRoot := t.TempDir()
	shard, gateway := durableRF3ExternalBinaryPaths(t, dataRoot)
	if shard == gateway || filepath.Dir(shard) != filepath.Dir(gateway) {
		t.Fatal("invalid independently owned executable paths")
	}
	write := func(path string, size int) {
		t.Helper()
		if err := os.WriteFile(path, bytes.Repeat([]byte{0x5a}, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"member.wal", "member.vdb", "catalog.vibejson", ".private-generation"} {
		write(filepath.Join(dataRoot, name), 64<<10)
	}
	before := replicaProcessAllocatedBytes(dataRoot, "")
	if before < 4*(64<<10) {
		t.Fatalf("data allocation missing: %d", before)
	}
	write(shard, 1<<20)
	write(gateway, 1<<20)
	if after := replicaProcessAllocatedBytes(dataRoot, ""); after != before {
		t.Fatalf("build artifacts entered database measurement: before=%d after=%d", before, after)
	}
	if binaries := replicaProcessAllocatedBytes(filepath.Dir(shard), ""); binaries < 2<<20 {
		t.Fatalf("build artifacts lost separate allocation evidence: %d", binaries)
	}
	write(filepath.Join(dataRoot, ".private-generation"), 1<<20)
	if after := replicaProcessAllocatedBytes(dataRoot, ""); after < before+(1<<20)-(64<<10) {
		t.Fatalf("private database growth excluded: before=%d after=%d", before, after)
	}
}
