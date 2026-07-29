//go:build windows

package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPublishableTableTempMovesWhileOpen(t *testing.T) {
	directory := t.TempDir()
	file, err := createPublishableTableTemp(directory, ".table.tmp-")
	if err != nil {
		t.Fatal(err)
	}
	from := file.Name()
	to := filepath.Join(directory, "table.vjc")
	if err := publishNewPath(from, to); err != nil {
		_ = file.Close()
		t.Fatalf("publish open table file: %v", err)
	}
	if _, err := file.Write([]byte("kept")); err != nil {
		_ = file.Close()
		t.Fatalf("write through moved table handle: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(to)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "kept" {
		t.Fatalf("moved table contents = %q, want kept", raw)
	}
}
