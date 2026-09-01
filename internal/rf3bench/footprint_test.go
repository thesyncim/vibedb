package rf3bench

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMeasureFootprintCountsExactRegularFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(root, "nested", "two")
	file, err := os.OpenFile(second, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(1 << 20); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, filepath.Join(root, "ignored-link")); err != nil {
		t.Fatal(err)
	}
	footprint, err := MeasureFootprint(root)
	if err != nil {
		t.Fatal(err)
	}
	if footprint.Files != 2 || footprint.ApparentBytes != 3+(1<<20) ||
		footprint.AllocatedBytes > footprint.ApparentBytes+8192 {
		t.Fatalf("unexpected footprint: %+v", footprint)
	}
}

func TestMeasureFootprintRejectsMissingAndEmptyRoots(t *testing.T) {
	if _, err := MeasureFootprint(); err == nil {
		t.Fatal("empty root set succeeded")
	}
	if _, err := MeasureFootprint(""); err == nil {
		t.Fatal("empty root succeeded")
	}
	if _, err := MeasureFootprint(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing root succeeded")
	}
}
