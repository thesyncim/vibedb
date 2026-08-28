//go:build linux || darwin

package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

// TestFilePrimaryAdvancedRepackAmplification binds the schema+exact-index+
// mixed-overflow path to normalized physical-space and device-write limits.
// A corpus large enough to amortize fixed roots prevents byte ceilings from
// hiding amplification as formats evolve.
func TestFilePrimaryAdvancedRepackAmplification(t *testing.T) {
	const rows = 2_048
	options := testFileCatalogOptions(t)
	options.Durability = DurabilityBufferedVisible
	options.InlineValueBytes = 256
	options.MaxDocumentBytes = 16 << 10
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "advanced-source.vibe")
	sourceFile, err := os.OpenFile(
		sourcePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	source, err := Create(sourceFile, options)
	if err != nil {
		t.Fatal(err)
	}
	logicalBytes := uint64(0)
	for row := range rows {
		key := fmt.Appendf(nil, "row-%08d", row)
		value := strconv.AppendInt([]byte(`{"id":`), int64(row), 10)
		if row&1 != 0 {
			value = append(value, `,"pad":"`...)
			value = append(value, bytes.Repeat([]byte{'x'}, 6<<10)...)
			value = append(value, '"')
		}
		value = append(value, '}')
		logicalBytes += uint64(len(key) + len(value))
		if _, err = source.Put(key, value); err != nil {
			t.Fatalf("put row %d: %v", row, err)
		}
	}
	if err = source.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	if err = sourceFile.Close(); err != nil {
		t.Fatal(err)
	}
	sourceFile, err = os.OpenFile(sourcePath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceFile.Close()
	outFile, err := os.OpenFile(
		filepath.Join(directory, "advanced-repacked.vibe"),
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()
	report, err := Repack(sourceFile, outFile, options)
	if err != nil {
		t.Fatal(err)
	}
	if report.Documents != rows {
		t.Fatalf("repacked documents = %d, want %d", report.Documents, rows)
	}
	info, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(outFile.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	allocated := uint64(stat.Blocks) * 512
	apparent := uint64(info.Size())
	if apparent > 3*logicalBytes {
		t.Fatalf("advanced repack apparent amplification = %d/%d (>3x)", apparent, logicalBytes)
	}
	if allocated > 3*logicalBytes {
		t.Fatalf("advanced repack allocated amplification = %d/%d (>3x)", allocated, logicalBytes)
	}
	if report.OutputDeviceBytes == 0 || report.OutputDeviceBytes > 8*logicalBytes {
		t.Fatalf("advanced repack device-write amplification = %d/%d (>8x)", report.OutputDeviceBytes, logicalBytes)
	}
	t.Logf("advanced repack ratios: apparent=%.3fx allocated=%.3fx device-write=%.3fx",
		float64(apparent)/float64(logicalBytes),
		float64(allocated)/float64(logicalBytes),
		float64(report.OutputDeviceBytes)/float64(logicalBytes))
}
