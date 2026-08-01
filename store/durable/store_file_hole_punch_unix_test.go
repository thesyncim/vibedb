//go:build linux || darwin

package durable

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFileStorePlatformHolePunchReducesAllocatedBlocks(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "hole-punch-physical-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	const (
		block     = 4096
		fileBytes = 16 << 20
		offset    = 4 << 20
		length    = 8 << 20
	)
	buffer := make([]byte, 64<<10)
	for index := range buffer {
		buffer[index] = byte(index*131 + 17)
	}
	for written := 0; written < fileBytes; written += len(buffer) {
		if _, err := file.Write(buffer); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	before, err := holePunchAllocatedBytes(file)
	if err != nil {
		t.Fatal(err)
	}
	supported, err := punchFileStoreHole(file, offset, length)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Skip("filesystem does not support hole punching")
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	after, err := holePunchAllocatedBytes(file)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != fileBytes {
		t.Fatalf("apparent size = %d, want %d", info.Size(), fileBytes)
	}
	if after >= before || before-after < length-block {
		t.Fatalf(
			"allocated bytes before=%d after=%d, want about %d released",
			before, after, length,
		)
	}
	probe := make([]byte, block)
	if _, err := file.ReadAt(probe, offset+length/2); err != nil {
		t.Fatal(err)
	}
	for index, value := range probe {
		if value != 0 {
			t.Fatalf("punched byte %d = %02x, want zero", index, value)
		}
	}
}

func holePunchAllocatedBytes(file *os.File) (uint64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Blocks) * 512, nil
}
