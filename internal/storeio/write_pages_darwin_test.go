//go:build darwin

package storeio

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinVectoredWritesMatchPositionalWrites(t *testing.T) {
	pageSize := os.Getpagesize()
	tests := []struct {
		name    string
		offsets []int64
	}{
		{name: "single", offsets: []int64{int64(pageSize)}},
		{name: "contiguous pair", offsets: []int64{int64(pageSize), int64(2 * pageSize)}},
		{name: "vector boundary", offsets: pageOffsets(pageSize, 10, false)},
		{name: "sparse", offsets: pageOffsets(pageSize, 10, true)},
		{name: "mixed runs", offsets: []int64{
			int64(pageSize), int64(2 * pageSize), int64(4 * pageSize),
			int64(5 * pageSize), int64(6 * pageSize), int64(9 * pageSize),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arena := make([]byte, len(test.offsets)*pageSize)
			writes := make([]Write, len(test.offsets))
			for index, offset := range test.offsets {
				buffer := arena[index*pageSize : (index+1)*pageSize]
				for byteIndex := range buffer {
					buffer[byteIndex] = byte(1 + index + byteIndex%251)
				}
				writes[index] = Write{
					Offset: offset, Length: uint32(pageSize), Buffer: uint16(index),
				}
			}

			wantFile, err := os.CreateTemp(t.TempDir(), "positional")
			if err != nil {
				t.Fatal(err)
			}
			defer wantFile.Close()
			for _, write := range writes {
				if err := writeArenaAt(
					wantFile, arena, pageSize, nil, 0, write,
				); err != nil {
					t.Fatal(err)
				}
			}

			gotFile, err := os.CreateTemp(t.TempDir(), "vectored")
			if err != nil {
				t.Fatal(err)
			}
			defer gotFile.Close()
			if err := writeDataPages(
				gotFile, arena, pageSize, nil, 0, writes,
			); err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(wantFile.Name())
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(gotFile.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("vectored bytes differ from positional bytes")
			}
		})
	}
}

func TestDarwinVectoredDataFailureDoesNotWriteRoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read-only-store")
	wantRoot := []byte("old-root")
	if err := os.WriteFile(path, wantRoot, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 3, BufferSize: pageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	copy(deviceBuffer(t, device, 0), "first-page")
	copy(deviceBuffer(t, device, 1), "second-page")
	copy(deviceBuffer(t, device, 2), "new-root")
	err = device.Commit([]Write{
		{Offset: int64(pageSize), Length: uint32(pageSize), Buffer: 0},
		{Offset: int64(2 * pageSize), Length: uint32(pageSize), Buffer: 1},
	}, Write{Offset: 0, Length: 8, Buffer: 2})
	if err == nil {
		t.Fatal("vectored commit to read-only file succeeded")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wantRoot) {
		t.Fatalf("root changed after vectored data failure: %q", got)
	}
}

func TestDarwinPwritevClosedDescriptor(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "closed-descriptor")
	if err != nil {
		t.Fatal(err)
	}
	fd := int(file.Fd())
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	vectors := [][]byte{[]byte("first"), []byte("second")}
	if err := pwritevFull(fd, vectors, 0, 11); !errors.Is(err, unix.EBADF) {
		t.Fatalf("pwritevFull error = %v, want %v", err, unix.EBADF)
	}
}

func TestDarwinTrimWrittenVectors(t *testing.T) {
	vectors := [][]byte{[]byte("abcd"), []byte("ef"), []byte("ghi")}
	got := trimWrittenVectors(vectors, 5)
	if len(got) != 2 || string(got[0]) != "f" || string(got[1]) != "ghi" {
		t.Fatalf("trimmed vectors = %q", got)
	}
}

func TestDarwinVectoredWritesDoNotAllocate(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "vectored-alloc")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	arena := make([]byte, darwinWritevWidth*pageSize)
	writes := make([]Write, darwinWritevWidth)
	for index := range writes {
		writes[index] = Write{
			Offset: int64((index + 1) * pageSize),
			Length: uint32(pageSize),
			Buffer: uint16(index),
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := writeDataPages(
			file, arena, pageSize, nil, 0, writes,
		); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("vectored writes allocated %g times, want 0", allocs)
	}
}

func pageOffsets(pageSize, count int, sparse bool) []int64 {
	offsets := make([]int64, count)
	for index := range offsets {
		multiplier := index + 1
		if sparse {
			multiplier = 2*index + 1
		}
		offsets[index] = int64(multiplier * pageSize)
	}
	return offsets
}
