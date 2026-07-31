package storeio

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
)

func BenchmarkPortableCommit(b *testing.B) {
	for _, pageCount := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("contiguous-pages-%d", pageCount), func(b *testing.B) {
			benchmarkPortableCommit(b, pageCount, false)
		})
	}
	b.Run("sparse-pages-8", func(b *testing.B) {
		benchmarkPortableCommit(b, 8, true)
	})
}

func benchmarkPortableCommit(b *testing.B, pageCount int, sparse bool) {
	file, err := os.CreateTemp(b.TempDir(), "portable-commit")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: pageCount + 1, BufferSize: pageSize,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer device.Close()

	writes := make([]Write, pageCount)
	for index := range writes {
		buffer, bufferErr := device.Buffer(index)
		if bufferErr != nil {
			b.Fatal(bufferErr)
		}
		for offset := range buffer {
			buffer[offset] = byte(index + 1)
		}
		multiplier := index + 1
		if sparse {
			multiplier = 2*index + 1
		}
		writes[index] = Write{
			Offset: int64(multiplier * pageSize),
			Length: uint32(pageSize),
			Buffer: uint16(index),
		}
	}
	rootBuffer, err := device.Buffer(pageCount)
	if err != nil {
		b.Fatal(err)
	}
	rootBuffer[0] = 1
	root := Write{Offset: 0, Length: uint32(pageSize), Buffer: uint16(pageCount)}

	b.SetBytes(int64((pageCount + 1) * pageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := device.Commit(writes, root); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIOUringCommit(b *testing.B) {
	for _, pageCount := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("contiguous-pages-%d", pageCount), func(b *testing.B) {
			benchmarkIOUringCommit(b, pageCount, false)
		})
	}
	b.Run("sparse-pages-8", func(b *testing.B) {
		benchmarkIOUringCommit(b, 8, true)
	})
}

func benchmarkIOUringCommit(b *testing.B, pageCount int, sparse bool) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	file, err := os.CreateTemp(b.TempDir(), "io-uring-commit")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendIOUring, BufferCount: pageCount + 1,
		BufferSize: pageSize, QueueDepth: max(64, pageCount+1), SingleIssuer: true,
	})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		b.Skip(err)
	}
	if err != nil {
		b.Fatal(err)
	}
	defer device.Close()

	writes := make([]Write, pageCount)
	for index := range writes {
		buffer, bufferErr := device.Buffer(index)
		if bufferErr != nil {
			b.Fatal(bufferErr)
		}
		buffer[0] = byte(index + 1)
		multiplier := index + 1
		if sparse {
			multiplier = 2*index + 1
		}
		writes[index] = Write{
			Offset: int64(multiplier * pageSize),
			Length: uint32(pageSize), Buffer: uint16(index),
		}
	}
	rootBuffer, err := device.Buffer(pageCount)
	if err != nil {
		b.Fatal(err)
	}
	rootBuffer[0] = 1
	root := Write{Length: uint32(pageSize), Buffer: uint16(pageCount)}

	b.SetBytes(int64((pageCount + 1) * pageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := device.Commit(writes, root); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDarwinDataPageWrites(b *testing.B) {
	for _, pageCount := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("contiguous-pages-%d", pageCount), func(b *testing.B) {
			benchmarkDarwinDataPageWrites(b, pageCount, false)
		})
	}
	b.Run("sparse-pages-8", func(b *testing.B) {
		benchmarkDarwinDataPageWrites(b, 8, true)
	})
}

func benchmarkDarwinDataPageWrites(b *testing.B, pageCount int, sparse bool) {
	file, err := os.CreateTemp(b.TempDir(), "data-page-writes")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	arena := make([]byte, pageCount*pageSize)
	writes := make([]Write, pageCount)
	for index := range writes {
		multiplier := index + 1
		if sparse {
			multiplier = 2*index + 1
		}
		writes[index] = Write{
			Offset: int64(multiplier * pageSize),
			Length: uint32(pageSize),
			Buffer: uint16(index),
		}
	}
	b.SetBytes(int64(pageCount * pageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := writeDataPages(
			file, arena, pageSize, nil, 0, writes,
		); err != nil {
			b.Fatal(err)
		}
	}
}
