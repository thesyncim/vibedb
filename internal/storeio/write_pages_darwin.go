//go:build darwin

package storeio

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// darwinWritevWidth matches x/sys/unix's stack-backed iovec capacity. Keeping
// each call at or below this width removes write syscalls without introducing
// a heap allocation in the durable commit path.
const darwinWritevWidth = 8

// writeDataPages batches adjacent physical ranges into bounded positional
// vectored writes. Commit has already validated that pages are sorted and
// non-overlapping. Gaps deliberately end a vector: pwritev describes one
// contiguous file range and must never fill an uncommitted hole.
//
// Only the data phase is batched. portableDevice.Commit still completes its
// data barrier before writing the alternate root and completing the final
// barrier, preserving root-last recovery semantics.
func writeDataPages(
	file *os.File,
	arena []byte,
	bufferSize int,
	frameArena []byte,
	frameSize int,
	pages []Write,
) error {
	var vectors [darwinWritevWidth][]byte
	for index := 0; index < len(pages); {
		count := 1
		end := pages[index].Offset + int64(pages[index].Length)
		for index+count < len(pages) && count < len(vectors) &&
			pages[index+count].Offset == end {
			end += int64(pages[index+count].Length)
			count++
		}
		if count == 1 {
			if err := writeArenaAt(
				file, arena, bufferSize, frameArena, frameSize, pages[index],
			); err != nil {
				return err
			}
			index++
			continue
		}
		total := 0
		for vector := 0; vector < count; vector++ {
			write := pages[index+vector]
			data, err := writeBytes(
				arena, bufferSize, frameArena, frameSize, write,
			)
			if err != nil {
				return err
			}
			vectors[vector] = data
			total += len(data)
		}
		if err := pwritevFull(int(file.Fd()), vectors[:count], pages[index].Offset, total); err != nil {
			return err
		}
		clear(vectors[:count])
		index += count
	}
	return nil
}

func pwritevFull(fd int, vectors [][]byte, offset int64, total int) error {
	written := 0
	for written < total {
		n, err := unix.Pwritev(fd, vectors, offset+int64(written))
		if n > 0 {
			written += n
			vectors = trimWrittenVectors(vectors, n)
		}
		if err != nil && !errors.Is(err, unix.EINTR) {
			return err
		}
		if n == 0 && err == nil {
			return io.ErrShortWrite
		}
	}
	return nil
}

func trimWrittenVectors(vectors [][]byte, written int) [][]byte {
	for len(vectors) != 0 && written >= len(vectors[0]) {
		written -= len(vectors[0])
		vectors[0] = nil
		vectors = vectors[1:]
	}
	if written != 0 {
		vectors[0] = vectors[0][written:]
	}
	return vectors
}
