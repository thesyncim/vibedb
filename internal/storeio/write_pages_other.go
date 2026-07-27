//go:build !darwin

package storeio

import "os"

func writeDataPages(
	file *os.File,
	arena []byte,
	bufferSize int,
	frameArena []byte,
	frameSize int,
	pages []Write,
) error {
	for _, write := range pages {
		if err := writeArenaAt(
			file, arena, bufferSize, frameArena, frameSize, write,
		); err != nil {
			return err
		}
	}
	return nil
}
