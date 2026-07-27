//go:build !darwin && !linux

package storeio

import "os"

func dataSync(file *os.File) error { return file.Sync() }

// filesystemSync implements the ordinary filesystem-strength checkpoint
// boundary. Keeping it distinct avoids Darwin's os.File.Sync F_FULLFSYNC trap;
// ordinary mode must not pay for a power-safe device-cache drain.
func filesystemSync(file *os.File) error { return file.Sync() }

func dataBarrier(file *os.File) error { return dataSync(file) }
