//go:build !linux

package seglog

import "os"

// File.Sync is the strongest portable primitive; supported platforms may
// replace this with their qualified data-only equivalent.
func syncActiveData(file *os.File) error { return file.Sync() }
