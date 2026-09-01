//go:build !linux && !darwin

package seglog

import "os"

func allocatedFileBytes(os.FileInfo) (uint64, bool) { return 0, false }
