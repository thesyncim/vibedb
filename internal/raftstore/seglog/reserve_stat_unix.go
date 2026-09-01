//go:build linux || darwin

package seglog

import (
	"os"
	"syscall"
)

func allocatedFileBytes(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Blocks < 0 {
		return 0, false
	}
	return uint64(stat.Blocks) * 512, true
}
