//go:build linux || darwin

package replicatedstate

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func sessionLifecycleAllocatedBytes(file *os.File) (uint64, bool, error) {
	if file == nil {
		return 0, true, fmt.Errorf("nil file")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, true, err
	}
	if stat.Blocks < 0 {
		return 0, true, fmt.Errorf("negative allocated block count")
	}
	return uint64(stat.Blocks) * 512, true, nil
}
