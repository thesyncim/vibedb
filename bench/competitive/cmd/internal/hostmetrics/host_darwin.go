//go:build darwin

package hostmetrics

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

func PhysicalMemoryBytes() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	return int64(n), err
}

func MaxRSSBytes() (int64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return int64(usage.Maxrss), nil
}

func DropFileCaches() (string, error) {
	return "unsupported", fmt.Errorf("darwin exposes no unprivileged, independently verifiable whole-host file-cache drop control")
}
