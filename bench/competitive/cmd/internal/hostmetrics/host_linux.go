//go:build linux

package hostmetrics

import (
	"fmt"
	"os"
	"syscall"
)

func PhysicalMemoryBytes() (int64, error) {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0, err
	}
	return int64(info.Totalram) * int64(info.Unit), nil
}

func MaxRSSBytes() (int64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return int64(usage.Maxrss) * 1024, nil
}

// DropFileCaches uses Linux's global, synchronous cache-drop control. It is
// intentionally strict: per-file advisory eviction cannot prove that mapped,
// metadata, journal, and directory pages were evicted for every adapter.
func DropFileCaches() (string, error) {
	syscall.Sync()
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return "unsupported", fmt.Errorf("linux global cache drop requires write access to /proc/sys/vm/drop_caches: %w", err)
	}
	if _, err := f.WriteString("3\n"); err != nil {
		_ = f.Close()
		return "unsupported", fmt.Errorf("linux global cache drop write: %w", err)
	}
	if err := f.Close(); err != nil {
		return "unsupported", fmt.Errorf("linux global cache drop close: %w", err)
	}
	return "linux-procfs-global-sync-3", nil
}
