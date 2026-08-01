//go:build darwin

package durable

import (
	"errors"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// fpunchhole mirrors Darwin's fpunchhole_t from <sys/fcntl.h>. The two 32-bit
// fields keep the following off_t values naturally aligned on both supported
// Darwin architectures.
type fileStorePunchHole struct {
	flags    uint32
	reserved uint32
	offset   int64
	length   int64
}

func punchFileStoreHole(
	file *os.File,
	offset, length uint64,
) (bool, error) {
	if file == nil {
		return false, nil
	}
	request := fileStorePunchHole{
		offset: int64(offset), length: int64(length),
	}
	for attempt := 0; attempt < fileStoreHolePunchMaxAttempts; attempt++ {
		_, _, errno := unix.Syscall(
			unix.SYS_FCNTL, file.Fd(), uintptr(unix.F_PUNCHHOLE),
			uintptr(unsafe.Pointer(&request)),
		)
		runtime.KeepAlive(&request)
		if errno == 0 {
			return true, nil
		}
		if errors.Is(errno, unix.EINTR) &&
			attempt+1 < fileStoreHolePunchMaxAttempts {
			continue
		}
		if errors.Is(errno, unix.EOPNOTSUPP) ||
			errors.Is(errno, unix.ENOTSUP) ||
			errors.Is(errno, unix.ENOSYS) ||
			errors.Is(errno, unix.EINVAL) {
			return false, nil
		}
		return true, errno
	}
	return true, unix.EINTR
}
