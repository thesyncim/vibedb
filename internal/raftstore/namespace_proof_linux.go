//go:build linux

package raftstore

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// provePinnedNamedFile is the steady-state namespace fence. Paths are cached
// NUL-terminated strings owned by Store, so Linux path syscalls do not allocate
// C-string copies on every Ready.
func provePinnedNamedFile(
	pinnedDirectory *os.File,
	parentPathNUL, baseNUL string,
	file *os.File,
	expectedSize int64,
) error {
	if pinnedDirectory == nil || file == nil || !validNULTerminatedPath(parentPathNUL) ||
		!validNULTerminatedPath(baseNUL) {
		return ErrNamespaceChanged
	}
	var pinnedDirectoryStat, pinnedEntry, fileStat unix.Stat_t
	if err := unix.Fstat(int(pinnedDirectory.Fd()), &pinnedDirectoryStat); err != nil {
		return fmt.Errorf("%w: stat pinned parent: %v", ErrNamespaceChanged, err)
	}
	if err := rawFstatat(
		int(pinnedDirectory.Fd()), baseNUL, &pinnedEntry, unix.AT_SYMLINK_NOFOLLOW,
	); err != nil || pinnedEntry.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("%w: stat pinned WAL leaf: %v", ErrNamespaceChanged, err)
	}
	if err := unix.Fstat(int(file.Fd()), &fileStat); err != nil ||
		!sameUnixFile(&fileStat, &pinnedEntry) || fileStat.Size != expectedSize {
		return fmt.Errorf("%w: WAL descriptor no longer names leaf/capacity", ErrNamespaceChanged)
	}
	liveFD, err := rawOpenDirectory(parentPathNUL)
	if err != nil {
		return fmt.Errorf("%w: reopen live parent: %v", ErrNamespaceChanged, err)
	}
	defer unix.Close(liveFD)
	var liveDirectory, liveEntry unix.Stat_t
	if err := unix.Fstat(liveFD, &liveDirectory); err != nil ||
		!sameUnixFile(&pinnedDirectoryStat, &liveDirectory) {
		return fmt.Errorf("%w: live parent path was rebound", ErrNamespaceChanged)
	}
	if err := rawFstatat(liveFD, baseNUL, &liveEntry, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		liveEntry.Mode&unix.S_IFMT != unix.S_IFREG || !sameUnixFile(&fileStat, &liveEntry) {
		return fmt.Errorf("%w: live WAL leaf was replaced", ErrNamespaceChanged)
	}
	return nil
}

func rawFstatat(directory int, pathNUL string, stat *unix.Stat_t, flags int) error {
	_, _, errno := unix.Syscall6(
		rawFstatatTrap,
		uintptr(directory),
		uintptr(unsafe.Pointer(unsafe.StringData(pathNUL))),
		uintptr(unsafe.Pointer(stat)),
		uintptr(flags), 0, 0,
	)
	runtime.KeepAlive(pathNUL)
	if errno != 0 {
		return errno
	}
	return nil
}

func rawOpenDirectory(pathNUL string) (int, error) {
	directory := unix.AT_FDCWD
	fd, _, errno := unix.Syscall6(
		unix.SYS_OPENAT,
		uintptr(directory),
		uintptr(unsafe.Pointer(unsafe.StringData(pathNUL))),
		uintptr(unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC),
		0, 0, 0,
	)
	runtime.KeepAlive(pathNUL)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

func validNULTerminatedPath(path string) bool {
	return len(path) > 1 && path[len(path)-1] == 0
}

func sameUnixFile(left, right *unix.Stat_t) bool {
	return left != nil && right != nil &&
		uint64(left.Dev) == uint64(right.Dev) && uint64(left.Ino) == uint64(right.Ino)
}
