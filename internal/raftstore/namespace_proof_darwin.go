//go:build darwin

package raftstore

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func provePinnedNamedFile(
	pinnedDirectory *os.File,
	parentPathNUL, baseNUL string,
	file *os.File,
	expectedSize int64,
) error {
	if pinnedDirectory == nil || file == nil || len(parentPathNUL) < 2 || len(baseNUL) < 2 {
		return ErrNamespaceChanged
	}
	parentPath := parentPathNUL[:len(parentPathNUL)-1]
	base := baseNUL[:len(baseNUL)-1]
	var pinnedDirectoryStat, pinnedEntry, fileStat unix.Stat_t
	if err := unix.Fstat(int(pinnedDirectory.Fd()), &pinnedDirectoryStat); err != nil {
		return fmt.Errorf("%w: stat pinned parent: %v", ErrNamespaceChanged, err)
	}
	if err := unix.Fstatat(int(pinnedDirectory.Fd()), base, &pinnedEntry, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		pinnedEntry.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("%w: stat pinned WAL leaf: %v", ErrNamespaceChanged, err)
	}
	if err := unix.Fstat(int(file.Fd()), &fileStat); err != nil ||
		!sameUnixFile(&fileStat, &pinnedEntry) || fileStat.Size != expectedSize {
		return fmt.Errorf("%w: WAL descriptor no longer names leaf/capacity", ErrNamespaceChanged)
	}
	liveFD, err := unix.Open(parentPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("%w: reopen live parent: %v", ErrNamespaceChanged, err)
	}
	defer unix.Close(liveFD)
	var liveDirectory, liveEntry unix.Stat_t
	if err := unix.Fstat(liveFD, &liveDirectory); err != nil ||
		!sameUnixFile(&pinnedDirectoryStat, &liveDirectory) {
		return fmt.Errorf("%w: live parent path was rebound", ErrNamespaceChanged)
	}
	if err := unix.Fstatat(liveFD, base, &liveEntry, unix.AT_SYMLINK_NOFOLLOW); err != nil ||
		liveEntry.Mode&unix.S_IFMT != unix.S_IFREG || !sameUnixFile(&fileStat, &liveEntry) {
		return fmt.Errorf("%w: live WAL leaf was replaced", ErrNamespaceChanged)
	}
	return nil
}

func sameUnixFile(left, right *unix.Stat_t) bool {
	return left != nil && right != nil &&
		uint64(left.Dev) == uint64(right.Dev) && uint64(left.Ino) == uint64(right.Ino)
}
