//go:build linux && 386

package raftstore

import "golang.org/x/sys/unix"

// Linux/386 exposes the same fstatat operation under its legacy fstatat64
// syscall name. Keep the raw trap architecture-local so the zero-allocation
// namespace proof remains portable.
const rawFstatatTrap = unix.SYS_FSTATAT64
