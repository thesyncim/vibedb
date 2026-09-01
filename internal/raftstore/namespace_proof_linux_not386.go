//go:build linux && !386

package raftstore

import "golang.org/x/sys/unix"

const rawFstatatTrap = unix.SYS_NEWFSTATAT
