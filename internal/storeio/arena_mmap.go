//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storeio

import "syscall"

func allocateArena(size int) ([]byte, error) {
	return syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON)
}

func releaseArena(arena []byte) error { return syscall.Munmap(arena) }
