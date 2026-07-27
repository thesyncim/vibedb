//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storemem

import "syscall"

const outsideHeap = true

func allocate(size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	return syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON)
}

func release(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
