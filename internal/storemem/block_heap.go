//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package storemem

const outsideHeap = false

func allocate(size int) ([]byte, error) { return make([]byte, size), nil }

func release([]byte) error { return nil }
