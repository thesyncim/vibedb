//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package storeio

func allocateArena(size int) ([]byte, error) { return make([]byte, size), nil }

func releaseArena([]byte) error { return nil }
