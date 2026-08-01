//go:build !linux && !darwin

package durable

import "os"

func punchFileStoreHole(
	_ *os.File,
	_, _ uint64,
) (bool, error) {
	return false, nil
}
