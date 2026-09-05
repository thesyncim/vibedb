//go:build !linux && !darwin

package storeio

import "os"

func preallocatePortableFile(file *os.File, target int64) error {
	return growPortableFile(file, target)
}
