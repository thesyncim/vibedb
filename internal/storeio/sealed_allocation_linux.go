//go:build linux

package storeio

import "os"

func preallocatePortableFile(file *os.File, target int64) error {
	return growPortableFile(file, target)
}
