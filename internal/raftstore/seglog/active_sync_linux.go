//go:build linux

package seglog

import (
	"golang.org/x/sys/unix"
	"os"
)

func syncActiveData(file *os.File) error { return unix.Fdatasync(int(file.Fd())) }
