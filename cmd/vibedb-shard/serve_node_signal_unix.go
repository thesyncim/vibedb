//go:build !windows

package main

import (
	"os"
	"syscall"
)

func rf3DiagnosticSignal() os.Signal {
	return syscall.SIGUSR1
}
