//go:build windows

package main

import "os"

func rf3DiagnosticSignal() os.Signal {
	return nil
}
