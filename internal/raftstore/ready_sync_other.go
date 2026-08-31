//go:build !darwin && !linux

package raftstore

import "os"

// Unsupported platforms retain the conservative fallback so package tests and
// cross-compilation do not silently weaken the ordering contract.
func syncReadyRecord(file *os.File) error { return file.Sync() }
