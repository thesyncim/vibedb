//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package storeio

import "os"

func lockWriterPlatform(*os.File) error   { return ErrWriterLockUnsupported }
func unlockWriterPlatform(*os.File) error { return nil }
