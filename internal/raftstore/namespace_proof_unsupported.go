//go:build !linux && !darwin

package raftstore

import "os"

func provePinnedNamedFile(*os.File, string, string, *os.File, int64) error {
	return ErrPlatformUnsupported
}
