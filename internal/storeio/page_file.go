package storeio

import (
	"errors"
	"fmt"
	"os"
)

// ErrDirectIOUnsupported reports a platform or filesystem that cannot honor
// required direct page I/O.
var ErrDirectIOUnsupported = errors.New("vibejson: direct Store page I/O unsupported")

// DirectMode controls explicit direct-I/O admission. DirectTry falls back only
// when the platform or filesystem rejects direct I/O; unrelated open errors
// remain errors.
type DirectMode uint8

const (
	DirectOff DirectMode = iota
	DirectTry
	DirectRequire
)

// OpenPageCacheFile returns a descriptor suitable for PageCache reads. Off
// preserves the caller-owned descriptor. On Linux, Try and Require reopen the
// same inode through /proc/self/fd with O_DIRECT, producing an independent open
// file description so read policy cannot alter the writer's flags. The caller
// owns and must close a returned descriptor only when it differs from file.
func OpenPageCacheFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if file == nil {
		return nil, false, fmt.Errorf("%w: nil file", ErrPageCacheReference)
	}
	if mode > DirectRequire {
		return nil, false, fmt.Errorf("%w: direct mode %d", ErrPageCacheReference, mode)
	}
	return openPageCacheFile(file, mode)
}

// OpenPageCommitFile returns a descriptor suitable for Device writes. Off
// preserves the caller-owned descriptor. On Linux, Try and Require reopen the
// same inode through /proc/self/fd with O_DIRECT. The returned open file
// description is independent, so the direct policy cannot alter the caller's
// flags or offset. The caller owns and must close a returned descriptor only
// when it differs from file.
func OpenPageCommitFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if file == nil {
		return nil, false, fmt.Errorf("%w: nil file", ErrInvalidWrite)
	}
	if mode > DirectRequire {
		return nil, false, fmt.Errorf("%w: direct mode %d", ErrInvalidWrite, mode)
	}
	return openPageCommitFile(file, mode)
}
