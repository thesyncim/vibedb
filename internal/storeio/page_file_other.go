//go:build !linux

package storeio

import (
	"fmt"
	"os"
)

func openPageFile(path string, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectRequire {
		return nil, false, fmt.Errorf("%w: direct page reads require Linux", ErrDirectIOUnsupported)
	}
	file, err := os.Open(path)
	return file, false, err
}

func openPageCacheFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectRequire {
		return nil, false, fmt.Errorf("%w: direct page reads require Linux", ErrDirectIOUnsupported)
	}
	return file, false, nil
}

func openPageCommitFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if mode == DirectRequire {
		return nil, false, fmt.Errorf("%w: direct page writes require Linux", ErrDirectIOUnsupported)
	}
	return file, false, nil
}
