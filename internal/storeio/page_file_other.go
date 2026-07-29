//go:build !linux

package storeio

import (
	"fmt"
	"os"
)

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
