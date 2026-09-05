package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const devClusterLockName = ".supervisor.lock"

// Keep the same lock inode across restarts. Removing it would allow a new
// supervisor to acquire a different inode while an old owner still holds it.
// The caller retains ownership through DDL shutdown and every child join.
func lockDevCluster(path string) (func(), error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return nil, errDevCluster
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errDevCluster, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if info, err := root.Lstat(devClusterLockName); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, errors.Join(errDevCluster, err)
	}
	file, err := root.OpenFile(devClusterLockName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, errors.Join(fmt.Errorf("%w: another supervisor owns this cluster", errDevCluster), err, file.Close())
	}
	return func() { _ = storeio.UnlockWriter(file); _ = file.Close() }, nil
}
