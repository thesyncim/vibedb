//go:build unix

package clusterbackup

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

func repositoryAllocatedBytes(path string) (uint64, bool, error) {
	var allocated uint64
	err := filepath.WalkDir(path, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := os.Lstat(name)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Blocks < 0 {
			return ErrRepository
		}
		allocated += uint64(stat.Blocks) * 512
		return nil
	})
	return allocated, true, err
}
