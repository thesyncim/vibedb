//go:build unix

package resourceguard

import "syscall"

func freeDiskBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	if st.Bsize <= 0 {
		return 0, nil
	}
	return st.Bavail * uint64(st.Bsize), nil
}
