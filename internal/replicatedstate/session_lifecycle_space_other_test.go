//go:build !linux && !darwin

package replicatedstate

import "os"

func sessionLifecycleAllocatedBytes(*os.File) (uint64, bool, error) {
	return 0, false, nil
}
