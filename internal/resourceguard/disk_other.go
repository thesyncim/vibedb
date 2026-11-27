//go:build !unix

package resourceguard

import "errors"

func freeDiskBytes(string) (uint64, error) {
	return 0, errors.New("resourceguard: free disk sampling unsupported on this platform")
}
