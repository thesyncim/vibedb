package storeio

import (
	"errors"
	"fmt"
	"os"
)

var (
	// ErrStrictAllocationUnsupported reports that the host cannot prove a
	// complete, private physical allocation for a file prefix. Callers that use
	// the proof as a durability admission boundary must fail closed.
	ErrStrictAllocationUnsupported = errors.New(
		"vibedb: strict physical allocation proof is unsupported",
	)
	// ErrSealedCapacityMismatch reports that a caller's exact sealed-capacity
	// contract differs from the persisted file geometry.
	ErrSealedCapacityMismatch = errors.New(
		"vibedb: sealed capacity mismatch",
	)
	// strictAllocationDataSync is the durability fence between a successful
	// strict allocation proof and its authoritative post-sync EOF check. Tests
	// replace it to model filesystem geometry changing at that exact boundary.
	strictAllocationDataSync = dataSync
)

// StrictlyAllocateFile proves that the complete prefix [0,target) has private
// physical backing. It may repair holes and unshare copy-on-write extents, but
// it does not issue the durability fence: the caller must Sync after any bytes
// that make the proof authoritative have been written.
func StrictlyAllocateFile(file *os.File, target int64) error {
	if file == nil || target <= 0 {
		return fmt.Errorf("%w: invalid file or target", ErrStrictAllocationUnsupported)
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: allocation target is not a regular file", ErrStrictAllocationUnsupported)
	}
	return strictlyAllocateFile(file, target)
}
