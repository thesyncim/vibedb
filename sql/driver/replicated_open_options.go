package driver

import (
	"context"
	"os"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// ReplicatedOpenOptions is runtime-only startup policy, never part of a SQL,
// Raft, or disk identity. Zero keeps immediate writer-lock admission. A caller
// opting into waiting must supply both fields and share the same absolute
// deadline across all startup groups, including schema/generation recovery.
// Existing namespace recovery leases stay held: namespace publication can be
// delayed by at most the same contention deadline, not a fresh per-file wait.
type ReplicatedOpenOptions struct {
	WriterLockContext  context.Context
	WriterLockDeadline time.Time
}

func replicatedOpeningOptions(options []ReplicatedOpenOptions) (ReplicatedOpenOptions, error) {
	if len(options) > 1 {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	var result ReplicatedOpenOptions
	if len(options) == 1 {
		result = options[0]
	}
	if (result.WriterLockContext == nil) != result.WriterLockDeadline.IsZero() {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	return result, nil
}

func (options ReplicatedOpenOptions) lockWriter(file *os.File) error {
	return storeio.LockWriterUntil(file, options.WriterLockContext, options.WriterLockDeadline)
}
