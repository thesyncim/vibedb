package driver

import (
	"context"
	"os"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
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
	// SchemaEmptySuffixPreparedApplied and SchemaEmptySuffixPreCommandApplied
	// are a runtime proof supplied only after the RF3 owner has verified every
	// intervening WAL entry as an empty EntryNormal. Persisted activation bytes
	// must independently bind the same cut before source recovery can use it.
	SchemaEmptySuffixPreparedApplied   uint64
	SchemaEmptySuffixPreCommandApplied uint64
	// SchemaCommittedTransition is copied from the exact committed WAL entry
	// by the RF3 owner. Source recovery accepts it only when its semantic
	// transition equals the replica-local activation except for CatalogCAS.
	SchemaCommittedTransition string
}

func replicatedOpeningOptions(options []ReplicatedOpenOptions) (ReplicatedOpenOptions, error) {
	if len(options) > 1 {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	var result ReplicatedOpenOptions
	if len(options) == 1 {
		result = options[0]
	}
	if len(result.SchemaCommittedTransition) > replicatedstate.MaxSchemaTransitionBytes {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	if (result.WriterLockContext == nil) != result.WriterLockDeadline.IsZero() {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	if (result.SchemaEmptySuffixPreparedApplied == 0) !=
		(result.SchemaEmptySuffixPreCommandApplied == 0) ||
		result.SchemaEmptySuffixPreCommandApplied != 0 &&
			result.SchemaEmptySuffixPreCommandApplied <= result.SchemaEmptySuffixPreparedApplied {
		return ReplicatedOpenOptions{}, ErrReplicatedApplyMismatch
	}
	return result, nil
}

func (options ReplicatedOpenOptions) lockWriter(file *os.File) error {
	return storeio.LockWriterUntil(file, options.WriterLockContext, options.WriterLockDeadline)
}
