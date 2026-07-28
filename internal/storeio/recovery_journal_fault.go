package storeio

import (
	"io"
	"os"
	"sync"
	"syscall"
)

// recoveryJournalPreallocate is a package seam so crash tests can induce ENOSPC
// on the journal's up-front preallocation, exactly where a real out-of-space
// device would fail before the journal is usable. Production keeps the platform
// preallocator.
var recoveryJournalPreallocate = preallocateRecoveryJournal

// JournalFaultPhase names where a FaultJournal induces a crash inside the
// journal's append/recycle sequence. It mirrors FaultDevice for the separate
// journal file: the crash-relevant boundaries here are the record append and
// the recycle header rewrite, each with its own sync.
type JournalFaultPhase uint8

const (
	// JournalFaultNone records the write sequence without inducing any crash.
	JournalFaultNone JournalFaultPhase = iota
	// JournalFaultTornAppend writes only a leading sector of the record at
	// AppendIndex and then stops. The half-written record must fail its CRC and
	// recovery must truncate the tail at the previous record.
	JournalFaultTornAppend
	// JournalFaultDropAppend skips the record write at AppendIndex entirely and
	// stops. It models a reordering that lost an earlier append while a later
	// one would have landed: the resulting sequence hole must stop replay at the
	// last contiguous record.
	JournalFaultDropAppend
	// JournalFaultENOSPCAppend returns ENOSPC in place of the record write at
	// AppendIndex. The append fails without extending the file; the caller must
	// force a checkpoint.
	JournalFaultENOSPCAppend
	// JournalFaultTornRecycle writes only a leading prefix of the recycle header
	// and stops. The half-written header must fail its checksum, so recovery
	// falls back to the previous durable header and re-applies the records it
	// still describes.
	JournalFaultTornRecycle
	// JournalFaultENOSPCRecycle returns ENOSPC in place of the recycle header
	// write.
	JournalFaultENOSPCRecycle
)

// JournalFaultPlan programs a FaultJournal to induce exactly one crash.
type JournalFaultPlan struct {
	Phase JournalFaultPhase
	// AppendIndex selects which append since Program is faulted, for the append
	// phases. Appends before it complete normally.
	AppendIndex int
}

// FaultJournal wraps a RecoveryJournal's raw file writes and can stop an append
// or a recycle at an exact point, leaving on disk exactly the bytes a crash
// there would have left. It records every append and recycle it observes. It is
// not safe for concurrent use; the collection's single writer owns it, exactly
// as it owns the RecoveryJournal it stands in for.
type FaultJournal struct {
	file *os.File

	mu          sync.Mutex
	plan        JournalFaultPlan
	appends     int
	recycles    int
	faulted     bool
	tornWrites  int
	appendBytes int64
}

// NewFaultJournal wraps the file backing rj and installs its write seam so
// every subsequent append and recycle passes through the fault plan. Sync
// barriers are left intact: a fault models lost or torn bytes, and the caller's
// own sync failure handling covers a failed barrier.
func NewFaultJournal(rj *RecoveryJournal) *FaultJournal {
	fj := &FaultJournal{file: rj.file}
	rj.writeAt = fj.writeAt
	return fj
}

// Program sets the crash plan. It must be called before the append or recycle
// it targets. AppendIndex is the cumulative append ordinal since the wrapper was
// created; Program leaves the observed counters intact, exactly as FaultDevice
// leaves its commit ordinal, so a plan can target an append that follows earlier
// clean ones.
func (fj *FaultJournal) Program(plan JournalFaultPlan) {
	fj.mu.Lock()
	fj.plan = plan
	fj.faulted = false
	fj.mu.Unlock()
}

// Faulted reports whether the programmed crash fired.
func (fj *FaultJournal) Faulted() bool {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return fj.faulted
}

// Appends reports how many record appends were observed since Program.
func (fj *FaultJournal) Appends() int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return fj.appends
}

func (fj *FaultJournal) writeAt(p []byte, off int64) (int, error) {
	fj.mu.Lock()
	plan := fj.plan
	header := off < recoveryJournalRegionStart
	var index int
	if header {
		index = fj.recycles
		fj.recycles++
	} else {
		index = fj.appends
		fj.appends++
	}
	fj.mu.Unlock()

	if header {
		switch plan.Phase {
		case JournalFaultENOSPCRecycle:
			return fj.fault(0, syscall.ENOSPC)
		case JournalFaultTornRecycle:
			prefix := max(len(p)/2, 1)
			n, err := fj.file.WriteAt(p[:prefix], off)
			if err != nil {
				return fj.fault(n, err)
			}
			return fj.fault(prefix, io.ErrShortWrite)
		}
		return fj.file.WriteAt(p, off)
	}

	if index == plan.AppendIndex {
		switch plan.Phase {
		case JournalFaultDropAppend:
			return fj.fault(len(p), nil)
		case JournalFaultENOSPCAppend:
			return fj.fault(0, syscall.ENOSPC)
		case JournalFaultTornAppend:
			// Write only the fixed framing prefix and stop. The key, value, and
			// CRC trailer that follow are never written, so the record's
			// CRC-covered body is guaranteed incomplete regardless of its size and
			// the record fails validation on the next open.
			prefix := min(RecoveryJournalRecordPrefixSize, len(p))
			n, err := fj.file.WriteAt(p[:prefix], off)
			if err != nil {
				return fj.fault(n, err)
			}
			return fj.fault(prefix, nil)
		}
	}
	return fj.file.WriteAt(p, off)
}

func (fj *FaultJournal) fault(n int, err error) (int, error) {
	fj.mu.Lock()
	fj.faulted = true
	fj.mu.Unlock()
	return n, err
}

// Recycles reports how many header rewrites were observed since the wrapper
// was created; replay-safety tests assert a recovering Open performs exactly
// one, after the post-replay fold.
func (fj *FaultJournal) Recycles() int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return fj.recycles
}
