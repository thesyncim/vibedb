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
	// JournalFaultSyncError returns EIO in place of the journal sync barrier at
	// SyncIndex, once; later syncs run normally. One-shot is deliberate: the
	// store's die-don't-retry posture means a poisoned lane must never issue a
	// second sync, so a test that fails the leader's sync once and asserts every
	// waiter got the poison would instead observe a spurious success if any
	// second leader illegally retried.
	JournalFaultSyncError
)

// JournalFaultPlan programs a FaultJournal to induce exactly one crash.
type JournalFaultPlan struct {
	Phase JournalFaultPhase
	// AppendIndex selects which append since Program is faulted, for the append
	// phases. Appends before it complete normally.
	AppendIndex int
	// SyncIndex selects which sync barrier since the wrapper was created is
	// faulted, for JournalFaultSyncError. Syncs before it complete normally.
	SyncIndex int
}

// FaultJournal wraps a RecoveryJournal's raw file writes and sync barriers and
// can stop an append, a recycle, or a sync at an exact point, leaving on disk
// exactly the bytes a crash or device failure there would have left. It records
// every append, recycle, and sync it observes. The write path is owned by the
// collection's single writer exactly as the RecoveryJournal it stands in for;
// the sync path additionally serves the group-commit leader, which syncs
// outside the writer, so the counters live under the wrapper's own mutex.
type FaultJournal struct {
	file *os.File

	mu          sync.Mutex
	plan        JournalFaultPlan
	appends     int
	recycles    int
	syncs       int
	faulted     bool
	tornWrites  int
	appendBytes int64
}

// NewFaultJournal wraps the file backing rj and installs its write and sync
// seams so every subsequent append, recycle, and sync barrier passes through
// the fault plan. Under JournalFaultNone (and every write-phase plan) the sync
// wrappers are pure pass-throughs, so pre-existing write-fault sweeps observe
// the platform barriers unchanged; JournalFaultSyncError is what lets a test
// fail the group-commit leader's fence, which no write fault can reach.
func NewFaultJournal(rj *RecoveryJournal) *FaultJournal {
	fj := &FaultJournal{file: rj.file}
	rj.writeAt = fj.writeAt
	realSync := rj.journalSync
	realDataSync := rj.journalDataSync
	rj.journalSync = func(file *os.File) error {
		return fj.sync(file, realSync)
	}
	rj.journalDataSync = func(file *os.File) error {
		return fj.sync(file, realDataSync)
	}
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

// Syncs reports how many sync barriers were observed since the wrapper was
// created, so a plan can target the next barrier with SyncIndex regardless of
// how many the open/replay path already issued.
func (fj *FaultJournal) Syncs() int {
	fj.mu.Lock()
	defer fj.mu.Unlock()
	return fj.syncs
}

// sync counts one barrier and either fails it per the plan or forwards it to
// the real platform barrier captured at wrap time.
func (fj *FaultJournal) sync(file *os.File, real func(*os.File) error) error {
	fj.mu.Lock()
	plan := fj.plan
	index := fj.syncs
	fj.syncs++
	fj.mu.Unlock()
	if plan.Phase == JournalFaultSyncError && index == plan.SyncIndex {
		_, err := fj.fault(0, syscall.EIO)
		return err
	}
	return real(file)
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
