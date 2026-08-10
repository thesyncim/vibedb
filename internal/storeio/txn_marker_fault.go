package storeio

import (
	"io"
	"os"
	"sync"
	"syscall"
)

// TxnMarkerFaultPhase names where a FaultTxnMarker or the create-fence seam
// induces a crash inside the decision log's mint, append, recycle, or sync
// sequence. It mirrors FaultJournal / FaultDevice for the txn.vtm sidecar.
type TxnMarkerFaultPhase uint8

const (
	// TxnMarkerFaultNone records the write sequence without inducing any crash.
	TxnMarkerFaultNone TxnMarkerFaultPhase = iota
	// TxnMarkerFaultTornAppend writes only a leading prefix of the record at
	// AppendIndex and then stops. Recovery must truncate the torn tail.
	TxnMarkerFaultTornAppend
	// TxnMarkerFaultAppendError returns EIO in place of the record write at
	// AppendIndex without persisting the record body.
	TxnMarkerFaultAppendError
	// TxnMarkerFaultENOSPCAppend returns ENOSPC in place of the record write at
	// AppendIndex.
	TxnMarkerFaultENOSPCAppend
	// TxnMarkerFaultTornRecycle writes only a leading prefix of the recycle
	// header and stops so Open falls back to the previous header.
	TxnMarkerFaultTornRecycle
	// TxnMarkerFaultENOSPCRecycle returns ENOSPC in place of the recycle header
	// write.
	TxnMarkerFaultENOSPCRecycle
	// TxnMarkerFaultSyncError returns EIO in place of the marker sync barrier
	// at SyncIndex, once.
	TxnMarkerFaultSyncError
	// TxnMarkerFaultCreateHeaderWrite stops during the mint's header-sector
	// writes, leaving no usable sealed header pair.
	TxnMarkerFaultCreateHeaderWrite
	// TxnMarkerFaultCreateFileSync returns EIO in place of the mint's file sync.
	TxnMarkerFaultCreateFileSync
	// TxnMarkerFaultCreateParentDirSync returns EIO in place of the mint's
	// parent-directory fsync (L2 / W7).
	TxnMarkerFaultCreateParentDirSync
)

// TxnMarkerFaultPlan programs exactly one induced failure.
type TxnMarkerFaultPlan struct {
	Phase TxnMarkerFaultPhase
	// AppendIndex selects which append since Program is faulted for the append
	// phases. Appends before it complete normally.
	AppendIndex int
	// SyncIndex selects which sync barrier since the wrapper was created is
	// faulted for TxnMarkerFaultSyncError.
	SyncIndex int
}

var (
	txnMarkerCreateFaultMu   sync.Mutex
	txnMarkerCreateFaultPlan TxnMarkerFaultPlan
	txnMarkerCreateFaulted   bool
)

// ProgramTxnMarkerCreateFault sets the mint-fence crash plan consulted by
// CreateTxnMarker. It must be called before the CreateTxnMarker it targets.
// Pass TxnMarkerFaultNone to clear.
func ProgramTxnMarkerCreateFault(plan TxnMarkerFaultPlan) {
	txnMarkerCreateFaultMu.Lock()
	txnMarkerCreateFaultPlan = plan
	txnMarkerCreateFaulted = false
	txnMarkerCreateFaultMu.Unlock()
}

// TxnMarkerCreateFaulted reports whether the programmed mint-fence crash fired.
func TxnMarkerCreateFaulted() bool {
	txnMarkerCreateFaultMu.Lock()
	defer txnMarkerCreateFaultMu.Unlock()
	return txnMarkerCreateFaulted
}

func txnMarkerCreateHeaderFault() error {
	txnMarkerCreateFaultMu.Lock()
	plan := txnMarkerCreateFaultPlan
	if plan.Phase != TxnMarkerFaultCreateHeaderWrite || txnMarkerCreateFaulted {
		txnMarkerCreateFaultMu.Unlock()
		return nil
	}
	txnMarkerCreateFaulted = true
	txnMarkerCreateFaultMu.Unlock()
	return ErrFaultInjected
}

func runTxnMarkerCreateFileSync(file *os.File) error {
	txnMarkerCreateFaultMu.Lock()
	plan := txnMarkerCreateFaultPlan
	if plan.Phase == TxnMarkerFaultCreateFileSync && !txnMarkerCreateFaulted {
		txnMarkerCreateFaulted = true
		txnMarkerCreateFaultMu.Unlock()
		return syscall.EIO
	}
	txnMarkerCreateFaultMu.Unlock()
	return txnMarkerCreateFileSync(file)
}

func runTxnMarkerCreateParentDirSync(root *os.Root) error {
	txnMarkerCreateFaultMu.Lock()
	plan := txnMarkerCreateFaultPlan
	if plan.Phase == TxnMarkerFaultCreateParentDirSync && !txnMarkerCreateFaulted {
		txnMarkerCreateFaulted = true
		txnMarkerCreateFaultMu.Unlock()
		return syscall.EIO
	}
	txnMarkerCreateFaultMu.Unlock()
	return txnMarkerParentDirSync(root)
}

// FaultTxnMarker wraps a TxnMarker's raw file writes and sync barriers and can
// stop an append, a recycle, or a sync at an exact point, leaving on disk
// exactly the bytes a crash or device failure there would have left. Production
// paths have zero overhead when the seam is never installed.
type FaultTxnMarker struct {
	file *os.File

	mu       sync.Mutex
	plan     TxnMarkerFaultPlan
	appends  int
	recycles int
	syncs    int
	faulted  bool
}

// NewFaultTxnMarker wraps the file backing m and installs its write and sync
// seams so every subsequent append, recycle, and sync barrier passes through
// the fault plan.
func NewFaultTxnMarker(m *TxnMarker) *FaultTxnMarker {
	fm := &FaultTxnMarker{file: m.file}
	m.writeAt = fm.writeAt
	realSync := m.markerSync
	m.markerSync = func(file *os.File) error {
		return fm.sync(file, realSync)
	}
	return fm
}

// Program sets the crash plan. It must be called before the append or recycle
// it targets. Observed counters are left intact so a plan can target an append
// that follows earlier clean ones.
func (fm *FaultTxnMarker) Program(plan TxnMarkerFaultPlan) {
	fm.mu.Lock()
	fm.plan = plan
	fm.faulted = false
	fm.mu.Unlock()
}

// Faulted reports whether the programmed crash fired.
func (fm *FaultTxnMarker) Faulted() bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.faulted
}

// Appends reports how many record appends were observed.
func (fm *FaultTxnMarker) Appends() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.appends
}

// Syncs reports how many sync barriers were observed since the wrapper was
// created.
func (fm *FaultTxnMarker) Syncs() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.syncs
}

func (fm *FaultTxnMarker) sync(
	file *os.File, real func(*os.File) error,
) error {
	fm.mu.Lock()
	plan := fm.plan
	index := fm.syncs
	fm.syncs++
	fm.mu.Unlock()
	if plan.Phase == TxnMarkerFaultSyncError && index == plan.SyncIndex {
		_, err := fm.fault(0, syscall.EIO)
		return err
	}
	return real(file)
}

func (fm *FaultTxnMarker) writeAt(p []byte, off int64) (int, error) {
	fm.mu.Lock()
	plan := fm.plan
	header := off < txnMarkerRegionStart
	var index int
	if header {
		index = fm.recycles
		fm.recycles++
	} else {
		index = fm.appends
		fm.appends++
	}
	fm.mu.Unlock()

	if header {
		switch plan.Phase {
		case TxnMarkerFaultENOSPCRecycle:
			return fm.fault(0, syscall.ENOSPC)
		case TxnMarkerFaultTornRecycle:
			prefix := max(len(p)/2, 1)
			n, err := fm.file.WriteAt(p[:prefix], off)
			if err != nil {
				return fm.fault(n, err)
			}
			return fm.fault(prefix, io.ErrShortWrite)
		}
		return fm.file.WriteAt(p, off)
	}

	if index == plan.AppendIndex {
		switch plan.Phase {
		case TxnMarkerFaultAppendError:
			return fm.fault(0, syscall.EIO)
		case TxnMarkerFaultENOSPCAppend:
			return fm.fault(0, syscall.ENOSPC)
		case TxnMarkerFaultTornAppend:
			prefix := min(TxnMarkerRecordPrefixSize, len(p))
			n, err := fm.file.WriteAt(p[:prefix], off)
			if err != nil {
				return fm.fault(n, err)
			}
			return fm.fault(prefix, nil)
		}
	}
	return fm.file.WriteAt(p, off)
}

func (fm *FaultTxnMarker) fault(n int, err error) (int, error) {
	fm.mu.Lock()
	fm.faulted = true
	fm.mu.Unlock()
	return n, err
}
