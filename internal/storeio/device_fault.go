package storeio

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

// ErrFaultInjected reports a deliberately induced commit crash. Durability
// crash tests program a FaultDevice to stop a commit at an exact point in its
// write sequence and return this sentinel; the resulting on-disk image is the
// bytes a real crash at that point would have left behind.
var ErrFaultInjected = errors.New("storeio: fault-injected commit crash point")

// FaultPhase names where inside a single Device.Commit a FaultDevice induces a
// crash. The commit protocol it models is the portable device's exact
// sequence: write each data page, one durability barrier, the alternate root,
// then the final barrier.
type FaultPhase uint8

const (
	// FaultNone records the write sequence without inducing any crash. It is the
	// clean probe pass used to enumerate the crash points of every commit.
	FaultNone FaultPhase = iota
	// FaultAfterDataWrite stops the commit immediately after the data page at
	// DataIndex is written, before the data barrier. No root is published, so
	// recovery must select the previous generation.
	FaultAfterDataWrite
	// FaultAfterBarrier stops the commit after the data barrier completes but
	// before the alternate root is written.
	FaultAfterBarrier
	// FaultAfterRootWrite stops the commit after the alternate root reaches the
	// file but before the final barrier. The outcome is unknown to the writer:
	// recovery decides whether the old or new generation won.
	FaultAfterRootWrite
	// FaultAfterFinalSync stops the commit after the final barrier. The new
	// generation is fully durable; recovery must select it.
	FaultAfterFinalSync
	// FaultTornRoot writes only a leading prefix of the alternate root and then
	// stops. The half-written root must fail its checksum and recovery must fall
	// back to the previous generation.
	FaultTornRoot
	// FaultDropDataThenApply skips the data page at DataIndex, applies every
	// later data page, then stops before the barrier. It models the bounded
	// reordering the contract permits before a barrier: an earlier write is lost
	// while later ones land. No root is published, so the previous generation
	// must survive.
	FaultDropDataThenApply
	// FaultENOSPCData returns ENOSPC in place of the data page write at
	// DataIndex.
	FaultENOSPCData
	// FaultENOSPCRoot returns ENOSPC in place of the alternate root write.
	FaultENOSPCRoot
	// FaultENOSPCGrowth returns ENOSPC on the first write of the commit that
	// extends the file past its length at the commit's start.
	FaultENOSPCGrowth
)

// FaultPlan programs a FaultDevice to induce exactly one crash.
type FaultPlan struct {
	// Commit is the zero-based Device.Commit ordinal to fault. Commits before it
	// complete normally; commits after it never run because the committer is
	// poisoned by the induced failure.
	Commit int
	// Phase selects where inside the target commit the crash lands. FaultNone
	// disables injection entirely.
	Phase FaultPhase
	// DataIndex selects the data page for FaultAfterDataWrite, FaultENOSPCData,
	// and FaultDropDataThenApply.
	DataIndex int
}

// FaultWrite records one physical write a commit issued.
type FaultWrite struct {
	Offset int64
	Length uint32
	// Grows reports that the write extends the file past its length at the
	// start of the commit that issued it.
	Grows bool
}

// FaultCommitRecord is the observed write plan of one Device.Commit.
type FaultCommitRecord struct {
	DataPages []FaultWrite
	Root      FaultWrite
}

// FaultDevice wraps a portable Device, records the write plan of every commit,
// and can stop a chosen commit at an exact point in its sequence, leaving on
// disk exactly the bytes a crash there would have left. It reproduces the
// portable device's data-page / barrier / root / final-barrier order rather
// than delegating to it, so it can intervene between the individual steps.
//
// It is not safe for concurrent Commit calls; Store's single background writer
// owns it, exactly as it owns the portable device it stands in for.
type FaultDevice struct {
	real *portableDevice
	file *os.File

	mu         sync.Mutex
	plan       FaultPlan
	commits    int
	records    []FaultCommitRecord
	faulted    bool
	growthDone bool
}

// OpenFaultDevice constructs a FaultDevice over file. It forces the portable
// backend because the wrapper reimplements that backend's commit sequence.
func OpenFaultDevice(file *os.File, options DeviceOptions) (*FaultDevice, error) {
	options.Backend = BackendPortable
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	real, err := openPortableDevice(file, normalized)
	if err != nil {
		return nil, err
	}
	return &FaultDevice{real: real, file: file}, nil
}

// Program sets the crash plan. It must be called before the commit it targets.
func (d *FaultDevice) Program(plan FaultPlan) {
	d.mu.Lock()
	d.plan = plan
	d.mu.Unlock()
}

// Commits reports how many Device.Commit calls have been observed.
func (d *FaultDevice) Commits() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.commits
}

// Faulted reports whether the programmed crash has fired.
func (d *FaultDevice) Faulted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.faulted
}

// Records returns a copy of the write plans observed so far. The per-record
// slices are shared and must be treated as read-only.
func (d *FaultDevice) Records() []FaultCommitRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]FaultCommitRecord(nil), d.records...)
}

func (d *FaultDevice) Backend() Backend { return BackendPortable }

func (d *FaultDevice) Buffer(index int) ([]byte, error) { return d.real.Buffer(index) }

// bindFrameArena forwards the cache-frame arena to the wrapped device so that
// frame-native writes resolve to the same bytes the portable device would
// write. It satisfies frameArenaDevice.
func (d *FaultDevice) bindFrameArena(arena []byte, frameSize int) error {
	return d.real.bindFrameArena(arena, frameSize)
}

// Prewrite forwards the opportunistic manual-checkpoint pre-write unchanged; it
// names no root, so it is not a crash boundary. It satisfies prewriteDevice.
func (d *FaultDevice) Prewrite(pages []Write) error { return d.real.Prewrite(pages) }

// Commit reproduces the portable device's commit sequence write by write,
// inducing the programmed crash between the appropriate steps.
func (d *FaultDevice) Commit(pages []Write, root Write) error {
	if d.real.closed {
		return ErrClosed
	}
	if err := validateCommit(d.real.buffers, d.real.bufferSize, d.real.seen, pages, root); err != nil {
		return err
	}
	startSize, err := d.fileSize()
	if err != nil {
		return err
	}

	rec := FaultCommitRecord{Root: FaultWrite{
		Offset: root.Offset, Length: root.Length,
		Grows: root.Offset+int64(root.Length) > startSize,
	}}
	for i := range pages {
		rec.DataPages = append(rec.DataPages, FaultWrite{
			Offset: pages[i].Offset, Length: pages[i].Length,
			Grows: pages[i].Offset+int64(pages[i].Length) > startSize,
		})
	}
	d.mu.Lock()
	ordinal := d.commits
	d.commits++
	d.records = append(d.records, rec)
	plan := d.plan
	d.mu.Unlock()

	active := plan.Phase != FaultNone && ordinal == plan.Commit
	growthArmed := active && plan.Phase == FaultENOSPCGrowth

	// Data phase: each page as its own positional write so a crash can land
	// between any two of them.
	for i := range pages {
		if active && plan.Phase == FaultDropDataThenApply && i == plan.DataIndex {
			continue
		}
		if active && plan.Phase == FaultENOSPCData && i == plan.DataIndex {
			return d.fault(syscall.ENOSPC, false)
		}
		if growthArmed && rec.DataPages[i].Grows {
			return d.fault(syscall.ENOSPC, false)
		}
		if werr := writeArenaAt(
			d.real.file, d.real.arena, d.real.bufferSize,
			d.real.frameArena, d.real.frameSize, pages[i],
		); werr != nil {
			return werr
		}
		if active && plan.Phase == FaultAfterDataWrite && i == plan.DataIndex {
			return d.fault(ErrFaultInjected, false)
		}
	}
	if active && plan.Phase == FaultDropDataThenApply {
		// An earlier write was dropped while later ones landed; stop before the
		// barrier so the previous root remains the only published one.
		return d.fault(ErrFaultInjected, false)
	}

	if len(pages) != 0 {
		barrier := d.real.checkpointBarrier
		if barrier == nil {
			barrier = dataBarrier
		}
		if berr := barrier(d.real.file); berr != nil {
			return berr
		}
	}
	if active && plan.Phase == FaultAfterBarrier {
		return d.fault(ErrFaultInjected, false)
	}

	// Root phase. Everything from here on has an unknown outcome to the writer.
	if active && plan.Phase == FaultENOSPCRoot {
		return d.fault(syscall.ENOSPC, true)
	}
	if growthArmed && rec.Root.Grows {
		return d.fault(syscall.ENOSPC, true)
	}
	if active && plan.Phase == FaultTornRoot {
		data, derr := writeBytes(
			d.real.arena, d.real.bufferSize,
			d.real.frameArena, d.real.frameSize, root,
		)
		if derr != nil {
			return derr
		}
		prefix := len(data) / 2
		if prefix < 1 {
			prefix = 1
		}
		if n, werr := d.file.WriteAt(data[:prefix], root.Offset); werr != nil {
			return d.fault(werr, true)
		} else if n != prefix {
			return d.fault(io.ErrShortWrite, true)
		}
		return d.fault(ErrFaultInjected, true)
	}
	if werr := writeArenaAt(
		d.real.file, d.real.arena, d.real.bufferSize,
		d.real.frameArena, d.real.frameSize, root,
	); werr != nil {
		return d.fault(werr, true)
	}
	if active && plan.Phase == FaultAfterRootWrite {
		return d.fault(ErrFaultInjected, true)
	}

	finalSync := d.real.checkpointFinalSync
	if finalSync == nil {
		finalSync = dataSync
	}
	if serr := finalSync(d.real.file); serr != nil {
		return d.fault(serr, true)
	}
	if active && plan.Phase == FaultAfterFinalSync {
		return d.fault(ErrFaultInjected, true)
	}
	return nil
}

func (d *FaultDevice) Close() error { return d.real.Close() }

func (d *FaultDevice) fault(err error, unknown bool) error {
	d.mu.Lock()
	d.faulted = true
	d.mu.Unlock()
	if unknown {
		return commitOutcomeUnknown(err)
	}
	return err
}

func (d *FaultDevice) fileSize() (int64, error) {
	info, err := d.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
