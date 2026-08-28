package storeio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"weak"
)

var (
	// ErrWriterLocked reports that another mutable page-file owner already
	// holds the process/filesystem advisory writer lease.
	ErrWriterLocked = errors.New("vibedb: Store page file already has a writer")
	// ErrWriterLockUnsupported rejects mutable open on a platform where this
	// package cannot enforce the single-writer invariant.
	ErrWriterLockUnsupported = errors.New("vibedb: Store page writer locking is unsupported")
)

var writerLockRegistry struct {
	sync.Mutex
	entries []writerLockIdentity
}

type writerLockIdentity struct {
	owner weak.Pointer[os.File]
	info  os.FileInfo
}

// LockWriter acquires the process and operating-system exclusive writer lease
// for file. The in-process file-identity registry closes a hole in advisory
// locks: reacquiring through the same or a duplicated descriptor may otherwise
// succeed and let two durable Store values mutate one generation stream.
//
// Identity, not descriptor number, is what makes an entry a conflict. Both the
// duplicate-descriptor and the reopened-file cases are the same file, so
// os.SameFile decides them, and it is the only test that can: a descriptor
// number is unique only while its descriptor is open, and the kernel hands the
// lowest free number to the next open. An owner that never unlocked — a Store
// dropped without Close, or a test that failed before its cleanup — leaves an
// entry behind whose descriptor the process then reuses for an unrelated file,
// and matching on the number alone refused that file forever after. In this
// repository's own suite one test failing before Close made every later durable
// test in the package fail with "already has a writer: descriptor 4", turning a
// single timing-sensitive failure into forty unattributable ones.
//
// A stale entry is therefore dropped rather than honoured. The registry keeps a
// weak identity for the exact owning os.File and verifies that a reachable owner
// still names the identity captured at lock time. The weak reference neither
// prevents the file finalizer nor confuses a later object at a reused address,
// while remaining correct if the operating system rapidly reuses both the
// closed descriptor number and the unlinked file's inode.
func LockWriter(file *os.File) error {
	if file == nil {
		return ErrInvalidWrite
	}
	fd := file.Fd()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	ownerIdentity := weak.Make(file)
	writerLockRegistry.Lock()
	defer writerLockRegistry.Unlock()
	live := writerLockRegistry.entries[:0]
	conflict := false
	for _, entry := range writerLockRegistry.entries {
		owner := entry.owner.Value()
		if owner == nil {
			continue
		}
		ownerInfo, ownerErr := owner.Stat()
		if ownerErr != nil || !os.SameFile(entry.info, ownerInfo) {
			// This os.File no longer owns a process lease. A registered kernel
			// reference may still retain its flock after descriptor close; the
			// operating-system admission below continues to enforce that lock.
			// Exact owner identity also handles descriptor/inode reuse.
			continue
		}
		if os.SameFile(entry.info, info) {
			conflict = true
		}
		live = append(live, entry)
	}
	// Clearing the tail releases the dropped owners and their os.FileInfo.
	clear(writerLockRegistry.entries[len(live):])
	writerLockRegistry.entries = live
	if conflict {
		return fmt.Errorf("%w: file %q descriptor %d", ErrWriterLocked, file.Name(), fd)
	}
	if err := lockWriterPlatform(file); err != nil {
		return fmt.Errorf("lock writer file %q: %w", file.Name(), err)
	}
	writerLockRegistry.entries = append(writerLockRegistry.entries, writerLockIdentity{
		owner: ownerIdentity,
		info:  info,
	})
	return nil
}

// LockWriterUntil keeps the ordinary nonblocking admission attempt, but permits
// bounded waiting for an operating-system writer lock. A killed io_uring owner
// can leave registered file descriptions alive after waitpid returns. Only the
// kernel releasing that lock permits admission; this never unlocks another
// owner or retries recovery. In-process ownership conflicts remain immediate.
// The deadline limits contention waiting, not subsequent recovery work. Callers
// opening several files must reuse one absolute deadline. A nil context and
// zero deadline retain LockWriter's immediate behavior.
func LockWriterUntil(file *os.File, ctx context.Context, deadline time.Time) error {
	if ctx == nil && deadline.IsZero() {
		return LockWriter(file)
	}
	if ctx == nil || deadline.IsZero() {
		return ErrInvalidWrite
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		err := LockWriter(file)
		if err == nil || !errors.Is(err, ErrWriterLocked) ||
			!errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.Join(err, context.DeadlineExceeded)
		}
		// LockWriter has released the process registry mutex before this wait.
		// One reusable timer bounds allocations independently of contention.
		delay := min(remaining, time.Millisecond)
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, context.Cause(ctx))
		case <-timer.C:
		}
	}
}

// UnlockWriter releases a lease acquired by LockWriter. Unknown owners are
// ignored so partially constructed stores can use one cleanup path.
func UnlockWriter(file *os.File) error {
	if file == nil {
		return nil
	}
	ownerIdentity := weak.Make(file)
	writerLockRegistry.Lock()
	defer writerLockRegistry.Unlock()
	found := -1
	for i := range writerLockRegistry.entries {
		if writerLockRegistry.entries[i].owner == ownerIdentity {
			found = i
			break
		}
	}
	if found < 0 {
		return nil
	}
	if info, err := file.Stat(); err == nil &&
		os.SameFile(writerLockRegistry.entries[found].info, info) {
		if err := unlockWriterPlatform(file); err != nil {
			return err
		}
	}
	last := len(writerLockRegistry.entries) - 1
	writerLockRegistry.entries[found] = writerLockRegistry.entries[last]
	writerLockRegistry.entries[last] = writerLockIdentity{}
	writerLockRegistry.entries = writerLockRegistry.entries[:last]
	return nil
}
