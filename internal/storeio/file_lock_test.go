package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The writer-lock registry is process-global mutable state, which makes every
// mistake in it a cross-test and cross-caller fault rather than a local one.
// These tests pin the two halves of what it must do: refuse a second writer for
// the same file however it was opened, and never refuse a file it has no live
// claim on.

// Given a registry entry whose owner never unlocked, when the operating system
// hands the same descriptor number to a different file, then locking that file
// succeeds.
//
// This is the failure that cascades. Descriptor numbers are unique only while
// the descriptor is open and the kernel reissues the lowest free one, so an
// entry that outlives its descriptor poisons a number that some unrelated file
// will be given next. Keying the conflict test on the number made one durable
// store that was dropped without Close refuse every store opened afterwards, and
// in this repository's suite that turned a single timing-sensitive failure into
// every later durable test in the package failing with the same message.
func TestLockWriterAcceptsRecycledDescriptorOfADifferentFile(t *testing.T) {
	directory := t.TempDir()
	abandoned, err := os.Create(filepath.Join(directory, "abandoned"))
	if err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(abandoned); err != nil {
		t.Fatal(err)
	}
	number := abandoned.Fd()
	// Closing without UnlockWriter is a store dropped without Close, or a test
	// that failed before its cleanup ran. The flock goes with the descriptor;
	// only the registry entry survives.
	if err := abandoned.Close(); err != nil {
		t.Fatal(err)
	}

	replacement, err := os.Create(filepath.Join(directory, "replacement"))
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if replacement.Fd() != number {
		t.Skipf("the runtime did not reuse descriptor %d, so there is nothing to "+
			"recycle here; it opened %d instead", number, replacement.Fd())
	}
	if err := LockWriter(replacement); err != nil {
		t.Fatalf("locking a different file that was handed the abandoned entry's "+
			"descriptor number %d failed with %v; a stale entry must not reserve a "+
			"number the kernel has already reissued", number, err)
	}
	if err := UnlockWriter(replacement); err != nil {
		t.Fatal(err)
	}
}

// Closing an owner releases its kernel lease before the Store cleanup path can
// call UnlockWriter. That cleanup must still remove the in-process identity;
// otherwise a rapidly reused inode can make an unrelated file look locked.
func TestUnlockWriterDropsAnOwnerThatWasAlreadyClosed(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "collection"))
	if err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(file); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UnlockWriter(file); err != nil {
		t.Fatalf("unlock after caller closed the owner: %v", err)
	}
}

// Given a file that is already locked, when it is opened again through a second
// descriptor or duplicated onto a third, then both are refused.
//
// This is the invariant the registry exists for, and it is what must survive the
// stale-entry sweep above: identity is the conflict test, so neither a fresh
// open nor a dup may slip past it.
func TestLockWriterRefusesTheSameFileThroughAnyDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collection")
	owner, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := LockWriter(owner); err != nil {
		t.Fatal(err)
	}
	defer UnlockWriter(owner)

	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := LockWriter(reopened); !errors.Is(err, ErrWriterLocked) {
		_ = UnlockWriter(reopened)
		t.Fatalf("a second open of the locked file returned %v, want %v",
			err, ErrWriterLocked)
	}
	if err := LockWriter(owner); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("relocking through the owning descriptor returned %v, want %v",
			err, ErrWriterLocked)
	}
}

// Given a lock that was released, when the same file is locked again, then it
// succeeds. Sweeping stale entries must not also drop live ones.
func TestLockWriterReacquiresAfterUnlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collection")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for round := range 3 {
		if err := LockWriter(file); err != nil {
			t.Fatalf("round %d lock: %v", round, err)
		}
		if err := UnlockWriter(file); err != nil {
			t.Fatalf("round %d unlock: %v", round, err)
		}
	}
	// A second file must still be lockable alongside a first, or the sweep has
	// started evicting entries that are alive.
	other, err := os.Create(filepath.Join(t.TempDir(), "other"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := LockWriter(file); err != nil {
		t.Fatal(err)
	}
	defer UnlockWriter(file)
	if err := LockWriter(other); err != nil {
		t.Fatalf("locking an unrelated second file failed: %v", err)
	}
	if err := UnlockWriter(other); err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(other); err != nil {
		t.Fatalf("relocking the second file after unlock failed: %v", err)
	}
	if err := UnlockWriter(other); err != nil {
		t.Fatal(err)
	}
}
