//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storeio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func kernelWriterPair(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "writer.page")
	owner, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unlockWriterPlatform(owner); _ = owner.Close() })
	if err := lockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	contender, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = UnlockWriter(contender); _ = contender.Close() })
	return owner, contender
}

func TestWriterLockWaitExactKernelContentionAndRelease(t *testing.T) {
	owner, contender := kernelWriterPair(t)
	if err := LockWriter(contender); !errors.Is(err, ErrWriterLocked) ||
		!errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("immediate lock lost exact OS contention: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- LockWriterUntil(contender, ctx, time.Now().Add(time.Second)) }()
	select {
	case err := <-done:
		t.Fatalf("live kernel owner admitted or failed before release: %v", err)
	case <-time.After(15 * time.Millisecond):
	}
	// The waiter must not retain the global process registry while sleeping.
	unrelated, err := os.Create(filepath.Join(t.TempDir(), "unrelated"))
	if err != nil {
		t.Fatal(err)
	}
	defer unrelated.Close()
	other := make(chan error, 1)
	go func() { other <- LockWriter(unrelated) }()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
		defer UnlockWriter(unrelated)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("writer registry held during contention wait")
	}
	if err := unlockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("released lock: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("released lock was not acquired")
	}
	// The same descriptor now owns an ordinary exclusive lease.
	if err := lockWriterPlatform(owner); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("successful wait did not retain exclusive ownership: %v", err)
	}
}

func TestWriterLockWaitDeadlineCancellationAndNonContention(t *testing.T) {
	owner, contender := kernelWriterPair(t)
	deadline := time.Now().Add(15 * time.Millisecond)
	err := LockWriterUntil(contender, context.Background(), deadline)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrWriterLocked) ||
		!errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("deadline lost original contention: %v", err)
	}
	// Reusing the expired budget cannot grant another per-file wait.
	for range 3 {
		err = LockWriterUntil(contender, context.Background(), deadline)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("renewed budget: %v", err)
		}
	}
	cause := errors.New("startup stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() { done <- LockWriterUntil(contender, ctx, time.Now().Add(time.Second)) }()
	cancel(cause)
	if err := <-done; !errors.Is(err, cause) {
		t.Fatalf("cancel cause: %v", err)
	}
	if err := unlockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	if err := LockWriterUntil(contender, ctx, deadline); !errors.Is(err, cause) {
		t.Fatalf("canceled startup acquired uncontended lock: %v", err)
	}
	// Deadline only limits waiting, not uncontended recovery after a long open.
	if err := LockWriterUntil(contender, context.Background(), deadline); err != nil {
		t.Fatal(err)
	}
	if err := UnlockWriter(contender); err != nil {
		t.Fatal(err)
	}
	if err := contender.Close(); err != nil {
		t.Fatal(err)
	}
	if err := LockWriterUntil(contender, context.Background(), time.Now().Add(time.Second)); !errors.Is(err, os.ErrClosed) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("non-contention error retried: %v", err)
	}
}

func TestWriterLockWaitRejectsProcessOwnerAndMalformedPolicy(t *testing.T) {
	owner, contender := kernelWriterPair(t)
	if err := unlockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(owner); err != nil {
		t.Fatal(err)
	}
	defer UnlockWriter(owner)
	err := LockWriterUntil(contender, context.Background(), time.Now().Add(time.Second))
	if !errors.Is(err, ErrWriterLocked) || errors.Is(err, syscall.EWOULDBLOCK) ||
		errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("process owner must fail immediately, not masquerade as OS wait: %v", err)
	}
	if err := LockWriterUntil(contender, nil, time.Now()); !errors.Is(err, ErrInvalidWrite) {
		t.Fatal(err)
	}
	if err := LockWriterUntil(contender, context.Background(), time.Time{}); !errors.Is(err, ErrInvalidWrite) {
		t.Fatal(err)
	}
}
