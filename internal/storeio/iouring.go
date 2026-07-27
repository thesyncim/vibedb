// Package storeio provides the bounded page-I/O and automatic commit substrate
// used by Store persistence. It is intentionally not a general io_uring
// binding: constraining the surface keeps kernel pointer lifetimes, queue
// ownership, durability ordering, and completion accounting reviewable.
package storeio

import "errors"

var (
	// ErrUnavailable reports that io_uring is absent, disabled, blocked by the
	// process sandbox, or unavailable on this platform. Callers should select
	// the portable positional-I/O backend.
	ErrUnavailable = errors.New("vibejson: io_uring unavailable")
	// ErrUnsupported reports a ring that cannot execute the fixed-buffer file
	// operations required by the Store page engine.
	ErrUnsupported = errors.New("vibejson: io_uring lacks required operations")
	// ErrQueueFull reports that every submission slot is owned by the kernel or
	// waiting for its completion to be consumed.
	ErrQueueFull = errors.New("vibejson: io_uring submission queue full")
	// ErrBufferBusy reports an attempt to submit a registered staging buffer
	// that is already owned by an in-flight request.
	ErrBufferBusy = errors.New("vibejson: io_uring buffer is in flight")
	// ErrOverflow reports a kernel submission drop, completion overflow, or a
	// completion token that does not name an outstanding request. The caller
	// must treat the ring as failed because request accounting is no longer
	// trustworthy.
	ErrOverflow = errors.New("vibejson: io_uring queue accounting lost")
	// ErrClosed reports use after a Store I/O owner begins closing.
	ErrClosed = errors.New("vibejson: io_uring ring closed")
)

// Config fixes the bounded memory owned by a Ring.
type Config struct {
	// Entries is the maximum number of outstanding requests. Zero selects 256.
	// Open rounds it up to a power of two and caps it at 32,768.
	Entries uint32
	// SingleIssuer enables the Linux single-issuer and deferred-task-run hints
	// when the kernel accepts them. The caller must lock its goroutine to one OS
	// thread before Open and keep it locked until Close.
	SingleIssuer bool
}

// Features records the setup features actually accepted by the kernel.
type Features struct {
	SingleIssuer bool
	SingleMmap   bool
	NoDrop       bool
	// AsyncRead reports support for IORING_OP_READ into caller-owned stable
	// memory. PageCache uses it to read directly into its mmap arena without
	// registered-buffer pinning or a staging copy.
	AsyncRead bool
}

// Completion is one consumed completion queue entry. UserData is the opaque
// value supplied at preparation time. Result is a byte count or a negated
// Linux errno.
type Completion struct {
	UserData uint64
	Result   int32
	Flags    uint32
}
