package exchange

import (
	"context"
	"errors"
	"math/bits"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	DefaultMaxMailboxes           = 4096
	DefaultMaxReservedBufferBytes = 4 << 30
	MaxPartitions                 = 1 << 20
)

var (
	ErrRegistryLimit = errors.New("exchange: registry admission limit exceeded")
	ErrSpecConflict  = errors.New("exchange: mailbox identity has a different specification")
	ErrNotFound      = errors.New("exchange: mailbox was not found")
	ErrPartitions    = errors.New("exchange: partition count is invalid")
)

type RegistryOptions struct {
	MaxMailboxes           int
	MaxReservedBufferBytes uint64
}

// Registry owns the bounded mailbox directory for one worker. Open reserves
// buffer quota before publishing a key, so many empty mailboxes cannot promise
// more aggregate queue memory than the worker admitted.
type Registry struct {
	mu       sync.Mutex
	boxes    map[Key]*Mailbox
	max      int
	maxBytes uint64
	reserved uint64
	closed   bool
}

func NewRegistry(options RegistryOptions) *Registry {
	if options.MaxMailboxes <= 0 {
		options.MaxMailboxes = DefaultMaxMailboxes
	}
	if options.MaxReservedBufferBytes == 0 {
		options.MaxReservedBufferBytes = DefaultMaxReservedBufferBytes
	}
	return &Registry{
		boxes: make(map[Key]*Mailbox), max: options.MaxMailboxes,
		maxBytes: options.MaxReservedBufferBytes,
	}
}

// Open creates a mailbox or returns the existing mailbox for an identical
// retry. Reusing a key with different limits fails closed.
func (r *Registry) Open(spec Spec) (*Mailbox, error) {
	if r == nil || !spec.Valid() {
		return nil, ErrInvalidSpec
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if existing := r.boxes[spec.Key]; existing != nil {
		if !sameOpenSpec(existing.spec, spec) {
			return nil, ErrSpecConflict
		}
		if existing.Err() != nil {
			return nil, ErrClosed
		}
		return existing, nil
	}
	if len(r.boxes) >= r.max || spec.BufferedBytes > r.maxBytes-r.reserved {
		return nil, ErrRegistryLimit
	}
	box := newMailbox(spec)
	r.boxes[spec.Key] = box
	r.reserved += spec.BufferedBytes
	return box, nil
}

// sameOpenSpec deliberately ignores the locally-derived absolute deadline.
// A transport retry may arrive later and recompute that value, but it must not
// mutate or extend the lifetime admitted by the first successful Open.
func sameOpenSpec(a, b Spec) bool {
	a.DeadlineUnixNano = 0
	b.DeadlineUnixNano = 0
	return a == b
}

func (r *Registry) Lookup(key Key) (*Mailbox, bool) {
	if r == nil || !key.valid() {
		return nil, false
	}
	r.mu.Lock()
	box := r.boxes[key]
	r.mu.Unlock()
	return box, box != nil
}

// Delete removes and cancels one mailbox, releasing its reserved buffer quota.
func (r *Registry) Delete(key Key, cause error) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	box := r.boxes[key]
	if box != nil {
		delete(r.boxes, key)
		r.reserved -= box.spec.BufferedBytes
	}
	r.mu.Unlock()
	if box != nil {
		box.Cancel(cause)
		return true
	}
	return false
}

// Reap cancels expired mailboxes and returns their count. It allocates nothing
// and is intended for a bounded control-loop tick, not a data-path goroutine.
func (r *Registry) Reap(now time.Time) int {
	if r == nil {
		return 0
	}
	nanos := now.UnixNano()
	reaped := 0
	r.mu.Lock()
	for key, box := range r.boxes {
		deadline := box.spec.DeadlineUnixNano
		if deadline == 0 || nanos < deadline {
			continue
		}
		delete(r.boxes, key)
		r.reserved -= box.spec.BufferedBytes
		box.Cancel(context.DeadlineExceeded)
		reaped++
	}
	r.mu.Unlock()
	return reaped
}

// Close cancels every mailbox and permanently closes the directory.
func (r *Registry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	for key, box := range r.boxes {
		delete(r.boxes, key)
		box.Cancel(ErrClosed)
	}
	r.reserved = 0
	r.mu.Unlock()
}

// PartitionFor maps a canonical 64-bit key hash onto a fixed stage partition
// without modulo bias. The partition count is immutable for one stage attempt.
func PartitionFor(hash uint64, partitions uint32) (uint32, error) {
	if partitions == 0 || partitions > MaxPartitions {
		return 0, ErrPartitions
	}
	hi, _ := bits.Mul64(hash, uint64(partitions))
	return uint32(hi), nil
}

// PartitionForKey hashes one already-canonical composite key with the fixed
// cross-process xxhash algorithm, then uses multiply-high reduction. Every
// producer therefore maps equal keys to one stage partition without serializing
// a seed or paying modulo bias.
func PartitionForKey(key []byte, partitions uint32) (uint32, error) {
	return PartitionFor(xxhash.Sum64(key), partitions)
}
