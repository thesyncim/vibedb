package shardservice

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

var (
	errReadFenceBusy     = errors.New("shardservice: coherent read fence intersects an admitted writer")
	errReadFenceCapacity = errors.New("shardservice: active coherent read-fence limit reached")
)

type readFence struct {
	deadline   time.Time
	bucketBits uint8
	scopes     []distributedtxn.IntentScope
}

type readFenceWriter struct {
	token      uint64
	bucketBits uint8
	scopes     []distributedtxn.IntentScope
}

type targetReservation = readFenceWriter

// readFenceSet is a short-lived, shard-local scoped reader/writer admission
// gate. Read fences are rare and writes stay allocation-free after the writer
// slice reaches its concurrency high-water mark. The gate is deliberately not
// durable: leases make an abandoned distributed read self-releasing.
type readFenceSet struct {
	mu        sync.Mutex
	active    map[distributedtxn.ID]readFence
	writers   []readFenceWriter
	barriers  []targetReservation
	changed   chan struct{}
	nextToken uint64
	limit     int
	closed    bool
}

func newReadFenceSet(limit int) *readFenceSet {
	return &readFenceSet{
		active: make(map[distributedtxn.ID]readFence), changed: make(chan struct{}), limit: limit,
	}
}

func (f *readFenceSet) acquire(
	id distributedtxn.ID,
	lease time.Duration,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) error {
	if id.IsZero() || !distributedtxn.ValidateIntentScopes(scopes, bucketBits) {
		return distributedtxn.ErrJournalConflict
	}
	if lease <= 0 {
		lease = time.Second
	}
	if lease > 10*time.Minute {
		lease = 10 * time.Minute
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return distributedtxn.ErrJournalClosed
	}
	now := time.Now()
	f.expireLocked(now)
	if prior, ok := f.active[id]; ok {
		if prior.bucketBits != bucketBits || !slices.Equal(prior.scopes, scopes) {
			return distributedtxn.ErrJournalConflict
		}
		// Do not prolong a fence ahead of an already admitted writer. The first
		// lease remains valid, preserving idempotency without writer starvation.
		if !f.writerConflictLocked(bucketBits, scopes) &&
			!f.barrierConflictLocked(bucketBits, scopes) {
			candidate := now.Add(lease)
			if candidate.After(prior.deadline) {
				prior.deadline = candidate
				f.active[id] = prior
			}
		}
		return nil
	}
	if len(f.active) >= f.limit {
		return errReadFenceCapacity
	}
	if f.writerConflictLocked(bucketBits, scopes) ||
		f.barrierConflictLocked(bucketBits, scopes) {
		return errReadFenceBusy
	}
	f.active[id] = readFence{
		deadline: now.Add(lease), bucketBits: bucketBits,
		scopes: append([]distributedtxn.IntentScope(nil), scopes...),
	}
	return nil
}

func (f *readFenceSet) release(id distributedtxn.ID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return distributedtxn.ErrJournalClosed
	}
	if _, ok := f.active[id]; ok {
		delete(f.active, id)
		f.signalLocked()
	}
	return nil
}

func (f *readFenceSet) validate(
	id distributedtxn.ID,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	f.expireLocked(time.Now())
	fence, ok := f.active[id]
	return ok && fence.bucketBits == bucketBits && slices.Equal(fence.scopes, scopes)
}

// enterWrite admits one scoped writer and returns a token that must be held
// through publication. Registering before checking active fences closes
// the race in which a reader could otherwise enter after admission but before
// the storage mutation becomes visible.
func (f *readFenceSet) enterWrite(
	ctx context.Context,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) (uint64, error) {
	if !distributedtxn.ValidateIntentScopes(scopes, bucketBits) {
		return 0, distributedtxn.ErrJournalConflict
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, distributedtxn.ErrJournalClosed
	}
	var token uint64
	for {
		f.expireLocked(time.Now())
		if token == 0 {
			if f.barrierConflictLocked(bucketBits, scopes) {
				changed := f.changed
				f.mu.Unlock()
				select {
				case <-changed:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
				f.mu.Lock()
				if f.closed {
					f.mu.Unlock()
					return 0, distributedtxn.ErrJournalClosed
				}
				continue
			}
			token = f.nextTokenLocked()
			f.writers = append(f.writers, readFenceWriter{
				token: token, bucketBits: bucketBits, scopes: scopes,
			})
		}
		if !f.fenceConflictLocked(bucketBits, scopes) {
			f.mu.Unlock()
			return token, nil
		}
		changed := f.changed
		deadline := f.earliestFenceDeadlineLocked(bucketBits, scopes)
		wait := time.Until(deadline)
		f.mu.Unlock()
		if wait <= 0 {
			f.mu.Lock()
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			f.leaveWrite(token)
			return 0, ctx.Err()
		}
		f.mu.Lock()
		if f.closed {
			f.removeWriterLocked(token)
			f.mu.Unlock()
			return 0, distributedtxn.ErrJournalClosed
		}
	}
}

// enterTarget reserves a scope while an in-flight ordinary writer drains,
// then holds it until the durable target barrier is published. New
// writers cannot cross the reservation; registered writers retain precedence.
func (f *readFenceSet) enterTarget(
	ctx context.Context,
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) (uint64, error) {
	if !distributedtxn.ValidateIntentScopes(scopes, bucketBits) {
		return 0, distributedtxn.ErrJournalConflict
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, distributedtxn.ErrJournalClosed
	}
	token := f.nextTokenLocked()
	f.barriers = append(f.barriers, targetReservation{
		token: token, bucketBits: bucketBits, scopes: scopes,
	})
	for {
		f.expireLocked(time.Now())
		writerConflict := f.writerConflictLocked(bucketBits, scopes)
		fenceConflict := f.fenceConflictLocked(bucketBits, scopes)
		if !writerConflict && !fenceConflict {
			f.mu.Unlock()
			return token, nil
		}
		changed := f.changed
		deadline := f.earliestFenceDeadlineLocked(bucketBits, scopes)
		f.mu.Unlock()
		if writerConflict || deadline.IsZero() {
			select {
			case <-changed:
			case <-ctx.Done():
				f.leaveTarget(token)
				return 0, ctx.Err()
			}
		} else {
			wait := time.Until(deadline)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-changed:
					stopReadFenceTimer(timer)
				case <-timer.C:
				case <-ctx.Done():
					stopReadFenceTimer(timer)
					f.leaveTarget(token)
					return 0, ctx.Err()
				}
			}
		}
		f.mu.Lock()
		if f.closed {
			f.removeTargetLocked(token)
			f.mu.Unlock()
			return 0, distributedtxn.ErrJournalClosed
		}
	}
}

func (f *readFenceSet) leaveTarget(token uint64) {
	f.mu.Lock()
	f.removeTargetLocked(token)
	f.mu.Unlock()
}

func (f *readFenceSet) removeTargetLocked(token uint64) {
	for i := range f.barriers {
		if f.barriers[i].token != token {
			continue
		}
		last := len(f.barriers) - 1
		f.barriers[i] = f.barriers[last]
		f.barriers[last] = targetReservation{}
		f.barriers = f.barriers[:last]
		f.signalLocked()
		return
	}
}

func (f *readFenceSet) nextTokenLocked() uint64 {
	f.nextToken++
	if f.nextToken == 0 {
		f.nextToken++
	}
	return f.nextToken
}

func (f *readFenceSet) leaveWrite(token uint64) {
	f.mu.Lock()
	f.removeWriterLocked(token)
	f.mu.Unlock()
}

func (f *readFenceSet) removeWriterLocked(token uint64) {
	for i := range f.writers {
		if f.writers[i].token != token {
			continue
		}
		last := len(f.writers) - 1
		f.writers[i] = f.writers[last]
		f.writers[last] = readFenceWriter{}
		f.writers = f.writers[:last]
		if len(f.barriers) != 0 {
			f.signalLocked()
		}
		return
	}
}

func (f *readFenceSet) barrierConflictLocked(
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) bool {
	for i := range f.barriers {
		barrier := &f.barriers[i]
		if readScopesConflict(bucketBits, scopes, barrier.bucketBits, barrier.scopes) {
			return true
		}
	}
	return false
}

func (f *readFenceSet) writerConflictLocked(
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) bool {
	for i := range f.writers {
		writer := &f.writers[i]
		if readScopesConflict(bucketBits, scopes, writer.bucketBits, writer.scopes) {
			return true
		}
	}
	return false
}

func (f *readFenceSet) fenceConflictLocked(
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) bool {
	for _, fence := range f.active {
		if readScopesConflict(bucketBits, scopes, fence.bucketBits, fence.scopes) {
			return true
		}
	}
	return false
}

func (f *readFenceSet) earliestFenceDeadlineLocked(
	bucketBits uint8,
	scopes []distributedtxn.IntentScope,
) time.Time {
	earliest := time.Time{}
	for _, fence := range f.active {
		if readScopesConflict(bucketBits, scopes, fence.bucketBits, fence.scopes) &&
			(earliest.IsZero() || fence.deadline.Before(earliest)) {
			earliest = fence.deadline
		}
	}
	return earliest
}

func readScopesConflict(
	aBits uint8,
	a []distributedtxn.IntentScope,
	bBits uint8,
	b []distributedtxn.IntentScope,
) bool {
	return len(a) == 0 || len(b) == 0 || aBits != bBits ||
		distributedtxn.IntentScopesOverlap(a, b)
}

func stopReadFenceTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (f *readFenceSet) expireLocked(now time.Time) {
	changed := false
	for id, fence := range f.active {
		if !fence.deadline.After(now) {
			delete(f.active, id)
			changed = true
		}
	}
	if changed {
		f.signalLocked()
	}
}

func (f *readFenceSet) signalLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *readFenceSet) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	clear(f.active)
	close(f.changed)
}
