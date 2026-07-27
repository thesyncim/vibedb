package storeio

import (
	"sync"
	"sync/atomic"
)

// indexPool is a fixed stack of indexes. Ordinary acquire/release remains
// available for descriptor pools, while buffer reservations use popN/pushN so
// one transaction pays one lock round trip and one slice copy rather than one
// synchronization round trip per page.
//
// notify is only a parking hint after an exact reservation is unavailable and
// is not part of the ownership protocol.
type indexPool struct {
	mu      sync.Mutex
	indexes []uint32
	notify  chan struct{}
	waiters uint32
	// available mirrors successful stack ownership transfers. It is a
	// lock-free capacity hint for serialized producers; pop/popN remain the
	// authoritative admission operations under races.
	available atomic.Int64
}

func newIndexPool(count int) *indexPool {
	p := &indexPool{
		indexes: make([]uint32, count),
		notify:  make(chan struct{}, 1),
	}
	// The end of indexes is the stack top. Reversing initialization preserves
	// the established first-pop order 0, 1, ... without paying for it later.
	for index := range count {
		p.indexes[index] = uint32(count - index - 1)
	}
	p.available.Store(int64(count))
	return p
}

func (p *indexPool) pop() (uint32, bool) {
	p.mu.Lock()
	if len(p.indexes) == 0 {
		p.mu.Unlock()
		return 0, false
	}
	last := len(p.indexes) - 1
	index := p.indexes[last]
	p.indexes = p.indexes[:last]
	p.available.Add(-1)
	p.mu.Unlock()
	return index, true
}

func (p *indexPool) push(index uint32) {
	p.pushN([]uint32{index})
}

// popN atomically acquires exactly len(dst) indexes. It changes nothing when
// the complete reservation is unavailable, so a large transaction cannot hold
// a partial reservation while waiting for the worker to recycle the rest.
func (p *indexPool) popN(dst []uint32) bool {
	if len(dst) == 0 {
		return true
	}
	p.mu.Lock()
	if len(p.indexes) < len(dst) {
		p.mu.Unlock()
		return false
	}
	start := len(p.indexes) - len(dst)
	copy(dst, p.indexes[start:])
	p.indexes = p.indexes[:start]
	p.available.Add(-int64(len(dst)))
	p.mu.Unlock()
	return true
}

// pushN returns every index with one lock acquisition and one slice copy.
// Waiting exact reservations are notified after every non-empty release: a
// pool need not have been completely empty for a larger waiter to become
// satisfiable.
func (p *indexPool) pushN(indexes []uint32) {
	if len(indexes) == 0 {
		return
	}
	p.mu.Lock()
	p.indexes = append(p.indexes, indexes...)
	p.available.Add(int64(len(indexes)))
	if p.waiters != 0 {
		// Exact reservations can have different sizes. Wake every waiter so a
		// large unsatisfied reservation cannot consume the only notification
		// while a smaller satisfiable reservation remains parked.
		close(p.notify)
		p.notify = make(chan struct{})
	}
	p.mu.Unlock()
}

// prepareWait registers a waiter and returns the current broadcast generation.
// Callers retry their pop after registering, closing the race with pushN.
func (p *indexPool) prepareWait() <-chan struct{} {
	p.mu.Lock()
	p.waiters++
	notify := p.notify
	p.mu.Unlock()
	return notify
}

func (p *indexPool) finishWait() {
	p.mu.Lock()
	p.waiters--
	p.mu.Unlock()
}

func (p *indexPool) availableCount() int64 {
	if p == nil {
		return 0
	}
	return p.available.Load()
}
