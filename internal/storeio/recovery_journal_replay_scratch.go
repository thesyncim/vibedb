package storeio

import "sync"

// Keep one record-sized replay buffer, independently of append scratch. A large
// or hostile record can still be decoded under the journal's existing bound,
// but cannot make every idle journal retain its maximum physical capacity.
const recoveryReplayRetainedBytes = 4 << 20

type recoveryReplayScratch struct {
	mu     sync.Mutex
	buffer []byte
	closed bool
}

func (s *recoveryReplayScratch) take() []byte {
	s.mu.Lock()
	buffer := s.buffer
	s.buffer = nil
	s.mu.Unlock()
	return buffer[:0]
}

func (s *recoveryReplayScratch) put(buffer []byte) {
	if cap(buffer) == 0 || cap(buffer) > recoveryReplayRetainedBytes {
		return
	}
	s.mu.Lock()
	// The active replay owns its buffer exclusively, including across callbacks.
	// Nested/concurrent replay therefore gets independent storage. On return only
	// the larger bounded buffer remains cached; no list accumulates per replay.
	if !s.closed && cap(buffer) > cap(s.buffer) {
		s.buffer = buffer[:0]
	}
	s.mu.Unlock()
}

func (s *recoveryReplayScratch) close() {
	s.mu.Lock()
	s.closed = true
	s.buffer = nil
	s.mu.Unlock()
}
