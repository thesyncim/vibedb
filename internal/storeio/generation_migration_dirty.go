package storeio

import (
	"fmt"
	"sync"
)

// GenerationMigrationDirtySet is a fixed-memory coalescing set of immutable
// source leaf identities dirtied while a replacement generation is copied.
// A full set rejects the publication observer before visibility, providing
// explicit foreground backpressure rather than losing reconciliation work.
type GenerationMigrationDirtySet struct {
	mu         sync.Mutex
	entries    []uint64
	used       int
	generation uint64
	topology   bool
}

func NewGenerationMigrationDirtySet(capacity int) (*GenerationMigrationDirtySet, error) {
	if capacity < 1 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("%w: migration dirty-set capacity", ErrInvalidWrite)
	}
	return &GenerationMigrationDirtySet{entries: make([]uint64, capacity)}, nil
}

func migrationDirtyHash(id uint64) uint64 {
	id ^= id >> 30
	id *= 0xbf58476d1ce4e5b9
	id ^= id >> 27
	id *= 0x94d049bb133111eb
	return id ^ id>>31
}

func (s *GenerationMigrationDirtySet) markLocked(id uint64) error {
	// Stored values are biased so zero remains the empty marker.
	if id == ^uint64(0) {
		return fmt.Errorf("%w: migration dirty identity", ErrInvalidWrite)
	}
	value := id + 1
	mask := uint64(len(s.entries) - 1)
	for probe := uint64(0); probe < uint64(len(s.entries)); probe++ {
		at := (migrationDirtyHash(id) + probe) & mask
		if s.entries[at] == value {
			return nil
		}
		if s.entries[at] == 0 {
			s.entries[at] = value
			s.used++
			return nil
		}
	}
	return ErrQueueFull
}

// Mark coalesces one source identity without allocating.
func (s *GenerationMigrationDirtySet) Mark(id uint64, generation uint64) error {
	if s == nil || generation == 0 {
		return fmt.Errorf("%w: migration dirty mark", ErrInvalidWrite)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.markLocked(id); err != nil {
		return err
	}
	if generation > s.generation {
		s.generation = generation
	}
	return nil
}

// ObservePublication returns a required committer observer. route maps each
// canonical mutation key to its immutable source-leaf identity. A topology-
// only descriptor conservatively requests root/vector revalidation.
func (s *GenerationMigrationDirtySet) ObservePublication(
	route func(key []byte) (uint64, bool),
) func(uint64, []byte) error {
	return func(generation uint64, descriptor []byte) error {
		if s == nil || route == nil || generation == 0 {
			return fmt.Errorf("%w: migration publication observer", ErrInvalidWrite)
		}
		view, err := OpenPublicationDescriptor(descriptor)
		if err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		mutations := 0
		for {
			mutation, ok, err := view.Next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			mutations++
			id, ok := route(mutation.Key)
			if !ok {
				return fmt.Errorf("%w: migration publication route", ErrInvalidWrite)
			}
			if err := s.markLocked(id); err != nil {
				return err
			}
		}
		if mutations == 0 {
			s.topology = true
		}
		if generation > s.generation {
			s.generation = generation
		}
		return nil
	}
}

// Drain appends the current coalesced identities into caller-owned dst and
// atomically begins the next reconciliation round. topology means the caller
// must also revalidate the complete root/router vector. No steady allocation
// occurs when dst has capacity s.Capacity().
func (s *GenerationMigrationDirtySet) Drain(
	dst []uint64,
) (ids []uint64, generation uint64, topology bool) {
	if s == nil {
		return dst, 0, false
	}
	s.mu.Lock()
	for at, value := range s.entries {
		if value != 0 {
			dst = append(dst, value-1)
			s.entries[at] = 0
		}
	}
	generation, topology = s.generation, s.topology
	s.used, s.topology = 0, false
	s.mu.Unlock()
	return dst, generation, topology
}

func (s *GenerationMigrationDirtySet) Capacity() int {
	if s == nil {
		return 0
	}
	return len(s.entries)
}
