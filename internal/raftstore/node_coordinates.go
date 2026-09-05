package raftstore

import (
	"errors"
	"go.etcd.io/raft/v3"
)

// nodeLogCoordinates is a serving copy of durable metadata. It is published
// after the log sync and namespace proof, before Ready completion. Raft may read
// the preceding durable cut while an unrelated wave is in flight; these reads
// must never take the device mutex held across that wave's disk I/O.
type nodeLogCoordinates struct{ first, last, commit uint64 }
type nodeCoordinateFailure struct{ err error }

// poisonLocked requires mu and makes failure visible to both the storage and
// coordinate-only paths. No coordinate can be mistaken for a healthy serving
// cut after an outcome-unknown failure has been published.
func (s *NodeStore) poisonLocked(err error) {
	s.poisoned = err
	s.coordinateFailure.Store(&nodeCoordinateFailure{err: errors.Join(ErrPersistenceUnknown, err)})
}

// publishCoordinatesLocked requires mu. The separate lock is held only for the
// in-memory publication, never for log writes, syncs, checkpoint I/O, or readers.
// Only warmed groups need updates, keeping publication proportional to the wave.
func (s *NodeStore) publishCoordinatesLocked(group uint64) {
	if s.coordinates == nil {
		return
	}
	s.coordinateMu.RLock()
	_, warmed := s.coordinates[group]
	s.coordinateMu.RUnlock()
	if !warmed {
		return
	}
	state, ok := s.engine.Metadata(group)
	if !ok {
		return
	}
	s.coordinateMu.Lock()
	s.coordinates[group] = nodeLogCoordinates{state.FirstIndex, state.LastIndex, state.Hard.Commit}
	s.coordinateMu.Unlock()
}

func (v *GroupView) logCoordinates() (nodeLogCoordinates, error) {
	s := v.store
	if s.closingFlag.Load() {
		return nodeLogCoordinates{}, ErrClosed
	}
	if failure := s.coordinateFailure.Load(); failure != nil {
		return nodeLogCoordinates{}, failure.err
	}
	if err := s.engine.PublishedFailure(); err != nil {
		return nodeLogCoordinates{}, errors.Join(ErrPersistenceUnknown, err)
	}
	s.coordinateMu.RLock()
	cut, found := s.coordinates[v.group]
	s.coordinateMu.RUnlock()
	if found {
		return cut, nil
	}
	// Cold admission initializes only real groups. Invalid group IDs cannot grow
	// the map, and reopened stores reconstruct coordinates from recovered metadata.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nodeLogCoordinates{}, err
	}
	state, ok := s.engine.Metadata(v.group)
	if !ok {
		return nodeLogCoordinates{}, raft.ErrUnavailable
	}
	cut = nodeLogCoordinates{state.FirstIndex, state.LastIndex, state.Hard.Commit}
	s.coordinateMu.Lock()
	if s.coordinates == nil {
		s.coordinates = make(map[uint64]nodeLogCoordinates)
	}
	s.coordinates[v.group] = cut
	s.coordinateMu.Unlock()
	return cut, nil
}
