package raftstore

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const nodeRecentTerms = 64

type nodeTermCoordinate struct {
	index, term, payloadGeneration uint64
	payloadBytes                   uint32
	kind                           pb.EntryType
}

// nodeLogCoordinates is a serving copy of durable metadata. It is published
// after the log sync and namespace proof, before Ready completion. Raft may read
// the preceding durable cut while an unrelated wave is in flight; these reads
// must never take the device mutex held across that wave's disk I/O.
type nodeLogCoordinates struct {
	first, last, commit, baseTerm uint64
	liveCommit                    uint64
	terms                         *[nodeRecentTerms]nodeTermCoordinate
}
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
func (s *NodeStore) publishCoordinatesLocked(group uint64, batch *seglog.ReadyBatch, payloads *[nodeRecentTerms]nodeTermCoordinate) {
	if s.coordinates == nil {
		return
	}
	s.coordinateMu.RLock()
	previous, warmed := s.coordinates[group]
	s.coordinateMu.RUnlock()
	if !warmed {
		return
	}
	state, ok := s.engine.Metadata(group)
	if !ok {
		return
	}
	s.coordinateMu.Lock()
	cut := coordinatesFromMetadata(state, previous.terms)
	if batch != nil {
		if batch.ReplaceFrom != 0 {
			for index := range cut.terms {
				if cut.terms[index].index >= batch.ReplaceFrom {
					cut.terms[index] = nodeTermCoordinate{}
				}
			}
		}
		for index, entry := range batch.Entries {
			if index < len(batch.Entries)-nodeRecentTerms {
				continue
			}
			term := nodeTermCoordinate{index: entry.Index, term: entry.Term, kind: entry.Type}
			if payloads != nil {
				prepared := payloads[entry.Index%nodeRecentTerms]
				if prepared.index == entry.Index && prepared.term == entry.Term {
					term = prepared
				}
			}
			cut.terms[entry.Index%nodeRecentTerms] = term
		}
	}
	s.coordinates[group] = cut
	s.coordinateMu.Unlock()
}

func (v *GroupView) logCoordinates() (nodeLogCoordinates, error) {
	s := v.store
	if err := s.coordinateReadError(); err != nil {
		return nodeLogCoordinates{}, err
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
	cut = coordinatesFromMetadata(state, new([nodeRecentTerms]nodeTermCoordinate))
	if hint, ok := s.commitHints[v.group]; ok {
		cut.liveCommit = hint.hard.Commit
	}
	if _, term, compacted, found, err := s.engine.LookupExact(v.group, state.LastIndex); err == nil && found && !compacted {
		cut.terms[state.LastIndex%nodeRecentTerms] = nodeTermCoordinate{index: state.LastIndex, term: term}
	}
	s.coordinateMu.Lock()
	if s.coordinates == nil {
		s.coordinates = make(map[uint64]nodeLogCoordinates)
		s.entryCacheArena = make([]byte, nodeEntryCacheSlots*nodeEntryCacheSlotBytes)
	}
	s.coordinates[v.group] = cut
	s.coordinateMu.Unlock()
	return cut, nil
}

func coordinatesFromMetadata(state seglog.GroupMetadata, terms *[nodeRecentTerms]nodeTermCoordinate) nodeLogCoordinates {
	baseTerm := state.TruncateTerm
	if state.Checkpoint.Index == state.FirstIndex-1 {
		baseTerm = state.Checkpoint.Term
	}
	return nodeLogCoordinates{first: state.FirstIndex, last: state.LastIndex, commit: state.Hard.Commit, baseTerm: baseTerm, terms: terms}
}

func (s *NodeStore) coordinateReadError() error {
	if s.closingFlag.Load() {
		return ErrClosed
	}
	if failure := s.coordinateFailure.Load(); failure != nil {
		return failure.err
	}
	if err := s.engine.PublishedFailure(); err != nil {
		return errors.Join(ErrPersistenceUnknown, err)
	}
	return nil
}

// cachedTerm covers the compacted boundary and a fixed recent-entry ring. An
// exact index tag prevents ring collisions from returning another entry's term.
// Historical misses keep the ordinary authenticated storage lookup.
func (v *GroupView) cachedTerm(index uint64) (uint64, bool, error) {
	s := v.store
	if err := s.coordinateReadError(); err != nil {
		return 0, true, err
	}
	s.coordinateMu.RLock()
	defer s.coordinateMu.RUnlock()
	cut, found := s.coordinates[v.group]
	if !found {
		return 0, false, nil
	}
	if index < cut.first-1 {
		return 0, true, raft.ErrCompacted
	}
	if index == cut.first-1 {
		return cut.baseTerm, true, nil
	}
	if index > cut.last {
		return 0, true, raft.ErrUnavailable
	}
	entry := cut.terms[index%nodeRecentTerms]
	if entry.index == index {
		return entry.term, true, nil
	}
	return 0, false, nil
}
