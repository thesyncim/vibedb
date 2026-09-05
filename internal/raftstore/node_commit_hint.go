package raftstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

// nodeCommitHint contains no borrowed protobuf fields or log bytes. Like the
// legacy Store's volatile commit path, it records only knowledge about entries
// which are already durable. The next durable batch folds the Ready cursor and
// commit into its authenticated frame. A crash may lose this knowledge; Raft or
// the state machine's durable publication reestablishes the committed prefix.
// At most MaximumReadySpan-1 logical Readies can remain pending per real group.
type nodeCommitHint struct {
	incarnation, readyID uint64
	digest               [16]byte
	hard                 seglog.HardState
}

func (s *NodeStore) liveNodeMetadata(group uint64, state seglog.GroupMetadata) seglog.GroupMetadata {
	if hint, ok := s.commitHints[group]; ok {
		state.Hard, state.ReadyID, state.ReadyDigest = hint.hard, hint.readyID, hint.digest
	}
	return state
}

func nodeHintEligible(batch raftmodel.PersistBatch, hard seglog.HardState) bool {
	return !batch.MustSync && len(batch.Entries) == 0 && canonicalEmptySnapshot(batch.Snapshot) &&
		(isEmptyHardState(batch.HardState) || batch.HardState.GetTerm() == hard.Term && batch.HardState.GetVote() == hard.Vote)
}

func (s *NodeStore) publishCommitHintLocked(group uint64, hint nodeCommitHint) {
	if s.commitHints == nil {
		s.commitHints = make(map[uint64]nodeCommitHint)
	}
	s.commitHints[group] = hint
	s.coordinateMu.Lock()
	if cut, ok := s.coordinates[group]; ok {
		cut.liveCommit = hint.hard.Commit
		s.coordinates[group] = cut
	}
	s.coordinateMu.Unlock()
}

// flushCommitHintsLocked is used only at a span or control boundary. Groups
// must be sorted. Pending knowledge stays intact on failure, and neither an
// engine frame nor a published durable coordinate advances before data sync.
func (s *NodeStore) flushCommitHintsLocked(groups []uint64) error {
	if len(groups) == 0 || len(s.commitHints) == 0 {
		return nil
	}
	if len(groups) > MaxPersistGroupBatches {
		return ErrBounds
	}
	var batches [MaxPersistGroupBatches]seglog.ReadyBatch
	var hard [MaxPersistGroupBatches]seglog.HardState
	var canonical [32 + MaxPersistGroupBatches*40]byte
	offset := copy(canonical[:], "vibedb/node-commit-hints/v1")
	count := 0
	for _, group := range groups {
		hint, ok := s.commitHints[group]
		if !ok {
			continue
		}
		state, ok := s.engine.Summary(group)
		if !ok || state.NodeIncarnation != hint.incarnation || hint.readyID <= state.ReadyID || hint.readyID-state.ReadyID >= seglog.MaximumReadySpan {
			return ErrInvalid
		}
		hard[count] = hint.hard
		batches[count] = seglog.ReadyBatch{GroupID: group, NodeIncarnation: hint.incarnation, ReadyID: hint.readyID, ReadySpan: hint.readyID - state.ReadyID, ReadyDigest: hint.digest, Hard: &hard[count]}
		binary.LittleEndian.PutUint64(canonical[offset:], group)
		binary.LittleEndian.PutUint64(canonical[offset+8:], hint.incarnation)
		binary.LittleEndian.PutUint64(canonical[offset+16:], hint.readyID)
		copy(canonical[offset+24:], hint.digest[:])
		offset += 40
		count++
	}
	if count == 0 {
		return nil
	}
	if err := s.proveNamespace(); err != nil {
		s.poisonLocked(err)
		return errors.Join(ErrPersistenceUnknown, err)
	}
	digest := sha256.Sum256(canonical[:offset])
	var id seglog.WaveID
	copy(id[:], digest[:16])
	wave := seglog.Wave{ID: id, Batches: batches[:count]}
	var err error
	if s.persistWaveTest != nil {
		err = s.persistWaveTest(wave)
	} else {
		err = s.engine.PersistWave(wave)
	}
	if err != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisonLocked(fatal)
			return errors.Join(ErrPersistenceUnknown, err, fatal)
		}
		if errors.Is(err, seglog.ErrBackpressure) {
			return errors.Join(ErrDurabilityBackpressure, err)
		}
		return errors.Join(ErrInvalid, err)
	}
	if err = s.proveNamespace(); err != nil {
		s.poisonLocked(err)
		return errors.Join(ErrPersistenceUnknown, err)
	}
	for _, batch := range batches[:count] {
		delete(s.commitHints, batch.GroupID)
		s.publishCoordinatesLocked(batch.GroupID, nil, nil)
	}
	return nil
}

func (s *NodeStore) flushAllCommitHintsLocked() error {
	for len(s.commitHints) != 0 {
		var groups [MaxPersistGroupBatches]uint64
		count := 0
		for group := range s.commitHints {
			groups[count] = group
			count++
			if count == len(groups) {
				break
			}
		}
		sort.Slice(groups[:count], func(i, j int) bool { return groups[i] < groups[j] })
		if err := s.flushCommitHintsLocked(groups[:count]); err != nil {
			return err
		}
	}
	return nil
}
