package seglog

import (
	"encoding/binary"
	"io"
)

const (
	checkpointSummaryHard = 1 << iota
	checkpointSummaryTruncate
	checkpointSummaryCheckpoint
	checkpointSummaryReady
	checkpointSummaryWave
)

type checkpointGroupSummary struct {
	GroupID uint64
	Summary sealedRunSummary
}

// appendCheckpointGroupSummary is the compact canonical control-state codec
// used by streamed anchor checkpoints. It carries no route geometry and stores
// the latest retry identity inline because checkpoint groups are independent.
func appendCheckpointGroupSummary(dst []byte, previousGroup uint64, group checkpointGroupSummary) ([]byte, error) {
	s := group.Summary
	if group.GroupID <= previousGroup || (s.LastIndex == 0) != (s.LastTerm == 0) || (s.TruncateIndex == 0) != (s.TruncateTerm == 0) || (s.LatestWaveID == (WaveID{})) != (s.LatestWaveDigest == ([32]byte{}) && s.LatestWaveSequence == 0) {
		return nil, ErrCorrupt
	}
	dst = appendUvarint(dst, group.GroupID-previousGroup)
	flags := byte(0)
	if s.Hard != (HardState{}) {
		flags |= checkpointSummaryHard
	}
	if s.TruncateIndex != 0 {
		flags |= checkpointSummaryTruncate
	}
	if s.Checkpoint != (Checkpoint{}) {
		flags |= checkpointSummaryCheckpoint
	}
	if s.NodeIncarnation != 0 {
		flags |= checkpointSummaryReady
	}
	if s.LatestWaveID != (WaveID{}) {
		flags |= checkpointSummaryWave
	}
	dst = append(dst, flags)
	dst = appendUvarint(dst, s.LastIndex)
	dst = appendUvarint(dst, s.LastTerm)
	if flags&checkpointSummaryHard != 0 {
		dst = appendUvarint(dst, s.Hard.Term)
		dst = appendUvarint(dst, s.Hard.Vote)
		dst = appendUvarint(dst, s.Hard.Commit)
	}
	if flags&checkpointSummaryTruncate != 0 {
		dst = appendUvarint(dst, s.TruncateIndex)
		dst = appendUvarint(dst, s.TruncateTerm)
	}
	if flags&checkpointSummaryCheckpoint != 0 {
		dst = append(dst, s.Checkpoint.ID[:]...)
		dst = appendUvarint(dst, s.Checkpoint.Index)
		dst = appendUvarint(dst, s.Checkpoint.Term)
	}
	if flags&checkpointSummaryReady != 0 {
		dst = appendUvarint(dst, s.NodeIncarnation)
		dst = appendUvarint(dst, s.ReadyID)
		dst = append(dst, s.ReadyDigest[:]...)
		dst = append(dst, s.ReadyWaveID[:]...)
	}
	if flags&checkpointSummaryWave != 0 {
		dst = append(dst, s.LatestWaveID[:]...)
		dst = append(dst, s.LatestWaveDigest[:]...)
		dst = appendUvarint(dst, s.LatestWaveSequence)
	}
	return dst, nil
}

type checkpointByteReader interface {
	io.ByteReader
	Read([]byte) (int, error)
}

func readCanonicalUvarint(reader io.ByteReader) (uint64, error) {
	var raw [10]byte
	for i := range raw {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		raw[i] = b
		if b < 0x80 {
			value, n := binary.Uvarint(raw[:i+1])
			if n != i+1 || binary.PutUvarint(raw[:], value) != i+1 {
				return 0, ErrCorrupt
			}
			return value, nil
		}
	}
	return 0, ErrCorrupt
}

func readCheckpointGroupSummary(reader checkpointByteReader, previousGroup uint64) (checkpointGroupSummary, error) {
	delta, err := readCanonicalUvarint(reader)
	if err != nil || delta == 0 || previousGroup > ^uint64(0)-delta {
		return checkpointGroupSummary{}, ErrCorrupt
	}
	flags, err := reader.ReadByte()
	if err != nil || flags&^(checkpointSummaryHard|checkpointSummaryTruncate|checkpointSummaryCheckpoint|checkpointSummaryReady|checkpointSummaryWave) != 0 {
		return checkpointGroupSummary{}, ErrCorrupt
	}
	group := checkpointGroupSummary{GroupID: previousGroup + delta}
	s := &group.Summary
	if s.LastIndex, err = readCanonicalUvarint(reader); err != nil {
		return checkpointGroupSummary{}, ErrCorrupt
	}
	if s.LastTerm, err = readCanonicalUvarint(reader); err != nil || (s.LastIndex == 0) != (s.LastTerm == 0) {
		return checkpointGroupSummary{}, ErrCorrupt
	}
	if flags&checkpointSummaryHard != 0 {
		if s.Hard.Term, err = readCanonicalUvarint(reader); err == nil {
			s.Hard.Vote, err = readCanonicalUvarint(reader)
		}
		if err == nil {
			s.Hard.Commit, err = readCanonicalUvarint(reader)
		}
	}
	if err == nil && flags&checkpointSummaryTruncate != 0 {
		if s.TruncateIndex, err = readCanonicalUvarint(reader); err == nil {
			s.TruncateTerm, err = readCanonicalUvarint(reader)
		}
	}
	if err == nil && flags&checkpointSummaryCheckpoint != 0 {
		_, err = io.ReadFull(reader, s.Checkpoint.ID[:])
		if err == nil {
			s.Checkpoint.Index, err = readCanonicalUvarint(reader)
		}
		if err == nil {
			s.Checkpoint.Term, err = readCanonicalUvarint(reader)
		}
	}
	if err == nil && flags&checkpointSummaryReady != 0 {
		if s.NodeIncarnation, err = readCanonicalUvarint(reader); err == nil {
			s.ReadyID, err = readCanonicalUvarint(reader)
		}
		if err == nil {
			_, err = io.ReadFull(reader, s.ReadyDigest[:])
		}
		if err == nil {
			_, err = io.ReadFull(reader, s.ReadyWaveID[:])
		}
	}
	if err == nil && flags&checkpointSummaryWave != 0 {
		_, err = io.ReadFull(reader, s.LatestWaveID[:])
		if err == nil {
			_, err = io.ReadFull(reader, s.LatestWaveDigest[:])
		}
		if err == nil {
			s.LatestWaveSequence, err = readCanonicalUvarint(reader)
		}
	}
	if err != nil || (s.TruncateIndex == 0) != (s.TruncateTerm == 0) || flags&checkpointSummaryCheckpoint != 0 && (s.Checkpoint.ID == ([16]byte{}) || s.Checkpoint.Index == 0 || s.Checkpoint.Term == 0) || flags&checkpointSummaryReady != 0 && s.NodeIncarnation == 0 || flags&checkpointSummaryWave != 0 && (s.LatestWaveID == (WaveID{}) || s.LatestWaveDigest == ([32]byte{}) || s.LatestWaveSequence == 0) {
		return checkpointGroupSummary{}, ErrCorrupt
	}
	return group, nil
}
