package storeio

import (
	"encoding/binary"
	"io"
	"os"
)

const recoveryReadWindowBytes = 64 << 10

// recoveryRecordStream bounds recovery scratch by the current record, not the
// preallocated journal capacity. Small records share a read-ahead window. Its
// bytes remain borrowed until the next record, as required by RecoveryRecord.
// This buffer is separate from append scratch: replay callbacks cannot overwrite
// the record they are consuming by using the journal's append workspace.
type recoveryRecordStream struct {
	file     *os.File
	capacity uint64
	start    uint64
	buffer   []byte
	prefix   [RecoveryJournalRecordPrefixSize + RecoveryJournalRecordTrailerSize]byte
}

func (s *recoveryRecordStream) open(file *os.File, capacity uint64) error {
	// Preserve the full-region reader's refusal of a physically truncated file,
	// including an unused truncated suffix, before exposing any replay records.
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if capacity > RecoveryJournalMaxCapacityBytes ||
		info.Size() < int64(recoveryJournalRegionStart)+int64(capacity) {
		return io.ErrUnexpectedEOF
	}
	s.file, s.capacity = file, capacity
	return nil
}

func (s *recoveryRecordStream) record(cursor uint64, sector uint32, sequence uint64) (RecoveryRecord, int, error) {
	remaining := s.capacity - cursor
	var prefix []byte
	if cursor >= s.start && cursor-s.start <= uint64(len(s.buffer)) {
		prefix = s.buffer[int(cursor-s.start):]
	}
	minimum := min(uint64(len(s.prefix)), remaining)
	if uint64(len(prefix)) < minimum {
		prefix = s.prefix[:int(minimum)]
		if _, err := readFullAt(s.file, prefix, int64(recoveryJournalRegionStart)+int64(cursor)); err != nil {
			return RecoveryRecord{}, 0, err
		}
	}
	needed := minimum
	if len(prefix) >= RecoveryJournalRecordPrefixSize {
		// The decoder authenticates BOTH current layouts before classifying an
		// unknown/corrupt kind or magic as a truncatable tail. Read enough for
		// either CRC; selecting a size from the untrusted kind would lose that
		// fail-closed distinction. Batch count occupies standalone keyLen's word.
		needed = min(remaining, uint64(len(s.prefix))+
			uint64(binary.LittleEndian.Uint32(prefix[24:28]))+
			uint64(binary.LittleEndian.Uint32(prefix[28:32])))
	}
	if uint64(len(prefix)) >= needed {
		return DecodeRecoveryRecord(prefix, sector, sequence)
	}
	if err := s.fill(cursor, int(needed)); err != nil {
		return RecoveryRecord{}, 0, err
	}
	return DecodeRecoveryRecord(s.buffer, sector, sequence)
}

func (s *recoveryRecordStream) fill(cursor uint64, needed int) error {
	var retained []byte
	if cursor >= s.start && cursor-s.start <= uint64(len(s.buffer)) {
		retained = s.buffer[int(cursor-s.start):]
	}
	if cap(s.buffer) < needed {
		next := make([]byte, min(int(s.capacity), max(recoveryReadWindowBytes, needed)))
		copy(next, retained)
		s.buffer = next[:len(retained)]
	} else {
		n := copy(s.buffer[:cap(s.buffer)], retained)
		s.buffer = s.buffer[:n]
	}
	s.start = cursor
	end := min(cap(s.buffer), int(s.capacity-cursor))
	previous := len(s.buffer)
	s.buffer = s.buffer[:end]
	_, err := readFullAt(s.file, s.buffer[previous:], int64(recoveryJournalRegionStart)+int64(cursor)+int64(previous))
	return err
}
