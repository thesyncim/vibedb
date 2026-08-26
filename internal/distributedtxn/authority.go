package distributedtxn

import "encoding/binary"

// ValidateReplicatedCoordinatorAuthorityWitnesses validates an inline or
// segmented fused coordinator start and requires a nonzero authority witness
// for every participant it contains. The common VTC1/VTM1 decoders continue to
// permit zero for the static non-RF3 path; replicated serving calls this stricter
// allocation-free boundary before persisting any coordinator bytes.
func ValidateReplicatedCoordinatorAuthorityWitnesses(payload []byte) error {
	if len(payload) < 4 {
		return ErrCorrupt
	}
	if equal4(payload[:4], coordinatorMagic) {
		var scratch [MaxInlineParticipants]ParticipantRef
		record, err := OpenCoordinatorInto(payload, scratch[:])
		if err != nil || !canonicalCoordinatorBytes(payload) {
			return ErrCorrupt
		}
		for index := range record.Participants {
			if record.Participants[index].AuthorityWitness == (AuthorityWitness{}) {
				return ErrCorrupt
			}
		}
		return nil
	}
	if !equal4(payload[:4], manifestCoordinatorMagic) {
		return ErrCorrupt
	}
	_, segments, err := OpenReplicatedManifestStart(payload)
	if err != nil {
		return err
	}
	return segments.ValidateAuthorityWitnesses()
}

// ValidateAuthorityWitnesses requires a nonzero authority witness on every
// participant in an already validated direct VTM1 sequence. It performs no
// allocation and does not impose a participant-count limit.
func (s ManifestSegmentSequence) ValidateAuthorityWitnesses() error {
	if len(s.raw) == 0 || s.count == 0 {
		return ErrCorrupt
	}
	iterator := s.Iterator()
	seen := 0
	for iterator.Next() {
		if !manifestSegmentAuthorityWitnessesPresent(iterator.Segment().Raw) {
			return ErrCorrupt
		}
		seen++
	}
	if seen != int(s.count) {
		return ErrCorrupt
	}
	return nil
}

// manifestSegmentAuthorityWitnessesPresent accepts only a page already opened
// by the canonical sequence decoder. Reusing its exact self-delimiting entry
// lengths avoids reconstructing identities or allocating participant scratch.
func manifestSegmentAuthorityWitnessesPresent(raw []byte) bool {
	if len(raw) < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 {
		return false
	}
	count := int(binary.LittleEndian.Uint32(raw[12:16]))
	cursor, end := manifestSegmentHeaderBytes, len(raw)-4
	for range count {
		if end-cursor < manifestEntryFixedBytes {
			return false
		}
		entry := raw[cursor:]
		var witness AuthorityWitness
		copy(witness[:], entry[64:80])
		if witness == (AuthorityWitness{}) {
			return false
		}
		cursor += manifestEntryFixedBytes + int(entry[1]) + int(entry[3])
		if cursor > end {
			return false
		}
	}
	return cursor == end
}
