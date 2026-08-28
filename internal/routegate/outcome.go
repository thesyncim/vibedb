package routegate

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// OutcomeBytes is the exact settlement-result size for every transition.
	OutcomeBytes = 124

	outcomeBodyBytes = OutcomeBytes - 4
)

var outcomeMagic = [4]byte{'V', 'R', 'G', 'R'}

// AppendOutcome appends one canonical fixed-size settlement result. Replicas
// can return these bytes directly through the committed-result sink.
func AppendOutcome(dst []byte, outcome Outcome) ([]byte, error) {
	if !validOutcome(outcome) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, OutcomeBytes)...)
	encoded := dst[start : start+OutcomeBytes]
	copy(encoded[:4], outcomeMagic[:])
	encoded[4] = byte(outcome.Reason)
	if outcome.Mutated {
		encoded[5] = 1
	}
	encoded[6] = byte(outcome.Status.Drain.State)
	binary.LittleEndian.PutUint64(encoded[8:16], outcome.Status.Revision)
	binary.LittleEndian.PutUint64(encoded[16:24], outcome.Status.Epoch)
	binary.LittleEndian.PutUint64(encoded[24:32], outcome.Status.ActivePins)
	binary.LittleEndian.PutUint64(encoded[32:40], outcome.Status.ReleasedPins)
	binary.LittleEndian.PutUint64(encoded[40:48], outcome.Status.RetainedRecords)
	binary.LittleEndian.PutUint64(encoded[48:56], outcome.Status.Drain.Epoch)
	copy(encoded[56:88], outcome.Status.Drain.Identity[:])
	copy(encoded[88:120], outcome.Status.Drain.Binding[:])
	binary.LittleEndian.PutUint32(
		encoded[outcomeBodyBytes:], crc32.Checksum(encoded[:outcomeBodyBytes], castagnoli),
	)
	return dst, nil
}

// OpenOutcome authenticates and opens exactly one canonical settlement result.
func OpenOutcome(raw []byte) (Outcome, error) {
	if len(raw) != OutcomeBytes || raw[0] != outcomeMagic[0] ||
		raw[1] != outcomeMagic[1] || raw[2] != outcomeMagic[2] ||
		raw[3] != outcomeMagic[3] || raw[5] > 1 || raw[7] != 0 ||
		binary.LittleEndian.Uint32(raw[outcomeBodyBytes:]) !=
			crc32.Checksum(raw[:outcomeBodyBytes], castagnoli) {
		return Outcome{}, ErrCorrupt
	}
	outcome := Outcome{
		Reason: Reason(raw[4]), Mutated: raw[5] == 1,
		Status: Status{
			Revision:        binary.LittleEndian.Uint64(raw[8:16]),
			Epoch:           binary.LittleEndian.Uint64(raw[16:24]),
			ActivePins:      binary.LittleEndian.Uint64(raw[24:32]),
			ReleasedPins:    binary.LittleEndian.Uint64(raw[32:40]),
			RetainedRecords: binary.LittleEndian.Uint64(raw[40:48]),
		},
	}
	outcome.Status.Drain.State = DrainState(raw[6])
	outcome.Status.Drain.Epoch = binary.LittleEndian.Uint64(raw[48:56])
	copy(outcome.Status.Drain.Identity[:], raw[56:88])
	copy(outcome.Status.Drain.Binding[:], raw[88:120])
	if !validOutcome(outcome) {
		return Outcome{}, ErrCorrupt
	}
	return outcome, nil
}

func validOutcome(outcome Outcome) bool {
	if outcome.Reason <= ReasonInvalid || outcome.Reason > ReasonExhausted ||
		outcome.Status.Epoch == 0 ||
		outcome.Status.ActivePins > outcome.Status.RetainedRecords ||
		outcome.Status.ReleasedPins !=
			outcome.Status.RetainedRecords-outcome.Status.ActivePins ||
		!validDrainSnapshot(
			outcome.Status.Drain, outcome.Status.Epoch, outcome.Status.ActivePins,
		) {
		return false
	}
	mutating := outcome.Reason == ReasonAcquired || outcome.Reason == ReasonReleased ||
		outcome.Reason == ReasonDrainPending || outcome.Reason == ReasonDrainAcquired ||
		outcome.Reason == ReasonDrainReleased || outcome.Reason == ReasonCompacted
	return outcome.Mutated == mutating
}
