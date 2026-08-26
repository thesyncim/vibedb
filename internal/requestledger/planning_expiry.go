package requestledger

import "encoding/binary"

const PlanningExpiryRequestBytes = 96

var planningExpiryMagic = [4]byte{'V', 'R', 'L', 'E'}

type PlanningExpiryRequest struct {
	ObservedAppliedIndex uint64
	LeaseGeneration      uint64
	PlanBuildID          Digest
	KeyDigest            Digest
}

func NewPlanningExpiryRequest(head HeadRecord, observedAppliedIndex uint64) (PlanningExpiryRequest, error) {
	r := PlanningExpiryRequest{ObservedAppliedIndex: observedAppliedIndex, LeaseGeneration: head.PlanningLeaseGeneration,
		PlanBuildID: head.PlanBuildID, KeyDigest: head.KeyDigest}
	if err := ValidatePlanningExpiry(head, r); err != nil {
		return PlanningExpiryRequest{}, err
	}
	return r, nil
}

func ValidatePlanningExpiry(head HeadRecord, request PlanningExpiryRequest) error {
	if err := validateHead(head); err != nil || head.Phase != PhasePlanning || request.ObservedAppliedIndex < head.PlanningLeaseExpiryIndex ||
		request.LeaseGeneration != head.PlanningLeaseGeneration || request.PlanBuildID != head.PlanBuildID || request.KeyDigest != head.KeyDigest {
		return ErrInvalidState
	}
	return nil
}

func AppendPlanningExpiryRequest(dst []byte, r PlanningExpiryRequest) ([]byte, error) {
	if r.ObservedAppliedIndex == 0 || r.LeaseGeneration == 0 || !nonzeroDigest(r.PlanBuildID) || !nonzeroDigest(r.KeyDigest) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, PlanningExpiryRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], planningExpiryMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], r.ObservedAppliedIndex)
	binary.LittleEndian.PutUint64(out[16:24], r.LeaseGeneration)
	putDigest(out[24:56], r.PlanBuildID)
	putDigest(out[56:88], r.KeyDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanningExpiryRequest(raw []byte) (PlanningExpiryRequest, error) {
	if len(raw) != PlanningExpiryRequestBytes || !magicOK(raw, planningExpiryMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[88:92]) || !checksumOK(raw) {
		return PlanningExpiryRequest{}, ErrCorrupt
	}
	r := PlanningExpiryRequest{ObservedAppliedIndex: binary.LittleEndian.Uint64(raw[8:16]), LeaseGeneration: binary.LittleEndian.Uint64(raw[16:24]), PlanBuildID: readDigest(raw[24:56]), KeyDigest: readDigest(raw[56:88])}
	if r.ObservedAppliedIndex == 0 || r.LeaseGeneration == 0 || !nonzeroDigest(r.PlanBuildID) || !nonzeroDigest(r.KeyDigest) {
		return PlanningExpiryRequest{}, ErrCorrupt
	}
	return r, nil
}
