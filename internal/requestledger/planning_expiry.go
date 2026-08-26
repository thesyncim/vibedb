package requestledger

import "encoding/binary"

const PlanningExpiryRequestBytes = 104

var planningExpiryMagic = [4]byte{'V', 'R', 'L', 'E'}

type PlanningExpiryRequest struct {
	ObservedAppliedIndex uint64
	LeaseGeneration      uint64
	BuildGeneration      uint64
	PlanBuildID          Digest
	KeyDigest            Digest
}

func NewPlanningExpiryRequest(head HeadRecord, observedAppliedIndex uint64) (PlanningExpiryRequest, error) {
	r := PlanningExpiryRequest{ObservedAppliedIndex: observedAppliedIndex, LeaseGeneration: head.PlanningLeaseGeneration,
		BuildGeneration: head.PlanBuildGeneration, PlanBuildID: head.PlanBuildID, KeyDigest: head.KeyDigest}
	if err := ValidatePlanningExpiry(head, r); err != nil {
		return PlanningExpiryRequest{}, err
	}
	return r, nil
}

func ValidatePlanningExpiry(head HeadRecord, request PlanningExpiryRequest) error {
	if err := validateHead(head); err != nil || head.Phase != PhasePlanning || request.ObservedAppliedIndex < head.PlanningLeaseExpiryIndex ||
		request.LeaseGeneration != head.PlanningLeaseGeneration ||
		request.BuildGeneration != head.PlanBuildGeneration ||
		request.PlanBuildID != head.PlanBuildID || request.KeyDigest != head.KeyDigest {
		return ErrInvalidState
	}
	return nil
}

// MarkPlanningExpired fences the current build without deleting the request.
// Immutable page rows remain authorized only for bounded cleanup; restart is
// impossible until that cleanup completes and advances the build generation.
func MarkPlanningExpired(head HeadRecord, request PlanningExpiryRequest, revision uint64) (HeadRecord, error) {
	if err := ValidatePlanningExpiry(head, request); err != nil ||
		!nextRevision(head.Revision, revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Phase = PhaseExpired
	head.Revision = revision
	return head, validateHead(head)
}

func AppendPlanningExpiryRequest(dst []byte, r PlanningExpiryRequest) ([]byte, error) {
	if r.ObservedAppliedIndex == 0 || r.LeaseGeneration == 0 || r.BuildGeneration == 0 ||
		!nonzeroDigest(r.PlanBuildID) || !nonzeroDigest(r.KeyDigest) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, PlanningExpiryRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], planningExpiryMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], r.ObservedAppliedIndex)
	binary.LittleEndian.PutUint64(out[16:24], r.LeaseGeneration)
	binary.LittleEndian.PutUint64(out[24:32], r.BuildGeneration)
	putDigest(out[32:64], r.PlanBuildID)
	putDigest(out[64:96], r.KeyDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanningExpiryRequest(raw []byte) (PlanningExpiryRequest, error) {
	if len(raw) != PlanningExpiryRequestBytes || !magicOK(raw, planningExpiryMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[96:100]) || !checksumOK(raw) {
		return PlanningExpiryRequest{}, ErrCorrupt
	}
	r := PlanningExpiryRequest{ObservedAppliedIndex: binary.LittleEndian.Uint64(raw[8:16]),
		LeaseGeneration: binary.LittleEndian.Uint64(raw[16:24]),
		BuildGeneration: binary.LittleEndian.Uint64(raw[24:32]),
		PlanBuildID:     readDigest(raw[32:64]), KeyDigest: readDigest(raw[64:96])}
	if r.ObservedAppliedIndex == 0 || r.LeaseGeneration == 0 || r.BuildGeneration == 0 ||
		!nonzeroDigest(r.PlanBuildID) || !nonzeroDigest(r.KeyDigest) {
		return PlanningExpiryRequest{}, ErrCorrupt
	}
	return r, nil
}
