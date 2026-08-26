package requestledger

import (
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	PlanningExpiryStoragePrefix byte = systemkey.RequestLedgerFirst + 2
	PlanningExpiryKeyBytes           = 1 + 8 + 32 + 32
	PlanningExpiryRecordBytes        = 124
)

var planningExpiryIndexMagic = [4]byte{'V', 'R', 'L', 'I'}

type PlanningExpiryIndexRecord struct {
	ExpiryAppliedIndex, LeaseGeneration uint64
	Home                                LedgerHome
	KeyDigest, PlanBuildID              Digest
}

func NewPlanningExpiryIndexRecord(head HeadRecord) (PlanningExpiryIndexRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhasePlanning {
		return PlanningExpiryIndexRecord{}, ErrInvalidState
	}
	home, err := Home(head.Key)
	if err != nil {
		return PlanningExpiryIndexRecord{}, err
	}
	return PlanningExpiryIndexRecord{head.PlanningLeaseExpiryIndex, head.PlanningLeaseGeneration, home, head.KeyDigest, head.PlanBuildID}, nil
}
func AppendPlanningExpiryKey(dst []byte, index uint64, home LedgerHome, key Digest) []byte {
	dst = append(dst, PlanningExpiryStoragePrefix)
	dst = binary.BigEndian.AppendUint64(dst, index)
	dst = append(dst, home[:]...)
	return append(dst, key[:]...)
}
func OpenPlanningExpiryKey(raw []byte) (index uint64, home LedgerHome, key Digest, err error) {
	if len(raw) != PlanningExpiryKeyBytes || raw[0] != PlanningExpiryStoragePrefix {
		return 0, home, key, ErrCorrupt
	}
	index = binary.BigEndian.Uint64(raw[1:9])
	copy(home[:], raw[9:41])
	copy(key[:], raw[41:73])
	if index == 0 || home == (LedgerHome{}) || !nonzeroDigest(key) {
		return 0, LedgerHome{}, Digest{}, ErrCorrupt
	}
	return index, home, key, nil
}
func AppendPlanningExpiryIndex(dst []byte, r PlanningExpiryIndexRecord) ([]byte, error) {
	if err := validatePlanningExpiryIndex(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, PlanningExpiryRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], planningExpiryIndexMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], r.ExpiryAppliedIndex)
	binary.LittleEndian.PutUint64(out[16:24], r.LeaseGeneration)
	copy(out[24:56], r.Home[:])
	putDigest(out[56:88], r.KeyDigest)
	putDigest(out[88:120], r.PlanBuildID)
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenPlanningExpiryIndex(raw []byte) (PlanningExpiryIndexRecord, error) {
	if len(raw) != PlanningExpiryRecordBytes || !magicOK(raw, planningExpiryIndexMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PlanningExpiryIndexRecord{}, ErrCorrupt
	}
	r := PlanningExpiryIndexRecord{ExpiryAppliedIndex: binary.LittleEndian.Uint64(raw[8:16]), LeaseGeneration: binary.LittleEndian.Uint64(raw[16:24]), KeyDigest: readDigest(raw[56:88]), PlanBuildID: readDigest(raw[88:120])}
	copy(r.Home[:], raw[24:56])
	if err := validatePlanningExpiryIndex(r); err != nil {
		return PlanningExpiryIndexRecord{}, ErrCorrupt
	}
	return r, nil
}
func validatePlanningExpiryIndex(r PlanningExpiryIndexRecord) error {
	if r.ExpiryAppliedIndex == 0 || r.LeaseGeneration == 0 || r.Home == (LedgerHome{}) || !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.PlanBuildID) {
		return ErrCorrupt
	}
	return nil
}
