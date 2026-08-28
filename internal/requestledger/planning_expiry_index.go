package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

var planningExpiryIndexDigestDomain = []byte("vibedb/request-ledger/planning-expiry-index\x00")

const (
	PlanningExpiryStoragePrefix byte = systemkey.RequestLedgerFirst + 2
	PlanningExpiryKeyBytes           = 1 + 8 + 32 + 32
	PlanningExpiryRecordBytes        = 132
)

var planningExpiryIndexMagic = [4]byte{'V', 'R', 'L', 'I'}

type PlanningExpiryIndexRecord struct {
	ExpiryAppliedIndex, LeaseGeneration, BuildGeneration uint64
	Home                                                 LedgerHome
	KeyDigest, PlanBuildID                               Digest
}

func NewPlanningExpiryIndexRecord(head HeadRecord) (PlanningExpiryIndexRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhasePlanning {
		return PlanningExpiryIndexRecord{}, ErrInvalidState
	}
	home, err := Home(head.Key)
	if err != nil {
		return PlanningExpiryIndexRecord{}, err
	}
	return PlanningExpiryIndexRecord{head.PlanningLeaseExpiryIndex, head.PlanningLeaseGeneration,
		head.PlanBuildGeneration, home, head.KeyDigest, head.PlanBuildID}, nil
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
	binary.LittleEndian.PutUint64(out[24:32], r.BuildGeneration)
	copy(out[32:64], r.Home[:])
	putDigest(out[64:96], r.KeyDigest)
	putDigest(out[96:128], r.PlanBuildID)
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenPlanningExpiryIndex(raw []byte) (PlanningExpiryIndexRecord, error) {
	if len(raw) != PlanningExpiryRecordBytes || !magicOK(raw, planningExpiryIndexMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PlanningExpiryIndexRecord{}, ErrCorrupt
	}
	r := PlanningExpiryIndexRecord{ExpiryAppliedIndex: binary.LittleEndian.Uint64(raw[8:16]),
		LeaseGeneration: binary.LittleEndian.Uint64(raw[16:24]),
		BuildGeneration: binary.LittleEndian.Uint64(raw[24:32]),
		KeyDigest:       readDigest(raw[64:96]), PlanBuildID: readDigest(raw[96:128])}
	copy(r.Home[:], raw[32:64])
	if err := validatePlanningExpiryIndex(r); err != nil {
		return PlanningExpiryIndexRecord{}, ErrCorrupt
	}
	return r, nil
}
func validatePlanningExpiryIndex(r PlanningExpiryIndexRecord) error {
	if r.ExpiryAppliedIndex == 0 || r.LeaseGeneration == 0 || r.BuildGeneration == 0 ||
		r.Home == (LedgerHome{}) || !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.PlanBuildID) {
		return ErrCorrupt
	}
	return nil
}

func ValidatePlanningExpiryIndex(head HeadRecord, record PlanningExpiryIndexRecord) error {
	expected, err := NewPlanningExpiryIndexRecord(head)
	if err != nil || expected != record {
		return ErrInvalidState
	}
	return nil
}

// PlanningExpiryIndexDigest is the constant-size scanner witness for the
// secondary expiry ordering. It binds the logical tuple, not encoded padding.
func PlanningExpiryIndexDigest(record PlanningExpiryIndexRecord) Digest {
	var framed [len("vibedb/request-ledger/planning-expiry-index\x00") + 24 + 3*32]byte
	at := copy(framed[:], planningExpiryIndexDigestDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], record.ExpiryAppliedIndex)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.LeaseGeneration)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], record.BuildGeneration)
	at += 24
	at += copy(framed[at:], record.Home[:])
	at += copy(framed[at:], record.KeyDigest[:])
	copy(framed[at:], record.PlanBuildID[:])
	return Digest(sha256.Sum256(framed[:]))
}
