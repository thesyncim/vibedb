package requestledger

import (
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	PrincipalQuotaStoragePrefix byte = systemkey.RequestLedgerFirst + 3
	PrincipalQuotaKeyBytes           = 1 + 32 + 16
	PrincipalQuotaRecordBytes        = 132
)

var principalQuotaMagic = [4]byte{'V', 'R', 'L', 'O'}

type PrincipalQuotaRecord struct {
	TenantDigest                                                Digest
	Principal                                                   PrincipalID
	Revision                                                    uint64
	ResidentBytes, ReservedBytes, TombstoneBytes, PlanningBytes uint64
	RequestCount, TombstoneCount, PlanningCount                 uint64
}

func AppendPrincipalQuotaKey(dst []byte, tenant Digest, principal PrincipalID) []byte {
	dst = append(dst, PrincipalQuotaStoragePrefix)
	dst = append(dst, tenant[:]...)
	return append(dst, principal[:]...)
}
func OpenPrincipalQuotaKey(raw []byte) (tenant Digest, principal PrincipalID, err error) {
	if len(raw) != PrincipalQuotaKeyBytes || raw[0] != PrincipalQuotaStoragePrefix {
		return tenant, principal, ErrCorrupt
	}
	copy(tenant[:], raw[1:33])
	copy(principal[:], raw[33:49])
	if !nonzeroDigest(tenant) || principal == (PrincipalID{}) {
		return Digest{}, PrincipalID{}, ErrCorrupt
	}
	return tenant, principal, nil
}
func AppendPrincipalQuota(dst []byte, r PrincipalQuotaRecord) ([]byte, error) {
	if err := validatePrincipalQuota(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, PrincipalQuotaRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], principalQuotaMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], r.Revision)
	putDigest(out[16:48], r.TenantDigest)
	copy(out[48:64], r.Principal[:])
	values := []uint64{r.ResidentBytes, r.ReservedBytes, r.TombstoneBytes, r.PlanningBytes, r.RequestCount, r.TombstoneCount, r.PlanningCount}
	at := 64
	for _, v := range values {
		binary.LittleEndian.PutUint64(out[at:at+8], v)
		at += 8
	}
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenPrincipalQuota(raw []byte) (PrincipalQuotaRecord, error) {
	if len(raw) != PrincipalQuotaRecordBytes || !magicOK(raw, principalQuotaMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[120:128]) || !checksumOK(raw) {
		return PrincipalQuotaRecord{}, ErrCorrupt
	}
	r := PrincipalQuotaRecord{Revision: binary.LittleEndian.Uint64(raw[8:16]), TenantDigest: readDigest(raw[16:48]), ResidentBytes: binary.LittleEndian.Uint64(raw[64:72]), ReservedBytes: binary.LittleEndian.Uint64(raw[72:80]), TombstoneBytes: binary.LittleEndian.Uint64(raw[80:88]), PlanningBytes: binary.LittleEndian.Uint64(raw[88:96]), RequestCount: binary.LittleEndian.Uint64(raw[96:104]), TombstoneCount: binary.LittleEndian.Uint64(raw[104:112]), PlanningCount: binary.LittleEndian.Uint64(raw[112:120])}
	copy(r.Principal[:], raw[48:64])
	if err := validatePrincipalQuota(r); err != nil {
		return PrincipalQuotaRecord{}, ErrCorrupt
	}
	return r, nil
}
func validatePrincipalQuota(r PrincipalQuotaRecord) error {
	if !nonzeroDigest(r.TenantDigest) || r.Principal == (PrincipalID{}) || r.Revision == 0 || r.TombstoneBytes > r.ResidentBytes || r.PlanningBytes > r.ResidentBytes || r.TombstoneCount > r.RequestCount || r.PlanningCount > r.RequestCount {
		return ErrCorrupt
	}
	if _, err := checkedSum(r.ResidentBytes, r.ReservedBytes); err != nil {
		return err
	}
	return nil
}
func AdvancePrincipalQuota(r PrincipalQuotaRecord, revision uint64, residentDelta, reservedDelta, planningDelta, tombstoneDelta int64, requestDelta, planningCountDelta, tombstoneCountDelta int64) (PrincipalQuotaRecord, error) {
	if err := validatePrincipalQuota(r); err != nil || !nextRevision(r.Revision, revision) {
		return PrincipalQuotaRecord{}, ErrInvalidState
	}
	var err error
	r.ResidentBytes, err = applyCounterDelta(r.ResidentBytes, residentDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.ReservedBytes, err = applyCounterDelta(r.ReservedBytes, reservedDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.PlanningBytes, err = applyCounterDelta(r.PlanningBytes, planningDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.TombstoneBytes, err = applyCounterDelta(r.TombstoneBytes, tombstoneDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.RequestCount, err = applyCounterDelta(r.RequestCount, requestDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.PlanningCount, err = applyCounterDelta(r.PlanningCount, planningCountDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.TombstoneCount, err = applyCounterDelta(r.TombstoneCount, tombstoneCountDelta)
	if err != nil {
		return PrincipalQuotaRecord{}, err
	}
	r.Revision = revision
	return r, validatePrincipalQuota(r)
}
func applyCounterDelta(value uint64, delta int64) (uint64, error) {
	if delta >= 0 {
		add := uint64(delta)
		if value > ^uint64(0)-add {
			return 0, ErrTooLarge
		}
		return value + add, nil
	}
	subtract := uint64(-(delta + 1)) + 1
	if subtract > value {
		return 0, ErrInvalidState
	}
	return value - subtract, nil
}
