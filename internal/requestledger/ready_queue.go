package requestledger

import (
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	ReadyStoragePrefix   byte = systemkey.RequestLedgerFirst + 1
	ReadyStorageKeyBytes      = 1 + 32 + 1 + 32
	ReadyRecordBytes          = 260
)

var readyMagic = [4]byte{'V', 'R', 'L', 'Q'}

type Readiness uint8

const (
	ReadinessInvalid Readiness = iota
	ReadinessDeriveWave
	ReadinessDispatchPending
	ReadinessPlanningExpiry
	ReadinessRestartPlanning
	ReadinessPinAcquiring
	ReadinessPinRelease
	ReadinessDynamicBuild
	ReadinessPayloadCleanup
	ReadinessTerminalPrepared
	ReadinessComplete

	LastReadiness = ReadinessComplete
)

type ReadyRecord struct {
	Home                                           LedgerHome
	KeyDigest, RequestDigest, PlanRoot             Digest
	ContinuationDigest, PendingDigest, PlanBuildID Digest
	Revision, NextStepOrdinal                      uint64
	Readiness                                      Readiness
}

func NewReadyWorkRecord(head HeadRecord, readiness Readiness, workDigest Digest) (ReadyRecord, error) {
	if err := validateHead(head); err != nil || readiness < ReadinessPlanningExpiry ||
		readiness > LastReadiness || !nonzeroDigest(workDigest) {
		return ReadyRecord{}, ErrInvalidState
	}
	home, err := Home(head.Key)
	if err != nil {
		return ReadyRecord{}, err
	}
	r := ReadyRecord{Home: home, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, ContinuationDigest: head.ContinuationDigest, PendingDigest: workDigest,
		PlanBuildID: head.PlanBuildID, Revision: head.Revision, NextStepOrdinal: head.NextStepOrdinal,
		Readiness: readiness}
	return r, validateReady(r)
}

func NewReadyRecord(head HeadRecord, pending *PendingWaveRecord) (ReadyRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhaseSealed {
		return ReadyRecord{}, ErrInvalidState
	}
	home, err := Home(head.Key)
	if err != nil {
		return ReadyRecord{}, err
	}
	r := ReadyRecord{Home: home, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot, ContinuationDigest: head.ContinuationDigest, PlanBuildID: head.PlanBuildID, Revision: head.Revision, NextStepOrdinal: head.NextStepOrdinal, Readiness: ReadinessDeriveWave}
	if pending != nil {
		if err := validatePendingWave(*pending, head.TotalPlanBytes); err != nil || pending.KeyDigest != head.KeyDigest || pending.Revision != head.Revision {
			return ReadyRecord{}, ErrInvalidState
		}
		r.Readiness = ReadinessDispatchPending
		r.PendingDigest = pending.WaveDigest
	}
	return r, validateReady(r)
}

func AppendReadyKey(dst []byte, home LedgerHome, readiness Readiness, key Digest) []byte {
	dst = append(dst, ReadyStoragePrefix)
	dst = append(dst, home[:]...)
	dst = append(dst, byte(readiness))
	return append(dst, key[:]...)
}
func OpenReadyKey(raw []byte) (home LedgerHome, readiness Readiness, key Digest, err error) {
	if len(raw) != ReadyStorageKeyBytes || raw[0] != ReadyStoragePrefix {
		return home, 0, key, ErrCorrupt
	}
	copy(home[:], raw[1:33])
	readiness = Readiness(raw[33])
	copy(key[:], raw[34:66])
	if home == (LedgerHome{}) || !nonzeroDigest(key) || readiness < ReadinessDeriveWave || readiness > LastReadiness {
		return LedgerHome{}, 0, Digest{}, ErrCorrupt
	}
	return home, readiness, key, nil
}

func AppendReady(dst []byte, r ReadyRecord) ([]byte, error) {
	if err := validateReady(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, ReadyRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], readyMagic[:])
	out[8] = byte(r.Readiness)
	binary.LittleEndian.PutUint64(out[16:24], r.Revision)
	binary.LittleEndian.PutUint64(out[24:32], r.NextStepOrdinal)
	copy(out[32:64], r.Home[:])
	putDigest(out[64:96], r.KeyDigest)
	putDigest(out[96:128], r.RequestDigest)
	putDigest(out[128:160], r.PlanRoot)
	putDigest(out[160:192], r.ContinuationDigest)
	putDigest(out[192:224], r.PendingDigest)
	putDigest(out[224:256], r.PlanBuildID)
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenReady(raw []byte) (ReadyRecord, error) {
	if len(raw) != ReadyRecordBytes || !magicOK(raw, readyMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[9:16]) || !checksumOK(raw) {
		return ReadyRecord{}, ErrCorrupt
	}
	r := ReadyRecord{Readiness: Readiness(raw[8]), Revision: binary.LittleEndian.Uint64(raw[16:24]), NextStepOrdinal: binary.LittleEndian.Uint64(raw[24:32]), KeyDigest: readDigest(raw[64:96]), RequestDigest: readDigest(raw[96:128]), PlanRoot: readDigest(raw[128:160]), ContinuationDigest: readDigest(raw[160:192]), PendingDigest: readDigest(raw[192:224]), PlanBuildID: readDigest(raw[224:256])}
	copy(r.Home[:], raw[32:64])
	if err := validateReady(r); err != nil {
		return ReadyRecord{}, ErrCorrupt
	}
	return r, nil
}
func validateReady(r ReadyRecord) error {
	if r.Home == (LedgerHome{}) || !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.RequestDigest) || !nonzeroDigest(r.PlanRoot) || !nonzeroDigest(r.PlanBuildID) || r.Revision == 0 || (r.NextStepOrdinal == 0) != !nonzeroDigest(r.ContinuationDigest) || r.Readiness < ReadinessDeriveWave || r.Readiness > LastReadiness || (r.Readiness == ReadinessDeriveWave && nonzeroDigest(r.PendingDigest)) || (r.Readiness != ReadinessDeriveWave && !nonzeroDigest(r.PendingDigest)) {
		return ErrCorrupt
	}
	return nil
}
