package requestledger

import (
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
