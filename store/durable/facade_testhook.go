package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// CheckpointGroupFaultPhaseForFacadeTest names one post-I/O crash seam. It is
// exported only for cross-package recovery qualification; production code must
// never install a fault hook.
type CheckpointGroupFaultPhaseForFacadeTest uint8

const (
	CheckpointGroupFaultAfterPrepareAppendForFacadeTest CheckpointGroupFaultPhaseForFacadeTest = iota + 1
	CheckpointGroupFaultAfterDecisionAppendForFacadeTest
	CheckpointGroupFaultAfterJournalSyncForFacadeTest
	CheckpointGroupFaultAfterMarkerSyncForFacadeTest
	CheckpointGroupFaultAfterCertificateWriteForFacadeTest
	CheckpointGroupFaultAfterCertificateSyncForFacadeTest
	CheckpointGroupFaultAfterPhysicalCheckpointForFacadeTest
)

// InstallCheckpointGroupFaultForFacadeTest fails once after the selected
// durable phase. fired reports whether the exercised path reached that phase.
func InstallCheckpointGroupFaultForFacadeTest(
	phase CheckpointGroupFaultPhaseForFacadeTest,
) (fired func() bool, restore func()) {
	points := [...]checkpointGroupFaultPoint{
		CheckpointGroupFaultAfterPrepareAppendForFacadeTest:      checkpointGroupAfterPrepareAppend,
		CheckpointGroupFaultAfterDecisionAppendForFacadeTest:     checkpointGroupAfterDecisionAppend,
		CheckpointGroupFaultAfterJournalSyncForFacadeTest:        checkpointGroupAfterJournalSync,
		CheckpointGroupFaultAfterMarkerSyncForFacadeTest:         checkpointGroupAfterMarkerSync,
		CheckpointGroupFaultAfterCertificateWriteForFacadeTest:   checkpointGroupAfterCertificateWrite,
		CheckpointGroupFaultAfterCertificateSyncForFacadeTest:    checkpointGroupAfterCertificateSync,
		CheckpointGroupFaultAfterPhysicalCheckpointForFacadeTest: checkpointGroupAfterPhysicalCheckpoint,
	}
	if int(phase) >= len(points) || points[phase] == 0 {
		return func() bool { return false }, func() {}
	}
	previous := checkpointGroupFaultHook
	didFire := false
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if !didFire && point == points[phase] {
			didFire = true
			return ErrCommitOutcomeUnknown
		}
		return nil
	}
	return func() bool { return didFire }, func() { checkpointGroupFaultHook = previous }
}

// InstallTxnMarkerSyncFaultForFacadeTest programs the next decision-log mint so
// its first Sync fails with the unknown-outcome classification. Production
// callers leave the hook unset; this exists so the root facade regression can
// exercise catalog-scope poison without living inside package durable's tests.
func InstallTxnMarkerSyncFaultForFacadeTest() (restore func()) {
	previous := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		fm := storeio.NewFaultTxnMarker(l.marker)
		fm.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
		})
	}
	return func() { databaseTxnAfterMintHook = previous }
}

// InstallTxnMarkerAppendFaultForFacadeTest programs one decision append to
// fail with the conservative unknown-outcome classification. It is the
// zero-Sync CheckpointGroup counterpart of InstallTxnMarkerSyncFaultForFacadeTest:
// ordinary group transitions append their implementation decision without a
// marker Sync, so the append itself is the only per-transition marker I/O.
func InstallTxnMarkerAppendFaultForFacadeTest(appendIndex int) (restore func()) {
	previous := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		fm := storeio.NewFaultTxnMarker(l.marker)
		fm.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultAppendError, AppendIndex: appendIndex,
		})
	}
	return func() { databaseTxnAfterMintHook = previous }
}

// InstallCheckpointGroupDecisionAppendFaultForFacadeTest makes the next group
// transition report an unknown outcome after its decision append completes.
// Installing it immediately before the target transition avoids coupling a
// facade regression to unrelated marker appends during storage preparation.
func InstallCheckpointGroupDecisionAppendFaultForFacadeTest() (restore func()) {
	previous := checkpointGroupFaultHook
	fired := false
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if !fired && point == checkpointGroupAfterDecisionAppend {
			fired = true
			return ErrCommitOutcomeUnknown
		}
		return nil
	}
	return func() { checkpointGroupFaultHook = previous }
}

// InstallCheckpointGroupInitialCertificatePostRenameFaultForFacadeTest makes the next
// checkpoint-group creation lose its first directory barrier immediately after
// the initial certificate has been renamed into place. Creation must settle the
// exact official certificate before returning success.
func InstallCheckpointGroupInitialCertificatePostRenameFaultForFacadeTest() (restore func()) {
	previous := checkpointGroupFaultHook
	fired := false
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if !fired && point == checkpointGroupAfterCertificateRename {
			fired = true
			return ErrCommitOutcomeUnknown
		}
		return nil
	}
	return func() { checkpointGroupFaultHook = previous }
}
