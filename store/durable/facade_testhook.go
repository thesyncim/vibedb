package durable

import "github.com/thesyncim/vibedb/internal/storeio"

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
