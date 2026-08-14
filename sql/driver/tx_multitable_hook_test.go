package driver

import (
	"testing"
	_ "unsafe"

	"github.com/thesyncim/vibedb/store/durable"
)

//go:linkname durableDatabaseTxnAfterMintHook github.com/thesyncim/vibedb/store/durable.databaseTxnAfterMintHook
var durableDatabaseTxnAfterMintHook func(l *durable.TxnLog)

//go:linkname durableDatabaseTxnBeforeMarkerRecycleHook github.com/thesyncim/vibedb/store/durable.databaseTxnBeforeMarkerRecycleHook
var durableDatabaseTxnBeforeMarkerRecycleHook func(l *durable.TxnLog)

func setDatabaseTxnAfterMintHook(t *testing.T, hook func(*durable.TxnLog)) {
	t.Helper()
	previous := durableDatabaseTxnAfterMintHook
	durableDatabaseTxnAfterMintHook = hook
	t.Cleanup(func() { durableDatabaseTxnAfterMintHook = previous })
}

func setDatabaseTxnBeforeMarkerRecycleHook(
	t *testing.T, hook func(*durable.TxnLog),
) {
	t.Helper()
	previous := durableDatabaseTxnBeforeMarkerRecycleHook
	durableDatabaseTxnBeforeMarkerRecycleHook = hook
	t.Cleanup(func() { durableDatabaseTxnBeforeMarkerRecycleHook = previous })
}
