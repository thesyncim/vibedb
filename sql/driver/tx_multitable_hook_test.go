package driver

import (
	"testing"
	_ "unsafe"

	"github.com/thesyncim/vibedb/store/durable"
)

//go:linkname durableDatabaseTxnAfterMintHook github.com/thesyncim/vibedb/store/durable.databaseTxnAfterMintHook
var durableDatabaseTxnAfterMintHook func(l *durable.TxnLog)

func setDatabaseTxnAfterMintHook(t *testing.T, hook func(*durable.TxnLog)) {
	t.Helper()
	previous := durableDatabaseTxnAfterMintHook
	durableDatabaseTxnAfterMintHook = hook
	t.Cleanup(func() { durableDatabaseTxnAfterMintHook = previous })
}
