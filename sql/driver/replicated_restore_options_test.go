package driver

import (
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	"testing"
)

func TestReplicatedSnapshotLedgerOptionsPreserveExactContract(t *testing.T) {
	identity := ReplicatedApplyIdentity{RequestLedgerCapacityBytes: 64 << 20, RequestLedgerCleanupReserveBytes: 8 << 20, RequestLedgerRangeStart: [32]byte{1}, RequestLedgerRangeEnd: [32]byte{2}, RequestLedgerRangeIdentity: [32]byte{3}}
	base := replicatedstate.Options{TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20}, MaxSessions: 32, RetryWindow: 8}
	got := replicatedSnapshotLedgerOptions(identity, base)
	if got.RequestLedgerCapacityBytes != identity.RequestLedgerCapacityBytes || got.RequestLedgerCleanupReserveBytes != identity.RequestLedgerCleanupReserveBytes || [32]byte(got.RequestLedgerRange.Start) != identity.RequestLedgerRangeStart || [32]byte(got.RequestLedgerRange.End) != identity.RequestLedgerRangeEnd || [32]byte(got.RequestLedgerRange.Identity) != identity.RequestLedgerRangeIdentity || got.TxnLimits != base.TxnLimits || got.MaxSessions != base.MaxSessions || got.RetryWindow != base.RetryWindow {
		t.Fatalf("contract changed: %+v", got)
	}
	zero := replicatedSnapshotLedgerOptions(ReplicatedApplyIdentity{}, got)
	if zero.RequestLedgerCapacityBytes != 0 || zero.RequestLedgerCleanupReserveBytes != 0 || zero.RequestLedgerRange != (replicatedstate.RequestLedgerRange{}) {
		t.Fatal("disabled identity retained stale ledger contract")
	}
}
