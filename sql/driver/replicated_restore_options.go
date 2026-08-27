package driver

import (
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// The ledger budget/range participates in the replicated apply contract, so
// snapshot preparation and every recovery constructor must retain it exactly.
func replicatedSnapshotLedgerOptions(identity ReplicatedApplyIdentity, options replicatedstate.Options) replicatedstate.Options {
	options.RequestLedgerCapacityBytes = identity.RequestLedgerCapacityBytes
	options.RequestLedgerCleanupReserveBytes = identity.RequestLedgerCleanupReserveBytes
	options.RequestLedgerRange = replicatedstate.RequestLedgerRange{
		Start:    requestledger.LedgerHome(identity.RequestLedgerRangeStart),
		End:      requestledger.LedgerHome(identity.RequestLedgerRangeEnd),
		Identity: requestledger.Digest(identity.RequestLedgerRangeIdentity),
	}
	return options
}
