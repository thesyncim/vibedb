package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// InstallCollectionJournalFaultForSessionLifecycleTest wraps one exact live
// target instead of relying on the global journal-open hook (which cannot
// distinguish system, user, and capture files). It is test-archive-only.
func InstallCollectionJournalFaultForSessionLifecycleTest(
	collection *Collection,
	plan storeio.JournalFaultPlan,
) *storeio.FaultJournal {
	if collection == nil || collection.journal == nil {
		return nil
	}
	fault := storeio.NewFaultJournal(collection.journal)
	fault.Program(plan)
	return fault
}

// InstallTxnMarkerFaultForSessionLifecycleTest wraps an already-minted marker
// for the external replicated-state crash test. The symbol exists only in the
// durable package's test archive; production callers and binaries cannot see
// it.
func InstallTxnMarkerFaultForSessionLifecycleTest(
	log *TxnLog,
	plan storeio.TxnMarkerFaultPlan,
) *storeio.FaultTxnMarker {
	if log == nil || log.marker == nil {
		return nil
	}
	fault := storeio.NewFaultTxnMarker(log.marker)
	fault.Program(plan)
	return fault
}
