package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// InstallJournalFaultForFacadeTest exposes the package-private journal seam to
// the external facade regression in this package's test binary. It is not part
// of the production API.
func InstallJournalFaultForFacadeTest() (
	get func() *storeio.FaultJournal,
	restore func(),
) {
	previous := recoveryJournalFaultHook
	var fault *storeio.FaultJournal
	recoveryJournalFaultHook = func(journal *storeio.RecoveryJournal) {
		fault = storeio.NewFaultJournal(journal)
	}
	return func() *storeio.FaultJournal { return fault }, func() {
		recoveryJournalFaultHook = previous
	}
}
