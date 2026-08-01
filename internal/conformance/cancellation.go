package conformance

// CancellationCase is the shared observable cancellation contract for the two
// SQL entry points. Trigger and Error differ because database/sql owns a Go
// context while pgwire receives the protocol's out-of-band CancelRequest; the
// atomicity and session-reuse guarantees are deliberately identical.
// NoPartialWrite covers both durable publication and a database/sql
// transaction's staged overlay; Reusable means the same logical session can
// execute a later command after the boundary-specific transaction cleanup.
type CancellationCase struct {
	ID             string
	Entry          EntryPoint
	Trigger        string
	Error          string
	NoPartialWrite bool
	Reusable       bool
}

var CancellationCases = []CancellationCase{
	{
		ID: "database-sql-context-cancellation", Entry: DatabaseSQL,
		Trigger:        "context cancellation or deadline",
		Error:          "context.Canceled or context.DeadlineExceeded",
		NoPartialWrite: true, Reusable: true,
	},
	{
		ID: "pgwire-cancel-request", Entry: PGWire,
		Trigger:        "CancelRequest during an active command",
		Error:          "SQLSTATE 57014",
		NoPartialWrite: true, Reusable: true,
	},
}

func CancellationFor(entry EntryPoint) (CancellationCase, bool) {
	for _, cancellation := range CancellationCases {
		if cancellation.Entry == entry {
			return cancellation, true
		}
	}
	return CancellationCase{}, false
}
