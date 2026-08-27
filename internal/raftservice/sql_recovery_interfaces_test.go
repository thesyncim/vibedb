package raftservice_test

import (
	"github.com/thesyncim/vibedb/internal/raftservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Keep shipped SQL-backed sources wired to every coordinator recovery read.
// Missing optional interfaces otherwise appear as stale-fence wire refusals.
var (
	_ raftservice.TransactionRecoverySource = (*sqldriver.ReplicatedApply)(nil)
	_ raftservice.RequestLedgerSource       = (*sqldriver.ReplicatedApply)(nil)
	_ raftservice.ExecutionPinSource        = (*sqldriver.ReplicatedApply)(nil)
)
