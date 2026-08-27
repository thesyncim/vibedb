package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

// Exercise the actual production options normalizer, without creating SQL
// roots or relying on platform-specific strict physical allocation support.
// Dev, RF3 process, and restored members all freeze these same profiles.
func TestReplicatedApplyDurableProfilesPreflightEveryRetryWindow(t *testing.T) {
	for _, ledger := range []bool{false, true} {
		for retry := uint16(1); retry <= replicatedstate.MaxSessionRetryWindow; retry++ {
			limits := replicatedApplySystemLimitsForLedger(retry, ledger)
			options := replicatedApplyDurableOptions(limits)
			if err := durable.ValidateOptions(options); err != nil {
				t.Fatalf("ledger=%t retry=%d actual durable profile=%+v: %v", ledger, retry, limits, err)
			}
			if err := validateReplicatedApplySidecarsForLimits(canonicalReplicatedApplySidecarsForLimits(limits), limits); err != nil {
				t.Fatalf("ledger=%t retry=%d retained sidecars: %v", ledger, retry, err)
			}
			if !ledger && options.SealedRecoveryJournalBytes != ReplicatedSystemRecoveryJournalBytes {
				t.Fatalf("data-only journal changed: %d", options.SealedRecoveryJournalBytes)
			}
			if ledger && options.SealedRecoveryJournalBytes != 16838144 {
				t.Fatalf("ledger journal lost exact 514-record bound: %d", options.SealedRecoveryJournalBytes)
			}
			oversized := options
			oversized.SealedRecoveryJournalBytes = storeio.RecoveryJournalMaxCapacityBytes + storeio.RecoveryJournalMinSectorSize
			if err := durable.ValidateOptions(oversized); !errors.Is(err, durable.ErrSealedJournalCapacity) {
				t.Fatalf("ledger=%t retry=%d one-sector-over ceiling accepted: %v", ledger, retry, err)
			}
		}
	}
	if ReplicatedSystemRecoveryJournalBytes != 655872 || ReplicatedUserRecoveryJournalBytes != 16794624 {
		t.Fatal("data-only system or user journal changed with ledger ceiling")
	}
}
