package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
			if options.ResidentBytes != 0 {
				short := options
				short.ResidentBytes--
				var capacity *durable.ResidentCapacityError
				if err := durable.ValidateOptions(short); !errors.As(err, &capacity) || capacity.Required != uint64(options.ResidentBytes) {
					t.Fatalf("residency was not the exact minimum: %v", err)
				}
			}
			if retry == 8 && options.ResidentBytes != 0 {
				t.Fatal("ordinary retry window changed the default resident budget")
			}
			if err := validateReplicatedApplySidecarsForLimits(canonicalReplicatedApplySidecarsForLimits(limits), limits); err != nil {
				t.Fatalf("ledger=%t retry=%d retained sidecars: %v", ledger, retry, err)
			}
			if !ledger && options.SealedRecoveryJournalBytes != ReplicatedSystemRecoveryJournalBytes {
				t.Fatalf("data-only journal changed: %d", options.SealedRecoveryJournalBytes)
			}
			required := uint64(storeio.RecoveryBatchRecordPaddedSizeForPayload(
				storeio.RecoveryJournalMinSectorSize, limits.MaxBatchDocuments,
				limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize))
			if ledger && options.SealedRecoveryJournalBytes != max(uint64(16838144), required) {
				t.Fatalf("ledger journal differs from legacy floor and actual frozen limits: %d", options.SealedRecoveryJournalBytes)
			}
			oversized := options
			oversized.SealedRecoveryJournalBytes = storeio.RecoveryJournalMaxCapacityBytes + storeio.RecoveryJournalMinSectorSize
			if err := durable.ValidateOptions(oversized); !errors.Is(err, durable.ErrSealedJournalCapacity) {
				t.Fatalf("ledger=%t retry=%d one-sector-over ceiling accepted: %v", ledger, retry, err)
			}
		}
	}
	if ReplicatedSystemRecoveryJournalBytes != 16777216 || ReplicatedUserRecoveryJournalBytes != 16794624 {
		t.Fatal("data-only system or user journal changed with ledger ceiling")
	}
}

func TestReplicatedLegacyJournalGeometrySurvivesScopedSessionUpgrade(t *testing.T) {
	legacy := ReplicatedApplySidecarProfile{SystemRecoveryJournalBytes: 16838144}
	if canonicalReplicatedLedgerApplySidecars() != legacy {
		t.Fatal("legacy journal floor changed")
	}
	// Existing RF3 development roots have eight retry slots and transaction
	// capacity independent of the expanded scoped-session cleanup bound.
	for _, ledger := range []bool{false, true} {
		limits := replicatedApplySystemLimitsForLedger(8, ledger, 68)
		if err := validateReplicatedApplySidecarsForLimits(legacy, limits); err != nil {
			t.Fatalf("existing ledger=%t root rejected: %v", ledger, err)
		}
	}
	tooSmall := legacy
	tooSmall.SystemRecoveryJournalBytes -= storeio.RecoveryJournalMinSectorSize
	if err := validateReplicatedApplySidecarGrammar(tooSmall); err == nil {
		t.Fatal("undersized legacy geometry accepted")
	}
}

func TestReplicatedTransactionProfilesPreflightEveryRetryWindow(t *testing.T) {
	for _, ledger := range []bool{false, true} {
		for relations := uint16(1); relations <= 2; relations++ {
			base := ReplicatedShardStoreIdentity{RelationCount: relations,
				Relations: make([]ReplicatedShardRelationIdentity, relations)}
			for i := 0; i < int(relations); i++ {
				base.Relations[i].Limits.MaxBatchDocuments = replicatedstate.MaxDistinctMutations
			}
			for retry := uint16(1); retry <= replicatedstate.MaxSessionRetryWindow; retry++ {
				limits := replicatedApplyTransactionSystemLimits(base, retry, ledger, 2048)
				options := replicatedApplyDurableOptions(limits)
				if err := durable.ValidateOptions(options); err != nil {
					t.Fatalf("ledger=%t relations=%d retry=%d: %v", ledger, relations, retry, err)
				}
				if err := validateReplicatedApplySidecarsForLimits(
					canonicalReplicatedApplySidecarsForLimits(limits), limits); err != nil {
					t.Fatal(err)
				}
				if limits.MaxBatchDocuments >= 2048 || limits.MaxDocumentBytes != replication.MaxCommandBytes {
					t.Fatalf("profile did not derive relation-local stage geometry: %+v", limits)
				}
			}
		}
	}
}

func TestReplicatedTransactionSystemJournalHardCeiling(t *testing.T) {
	for _, ledger := range []bool{false, true} {
		limits := replicatedApplySystemLimitsForLedger(replicatedstate.MaxSessionRetryWindow,
			ledger, replicatedstate.MaxTransactionSystemDocuments)
		required := storeio.RecoveryBatchRecordPaddedSizeForPayload(storeio.RecoveryJournalMinSectorSize,
			limits.MaxBatchDocuments, limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize)
		if uint64(required) != storeio.RecoveryJournalMaxCapacityBytes {
			t.Fatalf("ledger=%t exact transaction record=%d hard ceiling=%d", ledger,
				required, storeio.RecoveryJournalMaxCapacityBytes)
		}
		profile := canonicalReplicatedApplySidecarsForLimits(limits)
		if err := validateReplicatedApplySidecarsForLimits(profile, limits); err != nil {
			t.Fatal(err)
		}
		profile.SystemRecoveryJournalBytes -= storeio.RecoveryJournalMinSectorSize
		if err := validateReplicatedApplySidecarsForLimits(profile, limits); err == nil {
			t.Fatal("one-sector-short transaction journal accepted")
		}
	}
}
