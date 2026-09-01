package driver

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

const (
	// ReplicatedUserRecoveryJournalBytes is the exact record region required by
	// the current replicated SQL user limits. The file also contains two
	// storeio header sectors.
	// Retain the data-only geometry independently of the larger optional
	// request-ledger system journal ceiling.
	ReplicatedUserRecoveryJournalBytes = (uint64(16) << 20) + 34*storeio.RecoveryJournalMinSectorSize
	// ReplicatedTransactionMarkerBytes is the exact fixed decision-log window.
	// Replicated SQL has exactly the user and system participants; one MiB holds
	// 2,048 current two-participant decisions so recycle is pressure handling,
	// not a per-apply steady-state fence.
	ReplicatedTransactionMarkerBytes uint64 = 1 << 20
	// ReplicatedSystemRecoveryJournalBytes is the exact sealed decision suffix
	// retained by the hidden state collection. A one-record geometry turns
	// journal recycling into steady-state checkpoint work even for tiny direct
	// writes. One ordinary 16 MiB segment holds many small group commits while
	// still covering the maximum legal hidden-state batch; the independently
	// bounded checkpoint cadence remains the durability/retention fence.
	ReplicatedSystemRecoveryJournalBytes uint64 = 16 << 20
)

// ReplicatedShardStoreSidecarProfile freezes the exact sealed sidecars owned
// by the base replicated SQL root. The user recovery journal is created with
// the replacement user-storage incarnation at bind. TransactionMarkerBytes is
// published by the same catalog cut before txn.vtm is minted, so every mint or
// recovery has an authoritative expected physical profile.
type ReplicatedShardStoreSidecarProfile struct {
	UserRecoveryJournalBytes uint64
	TransactionMarkerBytes   uint64
}

// ReplicatedApplySidecarProfile freezes the exact sealed recovery journal of
// the hidden system collection. The base identity already owns txn.vtm.
type ReplicatedApplySidecarProfile struct {
	SystemRecoveryJournalBytes uint64
}

type replicatedShardStoreSidecarVibe ReplicatedShardStoreSidecarProfile
type replicatedApplySidecarVibe ReplicatedApplySidecarProfile

var replicatedShardStoreSidecarFields = vibejson.MakeFieldSet(
	"user_recovery_journal_bytes", "transaction_marker_bytes",
)

var replicatedShardStoreSidecarFieldNames = [...]string{
	"user_recovery_journal_bytes", "transaction_marker_bytes",
}

func canonicalReplicatedShardStoreSidecars() ReplicatedShardStoreSidecarProfile {
	return ReplicatedShardStoreSidecarProfile{
		UserRecoveryJournalBytes: ReplicatedUserRecoveryJournalBytes,
		TransactionMarkerBytes:   ReplicatedTransactionMarkerBytes,
	}
}

func canonicalReplicatedApplySidecars() ReplicatedApplySidecarProfile {
	return ReplicatedApplySidecarProfile{
		SystemRecoveryJournalBytes: ReplicatedSystemRecoveryJournalBytes,
	}
}

// Retain the legacy ledger geometry as a compatibility floor. Transaction-
// capable data members also need the 16 MiB payload ceiling; their exact
// journal additionally includes the bounded intent and control rows. Every
// profile is checked against its collection limits before opening a sidecar.
func canonicalReplicatedLedgerApplySidecars() ReplicatedApplySidecarProfile {
	// This is an on-disk compatibility floor, not a derivation from today's
	// maximum retry window. Raising session cleanup geometry must not change
	// the expected journal of an already sealed, sufficiently sized root.
	// Larger profiles are derived separately from their actual frozen limits.
	return ReplicatedApplySidecarProfile{SystemRecoveryJournalBytes: 16838144}
}

func canonicalReplicatedApplySidecarsForLimits(limits ReplicatedShardStoreLimits) ReplicatedApplySidecarProfile {
	if limits.MaxDocumentBytes == requestledger.MaxCommandBytes {
		legacy := canonicalReplicatedLedgerApplySidecars()
		required := uint64(
			storeio.RecoveryBatchRecordPaddedSizeForPayload(storeio.RecoveryJournalMinSectorSize,
				limits.MaxBatchDocuments, limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize))
		if required > legacy.SystemRecoveryJournalBytes {
			return ReplicatedApplySidecarProfile{SystemRecoveryJournalBytes: required}
		}
		return legacy
	}
	return canonicalReplicatedApplySidecars()
}

func validateReplicatedShardStoreSidecarGrammar(
	profile ReplicatedShardStoreSidecarProfile,
) error {
	if profile.UserRecoveryJournalBytes != ReplicatedUserRecoveryJournalBytes ||
		profile.TransactionMarkerBytes != ReplicatedTransactionMarkerBytes {
		return fmt.Errorf(
			"%w: invalid sealed sidecar geometry",
			ErrReplicatedShardStoreProfile,
		)
	}
	return nil
}

func validateReplicatedApplySidecarGrammar(
	profile ReplicatedApplySidecarProfile,
) error {
	if profile != canonicalReplicatedApplySidecars() &&
		profile != canonicalReplicatedLedgerApplySidecars() &&
		(profile.SystemRecoveryJournalBytes < canonicalReplicatedLedgerApplySidecars().SystemRecoveryJournalBytes ||
			profile.SystemRecoveryJournalBytes > storeio.RecoveryJournalMaxCapacityBytes ||
			profile.SystemRecoveryJournalBytes%storeio.RecoveryJournalMinSectorSize != 0) {
		return fmt.Errorf(
			"%w: invalid sealed system-journal geometry",
			ErrReplicatedApplyMismatch,
		)
	}
	return nil
}

func validateReplicatedShardStoreSidecarsForLimits(
	profile ReplicatedShardStoreSidecarProfile,
	limits ReplicatedShardStoreLimits,
) error {
	if err := validateReplicatedShardStoreSidecarGrammar(profile); err != nil {
		return err
	}
	required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		limits.MaxBatchDocuments,
		limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	if required <= 0 || uint64(required) != profile.UserRecoveryJournalBytes {
		return fmt.Errorf(
			"%w: user journal does not exactly cover the frozen batch profile",
			ErrReplicatedShardStoreProfile,
		)
	}
	decisionBytes, ok := storeio.TxnDecisionRecordPaddedSize(2)
	if !ok || uint64(decisionBytes) > profile.TransactionMarkerBytes {
		return fmt.Errorf(
			"%w: transaction marker cannot hold one user/system decision",
			ErrReplicatedShardStoreProfile,
		)
	}
	return nil
}

func validateReplicatedApplySidecarsForLimits(
	profile ReplicatedApplySidecarProfile,
	limits ReplicatedShardStoreLimits,
) error {
	if err := validateReplicatedApplySidecarGrammar(profile); err != nil {
		return err
	}
	if profile != canonicalReplicatedApplySidecarsForLimits(limits) ||
		profile.SystemRecoveryJournalBytes > storeio.RecoveryJournalMaxCapacityBytes {
		return fmt.Errorf("%w: system journal does not match the frozen collection profile", ErrReplicatedApplyMismatch)
	}
	required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		limits.MaxBatchDocuments,
		limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	if required <= 0 || uint64(required) > profile.SystemRecoveryJournalBytes {
		return fmt.Errorf(
			"%w: system journal cannot cover the frozen batch profile",
			ErrReplicatedApplyMismatch,
		)
	}
	return nil
}

func (profile ReplicatedShardStoreSidecarProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedShardStoreSidecarGrammar(profile); err != nil {
		return nil, err
	}
	encoded := replicatedShardStoreSidecarVibe(profile)
	return vibejson.Marshal(&encoded)
}

func (profile *ReplicatedShardStoreSidecarProfile) UnmarshalJSON(data []byte) error {
	var decoded replicatedShardStoreSidecarVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*profile = ReplicatedShardStoreSidecarProfile(decoded)
	return nil
}

func (profile ReplicatedApplySidecarProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedApplySidecarGrammar(profile); err != nil {
		return nil, err
	}
	encoded := replicatedApplySidecarVibe(profile)
	return vibejson.Marshal(&encoded)
}

func (profile *ReplicatedApplySidecarProfile) UnmarshalJSON(data []byte) error {
	var decoded replicatedApplySidecarVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*profile = ReplicatedApplySidecarProfile(decoded)
	return nil
}

func (profile *replicatedShardStoreSidecarVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	value := (*ReplicatedShardStoreSidecarProfile)(profile)
	w = w.RawUnchecked(`{"user_recovery_journal_bytes":`).Uint(
		value.UserRecoveryJournalBytes,
	)
	w = w.RawUnchecked(`,"transaction_marker_bytes":`).Uint(value.TransactionMarkerBytes)
	return w.RawByteUnchecked('}')
}

func (profile *replicatedApplySidecarVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	return appendReplicatedApplySidecars(
		w, ReplicatedApplySidecarProfile(*profile),
	)
}

func (profile *replicatedShardStoreSidecarVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	if err := c.BeginObject("replicated shard sidecars"); err != nil {
		return c, fmt.Errorf("vibedb: SQL catalog replicated shard sidecars must be a JSON object")
	}
	var decoded ReplicatedShardStoreSidecarProfile
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return c, err
		}
		if !ok {
			break
		}
		index, known := replicatedShardStoreSidecarFields.Lookup(name, true)
		if !known {
			return c, unknownCatalogMember("replicated shard sidecars", name)
		}
		if err := markReplicatedCatalogField(
			&seen, index, "replicated shard sidecars", name,
		); err != nil {
			return c, err
		}
		switch index {
		case 0:
			err = c.Uint64(&decoded.UserRecoveryJournalBytes)
		case 1:
			err = c.Uint64(&decoded.TransactionMarkerBytes)
		}
		if err != nil {
			return c, err
		}
	}
	for index, name := range replicatedShardStoreSidecarFieldNames {
		if seen&(uint64(1)<<index) == 0 {
			return c, fmt.Errorf(
				"vibedb: replicated shard sidecars are missing member %q", name,
			)
		}
	}
	if err := validateReplicatedShardStoreSidecarGrammar(decoded); err != nil {
		return c, err
	}
	*profile = replicatedShardStoreSidecarVibe(decoded)
	return c, nil
}

func (profile *replicatedApplySidecarVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedApplySidecarProfile
	if err := decodeReplicatedApplySidecarsVibe(&c, &decoded); err != nil {
		return c, err
	}
	*profile = replicatedApplySidecarVibe(decoded)
	return c, nil
}
