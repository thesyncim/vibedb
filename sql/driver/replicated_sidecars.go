package driver

import (
	"encoding/json"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const (
	// ReplicatedUserRecoveryJournalBytes is the exact record region required by
	// the current replicated SQL user limits. The file also contains two
	// storeio header sectors.
	ReplicatedUserRecoveryJournalBytes = storeio.RecoveryJournalMaxCapacityBytes
	// ReplicatedTransactionMarkerBytes is the exact fixed decision-log window.
	// Replicated SQL has exactly the user and system participants; one MiB holds
	// 2,048 current two-participant decisions so recycle is pressure handling,
	// not a per-apply steady-state fence.
	ReplicatedTransactionMarkerBytes uint64 = 1 << 20
	// ReplicatedSystemRecoveryJournalBytes is the exact padded conditional
	// record for the current hidden state/completion pair.
	ReplicatedSystemRecoveryJournalBytes uint64 = 655872
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
	if profile.SystemRecoveryJournalBytes != ReplicatedSystemRecoveryJournalBytes {
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
	required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		limits.MaxBatchDocuments,
		limits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	if required <= 0 || uint64(required) != profile.SystemRecoveryJournalBytes {
		return fmt.Errorf(
			"%w: system journal does not exactly cover the frozen batch profile",
			ErrReplicatedApplyMismatch,
		)
	}
	return nil
}

func (profile ReplicatedShardStoreSidecarProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedShardStoreSidecarGrammar(profile); err != nil {
		return nil, err
	}
	type encoded struct {
		UserRecoveryJournalBytes uint64 `json:"user_recovery_journal_bytes"`
		TransactionMarkerBytes   uint64 `json:"transaction_marker_bytes"`
	}
	return json.Marshal(encoded(profile))
}

func (profile *ReplicatedShardStoreSidecarProfile) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedShardStoreSidecarProfile
	present := make(map[string]bool, 2)
	err := decodeCatalogObject(data, "replicated shard sidecars", func(
		name string, decoder *json.Decoder,
	) error {
		present[name] = true
		switch name {
		case "user_recovery_journal_bytes":
			return decoder.Decode(&decoded.UserRecoveryJournalBytes)
		case "transaction_marker_bytes":
			return decoder.Decode(&decoded.TransactionMarkerBytes)
		default:
			return unknownCatalogMember("replicated shard sidecars", name)
		}
	})
	if err != nil {
		return err
	}
	for _, name := range []string{
		"user_recovery_journal_bytes", "transaction_marker_bytes",
	} {
		if !present[name] {
			return fmt.Errorf(
				"vibedb: replicated shard sidecars are missing member %q",
				name,
			)
		}
	}
	if err := validateReplicatedShardStoreSidecarGrammar(decoded); err != nil {
		return err
	}
	*profile = decoded
	return nil
}

func (profile ReplicatedApplySidecarProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedApplySidecarGrammar(profile); err != nil {
		return nil, err
	}
	type encoded struct {
		SystemRecoveryJournalBytes uint64 `json:"system_recovery_journal_bytes"`
	}
	return json.Marshal(encoded(profile))
}

func (profile *ReplicatedApplySidecarProfile) UnmarshalJSON(data []byte) error {
	var decoded ReplicatedApplySidecarProfile
	present := false
	err := decodeCatalogObject(data, "replicated apply sidecars", func(
		name string, decoder *json.Decoder,
	) error {
		switch name {
		case "system_recovery_journal_bytes":
			present = true
			return decoder.Decode(&decoded.SystemRecoveryJournalBytes)
		default:
			return unknownCatalogMember("replicated apply sidecars", name)
		}
	})
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf(
			"vibedb: replicated apply sidecars are missing member %q",
			"system_recovery_journal_bytes",
		)
	}
	if err := validateReplicatedApplySidecarGrammar(decoded); err != nil {
		return err
	}
	*profile = decoded
	return nil
}
