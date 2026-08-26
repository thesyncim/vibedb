package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibejson"
)

// The retained replicated-apply identity uses vibejson's native hooks rather
// rather than routing catalog metadata through a reflective codec. The hooks keep the
// canonical field order stable, append fixed-width hexadecimal fields directly
// from bytes, and reject unknown, duplicate, or missing members in one pass.

type replicatedApplyMetaVibe replicatedApplyMeta
type replicatedPlacementProfileVibe ReplicatedPlacementProfile

var (
	replicatedApplyMetaFields = vibejson.MakeFieldSet(
		"format", "storage", "capture_storage", "validation_profile", "validation_digest",
		"system_limits", "capture_limits", "max_sessions", "retry_window", "txn_max_collections",
		"txn_max_documents", "txn_max_bytes", "request_ledger_capacity_bytes",
		"request_ledger_cleanup_reserve_bytes", "request_ledger_range_start",
		"request_ledger_range_end", "request_ledger_range_identity", "placement", "sidecars",
	)
	replicatedPlacementFields = vibejson.MakeFieldSet(
		"format", "shard_key", "tuple_version", "mapper_version",
		"range_start", "range_end", "range_end_max",
	)
	replicatedLimitsFields = vibejson.MakeFieldSet(
		"max_key_bytes", "max_document_bytes", "max_batch_documents", "max_batch_bytes",
	)
	replicatedApplySidecarFields = vibejson.MakeFieldSet(
		"system_recovery_journal_bytes",
	)
)

var (
	replicatedApplyMetaFieldNames = [...]string{
		"format", "storage", "capture_storage", "validation_profile", "validation_digest",
		"system_limits", "capture_limits", "max_sessions", "retry_window", "txn_max_collections",
		"txn_max_documents", "txn_max_bytes", "request_ledger_capacity_bytes",
		"request_ledger_cleanup_reserve_bytes", "request_ledger_range_start",
		"request_ledger_range_end", "request_ledger_range_identity", "placement", "sidecars",
	}
	replicatedPlacementFieldNames = [...]string{
		"format", "shard_key", "tuple_version", "mapper_version",
		"range_start", "range_end", "range_end_max",
	}
	replicatedLimitsFieldNames = [...]string{
		"max_key_bytes", "max_document_bytes", "max_batch_documents", "max_batch_bytes",
	}
)

func (m replicatedApplyMeta) MarshalJSON() ([]byte, error) {
	if m.Format != ReplicatedApplyFormat {
		return nil, fmt.Errorf("vibedb: unsupported replicated apply format %d", m.Format)
	}
	if err := validateReplicatedPlacementProfileGrammar(m.Placement); err != nil {
		return nil, err
	}
	if err := validateReplicatedApplySidecarGrammar(m.Sidecars); err != nil {
		return nil, err
	}
	if err := validateReplicatedRequestLedgerOptions(m.options()); err != nil {
		return nil, err
	}
	encoded := replicatedApplyMetaVibe(m)
	return vibejson.Marshal(&encoded)
}

func (profile ReplicatedPlacementProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedPlacementProfileGrammar(profile); err != nil {
		return nil, err
	}
	encoded := replicatedPlacementProfileVibe(profile)
	return vibejson.Marshal(&encoded)
}

// MarshalJSON emits the same strict grammar stored in the SQL catalog so
// orchestrators can retain an exact restart identity.
func (identity ReplicatedApplyIdentity) MarshalJSON() ([]byte, error) {
	return replicatedApplyMetaFromIdentity(identity).MarshalJSON()
}

func (m *replicatedApplyMetaVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	meta := (*replicatedApplyMeta)(m)
	w = w.RawUnchecked(`{"format":`).Uint(uint64(meta.Format))
	w = w.RawUnchecked(`,"storage":`).String(meta.Storage)
	w = w.RawUnchecked(`,"capture_storage":`).String(meta.CaptureStorage)
	w = w.RawUnchecked(`,"validation_profile":`).Uint(uint64(meta.ValidationProfile))
	w = w.RawUnchecked(`,"validation_digest":`)
	w = appendReplicatedHexString(w, meta.ValidationDigest[:])
	w = w.RawUnchecked(`,"system_limits":`)
	w = appendReplicatedLimits(w, meta.SystemLimits)
	w = w.RawUnchecked(`,"capture_limits":`)
	w = appendReplicatedLimits(w, meta.CaptureLimits)
	w = w.RawUnchecked(`,"max_sessions":`).Uint(meta.MaxSessions)
	w = w.RawUnchecked(`,"retry_window":`).Uint(uint64(meta.RetryWindow))
	w = w.RawUnchecked(`,"txn_max_collections":`).Int(int64(meta.TxnMaxCollections))
	w = w.RawUnchecked(`,"txn_max_documents":`).Int(int64(meta.TxnMaxDocuments))
	w = w.RawUnchecked(`,"txn_max_bytes":`).Int(meta.TxnMaxBytes)
	w = w.RawUnchecked(`,"request_ledger_capacity_bytes":`).Uint(
		meta.RequestLedgerCapacityBytes,
	)
	w = w.RawUnchecked(`,"request_ledger_cleanup_reserve_bytes":`).Uint(
		meta.RequestLedgerCleanupReserveBytes,
	)
	w = w.RawUnchecked(`,"request_ledger_range_start":`)
	w = appendReplicatedHexString(w, meta.RequestLedgerRangeStart[:])
	w = w.RawUnchecked(`,"request_ledger_range_end":`)
	w = appendReplicatedHexString(w, meta.RequestLedgerRangeEnd[:])
	w = w.RawUnchecked(`,"request_ledger_range_identity":`)
	w = appendReplicatedHexString(w, meta.RequestLedgerRangeIdentity[:])
	w = w.RawUnchecked(`,"placement":`)
	w = appendReplicatedPlacement(w, meta.Placement)
	w = w.RawUnchecked(`,"sidecars":`)
	w = appendReplicatedApplySidecars(w, meta.Sidecars)
	return w.RawByteUnchecked('}')
}

func (profile *replicatedPlacementProfileVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	return appendReplicatedPlacement(w, ReplicatedPlacementProfile(*profile))
}

func appendReplicatedLimits(
	w vibejson.TrustedAppender,
	limits ReplicatedShardStoreLimits,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"max_key_bytes":`).Int(int64(limits.MaxKeyBytes))
	w = w.RawUnchecked(`,"max_document_bytes":`).Int(int64(limits.MaxDocumentBytes))
	w = w.RawUnchecked(`,"max_batch_documents":`).Int(int64(limits.MaxBatchDocuments))
	w = w.RawUnchecked(`,"max_batch_bytes":`).Int(int64(limits.MaxBatchBytes))
	return w.RawByteUnchecked('}')
}

func appendReplicatedPlacement(
	w vibejson.TrustedAppender,
	profile ReplicatedPlacementProfile,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"format":`).Uint(uint64(profile.Format))
	w = w.RawUnchecked(`,"shard_key":`).String(profile.ShardKey)
	w = w.RawUnchecked(`,"tuple_version":`).Uint(uint64(profile.TupleVersion))
	w = w.RawUnchecked(`,"mapper_version":`).Uint(uint64(profile.MapperVersion))
	w = w.RawUnchecked(`,"range_start":`)
	w = appendReplicatedHexString(w, profile.Range.Start[:])
	w = w.RawUnchecked(`,"range_end":`)
	w = appendReplicatedHexString(w, profile.Range.End.Point[:])
	w = w.RawUnchecked(`,"range_end_max":`).Bool(profile.Range.End.Max)
	return w.RawByteUnchecked('}')
}

func appendReplicatedApplySidecars(
	w vibejson.TrustedAppender,
	profile ReplicatedApplySidecarProfile,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"system_recovery_journal_bytes":`).Uint(
		profile.SystemRecoveryJournalBytes,
	)
	return w.RawByteUnchecked('}')
}

func appendReplicatedHexString(
	w vibejson.TrustedAppender,
	src []byte,
) vibejson.TrustedAppender {
	var encoded [sha256.Size * 2]byte
	n := hex.Encode(encoded[:], src)
	w = w.RawByteUnchecked('"').RawBytesUnchecked(encoded[:n])
	return w.RawByteUnchecked('"')
}

// UnmarshalJSON decodes the strict retained grammar. Base-binding-dependent
// profile validation occurs in the exact open API before recovery.
func (identity *ReplicatedApplyIdentity) UnmarshalJSON(data []byte) error {
	var decoded replicatedApplyMetaVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*identity = replicatedApplyMeta(decoded).identity()
	return nil
}

func (profile *ReplicatedPlacementProfile) UnmarshalJSON(data []byte) error {
	var decoded replicatedPlacementProfileVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*profile = ReplicatedPlacementProfile(decoded)
	return nil
}

func (m *replicatedApplyMeta) UnmarshalJSON(data []byte) error {
	var decoded replicatedApplyMetaVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*m = replicatedApplyMeta(decoded)
	return nil
}

func (m *replicatedApplyMetaVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded replicatedApplyMeta
	if err := decodeReplicatedApplyMetaVibe(&c, &decoded); err != nil {
		return c, err
	}
	*m = replicatedApplyMetaVibe(decoded)
	return c, nil
}

func (profile *replicatedPlacementProfileVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedPlacementProfile
	if err := decodeReplicatedPlacementVibe(&c, &decoded); err != nil {
		return c, err
	}
	*profile = replicatedPlacementProfileVibe(decoded)
	return c, nil
}

func decodeReplicatedApplyMetaVibe(
	c *vibejson.DecodeCursor,
	dst *replicatedApplyMeta,
) error {
	if err := c.BeginObject("replicated apply"); err != nil {
		return fmt.Errorf("vibedb: SQL catalog replicated apply must be a JSON object")
	}
	var decoded replicatedApplyMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedApplyMetaFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated apply", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated apply", name); err != nil {
			return err
		}
		switch index {
		case 0:
			if err := decodeRequiredReplicatedUint16(
				c, "replicated apply format", &decoded.Format,
			); err != nil {
				return err
			}
		case 1:
			if err := c.String(&decoded.Storage); err != nil {
				return err
			}
		case 2:
			if err := c.String(&decoded.CaptureStorage); err != nil {
				return err
			}
		case 3:
			if err := c.Uint8(&decoded.ValidationProfile); err != nil {
				return err
			}
		case 4:
			if err := decodeReplicatedLowerHex(
				c, decoded.ValidationDigest[:],
				"vibedb: replicated apply validation digest must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated apply validation digest",
			); err != nil {
				return err
			}
		case 5:
			if err := decodeReplicatedSystemLimitsVibe(c, &decoded.SystemLimits); err != nil {
				return err
			}
		case 6:
			if err := decodeReplicatedSystemLimitsVibe(c, &decoded.CaptureLimits); err != nil {
				return err
			}
		case 7:
			if err := c.Uint64(&decoded.MaxSessions); err != nil {
				return err
			}
		case 8:
			if err := c.Uint16(&decoded.RetryWindow); err != nil {
				return err
			}
		case 9:
			if err := c.Int(&decoded.TxnMaxCollections); err != nil {
				return err
			}
		case 10:
			if err := c.Int(&decoded.TxnMaxDocuments); err != nil {
				return err
			}
		case 11:
			if err := c.Int64(&decoded.TxnMaxBytes); err != nil {
				return err
			}
		case 12:
			if err := c.Uint64(&decoded.RequestLedgerCapacityBytes); err != nil {
				return err
			}
		case 13:
			if err := c.Uint64(&decoded.RequestLedgerCleanupReserveBytes); err != nil {
				return err
			}
		case 14:
			if err := decodeReplicatedLowerHex(
				c, decoded.RequestLedgerRangeStart[:],
				"vibedb: replicated apply request-ledger range start must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated apply request-ledger range start",
			); err != nil {
				return err
			}
		case 15:
			if err := decodeReplicatedLowerHex(
				c, decoded.RequestLedgerRangeEnd[:],
				"vibedb: replicated apply request-ledger range end must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated apply request-ledger range end",
			); err != nil {
				return err
			}
		case 16:
			if err := decodeReplicatedLowerHex(
				c, decoded.RequestLedgerRangeIdentity[:],
				"vibedb: replicated apply request-ledger range identity must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated apply request-ledger range identity",
			); err != nil {
				return err
			}
		case 17:
			if err := decodeReplicatedPlacementVibe(c, &decoded.Placement); err != nil {
				return err
			}
		case 18:
			if err := decodeReplicatedApplySidecarsVibe(c, &decoded.Sidecars); err != nil {
				return err
			}
		}
	}
	for index, name := range replicatedApplyMetaFieldNames {
		if seen&(uint64(1)<<index) == 0 {
			return fmt.Errorf("vibedb: replicated apply is missing member %q", name)
		}
	}
	if decoded.Format != ReplicatedApplyFormat {
		return fmt.Errorf("vibedb: unsupported replicated apply format %d", decoded.Format)
	}
	if decoded.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutation) {
		return errors.New("vibedb: replicated apply has the wrong validation profile")
	}
	if decoded.RetryWindow == 0 ||
		decoded.RetryWindow > replicatedstate.MaxSessionRetryWindow {
		return fmt.Errorf("%w: retry window", ErrReplicatedApplyMismatch)
	}
	if decoded.SystemLimits != replicatedApplySystemLimits(decoded.RetryWindow) {
		return fmt.Errorf("%w: system collection limits", ErrReplicatedApplyMismatch)
	}
	if err := validateReplicatedRequestLedgerOptions(decoded.options()); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedPlacementVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedPlacementProfile,
) error {
	if err := c.BeginObject("replicated placement"); err != nil {
		return fmt.Errorf("vibedb: SQL catalog replicated placement must be a JSON object")
	}
	var decoded ReplicatedPlacementProfile
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedPlacementFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated placement", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated placement", name); err != nil {
			return err
		}
		switch index {
		case 0:
			if err := decodeRequiredReplicatedUint16(
				c, "replicated placement format", &decoded.Format,
			); err != nil {
				return err
			}
		case 1:
			if err := c.String(&decoded.ShardKey); err != nil {
				return err
			}
		case 2:
			var value uint32
			if err := c.Uint32(&value); err != nil {
				return err
			}
			decoded.TupleVersion = distribution.TupleVersion(value)
		case 3:
			var value uint32
			if err := c.Uint32(&value); err != nil {
				return err
			}
			decoded.MapperVersion = distribution.MapperVersion(value)
		case 4:
			if err := decodeReplicatedLowerHex(
				c, decoded.Range.Start[:],
				"vibedb: replicated placement range start must be lowercase 8-byte hexadecimal",
				"vibedb: replicated placement range start",
			); err != nil {
				return err
			}
		case 5:
			if err := decodeReplicatedLowerHex(
				c, decoded.Range.End.Point[:],
				"vibedb: replicated placement range end must be lowercase 8-byte hexadecimal",
				"vibedb: replicated placement range end",
			); err != nil {
				return err
			}
		case 6:
			if err := c.Bool(&decoded.Range.End.Max); err != nil {
				return err
			}
		}
	}
	for index, name := range replicatedPlacementFieldNames {
		if seen&(uint64(1)<<index) == 0 {
			return fmt.Errorf("vibedb: replicated placement is missing member %q", name)
		}
	}
	if decoded.Range.End.Max && decoded.Range.End.Point != (distribution.KeyspacePoint{}) {
		return errors.New("vibedb: replicated placement max range end must use the zero point")
	}
	if err := validateReplicatedPlacementProfileGrammar(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedSystemLimitsVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreLimits,
) error {
	if err := c.BeginObject("replicated shard store limits"); err != nil {
		return fmt.Errorf("vibedb: SQL catalog replicated shard store limits must be a JSON object")
	}
	var decoded ReplicatedShardStoreLimits
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedLimitsFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated shard store limits", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated shard store limits", name); err != nil {
			return err
		}
		switch index {
		case 0:
			if err := c.Int(&decoded.MaxKeyBytes); err != nil {
				return err
			}
		case 1:
			if err := c.Int(&decoded.MaxDocumentBytes); err != nil {
				return err
			}
		case 2:
			if err := c.Int(&decoded.MaxBatchDocuments); err != nil {
				return err
			}
		case 3:
			if err := c.Int(&decoded.MaxBatchBytes); err != nil {
				return err
			}
		}
	}
	for index, name := range replicatedLimitsFieldNames {
		if seen&(uint64(1)<<index) == 0 {
			return fmt.Errorf("vibedb: replicated shard store limits are missing member %q", name)
		}
	}
	*dst = decoded
	return nil
}

func decodeReplicatedApplySidecarsVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedApplySidecarProfile,
) error {
	if err := c.BeginObject("replicated apply sidecars"); err != nil {
		return fmt.Errorf("vibedb: SQL catalog replicated apply sidecars must be a JSON object")
	}
	var decoded ReplicatedApplySidecarProfile
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedApplySidecarFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated apply sidecars", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated apply sidecars", name); err != nil {
			return err
		}
		if err := c.Uint64(&decoded.SystemRecoveryJournalBytes); err != nil {
			return err
		}
	}
	if seen == 0 {
		return fmt.Errorf(
			"vibedb: replicated apply sidecars are missing member %q",
			"system_recovery_journal_bytes",
		)
	}
	if err := validateReplicatedApplySidecarGrammar(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func markReplicatedCatalogField(
	seen *uint64,
	index int,
	kind string,
	name string,
) error {
	mask := uint64(1) << index
	if *seen&mask != 0 {
		return fmt.Errorf(
			"vibedb: SQL catalog %s has duplicate member %q",
			kind, name,
		)
	}
	*seen |= mask
	return nil
}

func decodeRequiredReplicatedUint16(
	c *vibejson.DecodeCursor,
	label string,
	dst *uint16,
) error {
	null, err := c.Null()
	if err != nil {
		return err
	}
	if null {
		return fmt.Errorf("vibedb: %s must not be null", label)
	}
	return c.Uint16(dst)
}

func decodeReplicatedLowerHex(
	c *vibejson.DecodeCursor,
	dst []byte,
	grammar string,
	label string,
) error {
	raw, err := c.Raw()
	if err != nil {
		return err
	}
	expected := hex.EncodedLen(len(dst))
	text, unescaped := raw.StringBytes()
	if !unescaped {
		// One decoded byte can occupy at most six JSON source bytes (\u00xx).
		// Bound the escaped slow path before decoding so corrupt catalog input
		// cannot turn a fixed-width identity field into an unbounded allocation.
		if len(raw.Bytes()) > expected*6+2 {
			return errors.New(grammar)
		}
		var scratch [sha256.Size * 2 * 6]byte
		var isString bool
		text, isString, err = raw.AppendText(scratch[:0])
		if err != nil {
			return err
		}
		if !isString {
			return errors.New(grammar)
		}
	}
	if len(text) != expected || containsUpperASCII(text) {
		return errors.New(grammar)
	}
	if _, err := hex.Decode(dst, text); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func containsUpperASCII(value []byte) bool {
	for _, b := range value {
		if b >= 'A' && b <= 'Z' {
			return true
		}
	}
	return false
}
