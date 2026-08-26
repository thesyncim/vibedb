package requestledger

import (
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	// StoragePrefix is reserved for request-ledger hidden relation rows.
	StoragePrefix byte = systemkey.RequestLedgerFirst

	StorageHead         byte = 1
	StoragePlanPage     byte = 2
	StoragePending      byte = 3
	StorageTerminal     byte = 4
	StorageAck          byte = 5
	StorageContinuation byte = 6
	StoragePayloadChunk byte = 7
	StoragePayloadBuild byte = 8
	StorageRoutePin     byte = 9
	StoragePrepared     byte = 10
	StorageSchemaPin    byte = 11

	FixedStorageKeyBytes   = 2 + 32 + 32
	PageStorageKeyBytes    = FixedStorageKeyBytes + 8
	PayloadBuildKeyBytes   = FixedStorageKeyBytes
	PayloadStorageKeyBytes = FixedStorageKeyBytes + 32 + 8
)

func AppendHeadKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageHead, home, key)
}

func AppendPendingKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StoragePending, home, key)
}

func AppendTerminalKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageTerminal, home, key)
}

func AppendAckKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageAck, home, key)
}

func AppendContinuationKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageContinuation, home, key)
}

func AppendPayloadChunkKey(dst []byte, home LedgerHome, key, content Digest, ordinal uint64) []byte {
	dst = appendFixedStorageKey(dst, StoragePayloadChunk, home, key)
	dst = append(dst, content[:]...)
	return binary.BigEndian.AppendUint64(dst, ordinal)
}

func AppendPayloadBuildKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StoragePayloadBuild, home, key)
}
func AppendRoutePinKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageRoutePin, home, key)
}
func AppendPreparedTerminalKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StoragePrepared, home, key)
}
func AppendSchemaPinReleaseKey(dst []byte, home LedgerHome, key Digest) []byte {
	return appendFixedStorageKey(dst, StorageSchemaPin, home, key)
}

func AppendPlanPageKey(dst []byte, home LedgerHome, key Digest, ordinal uint64) []byte {
	dst = appendFixedStorageKey(dst, StoragePlanPage, home, key)
	// Big endian preserves ordinal scan order within one request prefix.
	return binary.BigEndian.AppendUint64(dst, ordinal)
}

func appendFixedStorageKey(dst []byte, kind byte, home LedgerHome, key Digest) []byte {
	// Home precedes the full equality key and kind. Sequenced issuer lanes are
	// therefore range-local even though their RequestID/sequence-bound KeyDigest
	// differs, while each individual request remains one contiguous subrange.
	dst = append(dst, StoragePrefix)
	dst = append(dst, home[:]...)
	dst = append(dst, key[:]...)
	return append(dst, kind)
}

type StorageKeyView struct {
	Kind    byte
	Home    LedgerHome
	Key     Digest
	Content Digest
	Ordinal uint64
}

// OpenStorageKey recognizes every exact hidden-row key without allocating.
func OpenStorageKey(raw []byte) (StorageKeyView, error) {
	if len(raw) < FixedStorageKeyBytes || raw[0] != StoragePrefix {
		return StorageKeyView{}, ErrCorrupt
	}
	view := StorageKeyView{Kind: raw[65]}
	copy(view.Home[:], raw[1:33])
	copy(view.Key[:], raw[33:65])
	if view.Home == (LedgerHome{}) || !nonzeroDigest(view.Key) {
		return StorageKeyView{}, ErrCorrupt
	}
	switch view.Kind {
	case StoragePlanPage:
		if len(raw) != PageStorageKeyBytes {
			return StorageKeyView{}, ErrCorrupt
		}
		view.Ordinal = binary.BigEndian.Uint64(raw[66:74])
	case StoragePayloadChunk:
		if len(raw) != PayloadStorageKeyBytes {
			return StorageKeyView{}, ErrCorrupt
		}
		copy(view.Content[:], raw[66:98])
		if !nonzeroDigest(view.Content) {
			return StorageKeyView{}, ErrCorrupt
		}
		view.Ordinal = binary.BigEndian.Uint64(raw[98:106])
	case StorageHead, StoragePending, StorageTerminal, StorageAck, StorageContinuation,
		StoragePayloadBuild, StorageRoutePin, StoragePrepared, StorageSchemaPin:
		if len(raw) != FixedStorageKeyBytes {
			return StorageKeyView{}, ErrCorrupt
		}
	default:
		return StorageKeyView{}, ErrCorrupt
	}
	return view, nil
}
