package gateway

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// ReplicatedCatalogDistribution and ReplicatedCatalogShard are the one
	// reserved placement identity for control-plane state. They are carried on
	// every resolved route and replicated command, so a catalog authority can
	// never be constructed over a tenant shard by configuration accident.
	ReplicatedCatalogDistribution distribution.DistributionName = "catalog"
	ReplicatedCatalogShard        distribution.ShardID          = "controlplane"
	// ReplicatedCatalogTable is the dedicated SQL relation owned by the catalog
	// Raft group. Catalog state must not share a user-data table.
	ReplicatedCatalogTable = "controlplane"
	// ReplicatedCatalogPrimaryKey is the document primary key required by that
	// relation. Every row below carries this exact scalar and is independently
	// validated by replicated SQL apply before it can become visible.
	ReplicatedCatalogPrimaryKey = "/id"
)

const (
	controlPlaneOperationHexBytes = 64
	controlPlaneOperationIDBytes  = len("operation/") + controlPlaneOperationHexBytes
	// orderedkey string components add one type byte and a two-byte terminator.
	controlPlaneOperationKeyBytes = controlPlaneOperationIDBytes + 3
	controlPlaneEnvelopeBaseBytes = len(`{"id":"","payload":}`)
)

var (
	replicatedCatalogHeadDocumentID = [...]byte{
		'c', 'a', 't', 'a', 'l', 'o', 'g', '/', 'h', 'e', 'a', 'd',
	}
	replicatedCatalogHeadWitnessDocumentID = [...]byte{
		'c', 'a', 't', 'a', 'l', 'o', 'g', '/', 'w', 'i', 't', 'n', 'e', 's', 's',
	}
	replicatedCatalogGenesisDocumentID = [...]byte{
		'c', 'a', 't', 'a', 'l', 'o', 'g', '/', 'g', 'e', 'n', 'e', 's', 'i', 's',
	}
	replicatedOperationDirectoryDocumentID = [...]byte{
		'o', 'p', 'e', 'r', 'a', 't', 'i', 'o', 'n', '/',
		'd', 'i', 'r', 'e', 'c', 't', 'o', 'r', 'y',
	}
	replicatedOperationDocumentPrefix = [...]byte{
		'o', 'p', 'e', 'r', 'a', 't', 'i', 'o', 'n', '/',
	}
	controlPlaneDocumentPrefix = [...]byte{'{', '"', 'i', 'd', '"', ':', '"'}
	controlPlanePayloadMarker  = [...]byte{'"', ',', '"', 'p', 'a', 'y', 'l', 'o', 'a', 'd', '"', ':'}

	replicatedCatalogHeadKey        = fixedControlPlaneKey(replicatedCatalogHeadDocumentID[:])
	replicatedCatalogHeadWitnessKey = fixedControlPlaneKey(replicatedCatalogHeadWitnessDocumentID[:])
	replicatedCatalogGenesisKey     = fixedControlPlaneKey(replicatedCatalogGenesisDocumentID[:])
	replicatedOperationDirectoryKey = fixedControlPlaneKey(
		replicatedOperationDirectoryDocumentID[:],
	)
)

var lowerHex = [...]byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'a', 'b', 'c', 'd', 'e', 'f',
}

func fixedControlPlaneKey(id []byte) []byte {
	key, ok := orderedkey.AppendString(nil, id, orderedkey.Ascending)
	if !ok {
		panic("gateway: invalid static control-plane key")
	}
	return key
}

// appendReplicatedOperationDocumentID materializes the stable 256-bit
// operation identity as lowercase hexadecimal. Hex preserves unsigned byte
// order, has exactly one spelling, needs no escaping, and avoids a transient
// Go string or base64 workspace.
func appendReplicatedOperationDocumentID(dst []byte, id [32]byte) []byte {
	dst = append(dst, replicatedOperationDocumentPrefix[:]...)
	for _, value := range id {
		dst = append(dst, lowerHex[value>>4], lowerHex[value&0x0f])
	}
	return dst
}

// replicatedOperationKey constructs the SQL table's native ordered /id key in
// fixed caller-owned storage. The operational path performs no heap or string
// allocation.
func replicatedOperationKey(id [32]byte) [controlPlaneOperationKeyBytes]byte {
	var text [controlPlaneOperationIDBytes]byte
	identifier := appendReplicatedOperationDocumentID(text[:0], id)
	var storage [controlPlaneOperationKeyBytes]byte
	key, ok := orderedkey.AppendString(storage[:0], identifier, orderedkey.Ascending)
	if !ok || len(key) != len(storage) {
		panic("gateway: invalid operation control-plane key")
	}
	return storage
}

func openReplicatedOperationDocumentID(raw []byte) ([32]byte, []byte, error) {
	var id [32]byte
	text, payload, ok := openFixedControlPlaneDocument(raw, controlPlaneOperationIDBytes)
	if !ok ||
		!bytes.Equal(text[:len(replicatedOperationDocumentPrefix)],
			replicatedOperationDocumentPrefix[:]) {
		return id, nil, ErrReplicatedCatalog
	}
	hexadecimal := text[len(replicatedOperationDocumentPrefix):]
	for index := range id {
		high, highOK := lowerHexNibble(hexadecimal[index*2])
		low, lowOK := lowerHexNibble(hexadecimal[index*2+1])
		if !highOK || !lowOK {
			return [32]byte{}, nil, ErrReplicatedCatalog
		}
		id[index] = high<<4 | low
	}
	if id == ([32]byte{}) || len(raw) > MaxReplicatedOperationBytes {
		return [32]byte{}, nil, ErrReplicatedCatalog
	}
	return id, payload, nil
}

func lowerHexNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	default:
		return 0, false
	}
}

// appendControlPlaneDocument wraps one canonical payload in the only
// control-plane row shape. The object member order is already vibejson's
// canonical lexical order (id, payload), and identifier bytes are restricted
// to the static ASCII grammar above, so no generic encoder or string exists on
// this path. Payload canonicalization still goes through vibejson.
func appendControlPlaneDocument(
	dst, identifier, payload []byte,
	maximum int,
) ([]byte, error) {
	start := len(dst)
	if len(identifier) == 0 || len(identifier) > controlPlaneOperationIDBytes ||
		len(payload) == 0 || maximum < controlPlaneEnvelopeBaseBytes+len(identifier) ||
		len(payload) > maximum-controlPlaneEnvelopeBaseBytes-len(identifier) {
		return dst, ErrReplicatedCatalog
	}
	dst = appendControlPlaneDocumentPrefix(dst, identifier)
	var err error
	dst, err = vibejson.AppendCanonicalize(dst, payload)
	if err != nil {
		return dst[:start], errors.Join(err, ErrReplicatedCatalog)
	}
	dst = append(dst, '}')
	if len(dst)-start > maximum {
		return dst[:start], ErrReplicatedCatalog
	}
	return dst, nil
}

// appendReplicatedCatalogDocument writes the bounded catalog payload directly
// behind its /id envelope. A large catalog therefore leaves one retained
// destination in the authority rather than separate payload and envelope
// arenas. The bounded snapshot encoder still performs the complete vibejson
// schema and canonicalization checks before this function closes the outer
// object.
func appendReplicatedCatalogDocument(
	dst []byte,
	snapshot *Snapshot,
	maximum int,
) ([]byte, error) {
	start := len(dst)
	identifier := replicatedCatalogHeadDocumentID[:]
	if snapshot == nil || maximum < controlPlaneEnvelopeBaseBytes+len(identifier) {
		return dst, ErrReplicatedCatalog
	}
	dst = appendControlPlaneDocumentPrefix(dst, identifier)
	var err error
	payloadMaximum := maximum - controlPlaneEnvelopeBaseBytes - len(identifier)
	dst, err = appendSnapshotDocumentBounded(dst, snapshot, payloadMaximum)
	if err != nil {
		return dst[:start], err
	}
	dst = append(dst, '}')
	if len(dst)-start > maximum {
		return dst[:start], ErrCatalogTooLarge
	}
	return dst, nil
}

func appendControlPlaneDocumentPrefix(dst, identifier []byte) []byte {
	dst = append(dst, controlPlaneDocumentPrefix[:]...)
	dst = append(dst, identifier...)
	return append(dst, controlPlanePayloadMarker[:]...)
}

// openControlPlaneDocument validates the complete generic envelope. Typed
// production callers use openTypedControlPlaneDocument instead so their
// schema decoder is the sole payload scan.
func openControlPlaneDocument(
	raw, identifier []byte,
	maximum int,
) ([]byte, error) {
	payload, err := openTypedControlPlaneDocument(raw, identifier, maximum)
	if err != nil || !vibejson.Valid(payload) {
		return nil, ErrReplicatedCatalog
	}
	return payload, nil
}

// openTypedControlPlaneDocument checks the fixed canonical framing, exact
// byte-native identity, and total bound without copying or scanning payload.
// Its typed caller decodes and byte-identically re-encodes the logical schema,
// which provides the one grammar and canonical-uniqueness witness.
func openTypedControlPlaneDocument(
	raw, identifier []byte,
	maximum int,
) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maximum || len(identifier) == 0 {
		return nil, ErrReplicatedCatalog
	}
	id, payload, ok := openFixedControlPlaneDocument(raw, len(identifier))
	if !ok || !bytes.Equal(id, identifier) {
		return nil, ErrReplicatedCatalog
	}
	return payload, nil
}

func openFixedControlPlaneDocument(
	raw []byte,
	identifierBytes int,
) (identifier, payload []byte, ok bool) {
	identifierStart := len(controlPlaneDocumentPrefix)
	identifierEnd := identifierStart + identifierBytes
	payloadStart := identifierEnd + len(controlPlanePayloadMarker)
	if identifierBytes <= 0 || len(raw) <= payloadStart || raw[len(raw)-1] != '}' ||
		!bytes.Equal(raw[:identifierStart], controlPlaneDocumentPrefix[:]) ||
		!bytes.Equal(raw[identifierEnd:payloadStart], controlPlanePayloadMarker[:]) {
		return nil, nil, false
	}
	identifier = raw[identifierStart:identifierEnd]
	payload = raw[payloadStart : len(raw)-1]
	if len(payload) == 0 {
		return nil, nil, false
	}
	return identifier, payload, true
}
