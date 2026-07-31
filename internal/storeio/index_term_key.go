package storeio

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/orderedkey"
)

const (
	// IndexTermMaxComponents matches the collection's bounded compound-index
	// width. Keeping the bound here lets storage reject an untrusted tuple
	// without importing the owning store package and creating a package cycle.
	IndexTermMaxComponents = 4

	// IndexTermMaxKeyBytes bounds the complete canonical tuple, including its
	// format byte and terminator. Terms above this limit remain eligible for a
	// residual document recheck instead of forcing an unbounded index key.
	IndexTermMaxKeyBytes = 4096

	indexTermKeyVersion    = byte(DevelopmentFormatVersion)
	indexTermKeyTerminator = byte(0)
)

// IndexTermKind is the JSON family carried by one canonical index component.
// Array and object are named so callers can translate their parser's kind
// without guessing, but AppendIndexTermKey deliberately rejects both: exact
// index terms are scalar tuples.
type IndexTermKind uint8

const (
	IndexTermInvalid IndexTermKind = iota
	IndexTermNull
	IndexTermBool
	IndexTermNumber
	IndexTermString
	IndexTermArray
	IndexTermObject
)

// IndexTermDirection is persisted as part of each ordered scalar component.
// Ascending is intentionally non-zero: callers must select it explicitly,
// rather than accidentally turning a zero-valued schema field into durable
// ordering semantics. A future descending format can add another value.
type IndexTermDirection uint8

const (
	IndexTermAscending IndexTermDirection = 1
)

// IndexTermComponent is one exact JSON scalar in a compound index term. JSON
// is the complete raw scalar spelling: "null", "true"/"false", a JSON number,
// or a quoted JSON string. Strings are decoded while encoding, and numbers are
// normalized by semantic decimal value, so equivalent JSON spellings produce
// one canonical key.
type IndexTermComponent struct {
	Kind      IndexTermKind
	Direction IndexTermDirection
	JSON      []byte
}

// AppendIndexTermKey appends one versioned, prefix-free canonical scalar tuple.
// Components use orderedkey's type order (null, false, true, number, string)
// and are concatenated in declared tuple order. The trailing byte makes a
// shorter tuple distinct from, and sort before, every tuple that extends it.
//
// False rejects an empty or over-wide tuple, a non-ascending component,
// malformed or mismatched JSON, a container, or an oversized canonical key.
// On failure the returned slice is exactly dst; no partial append is exposed.
func AppendIndexTermKey(dst []byte, components []IndexTermComponent) ([]byte, bool) {
	original := dst
	if len(components) == 0 || len(components) > IndexTermMaxComponents {
		return original, false
	}

	start := len(dst)
	dst = append(dst, indexTermKeyVersion)
	for _, component := range components {
		if component.Direction != IndexTermAscending ||
			len(component.JSON) == 0 ||
			len(component.JSON) > IndexTermMaxKeyBytes {
			return original, false
		}

		var ok bool
		switch component.Kind {
		case IndexTermNull:
			if !bytes.Equal(component.JSON, []byte("null")) {
				return original, false
			}
			dst, ok = orderedkey.AppendNull(dst, orderedkey.Ascending)
		case IndexTermBool:
			switch {
			case bytes.Equal(component.JSON, []byte("false")):
				dst, ok = orderedkey.AppendBool(dst, false, orderedkey.Ascending)
			case bytes.Equal(component.JSON, []byte("true")):
				dst, ok = orderedkey.AppendBool(dst, true, orderedkey.Ascending)
			default:
				return original, false
			}
		case IndexTermNumber:
			dst, ok = orderedkey.AppendNumber(dst, component.JSON, orderedkey.Ascending)
		case IndexTermString:
			dst, ok = orderedkey.AppendJSONString(dst, component.JSON, orderedkey.Ascending)
		default:
			return original, false
		}
		if !ok || len(dst)-start >= IndexTermMaxKeyBytes {
			return original, false
		}
	}
	dst = append(dst, indexTermKeyTerminator)
	if len(dst)-start > IndexTermMaxKeyBytes {
		return original, false
	}
	return dst, true
}

// ValidIndexTermKey reports whether key is one complete canonical ascending
// tuple emitted by AppendIndexTermKey. It performs no allocation. orderedkey's
// ascending tags all have their high bit clear; descending tags have it set.
func ValidIndexTermKey(key []byte) bool {
	if len(key) < 3 || len(key) > IndexTermMaxKeyBytes ||
		key[0] != indexTermKeyVersion ||
		key[len(key)-1] != indexTermKeyTerminator {
		return false
	}
	body := key[1 : len(key)-1]
	count := 0
	var decodedStorage [IndexTermMaxKeyBytes + 32]byte
	var canonicalStorage [IndexTermMaxKeyBytes]byte
	for offset := 0; offset < len(body); {
		if body[offset]&0x80 != 0 || count == IndexTermMaxComponents {
			return false
		}
		component, decoded, next, err := orderedkey.DecodeComponent(
			decodedStorage[:0], body, offset,
		)
		if err != nil || component.Descending || next <= offset {
			return false
		}
		var canonical []byte
		var ok bool
		switch component.Kind {
		case orderedkey.KindNull:
			canonical, ok = orderedkey.AppendNull(canonicalStorage[:0], orderedkey.Ascending)
		case orderedkey.KindBool:
			canonical, ok = orderedkey.AppendBool(canonicalStorage[:0], component.Bool, orderedkey.Ascending)
		case orderedkey.KindNumber:
			canonical, ok = orderedkey.AppendNumber(
				canonicalStorage[:0],
				decoded[component.PayloadStart:component.PayloadEnd],
				orderedkey.Ascending,
			)
		case orderedkey.KindString:
			canonical, ok = orderedkey.AppendString(
				canonicalStorage[:0],
				decoded[component.PayloadStart:component.PayloadEnd],
				orderedkey.Ascending,
			)
		default:
			return false
		}
		if !ok || !bytes.Equal(canonical, body[offset:next]) {
			return false
		}
		offset = next
		count++
	}
	return count != 0
}

// IndexTermRouteHash returns the StoreID-keyed SipHash routing fingerprint of
// a canonical term. The hash selects candidates only; it is never identity.
func IndexTermRouteHash(storeID [16]byte, canonical []byte) uint64 {
	return KeyHashBytes(storeID, canonical)
}

// IndexTermKeyRecord pairs a routing fingerprint with the complete canonical
// identity bytes. Canonical borrows caller-owned immutable storage.
type IndexTermKeyRecord struct {
	RouteHash uint64
	Canonical []byte
}

// OpenIndexTermKeyRecord validates canonical and builds its keyed route.
func OpenIndexTermKeyRecord(storeID [16]byte, canonical []byte) (IndexTermKeyRecord, bool) {
	if !ValidIndexTermKey(canonical) {
		return IndexTermKeyRecord{}, false
	}
	return IndexTermKeyRecord{
		RouteHash: IndexTermRouteHash(storeID, canonical),
		Canonical: canonical,
	}, true
}

// SameIdentity uses RouteHash as a fast rejection and always compares the full
// canonical bytes before accepting a match. Equal SipHash values therefore
// cannot merge distinct terms.
func (record IndexTermKeyRecord) SameIdentity(other IndexTermKeyRecord) bool {
	return record.RouteHash == other.RouteHash &&
		bytes.Equal(record.Canonical, other.Canonical)
}

// Matches reports whether a routed candidate is this exact canonical term.
func (record IndexTermKeyRecord) Matches(routeHash uint64, canonical []byte) bool {
	return record.RouteHash == routeHash &&
		bytes.Equal(record.Canonical, canonical)
}
