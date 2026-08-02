// Derived from Vitess (github.com/vitessio/vitess,
// go/vt/vtgate/vindexes/multicol.go and cfc.go), Apache License 2.0. The
// differential harness pins upstream v0.24.2; the reproduced multicol and
// prefix-range behavior is unchanged from v0.22.0 (the xxhash/cfc code is
// byte-identical and multicol differs only cosmetically). See LICENSE-VITESS
// and docs/provenance.md (ALGO-VITESS-MULTICOL-001).

package vitessroute

import (
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
)

// resolveColumnBytes reproduces Vitess getColumnBytes and then narrows the
// result to this compatibility profile. Explicit widths are read in column
// order; any unspecified trailing columns split the leftover bytes with the
// same ceil(remaining/pending) distribution Vitess uses. The profile then
// requires the widths to total exactly the 8-byte keyspace width with every
// column at least one byte, so a full key always yields an exactly-8-byte
// point. present distinguishes an absent column_bytes param from an empty one.
//
// Provenance: ALGO-VITESS-MULTICOL-001.
func resolveColumnBytes(columnCount int, columnBytes string, present bool) ([]int, error) {
	var parts []string
	if present {
		parts = strings.Split(columnBytes, ",")
	}
	if len(parts) > columnCount {
		return nil, &ConfigError{Reason: "column_bytes lists more entries than column_count"}
	}

	widths := make([]int, columnCount)
	defined := make([]bool, columnCount)
	bytesUsed := 0
	definedCount := 0
	for idx, part := range parts {
		if part == "" {
			// Vitess treats an empty entry as an unspecified column.
			continue
		}
		w, err := strconv.Atoi(part)
		if err != nil {
			return nil, &ConfigError{Reason: "column_bytes entry is not an integer"}
		}
		if w < 0 {
			return nil, &ConfigError{Reason: "column_bytes entry is negative"}
		}
		widths[idx] = w
		defined[idx] = true
		bytesUsed += w
		definedCount++
	}

	pendingCol := columnCount - definedCount
	remainingBytes := distribution.KeyspaceWidth - bytesUsed
	if pendingCol > remainingBytes {
		return nil, &ConfigError{Reason: "column_bytes total exceeds the 8-byte keyspace width"}
	}
	for idx := 0; idx < columnCount && pendingCol > 0; idx++ {
		if defined[idx] {
			continue
		}
		w := (remainingBytes + pendingCol - 1) / pendingCol // ceil(remaining/pending)
		widths[idx] = w
		remainingBytes -= w
		pendingCol--
	}

	total := 0
	for _, w := range widths {
		if w < 1 {
			return nil, &ConfigError{Reason: "every multicol column must map to at least one byte"}
		}
		if w > distribution.KeyspaceWidth {
			return nil, &ConfigError{Reason: "a multicol column cannot exceed the 8-byte keyspace width"}
		}
		total += w
	}
	if total != distribution.KeyspaceWidth {
		return nil, &ConfigError{Reason: "multicol column_bytes must total exactly 8 bytes"}
	}
	return widths, nil
}

// mapMultiCol reproduces Vitess MultiCol.mapKsid over the fixed 8-byte keyspace.
// Each supplied leading column is hashed independently by the xxhash sub-vindex
// to eight bytes, truncated to its configured width, and the parts concatenate
// in column order. A full key (len(values) == len(columnBytes)) fills all eight
// bytes and yields one point; a shorter leading prefix yields the half-open
// range Vitess NewKeyRangeFromPrefix derives from the partial keyspace id.
//
// Provenance: ALGO-VITESS-MULTICOL-001.
func mapMultiCol(columnBytes []int, values []distribution.Scalar) (distribution.DestinationSet, error) {
	ksid := make([]byte, 0, distribution.KeyspaceWidth)
	for idx := range values {
		h, err := xxhashPoint(values[idx])
		if err != nil {
			return distribution.DestinationSet{}, err
		}
		ksid = append(ksid, h[:columnBytes[idx]]...)
	}

	if len(values) == len(columnBytes) {
		var p distribution.KeyspacePoint
		copy(p[:], ksid) // ksid is exactly 8 bytes for a full key
		return distribution.DestinationSet{Points: []distribution.KeyspacePoint{p}}, nil
	}
	return distribution.DestinationSet{Ranges: []distribution.KeyRange{prefixRange(ksid)}}, nil
}

// prefixRange reproduces Vitess NewKeyRangeFromPrefix/addOne for a partial
// keyspace id of length 1..7. The start is the prefix zero-padded to eight
// bytes (the least id sharing that prefix); the exclusive end is addOne(prefix)
// zero-padded, where addOne increments the last non-0xff byte and zeroes the
// trailing 0xff run. An all-0xff prefix overflows to the open maximum end,
// distribution.KeyspaceEnd.Max (exactly 2^64).
//
// Provenance: ALGO-VITESS-MULTICOL-001.
func prefixRange(prefix []byte) distribution.KeyRange {
	var start distribution.KeyspacePoint
	copy(start[:], prefix)

	end := make([]byte, len(prefix))
	copy(end, prefix)
	overflow := true
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			overflow = false
			break
		}
		end[i] = 0
	}
	if overflow {
		return distribution.KeyRange{Start: start, End: distribution.KeyspaceEnd{Max: true}}
	}

	var endPoint distribution.KeyspacePoint
	copy(endPoint[:], end)
	return distribution.KeyRange{Start: start, End: distribution.KeyspaceEnd{Point: endPoint}}
}
