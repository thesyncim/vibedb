package replicatedstate

import (
	"bytes"
	"math"
	"strconv"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

// materializeJSONInt64Delta evaluates a JID1 operation against the row read
// from the apply snapshot. It returns a normal JSON document for the existing
// schema, index, ownership, and data-chain validation pipeline. A malformed
// descriptor, non-integer current value, arithmetic overflow, or non-object
// row is a deterministic invalid-document result; transaction apply converts
// that caller-data result into its normal abort vote while still advancing the
// Raft log.
func materializeJSONInt64Delta(
	document, descriptor []byte, maxBytes int,
) ([]byte, uint32) {
	column, delta, ok := replication.OpenJSONInt64Delta(descriptor)
	if !ok || maxBytes <= 0 {
		return nil, ResultInvalidDocument
	}
	parsed, err := vibejson.ParseOptions(document, vibejson.Options{ZeroCopy: true})
	if err != nil {
		return nil, ResultInvalidDocument
	}
	root := parsed.Node()
	iter, ok := root.ObjectIter()
	if !ok {
		return nil, ResultInvalidDocument
	}
	memberCount, _ := root.ObjectLen()
	targetMember := -1
	var currentValue []byte
	keyScratch := make([]byte, 0, len(column)+8)
	for member := 0; ; member++ {
		key, value, ok := iter.NextRaw()
		if !ok {
			break
		}
		keyScratch, ok = jsonDeltaKeyBytes(key, column, keyScratch)
		if !ok {
			continue
		}
		targetMember = member
		currentValue = value.Bytes()
	}

	newValue := []byte("null")
	var number [20]byte
	if currentValue != nil {
		raw := vibejson.RawValue{Src: currentValue}
		if raw.IsNull() {
			// SQL NULL arithmetic remains NULL. The regular final schema
			// validator decides whether this column permits NULL.
		} else {
			current, currentOK := raw.Int64()
			if !currentOK {
				return nil, ResultInvalidDocument
			}
			next, nextOK := addInt64Delta(current, delta)
			if !nextOK {
				return nil, ResultInvalidDocument
			}
			newValue = strconv.AppendInt(number[:0], next, 10)
		}
	}

	var addedKey []byte
	if targetMember < 0 {
		columnText := string(column)
		addedKey, err = vibejson.Marshal(&columnText)
		if err != nil {
			return nil, ResultInvalidDocument
		}
	}
	updatedBytes := 2 // braces
	if memberCount > 1 {
		updatedBytes += memberCount - 1
	}
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, ok := iter.NextRaw()
		if !ok {
			break
		}
		valueBytes := value.Bytes()
		if member == targetMember {
			valueBytes = newValue
		}
		if !jsonDeltaAddSize(&updatedBytes, len(key.Bytes())+1+len(valueBytes), maxBytes) {
			return nil, ResultTargetBound
		}
	}
	if targetMember < 0 {
		if memberCount != 0 && !jsonDeltaAddSize(&updatedBytes, 1, maxBytes) {
			return nil, ResultTargetBound
		}
		if !jsonDeltaAddSize(&updatedBytes, len(addedKey)+1+len(newValue), maxBytes) {
			return nil, ResultTargetBound
		}
	}
	if updatedBytes > maxBytes {
		return nil, ResultTargetBound
	}

	updated := make([]byte, 0, updatedBytes)
	updated = append(updated, '{')
	wrote := 0
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, ok := iter.NextRaw()
		if !ok {
			break
		}
		if wrote != 0 {
			updated = append(updated, ',')
		}
		updated = key.AppendJSON(updated)
		updated = append(updated, ':')
		if member == targetMember {
			updated = append(updated, newValue...)
		} else {
			updated = value.AppendJSON(updated)
		}
		wrote++
	}
	if targetMember < 0 {
		if wrote != 0 {
			updated = append(updated, ',')
		}
		updated = append(updated, addedKey...)
		updated = append(updated, ':')
		updated = append(updated, newValue...)
	}
	updated = append(updated, '}')
	return updated, ResultApplied
}

func jsonDeltaKeyBytes(key vibejson.RawValue, want, scratch []byte) ([]byte, bool) {
	if raw, clean := key.StringBytes(); clean {
		if bytes.Equal(raw, want) {
			return raw, true
		}
		return nil, false
	}
	decoded, clean, err := key.AppendText(scratch[:0])
	if err != nil || !clean || !bytes.Equal(decoded, want) {
		return decoded, false
	}
	return decoded, true
}

func addInt64Delta(current, delta int64) (int64, bool) {
	if delta > 0 && current > math.MaxInt64-delta {
		return 0, false
	}
	if delta < 0 && current < math.MinInt64-delta {
		return 0, false
	}
	return current + delta, true
}

func jsonDeltaAddSize(total *int, add, max int) bool {
	if total == nil || add < 0 || max < 0 || *total > max || add > max-*total {
		return false
	}
	*total += add
	return true
}
