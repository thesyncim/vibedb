package storeio

import (
	"bytes"
	"fmt"
)

// ConservativeScalarReplacementBatchPayload returns the physical payload
// bound that a compact stripe may need after a set of certified scalar
// replacements. The caller binds the returned bound to the exact immutable
// leaf and stream columns for the overlay epoch. A later batch may reuse that
// bound only for the same certified columns; a different column needs a new
// reservation or falls back to the complete COW planner.
//
// An integer stream always has a FOR representation no larger than
// 12-byte-header + 8-byte-base + 8 bytes per row. The encoder may retain a
// dictionary when it is within 25% of its best candidate, so the returned bound
// charges that factor as well. The trailing summary section is charged at its
// complete 4 KiB ceiling whenever summaries exist. A caller must compare the
// returned value with the compact leaf payload limit before publishing an
// overlay record; a false result declines the row-overlay lane.
func (v *CompactPrimaryStripeView) ConservativeScalarReplacementBatchPayload(
	replacements []CommonPrimaryUnifiedReplacement,
) (payload int, ok bool, err error) {
	if v == nil || len(v.payload) == 0 || len(replacements) == 0 ||
		v.shapeCount == 0 {
		return 0, false, nil
	}
	type changedStream struct{ shape, hole int }
	changed := make([]changedStream, 0, len(replacements))
	for replacementIndex := range replacements {
		replacement := &replacements[replacementIndex]
		if len(replacement.Key) == 0 || len(replacement.Value) == 0 ||
			!replacement.ScalarPatch.valid() {
			return 0, false, nil
		}
		for previous := 0; previous < replacementIndex; previous++ {
			if bytes.Equal(replacements[previous].Key, replacement.Key) {
				return 0, false, nil
			}
		}
		rank, found := v.FindKey(replacement.Key)
		if !found || v.IsOverflow(rank) {
			return 0, false, nil
		}
		if v.Len() <= CommonPrimaryLeafWideSlots {
			admittedSlot, slotOK := v.PostingSlot(rank)
			if !slotOK || admittedSlot != replacement.Slot {
				return 0, false, nil
			}
		}
		if replacement.ScalarPatch.exact() {
			base, decoded := v.AppendValue(nil, rank)
			if !decoded || !bytes.Equal(base, replacement.Value) {
				return 0, false, nil
			}
			continue
		}
		shape, hole, columnOK, columnErr := v.ScalarReplacementColumn(
			replacement.Key, replacement.Slot, replacement.ScalarPatch,
		)
		if columnErr != nil {
			return 0, false, columnErr
		}
		if !columnOK {
			return 0, false, nil
		}
		entry, entryOK := v.shapeEntry(shape)
		if !entryOK {
			return 0, false, fmt.Errorf(
				"%w: scalar batch shape", ErrCommonPrimaryLeafCorrupt,
			)
		}
		start := int(replacement.ScalarPatch.canonicalOffset)
		end := start + int(replacement.ScalarPatch.canonicalLength)
		if hole < 0 || hole >= entry.template.holes || start < 0 ||
			end <= start || end > len(replacement.Value) ||
			len(replacement.Value[start:end]) == 0 {
			return 0, false, nil
		}
		if _, integer := CanonicalIntValue(replacement.Value[start:end]); !integer {
			return 0, false, nil
		}
		streamRaw := entry.streamRaw
		var target compactStreamView
		for at := 0; at <= hole; at++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false, fmt.Errorf(
					"%w: scalar batch source stream", ErrCommonPrimaryLeafCorrupt,
				)
			}
			if at == hole {
				target = stream
				break
			}
			streamRaw = streamRaw[stream.encoded:]
		}
		if replacement.ScalarPatch.oldBodyLength() <= 0 ||
			!compactIntegerStream(target) {
			return 0, false, nil
		}
		foundStream := false
		for _, candidate := range changed {
			if candidate.shape == shape && candidate.hole == hole {
				foundStream = true
				break
			}
		}
		if !foundStream {
			changed = append(changed, changedStream{shape: shape, hole: hole})
		}
	}

	payload = len(v.payload)
	for _, changedStream := range changed {
		entry, entryOK := v.shapeEntry(changedStream.shape)
		if !entryOK {
			return 0, false, fmt.Errorf(
				"%w: scalar batch shape directory", ErrCommonPrimaryLeafCorrupt,
			)
		}
		streamRaw := entry.streamRaw
		for hole := 0; hole <= changedStream.hole; hole++ {
			stream, admitted := admittedCompactStream(streamRaw)
			if !admitted {
				return 0, false, fmt.Errorf(
					"%w: scalar batch stream directory", ErrCommonPrimaryLeafCorrupt,
				)
			}
			if hole == changedStream.hole {
				if !compactIntegerStream(stream) {
					return 0, false, nil
				}
				bound, boundOK := conservativeIntegerStreamBytes(stream.count)
				if !boundOK || bound < stream.encoded ||
					payload > int(^uint(0)>>1)-bound+stream.encoded {
					return 0, false, nil
				}
				payload += bound - stream.encoded
			}
			streamRaw = streamRaw[stream.encoded:]
		}
	}
	if len(changed) != 0 && v.summaryCount != 0 {
		if len(v.summaryRaw) > CompactPrimarySummaryMaxBytes ||
			payload > int(^uint(0)>>1)-CompactPrimarySummaryMaxBytes+
				len(v.summaryRaw) {
			return 0, false, nil
		}
		payload += CompactPrimarySummaryMaxBytes - len(v.summaryRaw)
	}
	return payload, true, nil
}

// ScalarReplacementColumn resolves the immutable compact shape and hole named
// by a scalar certificate. Exact replacements return (-1,-1,true,nil), since
// they do not alter a stream. The method is deliberately a narrow verifier:
// callers still need to validate the replacement spelling and use the returned
// coordinates to bind any cumulative fold reservation.
func (v *CompactPrimaryStripeView) ScalarReplacementColumn(
	key []byte,
	slot uint8,
	patch CommonPrimaryUnifiedScalarPatch,
) (shape, hole int, ok bool, err error) {
	if v == nil || len(key) == 0 || !patch.valid() {
		return 0, 0, false, nil
	}
	rank, found := v.FindKey(key)
	if !found || v.IsOverflow(rank) {
		return 0, 0, false, nil
	}
	if v.Len() <= CommonPrimaryLeafWideSlots {
		admittedSlot, slotOK := v.PostingSlot(rank)
		if !slotOK || admittedSlot != slot {
			return 0, 0, false, nil
		}
	}
	if patch.exact() {
		return -1, -1, true, nil
	}
	shape = v.rowShape(rank)
	entry, entryOK := v.shapeEntry(shape)
	if !entryOK {
		return 0, 0, false, fmt.Errorf(
			"%w: scalar replacement shape", ErrCommonPrimaryLeafCorrupt,
		)
	}
	hole = int(patch.bodyOffset)
	if hole < 0 || hole >= entry.template.holes {
		return 0, 0, false, nil
	}
	streamRaw := entry.streamRaw
	for at := 0; at <= hole; at++ {
		stream, admitted := admittedCompactStream(streamRaw)
		if !admitted {
			return 0, 0, false, fmt.Errorf(
				"%w: scalar replacement stream", ErrCommonPrimaryLeafCorrupt,
			)
		}
		if at == hole {
			return shape, hole, true, nil
		}
		streamRaw = streamRaw[stream.encoded:]
	}
	return 0, 0, false, nil
}

// compactIntegerStream is a codec proof for the representations emitted from
// canonical integer values. Dictionary streams are the one case that needs a
// bounded scan: the row IDs only repeat dictionary entries, so checking each
// unique spelling is enough to prove the whole stream numeric. PrefixInt is
// deliberately declined: a canonical target row does not prove that every
// reconstructed row is canonical (for example, a shared minus prefix can turn
// another row's zero into negative zero). Front/alphabet/date streams likewise
// have no constant-time whole-stream integer proof.
func compactIntegerStream(stream compactStreamView) bool {
	switch stream.kind {
	case compactStreamFOR, compactStreamDelta, compactStreamDeltaPack:
		return true
	case compactStreamDictionary:
		for id := 0; id < stream.dictCount; id++ {
			value, ok := stream.dictionaryEntry(id)
			if !ok {
				return false
			}
			if _, integer := CanonicalIntValue(value); !integer {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func conservativeIntegerStreamBytes(rows int) (int, bool) {
	if rows < 0 {
		return 0, false
	}
	base := uint64(compactStreamHeader) + 8
	if uint64(rows) > (^uint64(0)-base)/8 {
		return 0, false
	}
	base += uint64(rows) * 8
	if base > uint64(^uint(0)>>1) {
		return 0, false
	}
	bound := base + base/4
	if bound > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(bound), true
}
