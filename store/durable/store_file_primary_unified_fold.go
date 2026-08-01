package durable

import (
	"github.com/thesyncim/vibedb/internal/storeio"
)

// primaryUnifiedFixedReplacements coalesces one bucket's overlay to its final
// record per key and exposes the all-Put subset to the native class-5 patcher.
// Inserts and delete->different-value restores are allowed through here: the
// codec checks that every final key and slot already exists in the admitted
// base and declines otherwise. A final delete cannot preserve the envelope and
// takes the complete planner.
func (o *primaryUnifiedOverlay) primaryUnifiedFixedReplacements(
	dst []storeio.CommonPrimaryUnifiedReplacement,
	bucket storeio.BucketID,
	generation uint64,
) ([]storeio.CommonPrimaryUnifiedReplacement, bool, error) {
	if o == nil || cap(dst) < storeio.CommonPrimaryLeafWideSlots {
		return dst[:0], false, nil
	}
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	count, err := o.latestBucketRecords(&indexes, bucket, generation)
	if err != nil {
		return dst[:0], false, err
	}
	if count == 0 {
		return dst[:0], false, nil
	}
	dst = dst[:0]
	for _, index := range indexes[:count] {
		if int(index) >= len(o.records) {
			return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		record := &o.records[index]
		keyEnd := record.keyOffset + uint32(record.keyLen)
		valueEnd := record.valueOff + record.valueLen
		if record.kind != primaryUnifiedOverlayPut {
			return dst[:0], false, nil
		}
		if record.keyLen == 0 || record.valueLen == 0 ||
			keyEnd > uint32(len(o.arena)) ||
			valueEnd > uint32(len(o.arena)) {
			return dst[:0], false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		dst = append(dst, storeio.CommonPrimaryUnifiedReplacement{
			Key:   o.arena[record.keyOffset:keyEnd:keyEnd],
			Value: o.arena[record.valueOff:valueEnd:valueEnd],
			Slot:  record.slot,
		})
	}
	return dst, true, nil
}
