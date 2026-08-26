package routegate

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"slices"
)

var snapshotMagic = [4]byte{'V', 'R', 'G', 'S'}

// SnapshotBytes returns the exact encoded bytes for count pin records.
func SnapshotBytes(count uint64) (uint64, bool) {
	if count > MaxRetainedRecords {
		return 0, false
	}
	return SnapshotHeaderBytes + count*SnapshotRecordBytes + SnapshotChecksumBytes, true
}

// AppendSnapshot appends one canonical machine image. scratch must have room
// for every retained record; it is caller-owned so repeated snapshots can be
// allocation-free. Records are sorted by identity to make the image unique.
func AppendSnapshot(dst []byte, machine *Machine, scratch []PinRecord) ([]byte, error) {
	if machine == nil || machine.retainedPins != uint64(len(machine.pins)) {
		return dst, ErrCorrupt
	}
	count := machine.retainedPins
	total, ok := SnapshotBytes(count)
	if !ok {
		return dst, ErrTooLarge
	}
	if uint64(len(scratch)) < count {
		return dst, ErrScratch
	}
	records := scratch[:int(count)]
	index := 0
	for identity, record := range machine.pins {
		records[index] = PinRecord{
			Identity: identity, Binding: record.Binding, Epoch: record.Epoch, State: record.State,
		}
		index++
	}
	slices.SortFunc(records, func(left, right PinRecord) int {
		return bytes.Compare(left.Identity[:], right.Identity[:])
	})

	start := len(dst)
	dst = append(dst, make([]byte, int(total))...)
	body := dst[start : start+int(total)-SnapshotChecksumBytes]
	copy(body[:4], snapshotMagic[:])
	binary.LittleEndian.PutUint64(body[8:16], machine.revision)
	binary.LittleEndian.PutUint64(body[16:24], machine.epoch)
	binary.LittleEndian.PutUint64(body[24:32], count)
	binary.LittleEndian.PutUint64(body[32:40], machine.activePins)
	binary.LittleEndian.PutUint64(body[40:48], machine.releasedPins)
	body[48] = byte(machine.drain.State)
	binary.LittleEndian.PutUint64(body[56:64], machine.drain.Epoch)
	copy(body[64:96], machine.drain.Identity[:])
	copy(body[96:128], machine.drain.Binding[:])
	binary.LittleEndian.PutUint64(body[128:136], count*SnapshotRecordBytes)
	for recordIndex, record := range records {
		offset := SnapshotHeaderBytes + recordIndex*SnapshotRecordBytes
		encoded := body[offset : offset+SnapshotRecordBytes]
		copy(encoded[:32], record.Identity[:])
		copy(encoded[32:64], record.Binding[:])
		binary.LittleEndian.PutUint64(encoded[64:72], record.Epoch)
		encoded[72] = byte(record.State)
	}
	binary.LittleEndian.PutUint32(
		dst[start+int(total)-SnapshotChecksumBytes:start+int(total)],
		crc32.Checksum(body, castagnoli),
	)
	return dst, nil
}

// OpenSnapshot validates and restores one canonical image. maxRecords is the
// local state admission bound and may be lower than the grammar's hard bound.
func OpenSnapshot(raw []byte, maxRecords uint64) (*Machine, error) {
	if len(raw) < SnapshotHeaderBytes+SnapshotChecksumBytes ||
		uint64(len(raw)) > MaxSnapshotBytes || maxRecords == 0 ||
		maxRecords > MaxRetainedRecords {
		if uint64(len(raw)) > MaxSnapshotBytes || maxRecords > MaxRetainedRecords {
			return nil, ErrTooLarge
		}
		return nil, ErrCorrupt
	}
	body := raw[:len(raw)-SnapshotChecksumBytes]
	if body[0] != snapshotMagic[0] || body[1] != snapshotMagic[1] ||
		body[2] != snapshotMagic[2] || body[3] != snapshotMagic[3] ||
		!allZero(body[4:8]) || !allZero(body[49:56]) ||
		binary.LittleEndian.Uint32(raw[len(body):]) != crc32.Checksum(body, castagnoli) {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint64(body[24:32])
	total, ok := SnapshotBytes(count)
	if !ok || total != uint64(len(raw)) || count > maxRecords ||
		binary.LittleEndian.Uint64(body[128:136]) != count*SnapshotRecordBytes {
		if count > maxRecords || !ok {
			return nil, ErrTooLarge
		}
		return nil, ErrCorrupt
	}
	machine := &Machine{
		revision:     binary.LittleEndian.Uint64(body[8:16]),
		epoch:        binary.LittleEndian.Uint64(body[16:24]),
		activePins:   binary.LittleEndian.Uint64(body[32:40]),
		releasedPins: binary.LittleEndian.Uint64(body[40:48]),
		retainedPins: count,
		maxRecords:   maxRecords, pins: make(map[Identity]storedPin, int(count)),
	}
	machine.drain.State = DrainState(body[48])
	machine.drain.Epoch = binary.LittleEndian.Uint64(body[56:64])
	copy(machine.drain.Identity[:], body[64:96])
	copy(machine.drain.Binding[:], body[96:128])
	if machine.epoch == 0 || !validDrainSnapshot(machine.drain, machine.epoch, machine.activePins) {
		return nil, ErrCorrupt
	}

	var prior Identity
	var active, released uint64
	for index := uint64(0); index < count; index++ {
		offset := uint64(SnapshotHeaderBytes) + index*SnapshotRecordBytes
		encoded := body[int(offset):int(offset+SnapshotRecordBytes)]
		if !allZero(encoded[73:80]) {
			return nil, ErrCorrupt
		}
		record := PinRecord{Epoch: binary.LittleEndian.Uint64(encoded[64:72]), State: PinState(encoded[72])}
		copy(record.Identity[:], encoded[:32])
		copy(record.Binding[:], encoded[32:64])
		if record.Identity == (Identity{}) || record.Binding == (Binding{}) ||
			record.Epoch == 0 || record.Epoch > machine.epoch ||
			(index != 0 && bytes.Compare(prior[:], record.Identity[:]) >= 0) {
			return nil, ErrCorrupt
		}
		switch record.State {
		case PinHeld:
			active++
		case PinReleased:
			released++
		default:
			return nil, ErrCorrupt
		}
		machine.pins[record.Identity] = storedPin{
			Binding: record.Binding, Epoch: record.Epoch, State: record.State,
		}
		prior = record.Identity
	}
	if active != machine.activePins || released != machine.releasedPins ||
		active+released != count {
		return nil, ErrCorrupt
	}
	return machine, nil
}

func validDrainSnapshot(drain DrainRecord, epoch, activePins uint64) bool {
	switch drain.State {
	case DrainNone:
		return drain == (DrainRecord{})
	case DrainPending:
		return drain.Identity != (Identity{}) && drain.Binding != (Binding{}) &&
			drain.Epoch == epoch && activePins != 0
	case DrainActive:
		return drain.Identity != (Identity{}) && drain.Binding != (Binding{}) &&
			drain.Epoch == epoch && activePins == 0
	case DrainReleased:
		return drain.Identity != (Identity{}) && drain.Binding != (Binding{}) &&
			drain.Epoch != 0 && drain.Epoch < epoch
	default:
		return false
	}
}

func allZero(raw []byte) bool {
	var combined byte
	for _, value := range raw {
		combined |= value
	}
	return combined == 0
}
