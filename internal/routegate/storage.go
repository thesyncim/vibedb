package routegate

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// HeadBytes is the exact durable shard-local gate-head value size.
	HeadBytes = 132
	// StoredPinBytes is the exact durable pin value size. Identity is omitted
	// because it is already the hidden-row key.
	StoredPinBytes = 52
)

var (
	headMagic = [4]byte{'V', 'R', 'G', 'H'}
	pinMagic  = [4]byte{'V', 'R', 'G', 'P'}
)

// AppendHead appends one canonical fixed gate status.
func AppendHead(dst []byte, status Status) ([]byte, error) {
	if !validStatus(status) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, HeadBytes)...)
	frame := dst[start:]
	copy(frame[:4], headMagic[:])
	binary.LittleEndian.PutUint64(frame[8:16], status.Revision)
	binary.LittleEndian.PutUint64(frame[16:24], status.Epoch)
	binary.LittleEndian.PutUint64(frame[24:32], status.ActivePins)
	binary.LittleEndian.PutUint64(frame[32:40], status.ReleasedPins)
	binary.LittleEndian.PutUint64(frame[40:48], status.RetainedRecords)
	frame[48] = byte(status.Drain.State)
	binary.LittleEndian.PutUint64(frame[56:64], status.Drain.Epoch)
	copy(frame[64:96], status.Drain.Identity[:])
	copy(frame[96:128], status.Drain.Binding[:])
	binary.LittleEndian.PutUint32(frame[128:132], crc32.Checksum(frame[:128], castagnoli))
	return dst, nil
}

// OpenHead validates one exact durable gate status.
func OpenHead(raw []byte) (Status, error) {
	if len(raw) != HeadBytes || raw[0] != headMagic[0] || raw[1] != headMagic[1] ||
		raw[2] != headMagic[2] || raw[3] != headMagic[3] || !allZero(raw[4:8]) ||
		!allZero(raw[49:56]) || binary.LittleEndian.Uint32(raw[128:132]) !=
		crc32.Checksum(raw[:128], castagnoli) {
		return Status{}, ErrCorrupt
	}
	status := Status{
		Revision:        binary.LittleEndian.Uint64(raw[8:16]),
		Epoch:           binary.LittleEndian.Uint64(raw[16:24]),
		ActivePins:      binary.LittleEndian.Uint64(raw[24:32]),
		ReleasedPins:    binary.LittleEndian.Uint64(raw[32:40]),
		RetainedRecords: binary.LittleEndian.Uint64(raw[40:48]),
	}
	status.Drain.State = DrainState(raw[48])
	status.Drain.Epoch = binary.LittleEndian.Uint64(raw[56:64])
	copy(status.Drain.Identity[:], raw[64:96])
	copy(status.Drain.Binding[:], raw[96:128])
	if !validStatus(status) {
		return Status{}, ErrCorrupt
	}
	return status, nil
}

// AppendStoredPin appends one canonical pin value without duplicating its key.
func AppendStoredPin(dst []byte, record PinRecord) ([]byte, error) {
	if record.Identity == (Identity{}) || record.Binding == (Binding{}) ||
		record.Epoch == 0 || (record.State != PinHeld && record.State != PinReleased) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, StoredPinBytes)...)
	frame := dst[start:]
	copy(frame[:4], pinMagic[:])
	frame[4] = byte(record.State)
	binary.LittleEndian.PutUint64(frame[8:16], record.Epoch)
	copy(frame[16:48], record.Binding[:])
	binary.LittleEndian.PutUint32(frame[48:52], crc32.Checksum(frame[:48], castagnoli))
	return dst, nil
}

// OpenStoredPin validates one value under its separately authenticated key.
func OpenStoredPin(identity Identity, raw []byte) (PinRecord, error) {
	if identity == (Identity{}) || len(raw) != StoredPinBytes ||
		raw[0] != pinMagic[0] || raw[1] != pinMagic[1] ||
		raw[2] != pinMagic[2] || raw[3] != pinMagic[3] ||
		!allZero(raw[5:8]) || binary.LittleEndian.Uint32(raw[48:52]) !=
		crc32.Checksum(raw[:48], castagnoli) {
		return PinRecord{}, ErrCorrupt
	}
	record := PinRecord{Identity: identity, Epoch: binary.LittleEndian.Uint64(raw[8:16]), State: PinState(raw[4])}
	copy(record.Binding[:], raw[16:48])
	if record.Binding == (Binding{}) || record.Epoch == 0 ||
		(record.State != PinHeld && record.State != PinReleased) {
		return PinRecord{}, ErrCorrupt
	}
	return record, nil
}

// Records appends a detached record for every retained pin. Order is not
// specified; callers that encode canonical images sort their scratch.
func (machine *Machine) Records(dst []PinRecord) []PinRecord {
	if machine == nil {
		return dst
	}
	for identity, record := range machine.pins {
		dst = append(dst, PinRecord{
			Identity: identity, Binding: record.Binding, Epoch: record.Epoch, State: record.State,
		})
	}
	return dst
}

// Clone returns an independent planning image.
func (machine *Machine) Clone() *Machine {
	if machine == nil {
		return nil
	}
	clone := &Machine{
		revision: machine.revision, epoch: machine.epoch,
		activePins: machine.activePins, releasedPins: machine.releasedPins,
		retainedPins: machine.retainedPins,
		maxRecords:   machine.maxRecords, drain: machine.drain,
		pins: make(map[Identity]storedPin, len(machine.pins)),
	}
	for identity, record := range machine.pins {
		clone.pins[identity] = record
	}
	return clone
}

// RestoreMachine reconstructs one gate from independently stored head and pin
// rows. It rejects duplicates, counter drift, and future-epoch records.
func RestoreMachine(status Status, maxRecords uint64, records []PinRecord) (*Machine, error) {
	if maxRecords == 0 || maxRecords > MaxRetainedRecords ||
		status.RetainedRecords > maxRecords || uint64(len(records)) != status.RetainedRecords ||
		!validStatus(status) {
		return nil, ErrCorrupt
	}
	machine := &Machine{
		revision: status.Revision, epoch: status.Epoch,
		activePins: status.ActivePins, releasedPins: status.ReleasedPins,
		retainedPins: status.RetainedRecords,
		maxRecords:   maxRecords, drain: status.Drain,
		pins: make(map[Identity]storedPin, len(records)),
	}
	var active, released uint64
	for _, record := range records {
		if record.Identity == (Identity{}) || record.Binding == (Binding{}) ||
			record.Epoch == 0 || record.Epoch > status.Epoch {
			return nil, ErrCorrupt
		}
		if _, duplicate := machine.pins[record.Identity]; duplicate {
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
	}
	if active != status.ActivePins || released != status.ReleasedPins {
		return nil, ErrCorrupt
	}
	return machine, nil
}

func validStatus(status Status) bool {
	return status.Epoch != 0 && status.ActivePins <= status.RetainedRecords &&
		status.ReleasedPins == status.RetainedRecords-status.ActivePins &&
		validDrainSnapshot(status.Drain, status.Epoch, status.ActivePins)
}
