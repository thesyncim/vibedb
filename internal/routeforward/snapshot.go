package routeforward

import (
	"bytes"
	"encoding/binary"
	"slices"
)

const (
	SnapshotHeaderBytes    = 128
	SnapshotLiveBytes      = 616
	SnapshotTombstoneBytes = 88
	SnapshotChecksumBytes  = 32

	MaxRetainedRecords = (MaxSnapshotBytes - SnapshotHeaderBytes - SnapshotChecksumBytes) /
		SnapshotLiveBytes
)

var snapshotMagic = [8]byte{'V', 'R', 'F', 'W', 'S', 'N', 'A', 'P'}

const snapshotDigestDomain = "vibedb/route-forward/snapshot\x00"

// LiveRecord and TombstoneRecord are caller-owned snapshot scratch values.
type LiveRecord struct {
	Key Digest
	retainedEntry
}

type TombstoneRecord struct {
	Key Digest
	tombstone
}

// SnapshotBytes returns exact canonical image geometry.
func SnapshotBytes(live, tombstones uint64) (uint64, bool) {
	if live > MaxRetainedRecords || tombstones > MaxRetainedRecords ||
		live+tombstones > MaxRetainedRecords {
		return 0, false
	}
	bytes := uint64(SnapshotHeaderBytes) + live*SnapshotLiveBytes +
		tombstones*SnapshotTombstoneBytes + SnapshotChecksumBytes
	return bytes, bytes <= MaxSnapshotBytes
}

// AppendSnapshot appends one canonical map image without allocating when dst
// and both record scratch slices are pre-sized.
func AppendSnapshot(
	dst []byte,
	machine *Machine,
	liveScratch []LiveRecord,
	tombstoneScratch []TombstoneRecord,
) ([]byte, error) {
	if machine == nil || machine.live != uint64(len(machine.entries)) ||
		machine.tombstones != uint64(len(machine.retired)) ||
		uint64(len(liveScratch)) < machine.live ||
		uint64(len(tombstoneScratch)) < machine.tombstones {
		return dst, ErrCorrupt
	}
	total, ok := SnapshotBytes(machine.live, machine.tombstones)
	if !ok {
		return dst, ErrBound
	}
	live := liveScratch[:int(machine.live)]
	tombs := tombstoneScratch[:int(machine.tombstones)]
	index := 0
	for key, entry := range machine.entries {
		live[index] = LiveRecord{Key: key, retainedEntry: entry}
		index++
	}
	index = 0
	for key, tomb := range machine.retired {
		tombs[index] = TombstoneRecord{Key: key, tombstone: tomb}
		index++
	}
	slices.SortFunc(live, func(left, right LiveRecord) int {
		return bytes.Compare(left.Key[:], right.Key[:])
	})
	slices.SortFunc(tombs, func(left, right TombstoneRecord) int {
		return bytes.Compare(left.Key[:], right.Key[:])
	})

	start := len(dst)
	dst = append(dst, make([]byte, int(total))...)
	frame := dst[start:]
	copy(frame[:8], snapshotMagic[:])
	binary.LittleEndian.PutUint64(frame[8:16], total)
	binary.LittleEndian.PutUint64(frame[16:24], machine.revision)
	binary.LittleEndian.PutUint64(frame[24:32], machine.authorityEpoch)
	binary.LittleEndian.PutUint64(frame[32:40], machine.live)
	binary.LittleEndian.PutUint64(frame[40:48], machine.tombstones)
	binary.LittleEndian.PutUint64(frame[48:56], machine.maxRecords)
	copy(frame[56:88], machine.authority[:])
	binary.LittleEndian.PutUint64(frame[88:96], machine.live*SnapshotLiveBytes)
	binary.LittleEndian.PutUint64(frame[96:104], machine.tombstones*SnapshotTombstoneBytes)
	cursor := SnapshotHeaderBytes
	var entryStorage [EntryBytes]byte
	for _, record := range live {
		row := frame[cursor : cursor+SnapshotLiveBytes]
		copy(row[:32], record.Key[:])
		encoded, err := AppendEntry(entryStorage[:0], record.Entry)
		if err != nil {
			return dst[:start], err
		}
		copy(row[32:592], encoded)
		row[592] = byte(record.State)
		binary.LittleEndian.PutUint64(row[600:608], record.PublishedRevision)
		binary.LittleEndian.PutUint64(row[608:616], record.ActiveRevision)
		cursor += SnapshotLiveBytes
	}
	for _, record := range tombs {
		row := frame[cursor : cursor+SnapshotTombstoneBytes]
		copy(row[:32], record.Key[:])
		binary.LittleEndian.PutUint64(row[32:40], record.CatalogGeneration)
		binary.LittleEndian.PutUint64(row[40:48], record.AuthorityEpoch)
		binary.LittleEndian.PutUint64(row[48:56], record.PrunedRevision)
		copy(row[56:88], record.Certificate[:])
		cursor += SnapshotTombstoneBytes
	}
	digest := domainDigest(snapshotDigestDomain, frame[:cursor])
	copy(frame[cursor:cursor+SnapshotChecksumBytes], digest[:])
	return dst, nil
}

// OpenSnapshot restores one exact canonical authority image.
func OpenSnapshot(raw []byte, maxRecords uint64) (*Machine, error) {
	if len(raw) < SnapshotHeaderBytes+SnapshotChecksumBytes || uint64(len(raw)) > MaxSnapshotBytes ||
		maxRecords == 0 || maxRecords > MaxRetainedRecords || !bytes.Equal(raw[:8], snapshotMagic[:]) ||
		binary.LittleEndian.Uint64(raw[8:16]) != uint64(len(raw)) || !allZero(raw[104:128]) ||
		domainDigest(snapshotDigestDomain, raw[:len(raw)-SnapshotChecksumBytes]) !=
			Digest(raw[len(raw)-SnapshotChecksumBytes:]) {
		return nil, ErrCorrupt
	}
	live := binary.LittleEndian.Uint64(raw[32:40])
	tombstones := binary.LittleEndian.Uint64(raw[40:48])
	total, ok := SnapshotBytes(live, tombstones)
	if !ok || total != uint64(len(raw)) || live+tombstones > maxRecords ||
		binary.LittleEndian.Uint64(raw[48:56]) != maxRecords ||
		binary.LittleEndian.Uint64(raw[88:96]) != live*SnapshotLiveBytes ||
		binary.LittleEndian.Uint64(raw[96:104]) != tombstones*SnapshotTombstoneBytes {
		return nil, ErrCorrupt
	}
	var authority Digest
	copy(authority[:], raw[56:88])
	machine, machineOK := NewMachine(
		authority, binary.LittleEndian.Uint64(raw[24:32]), maxRecords,
	)
	if !machineOK {
		return nil, ErrCorrupt
	}
	machine.revision = binary.LittleEndian.Uint64(raw[16:24])
	if machine.revision == 0 {
		return nil, ErrCorrupt
	}
	cursor := SnapshotHeaderBytes
	var prior Digest
	for index := uint64(0); index < live; index++ {
		row := raw[cursor : cursor+SnapshotLiveBytes]
		var key Digest
		copy(key[:], row[:32])
		entry, err := OpenEntry(row[32:592])
		state := EntryState(row[592])
		published := binary.LittleEndian.Uint64(row[600:608])
		active := binary.LittleEndian.Uint64(row[608:616])
		if err != nil || key == (Digest{}) || key != EntryKey(entry) || !allZero(row[593:600]) ||
			(index != 0 && bytes.Compare(prior[:], key[:]) >= 0) ||
			state < EntryPrepared || state > EntryActive || published < 2 ||
			published > machine.revision ||
			(state == EntryPrepared && active != 0) ||
			(state == EntryActive && (active <= published || active > machine.revision)) {
			return nil, ErrCorrupt
		}
		retained := retainedEntry{
			Entry: entry, State: state, PublishedRevision: published, ActiveRevision: active,
		}
		retained.Certificate = entryCertificate(key, retained)
		machine.entries[key] = retained
		prior = key
		cursor += SnapshotLiveBytes
	}
	prior = Digest{}
	for index := uint64(0); index < tombstones; index++ {
		row := raw[cursor : cursor+SnapshotTombstoneBytes]
		var key, certificate Digest
		copy(key[:], row[:32])
		copy(certificate[:], row[56:88])
		tomb := tombstone{
			CatalogGeneration: binary.LittleEndian.Uint64(row[32:40]),
			AuthorityEpoch:    binary.LittleEndian.Uint64(row[40:48]),
			PrunedRevision:    binary.LittleEndian.Uint64(row[48:56]),
			Certificate:       certificate,
		}
		if key == (Digest{}) || tomb.CatalogGeneration == 0 ||
			tomb.AuthorityEpoch != machine.authorityEpoch ||
			tomb.PrunedRevision < 2 || tomb.PrunedRevision > machine.revision ||
			tomb.Certificate != tombstoneCertificate(key, tomb) ||
			(index != 0 && bytes.Compare(prior[:], key[:]) >= 0) {
			return nil, ErrCorrupt
		}
		if _, collision := machine.entries[key]; collision {
			return nil, ErrCorrupt
		}
		machine.retired[key] = tomb
		prior = key
		cursor += SnapshotTombstoneBytes
	}
	machine.live, machine.tombstones = live, tombstones
	return machine, nil
}
