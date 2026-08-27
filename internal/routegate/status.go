package routegate

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// StatusBytes is the exact read-result size for a route-gate status.
	StatusBytes = 124

	statusBodyBytes = StatusBytes - 4
)

var statusMagic = [4]byte{'V', 'R', 'G', 'S'}

// AppendStatus appends one canonical fixed-size route-gate status.
// The status is an observation, never a mutation outcome.
func AppendStatus(dst []byte, status Status) ([]byte, error) {
	if !validStatus(status) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, StatusBytes)...)
	encoded := dst[start : start+StatusBytes]
	copy(encoded[:4], statusMagic[:])
	encoded[6] = byte(status.Drain.State)
	binary.LittleEndian.PutUint64(encoded[8:16], status.Revision)
	binary.LittleEndian.PutUint64(encoded[16:24], status.Epoch)
	binary.LittleEndian.PutUint64(encoded[24:32], status.ActivePins)
	binary.LittleEndian.PutUint64(encoded[32:40], status.ReleasedPins)
	binary.LittleEndian.PutUint64(encoded[40:48], status.RetainedRecords)
	binary.LittleEndian.PutUint64(encoded[48:56], status.Drain.Epoch)
	copy(encoded[56:88], status.Drain.Identity[:])
	copy(encoded[88:120], status.Drain.Binding[:])
	binary.LittleEndian.PutUint32(
		encoded[statusBodyBytes:], crc32.Checksum(encoded[:statusBodyBytes], castagnoli),
	)
	return dst, nil
}

// OpenStatus authenticates and opens exactly one canonical route-gate status.
func OpenStatus(raw []byte) (Status, error) {
	if len(raw) != StatusBytes || raw[0] != statusMagic[0] ||
		raw[1] != statusMagic[1] || raw[2] != statusMagic[2] ||
		raw[3] != statusMagic[3] || raw[4] != 0 || raw[5] != 0 || raw[7] != 0 ||
		binary.LittleEndian.Uint32(raw[statusBodyBytes:]) !=
			crc32.Checksum(raw[:statusBodyBytes], castagnoli) {
		return Status{}, ErrCorrupt
	}
	status := Status{
		Revision:        binary.LittleEndian.Uint64(raw[8:16]),
		Epoch:           binary.LittleEndian.Uint64(raw[16:24]),
		ActivePins:      binary.LittleEndian.Uint64(raw[24:32]),
		ReleasedPins:    binary.LittleEndian.Uint64(raw[32:40]),
		RetainedRecords: binary.LittleEndian.Uint64(raw[40:48]),
	}
	status.Drain.State = DrainState(raw[6])
	status.Drain.Epoch = binary.LittleEndian.Uint64(raw[48:56])
	copy(status.Drain.Identity[:], raw[56:88])
	copy(status.Drain.Binding[:], raw[88:120])
	if !validStatus(status) {
		return Status{}, ErrCorrupt
	}
	return status, nil
}
