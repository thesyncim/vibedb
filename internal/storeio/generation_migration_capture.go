package storeio

import (
	"encoding/binary"
	"fmt"
)

const GenerationMigrationCaptureHeaderBytes = 80

type GenerationMigrationMutation struct {
	StoreID, MigrationID [16]byte
	Sequence, Generation uint64
	Delete               bool
	Key, Value           []byte
}

// EncodeGenerationMigrationMutation writes one sector-rounded, checksum-sealed
// byte-native capture record. The caller owns dst and therefore the exact hard
// memory bound; no key, value, or string allocation occurs.
func EncodeGenerationMigrationMutation(dst []byte, m GenerationMigrationMutation) ([]byte, error) {
	used := GenerationMigrationCaptureHeaderBytes + len(m.Key) + len(m.Value) + 8
	length := (used + 4095) &^ 4095
	if m.StoreID == ([16]byte{}) || m.MigrationID == ([16]byte{}) || m.Sequence == 0 ||
		m.Generation == 0 || len(m.Key) == 0 || m.Delete && len(m.Value) != 0 ||
		length > len(dst) {
		return nil, fmt.Errorf("%w: migration capture", ErrInvalidWrite)
	}
	b := dst[:length]
	clear(b)
	copy(b[:8], "SGMCAP00")
	binary.LittleEndian.PutUint32(b[8:12], DevelopmentFormatVersion)
	if m.Delete {
		b[12] = 1
	}
	copy(b[16:32], m.StoreID[:])
	copy(b[32:48], m.MigrationID[:])
	binary.LittleEndian.PutUint64(b[48:56], m.Sequence)
	binary.LittleEndian.PutUint64(b[56:64], m.Generation)
	binary.LittleEndian.PutUint32(b[64:68], uint32(len(m.Key)))
	binary.LittleEndian.PutUint32(b[68:72], uint32(len(m.Value)))
	copy(b[GenerationMigrationCaptureHeaderBytes:], m.Key)
	copy(b[GenerationMigrationCaptureHeaderBytes+len(m.Key):], m.Value)
	at := len(b) - 8
	sum := PageChecksum(b[:at])
	binary.LittleEndian.PutUint32(b[at:], sum)
	binary.LittleEndian.PutUint32(b[at+4:], ^sum)
	return b, nil
}

func OpenGenerationMigrationMutation(src []byte) (GenerationMigrationMutation, error) {
	var m GenerationMigrationMutation
	if len(src) < 4096 || len(src)&4095 != 0 || string(src[:8]) != "SGMCAP00" ||
		binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion {
		return m, ErrGenerationMigrationManifestCorrupt
	}
	at := len(src) - 8
	sum := binary.LittleEndian.Uint32(src[at:])
	if binary.LittleEndian.Uint32(src[at+4:]) != ^sum || PageChecksum(src[:at]) != sum {
		return m, ErrGenerationMigrationManifestCorrupt
	}
	copy(m.StoreID[:], src[16:32])
	copy(m.MigrationID[:], src[32:48])
	m.Delete = src[12] == 1
	m.Sequence = binary.LittleEndian.Uint64(src[48:56])
	m.Generation = binary.LittleEndian.Uint64(src[56:64])
	k, v := int(binary.LittleEndian.Uint32(src[64:68])), int(binary.LittleEndian.Uint32(src[68:72]))
	end := GenerationMigrationCaptureHeaderBytes + k + v
	if m.StoreID == ([16]byte{}) || m.MigrationID == ([16]byte{}) || m.Sequence == 0 || m.Generation == 0 || k == 0 || end > at || m.Delete && v != 0 || !allZero(src[end:at]) {
		return GenerationMigrationMutation{}, ErrGenerationMigrationManifestCorrupt
	}
	m.Key = src[GenerationMigrationCaptureHeaderBytes : GenerationMigrationCaptureHeaderBytes+k]
	m.Value = src[GenerationMigrationCaptureHeaderBytes+k : end]
	return m, nil
}

func ValidateGenerationMigrationMutationOrder(previous, next GenerationMigrationMutation) error {
	if previous.StoreID != next.StoreID || previous.MigrationID != next.MigrationID || next.Sequence != previous.Sequence+1 || next.Generation < previous.Generation {
		return fmt.Errorf("%w: migration capture order", ErrInvalidWrite)
	}
	return nil
}
