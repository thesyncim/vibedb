package buildgate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	capabilitySetBytes = capabilityWords * 8
	// PrefaceBytes is the fixed size of the sole current compatibility preface.
	PrefaceBytes = 8 + 2*len(GrammarID{}) + 2*capabilitySetBytes
)

var (
	ErrInvalidPreface = errors.New("buildgate: invalid preface")
	prefaceMagic      = [8]byte{'V', 'D', 'B', 'G', 'A', 'T', 'E', 0}
)

// AppendPreface appends the sole canonical compatibility preface. dst remains
// unchanged on error.
func AppendPreface(dst []byte, profile Profile) ([]byte, error) {
	if !profile.Valid() || len(dst) > math.MaxInt-PrefaceBytes {
		return dst, ErrInvalidPreface
	}
	start := len(dst)
	dst = append(dst, make([]byte, PrefaceBytes)...)
	out := dst[start:]
	copy(out[:8], prefaceMagic[:])
	copy(out[8:24], profile.WireGrammar[:])
	copy(out[24:40], profile.DiskGrammar[:])
	appendCapabilitySet(out[40:72], profile.Provided)
	appendCapabilitySet(out[72:104], profile.Required)
	return dst, nil
}

// OpenPreface accepts exactly one canonical fixed-width preface and never
// allocates from fields controlled by its sender.
func OpenPreface(raw []byte) (Profile, error) {
	if len(raw) != PrefaceBytes || !bytes.Equal(raw[:8], prefaceMagic[:]) {
		return Profile{}, ErrInvalidPreface
	}
	var profile Profile
	copy(profile.WireGrammar[:], raw[8:24])
	copy(profile.DiskGrammar[:], raw[24:40])
	profile.Provided = openCapabilitySet(raw[40:72])
	profile.Required = openCapabilitySet(raw[72:104])
	if !profile.Valid() {
		return Profile{}, ErrInvalidPreface
	}
	return profile, nil
}

func appendCapabilitySet(dst []byte, set CapabilitySet) {
	for i, word := range set {
		binary.BigEndian.PutUint64(dst[i*8:i*8+8], word)
	}
}

func openCapabilitySet(raw []byte) (set CapabilitySet) {
	for i := range set {
		set[i] = binary.BigEndian.Uint64(raw[i*8 : i*8+8])
	}
	return set
}
