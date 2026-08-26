package buildgate

import "errors"

const (
	// CapabilityCount is the fixed number of independently addressable current
	// grammar capabilities. Increasing it changes the preface grammar.
	CapabilityCount = 256
	capabilityWords = CapabilityCount / 64
)

var (
	ErrInvalidProfile       = errors.New("buildgate: invalid profile")
	ErrWireGrammar          = errors.New("buildgate: incompatible wire grammar")
	ErrDiskGrammar          = errors.New("buildgate: incompatible disk grammar")
	ErrRequiredCapabilities = errors.New("buildgate: required capabilities unavailable")
)

// GrammarID is an opaque identity for one exact grammar. Its bytes have no
// numeric ordering or compatibility semantics.
type GrammarID [16]byte

func (id GrammarID) Valid() bool { return id != (GrammarID{}) }

// Capability identifies one bit in a fixed CapabilitySet.
type Capability uint16

// CapabilitySet is the canonical fixed-width capability bitmap. Word and bit
// ordering are part of the sole current preface grammar.
type CapabilitySet [capabilityWords]uint64

// With returns a copy containing capability and reports whether capability is
// representable by the fixed bitmap.
func (set CapabilitySet) With(capability Capability) (CapabilitySet, bool) {
	if capability >= CapabilityCount {
		return set, false
	}
	set[uint16(capability)>>6] |= uint64(1) << (uint16(capability) & 63)
	return set, true
}

// Has reports whether capability is representable and present.
func (set CapabilitySet) Has(capability Capability) bool {
	if capability >= CapabilityCount {
		return false
	}
	return set[uint16(capability)>>6]&(uint64(1)<<(uint16(capability)&63)) != 0
}

// HasAll reports whether set contains every bit in required.
func (set CapabilitySet) HasAll(required CapabilitySet) bool {
	for i := range set {
		if set[i]&required[i] != required[i] {
			return false
		}
	}
	return true
}

// Intersect returns the capabilities supplied by both sets.
func (set CapabilitySet) Intersect(other CapabilitySet) CapabilitySet {
	for i := range set {
		set[i] &= other[i]
	}
	return set
}

// Profile describes one build's exact grammars and current capabilities.
// Required must be a subset of Provided; optional provided bits may differ
// between compatible peers.
type Profile struct {
	WireGrammar GrammarID
	DiskGrammar GrammarID
	Provided    CapabilitySet
	Required    CapabilitySet
}

func (profile Profile) Valid() bool {
	return profile.WireGrammar.Valid() && profile.DiskGrammar.Valid() &&
		profile.Provided.HasAll(profile.Required)
}

// Compatible is the exact, symmetric peer-compatibility predicate.
func Compatible(local, remote Profile) bool {
	_, err := CheckCompatibility(local, remote)
	return err == nil
}

// CheckCompatibility validates both profiles, requires exact grammar
// identities, and proves that each peer provides every capability required by
// the other. It returns the common capability set on success.
func CheckCompatibility(local, remote Profile) (CapabilitySet, error) {
	if !local.Valid() || !remote.Valid() {
		return CapabilitySet{}, ErrInvalidProfile
	}
	if local.WireGrammar != remote.WireGrammar {
		return CapabilitySet{}, ErrWireGrammar
	}
	if local.DiskGrammar != remote.DiskGrammar {
		return CapabilitySet{}, ErrDiskGrammar
	}
	if !remote.Provided.HasAll(local.Required) || !local.Provided.HasAll(remote.Required) {
		return CapabilitySet{}, ErrRequiredCapabilities
	}
	return local.Provided.Intersect(remote.Provided), nil
}
