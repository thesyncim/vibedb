package buildgate

import (
	"encoding/binary"
	"errors"
	"math"
)

// DiskIdentityBytes is the fixed canonical disk-identity width: one opaque
// grammar followed by the fixed capability bitmap in big-endian word order.
const DiskIdentityBytes = len(GrammarID{}) + capabilitySetBytes

var (
	ErrInvalidDiskIdentity = errors.New("buildgate: invalid disk identity")
	ErrDiskAdoptionDenied  = errors.New("buildgate: disk adoption denied")
)

// DiskIdentity is the immutable compatibility identity read before a disk
// namespace is mutated or repaired.
type DiskIdentity struct {
	Grammar  GrammarID
	Required CapabilitySet
}

func (identity DiskIdentity) Valid() bool { return identity.Grammar.Valid() }

// AppendDiskIdentity appends the canonical fixed-width identity. dst is
// unchanged when identity is invalid or its growth would overflow.
func AppendDiskIdentity(dst []byte, identity DiskIdentity) ([]byte, error) {
	if !identity.Valid() || len(dst) > math.MaxInt-DiskIdentityBytes {
		return dst, ErrInvalidDiskIdentity
	}
	start := len(dst)
	dst = append(dst, make([]byte, DiskIdentityBytes)...)
	copy(dst[start:start+len(identity.Grammar)], identity.Grammar[:])
	at := start + len(identity.Grammar)
	for _, word := range identity.Required {
		binary.BigEndian.PutUint64(dst[at:at+8], word)
		at += 8
	}
	return dst, nil
}

// OpenDiskIdentity accepts exactly one canonical fixed-width identity and
// performs no allocation based on input contents.
func OpenDiskIdentity(raw []byte) (DiskIdentity, error) {
	if len(raw) != DiskIdentityBytes {
		return DiskIdentity{}, ErrInvalidDiskIdentity
	}
	var identity DiskIdentity
	copy(identity.Grammar[:], raw[:len(identity.Grammar)])
	at := len(identity.Grammar)
	for index := range identity.Required {
		identity.Required[index] = binary.BigEndian.Uint64(raw[at : at+8])
		at += 8
	}
	if !identity.Valid() {
		return DiskIdentity{}, ErrInvalidDiskIdentity
	}
	return identity, nil
}

// DiskAdoptionPermit is an opaque capability tied to one exact DiskIdentity.
// Its zero value is invalid and callers outside this package cannot mint one.
type DiskAdoptionPermit struct {
	identity DiskIdentity
	seal     uint64
}

const diskAdoptionSeal uint64 = 0x919fd4942df5644b

func (permit DiskAdoptionPermit) allows(identity DiskIdentity) bool {
	return permit.seal == diskAdoptionSeal && permit.identity == identity
}

// DiskAdoptionGate authorizes an already inspected disk identity. Successful
// implementations return a permit bound to that exact identity.
type DiskAdoptionGate interface {
	AuthorizeDiskAdoption(DiskIdentity) (DiskAdoptionPermit, error)
}

// DiskAdoptionTarget separates read-only identity inspection from all mutation
// and repair. AdoptDisk never calls MutateOrRepairDisk unless the gate returns
// a valid permit bound to the inspected identity.
type DiskAdoptionTarget interface {
	InspectDiskIdentity() (DiskIdentity, error)
	MutateOrRepairDisk(DiskAdoptionPermit) error
}

// CurrentDiskGate admits only the exact current disk grammar and identities
// whose required capabilities are all supplied by the local build.
type CurrentDiskGate struct {
	profile Profile
}

func NewCurrentDiskGate(profile Profile) (CurrentDiskGate, error) {
	if !profile.Valid() {
		return CurrentDiskGate{}, ErrInvalidProfile
	}
	return CurrentDiskGate{profile: profile}, nil
}

func (gate CurrentDiskGate) AuthorizeDiskAdoption(identity DiskIdentity) (DiskAdoptionPermit, error) {
	if !gate.profile.Valid() || !identity.Valid() {
		return DiskAdoptionPermit{}, ErrInvalidDiskIdentity
	}
	if identity.Grammar != gate.profile.DiskGrammar {
		return DiskAdoptionPermit{}, ErrDiskGrammar
	}
	if !gate.profile.Provided.HasAll(identity.Required) {
		return DiskAdoptionPermit{}, ErrRequiredCapabilities
	}
	return DiskAdoptionPermit{identity: identity, seal: diskAdoptionSeal}, nil
}

// AdoptDisk enforces inspect, authorize, then mutate/repair ordering. A broken
// or malicious gate cannot bypass the boundary by returning a zero or
// differently bound permit with a nil error.
func AdoptDisk(gate DiskAdoptionGate, target DiskAdoptionTarget) error {
	if gate == nil || target == nil {
		return ErrDiskAdoptionDenied
	}
	identity, err := target.InspectDiskIdentity()
	if err != nil {
		return err
	}
	if !identity.Valid() {
		return ErrInvalidDiskIdentity
	}
	permit, err := gate.AuthorizeDiskAdoption(identity)
	if err != nil {
		return err
	}
	if !permit.allows(identity) {
		return ErrDiskAdoptionDenied
	}
	return target.MutateOrRepairDisk(permit)
}
