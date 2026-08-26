package buildgate

import "errors"

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
