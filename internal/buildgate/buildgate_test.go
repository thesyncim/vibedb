package buildgate

import (
	"bytes"
	"errors"
	"testing"
)

func testGrammar(seed byte) (id GrammarID) {
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testCapabilities(capabilities ...Capability) (set CapabilitySet) {
	for _, capability := range capabilities {
		var ok bool
		set, ok = set.With(capability)
		if !ok {
			panic("invalid test capability")
		}
	}
	return set
}

func testProfile() Profile {
	return Profile{
		WireGrammar: testGrammar(1),
		DiskGrammar: testGrammar(33),
		Provided:    testCapabilities(0, 1, 63, 64, 127, 128, 191, 192, 255),
		Required:    testCapabilities(0, 64),
	}
}

func TestCapabilitySetAddressesEveryBitExactly(t *testing.T) {
	var set CapabilitySet
	for capability := Capability(0); capability < CapabilityCount; capability++ {
		var ok bool
		set, ok = set.With(capability)
		if !ok || !set.Has(capability) {
			t.Fatalf("capability %d not represented", capability)
		}
	}
	for _, word := range set {
		if word != ^uint64(0) {
			t.Fatalf("word = %#x, want all bits", word)
		}
	}
	before := set
	for _, capability := range []Capability{CapabilityCount, ^Capability(0)} {
		if after, ok := set.With(capability); ok || after != before || set.Has(capability) {
			t.Fatalf("out-of-range capability %d accepted", capability)
		}
	}
}

func TestProfileRequiresOnlyProvidedCapabilities(t *testing.T) {
	profile := testProfile()
	if !profile.Valid() {
		t.Fatal("valid profile rejected")
	}
	profile.Required = testCapabilities(2)
	if profile.Valid() {
		t.Fatal("unprovided requirement accepted")
	}
	profile = testProfile()
	profile.WireGrammar = GrammarID{}
	if profile.Valid() {
		t.Fatal("zero wire grammar accepted")
	}
	profile = testProfile()
	profile.DiskGrammar = GrammarID{}
	if profile.Valid() {
		t.Fatal("zero disk grammar accepted")
	}
}

func TestCurrentProfileIsExactAndImmutableByValue(t *testing.T) {
	profile := CurrentProfile()
	if !profile.Valid() || !profile.Provided.Has(CapabilityRaftTransport) ||
		!profile.Required.Has(CapabilityRaftTransport) {
		t.Fatalf("invalid current profile: %#v", profile)
	}
	profile.WireGrammar = GrammarID{}
	profile.Provided = CapabilitySet{}
	if next := CurrentProfile(); !next.Valid() ||
		!next.Provided.Has(CapabilityRaftTransport) {
		t.Fatalf("current profile was externally mutated: %#v", next)
	}
}

func TestExactCompatibility(t *testing.T) {
	local := testProfile()
	remote := local
	remote.Provided = testCapabilities(0, 2, 64, 200)
	remote.Required = testCapabilities(0)

	agreed, err := CheckCompatibility(local, remote)
	if err != nil {
		t.Fatalf("compatible profiles: %v", err)
	}
	if want := testCapabilities(0, 64); agreed != want {
		t.Fatalf("agreed = %#v, want %#v", agreed, want)
	}
	if !Compatible(local, remote) || !Compatible(remote, local) {
		t.Fatal("compatibility is not symmetric")
	}

	tests := []struct {
		name string
		edit func(*Profile, *Profile)
		want error
	}{
		{"invalid-local", func(local, _ *Profile) { local.Required = testCapabilities(9) }, ErrInvalidProfile},
		{"invalid-remote", func(_, remote *Profile) { remote.DiskGrammar = GrammarID{} }, ErrInvalidProfile},
		{"wire-grammar", func(_, remote *Profile) { remote.WireGrammar = testGrammar(99) }, ErrWireGrammar},
		{"disk-grammar", func(_, remote *Profile) { remote.DiskGrammar = testGrammar(99) }, ErrDiskGrammar},
		{"local-requirement", func(local, _ *Profile) { local.Required = testCapabilities(1) }, ErrRequiredCapabilities},
		{"remote-requirement", func(_, remote *Profile) { remote.Required = testCapabilities(200) }, ErrRequiredCapabilities},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, right := local, remote
			test.edit(&left, &right)
			if _, err := CheckCompatibility(left, right); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if Compatible(left, right) {
				t.Fatal("incompatible profiles accepted")
			}
		})
	}
}

func TestPrefaceCanonicalRoundTrip(t *testing.T) {
	profile := testProfile()
	prefix := []byte{0xaa, 0xbb, 0xcc}
	encoded, err := AppendPreface(append([]byte(nil), prefix...), profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != len(prefix)+PrefaceBytes || !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatalf("encoded size/prefix = %d/%x", len(encoded), encoded[:len(prefix)])
	}
	opened, err := OpenPreface(encoded[len(prefix):])
	if err != nil {
		t.Fatal(err)
	}
	if opened != profile {
		t.Fatalf("opened = %#v, want %#v", opened, profile)
	}
	reencoded, err := AppendPreface(nil, opened)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, encoded[len(prefix):]) {
		t.Fatalf("noncanonical re-encoding\n got %x\nwant %x", reencoded, encoded[len(prefix):])
	}
}

func TestPrefaceRejectsMalformedInputs(t *testing.T) {
	canonical, err := AppendPreface(nil, testProfile())
	if err != nil {
		t.Fatal(err)
	}
	inputs := [][]byte{
		nil,
		canonical[:PrefaceBytes-1],
		append(append([]byte(nil), canonical...), 0),
	}
	badMagic := append([]byte(nil), canonical...)
	badMagic[0] ^= 1
	inputs = append(inputs, badMagic)
	zeroWire := append([]byte(nil), canonical...)
	clear(zeroWire[8:24])
	inputs = append(inputs, zeroWire)
	zeroDisk := append([]byte(nil), canonical...)
	clear(zeroDisk[24:40])
	inputs = append(inputs, zeroDisk)
	requiresUnavailable := append([]byte(nil), canonical...)
	requiresUnavailable[72] |= 0x40
	inputs = append(inputs, requiresUnavailable)

	for i, input := range inputs {
		if profile, err := OpenPreface(input); !errors.Is(err, ErrInvalidPreface) || profile != (Profile{}) {
			t.Fatalf("input %d: profile=%#v error=%v", i, profile, err)
		}
	}
	invalid := testProfile()
	invalid.Required = testCapabilities(2)
	prefix := []byte{1, 2, 3}
	got, err := AppendPreface(prefix, invalid)
	if !errors.Is(err, ErrInvalidPreface) || !bytes.Equal(got, prefix) {
		t.Fatalf("invalid append = %x, %v", got, err)
	}
}

type recordingDiskTarget struct {
	identity   DiskIdentity
	inspectErr error
	mutateErr  error
	inspected  int
	mutated    int
	permit     DiskAdoptionPermit
}

func (target *recordingDiskTarget) InspectDiskIdentity() (DiskIdentity, error) {
	target.inspected++
	return target.identity, target.inspectErr
}

func (target *recordingDiskTarget) MutateOrRepairDisk(permit DiskAdoptionPermit) error {
	target.mutated++
	target.permit = permit
	return target.mutateErr
}

type fixedDiskGate struct {
	permit DiskAdoptionPermit
	err    error
	calls  int
}

func (gate *fixedDiskGate) AuthorizeDiskAdoption(DiskIdentity) (DiskAdoptionPermit, error) {
	gate.calls++
	return gate.permit, gate.err
}

func TestDiskAdoptionFailsBeforeMutationOrRepair(t *testing.T) {
	profile := testProfile()
	gate, err := NewCurrentDiskGate(profile)
	if err != nil {
		t.Fatal(err)
	}
	needed := testCapabilities(0, 64)

	tests := []struct {
		name     string
		identity DiskIdentity
		want     error
	}{
		{"invalid", DiskIdentity{}, ErrInvalidDiskIdentity},
		{"grammar", DiskIdentity{Grammar: testGrammar(99), Required: needed}, ErrDiskGrammar},
		{"capability", DiskIdentity{Grammar: profile.DiskGrammar, Required: testCapabilities(2)}, ErrRequiredCapabilities},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := &recordingDiskTarget{identity: test.identity}
			if err := AdoptDisk(gate, target); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if target.inspected != 1 || target.mutated != 0 {
				t.Fatalf("inspect/mutate = %d/%d", target.inspected, target.mutated)
			}
		})
	}

	inspectFailure := errors.New("inspect failure")
	target := &recordingDiskTarget{inspectErr: inspectFailure}
	if err := AdoptDisk(gate, target); !errors.Is(err, inspectFailure) || target.mutated != 0 {
		t.Fatalf("inspect error/mutations = %v/%d", err, target.mutated)
	}

	for _, broken := range []*fixedDiskGate{
		{},
		{permit: DiskAdoptionPermit{identity: DiskIdentity{Grammar: profile.DiskGrammar}, seal: diskAdoptionSeal}},
	} {
		target = &recordingDiskTarget{identity: DiskIdentity{Grammar: profile.DiskGrammar, Required: needed}}
		if err := AdoptDisk(broken, target); !errors.Is(err, ErrDiskAdoptionDenied) || target.mutated != 0 {
			t.Fatalf("broken gate error/mutations = %v/%d", err, target.mutated)
		}
	}
}

func TestDiskAdoptionPassesExactPermitAndMutationError(t *testing.T) {
	profile := testProfile()
	gate, err := NewCurrentDiskGate(profile)
	if err != nil {
		t.Fatal(err)
	}
	mutationFailure := errors.New("mutation failure")
	identity := DiskIdentity{Grammar: profile.DiskGrammar, Required: testCapabilities(0, 64)}
	target := &recordingDiskTarget{identity: identity, mutateErr: mutationFailure}
	if err := AdoptDisk(gate, target); !errors.Is(err, mutationFailure) {
		t.Fatalf("error = %v, want mutation error", err)
	}
	if target.inspected != 1 || target.mutated != 1 || !target.permit.allows(identity) {
		t.Fatalf("inspect/mutate/permit = %d/%d/%v", target.inspected, target.mutated, target.permit.allows(identity))
	}
}

func TestBuildGateHotPathsAllocateZero(t *testing.T) {
	profile := testProfile()
	encoded := make([]byte, 0, PrefaceBytes)
	if allocations := testing.AllocsPerRun(1000, func() {
		out, err := AppendPreface(encoded[:0], profile)
		if err != nil || len(out) != PrefaceBytes {
			panic("append failed")
		}
	}); allocations != 0 {
		t.Fatalf("append allocations = %v", allocations)
	}
	raw, _ := AppendPreface(encoded[:0], profile)
	if allocations := testing.AllocsPerRun(1000, func() {
		opened, err := OpenPreface(raw)
		if err != nil || opened != profile {
			panic("open failed")
		}
	}); allocations != 0 {
		t.Fatalf("open allocations = %v", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !Compatible(profile, profile) {
			panic("compatibility failed")
		}
	}); allocations != 0 {
		t.Fatalf("compatibility allocations = %v", allocations)
	}
	gate, _ := NewCurrentDiskGate(profile)
	target := &recordingDiskTarget{identity: DiskIdentity{Grammar: profile.DiskGrammar, Required: profile.Required}}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := AdoptDisk(&gate, target); err != nil {
			panic("adoption failed")
		}
	}); allocations != 0 {
		t.Fatalf("disk adoption allocations = %v", allocations)
	}
}
