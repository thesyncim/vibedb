package buildgate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/buildgate/manifestgen"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var buildgateAt1AEB86A = Profile{
	WireGrammar: GrammarID{0xb6, 0x92, 0x36, 0x3d, 0x9c, 0x0b, 0x49, 0x22, 0x9e, 0xb4, 0x35, 0xe5, 0x0d, 0xba, 0xb8, 0xdd},
	DiskGrammar: GrammarID{0x71, 0xe5, 0xf4, 0x45, 0xb2, 0x45, 0x4a, 0x66, 0x8e, 0x68, 0xd4, 0x47, 0x2e, 0x26, 0xe1, 0x49},
	Provided:    CapabilitySet{1 << CapabilityRaftTransport},
	Required:    CapabilitySet{1 << CapabilityRaftTransport},
}

func TestGeneratedManifestMatchesCurrentLedgerSemantics(t *testing.T) {
	semantics := requestledger.SemanticsDigest()
	if [32]byte(semantics) != generatedRequestLedgerSemantics {
		t.Fatalf("generated request-ledger semantics = %x, current %x; run go generate ./internal/buildgate",
			generatedRequestLedgerSemantics, semantics)
	}
	raw, err := os.ReadFile(filepath.Join("manifest", "current.txt"))
	if err != nil {
		t.Fatal(err)
	}
	wire, disk, err := manifestgen.Derive(raw, [32]byte(semantics))
	if err != nil {
		t.Fatal(err)
	}
	current := CurrentProfile()
	if current.WireGrammar != GrammarID(wire) || current.DiskGrammar != GrammarID(disk) {
		t.Fatalf("generated identities are stale: got %x/%x want %x/%x; run go generate ./internal/buildgate",
			current.WireGrammar, current.DiskGrammar, wire, disk)
	}
	if !current.Required.Has(CapabilityRaftTransport) ||
		!current.Required.Has(CapabilityGatewayShardTransport) {
		t.Fatalf("current mandatory capabilities = %#v", current.Required)
	}
}

func TestBuildAt1AEB86AIsIncompatibleWithCurrentBeforeWireOrDiskAdmission(t *testing.T) {
	current := CurrentProfile()
	if current.WireGrammar == buildgateAt1AEB86A.WireGrammar ||
		current.DiskGrammar == buildgateAt1AEB86A.DiskGrammar {
		t.Fatal("current manifest retained a pre-request-ledger grammar identity")
	}
	if _, err := CheckCompatibility(current, buildgateAt1AEB86A); !errors.Is(err, ErrWireGrammar) {
		t.Fatalf("old/current peer compatibility = %v, want ErrWireGrammar", err)
	}
	gate, err := NewCurrentDiskGate(current)
	if err != nil {
		t.Fatal(err)
	}
	legacyDisk := DiskIdentity{
		Grammar: buildgateAt1AEB86A.DiskGrammar, Required: buildgateAt1AEB86A.Required,
	}
	if _, err := gate.AuthorizeDiskAdoption(legacyDisk); !errors.Is(err, ErrDiskGrammar) {
		t.Fatalf("old/current disk compatibility = %v, want ErrDiskGrammar", err)
	}
}

func FuzzCanonicalManifestDerivation(f *testing.F) {
	raw, err := os.ReadFile(filepath.Join("manifest", "current.txt"))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(nil))
	semantics := [32]byte(requestledger.SemanticsDigest())
	f.Fuzz(func(t *testing.T, candidate []byte) {
		wire, disk, err := manifestgen.Derive(candidate, semantics)
		if err == nil && (wire == ([16]byte{}) || disk == ([16]byte{})) {
			t.Fatal("accepted manifest derived a zero identity")
		}
	})
}
