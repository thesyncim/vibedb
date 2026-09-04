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

// buildgateBeforeSQLParameterTypes freezes the immediately preceding grammar
// identities. Parameter-analysis metadata extends both shard SQL requests and
// the durable target mutation batch, so neither identity may silently
// remain compatible with a build that predates those fields.
var buildgateBeforeSQLParameterTypes = Profile{
	WireGrammar: GrammarID{0x5b, 0x58, 0x5a, 0x02, 0xd6, 0xc4, 0x83, 0x52, 0x31, 0xd8, 0x75, 0x9c, 0x62, 0x10, 0x36, 0xe2},
	DiskGrammar: GrammarID{0x2c, 0x3e, 0x04, 0xe1, 0xfb, 0xa7, 0xd8, 0xd9, 0xc6, 0x6a, 0x10, 0x03, 0x2c, 0xb3, 0xed, 0xb8},
	Provided: CapabilitySet{
		1<<CapabilityRaftTransport | 1<<CapabilityGatewayShardTransport,
	},
	Required: CapabilitySet{
		1<<CapabilityRaftTransport | 1<<CapabilityGatewayShardTransport,
	},
}

// buildgateBeforeMutationImagesAndPostimages freezes the grammar identities
// that accidentally remained current after mutation-image marker 0xe4 and the
// replicated postimage/prepare-conflict semantics were introduced.
var buildgateBeforeMutationImagesAndPostimages = Profile{
	WireGrammar: GrammarID{0x71, 0x13, 0xcc, 0xdb, 0xf9, 0xc2, 0xa3, 0xcd, 0xac, 0x3d, 0x57, 0x96, 0xb9, 0x64, 0x21, 0xdf},
	DiskGrammar: GrammarID{0xde, 0xce, 0x27, 0xa5, 0x8f, 0xe9, 0xf3, 0x8d, 0xb5, 0x14, 0xd1, 0x41, 0xba, 0x23, 0x6e, 0x34},
	Provided: CapabilitySet{
		1<<CapabilityRaftTransport | 1<<CapabilityGatewayShardTransport,
	},
	Required: CapabilitySet{
		1<<CapabilityRaftTransport | 1<<CapabilityGatewayShardTransport,
	},
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

func TestBuildBeforeSQLParameterTypesIsIncompatibleBeforeWireOrDiskAdmission(t *testing.T) {
	current := CurrentProfile()
	if current.WireGrammar == buildgateBeforeSQLParameterTypes.WireGrammar ||
		current.DiskGrammar == buildgateBeforeSQLParameterTypes.DiskGrammar {
		t.Fatal("current manifest retained the pre-parameter-metadata grammar identity")
	}
	if _, err := CheckCompatibility(current, buildgateBeforeSQLParameterTypes); !errors.Is(err, ErrWireGrammar) {
		t.Fatalf("pre-parameter-metadata/current peer compatibility = %v, want ErrWireGrammar", err)
	}
	gate, err := NewCurrentDiskGate(current)
	if err != nil {
		t.Fatal(err)
	}
	legacyDisk := DiskIdentity{
		Grammar:  buildgateBeforeSQLParameterTypes.DiskGrammar,
		Required: buildgateBeforeSQLParameterTypes.Required,
	}
	if _, err := gate.AuthorizeDiskAdoption(legacyDisk); !errors.Is(err, ErrDiskGrammar) {
		t.Fatalf("pre-parameter-metadata/current disk compatibility = %v, want ErrDiskGrammar", err)
	}
}

func TestBuildBeforeMutationImagesAndPostimagesIsIncompatibleBeforeAdmission(t *testing.T) {
	current := CurrentProfile()
	legacy := buildgateBeforeMutationImagesAndPostimages
	if current.WireGrammar == legacy.WireGrammar || current.DiskGrammar == legacy.DiskGrammar {
		t.Fatal("current manifest retained the pre-mutation-image/postimage grammar identity")
	}
	if _, err := CheckCompatibility(current, legacy); !errors.Is(err, ErrWireGrammar) {
		t.Fatalf("legacy/current peer compatibility = %v, want ErrWireGrammar", err)
	}
	gate, err := NewCurrentDiskGate(current)
	if err != nil {
		t.Fatal(err)
	}
	legacyDisk := DiskIdentity{Grammar: legacy.DiskGrammar, Required: legacy.Required}
	if _, err := gate.AuthorizeDiskAdoption(legacyDisk); !errors.Is(err, ErrDiskGrammar) {
		t.Fatalf("legacy/current disk compatibility = %v, want ErrDiskGrammar", err)
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
