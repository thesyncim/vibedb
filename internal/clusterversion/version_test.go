package clusterversion

import (
	"errors"
	"testing"
)

func TestVersionParseAndBinaryRoundTrip(t *testing.T) {
	v, err := ParseVersion("1.1")
	if err != nil || v != V1_1 || v.String() != "1.1" {
		t.Fatalf("ParseVersion(1.1) = %v, %v", v, err)
	}
	raw := v.AppendBinary(nil)
	decoded, err := UnmarshalBinary(raw)
	if err != nil || decoded != v {
		t.Fatalf("UnmarshalBinary = %v, %v", decoded, err)
	}
	for _, bad := range []string{"", "0.0", "1", "1.2.3", "a.b", "-1.0"} {
		if _, err := ParseVersion(bad); !errors.Is(err, ErrInvalidVersion) {
			t.Fatalf("ParseVersion(%q) = %v, want ErrInvalidVersion", bad, err)
		}
	}
	if _, err := UnmarshalBinary([]byte{0, 0, 0, 0}); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("UnmarshalBinary(zero) = %v, want ErrInvalidVersion", err)
	}
}

func TestRollingUpgradeNegotiationAndFeatureGates(t *testing.T) {
	nodeV10 := NodeVersion{MinSupported: V1_0, MaxSupported: V1_0, BinaryVersion: V1_0}
	nodeV11 := LocalNodeVersion()

	// Mixed 3-node cluster during rolling upgrade: 2 nodes on 1.1, 1 node still on 1.0.
	win, err := Negotiate([]NodeVersion{nodeV11, nodeV10, nodeV11})
	if err != nil {
		t.Fatalf("Negotiate mixed cluster: %v", err)
	}
	if win.MinCommon != V1_0 || win.MaxCommon != V1_0 || win.MinBinary != V1_0 {
		t.Fatalf("mixed window = %+v, want [1.0, 1.0] minBinary=1.0", win)
	}
	// Cannot advance active version to 1.1 while any node is still on 1.0.
	if err := ValidateAdvance(V1_0, V1_1, []NodeVersion{nodeV11, nodeV10, nodeV11}); !errors.Is(err, ErrIncompatibleClusterVersion) {
		t.Fatalf("ValidateAdvance during mixed rolling upgrade = %v, want ErrIncompatibleClusterVersion", err)
	}
	if !FeatureBaseRF3.Enabled(V1_0) || FeatureGlobalIndexSplitPrune.Enabled(V1_0) {
		t.Fatal("feature gates mismatched at V1_0")
	}

	// Once the final node upgrades to 1.1, advancing 1.0 -> 1.1 succeeds.
	if err := ValidateAdvance(V1_0, V1_1, []NodeVersion{nodeV11, nodeV11, nodeV11}); err != nil {
		t.Fatalf("ValidateAdvance after full upgrade failed: %v", err)
	}
	if !FeatureGlobalIndexSplitPrune.Enabled(V1_1) || !FeatureTinPersistedSidecar.Enabled(V1_1) {
		t.Fatal("V1_1 features not enabled at V1_1")
	}

	// Downgrade 1.1 -> 1.0 must fail closed.
	if err := ValidateAdvance(V1_1, V1_0, []NodeVersion{nodeV11, nodeV11, nodeV11}); !errors.Is(err, ErrVersionDowngrade) {
		t.Fatalf("ValidateAdvance downgrade = %v, want ErrVersionDowngrade", err)
	}

	// Disjoint versions fail closed.
	nodeV20 := NodeVersion{MinSupported: Version{2, 0}, MaxSupported: Version{2, 0}, BinaryVersion: Version{2, 0}}
	if _, err := Negotiate([]NodeVersion{nodeV10, nodeV20}); !errors.Is(err, ErrIncompatibleClusterVersion) {
		t.Fatalf("Negotiate disjoint = %v, want ErrIncompatibleClusterVersion", err)
	}
}
