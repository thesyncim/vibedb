//go:build !vibedb_rf3_read_authority_lab

package main

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

func TestStandardShardRejectsEnabledReadAuthorityBeforeRuntime(t *testing.T) {
	config := &rf3ManifestReadAuthority{
		Enabled: true, FeatureVersion: rf3ReadAuthorityFeatureVersion,
		PolicyVersion:        rf3ReadAuthorityPolicyVersion,
		MaxGrantMillis:       uint64(rf3ReadAuthorityMaxGrant / time.Millisecond),
		ClockRatePPM:         rf3ReadAuthorityClockRatePPM,
		RoundingMarginMillis: uint64(rf3ReadAuthorityMargin / time.Millisecond),
		Voters:               []uint64{1, 2, 3},
		Capabilities: []rf3ManifestVoterCapability{
			{MemberID: 1, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 2, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 3, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
		},
	}
	group := rf3ManifestGroup{
		Members: [rf3ManifestMembers]rf3ManifestMember{
			{MemberID: 1, NodeID: rafttransport.NodeID{1}, StoreID: [16]byte{11}, NativeAddress: "127.0.0.1:7501"},
			{MemberID: 2, NodeID: rafttransport.NodeID{2}, StoreID: [16]byte{12}, NativeAddress: "127.0.0.1:7502"},
			{MemberID: 3, NodeID: rafttransport.NodeID{3}, StoreID: [16]byte{13}, NativeAddress: "127.0.0.1:7503"},
		},
		MemberCount: rf3ManifestMembers,
	}
	if err := validateRF3ReadAuthority(config, []rf3ManifestGroup{group}, false); !errors.Is(err, errRF3ReadAuthority) {
		t.Fatalf("standard enabled policy error = %v", err)
	}
}

func TestStandardOmittedPolicyStillRefusesEnrolledMarker(t *testing.T) {
	policy := raftauthority.ReadAuthorityPolicy{
		Enabled: true, PolicyVersion: rf3ReadAuthorityPolicyVersion,
		MaxGrant: rf3ReadAuthorityMaxGrant, ClockRatePPM: rf3ReadAuthorityClockRatePPM,
		RoundingMargin: rf3ReadAuthorityMargin, Voters: []uint64{1, 2, 3},
		Capabilities: []raftauthority.VoterCapability{
			{MemberID: 1, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 2, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 3, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
		},
	}
	root := t.TempDir()
	if err := ensureRF3ReadAuthorityState(root, policy); !errors.Is(err, errRF3ReadAuthority) {
		t.Fatalf("standard marker enrollment error = %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("standard marker enrollment wrote %d entries", len(entries))
	}
	state := rf3ReadAuthorityStateFor(policy)
	stateRaw, err := vibejson.Marshal(&state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rf3ReadAuthorityMarkerPath(root), stateRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureRF3ReadAuthorityDisabled(root); !errors.Is(err, errRF3ReadAuthorityDowngrade) {
		t.Fatalf("omitted policy marker error = %v", err)
	}
}
