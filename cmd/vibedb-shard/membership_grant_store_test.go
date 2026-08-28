package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type rf3GrantSink struct {
	grants map[raftmember.GroupKey]membershipgrant.Grant
}

func (sink *rf3GrantSink) InstallTransitionGrant(grant membershipgrant.Grant) error {
	if sink == nil || !grant.Valid() {
		return errRF3MembershipGrant
	}
	sink.grants[grant.Group] = grant
	return nil
}

func TestDurableRF3GrantRouterKeepsExactPerGroupFiles(t *testing.T) {
	root := t.TempDir()
	base := serveRF3TestManifest()
	base.EnrolledTarget = serveRF3TestEnrolledTarget()
	firstGroup := serveRF3TestGroup()
	secondGroup := firstGroup
	secondGroup.GroupID[0]++
	first := rf3ManifestGroup{Members: base.Members, MemberCount: rf3ManifestMembers,
		EnrolledTarget: base.EnrolledTarget, Route: rf3ManifestGroupRoute{
			Group: firstGroup, MembershipGrantPath: filepath.Join(root, "first.grant")}}
	second := first
	second.Route.Group = secondGroup
	second.Route.MembershipGrantPath = filepath.Join(root, "second.grant")
	manifest := rf3Manifest{Groups: []rf3ManifestGroup{first, second}}
	sink := &rf3GrantSink{grants: make(map[raftmember.GroupKey]membershipgrant.Grant)}
	router, err := openDurableRF3GrantRouter(manifest, sink)
	if err != nil {
		t.Fatal(err)
	}
	firstGrant := rf3MembershipGrantFixture(base, firstGroup, 9)
	secondGrant := rf3MembershipGrantFixture(base, secondGroup, 11)
	if err = router.InstallTransitionGrant(firstGrant); err != nil {
		t.Fatal(err)
	}
	if err = router.InstallTransitionGrant(secondGrant); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]membershipgrant.Grant{
		first.Route.MembershipGrantPath:  firstGrant,
		second.Route.MembershipGrantPath: secondGrant,
	} {
		got, found, readErr := readRF3MembershipGrant(path)
		if readErr != nil || !found || got != want {
			t.Fatalf("grant %q found=%t got=%+v err=%v", path, found, got, readErr)
		}
	}
	unknown := firstGrant
	unknown.Group.GroupID[1]++
	if err = router.InstallTransitionGrant(unknown); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("unknown group grant = %v", err)
	}
}

func TestDurableRF3GrantInstallerSurvivesUnknownAckAndLearnerHandoff(t *testing.T) {
	manifest := serveRF3TestManifest()
	manifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	group := serveRF3TestGroup()
	grant := rf3MembershipGrantFixture(manifest, group, 9)
	path := filepath.Join(t.TempDir(), "membership-grant")

	cold, err := openDurableRF3GrantInstaller(path, coldRF3GrantAuthority{
		group: group, members: manifest.Members, target: *manifest.EnrolledTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("injected crash before durable grant publication")
	cold.persist = func(string, membershipgrant.Grant) error { return crash }
	if err = cold.InstallTransitionGrant(grant); !errors.Is(err, crash) {
		t.Fatalf("pre-publication crash = %v", err)
	}
	if _, found, err := readRF3MembershipGrant(path); err != nil || found {
		t.Fatalf("failed publication became durable found=%t err=%v", found, err)
	}

	// A fresh cold process has no process-local grant. The exact retry installs
	// and fsyncs it before the control service can acknowledge.
	cold, err = openDurableRF3GrantInstaller(path, coldRF3GrantAuthority{
		group: group, members: manifest.Members, target: *manifest.EnrolledTarget,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = cold.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	if recovered, found, readErr := readRF3MembershipGrant(path); readErr != nil ||
		!found || recovered != grant {
		t.Fatalf("durable grant found=%t grant=%+v err=%v", found, recovered, readErr)
	}

	// Snapshot installation hands ownership to ordinary serve-rf3. Its new
	// registry starts at the durable learner cut and must restore the grant
	// before accepting any Raft traffic.
	members := rf3GrantTransportMembers(manifest, group, grant.InitialReplicaSetVersion+1,
		rafttransport.MemberLearner)
	registry, err := rafttransport.NewStaticRegistry(
		manifest.EnrolledTarget.NodeID, members,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: len(members)},
	)
	if err != nil {
		t.Fatal(err)
	}
	serving, err := openDurableRF3GrantInstaller(path, registry)
	if err != nil {
		t.Fatal(err)
	}
	if !serving.present || serving.grant != grant {
		t.Fatalf("serving handoff did not recover exact grant: %+v", serving)
	}
	if current, found, currentErr := registry.CurrentTransitionGrant(group); currentErr != nil ||
		!found || current != grant {
		t.Fatalf("registry grant found=%t grant=%+v err=%v", found, current, currentErr)
	}

	// A lost response after the durable publication is an exact idempotent
	// retry, while a different live transition can never overwrite authority.
	if err = serving.InstallTransitionGrant(grant); err != nil {
		t.Fatalf("retry after lost acknowledgement: %v", err)
	}
	forged := grant
	forged.TransitionID[0]++
	if err = serving.InstallTransitionGrant(forged); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("conflicting grant = %v", err)
	}
}

func TestDurableRF3GrantInstallerFailsClosedOnCorruptionAndWrongTarget(t *testing.T) {
	manifest := serveRF3TestManifest()
	manifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	group := serveRF3TestGroup()
	grant := rf3MembershipGrantFixture(manifest, group, 9)
	path := filepath.Join(t.TempDir(), "membership-grant")
	authority := coldRF3GrantAuthority{
		group: group, members: manifest.Members, target: *manifest.EnrolledTarget,
	}
	installer, err := openDurableRF3GrantInstaller(path, authority)
	if err != nil {
		t.Fatal(err)
	}
	wrongTarget := grant
	wrongTarget.TargetNode[0]++
	if err = installer.InstallTransitionGrant(wrongTarget); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("wrong target = %v", err)
	}
	if _, found, readErr := readRF3MembershipGrant(path); readErr != nil || found {
		t.Fatalf("forged grant persisted found=%t err=%v", found, readErr)
	}
	if err = installer.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1]++
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = openDurableRF3GrantInstaller(path, authority); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("corrupt durable grant = %v", err)
	}
}

func rf3MembershipGrantFixture(
	manifest rf3Manifest, group raftmember.GroupKey, version uint64,
) membershipgrant.Grant {
	var roster [3]membershipgrant.RosterMember
	var voters [3]uint64
	for index, member := range manifest.Members {
		voters[index] = member.MemberID
		roster[index] = membershipgrant.RosterMember{
			Member: member.MemberID, Node: [16]byte(member.NodeID),
		}
	}
	return membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{7}, MetadataEpoch: 8,
		CatalogGeneration: 9, InitialReplicaSetVersion: version,
		InitialVoters:           voters,
		InitialRosterDigest:     membershipgrant.CertifiedRosterDigest(group, version, roster),
		InitialDescriptorDigest: [32]byte{10}, SourceMember: manifest.Members[0].MemberID,
		TargetMember: manifest.EnrolledTarget.MemberID,
		TargetNode:   [16]byte(manifest.EnrolledTarget.NodeID),
	}
}

func rf3GrantTransportMembers(
	manifest rf3Manifest, group raftmember.GroupKey, version uint64,
	targetRole rafttransport.MemberRole,
) []rafttransport.Member {
	result := make([]rafttransport.Member, 0, len(manifest.Members)+1)
	for _, member := range manifest.Members {
		result = append(result, rafttransport.Member{
			Group: group, ReplicaSetVersion: version, MemberID: member.MemberID,
			Node: member.NodeID, Role: rafttransport.MemberVoter,
		})
	}
	result = append(result, rafttransport.Member{
		Group: group, ReplicaSetVersion: version,
		MemberID: manifest.EnrolledTarget.MemberID, Node: manifest.EnrolledTarget.NodeID,
		Role: targetRole,
	})
	return result
}
