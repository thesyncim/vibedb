package main

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestRF3DynamicGrantRouterPersistsAndDefersRestoredAuthority(t *testing.T) {
	manifest := serveRF3TestManifest()
	manifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	group := serveRF3TestGroup()
	grant := rf3MembershipGrantFixture(manifest, group, 9)
	path := filepath.Join(t.TempDir(), "membership-grant")
	sink := &rf3GrantSink{grants: make(map[raftmember.GroupKey]membershipgrant.Grant)}
	router := newRF3DynamicGrantRouter(sink)
	if err := router.InstallTransitionGrant(grant); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("unregistered group accepted grant: %v", err)
	}
	if _, found, err := router.Register(group, path); err != nil || found {
		t.Fatalf("new grant reservation found=%t err=%v", found, err)
	}
	if err := router.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	if persisted, found, err := readRF3MembershipGrant(path); err != nil || !found || persisted != grant {
		t.Fatalf("successful install did not persist exact grant: found=%t err=%v", found, err)
	}
	restartedSink := &rf3GrantSink{grants: make(map[raftmember.GroupKey]membershipgrant.Grant)}
	restarted := newRF3DynamicGrantRouter(restartedSink)
	if recovered, found, err := restarted.Register(group, path); err != nil || !found || recovered != grant {
		t.Fatalf("restart lost exact grant: found=%t err=%v", found, err)
	}
	if len(restartedSink.grants) != 0 {
		t.Fatal("registration exposed grant before atomic group restoration")
	}
	otherGroup := group
	otherGroup.GroupID[0]++
	if _, _, err := restarted.Register(otherGroup, path); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("foreign group reused grant file: %v", err)
	}
	changed := grant
	changed.TargetNode[0]++
	if err := restarted.InstallTransitionGrant(changed); !errors.Is(err, errRF3MembershipGrant) {
		t.Fatalf("changed grant replaced retained proof: %v", err)
	}
	if err := restarted.InstallTransitionGrant(grant); err != nil {
		t.Fatalf("exact retry after restart: %v", err)
	}
}
