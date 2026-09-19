package main

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
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

func TestRF3DynamicGrantRecoveryPreservesActiveAndReplacesCompletedAuthority(t *testing.T) {
	intent := rf3RecoveryEnrollmentIntent()
	cut := rf3RecoveryServingCut(t, intent)
	cut.CatalogGeneration = 20
	manifest := serveRF3TestManifest()
	manifest.EnrolledTarget = serveRF3TestEnrolledTarget()
	manifest.EnrolledTarget.NodeID = intent.Target.Node
	previous := rf3MembershipGrantFixture(manifest, intent.Group, 1)
	next := previous
	next.InitialReplicaSetVersion, next.CatalogGeneration = 4, previous.CatalogGeneration+1
	next.InitialVoters, next.SourceMember, next.TargetMember = [3]uint64{2, 3, 4}, 2, 5
	next.TargetNode = [16]byte{9}
	for _, name := range []string{"active", "successor", "absence", "stale"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "grant")
			if err := persistRF3MembershipGrant(path, previous); err != nil {
				t.Fatal(err)
			}
			router := newRF3DynamicGrantRouter(&rf3GrantSink{grants: make(map[raftmember.GroupKey]membershipgrant.Grant)})
			if _, _, err := router.Register(intent.Group, path); err != nil {
				t.Fatal(err)
			}
			current := cut
			candidate := next
			current.CurrentGrant = &candidate
			publication := raftmodel.Publication{ReplicaSetVersion: 4, ConfState: &pb.ConfState{Voters: []uint64{2, 3, 4}}}
			want := next
			switch name {
			case "active":
				publication.ReplicaSetVersion = 3
				publication.ConfState.Voters = []uint64{1, 2, 3, 4}
				want = previous
			case "absence":
				current.CurrentGrant = nil
				want = membershipgrant.Grant{}
			case "stale":
				candidate.CatalogGeneration = previous.CatalogGeneration
			}
			actual, err := router.Recover(intent.Group, publication, &current)
			if name == "stale" {
				if !errors.Is(err, nodecontrol.ErrStale) {
					t.Fatalf("stale grant accepted: %v", err)
				}
				want = previous
			} else if err != nil || actual != want {
				t.Fatalf("recovery got=%+v err=%v", actual, err)
			}
			stored, found, err := readRF3MembershipGrant(path)
			if err != nil || found != (want != (membershipgrant.Grant{})) || stored != want {
				t.Fatalf("wrong durable authority: %+v found=%v err=%v", stored, found, err)
			}
			if name != "stale" {
				if retry, err := router.Recover(intent.Group, publication, &current); err != nil || retry != want {
					t.Fatalf("recovery retry: %+v %v", retry, err)
				}
			}
		})
	}
}
