package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

type testHealthRevisionAuthority struct {
	mu        sync.Mutex
	revisions map[uint64]uint64
	published []gateway.ReplicaHealthRevision
}

type testHealthStatusAuthority struct{ *testHealthRevisionAuthority }

func (authority testHealthStatusAuthority) ReadReplicaHealthRevisionStatus(_ context.Context, _ raftmember.GroupKey, suspect uint64) (gateway.ReplicaHealthRevisionStatus, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	for i := len(authority.published) - 1; i >= 0; i-- {
		r := authority.published[i]
		if r.SuspectMember == suspect {
			return gateway.ReplicaHealthRevisionStatus{Revision: r.Revision, CatalogGeneration: r.CatalogGeneration,
				ReplicaSetVersion: r.ReplicaSetVersion, SuspectNode: r.SuspectNode, SuspectIncarnation: r.SuspectIncarnation,
				Healthy: !r.Attestations[0].Failed}, nil
		}
	}
	return gateway.ReplicaHealthRevisionStatus{}, nil
}

func TestReplicaHealthRevisionSkipsUnchangedHealthyStateAcrossRestart(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	client := testHealthRoundClient{status: map[uint64]raftmember.RuntimeStatus{
		1: {MemberID: 1, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
		2: {MemberID: 2, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
		3: {MemberID: 3, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
	}, failed: make(map[uint64]error)}
	authority := testHealthStatusAuthority{&testHealthRevisionAuthority{revisions: make(map[uint64]uint64)}}
	run := func(published, unchanged, suspects uint64) {
		t.Helper()
		// Reconstruct every time: suppression must rely on durable state, not a
		// process-local cache that can forget a failed observation on restart.
		controller, err := newGatewayReplicaHealthRevisionController(testReplicaHealthCatalog{snapshot}, client, authority)
		if err != nil {
			t.Fatal(err)
		}
		pass, err := controller.RunPass(t.Context())
		if err != nil || pass.Published != published || pass.Unchanged != unchanged || pass.Suspects != suspects {
			t.Fatalf("pass=%+v err=%v", pass, err)
		}
	}
	run(3, 0, 0)
	for range 3 {
		run(0, 3, 0)
	}
	client.failed[1] = errors.New("isolated member")
	for range 3 {
		run(1, 0, 1)
	}
	delete(client.failed, 1)
	run(1, 2, 0) // Recovery must persist the clear for the failed member.
	run(0, 3, 0)
}

func (authority *testHealthRevisionAuthority) ReadReplicaHealthRevision(
	_ context.Context, _ raftmember.GroupKey, suspect uint64,
) (uint64, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	return authority.revisions[suspect], nil
}

func (authority *testHealthRevisionAuthority) PublishReplicaHealthRevision(
	_ context.Context, revision gateway.ReplicaHealthRevision,
) error {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if revision.Revision != authority.revisions[revision.SuspectMember]+1 {
		return errors.New("nonmonotonic test revision")
	}
	authority.revisions[revision.SuspectMember] = revision.Revision
	authority.published = append(authority.published, revision)
	return nil
}

func TestReplicaHealthRevisionPublishesOnlyQuorumAbsentMember(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	authority := &testHealthRevisionAuthority{revisions: make(map[uint64]uint64)}
	controller, err := newGatewayReplicaHealthRevisionController(
		testReplicaHealthCatalog{snapshot}, testAuthenticatedHealthClient{}, authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	for passNumber := uint64(1); passNumber <= 2; passNumber++ {
		pass, runErr := controller.RunPass(context.Background())
		if runErr != nil || pass.Groups != 1 || pass.Certified != 1 ||
			pass.Suspects != 1 || pass.Published != 1 {
			t.Fatalf("pass=%+v err=%v", pass, runErr)
		}
		revision := authority.published[len(authority.published)-1]
		if revision.SuspectMember != 1 || revision.Revision != passNumber ||
			len(revision.Attestations) != 2 || !revision.Attestations[0].Failed ||
			revision.Attestations[0].Member != 2 || revision.Attestations[1].Member != 3 {
			t.Fatalf("revision=%+v", revision)
		}
	}
}

type testHealthRoundClient struct {
	failed map[uint64]error
	status map[uint64]raftmember.RuntimeStatus
}

func (client testHealthRoundClient) Observe(
	_ context.Context, _ rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if err := client.failed[request.TargetMember]; err != nil {
		return replicacontrol.Observation{}, err
	}
	status := client.status[request.TargetMember]
	return replicacontrol.Observation{Request: request, Status: status,
		Publication: raftmodel.Publication{Applied: status.Applied,
			ReplicaSetVersion: request.ExpectedReplicaSetVersion}}, nil
}

func TestReplicaHealthRevisionRejectsPartitionWithoutLeader(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	client := testHealthRoundClient{
		failed: map[uint64]error{2: errors.New("leader partitioned")},
		status: map[uint64]raftmember.RuntimeStatus{
			1: {MemberID: 1, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
			3: {MemberID: 3, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
		},
	}
	authority := &testHealthRevisionAuthority{revisions: make(map[uint64]uint64)}
	controller, err := newGatewayReplicaHealthRevisionController(
		testReplicaHealthCatalog{snapshot}, client, authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := controller.RunPass(context.Background())
	if err == nil || pass.Certified != 0 || pass.Published != 0 || len(authority.published) != 0 {
		t.Fatalf("pass=%+v publications=%d err=%v", pass, len(authority.published), err)
	}
}

func TestReplicaHealthRevisionRejectsStaleLeaderCut(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	client := testHealthRoundClient{status: map[uint64]raftmember.RuntimeStatus{
		1: {MemberID: 1, LeaderID: 1, Term: 3, Commit: 29, Applied: 29},
		2: {MemberID: 2, LeaderID: 1, Term: 4, Commit: 30, Applied: 30},
		3: {MemberID: 3, LeaderID: 1, Term: 4, Commit: 30, Applied: 30},
	}}
	authority := &testHealthRevisionAuthority{revisions: make(map[uint64]uint64)}
	controller, err := newGatewayReplicaHealthRevisionController(
		testReplicaHealthCatalog{snapshot}, client, authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := controller.RunPass(context.Background())
	if err == nil || pass.Certified != 0 || pass.Published != 0 {
		t.Fatalf("pass=%+v err=%v", pass, err)
	}
}

func TestReplicaHealthRevisionClearsEveryMemberOnlyOnFullAgreement(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	client := testHealthRoundClient{status: map[uint64]raftmember.RuntimeStatus{
		1: {MemberID: 1, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
		2: {MemberID: 2, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
		3: {MemberID: 3, LeaderID: 2, Term: 4, Commit: 30, Applied: 30},
	}}
	authority := &testHealthRevisionAuthority{revisions: make(map[uint64]uint64)}
	controller, err := newGatewayReplicaHealthRevisionController(
		testReplicaHealthCatalog{snapshot}, client, authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := controller.RunPass(context.Background())
	if err != nil || pass.Certified != 1 || pass.Suspects != 0 || pass.Published != 3 {
		t.Fatalf("pass=%+v err=%v", pass, err)
	}
	for _, revision := range authority.published {
		if len(revision.Attestations) != 3 || revision.Attestations[0].Failed {
			t.Fatalf("healthy revision=%+v", revision)
		}
	}
}
