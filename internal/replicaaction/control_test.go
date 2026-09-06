package replicaaction

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type memoryJournal struct {
	mu             sync.Mutex
	records        map[[32]byte]Record
	publishUnknown bool
}

func (journal *memoryJournal) ReadReplicaAction(_ context.Context, operation [32]byte, kind Kind) (Record, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, found := journal.records[replicaActionJournalKey(operation, kind)]
	if !found {
		return Record{}, ErrMissing
	}
	record.Request = cloneRequest(record.Request)
	return record, nil
}

func (journal *memoryJournal) PublishReplicaAction(_ context.Context, expected uint64, record Record) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	current, found := journal.records[replicaActionJournalKey(record.Request.Operation, record.Request.Kind)]
	if (!found && expected != 0) || (found && current.Revision != expected) {
		return ErrConflict
	}
	journal.records[replicaActionJournalKey(record.Request.Operation, record.Request.Kind)] = Record{Request: cloneRequest(record.Request), Revision: record.Revision, State: record.State}
	if journal.publishUnknown {
		journal.publishUnknown = false
		return errors.New("lost acknowledgement")
	}
	return nil
}

type fakeOwner struct {
	mu             sync.Mutex
	state          replicatedstate.State
	proposals      int
	applyOnPropose bool
	retirements    int
}

func (owner *fakeOwner) ProposeOwnershipTransition(_ context.Context, _ raftservice.ServingFence, raw []byte) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.proposals++
	if owner.applyOnPropose {
		view, _ := replicatedstate.OpenOwnershipTransition(raw)
		owner.state.Binding.OwnershipEpoch = view.ToOwnershipEpoch
		owner.state.Binding.RoutingVersion = view.ToRoutingVersion
		owner.state.Binding.RouteGeneration = view.ToRouteGeneration
		owner.state.Binding.OwnedRange = view.ToOwnedRange
	}
	return nil
}
func (owner *fakeOwner) ObserveReplica(_ context.Context, _ raftmember.GroupKey, _ uint64) (raftservice.ReplicaObservation, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return raftservice.ReplicaObservation{Publication: raftmodel.Publication{ReplicaSetVersion: owner.state.ReplicaSetVersion}, State: owner.state}, nil
}
func (owner *fakeOwner) RetireReplicaSource(_ context.Context, _ raftservice.ReplicaRetirementRequest) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.retirements++
	return nil
}

func actionFixture(t *testing.T, kind Kind) Request {
	t.Helper()
	binding := replicatedstate.Binding{Distribution: "accounts", Shard: "s-7", TopologyRecoveryEpoch: 3,
		AllocationGeneration: 5, ActivePolicyGeneration: 7, ProtectionEpoch: 11,
		OwnershipEpoch: 13, SchemaGeneration: 17, RoutingVersion: 19, RouteGeneration: 23,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	binding.ClusterID[0] = 1
	binding.ClusterIncarnation[0] = 2
	binding.ShardIncarnation[0] = 4
	binding.GroupID[0] = 6
	request := Request{Kind: kind, SourceMember: 2, TargetMember: 3}
	request.Operation[0] = 31
	request.Step[0] = 37
	request.Fence = raftservice.ServingFence{Group: raftmember.GroupKey{ClusterID: binding.ClusterID,
		ClusterIncarnation: binding.ClusterIncarnation, TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID}, AllocationGeneration: binding.AllocationGeneration,
		MemberID: 3, NodeIncarnation: 29, Term: 31}
	request.Fence.StoreID[0] = 41
	request.Fence.Command = raftservice.CommandFence{
		ReplicaSetVersion: 43, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration}
	request.Fence.Command.RelationManifestDigest[0] = 47
	if kind == OwnershipTransition {
		var err error
		request.Command, err = replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
			From: binding, ExpectedReplicaSetVersion: request.Fence.Command.ReplicaSetVersion,
			SourceMember: request.SourceMember, TargetMember: request.TargetMember,
			ToOwnershipEpoch: 14, ToRoutingVersion: 20, ToRouteGeneration: 24,
			ToOwnedRange: binding.OwnedRange})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		request.Fence.MemberID = request.SourceMember
	}
	return request
}

func ownerFor(request Request) *fakeOwner {
	view, _ := replicatedstate.OpenOwnershipTransition(request.Command)
	return &fakeOwner{state: replicatedstate.State{Binding: replicatedstate.Binding{
		ClusterID: view.ClusterID, ClusterIncarnation: view.ClusterIncarnation,
		TopologyRecoveryEpoch: view.TopologyRecoveryEpoch, Distribution: string(view.Distribution),
		Shard: string(view.Shard), AllocationGeneration: view.AllocationGeneration,
		ShardIncarnation: view.ShardIncarnation, GroupID: view.GroupID,
		ActivePolicyGeneration: view.ActivePolicyGeneration, ProtectionEpoch: view.ProtectionEpoch,
		OwnershipEpoch: view.OwnershipEpoch, SchemaGeneration: view.SchemaGeneration,
		RoutingVersion: view.RoutingVersion, RouteGeneration: view.RouteGeneration,
		OwnedRange: view.FromOwnedRange},
		ReplicaSetVersion: view.ExpectedReplicaSetVersion}}
}

func TestRequestCodecCanonicalRoundTripAndBounds(t *testing.T) {
	request := actionFixture(t, OwnershipTransition)
	raw, err := AppendRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRequest(raw)
	if err != nil || !equalRequest(opened, request) {
		t.Fatalf("open = %+v, %v", opened, err)
	}
	rebuilt, err := AppendRequest(nil, opened)
	if err != nil || !bytesEqual(rebuilt, raw) {
		t.Fatalf("canonical rebuild differs: %v", err)
	}
	for _, corrupt := range []func([]byte){
		func(b []byte) { b[9] = 1 }, func(b []byte) { b[152] = 1 }, func(b []byte) { b[324] = 1 },
		func(b []byte) { b[320]++ },
	} {
		candidate := append([]byte(nil), raw...)
		corrupt(candidate)
		if _, err := OpenRequest(candidate); !errors.Is(err, ErrControl) {
			t.Fatalf("corrupt accepted: %v", err)
		}
	}
	if _, err := OpenRequest(append(raw, 0)); !errors.Is(err, ErrControl) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
}

func TestOwnershipUnknownOutcomeSettlesOnExactReplay(t *testing.T) {
	request := actionFixture(t, OwnershipTransition)
	owner := ownerFor(request)
	journal := &memoryJournal{records: make(map[[32]byte]Record)}
	service := testService(t, journal, owner)
	if _, err := service.Execute(context.Background(), request); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("first execute err=%v, want outcome unknown", err)
	}
	if owner.proposals != 1 {
		t.Fatalf("proposals=%d, want 1", owner.proposals)
	}
	view, _ := replicatedstate.OpenOwnershipTransition(request.Command)
	owner.mu.Lock()
	owner.state.Binding.OwnershipEpoch = view.ToOwnershipEpoch
	owner.state.Binding.RoutingVersion = view.ToRoutingVersion
	owner.state.Binding.RouteGeneration = view.ToRouteGeneration
	owner.state.Binding.OwnedRange = view.ToOwnedRange
	owner.mu.Unlock()
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != Complete || record.Revision != 2 {
		t.Fatalf("replay = %+v, %v", record, err)
	}
	if owner.proposals != 1 {
		t.Fatalf("replay duplicated proposal: %d", owner.proposals)
	}
	conflict := request
	conflict.Step[1] = 1
	if _, err = service.Execute(context.Background(), conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("identity conflict err=%v", err)
	}
}

func TestRetirementIsJournaledAndIdempotent(t *testing.T) {
	request := actionFixture(t, SourceRetirement)
	owner := new(fakeOwner)
	journal := &memoryJournal{records: make(map[[32]byte]Record), publishUnknown: true}
	service := testService(t, journal, owner)
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != Complete {
		t.Fatalf("execute = %+v, %v", record, err)
	}
	if owner.retirements != 1 {
		t.Fatalf("retirements=%d", owner.retirements)
	}
	if _, err = service.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if owner.retirements != 1 {
		t.Fatalf("terminal replay duplicated retirement: %d", owner.retirements)
	}
}

func testService(t *testing.T, journal Journal, owner Owner) *Service {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewService(Options{Journal: journal, Owner: owner,
		Authorize:    func(rafttransport.PeerIdentity, Request) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 4})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
