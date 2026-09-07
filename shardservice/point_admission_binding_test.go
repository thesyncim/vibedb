package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

func TestReplicatedServerConcurrentServingBindingKeepsOneCanonicalPredicate(t *testing.T) {
	state := testReplicatedServingState()
	server := testReplicatedServer(&fakeReplicatedOwner{state: state})
	concurrent := func(candidate raftservice.ServingState) bool {
		return candidate == state
	}
	if err := server.BindConcurrentServingAuthority(concurrent); err != nil {
		t.Fatal(err)
	}
	if server.serving == nil || server.concurrentServing == nil ||
		!server.serving(state) || !server.concurrentServing(state) {
		t.Fatal("concurrent binding did not install one serving predicate")
	}
	if err := server.BindServingAuthority(func(raftservice.ServingState) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if server.concurrentServing != nil || server.serving(state) {
		t.Fatal("legacy serving rebind retained warm concurrent eligibility")
	}

	transition := func(candidate raftservice.ServingState, request *ReplicatedRequest) bool {
		return candidate == state && request != nil
	}
	if err := server.BindConcurrentTransitionalServingAuthority(transition); err != nil {
		t.Fatal(err)
	}
	probe := &ReplicatedRequest{}
	if server.transition == nil || server.concurrentTransition == nil ||
		!server.transition(state, probe) || !server.concurrentTransition(state, probe) {
		t.Fatal("concurrent transition binding did not install one predicate")
	}
	if err := server.BindTransitionalServingAuthority(func(raftservice.ServingState, *ReplicatedRequest) bool {
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if server.concurrentTransition != nil || server.transition(state, probe) {
		t.Fatal("legacy transition rebind retained warm concurrent eligibility")
	}
}

func TestReplicatedSQLPointAdmissionSelectsSafeBoundPredicate(t *testing.T) {
	state := testReplicatedServingState()
	state.Identity.Distribution, state.Identity.Shard = "data", "all"
	request, inner := testReplicatedSQLPointCall(state, PrimaryKeyReadRequest{
		Relation: 1, MaxDocumentBytes: 1024,
		PrimaryPath: []byte("/id"), Keys: [][]byte{[]byte("key")},
	})
	owner := &replicatedSQLPointPathOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
		pointErr:            raftmodel.ErrNotLeader,
	}
	server := testReplicatedServer(owner)
	if err := server.BindConcurrentServingAuthority(func(raftservice.ServingState) bool {
		return false
	}); err != nil {
		t.Fatal(err)
	}
	transition := func(candidate raftservice.ServingState, candidateRequest *ReplicatedRequest) bool {
		return candidate == state && candidateRequest == request
	}
	if err := server.BindConcurrentTransitionalServingAuthority(transition); err != nil {
		t.Fatal(err)
	}

	server.executeReplicatedAuthenticatedCallValidated(t.Context(), request, true, inner, true)
	if owner.pointRequest == nil || owner.pointRequest.Authorize != nil || owner.pointRequest.ConcurrentAuthorize == nil {
		t.Fatalf("authenticated concurrent request=%+v, want only ConcurrentAuthorize", owner.pointRequest)
	}
	if !owner.pointRequest.ConcurrentAuthorize(state) {
		t.Fatal("authenticated concurrent predicate dropped the transitional serving gate")
	}
	changed := state
	changed.Status.Term++
	if owner.pointRequest.ConcurrentAuthorize(changed) {
		t.Fatal("authenticated concurrent predicate accepted a changed state")
	}

	owner.pointRequest = nil
	server.executeReplicatedAuthenticatedCallValidated(t.Context(), request, false, inner, true)
	if owner.pointRequest == nil || owner.pointRequest.Authorize != nil || owner.pointRequest.ConcurrentAuthorize == nil {
		t.Fatalf("unauthenticated concurrent request=%+v, want only ConcurrentAuthorize", owner.pointRequest)
	}
	if owner.pointRequest.ConcurrentAuthorize(state) {
		t.Fatal("unauthenticated concurrent predicate borrowed transitional authority")
	}

	if err := server.BindTransitionalServingAuthority(transition); err != nil {
		t.Fatal(err)
	}
	owner.pointRequest = nil
	server.executeReplicatedAuthenticatedCallValidated(t.Context(), request, true, inner, true)
	if owner.pointRequest == nil || owner.pointRequest.Authorize == nil || owner.pointRequest.ConcurrentAuthorize != nil {
		t.Fatalf("legacy transition request=%+v, want only serialized Authorize", owner.pointRequest)
	}
	if !owner.pointRequest.Authorize(state) {
		t.Fatal("serialized fallback dropped the transitional serving gate")
	}
}
