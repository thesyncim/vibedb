package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestNativeSessionControlBindingRequiresDurableExactRouteMember(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	tenant := []byte("split-prune:0123456789abcdef")
	clientID := replication.ID128{0x71}
	session := &NativeSession{
		route: route, tenant: tenant, clientID: clientID,
		resolver: BaseRelationResolver{Relation: 1}, journal: &NativeSessionJournal{},
		proposalCapability: serviceauthz.CapabilityTopology,
	}
	if !NativeSessionMatchesControlBinding(
		session, raftServingFence(state.Fence), tenant, clientID, 1,
		serviceauthz.CapabilityTopology,
	) {
		t.Fatal("exact durable control binding rejected")
	}
	stale := raftServingFence(state.Fence)
	stale.Command.RouteGeneration++
	if NativeSessionMatchesControlBinding(
		session, stale, tenant, clientID, 1, serviceauthz.CapabilityTopology,
	) {
		t.Fatal("stale command fence accepted")
	}
	foreign := raftServingFence(state.Fence)
	foreign.StoreID[0] ^= 0xff
	if NativeSessionMatchesControlBinding(
		session, foreign, tenant, clientID, 1, serviceauthz.CapabilityTopology,
	) {
		t.Fatal("foreign source incarnation accepted")
	}
	restarted := raftServingFence(state.Fence)
	restarted.NodeIncarnation++
	if NativeSessionMatchesControlBinding(session, restarted, tenant, clientID, 1, serviceauthz.CapabilityTopology) {
		t.Fatal("unobserved process incarnation accepted")
	}
	for index := range session.route.Replicas {
		if session.route.Replicas[index].Member == restarted.MemberID {
			session.route.Replicas[index].NodeIncarnation = restarted.NodeIncarnation
		}
	}
	if !NativeSessionMatchesControlBinding(session, restarted, tenant, clientID, 1, serviceauthz.CapabilityTopology) {
		t.Fatal("exact refreshed process incarnation rejected")
	}
	session.journal = nil
	if NativeSessionMatchesControlBinding(
		session, raftServingFence(state.Fence), tenant, clientID, 1,
		serviceauthz.CapabilityTopology,
	) {
		t.Fatal("non-durable session accepted")
	}
}

func TestTopologyMutationJournalsExactCertifiedFingerprintAcrossUnknown(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	route.RangeIdentity = replication.Digest{0x31}
	route.LineageDigest = replication.Digest{0x32}
	route.ForwardingRuleDigest = replication.Digest{0x33}
	client := &nativeSessionClient{state: states["m2"], unknownMutationOnce: true}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("split-prune:certified")
	clientID := replication.ID128{0x72}
	binding, err := NativeSessionJournalBinding(
		route, string(route.Distribution), string(route.Shard), tenant, 1,
		serviceauthz.CapabilityTopology,
	)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(t.TempDir(), "prune")
	journal, err := OpenNativeSessionJournal(NativeSessionJournalOptions{
		Path: journalPath, ClientID: clientID,
		MaxCommandBytes: 1 << 20, Binding: binding,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(route.Distribution),
		Shard: string(route.Shard), Tenant: tenant, ClientID: clientID,
		Resolver: BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(context.Background(), 1<<60); err != nil {
		t.Fatal(err)
	}
	if !NativeSessionSupportsMutationBound(session, 1, 4, 1024) ||
		!NativeSessionSupportsMutationBound(session, 1, 4, 1<<20) ||
		NativeSessionSupportsMutationBound(session, 1, 5, 1024) {
		t.Fatal("native session mutation bounds were not enforced exactly")
	}
	fingerprint := replication.Digest{0x81, 0x82}
	proof := replication.RetainedPruneProof{
		OperationDigest: replication.Digest{1}, CertificateDigest: replication.Digest{2},
		BatchDigest: fingerprint, DataChainDigest: replication.Digest{3},
		EntryDigest: replication.Digest{4}, BaseDigest: replication.Digest{5},
		CutApplied: 6, CutTerm: 7, OwnershipEpoch: route.Command.OwnershipEpoch,
		RoutingVersion: route.Command.RoutingVersion, RouteGeneration: route.Command.RouteGeneration,
		RetainedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{
			Point: distribution.KeyspacePoint{0x80},
		}},
	}
	_, err = session.RetainedPruneBatch(context.Background(), []NativeMutation{{
		Kind: replication.MutationDelete, Key: []byte("outside-range"),
	}}, proof)
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || !session.Status().Pending {
		t.Fatalf("first topology mutation err=%v status=%+v", err, session.Status())
	}
	pending, openErr := replication.OpenCommand(session.PendingCommand())
	if openErr != nil || pending.Fingerprint != fingerprint ||
		pending.AuthorityClass != replication.CommandAuthorityTopology ||
		pending.Kind() != replication.CommandRetainedPrune {
		t.Fatalf("pending fingerprint=%x authority=%d err=%v",
			pending.Fingerprint, pending.AuthorityClass, openErr)
	}
	reopenedJournal, err := OpenNativeSessionJournal(NativeSessionJournalOptions{
		Path: journalPath, ClientID: clientID,
		MaxCommandBytes: 1 << 20, Binding: binding,
	})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(route.Distribution),
		Shard: string(route.Shard), Tenant: tenant, ClientID: clientID,
		Resolver: BaseRelationResolver{Relation: 1}, Journal: reopenedJournal,
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil || !restarted.Status().Pending ||
		!NativeSessionMatchesControlBinding(
			restarted, raftServingFence(states["m2"].Fence), tenant, clientID, 1,
			serviceauthz.CapabilityTopology,
		) {
		t.Fatalf("restart status=%+v err=%v", restarted.Status(), err)
	}
	result, err := restarted.RetryPending(context.Background())
	if err != nil || result.Completion.ResultCode != replicatedstate.ResultApplied ||
		restarted.Status().Pending {
		t.Fatalf("retry result=%+v status=%+v err=%v", result, restarted.Status(), err)
	}
}

func raftServingFence(fence shardservice.ReplicatedFence) raftservice.ServingFence {
	return raftservice.ServingFence{
		Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		Command: fence.Command, MemberID: fence.MemberID, StoreID: fence.StoreID,
		NodeIncarnation: fence.NodeIncarnation, Term: fence.Term,
	}
}
