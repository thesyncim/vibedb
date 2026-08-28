package splitcontroller

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type artifactRecoveryProvider struct {
	*planObservationTestProvider
	artifacts *rangesplit.ChildArtifactSet
	owner     uint64
}

func (provider artifactRecoveryProvider) ObserveSplitSource(ctx context.Context, request PlanObservationRequest, member uint64) (SourcePlanObservation, error) {
	cut, err := provider.planObservationTestProvider.ObserveSplitSource(ctx, request, member)
	if member == provider.owner {
		cut.Artifacts = provider.artifacts
	}
	return cut, err
}

type artifactRecoveryLeadership struct{ target uint64 }

func (leadership *artifactRecoveryLeadership) TransferSplitSourceLeadership(_ context.Context, _ raftservice.ServingFence, target uint64) error {
	leadership.target = target
	return nil
}

func TestArtifactRecoveryRejoinsOriginalCutAndRefusesUnobservedReplica(t *testing.T) {
	plan, catalog, descriptor := testReplicatedProjectionPlan(t)
	state := testSourceState(plan)
	state.ReplicaSetVersion = descriptor.Command.ReplicaSetVersion
	artifacts := testArtifactSet(t, plan, state)
	artifacts.Children[1].TargetChunkBytes = rangesplit.DefaultChildArtifactChunkBytes
	provider := artifactRecoveryProvider{planObservationTestProvider: &planObservationTestProvider{state: SourcePlanObservation{State: state}}, artifacts: &artifacts, owner: 2}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewPlanObservationService(PlanObservationServiceOptions{
		Provider: provider, Authorize: func(rafttransport.PeerIdentity, PlanObservationRequest, uint64, bool) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 3, MaxResponseBytes: MaxPlanObservationResponseBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	trust := rafttransport.TrustDomain{ClusterID: descriptor.Group.ClusterID, ClusterIncarnation: descriptor.Group.ClusterIncarnation}
	opener := &planObservationTestOpener{service: service, controller: rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}, TrustDomain: trust}, peers: make(map[rafttransport.NodeID]rafttransport.PeerIdentity)}
	opener.errors = make(chan error, 3)
	for _, replica := range descriptor.Replicas {
		opener.peers[replica.Node] = rafttransport.PeerIdentity{Node: replica.Node, TrustDomain: trust}
	}
	digest, _ := gateway.CatalogSnapshotDigest(catalog)
	requests, source, err := plan.observationRequests(catalog, digest)
	if err != nil {
		t.Fatal(err)
	}
	serving := servingForPlanObservation(requests[source], 1, state.Applied)
	observed := Observation{Catalog: catalog, SourceState: state, SourceStatus: serving.Status, SourceServing: serving}
	leadership := &artifactRecoveryLeadership{}
	recovering, err := RecoverSourceArtifactOwner(t.Context(), plan, observed, opener, deadline, leadership)
	if err != nil || !recovering || leadership.target != 2 {
		select {
		case serviceErr := <-opener.errors:
			t.Logf("observation service: %v", serviceErr)
		default:
		}
		t.Fatalf("original artifact owner not recovered: recovering=%t target=%d err=%v", recovering, leadership.target, err)
	}
	delete(opener.peers, descriptor.Replicas[2].Node)
	leadership.target = 0
	if _, err = RecoverSourceArtifactOwner(t.Context(), plan, observed, opener, deadline, leadership); err == nil || leadership.target != 0 {
		t.Fatal("unobserved replica allowed a competing artifact or transfer")
	}
}
