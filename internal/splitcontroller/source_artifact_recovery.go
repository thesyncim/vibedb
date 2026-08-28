package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type SplitSourceLeadership interface {
	TransferSplitSourceLeadership(context.Context, raftservice.ServingFence, uint64) error
}

type sourceArtifactCatalog struct{ snapshot *gateway.Snapshot }

func (catalog sourceArtifactCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.snapshot, nil
}

// RecoverSourceArtifactOwner runs only inside a witnessed BuildArtifacts
// action. An artifact is an immutable cut, not a fresh view of whichever
// replica is leader now. Rejoin its durable owner before continuing the split.
// If any source cannot be observed, refuse to invent a competing artifact.
func RecoverSourceArtifactOwner(ctx context.Context, plan *Plan, observed Observation,
	opener PlanObservationStreamOpener, deadline rafttransport.DeadlineFunc, leadership SplitSourceLeadership,
) (bool, error) {
	if ctx == nil || plan == nil || observed.Catalog == nil || leadership == nil {
		return false, ErrInvalidPlan
	}
	if observed.Artifacts != nil {
		return false, nil
	}
	digest, err := gateway.CatalogSnapshotDigest(observed.Catalog)
	if err != nil {
		return false, err
	}
	requests, source, err := plan.observationRequests(observed.Catalog, digest)
	if err != nil {
		return false, err
	}
	directory, err := NewCatalogPlanObservationPeerDirectory(sourceArtifactCatalog{observed.Catalog})
	if err != nil {
		return false, err
	}
	client, err := NewNetworkPlanObservationClient(NetworkPlanObservationClientOptions{
		Opener: opener, Directory: directory, ReadDeadline: deadline, WriteDeadline: deadline,
		MaxConcurrent: 1, MaxResponseBytes: MaxPlanObservationResponseBytes,
	})
	if err != nil {
		return false, err
	}
	request := requests[source]
	var artifact *rangesplit.ChildArtifactSet
	var owner uint64
	for _, endpoint := range request.ControlEndpoints {
		peer, err := directory.ResolvePlanObservationPeer(ctx, request, endpoint)
		if err != nil {
			return false, err
		}
		wire := planObservationWireRequest{Format: planObservationWireFormat, Kind: planObservationSource,
			TargetMember: peer.MemberID, Request: request}
		response, err := client.exchange(ctx, peer, wire)
		if err != nil {
			return false, err
		}
		cut, err := openSourcePlanObservation(response)
		if err != nil || !validNetworkSourceObservation(wire, cut) {
			return false, errors.Join(ErrPlanObservation, err)
		}
		if cut.Artifacts == nil {
			continue
		}
		if plan.partitioner.ValidateChildArtifactSet(*cut.Artifacts) != nil ||
			artifact != nil && *artifact != *cut.Artifacts {
			return false, ErrTopologyConflict
		}
		artifact = cut.Artifacts
		if owner == 0 || peer.MemberID < owner {
			owner = peer.MemberID
		}
	}
	if owner == 0 || owner == observed.SourceStatus.MemberID {
		return false, nil
	}
	return true, leadership.TransferSplitSourceLeadership(ctx, observed.SourceServing.Fence(), owner)
}
