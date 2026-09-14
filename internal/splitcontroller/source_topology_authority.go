package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	"github.com/thesyncim/vibedb/shardservice"
)

var ErrSourceTopology = errors.New("splitcontroller: source topology request is unauthorized or stale")

// SourceTopologyStep is the exact catalog-committed remote action, propagated
// only after local admission and its durable action witness have been checked.
type SourceTopologyStep struct {
	Operation         OperationID
	PlanDigest        [32]byte
	Step              [32]byte
	ExecutionRevision uint64
}

type sourceTopologyStepKey struct{}

func SourceTopologyStepFromContext(ctx context.Context) (SourceTopologyStep, bool) {
	if ctx == nil {
		return SourceTopologyStep{}, false
	}
	step, ok := ctx.Value(sourceTopologyStepKey{}).(SourceTopologyStep)
	return step, ok && step.valid()
}

func (step SourceTopologyStep) valid() bool {
	return step.Operation != (OperationID{}) && step.PlanDigest != ([32]byte{}) && step.Step != ([32]byte{}) && step.ExecutionRevision != 0
}

type SourceTopologyCatalog interface {
	ReadReplicatedCatalogHead(context.Context) (*gateway.Snapshot, replication.Digest, error)
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
	ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error)
}

type SourceTopologyNativeClient interface {
	gateway.ReplicatedRoundTripper
	ProbeReplicated(context.Context, gateway.ReplicatedRoute, gateway.ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error)
}

// Addresses never cross this boundary. The gateway selects a destination only
// from the current source allocation authenticated by the committed plan.
type sourceTopologyMember struct {
	Node        rafttransport.NodeID
	Member      uint64
	Store       [16]byte
	Incarnation uint64
}

type sourceTopologyRequest struct {
	Step        SourceTopologyStep
	Source      raftmember.RuntimeIdentity
	Destination sourceTopologyMember
	Native      *shardservice.ReplicatedRequest
}

func authorizeSourceTopology(record gateway.ReplicatedOperationRecord, catalog *gateway.Snapshot,
	directory gateway.NodeDirectoryCut, peer rafttransport.PeerBinding, request sourceTopologyRequest,
) (gateway.ReplicatedRoute, gateway.ReplicatedEndpoint, error) {
	deny := func() (gateway.ReplicatedRoute, gateway.ReplicatedEndpoint, error) {
		return gateway.ReplicatedRoute{}, gateway.ReplicatedEndpoint{}, ErrSourceTopology
	}
	if catalog == nil || !directory.Valid() || !request.Step.valid() ||
		record.ID != [32]byte(request.Step.Operation) || record.Kind != gateway.ReplicatedOperationSplit ||
		record.IntentDigest != request.Step.PlanDigest || record.ExecutionRevision != request.Step.ExecutionRevision ||
		request.Source.Group.ClusterID != peer.Identity.TrustDomain.ClusterID || request.Source.Group.ClusterIncarnation != peer.Identity.TrustDomain.ClusterIncarnation ||
		request.Native == nil || request.Native.Capability != serviceauthz.CapabilityTopology || request.Native.Continuation != nil ||
		shardservice.ValidateReplicatedRequest(request.Native) != nil {
		return deny()
	}
	action, err := pendingRemoteAction(record)
	if err != nil || (action.Kind != ActionStartCapture && action.Kind != ActionBuildArtifacts && action.Kind != ActionPruneRetained) {
		return deny()
	}
	requests, err := openRemoteExecution(record, action)
	if err != nil || len(requests) != 1 || requests[0].Step != request.Step.Step {
		return deny()
	}
	payload, err := openRemoteStepPayload(requests[0])
	if err != nil {
		return deny()
	}
	observed, err := openRemoteWitnessObservation(payload)
	if err != nil {
		return deny()
	}
	plan, err := openSourceTopologyPlan(record.Intent, catalog, payload, observed)
	if err != nil || plan.operation != request.Step.Operation || sha256.Sum256(record.Intent) != record.IntentDigest {
		return deny()
	}
	observed.Catalog = catalog
	if !targetMatchesSourceState(payload.Target, observed.SourceState) {
		return deny()
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, found := catalog.ResolveReplicatedRoute(distribution.DistributionName(request.Source.Distribution), distribution.ShardID(request.Source.Shard), replicas[:0])
	if !found || route.Group != payload.Target.Group || route.Group != request.Source.Group ||
		route.AllocationGeneration != payload.Target.Allocation || route.AllocationGeneration != request.Source.AllocationGeneration ||
		request.Source.RelationManifestDigest != route.Command.RelationManifestDigest ||
		request.Native.Fence.Group != route.Group || request.Native.Fence.AllocationGeneration != route.AllocationGeneration ||
		(payload.Target.Member != 0 && payload.Target.Member != request.Source.MemberID) {
		return deny()
	}
	var sourceOK bool
	var destination gateway.ReplicatedEndpoint
	for _, endpoint := range route.Replicas {
		if endpoint.Node == peer.Identity.Node && endpoint.Member == request.Source.MemberID && endpoint.StoreID == request.Source.StoreID && endpoint.NodeIncarnation <= request.Source.NodeIncarnation {
			sourceOK = true
		}
		if endpoint.Node == request.Destination.Node && endpoint.Member == request.Destination.Member && endpoint.StoreID == request.Destination.Store && endpoint.NodeIncarnation <= request.Destination.Incarnation {
			destination = endpoint
		}
	}
	if !sourceOK || destination.Member == 0 {
		return deny()
	}
	var current gateway.NodeRecord
	for _, node := range directory.Nodes {
		if node.NodeID == peer.Identity.Node && node.Incarnation > current.Incarnation {
			current = node
		}
	}
	if current.Incarnation == 0 || current.Incarnation > request.Source.NodeIncarnation || current.ServiceKeyDigest != replication.Digest(peer.ServiceKeyDigest) ||
		current.Roles&gateway.NodeRoleStorage == 0 || (current.Lifecycle != gateway.NodeActive && current.Lifecycle != gateway.NodeDraining) {
		return deny()
	}
	if request.Native.Operation == shardservice.ReplicatedProbe {
		return route, destination, nil
	}
	if request.Native.Operation != shardservice.ReplicatedPropose || request.Native.Fence.Command != route.Command ||
		request.Native.Fence.MemberID != destination.Member || request.Native.Fence.StoreID != destination.StoreID ||
		request.Native.Fence.NodeIncarnation != request.Destination.Incarnation {
		return deny()
	}
	command, err := replication.OpenCommand(request.Native.Command)
	if err != nil || !validSourceTopologyCommand(plan, observed, payload, action.Kind, command) {
		return deny()
	}
	return route, destination, nil
}

func validSourceTopologyCommand(plan *Plan, observed Observation, payload remoteStepPayload, action ActionKind, command replication.CommandView) bool {
	capture := action != ActionPruneRetained
	client, tenant, domain := RetainedPruneClientID(plan.operation), RetainedPruneTenant(plan.operation), "vibedb/split-prune/retry-home\x00"
	if capture {
		client, tenant, domain = SourceCaptureClientID(plan.operation), SourceCaptureTenant(plan.operation), "vibedb/split-capture/retry-home\x00"
	}
	digest := sha256.Sum256(append([]byte(domain), client[:]...))
	var home replication.RetryHome
	copy(home[:], digest[:])
	if command.AuthorityClass != replication.CommandAuthorityTopology || command.ClientID != client ||
		!bytes.Equal(command.Tenant, tenant) || command.RetryHome != home {
		return false
	}
	switch command.Kind() {
	case replication.CommandSessionOpen:
		return action != ActionBuildArtifacts && command.ClientEpoch == 0 && command.ClientSequence == 1 && command.ExpectedDeadlineUnixNano == 0 && command.NextDeadlineUnixNano == math.MaxInt64
	case replication.CommandSessionRetire, replication.CommandSessionRelease:
		return action == ActionBuildArtifacts
	case replication.CommandSplitCaptureActivate:
		if action != ActionStartCapture {
			return false
		}
		body, err := plan.AppendSourceCaptureActivation(nil, observed.SourceState)
		if err != nil {
			return false
		}
		expected, err := splitcapture.OpenCommand(body)
		actual, actualErr := command.OpenSplitCaptureActivation()
		// Session Open and concurrent writes may advance the prior cut. The
		// immutable split geometry must match; Raft apply verifies that newer
		// prior cut's exact entry/data digests before activation.
		return err == nil && actualErr == nil && sameSourceCaptureAuthority(expected.Command, actual.Command) &&
			actual.PriorApplied >= expected.PriorApplied && actual.PriorTerm >= expected.PriorTerm
	case replication.CommandRetainedPrune:
		if action != ActionPruneRetained || observed.Certificate == nil || len(plan.indexRelations) != 0 {
			return false
		}
		certificate, err := gateway.OpenRetainedPruneCertificate(payload.RetainedPrune)
		if err != nil || !validSourceTopologyPruneCertificate(plan, payload, observed, certificate) {
			return false
		}
		proof, ok := command.RetainedPruneProof()
		cut, coords := observed.Certificate.SourceCut(), observed.Certificate.SourceCoordinates()
		if !ok || proof.OperationDigest != replication.Digest(plan.operation) || proof.CertificateDigest != replication.Digest(observed.Certificate.Digest()) ||
			proof.DataChainDigest != replication.Digest(cut.DataChainDigest) || proof.EntryDigest != replication.Digest(cut.EntryDigest) || proof.BaseDigest != replication.Digest(cut.BaseDigest) ||
			proof.CutApplied != cut.Applied || proof.CutTerm != cut.Term || proof.OwnershipEpoch != coords.OwnershipEpoch || proof.RoutingVersion != coords.RoutingVersion ||
			proof.RouteGeneration != coords.RouteGeneration || proof.RetainedRange != plan.children[plan.retained].Range || proof.BatchDigest != command.Fingerprint {
			return false
		}
		batches := command.RelationBatches()
		for batches.Next() {
			batch := batches.Batch()
			if batch.Relation != 1 {
				return false
			}
			items := batch.Mutations()
			for items.Next() {
				if items.Mutation().Kind != replication.MutationDeleteDigestEqual {
					return false
				}
			}
		}
		return command.MutationCount() > 0
	default:
		return false
	}
}
