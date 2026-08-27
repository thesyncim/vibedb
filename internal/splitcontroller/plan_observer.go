package splitcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var ErrPlanObservation = errors.New("splitcontroller: invalid coherent plan observation")

// PlanObservationRequest binds one authenticated observation RPC to an exact
// immutable split and catalog image. Endpoints are the catalog-ordered control
// endpoints that the client must contact; a client must authenticate every
// peer, impose a deadline, and return only after its bounded read has settled.
type PlanObservationRequest struct {
	Operation         OperationID                            `json:"operation"`
	CatalogGeneration uint64                                 `json:"catalog_generation"`
	CatalogDigest     [sha256.Size]byte                      `json:"catalog_digest"`
	RequestDigest     [sha256.Size]byte                      `json:"request_digest"`
	Distribution      distribution.DistributionName          `json:"distribution"`
	Shard             distribution.ShardID                   `json:"shard"`
	Allocation        distribution.ShardAllocationGeneration `json:"allocation"`
	Child             uint8                                  `json:"child"`
	Group             raftmember.GroupKey                    `json:"group"`
	Command           raftservice.CommandFence               `json:"command"`
	SourceTransition  *PlanObservationSourceTransition       `json:"source_transition,omitempty"`
	ControlEndpoints  []distribution.EndpointID              `json:"control_endpoints"`
}

// PlanObservationSourceTransition names the two exact read-only source cuts
// of this plan. Sealing necessarily precedes catalog publication, so the
// catalog command alone cannot describe the current source during cutover.
// This is observation scope, never authority to submit a command.
type PlanObservationSourceTransition struct {
	From  raftservice.CommandFence `json:"from"`
	To    raftservice.CommandFence `json:"to"`
	Range distribution.KeyRange    `json:"range"`
}

// SourcePlanObservation is one detached cut read by the source owner lane.
// CaptureHead deliberately replaces the process-local SourceCapture pointer.
type SourcePlanObservation struct {
	RequestDigest [sha256.Size]byte
	State         replicatedstate.State
	Status        raftmember.RuntimeStatus
	// Serving is the production network proof from the exact serialized owner
	// lane. Status remains for local observers; authenticated services require
	// Serving and the coordinator derives Status from it.
	Serving     raftservice.ServingState
	CaptureHead uint64
	Artifacts   *rangesplit.ChildArtifactSet
	Tail        *rangesplit.TailCursor
	Certificate *rangesplit.CutoverCertificate
	Prune       *rangesplit.RetainedPruneCursor
}

// ChildPlanObservation is one destination's durable stage/runtime cut. A nil
// Stage or Runtime is authoritative absence, not an omitted best-effort read.
type ChildPlanObservation struct {
	RequestDigest [sha256.Size]byte
	Stage         *rangesplit.ChildStageCursor
	Runtime       *ChildObservation
}

// AuthenticatedPlanObservationClient is the transport seam. Implementations
// must use authenticated bounded streams and must not synthesize observations
// from cached gateway routing state.
type AuthenticatedPlanObservationClient interface {
	ObserveSplitSource(context.Context, PlanObservationRequest) (SourcePlanObservation, error)
	ObserveSplitChild(context.Context, PlanObservationRequest) (ChildPlanObservation, error)
}

// PlanCatalogDrainRequest binds a cluster-wide old-generation drain proof to
// the same exact catalog image used by the observation wave.
type PlanCatalogDrainRequest struct {
	Operation         OperationID
	CurrentGeneration uint64
	NextGeneration    uint64
	CatalogDigest     [sha256.Size]byte
	RequestDigest     [sha256.Size]byte
}

type PlanCatalogDrainProof struct {
	RequestDigest [sha256.Size]byte
	Certificate   gateway.ClusterCatalogDrainCertificate
}

// PlanCatalogDrainAuthority must collect every configured gateway's
// authenticated acknowledgement. A nonzero proof is a durable certificate;
// absence or an error leaves the controller safely waiting.
type PlanCatalogDrainAuthority interface {
	ObservePlanCatalogDrain(context.Context, PlanCatalogDrainRequest) (PlanCatalogDrainProof, error)
}

type CoherentPlanObserverOptions struct {
	Catalog       ControllerCatalog
	Observations  AuthenticatedPlanObservationClient
	CatalogDrain  PlanCatalogDrainAuthority
	MaxConcurrent int
	MaxAttempts   int
}

// CoherentPlanObserver reconstructs every decision from durable authorities.
// It retains no progress or lease state, so a replacement process can resume
// immediately. One attempt issues at most MaxSplitChildren authenticated reads
// and one drain proof, with caller-configured concurrency and retry bounds.
type CoherentPlanObserver struct {
	catalog      ControllerCatalog
	observations AuthenticatedPlanObservationClient
	drain        PlanCatalogDrainAuthority
	concurrency  int
	attempts     int
}

func NewCoherentPlanObserver(options CoherentPlanObserverOptions) (*CoherentPlanObserver, error) {
	if options.Catalog == nil || options.Observations == nil || options.CatalogDrain == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > autosplit.MaxSplitChildren ||
		options.MaxAttempts <= 0 {
		return nil, ErrPlanObservation
	}
	return &CoherentPlanObserver{
		catalog: options.Catalog, observations: options.Observations,
		drain: options.CatalogDrain, concurrency: options.MaxConcurrent,
		attempts: options.MaxAttempts,
	}, nil
}

func (observer *CoherentPlanObserver) ObservePlan(
	ctx context.Context, plan *Plan,
) (Observation, error) {
	if observer == nil || ctx == nil || plan == nil {
		return Observation{}, ErrPlanObservation
	}
	var last error
	for attempt := 0; attempt < observer.attempts; attempt++ {
		observed, retry, err := observer.observeAttempt(ctx, plan)
		if err == nil {
			return observed, nil
		}
		last = errors.Join(last, err)
		if !retry || ctx.Err() != nil {
			break
		}
	}
	return Observation{}, errors.Join(ErrPlanObservation, last, ctx.Err())
}

func (observer *CoherentPlanObserver) observeAttempt(
	ctx context.Context, plan *Plan,
) (Observation, bool, error) {
	catalog, err := observer.catalog.Read(ctx)
	if err != nil || catalog == nil {
		return Observation{}, false, err
	}
	if _, err = plan.catalogStage(catalog); err != nil {
		return Observation{}, false, fmt.Errorf("catalog stage generation=%d source=%d target=%d: %w", catalog.Generation(), plan.current, plan.next, err)
	}
	catalogDigest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil {
		return Observation{}, false, err
	}
	requests, sourceIndex, err := plan.observationRequests(catalog, catalogDigest)
	if err != nil {
		return Observation{}, false, err
	}

	type result struct {
		source SourcePlanObservation
		child  ChildPlanObservation
		err    error
	}
	results := make([]result, len(requests))
	semaphore := make(chan struct{}, observer.concurrency)
	var group sync.WaitGroup
	for index := range requests {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results[index].err = ctx.Err()
				return
			}
			if index == sourceIndex {
				results[index].source, results[index].err =
					observer.observations.ObserveSplitSource(ctx, requests[index])
				return
			}
			results[index].child, results[index].err =
				observer.observations.ObserveSplitChild(ctx, requests[index])
		}()
	}
	group.Wait()

	observed := Observation{Catalog: catalog}
	for index := range results {
		if results[index].err != nil {
			return Observation{}, false, fmt.Errorf("observation member group %d (source=%d): %w", index, sourceIndex, results[index].err)
		}
		request := requests[index]
		if index == sourceIndex {
			cut := results[index].source
			if cut.RequestDigest != request.RequestDigest {
				return Observation{}, false, ErrPlanObservation
			}
			observed.SourceState, observed.SourceStatus = cut.State, cut.Status
			if cut.Serving.Identity != (raftmember.RuntimeIdentity{}) {
				observed.SourceStatus = cut.Serving.Status
				observed.SourceServing = cut.Serving
			}
			observed.CaptureHead = cut.CaptureHead
			observed.Artifacts, observed.Tail = cloneObservationPointer(cut.Artifacts), cloneObservationPointer(cut.Tail)
			observed.Certificate, observed.Prune = cloneObservationPointer(cut.Certificate), cloneObservationPointer(cut.Prune)
			continue
		}
		cut := results[index].child
		if cut.RequestDigest != request.RequestDigest || request.Child >= autosplit.MaxSplitChildren {
			return Observation{}, false, ErrPlanObservation
		}
		if cut.Stage != nil {
			copy := *cut.Stage
			observed.Stages[request.Child] = &copy
		}
		if cut.Runtime != nil {
			observed.Children[request.Child] = cloneChildPlanRuntime(cut.Runtime)
		}
	}

	after, err := observer.catalog.Read(ctx)
	if err != nil || after == nil {
		return Observation{}, false, err
	}
	afterDigest, err := gateway.CatalogSnapshotDigest(after)
	if err != nil {
		return Observation{}, false, err
	}
	if after.Generation() != catalog.Generation() || afterDigest != catalogDigest {
		return Observation{}, true, ErrPlanObservation
	}
	if after.Generation() == plan.next {
		drainRequest := planCatalogDrainRequest(plan, catalogDigest)
		proof, proofErr := observer.drain.ObservePlanCatalogDrain(ctx, drainRequest)
		if proofErr != nil {
			return Observation{}, false, proofErr
		}
		if proof.RequestDigest != drainRequest.RequestDigest {
			return Observation{}, false, ErrPlanObservation
		}
		request := clusterPlanCatalogDrainRequest(drainRequest)
		if !proof.Certificate.ValidFor(request) {
			return Observation{}, false, ErrPlanObservation
		}
		observed.OlderCatalogDrained = true
		observed.CatalogDrainCertificate = proof.Certificate
		if observed.Certificate == nil {
			return Observation{}, false, ErrPlanObservation
		}
		pruneCertificate, certificateErr := deriveRetainedPruneCertificate(
			plan, catalog, *observed.Certificate, proof.Certificate,
		)
		if certificateErr != nil {
			return Observation{}, false, errors.Join(ErrPlanObservation, certificateErr)
		}
		observed.RetainedPruneCertificate = pruneCertificate
	}
	if _, err = Reconcile(plan, observed); err != nil {
		return Observation{}, false, fmt.Errorf("reconcile source applied=%d capture=%d artifacts=%t tail=%t certificate=%t: %w", observed.SourceState.Applied, observed.CaptureHead, observed.Artifacts != nil, observed.Tail != nil, observed.Certificate != nil, err)
	}
	return observed, false, nil
}

func cloneObservationPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (plan *Plan) observationRequests(
	catalog *gateway.Snapshot, catalogDigest [sha256.Size]byte,
) ([]PlanObservationRequest, int, error) {
	if plan == nil || catalog == nil {
		return nil, 0, ErrPlanObservation
	}
	requests := make([]PlanObservationRequest, 0, plan.childCount)
	sourceEndpoints, ok := manifestControlEndpoints(plan.sourceManifest, plan.source.Shard)
	if !ok {
		return nil, 0, ErrPlanObservation
	}
	requests = append(requests, newPlanObservationRequest(
		plan, catalog, catalogDigest, plan.source.Distribution, plan.source.Shard,
		plan.source.AllocationGeneration, plan.retained, sourceEndpoints,
	))
	if requests[0].Command.Valid() {
		from := requests[0].Command
		if plan.sourceAuthority != nil {
			from = plan.sourceAuthority.Command
		} else {
			from.OwnershipEpoch = uint64(plan.source.OwnershipEpoch)
			from.RoutingVersion = uint64(plan.source.RoutingVersion)
			from.RouteGeneration = plan.current
		}
		to := from
		to.OwnershipEpoch = uint64(plan.children[plan.retained].OwnershipEpoch)
		to.RoutingVersion = uint64(plan.targetManifest.Version())
		to.RouteGeneration = plan.next
		requests[0].SourceTransition = &PlanObservationSourceTransition{
			From: from, To: to, Range: plan.children[plan.retained].Range,
		}
		requests[0].RequestDigest = planObservationRequestDigest(requests[0])
	}
	for child := uint8(0); child < plan.childCount; child++ {
		identity := plan.children[child]
		if identity.Retained {
			continue
		}
		endpoints, found := manifestControlEndpoints(plan.targetManifest, identity.Shard)
		if !found || len(endpoints) != int(plan.leaderCounts[child]) {
			return nil, 0, ErrPlanObservation
		}
		requests = append(requests, newPlanObservationRequest(
			plan, catalog, catalogDigest, plan.source.Distribution, identity.Shard,
			identity.AllocationGeneration, child, endpoints,
		))
	}
	return requests, 0, nil
}

func manifestControlEndpoints(
	manifest *distribution.Manifest, shard distribution.ShardID,
) ([]distribution.EndpointID, bool) {
	if manifest == nil {
		return nil, false
	}
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		metadata, ok := manifest.ShardMetadataAt(ordinal)
		if !ok || metadata.ID != shard || metadata.LeaderCount <= 0 {
			continue
		}
		endpoints := make([]distribution.EndpointID, metadata.LeaderCount)
		for leader := range endpoints {
			endpoint, endpointOK := manifest.ShardLeaderAt(ordinal, leader)
			if !endpointOK || endpoint == "" {
				return nil, false
			}
			endpoints[leader] = endpoint
		}
		return endpoints, true
	}
	return nil, false
}

func newPlanObservationRequest(
	plan *Plan, catalog *gateway.Snapshot, catalogDigest [sha256.Size]byte,
	distributionName distribution.DistributionName, shard distribution.ShardID,
	allocation distribution.ShardAllocationGeneration, child uint8,
	endpoints []distribution.EndpointID,
) PlanObservationRequest {
	request := PlanObservationRequest{
		Operation: plan.operation, CatalogGeneration: catalog.Generation(),
		CatalogDigest: catalogDigest, Distribution: distributionName,
		Shard: shard, Allocation: allocation, Child: child,
		ControlEndpoints: append([]distribution.EndpointID(nil), endpoints...),
	}
	if route, found := catalog.ResolveReplicatedRoute(
		distributionName, shard, make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	); found && route.AllocationGeneration == uint64(allocation) {
		request.Group, request.Command = route.Group, route.Command
	} else if child != plan.retained {
		target := plan.targets[child]
		request.Group = raftmember.GroupKey{
			ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
			TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
			ShardIncarnation:      target.WAL.ShardIncarnation, GroupID: target.WAL.GroupID,
		}
		request.Command = raftservice.CommandFence{
			ReplicaSetVersion:      target.ReplicaSetVersion,
			ActivePolicyGeneration: target.Authority.ActivePolicyGeneration,
			ProtectionEpoch:        target.Authority.ProtectionEpoch,
			OwnershipEpoch:         target.Authority.OwnershipEpoch,
			SchemaGeneration:       target.Authority.SchemaGeneration,
			RelationManifestDigest: target.RelationManifestDigest,
			RoutingVersion:         target.Authority.RoutingVersion,
			RouteGeneration:        target.Authority.RouteGeneration,
		}
	}
	request.RequestDigest = planObservationRequestDigest(request)
	return request
}

func planObservationRequestDigest(request PlanObservationRequest) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/splitcontroller/plan-observation\x00"))
	_, _ = hash.Write(request.Operation[:])
	_, _ = hash.Write(request.CatalogDigest[:])
	_, _ = hash.Write(request.Group.ClusterID[:])
	_, _ = hash.Write(request.Group.ClusterIncarnation[:])
	_, _ = hash.Write(request.Group.ShardIncarnation[:])
	_, _ = hash.Write(request.Group.GroupID[:])
	_, _ = hash.Write(request.Command.RelationManifestDigest[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], request.CatalogGeneration)
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(request.Allocation))
	_, _ = hash.Write(scalar[:])
	for _, value := range [...]uint64{
		request.Group.TopologyRecoveryEpoch, request.Command.ReplicaSetVersion,
		request.Command.ActivePolicyGeneration, request.Command.ProtectionEpoch,
		request.Command.OwnershipEpoch, request.Command.SchemaGeneration,
		request.Command.RoutingVersion, request.Command.RouteGeneration,
	} {
		binary.LittleEndian.PutUint64(scalar[:], value)
		_, _ = hash.Write(scalar[:])
	}
	_, _ = hash.Write([]byte{request.Child})
	writeObservationString(hash, string(request.Distribution), &scalar)
	writeObservationString(hash, string(request.Shard), &scalar)
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(request.ControlEndpoints)))
	_, _ = hash.Write(scalar[:])
	for _, endpoint := range request.ControlEndpoints {
		writeObservationString(hash, string(endpoint), &scalar)
	}
	if request.SourceTransition != nil {
		// Canonical, fixed-size transition coordinates are covered by the
		// request digest; omitting the optional field preserves old requests.
		raw, err := appendCanonicalVibeJSON(nil, request.SourceTransition)
		if err != nil {
			return [sha256.Size]byte{}
		}
		_, _ = hash.Write([]byte("\x00source-transition\x00"))
		_, _ = hash.Write(raw)
	}
	var result [sha256.Size]byte
	hash.Sum(result[:0])
	return result
}

type observationHashWriter interface{ Write([]byte) (int, error) }

func writeObservationString(hash observationHashWriter, value string, scalar *[8]byte) {
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(value)))
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write([]byte(value))
}

func planCatalogDrainRequest(
	plan *Plan, catalogDigest [sha256.Size]byte,
) PlanCatalogDrainRequest {
	request := PlanCatalogDrainRequest{
		Operation: plan.operation, CurrentGeneration: plan.current,
		NextGeneration: plan.next, CatalogDigest: catalogDigest,
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/splitcontroller/catalog-drain\x00"))
	_, _ = hash.Write(request.Operation[:])
	_, _ = hash.Write(request.CatalogDigest[:])
	var raw [16]byte
	binary.LittleEndian.PutUint64(raw[:8], request.CurrentGeneration)
	binary.LittleEndian.PutUint64(raw[8:], request.NextGeneration)
	_, _ = hash.Write(raw[:])
	hash.Sum(request.RequestDigest[:0])
	return request
}

func clusterPlanCatalogDrainRequest(request PlanCatalogDrainRequest) gateway.ClusterCatalogDrainRequest {
	return gateway.ClusterCatalogDrainRequest{
		Operation: [sha256.Size]byte(request.Operation), Step: request.RequestDigest,
		Generation: request.NextGeneration, CatalogDigest: request.CatalogDigest,
	}
}
