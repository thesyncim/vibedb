package splitcontroller

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	vibejson "github.com/thesyncim/vibejson"
	"go.etcd.io/raft/v3"
)

const (
	planObservationWireFormat             = 1
	planObservationFrameHeaderBytes       = 12
	MaxPlanObservationRequestBytes        = 256 << 10
	MaxPlanObservationResponseBytes       = 20 << 20
	MaxPlanObservationEndpoints           = 256
	AbsoluteMaxPlanObservationConcurrency = 256
)

var (
	planObservationRequestMagic  = [8]byte{'V', 'B', 'S', 'P', 'O', 'B', 'S', 0}
	planObservationResponseMagic = [8]byte{'V', 'B', 'S', 'P', 'C', 'U', 'T', 0}
)

func PlanObservationRequestDiscriminator() [8]byte { return planObservationRequestMagic }

type planObservationKind uint8

const (
	planObservationSource planObservationKind = iota + 1
	planObservationChild
)

type planObservationWireRequest struct {
	Format       uint8                  `json:"format"`
	Kind         planObservationKind    `json:"kind"`
	TargetMember uint64                 `json:"target_member"`
	Request      PlanObservationRequest `json:"request"`
}

type planObservationWireResponse struct {
	Format        uint8                             `json:"format"`
	Kind          planObservationKind               `json:"kind"`
	RequestDigest [32]byte                          `json:"request_digest"`
	State         []byte                            `json:"state,omitempty"`
	Serving       planObservationWireServingState   `json:"serving,omitempty"`
	CaptureHead   uint64                            `json:"capture_head,omitempty"`
	Artifacts     []byte                            `json:"artifacts,omitempty"`
	Tail          []byte                            `json:"tail,omitempty"`
	Certificate   []byte                            `json:"certificate,omitempty"`
	Prune         []byte                            `json:"prune,omitempty"`
	Stage         []byte                            `json:"stage,omitempty"`
	Runtime       *ChildObservation                 `json:"runtime,omitempty"`
	Ready         []planObservationWireServingState `json:"ready,omitempty"`
}

type planObservationWireServingState struct {
	Identity raftmember.RuntimeIdentity       `json:"identity"`
	Command  raftservice.CommandFence         `json:"command"`
	Status   planObservationWireRuntimeStatus `json:"status"`
}

type planObservationWireRuntimeStatus struct {
	MemberID          uint64 `json:"member_id"`
	LeaderID          uint64 `json:"leader_id"`
	Term              uint64 `json:"term"`
	Commit            uint64 `json:"commit"`
	Applied           uint64 `json:"applied"`
	CheckpointApplied uint64 `json:"checkpoint_applied"`
	LeadTransferee    uint64 `json:"lead_transferee"`
	RaftState         uint8  `json:"raft_state"`
}

type PlanObservationProvider interface {
	ObserveSplitSource(context.Context, PlanObservationRequest, uint64) (SourcePlanObservation, error)
	ObserveSplitChild(context.Context, PlanObservationRequest, uint64) (ChildPlanObservation, error)
}

type PlanObservationAuthorizeFunc func(
	rafttransport.PeerIdentity, PlanObservationRequest, uint64, bool,
) bool

type PlanObservationServiceOptions struct {
	Provider         PlanObservationProvider
	Authorize        PlanObservationAuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxResponseBytes int
}

type PlanObservationService struct {
	provider      PlanObservationProvider
	authorize     PlanObservationAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	maxResponse   int
	slots         chan struct{}
}

func NewPlanObservationService(options PlanObservationServiceOptions) (*PlanObservationService, error) {
	if options.Provider == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxPlanObservationConcurrency ||
		options.MaxResponseBytes <= 0 || options.MaxResponseBytes > MaxPlanObservationResponseBytes {
		return nil, ErrPlanObservation
	}
	return &PlanObservationService{
		provider: options.Provider, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		maxResponse: options.MaxResponseBytes, slots: make(chan struct{}, options.MaxConcurrent),
	}, nil
}

func (service *PlanObservationService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrPlanObservation
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := setPlanObservationReadDeadline(ctx, connection, service.readDeadline); err != nil {
		return err
	}
	request, err := readPlanObservationRequest(connection)
	if err != nil {
		return err
	}
	wantDomain := rafttransport.TrustDomain{
		ClusterID:          request.Request.Group.ClusterID,
		ClusterIncarnation: request.Request.Group.ClusterIncarnation,
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != wantDomain || !service.authorize(
		peer, request.Request, request.TargetMember, request.Kind == planObservationSource,
	) {
		return ErrPlanObservation
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrPlanObservation
	}
	var response planObservationWireResponse
	response.Format, response.Kind = planObservationWireFormat, request.Kind
	response.RequestDigest = request.Request.RequestDigest
	switch request.Kind {
	case planObservationSource:
		cut, observeErr := service.provider.ObserveSplitSource(
			ctx, request.Request, request.TargetMember,
		)
		if observeErr != nil {
			return observeErr
		}
		if cut.RequestDigest != request.Request.RequestDigest ||
			!validNetworkSourceObservation(request, cut) {
			return ErrPlanObservation
		}
		response.State, err = replicatedstate.AppendState(nil, cut.State)
		if err != nil {
			return errors.Join(ErrPlanObservation, err)
		}
		response.Serving, response.CaptureHead = appendWireServingState(cut.Serving), cut.CaptureHead
		if response.Artifacts, err = appendOptionalArtifacts(cut.Artifacts); err != nil {
			return err
		}
		if response.Tail, err = appendOptionalTail(cut.Tail); err != nil {
			return err
		}
		if response.Certificate, err = appendOptionalCertificate(cut.Certificate); err != nil {
			return err
		}
		if response.Prune, err = appendOptionalPrune(cut.Prune); err != nil {
			return err
		}
	case planObservationChild:
		cut, observeErr := service.provider.ObserveSplitChild(
			ctx, request.Request, request.TargetMember,
		)
		if observeErr != nil {
			return observeErr
		}
		if cut.RequestDigest != request.Request.RequestDigest ||
			!validNetworkChildObservation(request, cut) {
			return ErrPlanObservation
		}
		if response.Stage, err = appendOptionalStage(cut.Stage); err != nil {
			return err
		}
		response.Runtime = cloneChildPlanRuntime(cut.Runtime)
		if response.Runtime != nil {
			response.Ready = make([]planObservationWireServingState, len(response.Runtime.ReadyReplicas))
			for index := range response.Runtime.ReadyReplicas {
				response.Ready[index] = appendWireServingState(response.Runtime.ReadyReplicas[index])
			}
			response.Runtime.ReadyReplicas = nil
		}
	default:
		return ErrPlanObservation
	}
	if err = setPlanObservationWriteDeadline(ctx, connection, service.writeDeadline); err != nil {
		return err
	}
	return writePlanObservationResponse(connection, response, service.maxResponse)
}

type PlanObservationPeer struct {
	Node     rafttransport.NodeID
	MemberID uint64
}

type PlanObservationPeerDirectory interface {
	ResolvePlanObservationPeer(
		context.Context, PlanObservationRequest, distribution.EndpointID,
	) (PlanObservationPeer, error)
}

type PlanObservationCatalog interface {
	Read(context.Context) (*gateway.Snapshot, error)
}

// CatalogPlanObservationPeerDirectory resolves already-published RF3 members
// from the same authenticated catalog image that bound the observation. It is
// allocation and command aware; an endpoint reused by another group cannot be
// selected accidentally. Prepared, not-yet-published children use a separate
// plan-intent directory implementing PlanObservationPeerDirectory.
type CatalogPlanObservationPeerDirectory struct{ catalog PlanObservationCatalog }

func NewCatalogPlanObservationPeerDirectory(
	catalog PlanObservationCatalog,
) (*CatalogPlanObservationPeerDirectory, error) {
	if catalog == nil {
		return nil, ErrPlanObservation
	}
	return &CatalogPlanObservationPeerDirectory{catalog: catalog}, nil
}

func (directory *CatalogPlanObservationPeerDirectory) ResolvePlanObservationPeer(
	ctx context.Context, request PlanObservationRequest, endpoint distribution.EndpointID,
) (PlanObservationPeer, error) {
	if directory == nil || directory.catalog == nil || ctx == nil || endpoint == "" ||
		!validNetworkPlanObservationRequest(request) {
		return PlanObservationPeer{}, ErrPlanObservation
	}
	snapshot, err := directory.catalog.Read(ctx)
	if err != nil || snapshot == nil || snapshot.Generation() != request.CatalogGeneration {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil || digest != request.CatalogDigest {
		return PlanObservationPeer{}, errors.Join(ErrPlanObservation, err)
	}
	route, found := snapshot.ResolveReplicatedRoute(
		request.Distribution, request.Shard,
		make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	)
	if !found || route.Group != request.Group ||
		route.AllocationGeneration != uint64(request.Allocation) || route.Command != request.Command {
		return PlanObservationPeer{}, ErrPlanObservation
	}
	for _, replica := range route.Replicas {
		if distribution.EndpointID(replica.ControlEndpoint) == endpoint ||
			distribution.EndpointID(replica.Endpoint) == endpoint {
			if replica.Node == (rafttransport.NodeID{}) || replica.Member == 0 {
				return PlanObservationPeer{}, ErrPlanObservation
			}
			return PlanObservationPeer{Node: replica.Node, MemberID: replica.Member}, nil
		}
	}
	return PlanObservationPeer{}, ErrPlanObservation
}

type PlanObservationStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type NetworkPlanObservationClientOptions struct {
	Opener           PlanObservationStreamOpener
	Directory        PlanObservationPeerDirectory
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxResponseBytes int
}

// NetworkPlanObservationClient is the gateway-side multi-group client. Every
// stream is authenticated against the exact catalog node and Raft trust domain;
// child member probes run in a bounded parallel wave and merge only identical
// durable lifecycle evidence.
type NetworkPlanObservationClient struct {
	opener        PlanObservationStreamOpener
	directory     PlanObservationPeerDirectory
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	concurrency   int
	maxResponse   int
}

type planObservationMemberResult struct {
	cut ChildPlanObservation
	err error
}

func NewNetworkPlanObservationClient(
	options NetworkPlanObservationClientOptions,
) (*NetworkPlanObservationClient, error) {
	if options.Opener == nil || options.Directory == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxPlanObservationConcurrency ||
		options.MaxResponseBytes <= 0 || options.MaxResponseBytes > MaxPlanObservationResponseBytes {
		return nil, ErrPlanObservation
	}
	return &NetworkPlanObservationClient{
		opener: options.Opener, directory: options.Directory,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		concurrency: options.MaxConcurrent, maxResponse: options.MaxResponseBytes,
	}, nil
}

func (client *NetworkPlanObservationClient) ObserveSplitSource(
	ctx context.Context, request PlanObservationRequest,
) (SourcePlanObservation, error) {
	if client == nil || !validNetworkPlanObservationRequest(request) {
		return SourcePlanObservation{}, ErrPlanObservation
	}
	var first *SourcePlanObservation
	var failures error
	for _, endpoint := range request.ControlEndpoints {
		peer, err := client.directory.ResolvePlanObservationPeer(ctx, request, endpoint)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		response, err := client.exchange(ctx, peer, planObservationWireRequest{
			Format: planObservationWireFormat, Kind: planObservationSource,
			TargetMember: peer.MemberID, Request: request,
		})
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		cut, err := openSourcePlanObservation(response)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		if first == nil {
			copy := cut
			first = &copy
		}
		if cut.Serving.Status.MemberID == cut.Serving.Status.LeaderID &&
			cut.Serving.Status.RaftState == raft.StateLeader {
			return cut, nil
		}
	}
	if first != nil {
		return *first, nil
	}
	return SourcePlanObservation{}, errors.Join(ErrPlanObservation, failures)
}

func (client *NetworkPlanObservationClient) ObserveSplitChild(
	ctx context.Context, request PlanObservationRequest,
) (ChildPlanObservation, error) {
	if client == nil || !validNetworkPlanObservationRequest(request) {
		return ChildPlanObservation{}, ErrPlanObservation
	}
	results := make([]planObservationMemberResult, len(request.ControlEndpoints))
	semaphore := make(chan struct{}, client.concurrency)
	var group sync.WaitGroup
	for index, endpoint := range request.ControlEndpoints {
		index, endpoint := index, endpoint
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
			peer, err := client.directory.ResolvePlanObservationPeer(ctx, request, endpoint)
			if err != nil {
				results[index].err = err
				return
			}
			response, err := client.exchange(ctx, peer, planObservationWireRequest{
				Format: planObservationWireFormat, Kind: planObservationChild,
				TargetMember: peer.MemberID, Request: request,
			})
			if err != nil {
				results[index].err = err
				return
			}
			results[index].cut, results[index].err = openChildPlanObservation(response)
		}()
	}
	group.Wait()
	return mergeChildPlanObservations(request, results)
}

func (client *NetworkPlanObservationClient) exchange(
	ctx context.Context, peer PlanObservationPeer, request planObservationWireRequest,
) (planObservationWireResponse, error) {
	if ctx == nil || peer.Node == (rafttransport.NodeID{}) || peer.MemberID == 0 {
		return planObservationWireResponse{}, ErrPlanObservation
	}
	connection, err := client.opener.OpenShardControl(ctx, peer.Node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return planObservationWireResponse{}, err
	}
	if connection == nil {
		return planObservationWireResponse{}, ErrPlanObservation
	}
	defer connection.Close()
	wantDomain := rafttransport.TrustDomain{
		ClusterID:          request.Request.Group.ClusterID,
		ClusterIncarnation: request.Request.Group.ClusterIncarnation,
	}
	identity := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		identity.Node != peer.Node || identity.TrustDomain != wantDomain {
		return planObservationWireResponse{}, ErrPlanObservation
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err = setPlanObservationWriteDeadline(ctx, connection, client.writeDeadline); err != nil {
		return planObservationWireResponse{}, err
	}
	if err = writePlanObservationRequest(connection, request); err != nil {
		return planObservationWireResponse{}, err
	}
	if err = setPlanObservationReadDeadline(ctx, connection, client.readDeadline); err != nil {
		return planObservationWireResponse{}, err
	}
	response, err := readPlanObservationResponse(connection, client.maxResponse)
	if err != nil || response.Kind != request.Kind ||
		response.RequestDigest != request.Request.RequestDigest {
		return planObservationWireResponse{}, errors.Join(ErrPlanObservation, err)
	}
	return response, nil
}

func validNetworkPlanObservationRequest(request PlanObservationRequest) bool {
	return request.Operation != (OperationID{}) && request.CatalogGeneration != 0 &&
		request.CatalogDigest != ([32]byte{}) && request.RequestDigest != ([32]byte{}) &&
		request.RequestDigest == planObservationRequestDigest(request) &&
		request.Distribution != "" && request.Shard != "" && request.Allocation != 0 &&
		request.Group.ClusterID != ([16]byte{}) && request.Group.ClusterIncarnation != ([16]byte{}) &&
		request.Group.ShardIncarnation != ([16]byte{}) && request.Group.GroupID != ([16]byte{}) &&
		request.Group.TopologyRecoveryEpoch != 0 && request.Command.Valid() &&
		len(request.ControlEndpoints) > 0 && len(request.ControlEndpoints) <= MaxPlanObservationEndpoints
}

func validNetworkSourceObservation(
	request planObservationWireRequest, cut SourcePlanObservation,
) bool {
	state, serving := cut.State, cut.Serving
	binding := state.Binding
	identity, status := serving.Identity, serving.Status
	return validNetworkPlanObservationRequest(request.Request) &&
		request.TargetMember != 0 && state.Applied != 0 &&
		binding.ClusterID == request.Request.Group.ClusterID &&
		binding.ClusterIncarnation == request.Request.Group.ClusterIncarnation &&
		binding.ShardIncarnation == request.Request.Group.ShardIncarnation &&
		binding.GroupID == request.Request.Group.GroupID &&
		binding.TopologyRecoveryEpoch == request.Request.Group.TopologyRecoveryEpoch &&
		binding.Distribution == string(request.Request.Distribution) &&
		binding.Shard == string(request.Request.Shard) &&
		binding.AllocationGeneration == uint64(request.Request.Allocation) &&
		state.ReplicaSetVersion == request.Request.Command.ReplicaSetVersion &&
		binding.ActivePolicyGeneration == request.Request.Command.ActivePolicyGeneration &&
		binding.ProtectionEpoch == request.Request.Command.ProtectionEpoch &&
		binding.OwnershipEpoch == request.Request.Command.OwnershipEpoch &&
		binding.SchemaGeneration == request.Request.Command.SchemaGeneration &&
		binding.RoutingVersion == request.Request.Command.RoutingVersion &&
		binding.RouteGeneration == request.Request.Command.RouteGeneration &&
		identity.Group == request.Request.Group &&
		identity.AllocationGeneration == uint64(request.Request.Allocation) &&
		identity.MemberID == request.TargetMember && status.MemberID == request.TargetMember &&
		identity.NodeIncarnation != 0 && identity.StoreID != ([16]byte{}) &&
		serving.Command == request.Request.Command && status.Applied == state.Applied &&
		status.Applied <= status.Commit && status.Term != 0
}

func validNetworkChildObservation(
	request planObservationWireRequest, cut ChildPlanObservation,
) bool {
	if !validNetworkPlanObservationRequest(request.Request) || request.TargetMember == 0 {
		return false
	}
	if cut.Stage != nil && cut.Stage.Child() != request.Request.Child {
		return false
	}
	if cut.Runtime == nil {
		return true
	}
	runtime := cut.Runtime
	if runtime.Child != request.Request.Child || len(runtime.ReadyReplicas) > 1 {
		return false
	}
	for _, serving := range runtime.ReadyReplicas {
		if serving.Identity.Group != request.Request.Group ||
			serving.Identity.AllocationGeneration != uint64(request.Request.Allocation) ||
			serving.Identity.MemberID != request.TargetMember ||
			serving.Status.MemberID != request.TargetMember ||
			serving.Command != request.Request.Command || serving.Identity.NodeIncarnation == 0 {
			return false
		}
	}
	return true
}

func mergeChildPlanObservations(
	request PlanObservationRequest,
	results []planObservationMemberResult,
) (ChildPlanObservation, error) {
	var merged ChildPlanObservation
	var stageRaw []byte
	var failures error
	successes := 0
	for index := range results {
		if results[index].err != nil {
			failures = errors.Join(failures, results[index].err)
			continue
		}
		successes++
		cut := results[index].cut
		if cut.RequestDigest != request.RequestDigest {
			return ChildPlanObservation{}, ErrPlanObservation
		}
		candidateStage, err := appendOptionalStage(cut.Stage)
		if err != nil {
			return ChildPlanObservation{}, err
		}
		if len(candidateStage) != 0 {
			if len(stageRaw) != 0 && !bytes.Equal(stageRaw, candidateStage) {
				return ChildPlanObservation{}, ErrPlanObservation
			}
			stageRaw = candidateStage
			merged.Stage = cloneObservationPointer(cut.Stage)
		}
		if cut.Runtime == nil {
			continue
		}
		base := *cut.Runtime
		ready := base.ReadyReplicas
		base.ReadyReplicas = nil
		if merged.Runtime != nil && !sameChildObservationBase(*merged.Runtime, base) {
			return ChildPlanObservation{}, ErrPlanObservation
		}
		if merged.Runtime == nil {
			copy := base
			merged.Runtime = &copy
		}
		for _, serving := range ready {
			duplicate := false
			for _, prior := range merged.Runtime.ReadyReplicas {
				duplicate = duplicate || prior.Identity.MemberID == serving.Identity.MemberID
			}
			if duplicate {
				return ChildPlanObservation{}, ErrPlanObservation
			}
			merged.Runtime.ReadyReplicas = append(merged.Runtime.ReadyReplicas, serving)
		}
	}
	if successes == 0 {
		return ChildPlanObservation{}, errors.Join(ErrPlanObservation, failures)
	}
	merged.RequestDigest = request.RequestDigest
	return merged, nil
}

func sameChildObservationBase(left, right ChildObservation) bool {
	return left.Child == right.Child && left.Phase == right.Phase &&
		left.ApplyIdentity == right.ApplyIdentity && left.ApplyProfile == right.ApplyProfile &&
		left.WALBinding == right.WALBinding && left.RuntimeIdentity == right.RuntimeIdentity
}

func cloneChildPlanRuntime(runtime *ChildObservation) *ChildObservation {
	if runtime == nil {
		return nil
	}
	copy := *runtime
	copy.ReadyReplicas = append([]raftservice.ServingState(nil), runtime.ReadyReplicas...)
	return &copy
}

func appendOptionalArtifacts(value *rangesplit.ChildArtifactSet) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return rangesplit.AppendChildArtifactSet(nil, *value)
}
func appendOptionalTail(value *rangesplit.TailCursor) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return rangesplit.AppendTailCursor(nil, *value)
}
func appendOptionalCertificate(value *rangesplit.CutoverCertificate) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return rangesplit.AppendCutoverCertificate(nil, value)
}
func appendOptionalPrune(value *rangesplit.RetainedPruneCursor) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return rangesplit.AppendRetainedPruneCursor(nil, value)
}
func appendOptionalStage(value *rangesplit.ChildStageCursor) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return rangesplit.AppendChildStageCursor(nil, value)
}

func openSourcePlanObservation(response planObservationWireResponse) (SourcePlanObservation, error) {
	state, err := replicatedstate.OpenState(response.State)
	if err != nil {
		return SourcePlanObservation{}, err
	}
	serving, err := openWireServingState(response.Serving)
	if err != nil {
		return SourcePlanObservation{}, err
	}
	result := SourcePlanObservation{
		RequestDigest: response.RequestDigest, State: state,
		Status: serving.Status, Serving: serving,
		CaptureHead: response.CaptureHead,
	}
	if len(response.Artifacts) != 0 {
		value, openErr := rangesplit.OpenChildArtifactSet(response.Artifacts)
		if openErr != nil {
			return SourcePlanObservation{}, openErr
		}
		result.Artifacts = &value
	}
	if len(response.Tail) != 0 {
		value, openErr := rangesplit.OpenTailCursor(response.Tail)
		if openErr != nil {
			return SourcePlanObservation{}, openErr
		}
		result.Tail = &value
	}
	if len(response.Certificate) != 0 {
		result.Certificate, err = rangesplit.OpenCutoverCertificate(response.Certificate)
		if err != nil {
			return SourcePlanObservation{}, err
		}
	}
	if len(response.Prune) != 0 {
		result.Prune, err = rangesplit.OpenRetainedPruneCursor(response.Prune)
		if err != nil {
			return SourcePlanObservation{}, err
		}
	}
	return result, nil
}

func openChildPlanObservation(response planObservationWireResponse) (ChildPlanObservation, error) {
	result := ChildPlanObservation{RequestDigest: response.RequestDigest,
		Runtime: cloneChildPlanRuntime(response.Runtime)}
	if result.Runtime == nil && len(response.Ready) != 0 {
		return ChildPlanObservation{}, ErrPlanObservation
	}
	if result.Runtime != nil {
		result.Runtime.ReadyReplicas = make([]raftservice.ServingState, len(response.Ready))
		for index := range response.Ready {
			serving, err := openWireServingState(response.Ready[index])
			if err != nil {
				return ChildPlanObservation{}, err
			}
			result.Runtime.ReadyReplicas[index] = serving
		}
	}
	if len(response.Stage) != 0 {
		stage, err := rangesplit.OpenChildStageCursor(response.Stage)
		if err != nil {
			return ChildPlanObservation{}, err
		}
		result.Stage = stage
	}
	return result, nil
}

func appendWireServingState(serving raftservice.ServingState) planObservationWireServingState {
	status := serving.Status
	return planObservationWireServingState{
		Identity: serving.Identity, Command: serving.Command,
		Status: planObservationWireRuntimeStatus{
			MemberID: status.MemberID, LeaderID: status.LeaderID, Term: status.Term,
			Commit: status.Commit, Applied: status.Applied,
			CheckpointApplied: status.CheckpointApplied,
			LeadTransferee:    status.LeadTransferee, RaftState: uint8(status.RaftState),
		},
	}
}

func openWireServingState(wire planObservationWireServingState) (raftservice.ServingState, error) {
	if wire.Status.RaftState > uint8(raft.StatePreCandidate) {
		return raftservice.ServingState{}, ErrPlanObservation
	}
	return raftservice.ServingState{
		Identity: wire.Identity, Command: wire.Command,
		Status: raftmember.RuntimeStatus{
			MemberID: wire.Status.MemberID, LeaderID: wire.Status.LeaderID,
			Term: wire.Status.Term, Commit: wire.Status.Commit, Applied: wire.Status.Applied,
			CheckpointApplied: wire.Status.CheckpointApplied,
			LeadTransferee:    wire.Status.LeadTransferee,
			RaftState:         raft.StateType(wire.Status.RaftState),
		},
	}, nil
}

func writePlanObservationRequest(writer io.Writer, request planObservationWireRequest) error {
	if !validWirePlanObservationRequest(request) {
		return ErrPlanObservation
	}
	return writePlanObservationFrame(writer, planObservationRequestMagic, &request, MaxPlanObservationRequestBytes)
}

func readPlanObservationRequest(reader io.Reader) (planObservationWireRequest, error) {
	var request planObservationWireRequest
	if err := readPlanObservationFrame(reader, planObservationRequestMagic,
		MaxPlanObservationRequestBytes, &request); err != nil || !validWirePlanObservationRequest(request) {
		return planObservationWireRequest{}, errors.Join(ErrPlanObservation, err)
	}
	return request, nil
}

func writePlanObservationResponse(writer io.Writer, response planObservationWireResponse, maximum int) error {
	if !validWirePlanObservationResponse(response) {
		return ErrPlanObservation
	}
	return writePlanObservationFrame(writer, planObservationResponseMagic, &response, maximum)
}

func readPlanObservationResponse(reader io.Reader, maximum int) (planObservationWireResponse, error) {
	var response planObservationWireResponse
	if err := readPlanObservationFrame(reader, planObservationResponseMagic, maximum, &response); err != nil ||
		!validWirePlanObservationResponse(response) {
		return planObservationWireResponse{}, errors.Join(ErrPlanObservation, err)
	}
	return response, nil
}

func validWirePlanObservationRequest(request planObservationWireRequest) bool {
	return request.Format == planObservationWireFormat &&
		(request.Kind == planObservationSource || request.Kind == planObservationChild) &&
		request.TargetMember != 0 && validNetworkPlanObservationRequest(request.Request)
}

func validWirePlanObservationResponse(response planObservationWireResponse) bool {
	if response.Format != planObservationWireFormat ||
		(response.Kind != planObservationSource && response.Kind != planObservationChild) ||
		response.RequestDigest == ([32]byte{}) {
		return false
	}
	if response.Kind == planObservationSource {
		return len(response.State) != 0 && len(response.Stage) == 0 && response.Runtime == nil
	}
	return len(response.State) == 0 && response.Serving == (planObservationWireServingState{}) &&
		response.CaptureHead == 0 && len(response.Artifacts) == 0 && len(response.Tail) == 0 &&
		len(response.Certificate) == 0 && len(response.Prune) == 0
}

func writePlanObservationFrame[T any](writer io.Writer, magic [8]byte, value *T, maximum int) error {
	payload, err := appendCanonicalVibeJSON(nil, value)
	if err != nil || len(payload) == 0 || len(payload) > maximum || len(payload) > math.MaxUint32 {
		return errors.Join(ErrPlanObservation, err)
	}
	var header [planObservationFrameHeaderBytes]byte
	copy(header[:8], magic[:])
	binary.BigEndian.PutUint32(header[8:], uint32(len(payload)))
	if err = writePlanObservationFull(writer, header[:]); err != nil {
		return err
	}
	return writePlanObservationFull(writer, payload)
}

func readPlanObservationFrame[T any](reader io.Reader, magic [8]byte, maximum int, value *T) error {
	if reader == nil || maximum <= 0 || maximum > MaxPlanObservationResponseBytes {
		return ErrPlanObservation
	}
	var header [planObservationFrameHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(header[8:]))
	if !bytes.Equal(header[:8], magic[:]) || length <= 0 || length > maximum {
		return ErrPlanObservation
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return err
	}
	if err := vibejson.Unmarshal(payload, value); err != nil {
		return err
	}
	canonical, err := appendCanonicalVibeJSON(nil, value)
	if err != nil || !bytes.Equal(canonical, payload) {
		return errors.Join(ErrPlanObservation, err)
	}
	return nil
}

func appendCanonicalVibeJSON[T any](dst []byte, value *T) ([]byte, error) {
	raw, err := vibejson.Marshal(value)
	if err != nil {
		return dst, err
	}
	return vibejson.AppendCanonicalize(dst, raw)
}

func writePlanObservationFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

func setPlanObservationReadDeadline(ctx context.Context, connection rafttransport.PeerConnection, deadline rafttransport.DeadlineFunc) error {
	value := boundedPlanObservationDeadline(ctx, deadline())
	if value.IsZero() {
		return ErrPlanObservation
	}
	return connection.SetReadDeadline(value)
}
func setPlanObservationWriteDeadline(ctx context.Context, connection rafttransport.PeerConnection, deadline rafttransport.DeadlineFunc) error {
	value := boundedPlanObservationDeadline(ctx, deadline())
	if value.IsZero() {
		return ErrPlanObservation
	}
	return connection.SetWriteDeadline(value)
}
func boundedPlanObservationDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}
