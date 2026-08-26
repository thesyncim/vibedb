package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
)

var errGatewayReplicaControl = errors.New("vibedb-gateway: invalid replica control configuration")

type gatewayReplicaRemoteClientOptions struct {
	Opener        *gatewayShardControlOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	Authority     *gateway.ReplicatedCatalogAuthority
	Replicated    *gateway.ReplicatedExecutor
	Routes        rebalanceexec.MoveRouteResolver
	Observer      rebalance.ReplicatedMoveObserver
	Drainer       rebalanceexec.CatalogDrainCertifier
}

// newGatewayReplicaRemoteClients composes every fixed shard-control client in
// one place. Observer, route-history, and cluster-drain authority are supplied
// explicitly because their durable implementations span catalog and gateway
// control services; this constructor never substitutes local process state.
func newGatewayReplicaRemoteClients(
	options gatewayReplicaRemoteClientOptions,
) (gatewayReplicaMoveControls, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.Authority == nil || options.Replicated == nil || options.Routes == nil ||
		options.Observer == nil || options.Drainer == nil {
		return gatewayReplicaMoveControls{}, errGatewayReplicaControl
	}
	observations, err := replicacontrol.NewClient(replicacontrol.ClientOptions{
		Opener: options.Opener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	actions, err := replicaaction.NewClient(replicaaction.ClientOptions{
		Opener: options.Opener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	source, err := snapshottransfer.NewSourceControlClient(snapshottransfer.SourceControlClientOptions{
		Opener: options.Opener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	bootstrap, err := snapshottransfer.NewBootstrapControlClient(snapshottransfer.BootstrapControlClientOptions{
		Opener: options.Opener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	grantInstaller, err := shardservice.NewMembershipGrantControlClient(
		options.Opener, options.ReadDeadline, options.WriteDeadline,
	)
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	remote := &gatewayReplicaRemoteActions{observer: observations, actions: actions,
		routes: options.Routes}
	return gatewayReplicaMoveControls{
		Observer: options.Observer, Routes: options.Routes,
		Membership: gatewayGrantedMembershipClient{grants: options.Authority,
			installer: grantInstaller, applier: options.Replicated},
		Snapshots: source, Bootstrap: bootstrap, Awaiter: remote, Ownership: remote,
		Drainer: options.Drainer, Retirement: remote,
	}, nil
}

// gatewayShardControlOpener is the one authenticated stream boundary shared by
// the fixed replica-control grammars. The endpoint directory is detached at
// construction and the semaphore is held until the authenticated stream is
// closed, so stalled peers cannot evade the configured connection bound.
type gatewayShardControlOpener struct {
	tls       *rafttransport.PeerTLS
	deadline  rafttransport.DeadlineFunc
	dial      func(context.Context, string) (net.Conn, error)
	addresses map[rafttransport.NodeID]string
	slots     chan struct{}
}

func newGatewayShardControlOpener(
	tls *rafttransport.PeerTLS,
	deadline rafttransport.DeadlineFunc,
	dial func(context.Context, string) (net.Conn, error),
	endpoints []gateway.ReplicatedEndpoint,
	maxConnections int,
) (*gatewayShardControlOpener, error) {
	if tls == nil || deadline == nil || dial == nil || maxConnections <= 0 ||
		maxConnections > 4096 || len(endpoints) == 0 {
		return nil, errGatewayReplicaControl
	}
	addresses := make(map[rafttransport.NodeID]string, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Node == (rafttransport.NodeID{}) || endpoint.ControlAddress == "" {
			return nil, errGatewayReplicaControl
		}
		if prior, found := addresses[endpoint.Node]; found && prior != endpoint.ControlAddress {
			return nil, errGatewayReplicaControl
		}
		addresses[endpoint.Node] = endpoint.ControlAddress
	}
	return &gatewayShardControlOpener{tls: tls, deadline: deadline, dial: dial,
		addresses: addresses, slots: make(chan struct{}, maxConnections)}, nil
}

func (opener *gatewayShardControlOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	if opener == nil || ctx == nil || node == (rafttransport.NodeID{}) {
		return nil, errGatewayReplicaControl
	}
	address, found := opener.addresses[node]
	if !found {
		return nil, errGatewayReplicaControl
	}
	select {
	case opener.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	raw, err := opener.dial(ctx, address)
	if err != nil || raw == nil {
		<-opener.slots
		if raw != nil {
			_ = raw.Close()
		}
		return nil, errors.Join(err, errGatewayReplicaControl)
	}
	connection, err := opener.tls.Client(
		ctx, raw, node, rafttransport.TrafficShardControl, opener.deadline,
	)
	if err != nil {
		<-opener.slots
		return nil, err
	}
	return &gatewayBoundedPeerConnection{PeerConnection: connection, release: func() {
		<-opener.slots
	}}, nil
}

type gatewayBoundedPeerConnection struct {
	rafttransport.PeerConnection
	once    sync.Once
	release func()
}

func (connection *gatewayBoundedPeerConnection) Close() error {
	err := connection.PeerConnection.Close()
	connection.once.Do(connection.release)
	return err
}

type gatewayMembershipGrantInstaller interface {
	InstallMembershipGrant(context.Context, rafttransport.NodeID, membershipgrant.Grant) error
}

type gatewayMembershipApplier interface {
	ApplyMembership(context.Context, gateway.ReplicatedMembershipRoute,
		shardservice.ReplicatedMembershipRequest) (gateway.ReplicatedMembershipResult, error)
}

// gatewayGrantedMembershipClient installs the exact catalog-Raft grant on all
// RF3 voters and the enrolled target before any member can receive the
// ConfChange. Exact retries are idempotent; partial fanout never authorizes a
// proposal because ApplyMembership is reached only after complete fanout.
type gatewayGrantedMembershipClient struct {
	grants    membershipgrant.Source
	installer gatewayMembershipGrantInstaller
	applier   gatewayMembershipApplier
}

func (client gatewayGrantedMembershipClient) ApplyMembership(
	ctx context.Context,
	route gateway.ReplicatedMembershipRoute,
	request shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	if ctx == nil || client.grants == nil || client.installer == nil || client.applier == nil {
		return gateway.ReplicatedMembershipResult{}, errGatewayReplicaControl
	}
	grant, found, err := client.grants.ReadMembershipGrant(ctx, route.Serving.Group)
	if err != nil || !found || !grant.Valid() || grant.TransitionID != request.TransitionID ||
		grant.MetadataEpoch != request.MetadataEpoch ||
		grant.CatalogGeneration != request.CatalogGeneration ||
		grant.SourceMember != request.SourceMember || grant.TargetMember != request.TargetMember {
		return gateway.ReplicatedMembershipResult{}, errors.Join(err, errGatewayReplicaControl)
	}
	targetPresent := false
	for _, endpoint := range route.Serving.Replicas {
		if endpoint.Member == grant.TargetMember && [16]byte(endpoint.Node) == grant.TargetNode {
			targetPresent = true
		}
	}
	if route.HasEnrolledTarget {
		if route.EnrolledTarget.Member == grant.TargetMember &&
			[16]byte(route.EnrolledTarget.Node) == grant.TargetNode {
			targetPresent = true
		}
	}
	if !targetPresent {
		return gateway.ReplicatedMembershipResult{}, errGatewayReplicaControl
	}
	for _, endpoint := range route.Serving.Replicas {
		if err = client.installer.InstallMembershipGrant(ctx, endpoint.Node, grant); err != nil {
			return gateway.ReplicatedMembershipResult{}, err
		}
	}
	if route.HasEnrolledTarget {
		if err = client.installer.InstallMembershipGrant(ctx, route.EnrolledTarget.Node, grant); err != nil {
			return gateway.ReplicatedMembershipResult{}, err
		}
	}
	return client.applier.ApplyMembership(ctx, route, request)
}

type gatewayReplicaObservationClient interface {
	Observe(context.Context, rafttransport.NodeID, replicacontrol.Request) (replicacontrol.Observation, error)
}

type gatewayReplicaActionClient interface {
	Execute(context.Context, rafttransport.NodeID, replicaaction.Request) error
}

// gatewayReplicaRemoteActions carries the two mutating post-catch-up actions
// over replicaaction's durable request journal. It obtains the local member
// term immediately before constructing the ServingFence; catalog routing
// metadata alone is deliberately insufficient authority.
type gatewayReplicaRemoteActions struct {
	observer gatewayReplicaObservationClient
	actions  gatewayReplicaActionClient
	routes   rebalanceexec.MoveRouteResolver
}

func (remote gatewayReplicaRemoteActions) AwaitReplicaMove(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	if ctx == nil || plan == nil || remote.observer == nil || remote.routes == nil {
		return errGatewayReplicaControl
	}
	cut, err := remote.routes.ResolveReplicaMove(ctx, operation, plan, execution)
	if err != nil {
		return err
	}
	request := replicacontrol.Request{Operation: [32]byte(operation), Step: execution.Proof,
		Group: plan.Group(), TargetMember: plan.TargetMember(),
		ExpectedReplicaSetVersion: execution.PublicationReplicaSet}
	switch execution.Action.Kind {
	case rebalance.ActionAwaitLeader:
		for _, endpoint := range cut.Membership.Serving.Replicas {
			request.TargetMember = endpoint.Member
			observation, observeErr := remote.observer.Observe(ctx, endpoint.Node, request)
			if observeErr == nil && observation.Status.LeaderID != 0 &&
				observation.Status.Term != 0 {
				return nil
			}
			err = errors.Join(err, observeErr)
		}
		return errors.Join(err, errGatewayReplicaControl)
	case rebalance.ActionAwaitSnapshotInstall:
		observation, observeErr := remote.observer.Observe(ctx, cut.Target.Node, request)
		if observeErr != nil || execution.SnapshotBaseDigest == ([32]byte{}) ||
			observation.State.SnapshotBaseDigest != execution.SnapshotBaseDigest {
			return errors.Join(observeErr, errGatewayReplicaControl)
		}
		return nil
	case rebalance.ActionAwaitCatchUp:
		leader := cut.Membership.Serving.Replicas
		for _, endpoint := range leader {
			observation, observeErr := remote.observer.Observe(ctx, endpoint.Node, request)
			if observeErr == nil && observation.Status.MemberID == observation.Status.LeaderID &&
				observation.ProgressFound && observation.Progress.RecentActive &&
				observation.Progress.Match >= execution.PublicationApplied {
				return nil
			}
			err = errors.Join(err, observeErr)
		}
		return errors.Join(err, errGatewayReplicaControl)
	default:
		return errGatewayReplicaControl
	}
}

func (remote gatewayReplicaRemoteActions) ProposeReplicaMoveOwnership(
	ctx context.Context,
	operation rebalance.OperationID,
	step [32]byte,
	route gateway.ReplicatedMembershipRoute,
	command []byte,
) error {
	if ctx == nil || remote.observer == nil || remote.actions == nil || len(command) == 0 {
		return errGatewayReplicaControl
	}
	targetMember := ownershipTarget(command)
	target := route.EnrolledTarget
	if !route.HasEnrolledTarget || target.Member != targetMember {
		return errGatewayReplicaControl
	}
	observation, err := remote.observer.Observe(ctx, target.Node, replicacontrol.Request{
		Operation: [32]byte(operation), Step: step, Group: route.Serving.Group,
		TargetMember: target.Member, ExpectedReplicaSetVersion: route.Serving.Command.ReplicaSetVersion,
	})
	if err != nil || observation.Status.Term == 0 {
		return errors.Join(err, errGatewayReplicaControl)
	}
	return remote.actions.Execute(ctx, target.Node, replicaaction.Request{
		Operation: [32]byte(operation), Step: step, Kind: replicaaction.OwnershipTransition,
		Fence: raftservice.ServingFence{Group: route.Serving.Group,
			AllocationGeneration: route.Serving.AllocationGeneration, Command: route.Serving.Command,
			MemberID: target.Member, StoreID: target.StoreID,
			NodeIncarnation: target.NodeIncarnation, Term: observation.Status.Term},
		SourceMember: ownershipSource(command), TargetMember: target.Member,
		Command: command,
	})
}

func (remote gatewayReplicaRemoteActions) RetireReplicaSource(
	ctx context.Context, request rebalanceexec.SourceRetirementRequest,
) error {
	if ctx == nil || remote.actions == nil || request.Term == 0 ||
		request.Source.Member == 0 || request.Target.Member == 0 {
		return errGatewayReplicaControl
	}
	return remote.actions.Execute(ctx, request.Source.Node, replicaaction.Request{
		Operation: request.Operation, Step: request.Step, Kind: replicaaction.SourceRetirement,
		Fence: raftservice.ServingFence{Group: request.Group,
			AllocationGeneration: request.AllocationGeneration, Command: request.Command,
			MemberID: request.Source.Member,
			StoreID:  request.Source.StoreID, NodeIncarnation: request.Source.NodeIncarnation,
			Term: request.Term},
		SourceMember: request.Source.Member, TargetMember: request.Target.Member,
	})
}

// These tiny decoders use the canonical replicated-state grammar rather than
// interpreting command bytes as strings.
func ownershipSource(command []byte) uint64 {
	transition, err := openOwnershipTransition(command)
	if err != nil {
		return 0
	}
	return transition.source
}

func ownershipTarget(command []byte) uint64 {
	transition, err := openOwnershipTransition(command)
	if err != nil {
		return 0
	}
	return transition.target
}

type ownershipMembers struct{ source, target uint64 }

func openOwnershipTransition(command []byte) (ownershipMembers, error) {
	view, err := replicatedstate.OpenOwnershipTransition(command)
	if err != nil {
		return ownershipMembers{}, err
	}
	return ownershipMembers{source: view.SourceMember, target: view.TargetMember}, nil
}
