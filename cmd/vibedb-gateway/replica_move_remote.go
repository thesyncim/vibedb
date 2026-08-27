package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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

type gatewayReplicaCatalogReader interface {
	Read(context.Context) (*gateway.Snapshot, error)
}

type gatewayReplicaMoveAuthority interface {
	gatewayReplicaCatalogReader
	membershipgrant.Source
}

type gatewayReplicaMoveObserver struct {
	authority gatewayReplicaMoveAuthority
	remote    gatewayReplicaObservationClient
	drainer   rebalanceexec.CatalogDrainCertifier
}

type gatewayCatalogDigestVerifier struct{ catalog gatewayReplicaCatalogReader }

func (verifier gatewayCatalogDigestVerifier) VerifyClusterCatalogDigest(
	ctx context.Context, generation uint64, digest [32]byte,
) error {
	if ctx == nil || verifier.catalog == nil || generation == 0 || digest == ([32]byte{}) {
		return gateway.ErrClusterCatalogDrainUnknown
	}
	snapshot, err := verifier.catalog.Read(ctx)
	if err != nil || snapshot == nil || snapshot.Generation() != generation {
		return errors.Join(err, gateway.ErrClusterCatalogDrainUnknown)
	}
	actual, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil || actual != digest {
		return errors.Join(err, gateway.ErrClusterCatalogDrainUnknown)
	}
	return nil
}

func (observer gatewayReplicaMoveObserver) ObserveReplicaMove(
	ctx context.Context,
	operation rebalance.OperationID,
	record gateway.ReplicatedOperationRecord,
	initial *rebalance.Plan,
) (rebalance.ReplicatedMoveCut, error) {
	if ctx == nil || operation == (rebalance.OperationID{}) || observer.authority == nil ||
		observer.remote == nil || observer.drainer == nil {
		return rebalance.ReplicatedMoveCut{}, errGatewayReplicaControl
	}
	var request rebalance.MoveRequest
	var sourceGeneration uint64
	if initial != nil {
		if initial.OperationID() != operation {
			return rebalance.ReplicatedMoveCut{}, errGatewayReplicaControl
		}
		request, sourceGeneration = initial.Request(), initial.CatalogGeneration()
	} else {
		identity, err := rebalance.InspectReplicaMoveIntent(record.Intent)
		if err != nil || identity.Operation != operation {
			return rebalance.ReplicatedMoveCut{}, errors.Join(err, errGatewayReplicaControl)
		}
		request, sourceGeneration = identity.Request, identity.SourceGeneration
	}
	catalog, err := observer.authority.Read(ctx)
	if err != nil || catalog == nil || catalog.Generation() < sourceGeneration ||
		catalog.Generation() > sourceGeneration+2 {
		return rebalance.ReplicatedMoveCut{}, errors.Join(err, errGatewayReplicaControl)
	}
	route, err := resolveGatewayReplicaMoveRoute(catalog, request)
	if err != nil {
		return rebalance.ReplicatedMoveCut{}, err
	}
	step := record.Proof
	if step == ([32]byte{}) {
		step = gatewayReplicaObservationStep(operation, catalog.Generation())
	}
	observeRequest := replicacontrol.Request{Operation: [32]byte(operation), Step: step,
		Group: request.Group, TargetMember: request.TargetMember}
	// Membership commits before catalog publication. Requiring the old catalog
	// version here prevents observing the very ConfChange this controller just
	// proposed. Discover an authenticated current cut, then reject regressions;
	// the reconciler still validates the exact roster and journaled transition.
	minimumReplicaSet := max(route.Command.ReplicaSetVersion, record.Cursor[4])
	var leader replicacontrol.Observation
	var leaderFound bool
	var observeErrors error
	for _, endpoint := range route.Membership.Serving.Replicas {
		candidate, observeErr := observer.remote.Observe(ctx, endpoint.Node, observeRequest)
		if observeErr == nil && candidate.Publication.ReplicaSetVersion >= minimumReplicaSet &&
			candidate.Status.MemberID == endpoint.Member && candidate.Status.MemberID == candidate.Status.LeaderID &&
			candidate.Status.Term != 0 {
			leader, leaderFound = candidate, true
			break
		}
		observeErrors = errors.Join(observeErrors, observeErr)
	}
	if !leaderFound {
		return rebalance.ReplicatedMoveCut{}, errors.Join(observeErrors, errGatewayReplicaControl)
	}
	target, targetErr := observer.remote.Observe(ctx, route.Target.Node, observeRequest)
	if targetErr != nil {
		target = replicacontrol.Observation{}
	}
	cut := rebalance.ReplicatedMoveCut{Observation: rebalance.Observation{
		Catalog: catalog, Publication: leader.Publication, LeaderStatus: leader.Status,
		TargetStatus: target.Status, TargetState: target.State,
		TargetProgress: leader.Progress, ProgressFound: leader.ProgressFound,
	}}
	if target.SnapshotBase != nil {
		cut.SnapshotBase = target.SnapshotBase
	} else {
		cut.SnapshotBase = leader.SnapshotBase
	}
	if catalog.Generation() > sourceGeneration && record.Proof != ([32]byte{}) {
		digest, digestErr := gateway.CatalogSnapshotDigest(catalog)
		if digestErr != nil {
			return rebalance.ReplicatedMoveCut{}, digestErr
		}
		drainRequest := gateway.ClusterCatalogDrainRequest{Operation: [32]byte(operation),
			Step: record.Proof, Generation: catalog.Generation(), CatalogDigest: digest}
		certificate, drainErr := observer.drainer.CertifyClusterCatalogDrain(ctx, drainRequest)
		if drainErr == nil && certificate.ValidFor(drainRequest) {
			cut.DrainedCatalogGeneration = catalog.Generation()
		}
	}
	if catalog.Generation() == sourceGeneration+2 {
		_, grantFound, grantErr := observer.authority.ReadMembershipGrant(ctx, request.Group)
		if grantErr != nil {
			return rebalance.ReplicatedMoveCut{}, grantErr
		}
		cut.RetiringReplicaRetired = !grantFound
	}
	return cut, nil
}

func gatewayReplicaObservationStep(operation rebalance.OperationID, generation uint64) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/gateway/replica-move-observation\x00"))
	hash.Write(operation[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], generation)
	hash.Write(scalar[:])
	var step [32]byte
	hash.Sum(step[:0])
	return step
}

// gatewayReplicaMoveRouteResolver rebuilds every action route from the current
// authoritative catalog plus the exact retiring identity persisted in the
// move intent. It therefore remains restart-safe after G+1 removes the source
// from the serving RF3 directory.
type gatewayReplicaMoveRouteResolver struct{ catalog gatewayReplicaCatalogReader }

func (resolver gatewayReplicaMoveRouteResolver) ResolveReplicaMove(
	ctx context.Context,
	operation rebalance.OperationID,
	plan *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) (rebalanceexec.MoveRoute, error) {
	if ctx == nil || resolver.catalog == nil || plan == nil || operation == (rebalance.OperationID{}) ||
		operation != plan.OperationID() || execution.Proof == ([32]byte{}) {
		return rebalanceexec.MoveRoute{}, errGatewayReplicaControl
	}
	catalog, err := resolver.catalog.Read(ctx)
	if err != nil || catalog == nil {
		return rebalanceexec.MoveRoute{}, errors.Join(err, errGatewayReplicaControl)
	}
	request := plan.Request()
	return resolveGatewayReplicaMoveRoute(catalog, request)
}

func resolveGatewayReplicaMoveRoute(
	catalog *gateway.Snapshot, request rebalance.MoveRequest,
) (rebalanceexec.MoveRoute, error) {
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	membership, found := catalog.ResolveReplicatedMembershipRoute(
		request.Distribution, request.Shard, workspace[:0],
	)
	if !found || membership.Serving.Group != request.Group {
		return rebalanceexec.MoveRoute{}, errGatewayReplicaControl
	}
	cut := rebalanceexec.MoveRoute{Catalog: catalog, Membership: membership,
		Command: membership.Serving.Command}
	for _, endpoint := range membership.Serving.Replicas {
		switch endpoint.Member {
		case request.RetiringMember:
			cut.Retiring = endpoint
		case request.SnapshotSourceMember:
			cut.SnapshotSource = endpoint
		case request.TargetMember:
			cut.Target = endpoint
		}
	}
	if membership.HasEnrolledTarget && membership.EnrolledTarget.Member == request.TargetMember {
		cut.Target = membership.EnrolledTarget
	}
	if cut.Retiring.Member == 0 {
		identity := request.RetiringReplica
		address, addressErr := catalog.Address(identity.ControlEndpoint)
		if addressErr != nil {
			return rebalanceexec.MoveRoute{}, errors.Join(addressErr, errGatewayReplicaControl)
		}
		cut.Retiring = gateway.ReplicatedEndpoint{Member: identity.Member, Node: identity.Node,
			StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation,
			ControlEndpoint: string(identity.ControlEndpoint), ControlAddress: address}
	}
	if cut.Target.Member != request.TargetMember || cut.SnapshotSource.Member != request.SnapshotSourceMember ||
		cut.Retiring.Member != request.RetiringMember {
		return rebalanceexec.MoveRoute{}, errGatewayReplicaControl
	}
	return cut, nil
}

// newGatewayReplicaRemoteClients composes every fixed shard-control client in
// one place. Observer, route-history, and cluster-drain authority are supplied
// explicitly because their durable implementations span catalog and gateway
// control services; this constructor never substitutes local process state.
func newGatewayReplicaRemoteClients(
	options gatewayReplicaRemoteClientOptions,
) (gatewayReplicaMoveControls, error) {
	if options.Routes == nil && options.Authority != nil {
		options.Routes = gatewayReplicaMoveRouteResolver{catalog: options.Authority}
	}
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.Authority == nil || options.Replicated == nil || options.Routes == nil ||
		options.Drainer == nil {
		return gatewayReplicaMoveControls{}, errGatewayReplicaControl
	}
	observations, err := replicacontrol.NewClient(replicacontrol.ClientOptions{
		Opener: options.Opener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return gatewayReplicaMoveControls{}, err
	}
	if options.Observer == nil {
		options.Observer = gatewayReplicaMoveObserver{authority: options.Authority,
			remote: observations, drainer: options.Drainer}
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
		Observer: options.Observer, HealthObservations: observations,
		GrantInstaller: grantInstaller, Routes: options.Routes,
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

type gatewayControlEndpoint struct {
	Member  gateway.ClusterCatalogDrainMember
	Address string
}

type gatewayClusterControlOpener struct {
	tls       *rafttransport.PeerTLS
	deadline  rafttransport.DeadlineFunc
	dial      func(context.Context, string) (net.Conn, error)
	addresses map[rafttransport.NodeID]string
	slots     chan struct{}
}

func newGatewayClusterControlOpener(
	tls *rafttransport.PeerTLS,
	deadline rafttransport.DeadlineFunc,
	dial func(context.Context, string) (net.Conn, error),
	endpoints []gatewayControlEndpoint,
	maxConnections int,
) (*gatewayClusterControlOpener, error) {
	if tls == nil || deadline == nil || dial == nil || len(endpoints) == 0 ||
		maxConnections <= 0 || maxConnections > gateway.AbsoluteMaxCatalogDrainConcurrency {
		return nil, errGatewayReplicaControl
	}
	addresses := make(map[rafttransport.NodeID]string, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Member.Node == (rafttransport.NodeID{}) || endpoint.Member.Incarnation == 0 ||
			endpoint.Address == "" {
			return nil, errGatewayReplicaControl
		}
		if prior, found := addresses[endpoint.Member.Node]; found && prior != endpoint.Address {
			return nil, errGatewayReplicaControl
		}
		addresses[endpoint.Member.Node] = endpoint.Address
	}
	return &gatewayClusterControlOpener{tls: tls.WithLocalGatewayControlConnections(), deadline: deadline, dial: dial,
		addresses: addresses, slots: make(chan struct{}, maxConnections)}, nil
}

func (opener *gatewayClusterControlOpener) OpenGatewayControl(
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
		ctx, raw, node, rafttransport.TrafficGatewayControl, opener.deadline,
	)
	if err != nil {
		<-opener.slots
		return nil, err
	}
	return &gatewayBoundedPeerConnection{PeerConnection: connection, release: func() {
		<-opener.slots
	}}, nil
}

func newGatewayClusterDrainCertifier(
	trust rafttransport.TrustDomain,
	tls *rafttransport.PeerTLS,
	handshake, readDeadline, writeDeadline rafttransport.DeadlineFunc,
	dial func(context.Context, string) (net.Conn, error),
	endpoints []gatewayControlEndpoint,
	maxConcurrent int,
) (*gateway.ClusterCatalogDrainCoordinator, error) {
	if len(endpoints) == 0 {
		return nil, errGatewayReplicaControl
	}
	opener, err := newGatewayClusterControlOpener(
		tls, handshake, dial, endpoints, maxConcurrent,
	)
	if err != nil {
		return nil, err
	}
	client, err := gateway.NewClusterCatalogDrainClient(gateway.ClusterCatalogDrainClientOptions{
		Opener: opener, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
		MaxConcurrent: maxConcurrent,
	})
	if err != nil {
		return nil, err
	}
	members := make([]gateway.ClusterCatalogDrainMember, len(endpoints))
	for index := range endpoints {
		members[index] = endpoints[index].Member
	}
	return gateway.NewClusterCatalogDrainCoordinator(trust, members, client)
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

// gatewayGrantedMembershipClient installs the exact catalog-Raft grant on a
// quorum of the current RF3 voters and the target before proposing ConfChange.
// It still attempts every endpoint. Requiring all three would make replacement
// impossible precisely when one voter has failed; requiring fewer than quorum
// could admit a command which cannot commit.
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
	if err = installGatewayMembershipGrant(ctx, route, grant, client.installer); err != nil {
		return gateway.ReplicatedMembershipResult{}, err
	}
	return client.applier.ApplyMembership(ctx, route, request)
}

// installGatewayMembershipGrant makes a pre-change grant available to the
// reachable voter quorum and the exact enrolled target. It always attempts
// every current voter so a certified failed-replica replacement can tolerate
// the absent source without silently reducing the quorum requirement.
func installGatewayMembershipGrant(
	ctx context.Context,
	route gateway.ReplicatedMembershipRoute,
	grant membershipgrant.Grant,
	installer gatewayMembershipGrantInstaller,
) error {
	if ctx == nil || installer == nil || route.Serving.Group != grant.Group ||
		len(route.Serving.Replicas) != gateway.ServingReplicaCount ||
		!route.HasEnrolledTarget || route.EnrolledTarget.Member != grant.TargetMember ||
		[16]byte(route.EnrolledTarget.Node) != grant.TargetNode {
		return errGatewayReplicaControl
	}
	installedVoters := 0
	targetInstalled := false
	var installErrors error
	for _, endpoint := range route.Serving.Replicas {
		installErr := installer.InstallMembershipGrant(ctx, endpoint.Node, grant)
		if installErr == nil {
			installedVoters++
		} else {
			installErrors = errors.Join(installErrors, installErr)
		}
	}
	installErr := installer.InstallMembershipGrant(ctx, route.EnrolledTarget.Node, grant)
	if installErr == nil {
		targetInstalled = true
	} else {
		installErrors = errors.Join(installErrors, installErr)
	}
	if installedVoters < gateway.ServingReplicaCount/2+1 || !targetInstalled {
		return errors.Join(installErrors, errGatewayReplicaControl)
	}
	return nil
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
	request := replicacontrol.Request{Operation: [32]byte(operation), Step: step,
		Group: route.Serving.Group, TargetMember: target.Member,
		ExpectedReplicaSetVersion: route.Serving.Command.ReplicaSetVersion}
	candidates := append([]gateway.ReplicatedEndpoint(nil), route.Serving.Replicas...)
	foundTarget := false
	for _, candidate := range candidates {
		foundTarget = foundTarget || candidate.Member == target.Member
	}
	if !foundTarget {
		candidates = append(candidates, target)
	}
	var leader gateway.ReplicatedEndpoint
	var observation replicacontrol.Observation
	var observeErrors error
	for _, candidate := range candidates {
		cut, observeErr := remote.observer.Observe(ctx, candidate.Node, request)
		if observeErr == nil && cut.Status.MemberID == candidate.Member &&
			cut.Status.LeaderID == candidate.Member && cut.Status.Term != 0 {
			leader, observation = candidate, cut
			break
		}
		observeErrors = errors.Join(observeErrors, observeErr)
	}
	if leader.Member == 0 {
		return errors.Join(observeErrors, errGatewayReplicaControl)
	}
	return remote.actions.Execute(ctx, leader.Node, replicaaction.Request{
		Operation: [32]byte(operation), Step: step, Kind: replicaaction.OwnershipTransition,
		Fence: raftservice.ServingFence{Group: route.Serving.Group,
			AllocationGeneration: route.Serving.AllocationGeneration, Command: route.Serving.Command,
			MemberID: leader.Member, StoreID: leader.StoreID,
			NodeIncarnation: leader.NodeIncarnation, Term: observation.Status.Term},
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
