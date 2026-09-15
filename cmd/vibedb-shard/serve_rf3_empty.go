package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterbackupservice"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// rf3EmptyNodeRuntime is the live process boundary retained by an empty node.
// It starts all authenticated listeners and the zero-group execution owner,
// but publishes no execution group until a later certified learner install
// calls RegisterExecutionGroup. The control reader slot is intentionally
// empty at construction; a committed-directory bootstrap adapter must attach
// it after authenticating a seed and the node's exact Joining record.
type rf3EmptyNodeRuntime struct {
	peer          *raftservice.AuthenticatedExecutionPeerRuntime
	registry      *rafttransport.StaticRegistry
	lanes         *multiraft.ExecutionLanes
	serving       *raftserve.Registry
	reader        *nodecontrol.IntentReaderSlot
	receivers     *rf3DynamicBootstrapRegistry
	learner       *rf3DynamicLearnerFactory
	grants        *rf3DynamicGrantRouter
	schemas       *rf3SchemaActivator
	donors        *rf3DynamicDonorServices
	native        *rf3NativeAuthorities
	actionJournal *replicaaction.FileJournal
	controlMu     sync.Mutex
	// servingGroups is separate from transport membership. A group becomes
	// native-serving only after the certified snapshot installer calls
	// RegisterExecutionGroup; an empty process therefore remains fail-closed.
	servingGroups *atomic.Int64
}

// RegisterExecutionGroup is the only path that can make a transferred learner
// visible to ordinary transport and native execution. It is intentionally
// useful to the snapshot installer while retaining one shared physical-node
// peer runtime.
func (runtime *rf3EmptyNodeRuntime) RegisterExecutionGroup(
	roster []rafttransport.Member, group raftservice.ExecutionGroup,
) error {
	return runtime.RegisterExecutionGroupWithGrant(roster, group, membershipgrant.Grant{})
}

func (runtime *rf3EmptyNodeRuntime) RegisterExecutionGroupWithGrant(
	roster []rafttransport.Member, group raftservice.ExecutionGroup, grant membershipgrant.Grant,
) error {
	if runtime == nil || runtime.peer == nil {
		return raftservice.ErrInvalidOwner
	}
	servingBefore := int64(0)
	if runtime.servingGroups != nil {
		servingBefore = runtime.servingGroups.Load()
	}
	var err error
	if grant != (membershipgrant.Grant{}) {
		err = runtime.peer.RegisterExecutionGroupWithGrant(roster, group, grant)
	} else {
		err = runtime.peer.RegisterExecutionGroup(roster, group)
	}
	if err != nil {
		return fmt.Errorf("rf3 empty-node group activation failed group=%x member=%d node_incarnation=%d roster_members=%d serving_groups=%d: %w",
			group.Identity.Group.GroupID, group.Identity.MemberID, group.Identity.NodeIncarnation,
			len(roster), servingBefore, err)
	}
	if runtime.servingGroups != nil {
		runtime.servingGroups.Add(1)
	}
	return nil
}

// UnregisterExecutionGroup withdraws a quiescent group from the shared peer
// and native listener. It is the inverse of RegisterExecutionGroup and keeps
// a different adopted group serving while one group is retired.
func (runtime *rf3EmptyNodeRuntime) UnregisterExecutionGroup(identity raftmember.RuntimeIdentity) error {
	if runtime == nil || runtime.peer == nil {
		return raftservice.ErrInvalidOwner
	}
	if err := runtime.peer.UnregisterExecutionGroup(identity); err != nil {
		return err
	}
	var cleanup error
	cleanup = errors.Join(cleanup, runtime.native.unregisterDynamic(identity))
	if runtime.donors != nil {
		cleanup = errors.Join(cleanup, runtime.donors.Unregister(identity))
	}
	if runtime.schemas != nil {
		cleanup = errors.Join(cleanup, runtime.schemas.UnregisterDynamic(identity))
	}
	if runtime.servingGroups != nil {
		for {
			count := runtime.servingGroups.Load()
			if count <= 0 || runtime.servingGroups.CompareAndSwap(count, count-1) {
				break
			}
		}
	}
	return cleanup
}

func (runtime *rf3EmptyNodeRuntime) nativeServing() bool {
	return runtime != nil && runtime.servingGroups != nil && runtime.servingGroups.Load() > 0
}

func (runtime *rf3EmptyNodeRuntime) IntentReaderSlot() *nodecontrol.IntentReaderSlot {
	if runtime == nil {
		return nil
	}
	return runtime.reader
}

func (runtime *rf3EmptyNodeRuntime) BootstrapReceivers() *rf3DynamicBootstrapRegistry {
	if runtime == nil {
		return nil
	}
	return runtime.receivers
}

// BindIntentReader attaches the authenticated committed-directory client. It
// is deliberately a one-time capability handoff; until it is attached the
// node-control service fails closed before any journal or storage side effect.
func (runtime *rf3EmptyNodeRuntime) BindIntentReader(reader nodecontrol.IntentReader) error {
	if runtime == nil || runtime.reader == nil {
		return nodecontrol.ErrControl
	}
	return runtime.reader.Set(reader)
}

// RegisterBootstrapService creates the shipped receiver/installer composition
// for one certified post-AddLearner descriptor. A reservation alone never
// creates this service, so an activated empty target cannot receive or install
// arbitrary snapshot bytes.
func (runtime *rf3EmptyNodeRuntime) RegisterBootstrapService(
	ctx context.Context, intent gateway.GroupEnrollmentIntent,
	proof gateway.PreparedReplicaProof, descriptor snapshottransfer.Descriptor,
) error {
	if runtime == nil || runtime.learner == nil {
		return nodecontrol.ErrControl
	}
	return runtime.learner.Register(ctx, intent, proof, descriptor)
}

func (runtime *rf3EmptyNodeRuntime) CloseBootstrapServices() error {
	if runtime == nil || runtime.learner == nil {
		return nil
	}
	return runtime.learner.Close()
}

func rf3TransportRegistryLimits() rafttransport.Limits {
	return rafttransport.Limits{
		MaxGroups: maxRF3ManifestGroups,
		// Each retained RF3 group also needs its incoming or outgoing member
		// mapping while a certified placement transition is in progress.
		MaxMembers: maxRF3ManifestGroups * (rf3ManifestMembers + 1),
		MaxPeers:   rafttransport.AbsoluteMaxTransportPeers,
	}
}

// servePreparedRF3EmptyNode starts the explicit zero-group physical-node
// grammar. The node owns no group SQL/WAL yet, so startup must not call any
// group bootstrap constructor or synthesize a ConfState. A later enrollment
// uses nodecontrol's committed-intent check, then snapshottransfer's certified
// descriptor/install path, and finally RegisterExecutionGroup.
func servePreparedRF3EmptyNode(
	parent context.Context,
	manifest rf3Manifest,
	executionLaneCount int,
	listen rf3ListenFunc,
	embeddedGateway *rf3ManifestGateway,
	diagnostics <-chan os.Signal,
	profile *rafttransport.PeerTLS,
	policy *serviceauthz.Policy,
	gate *serviceauthz.Gate,
	controlTLS *servicetls.Server,
	_ *shardservice.ReplicatedServerTLS,
	nodeOwner *rf3NodeOwner,
	adoptedInventory *rf3AdoptedGroupInventory,
	migrationBudget *migrationbudget.Budget,
	actionJournal *replicaaction.FileJournal,
	prepared *preparedRF3Set,
) (resultErr error) {
	if parent == nil || listen == nil || profile == nil || policy == nil || gate == nil ||
		controlTLS == nil || nodeOwner == nil || adoptedInventory == nil || actionJournal == nil || prepared == nil || manifest.NodeLog == nil ||
		manifest.NodeIncarnation == 0 || len(manifest.groupBundles()) != 0 ||
		embeddedGateway != nil || !validRF3ExecutionLanes(executionLaneCount) {
		return errRF3Serving
	}
	defer func() { resultErr = closePreparedRF3Groups(prepared.groups, resultErr) }()
	if cause := context.Cause(parent); cause != nil {
		return componentShutdownError(cause)
	}
	local := profile.LocalIdentity()
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	template, err := rf3NodePreparationTemplateFromManifest(manifest)
	if err != nil {
		return fmt.Errorf("%w: empty-node preparation template: %v", errRF3Serving, err)
	}
	transportRegistry, err := rafttransport.NewEmptyRegistry(local.Node, local.TrustDomain, rf3TransportRegistryLimits())
	if err != nil {
		return fmt.Errorf("%w: empty-node transport registry: %v", errRF3Serving, err)
	}
	servingRegistry, err := raftserve.NewRegistry(rf3RegistryLimitsForGroups(maxRF3ManifestGroups))
	if err != nil {
		return err
	}
	lanes, err := servingRegistry.NewExecutionLanes(executionLaneCount, rf3HostLimitsForGroups(maxRF3ManifestGroups))
	if err != nil {
		return errors.Join(err, servingRegistry.Close())
	}
	peerListener, err := listen("tcp", manifest.Listeners.Peer)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	closeListener := func(listener net.Listener) error {
		if listener == nil {
			return nil
		}
		return listener.Close()
	}
	defer func() { resultErr = errors.Join(resultErr, closeListener(peerListener)) }()
	controlListener, err := listen("tcp", manifest.Listeners.Control)
	if err != nil {
		return errors.Join(err, closeListener(peerListener), lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, closeListener(controlListener)) }()
	snapshotListener, err := listen("tcp", manifest.Listeners.Snapshot)
	if err != nil {
		return errors.Join(err, closeListener(peerListener), closeListener(controlListener), lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, closeListener(snapshotListener)) }()
	nativeListener, err := listen("tcp", manifest.Listeners.Native)
	if err != nil {
		return errors.Join(err, closeListener(peerListener), closeListener(controlListener), closeListener(snapshotListener), lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, closeListener(nativeListener)) }()

	// Resolve every reconnect from the currently committed physical endpoint.
	// The ordinary transport never retains a caller-supplied address and does
	// not create a queue until F's authenticated enrollment commit publishes it.
	dial := func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		physical, lookupErr := transportRegistry.PhysicalPeer(node)
		if lookupErr != nil || physical.Endpoint == "" {
			return nil, rafttransport.ErrPeerUnauthorized
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", physical.Endpoint)
	}
	pulse := make(chan struct{}, 1)
	peer, err := raftservice.NewAuthenticatedExecutionPeerRuntime(raftservice.AuthenticatedExecutionPeerOptions{
		Registry: transportRegistry, TLS: profile, Dial: dial, Listener: peerListener,
		HandshakeDeadline: deadline, MaxInboundStreams: 8,
		Execution: raftservice.ExecutionOptions{
			Registry: servingRegistry, Lanes: lanes, Members: nil, CommandFences: nil,
			ReadSources: nil, TransactionRecoverySources: nil, Pulse: pulse,
			Limits: rf3OwnerLimits(), ProgressMetrics: nil,
		},
		Transport: rf3TransportOptions(nil, deadline),
		Receiver:  rafttransport.OrdinaryReceiverOptions{ReadDeadline: deadline, RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes},
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	bindRF3TransportFailureDiagnostics(nodeOwner, peer, transportRegistry)
	if peer.Owners() == nil {
		return errors.Join(raftservice.ErrInvalidOwner, lanes.Close(), servingRegistry.Close())
	}
	var servingGroups atomic.Int64
	schemas, err := newRF3SchemaActivator(peer.Owners(), nil, nil)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	schemaServices, err := newRF3SchemaControlServices(schemas, transportRegistry, policy, manifest.ReplicaControl.SourceDataRoot)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, schemaServices.Close()) }()
	donors, err := newRF3DynamicDonorServices(schemas, transportRegistry, policy, manifest, migrationBudget, deadline)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, donors.Close()) }()
	var capacityRevision atomic.Uint64
	capacityDirectory, err := newRF3CapacitySourceDirectory(schemas, nil, nil,
		func(ctx context.Context, request replicacontrol.CapacityRequest, samples []replicacontrol.CapacitySourceSample) (replicacontrol.NodeCapacity, error) {
			return RF3CapacityNodeFromOwner(ctx, nodeOwner, manifest.NodeIncarnation, migrationBudget, &capacityRevision, request, samples)
		})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	capacityProvider, err := replicacontrol.NewCapacityProvider(capacityDirectory)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	capacityControl, err := newRF3CapacityControl(transportRegistry, policy, capacityProvider, deadline)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	// The local zero-group owner is fenced at the native serving boundary until
	// a certified runtime is atomically installed in the peer.
	nativeTLS, err := shardservice.NewReplicatedServerTLS(profile, policy.NodesWith(serviceauthz.CapabilityDelegate))
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	nativeServer, err := shardservice.NewReplicatedServer(peer.Owners(), shardservice.DefaultReplicatedInFlightFrameBytes, rf3RequestTimeout)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	nativeAuthorities, err := newRF3NativeAuthorities(transportRegistry, gate, nil, nil, nil)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	nativeAuthorities.adopted = adoptedInventory
	if err = nativeServer.BindAuthorization(gate, nil); err == nil {
		err = nativeServer.BindServingAuthority(nativeAuthorities.serving)
	}
	if err == nil {
		err = nativeServer.BindConcurrentServingAuthority(nativeAuthorities.serving)
	}
	if err == nil {
		err = nativeServer.BindTransitionalServingAuthority(nativeAuthorities.transitional)
	}
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}

	reader := new(nodecontrol.IntentReaderSlot)
	bootstrapTransport, err := bindRF3NodeBootstrapIntentReader(
		reader, profile, manifest.GatewaySeeds, local.Node, manifest.NodeIncarnation, deadline,
	)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, bootstrapTransport.Close()) }()
	journal, err := nodecontrol.NewFileJournal(filepath.Join(manifest.ReplicaControl.SourceDataRoot, "node-control-journal"))
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, journal.Close()) }()
	receivers, err := newRF3DynamicBootstrapRegistry(local.TrustDomain, deadline, 32)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	preparer := &rf3NodeControlPreparer{NodeRoot: manifest.ReplicaControl.SourceDataRoot, Template: template}
	adopter := &rf3NodeControlAdopter{NodeRoot: manifest.ReplicaControl.SourceDataRoot,
		ActivateReceiver: receivers.Activate}
	controlService, err := nodecontrol.NewService(nodecontrol.ServiceOptions{
		Reader: reader, Journal: journal, Preparer: preparer, Adopter: adopter,
		Authorize: func(identity rafttransport.PeerIdentity, request nodecontrol.Request) bool {
			if identity.TrustDomain != local.TrustDomain || request.TargetNode != local.Node ||
				request.TargetNodeIncarnation != manifest.NodeIncarnation {
				return false
			}
			return policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow ||
				policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		},
		ValidatePayload: func(ctx context.Context, intent gateway.GroupEnrollmentIntent, payload []byte) error {
			_, validateErr := validateRF3EnrollmentPayload(ctx, intent, payload, manifest.ReplicaControl.SourceDataRoot, template)
			return validateErr
		},
		LocalNode: local.Node, LocalIncarnation: manifest.NodeIncarnation,
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	nodeInfo, err := newRF3EmptyNodeInfo(nodeOwner.store, profile, manifest, policy, migrationBudget, &servingGroups, preparer, deadline)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	// Catch-up delivers the exact grant after snapshot registration. Retain it
	// beside the certified reservation so restart can restore historical
	// configuration replay authority atomically with the recovered group.
	grantRouter := newRF3DynamicGrantRouter(transportRegistry)
	runtime := &rf3EmptyNodeRuntime{native: nativeAuthorities, peer: peer, registry: transportRegistry, lanes: lanes,
		serving: servingRegistry, reader: reader, receivers: receivers, grants: grantRouter, schemas: schemas, donors: donors, actionJournal: actionJournal, servingGroups: &servingGroups}
	membershipControl, err := shardservice.NewMembershipGrantControlService(
		grantRouter, policy, deadline, deadline,
	)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	observationControl, err := replicacontrol.NewService(replicacontrol.ServiceOptions{
		Observer:               peer.Owners(),
		AuthorizeAuthenticated: rf3AuthenticatedReplicaObservationAuthorizer(transportRegistry, policy),
		ReadDeadline:           deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	actionControl, err := newRF3ReplicaActionControl(actionJournal, peer.Owners(), transportRegistry, policy, deadline,
		profile, rf3ReplicaRetirementCleanup(schemas, donors, &servingGroups, nativeAuthorities))
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	backupControl, err := clusterbackupservice.New(clusterbackupservice.Options{
		Owner: peer.Owners(),
		Authorize: func(identity rafttransport.PeerIdentity, request clusterbackup.LiveRequest) bool {
			member, err := transportRegistry.LocalMember(request.Group)
			return err == nil && member == request.SourceMember &&
				policy.Check(identity.Node, serviceauthz.CapabilityBackup) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
		ChunkBytes: int(manifest.ReplicaControl.SourceChunkBytes), MaxConcurrent: manifest.ReplicaControl.MaxSourceConcurrent,
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	metricsProvider := &rf3MetricsProvider{owners: peer.Owners(), schemas: schemas, backup: backupControl, action: actionControl,
		budget: migrationBudget}
	metricsControl, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider: metricsProvider,
		Authorize: func(identity rafttransport.PeerIdentity) bool {
			return policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	snapshotAuthorizer := mustRF3NodeAuthorizer(rf3ControlPeerNodes(manifest, policy.NodesWith(serviceauthz.CapabilityMembership)))
	enrollmentControl, err := newRF3EnrollmentControlService(transportRegistry, policy, deadline,
		func(intent rafttransport.EnrollmentIntent) error {
			return snapshotAuthorizer.Merge([]rafttransport.NodeID{intent.Peer.NodeID})
		})
	if err == nil {
		err = enrollmentControl.AttachTransport(peer.Transport())
	}
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	children, err := newRF3DynamicChildResources(manifest, nodeOwner, schemas, peer.Owners(), transportRegistry, adoptedInventory.templates)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	childPreparer, err := newRF3GroupChildPreparer(manifest, local.Node,
		peerListener.Addr(), nativeListener.Addr(), controlListener.Addr(), snapshotListener.Addr(), children)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, childPreparer.Close()) }()
	childPreparer.inventory = adoptedInventory
	if err = adoptedInventory.checkCapacity(childPreparer.slots); err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	concurrency := min(manifest.SplitControl.operationLimit(), 8)
	childPrepareControl, err := splitcontroller.NewChildPrepareService(splitcontroller.ChildPrepareServiceOptions{
		Preparer: childPreparer,
		Authorize: func(identity rafttransport.PeerIdentity, request splitcontroller.ChildPreparation) bool {
			return request.ReplicaTarget().Node == local.Node && policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
		}, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: concurrency,
		MaxInflightBytes: uint64(splitcontroller.MaxChildPrepareWireBytes) * uint64(concurrency),
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	splitRuntime, err := newRF3SplitServingRuntime(rf3SplitServingOptions{
		manifest: manifest, schemas: schemas, nodeOwner: nodeOwner, children: children,
		owners: peer.Owners(), registrar: runtime, profile: profile, policy: policy, deadline: deadline,
		registry: transportRegistry, childPreparer: childPreparer, inventory: adoptedInventory,
		topologyActions: func(identity raftmember.RuntimeIdentity, lease *splitcontroller.RuntimeStoreLease, apply *sqldriver.ReplicatedApply) (rf3SplitTopologyActions, error) {
			return newRF3ProxiedSplitTopologyActions(profile, serviceauthz.Authority{Node: local.Node, Generation: policy.Generation()}, manifest.GatewaySeeds, identity, lease, apply)
		},
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, splitRuntime.Close()) }()
	metricsProvider.split = splitRuntime.action
	controlMux, err := (rf3ControlServices{
		membership: membershipControl, observation: observationControl, metrics: metricsControl,
		capacity: capacityControl, action: actionControl, backup: backupControl, enrollment: enrollmentControl,
		schema: schemaServices.install, schemaBuild: schemaServices.build,
		source: donors.Control, preparation: donors.Preparation,
		split: splitRuntime.action, planObservation: splitRuntime.observation.service, admission: splitRuntime.admission,
		tail: splitRuntime.tail, terminal: splitRuntime.terminal, childPrepare: childPrepareControl,
		nodeInfo: nodeInfo, nodeControl: controlService, bootstrap: receivers,
	}).mux()
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	snapshotMux, err := newRF3SnapshotMux(donors.Data, splitRuntime.artifact)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	snapshotTLS, err := servicetls.NewServer(profile, rafttransport.TrafficSnapshot,
		snapshotAuthorizer)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}

	learner, err := newRF3DynamicLearnerFactory(runtime, nodeOwner, manifest, profile, policy, gate, migrationBudget, deadline)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	runtime.learner = learner
	if err := receivers.BindRegistrar(runtime.RegisterBootstrapService); err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, runtime.CloseBootstrapServices()) }()
	if err := nodeOwner.bindEmptyRuntime(runtime); err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer nodeOwner.unbindEmptyRuntime(runtime)
	peerCtx, stopPeer := context.WithCancelCause(context.Background())
	controlCtx, stopControl := context.WithCancelCause(context.Background())
	snapshotCtx, stopSnapshot := context.WithCancelCause(context.Background())
	nativeCtx, stopNative := context.WithCancelCause(context.Background())
	defer stopPeer(context.Canceled)
	defer stopControl(context.Canceled)
	defer stopSnapshot(context.Canceled)
	defer stopNative(context.Canceled)
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Run(peerCtx) }()
	select {
	case <-peer.Started():
	case <-parent.Done():
		stopPeer(context.Cause(parent))
		return componentShutdownError(<-peerDone)
	}
	if !peer.Running() || !peer.Owners().Running() {
		return errors.Join(errRF3Serving, <-peerDone)
	}
	// Recovery publishes retained groups through the serialized execution
	// owners, which must be running first. Native and control listeners stay
	// closed to requests until this startup recovery has finished.
	recoveryCtx, cancelRecovery := context.WithTimeout(parent, rf3NetworkTimeout)
	recoveryErr := recoverRF3EmptySplitChildren(recoveryCtx, runtime, prepared, adoptedInventory, profile)
	if recoveryErr == nil {
		recoveryErr = learner.Recover(recoveryCtx)
	}
	cancelRecovery()
	if recoveryErr != nil {
		// The peer now owns recovered runtimes and its listener. Join it before
		// closing the shared lane/serving state or bootstrap repositories.
		stopPeer(recoveryErr)
		return finishRF3Serving(errors.Join(recoveryErr, componentShutdownError(<-peerDone)), lanes, servingRegistry)
	}
	pulseDone := make(chan struct{})
	go runRF3Pulse(peerCtx, pulse, pulseDone)
	controlAdmission := newRF3AcceptReadyListener(controlListener)
	controlDone := make(chan error, 1)
	go func() {
		controlDone <- controlTLS.Serve(controlCtx, controlAdmission,
			servicetls.Limits{MaxConnections: 32, MaxHandshakes: 8, HandshakeDeadline: deadline},
			func(ctx context.Context, connection rafttransport.PeerConnection) {
				if serveErr := controlMux.Serve(ctx, connection); serveErr != nil && ctx.Err() == nil {
					fmt.Fprintf(os.Stderr, "RF3 empty-node control request failed: %v\n", serveErr)
				}
			})
	}()
	snapshotAdmission := newRF3AcceptReadyListener(snapshotListener)
	snapshotDone := make(chan error, 1)
	go func() {
		snapshotDone <- snapshotTLS.Serve(snapshotCtx, snapshotAdmission,
			servicetls.Limits{MaxConnections: 32, MaxHandshakes: 8, HandshakeDeadline: deadline},
			func(ctx context.Context, connection rafttransport.PeerConnection) {
				if err := snapshotMux.Serve(ctx, connection); err != nil && ctx.Err() == nil {
					fmt.Fprintf(os.Stderr, "RF3 empty-node snapshot request failed: %v\n", err)
				}
			})
	}()
	nativeAdmission := newRF3AcceptReadyListener(nativeListener)
	nativeDone := make(chan error, 1)
	go func() {
		nativeDone <- nativeServer.ServeAuthenticated(nativeCtx, nativeAdmission, nativeTLS, deadline, 64, 16)
	}()
	var serial atomic.Uint64
	fmt.Fprintf(os.Stderr, "vibedb-shard RF3 empty node ready node=%x incarnation=%d groups=%d peer=%s native=%s snapshot=%s control=%s gateway=disabled\n",
		local.Node, manifest.NodeIncarnation, servingGroups.Load(), peerListener.Addr(), nativeListener.Addr(), snapshotListener.Addr(), controlListener.Addr())
	var primary error
	peerFinished, controlFinished, snapshotFinished, nativeFinished := false, false, false, false
	for primary == nil {
		select {
		case <-diagnostics:
			emitRF3DiagnosticSnapshotWithResources(manifest, profile, nodeOwner, nativeServer, nil, &serial, nil, nil, nil, nil, rf3AuthorityDiagnostics{})
		case <-parent.Done():
			primary = componentShutdownError(context.Cause(parent))
		case err := <-peerDone:
			if !peerFinished {
				peerFinished = true
				primary = errors.Join(errRF3Serving, err)
			}
		case err := <-controlDone:
			if !controlFinished {
				controlFinished = true
				primary = errors.Join(errRF3Serving, err)
			}
		case err := <-snapshotDone:
			if !snapshotFinished {
				snapshotFinished = true
				primary = errors.Join(errRF3Serving, err)
			}
		case err := <-nativeDone:
			if !nativeFinished {
				nativeFinished = true
				primary = errors.Join(errRF3Serving, err)
			}
		}
	}
	stopNative(context.Canceled)
	if !nativeFinished {
		primary = errors.Join(primary, componentShutdownError(<-nativeDone))
		nativeFinished = true
	}
	stopSnapshot(context.Canceled)
	if !snapshotFinished {
		primary = errors.Join(primary, componentShutdownError(<-snapshotDone))
		snapshotFinished = true
	}
	stopControl(context.Canceled)
	if !controlFinished {
		primary = errors.Join(primary, componentShutdownError(<-controlDone))
		controlFinished = true
	}
	primary = errors.Join(primary, splitRuntime.Close())
	splitRuntime = nil
	stopPeer(context.Canceled)
	if !peerFinished {
		primary = errors.Join(primary, componentShutdownError(<-peerDone))
		peerFinished = true
	}
	<-pulseDone
	return finishRF3Serving(primary, lanes, servingRegistry)
}

func mustRF3NodeAuthorizer(nodes []rafttransport.NodeID) *servicetls.NodeAuthorizer {
	if authorizer, err := servicetls.NewNodeAuthorizer(nodes); err == nil {
		return authorizer
	}
	// Common manifests include at least one controller/membership identity. A
	// nil authorizer is never accepted by NewServer; preserve fail-closed startup
	// if an operator produced a policy with no trusted snapshot peer.
	return nil
}

type rf3RejectRF3Handler struct{}

func (rf3RejectRF3Handler) Serve(_ context.Context, connection rafttransport.PeerConnection) error {
	if connection != nil {
		_ = connection.Close()
	}
	return snapshottransfer.ErrBootstrapUnauthorized
}
