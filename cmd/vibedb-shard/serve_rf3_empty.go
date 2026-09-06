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
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
)

// rf3EmptyNodeRuntime is the live process boundary retained by an empty node.
// It starts all authenticated listeners and the zero-group execution owner,
// but publishes no execution group until a later certified learner install
// calls RegisterExecutionGroup. The control reader slot is intentionally
// empty at construction; a committed-directory bootstrap adapter must attach
// it after authenticating a seed and the node's exact Joining record.
type rf3EmptyNodeRuntime struct {
	peer      *raftservice.AuthenticatedExecutionPeerRuntime
	registry  *rafttransport.StaticRegistry
	lanes     *multiraft.ExecutionLanes
	serving   *raftserve.Registry
	reader    *nodecontrol.IntentReaderSlot
	receivers *rf3DynamicBootstrapRegistry
	learner   *rf3DynamicLearnerFactory
	controlMu sync.Mutex
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
	if runtime == nil || runtime.peer == nil {
		return raftservice.ErrInvalidOwner
	}
	if err := runtime.peer.RegisterExecutionGroup(roster, group); err != nil {
		return err
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
	if runtime.servingGroups != nil {
		for {
			count := runtime.servingGroups.Load()
			if count <= 0 || runtime.servingGroups.CompareAndSwap(count, count-1) {
				break
			}
		}
	}
	return nil
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
	_ *rf3AdoptedGroupInventory,
	migrationBudget *migrationbudget.Budget,
) (resultErr error) {
	if parent == nil || listen == nil || profile == nil || policy == nil || gate == nil ||
		controlTLS == nil || nodeOwner == nil || manifest.NodeLog == nil ||
		manifest.NodeIncarnation == 0 || len(manifest.groupBundles()) != 0 ||
		embeddedGateway != nil || !validRF3ExecutionLanes(executionLaneCount) {
		return errRF3Serving
	}
	if cause := context.Cause(parent); cause != nil {
		return componentShutdownError(cause)
	}
	local := profile.LocalIdentity()
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	template, err := rf3NodePreparationTemplateFromManifest(manifest)
	if err != nil {
		return fmt.Errorf("%w: empty-node preparation template: %v", errRF3Serving, err)
	}
	transportRegistry, err := rafttransport.NewEmptyRegistry(local.Node, local.TrustDomain,
		rafttransport.Limits{MaxGroups: maxRF3ManifestGroups, MaxMembers: maxRF3ManifestGroups * rf3ManifestMembers, MaxPeers: rafttransport.AbsoluteMaxTransportPeers})
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
	if peer.Owners() == nil {
		return errors.Join(raftservice.ErrInvalidOwner, lanes.Close(), servingRegistry.Close())
	}
	var servingGroups atomic.Int64
	capacityProvider, err := replicacontrol.NewCapacityProvider(replicacontrol.CapacitySourceDirectory{
		Sources: func(context.Context) ([]replicacontrol.CapacitySource, error) {
			return nil, replicacontrol.ErrCapacityUnavailable
		},
		Node: func(context.Context) (replicacontrol.NodeCapacity, error) {
			return replicacontrol.NodeCapacity{}, replicacontrol.ErrCapacityUnavailable
		},
	})
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
	if err = nativeServer.BindAuthorization(gate, nil); err == nil {
		err = nativeServer.BindServingAuthority(func(raftservice.ServingState) bool { return servingGroups.Load() > 0 })
	}
	if err == nil {
		err = nativeServer.BindTransitionalServingAuthority(func(raftservice.ServingState, *shardservice.ReplicatedRequest) bool { return false })
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
	controlMux, err := shardcontrol.New(
		shardcontrol.Route{Discriminator: nodecontrol.NodeInfoRequestDiscriminator(), Handler: nodeInfo},
		shardcontrol.Route{Discriminator: nodecontrol.RequestDiscriminator(), Handler: controlService},
		shardcontrol.Route{Discriminator: snapshottransfer.BootstrapRequestDiscriminator(), Handler: receivers},
		shardcontrol.Route{Discriminator: replicacontrol.CapacityRequestDiscriminator(), Handler: capacityControl},
	)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	snapshotMux, err := newRF3SnapshotMux(rf3RejectRF3Handler{}, nil)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	snapshotTLS, err := servicetls.NewServer(profile, rafttransport.TrafficSnapshot,
		mustRF3NodeAuthorizer(rf3ControlPeerNodes(manifest, policy.NodesWith(serviceauthz.CapabilityMembership))))
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}

	runtime := &rf3EmptyNodeRuntime{peer: peer, registry: transportRegistry, lanes: lanes,
		serving: servingRegistry, reader: reader, receivers: receivers, servingGroups: &servingGroups}
	learner, err := newRF3DynamicLearnerFactory(runtime, nodeOwner, manifest, profile, policy, gate, migrationBudget, deadline)
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	runtime.learner = learner
	defer func() { resultErr = errors.Join(resultErr, runtime.CloseBootstrapServices()) }()
	if err := nodeOwner.bindEmptyRuntime(runtime); err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	defer nodeOwner.unbindEmptyRuntime(runtime)
	// Reopen only descriptor-backed installs whose committed directory still
	// certifies the target.  A seed outage leaves the node cold and retryable;
	// corrupt or conflicting local receipts fail startup rather than guessing
	// a replacement group identity.
	recoveryCtx, cancelRecovery := context.WithTimeout(parent, rf3NetworkTimeout)
	recoveryErr := learner.Recover(recoveryCtx)
	cancelRecovery()
	if recoveryErr != nil && !errors.Is(recoveryErr, nodecontrol.ErrStale) &&
		!errors.Is(recoveryErr, nodecontrol.ErrNotCommitted) {
		return errors.Join(recoveryErr, lanes.Close(), servingRegistry.Close())
	}
	if recoveryErr != nil {
		fmt.Fprintf(os.Stderr, "RF3 empty-node enrollment recovery deferred: %v\n", recoveryErr)
	}
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
				_ = snapshotMux.Serve(ctx, connection)
			})
	}()
	nativeAdmission := newRF3AcceptReadyListener(nativeListener)
	nativeDone := make(chan error, 1)
	go func() {
		nativeDone <- nativeServer.ServeAuthenticated(nativeCtx, nativeAdmission, nativeTLS, deadline, 64, 16)
	}()
	var serial atomic.Uint64
	fmt.Fprintf(os.Stderr, "vibedb-shard RF3 empty node ready node=%x incarnation=%d groups=0 peer=%s native=%s snapshot=%s control=%s gateway=disabled\n",
		local.Node, manifest.NodeIncarnation, peerListener.Addr(), nativeListener.Addr(), snapshotListener.Addr(), controlListener.Addr())
	var primary error
	peerFinished, controlFinished, snapshotFinished, nativeFinished := false, false, false, false
	for primary == nil {
		select {
		case <-diagnostics:
			emitRF3DiagnosticSnapshotWithResources(manifest, profile, nodeOwner, nativeServer, nil, &serial, nil, nil, nil, nil)
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
