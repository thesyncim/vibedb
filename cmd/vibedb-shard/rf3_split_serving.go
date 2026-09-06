package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/splitartifact"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	protocol "github.com/thesyncim/vibedb/shardcontrol"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type rf3SplitServingRuntime struct {
	observation *rf3SplitObservationRuntime
	registries  *splitcontroller.LocalPlanAdmissionRegistries
	binder      *splitcontroller.BoundPlanAdmissionBinder
	grants      *splitcontroller.DynamicShardActionGrants
	data        *splitcontroller.DynamicSplitData

	action    *splitcontroller.ControlService
	admission shardcontrol.Handler
	tail      shardcontrol.Handler
	artifact  shardcontrol.Handler
	terminal  shardcontrol.Handler
}

type rf3SplitServingOptions struct {
	inventory     *rf3AdoptedGroupInventory
	childPreparer *rf3GroupChildPreparer
	manifest      rf3Manifest
	prepared      []preparedRF3Group
	identities    []raftmember.RuntimeIdentity
	commands      []raftservice.CommandFence
	owners        *raftservice.ExecutionOwners
	registrar     splitcontroller.ExecutionGroupRegistrar
	profile       *rafttransport.PeerTLS
	registry      *rafttransport.StaticRegistry
	// topologyProfile supplies native topology sessions with the independent
	// frontend principal in a fused node. Storage identity remains unchanged
	// for exact physical-node admission and split data-transfer grants.
	topologyProfile *rafttransport.PeerTLS
	policy          *serviceauthz.Policy
	deadline        rafttransport.DeadlineFunc
}

func newRF3SplitServingRuntime(options rf3SplitServingOptions) (*rf3SplitServingRuntime, error) {
	maxOperations := options.manifest.SplitControl.operationLimit()
	if len(options.prepared) == 0 || len(options.prepared) != len(options.identities) ||
		len(options.prepared) != len(options.commands) || options.owners == nil || options.registrar == nil ||
		options.profile == nil || options.policy == nil || options.deadline == nil || maxOperations <= 0 {
		return nil, errRF3Serving
	}
	result := new(rf3SplitServingRuntime)
	closeOnError := func(cause error) (*rf3SplitServingRuntime, error) {
		return nil, errors.Join(cause, result.Close())
	}
	observation, err := newRF3SplitObservationRuntime(
		options.prepared, options.identities, options.commands, options.owners,
		options.registry, options.policy, options.deadline, maxOperations,
	)
	if err != nil {
		return nil, err
	}
	result.observation = observation
	retained := make([]splitcontroller.RetainedPlanRuntimeRegistry, len(options.prepared))
	for index := range options.prepared {
		item := &options.prepared[index]
		retained[index] = splitcontroller.RetainedPlanRuntimeRegistry{
			Distribution: distribution.DistributionName(item.base.Binding.Distribution),
			Shard:        distribution.ShardID(item.base.Binding.Shard),
			Allocation:   distribution.ShardAllocationGeneration(item.base.Binding.AllocationGeneration),
			Registry:     observation.registries[index],
		}
	}
	registries, err := splitcontroller.NewLocalPlanAdmissionRegistries(
		options.profile.LocalIdentity().Node, retained, maxOperations, nil,
	)
	if err != nil {
		return closeOnError(err)
	}
	result.registries = registries
	grants, err := splitcontroller.NewDynamicShardActionGrants(maxOperations * (1 + gateway.ServingReplicaCount))
	if err != nil {
		return closeOnError(err)
	}
	data, err := splitcontroller.NewDynamicSplitData(maxOperations)
	if err != nil {
		return closeOnError(err)
	}
	result.grants, result.data = grants, data
	groupBundles := options.manifest.groupBundles()
	staticBootstraps := make([]*pb.Snapshot, len(groupBundles))
	for index, group := range groupBundles {
		staticBootstraps[index], err = loadRF3SplitStaticBootstrap(group.ChildRegistry)
		if err != nil {
			return closeOnError(err)
		}
	}
	topologyProfile := options.topologyProfile
	if topologyProfile == nil {
		topologyProfile = options.profile
	}
	authority := serviceauthz.Authority{
		Node: topologyProfile.LocalIdentity().Node, Generation: options.policy.Generation(),
	}
	if options.policy.Check(authority.Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow ||
		options.policy.Check(authority.Node, serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow {
		return closeOnError(errRF3Serving)
	}
	makeSource := func(identity raftmember.RuntimeIdentity, command raftservice.CommandFence, apply *sqldriver.ReplicatedApply, registry *splitcontroller.RuntimeStoreRegistry) (splitcontroller.AdmittedSourceRuntime, error) {
		target, targetErr := splitcontroller.ShardActionTargetForServing(identity, command)
		if targetErr != nil {
			return splitcontroller.AdmittedSourceRuntime{}, targetErr
		}
		return splitcontroller.AdmittedSourceRuntime{
			Distribution: distribution.DistributionName(identity.Distribution),
			Shard:        distribution.ShardID(identity.Shard),
			Allocation:   distribution.ShardAllocationGeneration(identity.AllocationGeneration),
			Registry:     registry, Target: target,
			NewExecutor: func(
				ctx context.Context, catalog *gateway.Snapshot, plan *splitcontroller.Plan,
				admission splitcontroller.PlanAdmission, lease *splitcontroller.RuntimeStoreLease,
			) (splitcontroller.ShardActionExecutor, error) {
				store, openErr := lease.PinnedStore()
				if openErr != nil {
					return nil, openErr
				}
				source, openErr := splitcontroller.NewLocalSourceRuntimeActions(store, apply)
				if openErr != nil {
					return nil, openErr
				}
				var routeReplicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
				route, found := catalog.ResolveReplicatedRoute(
					distribution.DistributionName(identity.Distribution), distribution.ShardID(identity.Shard),
					routeReplicas[:0],
				)
				if !found {
					return nil, errRF3Serving
				}
				sourceNodes := make([]rafttransport.NodeID, len(route.Replicas))
				for replicaIndex := range route.Replicas {
					sourceNodes[replicaIndex] = route.Replicas[replicaIndex].Node
				}
				opener, openErr := newRF3SplitStreamOpener(
					options.profile, options.deadline,
					func(ctx context.Context, address string) (net.Conn, error) {
						return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
					}, catalog, plan, 8,
				)
				if openErr != nil {
					return nil, openErr
				}
				tailClient, openErr := splitcontroller.NewTailStreamClient(splitcontroller.TailStreamClientOptions{
					Opener: opener, ReadDeadline: options.deadline, WriteDeadline: options.deadline,
					MaxConcurrent:    4,
					MaxInflightBytes: 4 * uint64(rangesplit.MaxTailStreamRequestBytes+rangesplit.TailStreamResponseBytes),
				})
				if openErr != nil {
					return nil, openErr
				}
				topologyFactory := &rf3RetainedPruneFactory{
					tls: topologyProfile, authority: authority, lease: lease, source: apply,
				}
				composite, openErr := splitcontroller.NewCompositeShardActionExecutor(splitcontroller.CompositeShardActionExecutorOptions{
					Operation: plan.OperationID(), Actions: splitcontroller.SourceSplitActionMask(),
					Source: source, TailSinks: splitcontroller.RF3TailSinkResolver{Client: tailClient},
					CaptureFactory: topologyFactory, Seal: options.owners,
					PruneFactory:       topologyFactory,
					PruneLimits:        rangesplit.RetainedPruneLimits{},
					ArtifactChunkBytes: rangesplit.DefaultChildArtifactChunkBytes,
					RecoverArtifacts: func(ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation) (bool, error) {
						// The durable action witness predates a possible election.
						// Leadership transfer must use the current local serving fence;
						// the admitted plan and immutable artifact proof stay unchanged.
						serving, err := options.owners.Probe(ctx, identity.Group)
						if err != nil {
							return false, err
						}
						observed.SourceStatus, observed.SourceServing = serving.Status, serving
						return splitcontroller.RecoverSourceArtifactOwner(ctx, plan, observed, opener, options.deadline, options.owners)
					},
				})
				if openErr != nil {
					return nil, openErr
				}
				return newRF3AdmittedExecutor(composite, func() error {
					return data.InstallSource(plan, admission.PlanDigest, lease, source, sourceNodes)
				}, func() error {
					return data.RevokeLocal(plan.OperationID(), admission.PlanDigest)
				}), nil
			},
		}, nil
	}
	sources := make([]splitcontroller.AdmittedSourceRuntime, len(options.prepared))
	for index := range options.prepared {
		sources[index], err = makeSource(options.identities[index], options.commands[index], options.prepared[index].apply, observation.registries[index])
		if err != nil {
			return closeOnError(err)
		}
	}
	liveSources := &rf3AdoptedSourceResolver{registries: registries, inventory: options.inventory,
		observation: observation.provider, owners: options.owners, makeSource: makeSource,
		live: make(map[raftmember.GroupKey]rf3RetainedSource)}
	for index, identity := range options.identities {
		liveSources.live[identity.Group] = rf3RetainedSource{runtime: rf3AdoptedRuntime{identity: identity, apply: options.prepared[index].apply}, registry: observation.registries[index]}
	}
	var adoptionCheckpoint splitcontroller.ChildAdoptionCheckpoint
	if options.inventory != nil {
		adoptionCheckpoint = options.inventory
	}
	childFactory := func(
		ctx context.Context, catalog *gateway.Snapshot, plan *splitcontroller.Plan,
		admission splitcontroller.PlanAdmission, child uint8, replica splitcontroller.ChildReplicaTarget,
		lease *splitcontroller.RuntimeStoreLease,
	) (splitcontroller.ShardActionExecutor, error) {
		registryIndex, childRegistry, found := rf3SplitChildRegistryForTarget(
			options.manifest, [32]byte(plan.OperationID()), child, replica,
		)
		if !found {
			return nil, errRF3Serving
		}
		if replay, group, restored, replayErr := liveSources.adoptedChildReplay(plan, admission, child, replica); replayErr != nil {
			return nil, replayErr
		} else if restored {
			return newRF3AdmittedExecutor(replay, func() error {
				return observation.provider.RegisterGroups([]splitcontroller.LocalObservationGroup{group})
			}, replay.Close), nil
		}
		opener, openErr := newRF3SplitStreamOpener(
			options.profile, options.deadline,
			func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
			}, catalog, plan, 8,
		)
		if openErr != nil {
			return nil, openErr
		}
		key, openErr := loadRF3SplitChildWALKey(childRegistry, &options.prepared[registryIndex])
		if openErr != nil {
			return nil, openErr
		}
		defer clear(key.Material[:])
		workspace := make([]byte, rangesplit.DefaultChildArtifactChunkBytes)
		executor, openErr := splitcontroller.NewLazyReplicatedChildExecutor(
			splitcontroller.LazyReplicatedChildExecutorOptions{
				Plan: plan, PlanDigest: admission.PlanDigest, Child: child, Replica: replica, Lease: lease,
				Registrar: options.registrar, StaticBootstrap: proto.Clone(staticBootstraps[registryIndex]).(*pb.Snapshot),
				AdoptionCheckpoint: adoptionCheckpoint,
				ArtifactOptions:    replicatedstate.SnapshotArtifactOptions{}, WALKey: key,
				WALOptions:      childRegistry.WAL.Options,
				CheckpointBytes: childRegistry.StageCheckpointBytes,
				Data:            data, Opener: opener, ReadDeadline: options.deadline, WriteDeadline: options.deadline,
				ChunkBytes: rangesplit.DefaultChildArtifactChunkBytes, MaxReconnects: 3, Workspace: workspace,
			},
		)
		if openErr != nil {
			clear(key.Material[:])
			return nil, openErr
		}
		target, found := plan.Target(child)
		if !found {
			_ = executor.Close()
			return nil, errRF3Serving
		}
		registry, registryErr := lease.PinnedRegistry()
		if registryErr != nil {
			_ = executor.Close()
			return nil, registryErr
		}
		group, openErr := splitcontroller.PreparedChildObservationGroup(target, replica, registry, executor)
		if openErr != nil {
			_ = executor.Close()
			return nil, errors.Join(errRF3Serving, openErr)
		}
		registered := false
		return newRF3AdmittedExecutor(executor, func() error {
			if err := executor.PublishTailTarget(); err != nil {
				return err
			}
			if err := observation.provider.RegisterGroups([]splitcontroller.LocalObservationGroup{group}); err != nil {
				return err
			}
			registered = true
			return nil
		}, func() error {
			var unregister error
			if registered && !liveSources.isRetained(group.Identity.Group) {
				unregister = observation.provider.UnregisterGroups([]splitcontroller.LocalObservationGroup{group})
			}
			return errors.Join(unregister, executor.Close())
		}), nil
	}
	factory, err := splitcontroller.NewLocalAdmittedGrantFactory(splitcontroller.LocalAdmittedGrantFactoryOptions{
		Node: options.profile.LocalIdentity().Node, Sources: sources, Children: childFactory,
	})
	if err != nil {
		return closeOnError(err)
	}
	liveSources.factory = factory
	binder, err := splitcontroller.NewBoundPlanAdmissionBinder(factory, grants)
	if err != nil {
		return closeOnError(err)
	}
	result.binder = binder
	installer, err := splitcontroller.NewPlanAdmissionInstaller(liveSources, binder, splitcontroller.AbsoluteMaxLocalPlanAdmissionStores)
	if err != nil {
		return closeOnError(err)
	}
	concurrency := min(maxOperations, splitcontroller.AbsoluteMaxPlanAdmissionConcurrency)
	admission, err := splitcontroller.NewPlanAdmissionService(splitcontroller.PlanAdmissionServiceOptions{
		Installer: installer,
		Authorize: func(peer rafttransport.PeerIdentity, _ splitcontroller.PlanAdmissionEnvelope) bool {
			return options.policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
		},
		ReadDeadline: options.deadline, WriteDeadline: options.deadline, MaxConcurrent: concurrency,
		MaxInflightBytes: uint64(splitcontroller.MaxPlanAdmissionRequestBytes) * uint64(concurrency),
	})
	if err != nil {
		return closeOnError(err)
	}
	dispatcher, err := splitcontroller.NewShardActionRuntimeDispatcher(grants)
	if err != nil {
		return closeOnError(err)
	}
	remote, err := splitcontroller.NewRemoteActionService(dispatcher)
	if err != nil {
		return closeOnError(err)
	}
	action, err := splitcontroller.OpenShardControlService(
		options.manifest.SplitControl.JournalPath,
		protocol.JournalLimits{MaxRecords: options.manifest.SplitControl.MaxRecords,
			MaxFileBytes: options.manifest.SplitControl.MaxFileBytes},
		options.manifest.SplitControl.Grants, rf3LeaderSplitActions{owners: options.owners, next: remote},
	)
	if err != nil {
		return closeOnError(err)
	}
	result.action = action
	tail, err := splitcontroller.NewTailStreamService(splitcontroller.TailStreamServiceOptions{
		Resolver: data, Authorize: data.AuthorizeTail,
		ReadDeadline: options.deadline, WriteDeadline: options.deadline, MaxConcurrent: concurrency,
		MaxInflightBytes: uint64(rangesplit.MaxTailStreamRequestBytes) * uint64(concurrency),
	})
	if err != nil {
		return closeOnError(err)
	}
	artifact, err := splitartifact.NewService(splitartifact.ServiceOptions{
		Source: data, Authorize: data.AuthorizeArtifact,
		ReadDeadline: options.deadline, WriteDeadline: options.deadline,
		MaxConnections: concurrency, MaxChunkBytes: rangesplit.DefaultChildArtifactChunkBytes,
		MaxInflightBytes: int64(rangesplit.DefaultChildArtifactChunkBytes) * int64(concurrency),
	})
	if err != nil {
		return closeOnError(err)
	}
	retirer, err := splitcontroller.NewLocalTerminalRetirer(binder, grants, data)
	if err != nil {
		return closeOnError(err)
	}
	terminal, err := splitcontroller.NewTerminalRetirementService(
		rf3PreparedChildRetirer{certified: retirer, preparer: options.childPreparer}, func(peer rafttransport.PeerIdentity, _ splitcontroller.TerminalRetirement) bool {
			return options.policy.Check(peer.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		}, options.deadline, options.deadline,
	)
	if err != nil {
		return closeOnError(err)
	}
	result.admission, result.tail, result.artifact, result.terminal = admission, tail, artifact, terminal
	return result, nil
}

// A node-backed source has no per-group WAL handle. Authenticate the child
// provider key against the physical log that actually retains its metadata.
func loadRF3SplitChildWALKey(registry rf3ManifestSplitChildRegistry, source *preparedRF3Group) (raftstore.Key, error) {
	key, err := loadRF3WALKey(registry.WAL.KeyID, registry.WAL.KeyMaterialPath)
	if err != nil {
		return raftstore.Key{}, err
	}
	if source.nodeOwner != nil {
		key.Wrapped, err = source.nodeOwner.store.AuthenticatedWrappedKeyMetadata(key)
	} else {
		key.Wrapped, err = source.wal.AuthenticatedWrappedKeyMetadata(key)
	}
	if err != nil {
		clear(key.Material[:])
		return raftstore.Key{}, fmt.Errorf("authenticate split child WAL key metadata: %w", err)
	}
	return key, nil
}

type rf3AdmittedExecutor struct {
	mu       sync.Mutex
	executor splitcontroller.ShardActionExecutor
	activate func() error
	abort    func() error
	active   bool
	aborted  bool
}

func newRF3AdmittedExecutor(
	executor splitcontroller.ShardActionExecutor, activate, abort func() error,
) *rf3AdmittedExecutor {
	return &rf3AdmittedExecutor{executor: executor, activate: activate, abort: abort}
}

func (executor *rf3AdmittedExecutor) ActivateAdmittedShardExecutor() error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.aborted {
		return errRF3Serving
	}
	if executor.active {
		return nil
	}
	if err := executor.activate(); err != nil {
		return err
	}
	executor.active = true
	return nil
}

func (executor *rf3AdmittedExecutor) AbortAdmittedShardExecutor() error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.aborted {
		return nil
	}
	executor.aborted = true
	return executor.abort()
}

func (executor *rf3AdmittedExecutor) ExecuteSplitAction(
	ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation,
	action splitcontroller.Action,
) error {
	executor.mu.Lock()
	active := executor.active && !executor.aborted
	executor.mu.Unlock()
	if !active {
		return splitcontroller.ErrRemoteExecution
	}
	return executor.executor.ExecuteSplitAction(ctx, plan, observed, action)
}

func (executor *rf3AdmittedExecutor) ExecuteAuthorizedSplitAction(
	ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation,
	action splitcontroller.Action,
) error {
	executor.mu.Lock()
	active := executor.active && !executor.aborted
	delegate, ok := executor.executor.(splitcontroller.AuthorizedShardActionExecutor)
	executor.mu.Unlock()
	if !active || !ok {
		return splitcontroller.ErrRemoteExecution
	}
	return delegate.ExecuteAuthorizedSplitAction(ctx, plan, observed, action)
}

var _ splitcontroller.AdmittedShardExecutorActivation = (*rf3AdmittedExecutor)(nil)
var _ splitcontroller.AuthorizedShardActionExecutor = (*rf3AdmittedExecutor)(nil)

func loadRF3SplitStaticBootstrap(registry rf3ManifestSplitChildRegistry) (*pb.Snapshot, error) {
	raw, err := readRF3BoundedFile(registry.StaticBootstrapPath, replicatedstate.MaxStaticBootstrapEnvelopeBytes)
	if err != nil {
		return nil, errors.Join(errRF3Serving, err)
	}
	if err := validateRF3SplitChildBootstrap(raw, registry); err != nil {
		return nil, fmt.Errorf("%w: split child static bootstrap: %w", errRF3Serving, err)
	}
	result := new(pb.Snapshot)
	if err = proto.Unmarshal(raw, result); err != nil {
		return nil, errors.Join(errRF3Serving, err)
	}
	clear(raw)
	return result, nil
}

func (runtime *rf3SplitServingRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var result error
	if runtime.action != nil {
		result = errors.Join(result, runtime.action.Close())
	}
	if runtime.data != nil {
		result = errors.Join(result, runtime.data.Close())
	}
	if runtime.binder != nil {
		result = errors.Join(result, runtime.binder.Close())
	}
	if runtime.registries != nil {
		result = errors.Join(result, runtime.registries.Close())
	}
	if runtime.observation != nil {
		result = errors.Join(result, runtime.observation.Close())
	}
	return result
}

// Source actions are group-scoped and must execute on the current local leader.
// Return an error before action execution so the journal never caches a
// follower's refusal as the terminal result of a replayable step.
type rf3LeaderSplitActions struct {
	owners interface {
		Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	}
	next protocol.ActionExecutor
}

func (actions rf3LeaderSplitActions) ExecuteAction(ctx context.Context, peer rafttransport.PeerIdentity, request protocol.Request) (protocol.Response, error) {
	target, err := splitcontroller.OpenRemoteActionTarget(request)
	if err != nil {
		return protocol.Response{}, err
	}
	if target.Member == 0 {
		state, err := actions.owners.Probe(ctx, target.Group)
		if err != nil {
			return protocol.Response{}, err
		}
		if state.Status.MemberID == 0 || state.Status.LeaderID != state.Status.MemberID {
			return protocol.Response{}, splitcontroller.ErrShardControlNotLeader
		}
	}
	return actions.next.ExecuteAction(ctx, peer, request)
}
