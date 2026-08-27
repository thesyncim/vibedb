package main

import (
	"context"
	"errors"
	"net"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/splitartifact"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	protocol "github.com/thesyncim/vibedb/shardcontrol"
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
}

type rf3SplitServingOptions struct {
	manifest   rf3Manifest
	prepared   []preparedRF3Group
	identities []raftmember.RuntimeIdentity
	commands   []raftservice.CommandFence
	owners     *raftservice.ExecutionOwners
	registrar  splitcontroller.ExecutionGroupRegistrar
	profile    *rafttransport.PeerTLS
	policy     *serviceauthz.Policy
	deadline   rafttransport.DeadlineFunc
}

func newRF3SplitServingRuntime(options rf3SplitServingOptions) (*rf3SplitServingRuntime, error) {
	maxOperations := options.manifest.SplitControl.ChildRegistry.MaxOperations
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
		options.policy, options.deadline, maxOperations,
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
	staticBootstrap, err := loadRF3SplitStaticBootstrap(options.manifest.SplitControl.ChildRegistry)
	if err != nil {
		return closeOnError(err)
	}
	authority := serviceauthz.Authority{
		Node: options.profile.LocalIdentity().Node, Generation: options.policy.Generation(),
	}
	if options.policy.Check(authority.Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
		return closeOnError(errRF3Serving)
	}
	sources := make([]splitcontroller.AdmittedSourceRuntime, len(options.prepared))
	for index := range options.prepared {
		item, identity, command := &options.prepared[index], options.identities[index], options.commands[index]
		target, targetErr := splitcontroller.ShardActionTargetForServing(identity, command)
		if targetErr != nil {
			return closeOnError(targetErr)
		}
		apply := item.apply
		sources[index] = splitcontroller.AdmittedSourceRuntime{
			Distribution:   distribution.DistributionName(identity.Distribution),
			Shard:          distribution.ShardID(identity.Shard),
			Allocation:     distribution.ShardAllocationGeneration(identity.AllocationGeneration),
			ManifestDigest: item.base.RelationManifestDigest, Target: target,
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
				if openErr = data.InstallSource(plan, admission.PlanDigest, lease, source, sourceNodes); openErr != nil {
					return nil, openErr
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
				return splitcontroller.NewCompositeShardActionExecutor(splitcontroller.CompositeShardActionExecutorOptions{
					Operation: plan.OperationID(), Actions: splitcontroller.SourceSplitActionMask(),
					Source: source, TailSinks: splitcontroller.RF3TailSinkResolver{Client: tailClient},
					Seal: options.owners,
					PruneFactory: &rf3RetainedPruneFactory{
						tls: options.profile, authority: authority, lease: lease,
					},
					PruneLimits:        rangesplit.RetainedPruneLimits{},
					ArtifactChunkBytes: rangesplit.DefaultChildArtifactChunkBytes,
				})
			},
		}
	}
	childFactory := func(
		ctx context.Context, catalog *gateway.Snapshot, plan *splitcontroller.Plan,
		admission splitcontroller.PlanAdmission, child uint8, replica splitcontroller.ChildReplicaTarget,
		lease *splitcontroller.RuntimeStoreLease,
	) (splitcontroller.ShardActionExecutor, error) {
		opener, openErr := newRF3SplitStreamOpener(
			options.profile, options.deadline,
			func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
			}, catalog, plan, 8,
		)
		if openErr != nil {
			return nil, openErr
		}
		key, openErr := loadRF3WALKey(
			options.manifest.SplitControl.ChildRegistry.WAL.KeyID,
			options.manifest.SplitControl.ChildRegistry.WAL.KeyMaterialPath,
		)
		if openErr != nil {
			return nil, openErr
		}
		workspace := make([]byte, rangesplit.DefaultChildArtifactChunkBytes)
		executor, openErr := splitcontroller.NewLazyReplicatedChildExecutor(
			splitcontroller.LazyReplicatedChildExecutorOptions{
				Plan: plan, PlanDigest: admission.PlanDigest, Child: child, Replica: replica, Lease: lease,
				Registrar: options.registrar, StaticBootstrap: proto.Clone(staticBootstrap).(*pb.Snapshot),
				ArtifactOptions: replicatedstate.SnapshotArtifactOptions{}, WALKey: key,
				WALOptions:      options.manifest.SplitControl.ChildRegistry.WAL.Options,
				CheckpointBytes: options.manifest.SplitControl.ChildRegistry.StageCheckpointBytes,
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
		if openErr != nil || observation.provider.RegisterGroups([]splitcontroller.LocalObservationGroup{group}) != nil {
			_ = executor.Close()
			return nil, errors.Join(errRF3Serving, openErr)
		}
		return executor, nil
	}
	factory, err := splitcontroller.NewLocalAdmittedGrantFactory(splitcontroller.LocalAdmittedGrantFactoryOptions{
		Node: options.profile.LocalIdentity().Node, Sources: sources, Children: childFactory,
	})
	if err != nil {
		return closeOnError(err)
	}
	binder, err := splitcontroller.NewBoundPlanAdmissionBinder(factory, grants)
	if err != nil {
		return closeOnError(err)
	}
	result.binder = binder
	installer, err := splitcontroller.NewPlanAdmissionInstaller(registries, binder, splitcontroller.AbsoluteMaxLocalPlanAdmissionStores)
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
		options.manifest.SplitControl.Grants, remote,
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
	result.admission, result.tail, result.artifact = admission, tail, artifact
	return result, nil
}

func loadRF3SplitStaticBootstrap(registry rf3ManifestSplitChildRegistry) (*pb.Snapshot, error) {
	raw, err := readRF3BoundedFile(registry.StaticBootstrapPath, replicatedstate.MaxStaticBootstrapEnvelopeBytes)
	if err != nil || validateRF3SplitChildBootstrap(raw, registry) != nil {
		return nil, errors.Join(errRF3Serving, err)
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
