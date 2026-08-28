package main

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type rf3SplitObservationRuntime struct {
	registries []*splitcontroller.RuntimeStoreRegistry
	provider   *splitcontroller.LocalPlanObservationProvider
	service    shardcontrol.Handler
}

func newRF3SplitObservationRuntime(
	prepared []preparedRF3Group,
	identities []raftmember.RuntimeIdentity,
	commands []raftservice.CommandFence,
	owners splitcontroller.LocalObservationOwner,
	policy *serviceauthz.Policy,
	deadline rafttransport.DeadlineFunc,
	maxOperations int,
) (*rf3SplitObservationRuntime, error) {
	if len(prepared) == 0 || len(prepared) != len(identities) || len(prepared) != len(commands) ||
		owners == nil || policy == nil || deadline == nil ||
		maxOperations <= 0 || maxOperations > maxRF3SplitChildOperations {
		return nil, errRF3Serving
	}
	result := &rf3SplitObservationRuntime{
		registries: make([]*splitcontroller.RuntimeStoreRegistry, 0, len(prepared)),
	}
	closeOnError := func(cause error) (*rf3SplitObservationRuntime, error) {
		return nil, errors.Join(cause, result.Close())
	}
	groups := make([]splitcontroller.LocalObservationGroup, 0, len(prepared))
	for index := range prepared {
		item := &prepared[index]
		digest := item.splitRuntimeDigest
		if digest == ([32]byte{}) {
			digest = item.base.RelationManifestDigest
		}
		registry, err := splitcontroller.OpenRuntimeStoreRegistry(
			item.manifest.Route.SplitRuntimeRoot,
			digest,
			maxOperations,
			nil,
		)
		if err != nil {
			return closeOnError(err)
		}
		result.registries = append(result.registries, registry)
		groups = append(groups, splitcontroller.LocalObservationGroup{
			Identity: identities[index], Command: commands[index],
			Registry: registry, Capture: item.apply,
		})
	}
	provider, err := splitcontroller.NewLocalPlanObservationProvider(owners, groups)
	if err != nil {
		return closeOnError(err)
	}
	concurrency := min(maxOperations, splitcontroller.AbsoluteMaxPlanObservationConcurrency)
	service, err := splitcontroller.NewPlanObservationService(
		splitcontroller.PlanObservationServiceOptions{
			Provider: provider,
			Authorize: func(peer rafttransport.PeerIdentity, _ splitcontroller.PlanObservationRequest, _ uint64, _ bool) bool {
				return policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
			},
			ReadDeadline: deadline, WriteDeadline: deadline,
			MaxConcurrent: concurrency, MaxResponseBytes: splitcontroller.MaxPlanObservationResponseBytes,
		},
	)
	if err != nil {
		return closeOnError(err)
	}
	result.provider, result.service = provider, service
	return result, nil
}

func (runtime *rf3SplitObservationRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var result error
	for _, registry := range runtime.registries {
		result = errors.Join(result, registry.Close())
	}
	runtime.registries = nil
	return result
}
