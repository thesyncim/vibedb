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
	registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy,
	deadline rafttransport.DeadlineFunc,
	maxOperations int,
) (*rf3SplitObservationRuntime, error) {
	if len(prepared) == 0 || len(prepared) != len(identities) || len(prepared) != len(commands) ||
		owners == nil || registry == nil || policy == nil || deadline == nil ||
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
			Provider:     provider,
			Authorize:    rf3PlanObservationAuthorizer(registry, policy),
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

// Controller observations retain the membership capability boundary. A
// source voter additionally needs a read-only observation of every peer while
// recovering the one durable artifact owner after a leader handoff. Bind that
// exception to the exact requested group and its current committed voter role;
// it grants neither child observation nor any membership mutation.
func rf3PlanObservationAuthorizer(
	registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy,
) splitcontroller.PlanObservationAuthorizeFunc {
	return func(
		peer rafttransport.PeerIdentity,
		request splitcontroller.PlanObservationRequest,
		_ uint64,
		source bool,
	) bool {
		if registry == nil || policy == nil {
			return false
		}
		if policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow {
			return true
		}
		if !source || policy.Check(peer.Node, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow {
			return false
		}
		member, err := registry.Member(request.Group, peer.Node)
		if err != nil {
			return false
		}
		role, err := registry.Role(request.Group, member)
		return err == nil && role == rafttransport.MemberVoter
	}
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
