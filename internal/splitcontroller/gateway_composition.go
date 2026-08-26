package splitcontroller

import (
	"context"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type CatalogGatewaySplitActions struct{ Authority SplitCatalogAuthority }

func (actions CatalogGatewaySplitActions) ExecuteGatewaySplitAction(
	ctx context.Context, plan *Plan, observed Observation, action Action,
) error {
	if actions.Authority == nil || action.Kind != ActionPublishCatalog {
		return ErrControllerTrigger
	}
	return ExecutePublishCatalog(ctx, plan, observed, actions.Authority)
}

type PlanAdmissionNodeClient interface {
	Install(context.Context, rafttransport.NodeID, *gateway.Snapshot, PlanAdmission) error
}

type RF3PlanAdmissionCoordinatorOptions struct {
	Client        PlanAdmissionNodeClient
	Routes        PlanAdmissionRoutePublisher
	MaxConcurrent int
	MaxAttempts   int
}

type RF3PlanAdmissionCoordinator struct {
	client      PlanAdmissionNodeClient
	routes      PlanAdmissionRoutePublisher
	concurrent  int
	maxAttempts int
}

func NewRF3PlanAdmissionCoordinator(
	options RF3PlanAdmissionCoordinatorOptions,
) (*RF3PlanAdmissionCoordinator, error) {
	if options.Client == nil || options.Routes == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > MaxPlanObservationEndpoints || options.MaxAttempts <= 0 ||
		options.MaxAttempts > 16 {
		return nil, ErrPlanAdmission
	}
	return &RF3PlanAdmissionCoordinator{
		client: options.Client, routes: options.Routes,
		concurrent: options.MaxConcurrent, maxAttempts: options.MaxAttempts,
	}, nil
}

func (coordinator *RF3PlanAdmissionCoordinator) AdmitPlan(
	ctx context.Context,
	catalog *gateway.Snapshot,
	plan *Plan,
	admission PlanAdmission,
) error {
	if coordinator == nil || ctx == nil || catalog == nil || plan == nil ||
		plan.OperationID() != admission.Operation {
		return ErrPlanAdmission
	}
	want, err := NewPlanAdmission(catalog, plan)
	if err != nil || want.CatalogGeneration != admission.CatalogGeneration ||
		want.CatalogDigest != admission.CatalogDigest || want.PlanDigest != admission.PlanDigest {
		return errors.Join(ErrPlanAdmission, err)
	}
	nodes, err := exactPlanAdmissionNodes(catalog, plan)
	if err != nil {
		return err
	}
	type result struct{ err error }
	results := make([]result, len(nodes))
	semaphore := make(chan struct{}, coordinator.concurrent)
	var group sync.WaitGroup
	for index := range nodes {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results[index].err = context.Cause(ctx)
				return
			}
			var joined error
			for attempt := 0; attempt < coordinator.maxAttempts; attempt++ {
				if cause := context.Cause(ctx); cause != nil {
					results[index].err = errors.Join(joined, cause)
					return
				}
				installErr := coordinator.client.Install(ctx, nodes[index], catalog, admission)
				if installErr == nil {
					return
				}
				joined = errors.Join(joined, installErr)
			}
			results[index].err = joined
		}()
	}
	group.Wait()
	var joined error
	for index := range results {
		joined = errors.Join(joined, results[index].err)
	}
	if joined != nil {
		return errors.Join(ErrPlanAdmission, joined)
	}
	if err = coordinator.routes.InstallPlanRoutes(catalog, plan, admission); err != nil {
		return errors.Join(ErrPlanAdmission, err)
	}
	return nil
}

func exactPlanAdmissionNodes(
	catalog *gateway.Snapshot, plan *Plan,
) ([]rafttransport.NodeID, error) {
	if catalog == nil || plan == nil {
		return nil, ErrPlanAdmission
	}
	route, ok := catalog.ResolveReplicatedRoute(
		plan.source.Distribution, plan.source.Shard,
		make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	)
	if !ok || len(route.Replicas) != gateway.ServingReplicaCount {
		return nil, ErrPlanAdmission
	}
	nodes := make([]rafttransport.NodeID, 0, gateway.ServingReplicaCount*int(plan.childCount))
	appendNode := func(node rafttransport.NodeID) error {
		if node == (rafttransport.NodeID{}) {
			return ErrPlanAdmission
		}
		for _, current := range nodes {
			if current == node {
				return nil
			}
		}
		nodes = append(nodes, node)
		return nil
	}
	for _, replica := range route.Replicas {
		if err := appendNode(replica.Node); err != nil {
			return nil, err
		}
	}
	for child := uint8(0); child < plan.childCount; child++ {
		target, present := plan.Target(child)
		if !present {
			continue
		}
		if len(target.Replicas) != gateway.ServingReplicaCount {
			return nil, ErrPlanAdmission
		}
		for _, replica := range target.Replicas {
			if err := appendNode(replica.Node); err != nil {
				return nil, err
			}
		}
	}
	if len(nodes) == 0 || len(nodes) > MaxPlanObservationEndpoints {
		return nil, ErrPlanAdmission
	}
	return nodes, nil
}
