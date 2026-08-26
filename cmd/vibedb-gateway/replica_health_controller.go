package main

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

var errGatewayReplicaHealth = errors.New("vibedb-gateway: invalid replica health controller configuration")

// gatewayFailureCertificateAuthority streams records committed by the
// replicated health authority. Implementations must not synthesize a
// certificate from gateway-local probe timeouts. The callback keeps a large
// cluster from requiring one process-sized failure slice.
type gatewayFailureCertificateAuthority interface {
	VisitFailureCertificates(
		context.Context, *gateway.Snapshot,
		func(rebalance.FailureQuorumCertificate) error,
	) error
}

// gatewayReplicaHealthObservation is an authenticated, detached shard cut.
// The publication and leader status are checked against the certificate by
// rebalance.PlanFailedReplicaReplacement; Healthy only selects a safe donor.
type gatewayReplicaHealthObservation struct {
	Publication raftmodel.Publication
	Leader      raftmember.RuntimeStatus
	Healthy     []rebalance.HealthyReplica
}

// gatewayReplicaHealthObserver obtains signed/authenticated observations from
// the current shard replicas. It is deliberately separate from the failure
// authority: reachability probes cannot grant removal authority.
type gatewayReplicaHealthObserver interface {
	ObserveReplicaHealth(
		context.Context, *gateway.Snapshot, rebalance.FailureQuorumCertificate,
	) (gatewayReplicaHealthObservation, error)
}

// gatewayReplicaCandidateInventory returns the configured placement inventory
// at the certificate's topology/health epoch. The scheduler performs the exact
// anti-affinity and freshness checks and selects in O(n), constant workspace.
type gatewayReplicaCandidateInventory interface {
	ReplacementCandidates(
		context.Context, *gateway.Snapshot, rebalance.FailureQuorumCertificate,
	) ([]rebalance.ReplacementCandidate, error)
}

type gatewayReplicaHealthPass struct {
	Certificates uint64
	Eligible     uint64
	Submitted    uint64
}

type gatewayFailedReplicaMoveSubmitter interface {
	Submit(context.Context, *rebalance.Plan) (rebalance.Action, error)
}

// gatewayFailedReplicaMoveSink hands the authorized plan to the existing RF3
// move controller. Controller.Submit writes the canonical intent through the
// ReplicatedCatalogAuthority before executing its first idempotent action.
type gatewayFailedReplicaMoveSink struct {
	controller gatewayFailedReplicaMoveSubmitter
}

func (sink gatewayFailedReplicaMoveSink) SubmitFailedReplicaMove(
	ctx context.Context, intent rebalance.FailedReplicaMoveIntent,
) error {
	if ctx == nil || sink.controller == nil || intent.Plan == nil ||
		intent.Operation == (rebalance.OperationID{}) ||
		intent.Plan.OperationID() != intent.Operation || len(intent.Intent) == 0 {
		return errGatewayReplicaHealth
	}
	evidence, placement, ok := intent.Plan.FailedReplicaAuthorizationDigests()
	if !ok || evidence != intent.Evidence || placement != intent.Placement {
		return errGatewayReplicaHealth
	}
	identity, err := rebalance.InspectReplicaMoveIntent(intent.Intent)
	if err != nil || identity.Operation != intent.Operation {
		return errors.Join(err, errGatewayReplicaHealth)
	}
	_, err = sink.controller.Submit(ctx, intent.Plan)
	return err
}

// gatewayReplicaHealthController converts only quorum-certified, multi-epoch
// failures into exact durable move intents. It owns neither a local failure
// timer nor a process-local work queue; every pass reopens all authorities.
type gatewayReplicaHealthController struct {
	catalog   gatewayReplicaCatalogReader
	failures  gatewayFailureCertificateAuthority
	observer  gatewayReplicaHealthObserver
	inventory gatewayReplicaCandidateInventory
	sink      rebalance.FailedReplicaMoveSink
	schedule  func(context.Context, rebalance.FailedReplicaPlanningCut,
		rebalance.FailedReplicaMoveSink) (rebalance.FailedReplicaMoveIntent, error)
}

func newGatewayReplicaHealthController(
	catalog gatewayReplicaCatalogReader,
	failures gatewayFailureCertificateAuthority,
	observer gatewayReplicaHealthObserver,
	inventory gatewayReplicaCandidateInventory,
	sink rebalance.FailedReplicaMoveSink,
) (*gatewayReplicaHealthController, error) {
	if catalog == nil || failures == nil || observer == nil || inventory == nil || sink == nil {
		return nil, errGatewayReplicaHealth
	}
	return &gatewayReplicaHealthController{
		catalog: catalog, failures: failures, observer: observer, inventory: inventory,
		sink: sink, schedule: rebalance.ScheduleFailedReplicaReplacement,
	}, nil
}

func (controller *gatewayReplicaHealthController) RunPass(
	ctx context.Context,
) (gatewayReplicaHealthPass, error) {
	if controller == nil || ctx == nil || controller.catalog == nil || controller.failures == nil ||
		controller.observer == nil || controller.inventory == nil || controller.sink == nil ||
		controller.schedule == nil {
		return gatewayReplicaHealthPass{}, errGatewayReplicaHealth
	}
	catalog, err := controller.catalog.Read(ctx)
	if err != nil || catalog == nil {
		return gatewayReplicaHealthPass{}, errors.Join(err, errGatewayReplicaHealth)
	}
	var pass gatewayReplicaHealthPass
	var failures error
	err = controller.failures.VisitFailureCertificates(ctx, catalog,
		func(certificate rebalance.FailureQuorumCertificate) error {
			pass.Certificates++
			if err := ctx.Err(); err != nil {
				return err
			}
			observation, observeErr := controller.observer.ObserveReplicaHealth(
				ctx, catalog, certificate,
			)
			if observeErr != nil {
				failures = errors.Join(failures, observeErr)
				return nil
			}
			candidates, inventoryErr := controller.inventory.ReplacementCandidates(
				ctx, catalog, certificate,
			)
			if inventoryErr != nil {
				failures = errors.Join(failures, inventoryErr)
				return nil
			}
			cut := rebalance.FailedReplicaPlanningCut{
				Catalog: catalog, Publication: observation.Publication,
				Leader: observation.Leader, Certificate: certificate,
				Healthy: observation.Healthy, Candidates: candidates,
			}
			_, scheduleErr := controller.schedule(ctx, cut, controller.sink)
			if scheduleErr != nil {
				failures = errors.Join(failures, scheduleErr)
				return nil
			}
			pass.Eligible++
			pass.Submitted++
			return nil
		})
	return pass, errors.Join(failures, err)
}

type replicaHealthPassRunner interface {
	RunPass(context.Context) (gatewayReplicaHealthPass, error)
}

func runReplicaHealthController(
	ctx context.Context,
	controller replicaHealthPassRunner,
	interval time.Duration,
	logf func(string, ...any),
) {
	if ctx == nil || controller == nil || interval <= 0 || logf == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		pass, err := controller.RunPass(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logf("gateway: replica health controller: %v", err)
		} else if pass.Submitted != 0 {
			logf("gateway: replica health controller submitted %d/%d certified replacement(s)",
				pass.Submitted, pass.Certificates)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
