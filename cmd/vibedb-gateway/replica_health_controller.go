package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
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

type gatewayFailedReplicaGrantAuthority interface {
	Read(context.Context) (*gateway.Snapshot, error)
	PublishMembershipGrant(context.Context, membershipgrant.Grant) error
	ReadMembershipGrant(context.Context, raftmember.GroupKey) (membershipgrant.Grant, bool, error)
	RetryPending(context.Context) error
}

// gatewayFailedReplicaMoveSink prepares the exact pre-change membership grant
// before handing the authorized plan to the existing RF3 move controller. A
// crash anywhere in this sequence is replay-safe: grant publication and
// installation are exact/idempotent, while Controller.Submit owns the durable
// move journal and its idempotent action execution.
type gatewayFailedReplicaMoveSink struct {
	controller gatewayFailedReplicaMoveSubmitter
	grants     gatewayFailedReplicaGrantAuthority
	installer  gatewayMembershipGrantInstaller
}

func (sink gatewayFailedReplicaMoveSink) SubmitFailedReplicaMove(
	ctx context.Context, intent rebalance.FailedReplicaMoveIntent,
) error {
	if ctx == nil || sink.controller == nil || sink.grants == nil || sink.installer == nil ||
		intent.Plan == nil ||
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
	catalog, err := sink.grants.Read(ctx)
	if err != nil || catalog == nil || catalog.Generation() != intent.Plan.CatalogGeneration() {
		return errors.Join(err, errGatewayReplicaHealth)
	}
	transition, epoch := gatewayFailedReplicaGrantIdentity(intent)
	grant, err := gateway.BuildReplicaReplacementMembershipGrant(
		catalog, intent.Plan.Group(), transition, epoch,
		intent.Plan.RetiringMember(), intent.Plan.TargetMember(),
	)
	if err != nil {
		return errors.Join(err, errGatewayReplicaHealth)
	}
	err = sink.grants.PublishMembershipGrant(ctx, grant)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		if retryErr := sink.grants.RetryPending(ctx); retryErr != nil {
			return errors.Join(err, retryErr)
		}
		err = nil
	}
	if err != nil {
		return err
	}
	stored, found, err := sink.grants.ReadMembershipGrant(ctx, grant.Group)
	if err != nil || !found || stored != grant {
		return errors.Join(err, errGatewayReplicaHealth)
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, found := catalog.ResolveReplicatedMembershipRoute(
		identity.Request.Distribution, identity.Request.Shard, workspace[:0],
	)
	if !found || route.Serving.Group != grant.Group {
		return errGatewayReplicaHealth
	}
	if err = installGatewayMembershipGrant(ctx, route, grant, sink.installer); err != nil {
		return err
	}
	_, err = sink.controller.Submit(ctx, intent.Plan)
	return err
}

func gatewayFailedReplicaGrantIdentity(
	intent rebalance.FailedReplicaMoveIntent,
) ([16]byte, uint64) {
	hash := sha256.New()
	hash.Write([]byte("vibedb/gateway/failed-replica-membership-grant\x00"))
	hash.Write(intent.Operation[:])
	hash.Write(intent.Evidence[:])
	hash.Write(intent.Placement[:])
	var digest [sha256.Size]byte
	hash.Sum(digest[:0])
	var transition [16]byte
	copy(transition[:], digest[:16])
	epoch := binary.LittleEndian.Uint64(digest[16:24])
	if epoch == 0 {
		epoch = 1
	}
	return transition, epoch
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

// newGatewayReplicaHealthRuntime is the shipped composition seam used by
// runServe once the replicated health authority is configured. The manifest
// supplies inventory, the shard-control client supplies authenticated cuts,
// and the existing move controller persists/executes the resulting intent.
func newGatewayReplicaHealthRuntime(
	catalog gatewayReplicaCatalogReader,
	failures gatewayFailureCertificateAuthority,
	observations gatewayReplicaObservationClient,
	inventory gatewayReplicaCandidateInventory,
	moves gatewayFailedReplicaMoveSubmitter,
	grants gatewayFailedReplicaGrantAuthority,
	installer gatewayMembershipGrantInstaller,
) (*gatewayReplicaHealthController, error) {
	if observations == nil || moves == nil || grants == nil || installer == nil {
		return nil, errGatewayReplicaHealth
	}
	return newGatewayReplicaHealthController(
		catalog, failures, gatewayAuthenticatedHealthObserver{client: observations}, inventory,
		gatewayFailedReplicaMoveSink{controller: moves, grants: grants, installer: installer},
	)
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

type replicaHealthRevisionPassRunner interface {
	RunPass(context.Context) (gatewayReplicaHealthRevisionPass, error)
}

// startGatewayReplicaControllers runs the evidence publisher, certificate
// consumer, and durable move resumer in that order for every interval. This
// makes a newly completed replicated confirmation window visible to scheduling
// in the same pass while bounding health observation to one RF3 at a time. The
// returned channel closes only after cancellation is observed.
func startGatewayReplicaControllers(
	ctx context.Context,
	revisions replicaHealthRevisionPassRunner,
	moves replicaMovePassRunner,
	health replicaHealthPassRunner,
	interval time.Duration,
	logf func(string, ...any),
) (<-chan struct{}, error) {
	if ctx == nil || revisions == nil || moves == nil || health == nil || interval <= 0 || logf == nil {
		return nil, errGatewayReplicaHealth
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			revisionPass, revisionErr := revisions.RunPass(ctx)
			if revisionErr != nil && !errors.Is(revisionErr, context.Canceled) {
				logf("gateway: replica health revision controller: %v", revisionErr)
			} else if revisionPass.Published != 0 {
				logf("gateway: replica health revision controller published %d/%d revision(s)",
					revisionPass.Published, revisionPass.Groups)
			}
			healthPass, healthErr := health.RunPass(ctx)
			if healthErr != nil && !errors.Is(healthErr, context.Canceled) {
				logf("gateway: replica health controller: %v", healthErr)
			} else if healthPass.Submitted != 0 {
				logf("gateway: replica health controller submitted %d/%d certified replacement(s)",
					healthPass.Submitted, healthPass.Certificates)
			}
			movePass, moveErr := moves.RunPass(ctx)
			if moveErr != nil && !errors.Is(moveErr, context.Canceled) {
				logf("gateway: replica move controller: %v", moveErr)
			} else if movePass.Advanced != 0 || movePass.Completed != 0 || movePass.AbandonmentDeleted != 0 {
				logf("gateway: replica move controller advanced %d/%d move(s), completed %d; abandoned %d/%d witnessed (%d scanned, %d bytes)",
					movePass.Advanced, movePass.Moves, movePass.Completed, movePass.AbandonmentDeleted,
					movePass.AbandonmentWitnessed, movePass.AbandonmentScanned, movePass.AbandonmentBytes)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return done, nil
}
