package gatewayruntime

// This file contains the online physical-node scaling reconciler.  The
// reconciler is deliberately a catalog reader/writer: all progress is
// represented by ScalingIntent and GroupEnrollmentIntent rows and every
// process restart starts from those rows again.  The move implementation is
// still the existing rebalanceexec saga; this controller only admits a move
// after a fresh placement cut and never invents a second membership protocol.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/scaling"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	ErrScalingControllerConfig  = errors.New("gatewayruntime: invalid scaling controller configuration")
	ErrScalingControllerBlocked = errors.New("gatewayruntime: scaling operation is blocked")
)

// scalingCatalogReader is intentionally smaller than ReplicatedCatalogAuthority
// so the reconciler can be tested against a linearizable catalog adapter.  In
// production Runtime always supplies the authority itself.
type scalingCatalogReader interface {
	Read(context.Context) (*gateway.Snapshot, error)
}

// scalingCapacityReader is the client side of the authenticated capacity
// protocol.  A missing reader is a hard blocker; zero demand is never used as
// a substitute for a cold or unavailable replica observation.
type scalingCapacityReader interface {
	Observe(context.Context, rafttransport.NodeID, replicacontrol.CapacityRequest) (replicacontrol.CapacityObservation, error)
}

// ScalingNodeReadiness verifies that an empty physical target has completed
// its local identity/schema/storage reservation.  Implementations must inspect
// the target's durable state through authenticated control; returning nil is
// the only point at which Joining can become Active.
type ScalingNodeReadiness interface {
	VerifyNode(context.Context, gateway.NodeRecord) (gateway.NodeRecord, error)
}

// ScalingEnrollmentBuilder supplies the immutable public preparation material
// for one group. It is intentionally required at admission time: the
// controller cannot derive a target member, store identity, or schema digest
// from a route alone without risking a side effect against the wrong source.
// A node-control deployment supplies the shared preparation-spec builder.
type ScalingEnrollmentBuilder interface {
	BuildEnrollment(context.Context, gateway.ScalingIntent, scaling.ReplicaMove, gateway.ReplicatedMembershipRoute, gateway.NodeRecord) (gateway.GroupEnrollmentIntent, error)
}

// ScalingControllerOptions wires concrete replicated authorities and the
// already-shipped rebalance controller.  Optional dependencies fail closed by
// retaining a bounded blocker in the durable intent; they never turn missing
// capacity, readiness, or enrollment evidence into an inferred success.
type ScalingControllerOptions struct {
	Directory   gateway.DirectoryReader
	Writer      gateway.DirectoryWriter
	Catalog     scalingCatalogReader
	Moves       *rebalanceexec.Controller
	Provisioner gateway.NodeProvisioner
	Capacity    scalingCapacityReader
	Observation gatewayReplicaObservationClient
	Readiness   ScalingNodeReadiness
	Enrollment  ScalingEnrollmentBuilder
	Interval    time.Duration
	Logf        func(string, ...any)
}

// ScalingController is safe to run from one goroutine.  The mutex protects
// only the pass guard; durable revisions remain the source of truth and make
// concurrent controller instances harmless (one loses the catalog CAS).
type ScalingController struct {
	directory   gateway.DirectoryReader
	writer      gateway.DirectoryWriter
	catalog     scalingCatalogReader
	moves       *rebalanceexec.Controller
	provision   gateway.NodeProvisioner
	capacity    scalingCapacityReader
	observation gatewayReplicaObservationClient
	readiness   ScalingNodeReadiness
	enrollment  ScalingEnrollmentBuilder
	interval    time.Duration
	logf        func(string, ...any)
	passMu      sync.Mutex
}

type ScalingControllerPass struct {
	Discovered uint32
	Advanced   uint32
	Completed  uint32
	Moves      uint32
	Blocked    uint32
}

func NewScalingController(options ScalingControllerOptions) (*ScalingController, error) {
	if options.Directory == nil || options.Writer == nil || options.Catalog == nil {
		return nil, ErrScalingControllerConfig
	}
	// These two authorities are part of the enrollment safety boundary.  A
	// controller that can only append a generic Reserved/Prepared row would
	// strand physical reservations and could never prove the catalog cut that
	// authorizes the learner.  Fail at construction so a shipped runtime cannot
	// quietly run with an implementation that can only make progress up to a
	// local side effect.
	if _, ok := options.Writer.(gateway.EnrollmentPreparationClaimer); !ok {
		return nil, fmt.Errorf("%w: enrollment preparation claimer is unavailable", ErrScalingControllerConfig)
	}
	if _, ok := options.Writer.(gateway.EnrollmentReceiptPublisher); !ok {
		return nil, fmt.Errorf("%w: enrollment receipt publisher is unavailable", ErrScalingControllerConfig)
	}
	if options.Interval <= 0 {
		options.Interval = time.Second
	}
	if options.Logf == nil {
		options.Logf = func(string, ...any) {}
	}
	return &ScalingController{directory: options.Directory, writer: options.Writer,
		catalog: options.Catalog, moves: options.Moves, provision: options.Provisioner,
		capacity: options.Capacity, observation: options.Observation, readiness: options.Readiness,
		enrollment: options.Enrollment, interval: options.Interval, logf: options.Logf}, nil
}

// Run starts the bounded, restart-safe reconciliation loop.  Context
// cancellation stops observation only; no durable intent is cancelled.
func (controller *ScalingController) Run(ctx context.Context) {
	if controller == nil || ctx == nil {
		return
	}
	ticker := time.NewTicker(controller.interval)
	defer ticker.Stop()
	for {
		_, err := controller.RunPass(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			controller.logf("gateway: scaling controller: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunPass rediscovers every unfinished scaling intent and advances each one
// through at most one durable boundary.  The complete route inventory is
// planned in one pass, so catalog, internal, request-ledger, and application
// groups are treated identically.
func (controller *ScalingController) RunPass(ctx context.Context) (ScalingControllerPass, error) {
	if controller == nil || ctx == nil {
		return ScalingControllerPass{}, ErrScalingControllerConfig
	}
	controller.passMu.Lock()
	defer controller.passMu.Unlock()
	intents, err := controller.directory.ListScalingIntents(ctx)
	if err != nil {
		return ScalingControllerPass{}, err
	}
	pass := ScalingControllerPass{Discovered: uint32(len(intents))}
	var failures error
	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			return pass, errors.Join(failures, err)
		}
		if intent.State >= gateway.ScalingComplete {
			continue
		}
		advanced, completed, moves, blocked, stepErr := controller.advance(ctx, intent)
		if advanced {
			pass.Advanced++
		}
		if completed {
			pass.Completed++
		}
		pass.Moves += uint32(moves)
		if blocked {
			pass.Blocked++
		}
		failures = errors.Join(failures, stepErr)
	}
	if controller.moves != nil {
		_, moveErr := controller.moves.RunPass(ctx)
		failures = errors.Join(failures, moveErr)
	}
	return pass, failures
}

func (controller *ScalingController) advance(ctx context.Context, intent gateway.ScalingIntent) (advanced, completed bool, moves int, blocked bool, err error) {
	if intent.State == gateway.ScalingReserved {
		next := intent
		next.State = gateway.ScalingRunning
		next.Revision++
		next.DirectoryRevision = next.Revision
		if err = controller.writer.PutScalingIntent(ctx, next, intent.Revision); err != nil {
			return false, false, 0, false, err
		}
		intent = next
		advanced = true
	}
	if intent.State != gateway.ScalingRunning {
		return advanced, false, 0, false, nil
	}
	if intent.Request.Kind == gateway.ScalingScaleOut {
		if promoted, promoteErr := controller.activateJoiningTargets(ctx, intent); promoteErr != nil {
			return advanced, false, 0, true, promoteErr
		} else if promoted {
			advanced = true
		}
	}

	resumed, resumeErr := controller.resumeEnrollments(ctx, intent)
	if resumeErr != nil {
		recordErr := controller.recordBlocker(ctx, intent, scalingBlockerFromError(resumeErr))
		return advanced, false, 0, true, errors.Join(resumeErr, recordErr)
	}
	if synchronized, syncErr := controller.reconcileIntentProgress(ctx, intent); syncErr != nil {
		return advanced, false, 0, true, syncErr
	} else if synchronized != nil {
		intent = *synchronized
		advanced = true
	}
	if resumed {
		// A durable enrollment is a live reservation. Re-open it before asking
		// the planner for another target; otherwise every restart would create a
		// second reservation or strand Prepared state behind InFlight.
		advanced = true
		return advanced, false, 0, false, nil
	}

	// Decommission has a strict terminal proof path. It is checked before
	// planning so a restart after the final move can retire without another
	// speculative reservation.
	if intent.Request.Kind == gateway.ScalingDecommission {
		if done, retireErr := controller.reconcileRetirement(ctx, intent); retireErr != nil {
			return advanced, false, 0, true, retireErr
		} else if done {
			return true, true, 0, false, nil
		}
		// The retirement scan may have persisted fresh evidence or blockers.
		// Never admit a move against the pre-scan revision.
		refreshed, readErr := controller.directory.ReadScalingIntent(ctx, intent.ID)
		if readErr != nil {
			return advanced, false, 0, true, readErr
		}
		intent = refreshed
	}

	plan, planErr := controller.plan(ctx, intent)
	if planErr != nil {
		if recordErr := controller.recordBlocker(ctx, intent, scalingBlockerFromError(planErr)); recordErr != nil {
			return advanced, false, 0, true, recordErr
		}
		return advanced, false, 0, true, nil
	}
	if plan.State == scaling.PlacementBlocked {
		blocked = true
		if recordErr := controller.recordPlacementBlockers(ctx, intent, plan.Blockers); recordErr != nil {
			return advanced, false, 0, true, recordErr
		}
		return advanced, false, 0, true, nil
	}
	if len(plan.Moves) == 0 {
		if intent.Request.Kind == gateway.ScalingDecommission {
			if recordErr := controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{Code: "evacuation_pending", Detail: "decommission references remain after this placement cut"}); recordErr != nil {
				return advanced, false, 0, true, recordErr
			}
			return advanced, false, 0, true, nil
		}
		_, err = controller.completeIntent(ctx, intent, nil)
		if err != nil {
			return advanced, false, 0, false, err
		}
		return true, true, 0, false, nil
	}

	// A node-control provisioner is mandatory before a move can be admitted.
	// This is a durable blocker rather than a process-local retry loop.
	if controller.provision == nil {
		blocked = true
		if recordErr := controller.recordBlocker(ctx, intent,
			gateway.ScalingBlocker{Code: "provisioner_unavailable", Detail: "authenticated physical node provisioner is not configured"}); recordErr != nil {
			return advanced, false, 0, true, recordErr
		}
		return advanced, false, 0, true, nil
	}
	if err = controller.admitMoves(ctx, intent, plan); err != nil {
		recordErr := controller.recordBlocker(ctx, intent, scalingBlockerFromError(err))
		return advanced, false, len(plan.Moves), true, errors.Join(err, recordErr)
	}
	return true, false, len(plan.Moves), false, nil
}

// resumeEnrollments settles the group-scoped physical preparation state that
// belongs to one scaling intent.  Ownership is proved by the deterministic
// intent ID; no process-local queue is consulted.  The exact row is reread
// immediately before every side effect so a CAS conflict cannot authorize a
// stale Prepare or Adopt call.
func (controller *ScalingController) resumeEnrollments(ctx context.Context, parent gateway.ScalingIntent) (bool, error) {
	snapshot, err := controller.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return false, errors.Join(err, errors.New("catalog snapshot unavailable while resuming enrollment"))
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	seen := make(map[raftmember.GroupKey]struct{}, snapshot.ReplicatedRouteCount())
	progressed := false
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return progressed, errors.New("catalog route inventory changed while resuming enrollment")
		}
		if _, found := seen[route.Group]; found {
			continue
		}
		seen[route.Group] = struct{}{}
		rows, listErr := controller.directory.ListEnrollmentIntents(ctx, route.Group)
		if listErr != nil {
			return progressed, listErr
		}
		for _, row := range rows {
			if row.State >= gateway.EnrollmentComplete || scalingEnrollmentID(parent.ID, row) != row.IntentID {
				continue
			}
			current, readErr := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
			if readErr != nil {
				return progressed, readErr
			}
			if current.Digest() != row.Digest() || current.State >= gateway.EnrollmentComplete {
				continue
			}
			switched, stepErr := controller.resumeEnrollment(ctx, current)
			if stepErr != nil {
				return progressed, stepErr
			}
			progressed = progressed || switched
		}
	}
	return progressed, nil
}

// reconcileIntentProgress projects durable group enrollment rows back onto
// their parent operation. The parent is the operator status boundary, while
// each enrollment row is the side-effect boundary; rebuilding this projection
// on every pass makes a crash between either write resumable.
func (controller *ScalingController) reconcileIntentProgress(ctx context.Context, parent gateway.ScalingIntent) (*gateway.ScalingIntent, error) {
	snapshot, err := controller.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return nil, errors.Join(err, errors.New("catalog snapshot unavailable while reconciling scaling progress"))
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	seen := make(map[raftmember.GroupKey]struct{}, snapshot.ReplicatedRouteCount())
	var planned, completed uint32
	var outstanding [][32]byte
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return nil, errors.New("catalog route inventory changed while reconciling scaling progress")
		}
		if _, found := seen[route.Group]; found {
			continue
		}
		seen[route.Group] = struct{}{}
		rows, listErr := controller.directory.ListEnrollmentIntents(ctx, route.Group)
		if listErr != nil {
			return nil, listErr
		}
		for _, row := range rows {
			if row.State < gateway.EnrollmentReserved || row.State == gateway.EnrollmentCancelled ||
				scalingEnrollmentID(parent.ID, row) != row.IntentID {
				continue
			}
			if planned >= gateway.MaxScalingMovesPerIntent {
				return nil, errors.New("scaling enrollment inventory exceeds the parent move bound")
			}
			planned++
			if row.State >= gateway.EnrollmentComplete {
				completed++
				continue
			}
			if row.MoveOperationID != ([32]byte{}) {
				outstanding = append(outstanding, row.MoveOperationID)
			}
		}
	}
	slices.SortFunc(outstanding, func(left, right [32]byte) int {
		for index := range left {
			if left[index] < right[index] {
				return -1
			}
			if left[index] > right[index] {
				return 1
			}
		}
		return 0
	})
	if parent.PlannedReplicas == planned && parent.CompletedReplicas == completed &&
		slices.Equal(parent.OutstandingMoves, outstanding) {
		return nil, nil
	}
	next := parent
	next.PlannedReplicas, next.CompletedReplicas = planned, completed
	next.OutstandingMoves = slices.Clone(outstanding)
	next.Revision++
	next.DirectoryRevision = next.Revision
	if err := controller.writer.PutScalingIntent(ctx, next, parent.Revision); err != nil {
		return nil, err
	}
	return &next, nil
}

func (controller *ScalingController) resumeEnrollment(ctx context.Context, row gateway.GroupEnrollmentIntent) (bool, error) {
	switch row.State {
	case gateway.EnrollmentReserved:
		if controller.provision == nil {
			return false, nil
		}
		if row.PreparationClaim == ([32]byte{}) {
			claimer, ok := controller.writer.(gateway.EnrollmentPreparationClaimer)
			if !ok {
				return false, errors.New("durable enrollment preparation claim is unavailable")
			}
			_, claimErr := claimer.ClaimEnrollmentPreparation(ctx, row.IntentID, row.Revision)
			if claimErr != nil {
				return false, claimErr
			}
			return true, nil
		}
		proof, err := controller.provision.PrepareReplica(ctx, row)
		if err != nil {
			return false, err
		}
		current, err := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
		if err != nil || current.Digest() != row.Digest() || current.State != gateway.EnrollmentReserved ||
			current.PreparationClaim != row.PreparationClaim {
			return false, errors.Join(err, gateway.ErrScalingRevision)
		}
		next := current
		next.State = gateway.EnrollmentPrepared
		next.Revision++
		next.PreparationClaim = [32]byte{}
		next.Proof = &proof
		if err = controller.writer.PutEnrollmentIntent(ctx, next, current.Revision); err != nil {
			return false, err
		}
		return true, nil
	case gateway.EnrollmentPrepared:
		if row.Proof == nil {
			return false, gateway.ErrInvalidScalingMetadata
		}
		current, err := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
		if err != nil {
			return false, err
		}
		if current.Digest() != row.Digest() || current.State != gateway.EnrollmentPrepared || current.Proof == nil {
			return false, gateway.ErrScalingRevision
		}
		row = current
		receipter, ok := controller.writer.(gateway.EnrollmentReceiptPublisher)
		if !ok {
			return false, nil
		}
		enrolled, err := receipter.PublishEnrollmentReceipt(ctx, row)
		if err != nil {
			return false, err
		}
		if !enrolled.Valid() || enrolled.State != gateway.EnrollmentEnrolled || enrolled.Receipt == nil {
			return false, errors.New("invalid certified enrollment receipt")
		}
		return true, nil
	case gateway.EnrollmentEnrolled:
		if row.Proof == nil || controller.provision == nil {
			return false, nil
		}
		current, err := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
		if err != nil {
			return false, err
		}
		if current.Digest() != row.Digest() || current.State != gateway.EnrollmentEnrolled || current.Proof == nil || current.Receipt == nil {
			return false, gateway.ErrScalingRevision
		}
		row = current
		if err := controller.provision.EnrollReplica(ctx, row, *row.Proof); err != nil {
			return false, err
		}
		if row.MoveOperationID != ([32]byte{}) {
			return false, nil
		}
		return controller.submitEnrollmentMove(ctx, row)
	case gateway.EnrollmentMoving:
		if row.MoveOperationID == ([32]byte{}) {
			return false, gateway.ErrInvalidScalingMetadata
		}
		reader, ok := controller.directory.(interface {
			ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
		})
		if !ok {
			return false, nil
		}
		record, err := reader.ReadOperation(ctx, row.MoveOperationID)
		if err != nil || record.State != gateway.ReplicatedOperationComplete {
			return false, err
		}
		current, err := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
		if err != nil || current.State != gateway.EnrollmentMoving || current.MoveOperationID != row.MoveOperationID {
			return false, errors.Join(err, gateway.ErrScalingRevision)
		}
		next := current
		next.State = gateway.EnrollmentComplete
		next.Revision++
		if err = controller.writer.PutEnrollmentIntent(ctx, next, current.Revision); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (controller *ScalingController) submitEnrollmentMove(ctx context.Context, row gateway.GroupEnrollmentIntent) (bool, error) {
	if controller.moves == nil || controller.observation == nil {
		return false, nil
	}
	snapshot, err := controller.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return false, errors.Join(err, errors.New("catalog snapshot unavailable while submitting enrolled move"))
	}
	membership, found := snapshot.ResolveReplicatedMembershipRoute(row.Distribution, row.Shard, nil)
	if !found || !membership.HasEnrolledTarget || membership.EnrolledTarget.Member != row.Target.Member ||
		row.Receipt == nil || !row.Receipt.Valid() || row.Receipt.IntentID != row.IntentID ||
		row.Receipt.Target != row.Target || row.Receipt.EnrolledCatalogGeneration != snapshot.Generation() ||
		row.Receipt.EnrolledCatalogHeadDigest == (replication.Digest{}) {
		return false, errors.New("enrollment receipt is not present in the current catalog cut")
	}
	if membership.EnrolledTarget.Node != row.Target.Node ||
		membership.EnrolledTarget.NodeIncarnation != row.Target.NodeIncarnation ||
		membership.EnrolledTarget.StoreID != row.Target.StoreID {
		return false, errors.New("catalog enrolled target does not match the certified receipt")
	}
	request := replicacontrol.Request{Operation: row.IntentID,
		Step: scalingCapacityStep(row.IntentID, row.Group, row.ReplicaOrdinal), Group: row.Group, TargetMember: row.Target.Member}
	var leader replicacontrol.Observation
	var leaderFound bool
	var observedErr error
	for _, endpoint := range membership.Serving.Replicas {
		candidate, observeErr := controller.observation.Observe(ctx, endpoint.Node, request)
		if observeErr != nil {
			observedErr = errors.Join(observedErr, observeErr)
			continue
		}
		if candidate.Status.MemberID != endpoint.Member || candidate.Status.LeaderID != endpoint.Member ||
			candidate.Status.Term == 0 || candidate.Publication.Applied == 0 || candidate.Publication.ReplicaSetVersion == 0 {
			continue
		}
		leader, leaderFound = candidate, true
		break
	}
	if !leaderFound {
		return false, errors.Join(observedErr, errors.New("no authenticated current leader observation for enrolled move"))
	}
	moveRequest := rebalance.MoveRequest{Distribution: row.Distribution, Shard: row.Shard, Group: row.Group,
		RetiringMember: row.Source.Member, SnapshotSourceMember: row.SnapshotSourceMember,
		TargetMember: row.Target.Member, Source: row.Source.Endpoint, Target: row.Target.Endpoint,
		RetiringReplica: rebalance.ReplicaIdentity{Member: row.Source.Member, Node: row.Source.Node,
			StoreID: row.Source.StoreID, NodeIncarnation: row.Source.NodeIncarnation, ControlEndpoint: row.Source.ControlEndpoint}}
	plan, err := rebalance.PlanReplicaMove(snapshot, leader.Publication, moveRequest)
	if err != nil {
		return false, err
	}
	if _, err = controller.moves.Submit(ctx, plan); err != nil {
		return false, err
	}
	current, err := controller.directory.ReadEnrollmentIntent(ctx, row.IntentID)
	if err != nil || current.State != gateway.EnrollmentEnrolled || current.Digest() != row.Digest() {
		return false, errors.Join(err, gateway.ErrScalingRevision)
	}
	next := current
	next.State = gateway.EnrollmentMoving
	next.Revision++
	next.MoveOperationID = [32]byte(plan.OperationID())
	if err = controller.writer.PutEnrollmentIntent(ctx, next, current.Revision); err != nil {
		return false, err
	}
	return true, nil
}

func (controller *ScalingController) activateJoiningTargets(ctx context.Context, intent gateway.ScalingIntent) (bool, error) {
	changed := false
	for _, target := range intent.Request.Targets {
		node, err := controller.directory.ReadNode(ctx, target.NodeID, target.Incarnation)
		if errors.Is(err, gateway.ErrScalingNodeMissing) {
			continue
		}
		if err != nil {
			return changed, err
		}
		if node.Lifecycle != gateway.NodeJoining {
			continue
		}
		if controller.readiness == nil {
			return changed, controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{
				Code: "target_not_verified", Detail: "empty physical target has no authenticated readiness verifier", Node: node.NodeID, Revision: node.Revision})
		}
		verified, verifyErr := controller.readiness.VerifyNode(ctx, node)
		if verifyErr != nil {
			return changed, controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{
				Code: "target_not_ready", Detail: boundedScalingError(verifyErr), Node: node.NodeID, Revision: node.Revision})
		}
		next := verified
		next.Lifecycle = gateway.NodeActive
		next.Revision++
		if snapshot, readErr := controller.catalog.Read(ctx); readErr == nil && snapshot != nil {
			next.CatalogGeneration = snapshot.Generation()
		}
		if err = controller.writer.PutNode(ctx, next, node.Revision); err != nil {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

func (controller *ScalingController) reconcileRetirement(ctx context.Context, intent gateway.ScalingIntent) (bool, error) {
	node, err := controller.directory.ReadNode(ctx, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
	if err != nil {
		return false, err
	}
	if node.Lifecycle == gateway.NodeDecommissioned {
		evidence, scanErr := controller.directory.ScanNodeReferences(ctx, node.NodeID, node.Incarnation)
		if scanErr != nil {
			return false, scanErr
		}
		if !evidence.ZeroAllReferences() || evidence.Digest == (replication.Digest{}) {
			priorEvidence := intent.Evidence
			intent.Evidence = scalingEvidenceFromNode(evidence)
			return false, controller.recordRetirementScan(ctx, intent, priorEvidence, blockersFromEvidence(evidence))
		}
		intent.Evidence = scalingEvidenceFromNode(evidence)
		intent.Evidence.DrainAcknowledged = true
		intent.Evidence.RetiredAcknowledged = true
		intent.Evidence.CatalogControlMigrated = true
		return controller.completeIntent(ctx, intent, &node)
	}
	if node.Lifecycle == gateway.NodeActive {
		// A retry after an interrupted decommission admission may still find the
		// node Active.  The intent is already durable, so the transition is a
		// fenced CAS and does not create a second saga.
		next := node
		next.Lifecycle = gateway.NodeDraining
		next.Revision++
		if err = controller.writer.PutNode(ctx, next, node.Revision); err != nil {
			return false, err
		}
		node = next
	}
	if node.Lifecycle != gateway.NodeDraining {
		return false, controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{
			Code: "invalid_drain_lifecycle", Detail: "decommission requires an Active or Draining node", Node: node.NodeID, Revision: node.Revision})
	}
	evidence, scanErr := controller.directory.ScanNodeReferences(ctx, node.NodeID, node.Incarnation)
	if scanErr != nil {
		return false, scanErr
	}
	priorEvidence := intent.Evidence
	intent.Evidence = scalingEvidenceFromNode(evidence)
	if !evidence.ZeroAllReferences() || evidence.Digest == (replication.Digest{}) {
		blockers := blockersFromEvidence(evidence)
		return false, controller.recordRetirementScan(ctx, intent, priorEvidence, blockers)
	}
	retirer, ok := controller.directory.(interface {
		RetireNode(context.Context, rafttransport.NodeID, uint64, uint64, gateway.NodeReferenceEvidence) error
	})
	if !ok {
		return false, controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{Code: "retirement_unavailable", Detail: "catalog does not expose the fenced retirement authority", Node: node.NodeID, Revision: node.Revision})
	}
	if err = retirer.RetireNode(ctx, node.NodeID, node.Incarnation, node.Revision, evidence); err != nil {
		return false, err
	}
	node.Lifecycle = gateway.NodeDecommissioned
	intent.Evidence.DrainAcknowledged = true
	intent.Evidence.RetiredAcknowledged = true
	intent.Evidence.CatalogControlMigrated = true
	return controller.completeIntent(ctx, intent, &node)
}

func (controller *ScalingController) recordRetirementScan(ctx context.Context, intent gateway.ScalingIntent, prior gateway.SafeToStopEvidence, blockers []gateway.ScalingBlocker) error {
	next := intent
	next.Blockers = slices.Clone(blockers)
	if next.Evidence == prior && sameScalingBlockers(next.Blockers, intent.Blockers) {
		return nil
	}
	next.Revision++
	next.DirectoryRevision = next.Revision
	return controller.writer.PutScalingIntent(ctx, next, intent.Revision)
}

func (controller *ScalingController) completeIntent(ctx context.Context, intent gateway.ScalingIntent, node *gateway.NodeRecord) (bool, error) {
	next := intent
	next.State = gateway.ScalingComplete
	next.Blockers = nil
	next.OutstandingMoves = nil
	if intent.Request.Kind == gateway.ScalingScaleIn || intent.Request.Kind == gateway.ScalingDecommission {
		// Terminal metadata validation requires the authority's complete,
		// current reference cut. Re-scan here even when the planner returned no
		// moves; a concurrent catalog publication must turn completion into a
		// retry rather than a false safe-to-stop result.
		reference, err := controller.directory.ScanNodeReferences(ctx, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
		if err != nil {
			return false, err
		}
		next.Evidence = gateway.SafeToStopEvidenceFromReference(reference)
		next.Evidence.DrainAcknowledged = reference.ZeroDataReferences()
		next.Evidence.CatalogControlMigrated = reference.CatalogVoterReferences == 0 && reference.ControlVoterReferences == 0
		if intent.Request.Kind == gateway.ScalingDecommission && node != nil && node.Lifecycle == gateway.NodeDecommissioned {
			next.Evidence.RetiredAcknowledged = true
		}
	}
	next.Revision++
	next.DirectoryRevision = next.Revision
	if node != nil {
		if node.Lifecycle != gateway.NodeDecommissioned || !next.Evidence.SafeToStop() {
			return false, ErrScalingControllerBlocked
		}
		next.Evidence.NodeID = node.NodeID
		next.Evidence.NodeIncarnation = node.Incarnation
	}
	if err := controller.writer.PutScalingIntent(ctx, next, intent.Revision); err != nil {
		return false, err
	}
	return true, nil
}

func (controller *ScalingController) plan(ctx context.Context, intent gateway.ScalingIntent) (scaling.PlacementPlan, error) {
	snapshot, err := controller.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return scaling.PlacementPlan{}, errors.Join(err, errors.New("catalog snapshot unavailable"))
	}
	catalogDigest, digestErr := gateway.CatalogSnapshotDigest(snapshot)
	if digestErr != nil {
		return scaling.PlacementPlan{}, digestErr
	}
	var nodes []gateway.NodeRecord
	var initialDirectory gateway.NodeDirectoryCut
	if reader, ok := controller.directory.(gateway.NodeDirectoryCutReader); ok {
		initialDirectory, err = reader.ReadNodeDirectoryCut(ctx)
		if err != nil {
			return scaling.PlacementPlan{}, err
		}
		nodes, err = scalingPlacementNodes(initialDirectory, snapshot.Generation())
		if err != nil {
			return scaling.PlacementPlan{}, err
		}
	} else {
		nodes, err = controller.directory.ListNodes(ctx)
		if err != nil {
			return scaling.PlacementPlan{}, err
		}
	}
	if controller.capacity == nil {
		return scaling.PlacementPlan{}, errors.New("authenticated capacity observer is not configured")
	}
	demands, groups, err := controller.collectCapacity(ctx, intent, snapshot, nodes)
	if err != nil {
		return scaling.PlacementPlan{}, err
	}
	var inflight []gateway.GroupEnrollmentIntent
	seen := make(map[raftmember.GroupKey]struct{}, len(groups))
	for _, group := range groups {
		if _, found := seen[group]; found {
			continue
		}
		seen[group] = struct{}{}
		rows, listErr := controller.directory.ListEnrollmentIntents(ctx, group)
		if listErr != nil {
			return scaling.PlacementPlan{}, listErr
		}
		inflight = append(inflight, rows...)
	}
	plan, err := scaling.Plan(scaling.PlacementInput{Snapshot: snapshot, Nodes: nodes,
		Request: intent.Request, Demands: demands, InFlight: inflight,
		Policy: scaling.DefaultPlacementPolicy()})
	if err != nil {
		return scaling.PlacementPlan{}, err
	}
	// Capacity observation is a detached physical cut. Re-read both replicated
	// authorities before admitting any move so a node lifecycle or catalog
	// publication racing the scan becomes a bounded retry instead of a stale
	// target reservation.
	latestSnapshot, readErr := controller.catalog.Read(ctx)
	if readErr != nil || latestSnapshot == nil {
		return scaling.PlacementPlan{}, errors.Join(readErr, gateway.ErrScalingRevision)
	}
	latestDigest, digestErr := gateway.CatalogSnapshotDigest(latestSnapshot)
	if digestErr != nil || latestSnapshot.Generation() != snapshot.Generation() || latestDigest != catalogDigest {
		return scaling.PlacementPlan{}, errors.Join(digestErr, gateway.ErrScalingRevision)
	}
	if reader, ok := controller.directory.(gateway.NodeDirectoryCutReader); ok {
		latestDirectory, readErr := reader.ReadNodeDirectoryCut(ctx)
		if readErr != nil || latestDirectory.Revision != initialDirectory.Revision || latestDirectory.Digest != initialDirectory.Digest {
			return scaling.PlacementPlan{}, errors.Join(readErr, gateway.ErrScalingRevision)
		}
	}
	return plan, nil
}

func (controller *ScalingController) collectCapacity(ctx context.Context, intent gateway.ScalingIntent, snapshot *gateway.Snapshot, nodes []gateway.NodeRecord) ([]scaling.ReplicaDemand, []raftmember.GroupKey, error) {
	var round [32]byte
	if _, err := rand.Read(round[:]); err != nil {
		return nil, nil, err
	}
	nodeByID := make(map[rafttransport.NodeID]int, len(nodes))
	for index := range nodes {
		nodeByID[nodes[index].NodeID] = index
	}
	observedNodes := make(map[rafttransport.NodeID]replicacontrol.NodeCapacity, len(nodes))
	demands := make([]scaling.ReplicaDemand, 0, snapshot.ReplicatedRouteCount()*gateway.ServingReplicaCount)
	groups := make([]raftmember.GroupKey, 0, snapshot.ReplicatedRouteCount())
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return nil, nil, errors.New("catalog route inventory changed during capacity scan")
		}
		groups = append(groups, route.Group)
		for ordinal, replica := range route.Replicas {
			nodeIndex, found := nodeByID[replica.Node]
			if !found {
				return nil, nil, fmt.Errorf("capacity node identity missing for %x", replica.Node)
			}
			request := replicacontrol.CapacityRequest{Operation: intent.ID, Round: round,
				Step: scalingCapacityStep(intent.ID, route.Group, uint8(ordinal)), Group: route.Group,
				TargetMember: replica.Member, ExpectedCatalogGeneration: snapshot.Generation(),
				MinimumApplied: 1}
			observation, observeErr := controller.capacity.Observe(ctx, replica.Node, request)
			if observeErr != nil {
				return nil, nil, fmt.Errorf("capacity %x group %x replica %d: %w", replica.Node, route.Group.GroupID, ordinal, observeErr)
			}
			if observation.CatalogGeneration != snapshot.Generation() || observation.Request != request ||
				observation.Identity.Group != route.Group || observation.Identity.MemberID != replica.Member ||
				observation.Identity.StoreID != replica.StoreID || observation.Identity.AllocationGeneration != route.AllocationGeneration ||
				observation.Identity.NodeIncarnation < replica.NodeIncarnation || observation.Node.NodeID != replica.Node ||
				observation.Node.NodeIncarnation != nodes[nodeIndex].Incarnation || observation.Applied == 0 || observation.SourceRevision == 0 ||
				observation.Node.Revision == 0 {
				return nil, nil, errors.New("capacity observation crossed catalog or node revision fence")
			}
			if prior, seen := observedNodes[replica.Node]; seen && prior != observation.Node {
				return nil, nil, errors.New("capacity observations for one node do not share a physical cut")
			}
			observedNodes[replica.Node] = observation.Node
			// Keep the durable lifecycle/revision untouched. The detached physical
			// report updates only the live capacity dimensions consumed by placement.
			nodes[nodeIndex].Capacity = observation.Node.Capacity
			nodes[nodeIndex].Used = observation.Node.Used
			nodes[nodeIndex].MigrationCapacity = observation.Node.MigrationCapacity
			nodes[nodeIndex].MigrationUsed = observation.Node.MigrationUsed
			nodes[nodeIndex].MaxReceives = observation.Node.MaxReceives
			nodes[nodeIndex].ActiveReceives = observation.Node.ActiveReceives
			demands = append(demands, scaling.ReplicaDemand{CatalogGeneration: snapshot.Generation(),
				Group: route.Group, ReplicaOrdinal: uint8(ordinal), Demand: observation.Demand,
				MigrationBytes: observation.MigrationBytes, KnownEmpty: observation.KnownEmpty})
		}
	}
	return demands, groups, nil
}

func (controller *ScalingController) admitMoves(ctx context.Context, intent gateway.ScalingIntent, plan scaling.PlacementPlan) error {
	if controller.enrollment == nil {
		return controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{Code: "enrollment_builder_unavailable", Detail: "certified public preparation-spec builder is not configured"})
	}
	snapshot, err := controller.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return errors.Join(err, errors.New("catalog snapshot unavailable while admitting moves"))
	}
	for _, move := range plan.Moves {
		membership, found := snapshot.ResolveReplicatedMembershipRoute(move.Distribution, move.Shard, nil)
		if !found {
			return errors.New("membership route disappeared before enrollment reservation")
		}
		enrollment, buildErr := controller.enrollment.BuildEnrollment(ctx, intent, move, membership, move.Target)
		if buildErr != nil {
			return buildErr
		}
		if !enrollment.Valid() {
			return errors.New("enrollment builder returned invalid immutable intent")
		}
		// Create-only admission may race another controller. An ErrScalingRevision
		// is permission to reread and compare the complete immutable tuple, never
		// permission to perform PrepareReplica against the caller's stale copy.
		if err = controller.writer.PutEnrollmentIntent(ctx, enrollment, 0); err != nil {
			if !errors.Is(err, gateway.ErrScalingRevision) {
				return err
			}
			current, readErr := controller.directory.ReadEnrollmentIntent(ctx, enrollment.IntentID)
			if readErr != nil || current.Digest() != enrollment.Digest() {
				return errors.Join(readErr, gateway.ErrScalingRevision)
			}
			if current.State != gateway.EnrollmentReserved {
				// Prepared/Enrolled/Moving rows are resumed from their durable
				// phase on the next pass; do not replay a side effect here.
				continue
			}
			enrollment = current
		}
		current, readErr := controller.directory.ReadEnrollmentIntent(ctx, enrollment.IntentID)
		if readErr != nil || current.Digest() != enrollment.Digest() || current.State != gateway.EnrollmentReserved {
			return errors.Join(readErr, gateway.ErrScalingRevision)
		}
		enrollment = current
		if enrollment.PreparationClaim == ([32]byte{}) {
			claimer, ok := controller.writer.(gateway.EnrollmentPreparationClaimer)
			if !ok {
				return errors.New("durable enrollment preparation claim is unavailable")
			}
			claimed, claimErr := claimer.ClaimEnrollmentPreparation(ctx, enrollment.IntentID, enrollment.Revision)
			if claimErr != nil {
				return claimErr
			}
			enrollment = claimed
		}
		if enrollment.PreparationClaim != gateway.EnrollmentPreparationClaim(enrollment) {
			return gateway.ErrScalingRevision
		}
		proof, prepareErr := controller.provision.PrepareReplica(ctx, enrollment)
		if prepareErr != nil {
			return prepareErr
		}
		prepared := enrollment
		prepared.State = gateway.EnrollmentPrepared
		prepared.Revision++
		prepared.Proof = &proof
		current, readErr = controller.directory.ReadEnrollmentIntent(ctx, enrollment.IntentID)
		if readErr != nil || current.Digest() != enrollment.Digest() || current.State != gateway.EnrollmentReserved ||
			current.PreparationClaim != enrollment.PreparationClaim {
			return errors.Join(readErr, gateway.ErrScalingRevision)
		}
		prepared.PreparationClaim = [32]byte{}
		prepared.Revision = current.Revision + 1
		if err = controller.writer.PutEnrollmentIntent(ctx, prepared, current.Revision); err != nil {
			return err
		}
		current, readErr = controller.directory.ReadEnrollmentIntent(ctx, prepared.IntentID)
		if readErr != nil || current.Digest() != prepared.Digest() || current.State != gateway.EnrollmentPrepared || current.Proof == nil {
			return errors.Join(readErr, gateway.ErrScalingRevision)
		}
		prepared = current
		// Receipt publication is a catalog-generation transition owned by the
		// authority.  It is intentionally left to an optional concrete adapter;
		// without it the durable row remains Prepared and the next pass retries
		// this exact proof rather than activating a learner early.
		receipter, ok := controller.writer.(gateway.EnrollmentReceiptPublisher)
		if !ok {
			return controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{Code: "enrollment_receipt_unavailable", Detail: "catalog enrollment receipt publisher is not configured"})
		}
		enrolled, receiptErr := receipter.PublishEnrollmentReceipt(ctx, prepared)
		if receiptErr != nil {
			return receiptErr
		}
		if !enrolled.Valid() || enrolled.State != gateway.EnrollmentEnrolled || enrolled.Receipt == nil {
			return errors.New("catalog enrollment receipt publisher returned invalid enrolled intent")
		}
		current, readErr = controller.directory.ReadEnrollmentIntent(ctx, enrolled.IntentID)
		if readErr != nil || current.Digest() != enrolled.Digest() || current.State != gateway.EnrollmentEnrolled || current.Proof == nil || current.Receipt == nil {
			return errors.Join(readErr, gateway.ErrScalingRevision)
		}
		enrolled = current
		if err = controller.provision.EnrollReplica(ctx, enrolled, *enrolled.Proof); err != nil {
			return err
		}
		// Submit through the existing rebalanceexec saga. The operation ID is
		// derived from the exact observed publication and is persisted in the
		// enrollment row only after the journal admits the immutable move.
		if controller.moves == nil || controller.observation == nil {
			return controller.recordBlocker(ctx, intent, gateway.ScalingBlocker{Code: "move_controller_unavailable", Detail: "authenticated replica move saga is not configured"})
		}
		if _, submitErr := controller.submitEnrollmentMove(ctx, enrolled); submitErr != nil {
			return submitErr
		}
	}
	return nil
}

func (controller *ScalingController) recordPlacementBlockers(ctx context.Context, intent gateway.ScalingIntent, blockers []scaling.PlacementBlocker) error {
	converted := make([]gateway.ScalingBlocker, 0, min(len(blockers), gateway.MaxScalingBlockers))
	for _, blocker := range blockers {
		converted = append(converted, gateway.ScalingBlocker{Code: blocker.Code, Detail: blocker.Detail,
			Group: blocker.Group, Shard: blocker.Shard, ReplicaOrdinal: blocker.ReplicaOrdinal,
			Node: blocker.Node, Revision: blocker.Revision})
	}
	if len(converted) == 0 {
		converted = append(converted, gateway.ScalingBlocker{Code: "placement_blocked", Detail: "placement planner returned no admissible target"})
	}
	return controller.recordBlockers(ctx, intent, converted)
}

func (controller *ScalingController) recordBlocker(ctx context.Context, intent gateway.ScalingIntent, blocker gateway.ScalingBlocker) error {
	return controller.recordBlockers(ctx, intent, []gateway.ScalingBlocker{blocker})
}

func (controller *ScalingController) recordBlockers(ctx context.Context, intent gateway.ScalingIntent, blockers []gateway.ScalingBlocker) error {
	if len(blockers) > gateway.MaxScalingBlockers {
		blockers = blockers[:gateway.MaxScalingBlockers]
	}
	next := intent
	next.Blockers = slices.Clone(blockers)
	if sameScalingBlockers(next.Blockers, intent.Blockers) {
		return nil
	}
	next.Revision++
	next.DirectoryRevision = next.Revision
	if err := controller.writer.PutScalingIntent(ctx, next, intent.Revision); err != nil {
		return err
	}
	return nil
}

func sameScalingBlockers(left, right []gateway.ScalingBlocker) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func scalingCapacityStep(operation [32]byte, group raftmember.GroupKey, ordinal uint8) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/scaling/capacity/v1\x00"))
	hash.Write(operation[:])
	hash.Write(group.ClusterID[:])
	hash.Write(group.ClusterIncarnation[:])
	hash.Write(group.ShardIncarnation[:])
	hash.Write(group.GroupID[:])
	hash.Write([]byte{ordinal})
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func scalingBlockerFromError(err error) gateway.ScalingBlocker {
	return gateway.ScalingBlocker{Code: "controller_error", Detail: boundedScalingError(err)}
}

func boundedScalingError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > gateway.MaxScalingStringBytes {
		message = message[:gateway.MaxScalingStringBytes]
	}
	return strings.ReplaceAll(strings.ReplaceAll(message, "\x00", ""), "\n", " ")
}

func scalingEvidenceFromNode(evidence gateway.NodeReferenceEvidence) gateway.SafeToStopEvidence {
	result := gateway.SafeToStopEvidenceFromReference(evidence)
	result.DrainAcknowledged = evidence.ZeroDataReferences()
	result.CatalogControlMigrated = evidence.CatalogVoterReferences == 0 && evidence.ControlVoterReferences == 0
	return result
}

func blockersFromEvidence(evidence gateway.NodeReferenceEvidence) []gateway.ScalingBlocker {
	result := make([]gateway.ScalingBlocker, 0, 8)
	add := func(code string, count uint32) {
		if count != 0 {
			result = append(result, gateway.ScalingBlocker{Code: code, Detail: fmt.Sprintf("%s reference count=%d", code, count), Node: evidence.NodeID, Revision: evidence.DirectoryRevision})
		}
	}
	add("serving_replicas", evidence.ServingReplicas)
	add("learner_replicas", evidence.LearnerReplicas)
	add("enrolled_targets", evidence.EnrolledTargets)
	add("outstanding_moves", evidence.OutstandingMoves)
	add("catalog_voters", evidence.CatalogVoterReferences)
	add("control_voters", evidence.ControlVoterReferences)
	add("gateway_sessions", evidence.GatewayParticipantRefs)
	if len(result) == 0 {
		result = append(result, gateway.ScalingBlocker{Code: "retirement_fence", Detail: "reference scan is not yet a safe-to-stop proof", Node: evidence.NodeID, Revision: evidence.DirectoryRevision})
	}
	return result
}

func scalingEnrollmentID(parent [32]byte, intent gateway.GroupEnrollmentIntent) [32]byte {
	copyOf := intent
	copyOf.IntentID = [32]byte{}
	// Enrollment state, revision, proof, receipt, and move operation are
	// recovery fields. Ownership must remain stable across Reserved ->
	// Prepared -> Enrolled -> Moving so a restart can rediscover the same row.
	copyOf.State = gateway.EnrollmentReserved
	copyOf.Revision = 0
	copyOf.Proof = nil
	copyOf.Receipt = nil
	copyOf.MoveOperationID = [32]byte{}
	raw, err := vibejson.Marshal(&copyOf)
	if err != nil {
		return [32]byte{}
	}
	hash := sha256.New()
	hash.Write([]byte("vibedb/scaling/enrollment/v1\x00"))
	hash.Write(parent[:])
	hash.Write(raw)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func nodeIDHex(node rafttransport.NodeID) string { return hex.EncodeToString(node[:]) }

func lifecycleName(lifecycle gateway.NodeLifecycle) string {
	switch lifecycle {
	case gateway.NodeJoining:
		return "joining"
	case gateway.NodeActive:
		return "active"
	case gateway.NodeDraining:
		return "draining"
	case gateway.NodeDecommissioned:
		return "decommissioned"
	default:
		return "unknown"
	}
}

// clusterControlBackend is the small interface used by the listener adapter.
// Keeping the protocol server below independent from Runtime also makes the
// canonical NDJSON grammar directly testable without opening a TLS socket.
type clusterControlBackend interface {
	ExecuteClusterControl(context.Context, clustercontrol.Request) clustercontrol.Response
}

var _ gateway.DirectoryReader = (*gateway.ReplicatedCatalogAuthority)(nil)
var _ gateway.DirectoryWriter = (*gateway.ReplicatedCatalogAuthority)(nil)

// scalingPlacementNodes binds unchanged directory rows to the catalog cut
// being planned. A row's CatalogGeneration records its last mutation, not the
// freshness of a new complete directory observation. The caller rechecks both
// directory digest/revision and catalog digest before admitting the plan.
func scalingPlacementNodes(cut gateway.NodeDirectoryCut, generation uint64) ([]gateway.NodeRecord, error) {
	if !cut.Valid() || generation == 0 || cut.CatalogGeneration > generation {
		return nil, gateway.ErrScalingRevision
	}
	nodes := slices.Clone(cut.Nodes)
	for index := range nodes {
		nodes[index].CatalogGeneration = generation
	}
	return nodes, nil
}
