package gatewayruntime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	errClusterControlUnavailable = errors.New("gatewayruntime: cluster control is unavailable")
	errClusterControlAuth        = errors.New("gatewayruntime: cluster control authorization denied")
)

// ScalingOperatorBackend is the concrete operator API backed by the
// replicated catalog.  It owns no process-local operation map: a request is
// either represented by a durable ScalingIntent or returns an error before
// the operation ID is exposed.
type ScalingOperatorBackend struct {
	directory          gateway.DirectoryReader
	writer             gateway.DirectoryWriter
	catalog            scalingCatalogReader
	distributedMetrics *gateway.DistributedMetrics
	moveFailure        func([32]byte) string
}

func NewScalingOperatorBackend(controller *ScalingController) (*ScalingOperatorBackend, error) {
	if controller == nil || controller.directory == nil || controller.writer == nil || controller.catalog == nil {
		return nil, errClusterControlUnavailable
	}
	backend, err := newScalingOperatorBackend(controller.directory, controller.writer, controller.catalog)
	if err != nil {
		return nil, err
	}
	if controller.moves != nil {
		backend.moveFailure = controller.moves.LastFailure
	}
	return backend, nil
}

// Participants expose the same durable operator API without starting another
// autonomous reconciliation loop. The active controller discovers these intents.
func newScalingOperatorBackend(directory gateway.DirectoryReader, writer gateway.DirectoryWriter, catalog scalingCatalogReader) (*ScalingOperatorBackend, error) {
	if directory == nil || writer == nil || catalog == nil {
		return nil, errClusterControlUnavailable
	}
	return &ScalingOperatorBackend{directory: directory, writer: writer, catalog: catalog}, nil
}

// ExecuteClusterControl implements the wire operation contract.  Mutating
// submission is detached from the client connection so a lost response does
// not strand a durable intent; read-only requests and the optional observation
// pass retain the caller's cancellation and deadline.  Cancelling --wait
// therefore ends only that wait and never rolls back the submitted intent.
func (backend *ScalingOperatorBackend) ExecuteClusterControl(ctx context.Context, request clustercontrol.Request) clustercontrol.Response {
	response := clustercontrol.Response{Format: clustercontrol.Format, Op: request.Op,
		RequestID: request.RequestID, OK: false}
	if backend == nil || ctx == nil || !request.Valid() {
		response.Error = "invalid cluster control request"
		return response
	}
	var operation [32]byte
	var err error
	switch request.Op {
	case clustercontrol.OpNodes:
		return backend.nodesResponse(ctx, response)
	case clustercontrol.OpStatus:
		if operation, err = decodeClusterOperationID(request.OperationID); err == nil {
			return backend.observeResponse(ctx, response, operation, request.WaitMillis)
		}
	case clustercontrol.OpJoin:
		operation, err = backend.submitJoin(context.WithoutCancel(ctx), request)
	case clustercontrol.OpRebalance:
		operation, err = backend.submitRebalance(context.WithoutCancel(ctx), request)
	case clustercontrol.OpDecommission:
		operation, err = backend.submitDecommission(context.WithoutCancel(ctx), request)
	default:
		err = errors.New("unsupported cluster control operation")
	}
	if err != nil {
		response.Error = boundedClusterControlError(err)
		return response
	}
	response.OK = true
	response.OperationID = hex.EncodeToString(operation[:])
	if request.WaitMillis != 0 {
		return backend.observeResponse(ctx, response, operation, request.WaitMillis)
	}
	response.State = "reserved"
	return response
}

func (backend *ScalingOperatorBackend) submitJoin(ctx context.Context, request clustercontrol.Request) ([32]byte, error) {
	if request.NodeDescriptor == nil {
		return [32]byte{}, clustercontrol.ErrInvalidNodeDescriptor
	}
	descriptor := *request.NodeDescriptor
	node, err := scalingNodeRecordFromDescriptor(descriptor, currentCatalogGeneration(ctx, backend.catalog))
	if err != nil {
		return [32]byte{}, err
	}
	if existing, readErr := backend.directory.ReadNode(ctx, node.NodeID, node.Incarnation); readErr == nil {
		if !samePublicNodeRecord(existing, node) {
			return [32]byte{}, fmt.Errorf("node %s incarnation %d is already registered with a different public identity", nodeIDHex(node.NodeID), node.Incarnation)
		}
	} else if !errors.Is(readErr, gateway.ErrScalingNodeMissing) {
		return [32]byte{}, readErr
	} else if err = backend.writer.PutNode(ctx, node, 0); err != nil {
		return [32]byte{}, err
	}
	target := gateway.NodeReference{NodeID: node.NodeID, Incarnation: node.Incarnation}
	intent, err := backend.newIntent(ctx, gateway.ScalingScaleOut, request.RequestID, gateway.NodeReference{}, []gateway.NodeReference{target}, 0, 4096, 1<<62, 0)
	if err != nil {
		return [32]byte{}, err
	}
	operation, err := backend.submitOrReuseIntent(ctx, intent)
	return operation, err
}

func (backend *ScalingOperatorBackend) submitRebalance(ctx context.Context, request clustercontrol.Request) ([32]byte, error) {
	intent, err := backend.newIntent(ctx, gateway.ScalingRebalance, request.RequestID, gateway.NodeReference{}, nil,
		request.DesiredNodeCount, request.MaxMoves, request.MaxMigrationBytes, request.HysteresisPPM)
	if err != nil {
		return [32]byte{}, err
	}
	operation, err := backend.submitOrReuseIntent(ctx, intent)
	return operation, err
}

func (backend *ScalingOperatorBackend) submitDecommission(ctx context.Context, request clustercontrol.Request) ([32]byte, error) {
	nodeID, err := decodeClusterNodeID(request.NodeID)
	if err != nil {
		return [32]byte{}, err
	}
	node, err := backend.directory.ReadNode(ctx, nodeID, request.NodeIncarnation)
	if err != nil {
		return [32]byte{}, err
	}
	if node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining && node.Lifecycle != gateway.NodeDecommissioned {
		return [32]byte{}, fmt.Errorf("node %s is not active or draining", request.NodeID)
	}
	drain := gateway.NodeReference{NodeID: nodeID, Incarnation: request.NodeIncarnation}
	intent, err := backend.newIntent(ctx, gateway.ScalingDecommission, request.RequestID, drain, nil, 0, 4096, 1<<62, 0)
	if err != nil {
		return [32]byte{}, err
	}
	// Submission only owns durable intent admission. The controller owns
	// frontend drain, lifecycle CAS and retirement, including retries after
	// crashes. Running that same workflow here races its first reconcile and
	// can report failure after the operation is already durably admitted.
	return backend.submitOrReuseIntent(ctx, intent)
}

func (backend *ScalingOperatorBackend) newIntent(ctx context.Context, kind gateway.ScalingKind, requestID string, drain gateway.NodeReference, targets []gateway.NodeReference, desired, maxMoves uint16, maxBytes, hysteresis uint64) (gateway.ScalingIntent, error) {
	requestIDBytes, err := decodeClusterRequestID(requestID)
	if err != nil {
		return gateway.ScalingIntent{}, err
	}
	request := gateway.ScalingIntentRequest{Kind: kind, RequestID: requestIDBytes, Drain: drain,
		Targets: targets, DesiredNodeCount: desired, MaxMoves: maxMoves, MaxMigrationBytes: maxBytes,
		HysteresisPPM: hysteresis}
	if !request.Valid() {
		return gateway.ScalingIntent{}, gateway.ErrInvalidScalingMetadata
	}
	snapshot, err := backend.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return gateway.ScalingIntent{}, errors.Join(err, errClusterControlUnavailable)
	}
	return gateway.ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: snapshot.Generation(),
		Revision: 1, DirectoryRevision: 1, State: gateway.ScalingReserved}, nil
}

func (backend *ScalingOperatorBackend) submitOrReuseIntent(ctx context.Context, intent gateway.ScalingIntent) ([32]byte, error) {
	intents, err := backend.directory.ListScalingIntents(ctx)
	if err != nil {
		return [32]byte{}, err
	}
	for _, prior := range intents {
		if prior.Request.RequestID != intent.Request.RequestID {
			continue
		}
		if prior.ID != intent.ID {
			return [32]byte{}, errors.New("request ID was already used for a different scaling intent")
		}
		return prior.ID, nil
	}
	if err = backend.writer.PutScalingIntent(ctx, intent, 0); err != nil {
		// A concurrent gateway may have committed the same immutable intent.
		if _, retryErr := backend.directory.ReadScalingIntent(ctx, intent.ID); retryErr == nil {
			return intent.ID, nil
		}
		return [32]byte{}, err
	}
	return intent.ID, nil
}

func (backend *ScalingOperatorBackend) nodesResponse(ctx context.Context, response clustercontrol.Response) clustercontrol.Response {
	nodes, generation, revision, err := backend.readNodeDirectoryStatus(ctx)
	if err != nil {
		response.Error = boundedClusterControlError(err)
		return response
	}
	progress, err := backend.progress(ctx, gateway.ScalingIntent{})
	if err != nil {
		response.Error = boundedClusterControlError(err)
		return response
	}
	slices.SortFunc(nodes, func(left, right gateway.NodeRecord) int {
		return bytesCompareNodeRecords(left, right)
	})
	response.OK = true
	response.CatalogGeneration = generation
	response.DirectoryRevision = revision
	response.GroupInventoryDigest = progress.inventoryDigest
	response.Nodes = make([]clustercontrol.NodeStatus, 0, len(nodes))
	acknowledged := backend.terminalDrainAcknowledgements(ctx)
	for _, node := range nodes {
		status := backend.nodeStatus(ctx, node, acknowledged[NodeReferenceKey{NodeID: node.NodeID, Incarnation: node.Incarnation}])
		response.Nodes = append(response.Nodes, status)
	}
	return response
}

func (backend *ScalingOperatorBackend) observeResponse(ctx context.Context, response clustercontrol.Response, operation [32]byte, waitMillis uint64) clustercontrol.Response {
	return observeClusterControlStatus(ctx, time.Duration(waitMillis)*time.Millisecond, func(readContext context.Context) clustercontrol.Response {
		return backend.observeOnce(readContext, response, operation)
	})
}

func observeClusterControlStatus(ctx context.Context, wait time.Duration, observe func(context.Context) clustercontrol.Response) clustercontrol.Response {
	deadline := time.Now().Add(wait)
	// The optional long-poll budget must not cancel the initial authoritative
	// read. Every successful response is backed by a completed observation.
	last := observe(ctx)
	terminal := func(value clustercontrol.Response) bool {
		return value.State == "complete" || value.State == "decommissioned" || value.SafeToStop || len(value.Blockers) != 0
	}
	if wait == 0 || !last.OK || last.Error != "" || terminal(last) {
		return last
	}
	waitContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		select {
		case <-waitContext.Done():
			return last
		case <-time.After(25 * time.Millisecond):
		}
		observed := observe(waitContext)
		if !observed.OK || observed.Error != "" {
			if waitContext.Err() != nil {
				return last
			}
			return observed
		}
		last = observed
		if terminal(last) {
			return last
		}
	}
}

func (backend *ScalingOperatorBackend) observeOnce(ctx context.Context, response clustercontrol.Response, operation [32]byte) clustercontrol.Response {
	response.OperationID = hex.EncodeToString(operation[:])
	intent, err := backend.directory.ReadScalingIntent(ctx, operation)
	if err != nil {
		response.Error = boundedClusterControlError(err)
		return response
	}
	response.OK = true
	response.OperationID = hex.EncodeToString(operation[:])
	response.State = scalingStateName(intent.State)
	response.Blockers = backend.clusterBlockers(ctx, intent, intent.Blockers)
	response.Evidence = clusterEvidence(intent.Evidence)
	response.SafeToStop = intent.Evidence.SafeToStop()
	response.Budget = backend.budgetStatus()
	progress, progressErr := backend.progress(ctx, intent)
	if progressErr != nil {
		response.Error = boundedClusterControlError(progressErr)
		response.OK = false
		return response
	}
	response.Phase = progress.phase
	if progress.moveFailure != "" {
		response.Blockers = append(response.Blockers, clustercontrol.Blocker{
			Code: "move_execution", Detail: progress.moveFailure,
		})
	}
	response.ApplicationGroupsMoved = progress.applicationGroupsMoved
	response.InternalGroupsMoved = progress.internalGroupsMoved
	response.GroupInventoryDigest = progress.inventoryDigest
	response.Nodes, response.CatalogGeneration, response.DirectoryRevision = backend.nodeStatuses(ctx)
	if intent.Request.Kind == gateway.ScalingScaleIn || intent.Request.Kind == gateway.ScalingDecommission {
		node, nodeErr := backend.directory.ReadNode(ctx, intent.Request.Drain.NodeID, intent.Request.Drain.Incarnation)
		if nodeErr == nil {
			evidence, scanErr := backend.scanReferenceEvidence(ctx, intent, node)
			if scanErr == nil {
				fresh := scalingEvidenceFromNode(evidence)
				response.Evidence = clusterEvidence(fresh)
				response.RetiringReferences = nodeReferenceCount(evidence)
				if node.Lifecycle == gateway.NodeDecommissioned {
					// A terminal node record proves only the lifecycle CAS and
					// reference scan. The final exact-cut acknowledgement remains
					// durable evidence on the intent; status must never synthesize
					// it from HasRetirementProof alone.
					if terminalDrainAcknowledgementMatches(intent.Evidence, node, evidence) {
						fresh.DrainAcknowledged = intent.Evidence.DrainAcknowledged
						fresh.RetiredAcknowledged = intent.Evidence.RetiredAcknowledged
						fresh.CatalogControlMigrated = intent.Evidence.CatalogControlMigrated
						response.Evidence = clusterEvidence(fresh)
						response.SafeToStop = fresh.SafeToStop()
					} else {
						response.SafeToStop = false
						response.Blockers = append(response.Blockers, clustercontrol.Blocker{
							Code:   "frontend_drain_terminal_ack_pending",
							Detail: "terminal exact cut acknowledgement is not durable",
							NodeID: nodeIDHex(node.NodeID), NodeIncarnation: node.Incarnation,
							Revision: node.Revision,
						})
					}
				} else {
					// Draining is an admission fence, never a stop proof. The
					// terminal RetireNode CAS is the only point that can make this
					// operation safe to stop. Scale-in keeps this fresh cut in
					// status even after its own operation is complete.
					response.SafeToStop = false
				}
				if evidence.ServingReplicas != 0 || evidence.LearnerReplicas != 0 ||
					evidence.EnrolledTargets != 0 || evidence.OutstandingMoves != 0 ||
					evidence.CatalogVoterReferences != 0 || evidence.ControlVoterReferences != 0 ||
					evidence.GatewayParticipantRefs != 0 {
					response.Blockers = append(response.Blockers,
						backend.clusterBlockersForReference(intent, blockersFromEvidence(evidence), node.NodeID, node.Incarnation)...)
				}
				// A terminal decommission proof is a fresh authoritative cut. An
				// older controller blocker must not remain attached to the
				// operator response after that cut has established safe-to-stop.
				if response.SafeToStop && evidence.ZeroAllReferences() {
					response.Blockers = nil
				}
			} else {
				response.SafeToStop = false
				response.Blockers = append(response.Blockers, clustercontrol.Blocker{
					Code: "retirement_scan_unavailable", Detail: boundedClusterControlError(scanErr),
					NodeID: nodeIDHex(node.NodeID), NodeIncarnation: node.Incarnation, Revision: node.Revision,
				})
			}
			if intent.Request.Kind == gateway.ScalingDecommission {
				response.State = lifecycleName(node.Lifecycle)
				response.Phase = lifecycleName(node.Lifecycle)
			}
		}
	}
	return response
}

func (backend *ScalingOperatorBackend) budgetStatus() *clustercontrol.BudgetStatus {
	if backend == nil || backend.distributedMetrics == nil {
		return nil
	}
	aggregate, err := backend.distributedMetrics.Aggregate()
	if err != nil || aggregate.Budget.MaxActive == 0 {
		return nil
	}
	return &clustercontrol.BudgetStatus{ThrottledCalls: aggregate.Budget.ThrottledCalls,
		ThrottledBytes: aggregate.Budget.ThrottledBytes, PeakActive: uint32(aggregate.Budget.PeakActive),
		MaxActive: uint32(aggregate.Budget.MaxActive)}
}

type clusterControlProgress struct {
	phase                  string
	applicationGroupsMoved uint32
	internalGroupsMoved    uint32
	retiringReferences     uint32
	inventoryDigest        string
	moveFailure            string
}

// progress derives operator-visible movement facts from the current catalog
// route inventory and atomically committed parent counters. Active enrollment
// rows describe only the current phase. It intentionally does not
// use planner output, in-memory queues, or node counts as a completion signal.
func (backend *ScalingOperatorBackend) progress(ctx context.Context, parent gateway.ScalingIntent) (clusterControlProgress, error) {
	if backend == nil || backend.catalog == nil {
		return clusterControlProgress{}, errClusterControlUnavailable
	}
	snapshot, err := backend.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return clusterControlProgress{}, errors.Join(err, errClusterControlUnavailable)
	}
	if parent.CompletedInternalReplicas > parent.CompletedReplicas {
		return clusterControlProgress{}, gateway.ErrInvalidScalingMetadata
	}
	result := clusterControlProgress{phase: scalingStateName(parent.State),
		applicationGroupsMoved: parent.CompletedReplicas - parent.CompletedInternalReplicas,
		internalGroupsMoved:    parent.CompletedInternalReplicas}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	seen := make(map[raftmember.GroupKey]struct{}, snapshot.ReplicatedRouteCount())
	hash := sha256.New()
	hash.Write([]byte("vibedb/cluster-control/group-inventory/v2\x00"))
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, replicas[:0])
		if !ok {
			return clusterControlProgress{}, errors.New("catalog route inventory changed while rendering status")
		}
		if _, found := seen[route.Group]; found {
			continue
		}
		seen[route.Group] = struct{}{}
		// Inventory describes the current committed physical placement, not
		// retained operation history. It is unchanged by history collection.
		entry := struct {
			Group                raftmember.GroupKey
			Distribution         distribution.DistributionName
			Shard                distribution.ShardID
			AllocationGeneration uint64
			Replicas             []gateway.ReplicatedEndpoint
		}{route.Group, route.Distribution, route.Shard, route.AllocationGeneration, route.Replicas}
		if parent.ID != ([32]byte{}) && parent.State < gateway.ScalingComplete {
			rows, listErr := backend.directory.ListEnrollmentIntents(ctx, route.Group)
			if listErr != nil {
				return clusterControlProgress{}, listErr
			}
			for _, row := range rows {
				if row.State >= gateway.EnrollmentComplete || row.State == gateway.EnrollmentCancelled ||
					scalingEnrollmentID(parent.ID, row) != row.IntentID {
					continue
				}
				if row.State > enrollmentStateForPhase(result.phase) {
					result.phase = enrollmentPhaseName(row.State)
				}
				if result.moveFailure == "" && row.MoveOperationID != ([32]byte{}) && backend.moveFailure != nil {
					if detail := backend.moveFailure(row.MoveOperationID); detail != "" {
						result.moveFailure = boundedClusterControlError(errors.New(detail))
					}
				}
			}
		}
		encoded, marshalErr := vibejson.Marshal(&entry)
		if marshalErr != nil {
			return clusterControlProgress{}, marshalErr
		}
		hash.Write(encoded)
	}
	digest := hash.Sum(nil)
	result.inventoryDigest = hex.EncodeToString(digest)
	if parent.State == gateway.ScalingComplete {
		result.phase = "complete"
	}
	return result, nil
}

func enrollmentPhaseName(state gateway.EnrollmentState) string {
	switch state {
	case gateway.EnrollmentReserved:
		return "reserved"
	case gateway.EnrollmentPrepared:
		return "prepared"
	case gateway.EnrollmentEnrolled:
		return "enrolled"
	case gateway.EnrollmentMoving:
		return "moving"
	case gateway.EnrollmentComplete:
		return "complete"
	default:
		return "unknown"
	}
}

func enrollmentStateForPhase(phase string) gateway.EnrollmentState {
	switch phase {
	case "reserved":
		return gateway.EnrollmentReserved
	case "prepared":
		return gateway.EnrollmentPrepared
	case "enrolled":
		return gateway.EnrollmentEnrolled
	case "moving":
		return gateway.EnrollmentMoving
	case "complete":
		return gateway.EnrollmentComplete
	default:
		return 0
	}
}

func nodeReferenceCount(evidence gateway.NodeReferenceEvidence) uint32 {
	return evidence.ServingReplicas + evidence.LearnerReplicas + evidence.EnrolledTargets +
		evidence.OutstandingMoves + evidence.CatalogVoterReferences + evidence.ControlVoterReferences +
		evidence.GatewayParticipantRefs
}

func (backend *ScalingOperatorBackend) scanReferenceEvidence(
	ctx context.Context, intent gateway.ScalingIntent, node gateway.NodeRecord,
) (gateway.NodeReferenceEvidence, error) {
	if intent.Request.Kind == gateway.ScalingDecommission && node.HasRetirementProof() {
		if scanner, ok := backend.directory.(gateway.TerminalNodeReferenceScanner); ok {
			return scanner.ScanDecommissionedNodeReferences(ctx, node.NodeID, node.Incarnation)
		}
	}
	return backend.directory.ScanNodeReferences(ctx, node.NodeID, node.Incarnation)
}

func (backend *ScalingOperatorBackend) nodeStatuses(ctx context.Context) ([]clustercontrol.NodeStatus, uint64, uint64) {
	nodes, generation, revision, err := backend.readNodeDirectoryStatus(ctx)
	if err != nil {
		return nil, 0, 0
	}
	slices.SortFunc(nodes, func(left, right gateway.NodeRecord) int {
		return bytesCompareNodeRecords(left, right)
	})
	result := make([]clustercontrol.NodeStatus, 0, len(nodes))
	acknowledged := backend.terminalDrainAcknowledgements(ctx)
	for _, node := range nodes {
		result = append(result, backend.nodeStatus(ctx, node,
			acknowledged[NodeReferenceKey{NodeID: node.NodeID, Incarnation: node.Incarnation}]))
	}
	return result, generation, revision
}

func (backend *ScalingOperatorBackend) readNodeDirectoryStatus(ctx context.Context) ([]gateway.NodeRecord, uint64, uint64, error) {
	if backend == nil || backend.directory == nil {
		return nil, 0, 0, errClusterControlUnavailable
	}
	if reader, ok := backend.directory.(gateway.NodeDirectoryCutReader); ok {
		cut, err := reader.ReadNodeDirectoryCut(ctx)
		if err != nil {
			return nil, 0, 0, err
		}
		return slices.Clone(cut.Nodes), cut.CatalogGeneration, cut.Revision, nil
	}
	nodes, err := backend.directory.ListNodes(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	var generation, revision uint64
	for _, node := range nodes {
		generation = max(generation, node.CatalogGeneration)
		revision = max(revision, node.Revision)
	}
	return nodes, generation, revision, nil
}

func bytesCompareNodeIDs(left, right rafttransport.NodeID) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func bytesCompareNodeRecords(left, right gateway.NodeRecord) int {
	if compared := bytesCompareNodeIDs(left.NodeID, right.NodeID); compared != 0 {
		return compared
	}
	if left.Incarnation < right.Incarnation {
		return -1
	}
	if left.Incarnation > right.Incarnation {
		return 1
	}
	return 0
}

// NodeReferenceKey identifies the physical incarnation used by a terminal
// acknowledgement. It keeps status projections independent of a stale
// operator intent's textual title or request ordering.
type NodeReferenceKey struct {
	NodeID      rafttransport.NodeID
	Incarnation uint64
}

func (backend *ScalingOperatorBackend) terminalDrainAcknowledgements(
	ctx context.Context,
) map[NodeReferenceKey]gateway.SafeToStopEvidence {
	result := make(map[NodeReferenceKey]gateway.SafeToStopEvidence)
	if backend == nil || backend.directory == nil || ctx == nil {
		return result
	}
	intents, err := backend.directory.ListScalingIntents(ctx)
	if err != nil {
		return result
	}
	terminal, historyErr := backend.directory.ListScalingTerminalIntents(ctx)
	if historyErr == nil {
		intents = append(intents, terminal...)
	}
	for _, intent := range intents {
		if intent.Request.Kind != gateway.ScalingDecommission || !intent.Request.Drain.Valid() ||
			!intent.Evidence.RetiredAcknowledged {
			continue
		}
		key := NodeReferenceKey{NodeID: intent.Request.Drain.NodeID, Incarnation: intent.Request.Drain.Incarnation}
		result[key] = intent.Evidence
	}
	return result
}

func (backend *ScalingOperatorBackend) nodeStatus(
	ctx context.Context, node gateway.NodeRecord, acknowledged gateway.SafeToStopEvidence,
) clustercontrol.NodeStatus {
	status := clustercontrol.NodeStatus{NodeID: nodeIDHex(node.NodeID), Incarnation: node.Incarnation,
		Lifecycle: lifecycleName(node.Lifecycle), Revision: node.Revision, CatalogGeneration: node.CatalogGeneration}
	if node.Lifecycle == gateway.NodeDecommissioned {
		if evidence, err := backend.scanNodeStatusReferences(ctx, node); err == nil {
			proof := scalingEvidenceFromNode(evidence)
			if terminalDrainAcknowledgementMatches(acknowledged, node, evidence) {
				proof.DrainAcknowledged = acknowledged.DrainAcknowledged
				proof.RetiredAcknowledged = acknowledged.RetiredAcknowledged
				proof.CatalogControlMigrated = acknowledged.CatalogControlMigrated
				status.SafeToStop = proof.SafeToStop()
			}
		}
	}
	return status
}

// terminalDrainAcknowledgementMatches binds the persisted terminal ACK to the
// current physical identity and fresh zero-reference cut. Completion removes
// the active scaling/enrollment/operation rows as part of its own CAS fences,
// so those three mutable directory digests and the aggregate witness digest
// are expected to advance after the ACK is retained. The node-directory,
// catalog-head, generation, and every reference count remain exact fences.
func terminalDrainAcknowledgementMatches(
	ack gateway.SafeToStopEvidence, node gateway.NodeRecord, reference gateway.NodeReferenceEvidence,
) bool {
	return ack.NodeID == node.NodeID && ack.NodeIncarnation == node.Incarnation &&
		ack.RetiredAcknowledged && ack.SafeToStop() &&
		ack.ScanCatalogGeneration == reference.CatalogGeneration &&
		ack.ScanDirectoryRevision == reference.DirectoryCutRevision &&
		ack.ScanDirectoryDigest == reference.DirectoryCutDigest &&
		ack.CatalogHeadDigest == reference.CatalogHeadDigest &&
		ack.ServingReplicas == reference.ServingReplicas &&
		ack.LearnerReplicas == reference.LearnerReplicas &&
		ack.EnrolledTargets == reference.EnrolledTargets &&
		ack.OutstandingMoves == reference.OutstandingMoves &&
		ack.CatalogVoters == reference.CatalogVoterReferences &&
		ack.ControlVoters == reference.ControlVoterReferences &&
		ack.GatewayParticipants == reference.GatewayParticipantRefs
}

// A terminal node's live gateway may have been stopped after the retirement
// CAS. Use the authenticated terminal scan in that case; it excludes the
// retired gateway participant while still checking the current catalog cut.
func (backend *ScalingOperatorBackend) scanNodeStatusReferences(
	ctx context.Context, node gateway.NodeRecord,
) (gateway.NodeReferenceEvidence, error) {
	if node.HasRetirementProof() {
		if scanner, ok := backend.directory.(gateway.TerminalNodeReferenceScanner); ok {
			return scanner.ScanDecommissionedNodeReferences(ctx, node.NodeID, node.Incarnation)
		}
	}
	return backend.directory.ScanNodeReferences(ctx, node.NodeID, node.Incarnation)
}

// Capacity and receive limits are refreshed from authenticated readiness;
// retries compare the stable public identity, not the initial capacity hints.
func samePublicNodeRecord(left, right gateway.NodeRecord) bool {
	return left.NodeID == right.NodeID && left.Incarnation == right.Incarnation &&
		left.ServiceKeyDigest == right.ServiceKeyDigest &&
		left.DataEndpoint == right.DataEndpoint && left.NativeEndpoint == right.NativeEndpoint &&
		left.ControlEndpoint == right.ControlEndpoint && left.GatewayEndpoint == right.GatewayEndpoint &&
		left.DataAddress == right.DataAddress && left.NativeAddress == right.NativeAddress &&
		left.ControlAddress == right.ControlAddress && left.GatewayAddress == right.GatewayAddress &&
		left.FailureDomain == right.FailureDomain && left.Roles == right.Roles
}

func scalingNodeRecordFromDescriptor(descriptor clustercontrol.NodeDescriptor, generation uint64) (gateway.NodeRecord, error) {
	if !descriptor.Valid() {
		return gateway.NodeRecord{}, clustercontrol.ErrInvalidNodeDescriptor
	}
	var node rafttransport.NodeID
	if _, err := hex.Decode(node[:], []byte(descriptor.NodeID)); err != nil {
		return gateway.NodeRecord{}, err
	}
	var serviceKeyDigest replication.Digest
	if _, err := hex.Decode(serviceKeyDigest[:], []byte(descriptor.ServiceKeyDigest)); err != nil || serviceKeyDigest == (replication.Digest{}) {
		return gateway.NodeRecord{}, clustercontrol.ErrInvalidNodeDescriptor
	}
	roles := gateway.NodeRole(0)
	for _, role := range descriptor.Roles {
		switch role {
		case "storage":
			roles |= gateway.NodeRoleStorage
		case "catalog":
			roles |= gateway.NodeRoleCatalog
		case "gateway":
			// A public descriptor cannot certify an active gateway session. A
			// gateway role is therefore rejected until the control participant
			// enrollment supplies GatewayIdentity through a trusted local path.
			return gateway.NodeRecord{}, errors.New("gateway role requires a certified participant identity")
		case "control":
			roles |= gateway.NodeRoleControl
		}
	}
	endpoint := func(value, suffix string) distribution.EndpointID {
		if value != "" {
			return distribution.EndpointID(value)
		}
		return distribution.EndpointID(descriptor.NodeID + "/" + suffix)
	}
	if generation == 0 {
		generation = 1
	}
	return gateway.NodeRecord{NodeID: node, Incarnation: descriptor.Incarnation, ServiceKeyDigest: serviceKeyDigest,
		DataEndpoint: endpoint(descriptor.DataEndpoint, "data"), NativeEndpoint: endpoint(descriptor.NativeEndpoint, "native"),
		ControlEndpoint: endpoint(descriptor.ControlEndpoint, "control"),
		DataAddress:     descriptor.DataAddress, NativeAddress: descriptor.NativeAddress, ControlAddress: descriptor.ControlAddress,
		FailureDomain: descriptor.FailureDomain, Roles: roles,
		Capacity: autosplit.CapacityVector(descriptor.Capacity), MigrationCapacity: descriptor.MigrationCapacity,
		MaxReceives: descriptor.MaxReceives, Lifecycle: gateway.NodeJoining, Revision: 1, CatalogGeneration: generation}, nil
}

func currentCatalogGeneration(ctx context.Context, catalog scalingCatalogReader) uint64 {
	if catalog == nil {
		return 1
	}
	snapshot, err := catalog.Read(ctx)
	if err != nil || snapshot == nil || snapshot.Generation() == 0 {
		return 1
	}
	return snapshot.Generation()
}

func decodeClusterRequestID(value string) ([32]byte, error) {
	var id [32]byte
	if len(value) != 64 || strings.ToLower(value) != value {
		return id, clustercontrol.ErrInvalidRequest
	}
	if _, err := hex.Decode(id[:], []byte(value)); err != nil || id == ([32]byte{}) {
		return [32]byte{}, clustercontrol.ErrInvalidRequest
	}
	return id, nil
}

func decodeClusterOperationID(value string) ([32]byte, error) { return decodeClusterRequestID(value) }
func decodeClusterNodeID(value string) (rafttransport.NodeID, error) {
	var id rafttransport.NodeID
	if len(value) != 32 || strings.ToLower(value) != value {
		return id, clustercontrol.ErrInvalidRequest
	}
	if _, err := hex.Decode(id[:], []byte(value)); err != nil || id == (rafttransport.NodeID{}) {
		return id, clustercontrol.ErrInvalidRequest
	}
	return id, nil
}

func scalingStateName(state gateway.ScalingIntentState) string {
	switch state {
	case gateway.ScalingReserved:
		return "reserved"
	case gateway.ScalingRunning:
		return "running"
	case gateway.ScalingComplete:
		return "complete"
	case gateway.ScalingCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func clusterBlockers(blockers []gateway.ScalingBlocker) []clustercontrol.Blocker {
	result := make([]clustercontrol.Blocker, 0, len(blockers))
	for _, blocker := range blockers {
		item := clustercontrol.Blocker{Code: blocker.Code, Detail: boundedClusterControlError(errors.New(blocker.Detail)), Revision: blocker.Revision}
		if blocker.Node != (rafttransport.NodeID{}) {
			item.NodeID = nodeIDHex(blocker.Node)
		}
		result = append(result, item)
	}
	return result
}

// clusterBlockers enriches the public blocker with the exact physical
// incarnation whenever the directory can prove it. ScalingBlocker predates
// the operator DTO and stores a directory revision, so that revision must
// never be copied into node_incarnation as if it were an identity fence.
func (backend *ScalingOperatorBackend) clusterBlockers(ctx context.Context, intent gateway.ScalingIntent, blockers []gateway.ScalingBlocker) []clustercontrol.Blocker {
	result := clusterBlockers(blockers)
	if len(result) == 0 || backend == nil || backend.directory == nil {
		return result
	}
	nodes, _, _, err := backend.readNodeDirectoryStatus(ctx)
	if err != nil {
		return result
	}
	for index, blocker := range blockers {
		if blocker.Node == (rafttransport.NodeID{}) {
			continue
		}
		if intent.Request.Drain.Valid() && blocker.Node == intent.Request.Drain.NodeID {
			result[index].NodeIncarnation = intent.Request.Drain.Incarnation
			continue
		}
		var matched uint64
		for _, node := range nodes {
			if node.NodeID != blocker.Node || (blocker.Revision != 0 && node.Revision != blocker.Revision) {
				continue
			}
			if matched != 0 {
				// A revision-free blocker must not choose between historical
				// incarnations. Omit the field instead of guessing.
				matched = 0
				break
			}
			matched = node.Incarnation
		}
		result[index].NodeIncarnation = matched
		if matched == 0 {
			result[index].NodeID = ""
			result[index].Revision = 0
		}
	}
	return result
}

func (backend *ScalingOperatorBackend) clusterBlockersForReference(_ gateway.ScalingIntent, blockers []gateway.ScalingBlocker, node rafttransport.NodeID, incarnation uint64) []clustercontrol.Blocker {
	result := clusterBlockers(blockers)
	for index, blocker := range blockers {
		if blocker.Node == node {
			result[index].NodeIncarnation = incarnation
		}
	}
	return result
}

func clusterEvidence(evidence gateway.SafeToStopEvidence) *clustercontrol.SafeToStopEvidence {
	if evidence.NodeID == (rafttransport.NodeID{}) || evidence.ScanCatalogGeneration == 0 || evidence.ScanDirectoryRevision == 0 {
		return nil
	}
	digest := hex.EncodeToString(evidence.Digest[:])
	return &clustercontrol.SafeToStopEvidence{NodeID: nodeIDHex(evidence.NodeID), NodeIncarnation: evidence.NodeIncarnation,
		CatalogGeneration: evidence.ScanCatalogGeneration, DirectoryRevision: evidence.ScanDirectoryRevision,
		ServingReplicas: evidence.ServingReplicas, LearnerReplicas: evidence.LearnerReplicas,
		EnrolledTargets: evidence.EnrolledTargets, OutstandingMoves: evidence.OutstandingMoves,
		CatalogVoters: evidence.CatalogVoters, ControlVoters: evidence.ControlVoters,
		GatewayParticipants: evidence.GatewayParticipants, DrainAcknowledged: evidence.DrainAcknowledged,
		RetiredAcknowledged: evidence.RetiredAcknowledged, CatalogControlMigrated: evidence.CatalogControlMigrated, Digest: digest}
}

func boundedClusterControlError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.NewReplacer("\x00", "", "\r", " ", "\n", " ").Replace(err.Error())
	if len(message) > clustercontrol.MaxErrorBytes {
		message = message[:clustercontrol.MaxErrorBytes]
	}
	return message
}

// clusterControlServer is the NDJSON adapter installed on the authenticated
// gateway-client listener. It handles one request per connection, matching
// clustercontrol.Client's bounded transport lifetime.
type clusterControlServer struct {
	backend   clusterControlBackend
	authorize func(context.Context, serviceauthz.Capability) bool
}

func (server *clusterControlServer) Serve(ctx context.Context, connection net.Conn) error {
	if server == nil || server.backend == nil || ctx == nil || connection == nil {
		return errClusterControlUnavailable
	}
	defer connection.Close()
	reader := bufio.NewReaderSize(connection, clustercontrol.MaxRequestBytes+1)
	line, err := reader.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return server.ServeLine(ctx, connection, line)
}

// ServeLine is used by the shared gateway scanner after it has already read
// the first NDJSON line.  It avoids putting bytes back into a net.Conn (which
// would be racy with TLS shutdown) while preserving the same bounded decoder.
func (server *clusterControlServer) ServeLine(ctx context.Context, connection net.Conn, line []byte) error {
	if server == nil || server.backend == nil || ctx == nil || connection == nil {
		return errClusterControlUnavailable
	}
	defer connection.Close()
	if len(line) > clustercontrol.MaxRequestBytes {
		return clustercontrol.ErrFrameBound
	}
	request, decodeErr := clustercontrol.DecodeRequest(line)
	if decodeErr != nil {
		requestID, _ := clustercontrol.NewRequestID()
		response := clustercontrol.Response{Format: clustercontrol.Format, Op: clustercontrol.OpNodes, OK: false, RequestID: requestID, Error: boundedClusterControlError(decodeErr)}
		encoded, encodeErr := clustercontrol.EncodeResponse(response)
		if encodeErr != nil {
			return encodeErr
		}
		_, writeErr := connection.Write(encoded)
		return writeErr
	}
	required := serviceauthz.CapabilityTopology
	if request.Op == clustercontrol.OpJoin || request.Op == clustercontrol.OpRebalance || request.Op == clustercontrol.OpDecommission {
		required |= serviceauthz.CapabilityMembership
	}
	if server.authorize != nil && !server.authorize(ctx, required) {
		response := clustercontrol.Response{Format: clustercontrol.Format, Op: request.Op, RequestID: request.RequestID,
			OK: false, Error: errClusterControlAuth.Error()}
		encoded, encodeErr := clustercontrol.EncodeResponse(response)
		if encodeErr != nil {
			return encodeErr
		}
		_, writeErr := connection.Write(encoded)
		return writeErr
	}
	response := server.backend.ExecuteClusterControl(ctx, request)
	encoded, err := clustercontrol.EncodeResponse(response)
	if err != nil {
		return err
	}
	_, err = connection.Write(encoded)
	return err
}

type clusterControlContextKey struct{}

func withClusterControlServer(ctx context.Context, server *clusterControlServer) context.Context {
	if ctx == nil || server == nil {
		return ctx
	}
	return context.WithValue(ctx, clusterControlContextKey{}, server)
}

func clusterControlServerFromContext(ctx context.Context) *clusterControlServer {
	if ctx == nil {
		return nil
	}
	server, _ := ctx.Value(clusterControlContextKey{}).(*clusterControlServer)
	return server
}

func clusterControlRequestCandidate(line []byte) bool {
	trimmed := strings.TrimSpace(string(line))
	return strings.HasPrefix(trimmed, `{"format":`) && strings.Contains(trimmed, `"op":"cluster_`)
}
