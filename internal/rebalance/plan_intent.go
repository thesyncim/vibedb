package rebalance

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	vibejson "github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// MaxPlanIntentBytes bounds the canonical control-plane record. The binary
// certificate is base64 encoded by vibejson, so twice its strict binary bound
// leaves room for the fixed request and operation identity without inventing a
// participant-count restriction.
const MaxPlanIntentBytes = 2 * replicatedstate.MaxSnapshotBaseCertificateBytes

var ErrPlanIntent = errors.New("rebalance: invalid persisted replica move intent")

type persistedPlanIntent struct {
	Operation        [32]byte             `json:"operation"`
	SourceGeneration uint64               `json:"source_generation"`
	Request          persistedMoveRequest `json:"request"`
	Certificate      []byte               `json:"certificate"`
}

type persistedMoveRequest struct {
	Distribution         distribution.DistributionName `json:"distribution"`
	Shard                distribution.ShardID          `json:"shard"`
	ClusterID            [16]byte                      `json:"cluster_id"`
	Incarnation          [16]byte                      `json:"cluster_incarnation"`
	Recovery             uint64                        `json:"topology_recovery_epoch"`
	ShardID              [16]byte                      `json:"shard_incarnation"`
	GroupID              [16]byte                      `json:"group_id"`
	RetiringMember       uint64                        `json:"retiring_member"`
	SnapshotSourceMember uint64                        `json:"snapshot_source_member"`
	TargetMember         uint64                        `json:"target_member"`
	Source               distribution.EndpointID       `json:"source"`
	Target               distribution.EndpointID       `json:"target"`
}

// AppendPlanIntent appends the unique canonical restart image for a replica
// move. A bound plan carries the exact authenticated snapshot-base envelope;
// an unbound plan has one canonical empty certificate.
func AppendPlanIntent(dst []byte, catalog *gateway.Snapshot, plan *Plan) ([]byte, error) {
	if catalog == nil || plan == nil || plan.operation == (OperationID{}) ||
		(catalog.Generation() != plan.catalogGeneration &&
			catalog.Generation() != plan.nextCatalogGeneration &&
			catalog.Generation() != plan.postRemoveGeneration) {
		return dst, ErrPlanIntent
	}
	if _, err := plan.catalogStage(catalog); err != nil {
		return dst, errors.Join(err, ErrPlanIntent)
	}
	intent := persistedPlanIntent{
		Operation: [32]byte(plan.operation), SourceGeneration: plan.catalogGeneration,
		Request: persistMoveRequest(plan.request),
	}
	if plan.baseBound {
		if plan.certificate.Digest == ([32]byte{}) {
			return dst, ErrPlanIntent
		}
		snapshot, err := replicatedstate.BuildSnapshotBase(
			plan.certificate.Manifest, plan.certificate.StaticBootstrap,
		)
		if err != nil {
			return dst, errors.Join(err, ErrPlanIntent)
		}
		intent.Certificate, err = proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
		if err != nil {
			return dst, errors.Join(err, ErrPlanIntent)
		}
	}
	raw, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, errors.Join(err, ErrPlanIntent)
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxPlanIntentBytes {
		return dst[:start], errors.Join(err, ErrPlanIntent)
	}
	return dst, nil
}

// AppendReplicaMoveIntent appends the immutable, canonical operation intent
// stored in the replicated operation journal. Snapshot-base certificates are
// deliberately excluded: they can be much larger than the journal's bounded
// intent cell and become available only after the learner exists. Recovery
// obtains that already-authenticated durable certificate from the injected
// runtime observer instead of rewriting the operation identity or intent.
func AppendReplicaMoveIntent(dst []byte, catalog *gateway.Snapshot, plan *Plan) ([]byte, error) {
	if catalog == nil || plan == nil || plan.operation == (OperationID{}) ||
		(catalog.Generation() != plan.catalogGeneration &&
			catalog.Generation() != plan.nextCatalogGeneration) {
		return dst, ErrPlanIntent
	}
	if _, err := plan.catalogStage(catalog); err != nil {
		return dst, errors.Join(err, ErrPlanIntent)
	}
	return appendPersistedPlanIntent(dst, persistedPlanIntent{
		Operation: [32]byte(plan.operation), SourceGeneration: plan.catalogGeneration,
		Request: persistMoveRequest(plan.request),
	})
}

// OpenPlanIntent validates canonical uniqueness and reconstructs the immutable
// move against either its source catalog or its already-published successor.
func OpenPlanIntent(
	raw []byte,
	catalog *gateway.Snapshot,
	publication raftmodel.Publication,
) (*Plan, error) {
	if len(raw) == 0 || len(raw) > MaxPlanIntentBytes || catalog == nil {
		return nil, ErrPlanIntent
	}
	var intent persistedPlanIntent
	if err := vibejson.Unmarshal(raw, &intent); err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	canonical, err := vibejson.Marshal(&intent)
	if err != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	request := openMoveRequest(intent.Request)
	if err != nil || !bytes.Equal(raw, canonical) || intent.Operation == ([32]byte{}) ||
		intent.SourceGeneration == 0 || invalidMoveRequest(request) ||
		len(intent.Certificate) == 0 && intent.Certificate != nil {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	var plan *Plan
	if len(intent.Certificate) == 0 {
		if catalog.Generation() != intent.SourceGeneration {
			return nil, ErrPlanIntent
		}
		plan, err = PlanReplicaMove(catalog, publication, request)
	} else {
		snapshot := new(pb.Snapshot)
		if proto.Unmarshal(intent.Certificate, snapshot) != nil {
			return nil, ErrPlanIntent
		}
		encoded, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
		if marshalErr != nil || !bytes.Equal(encoded, intent.Certificate) {
			return nil, errors.Join(marshalErr, ErrPlanIntent)
		}
		plan, err = RecoverReplicaMove(catalog, publication, request, snapshot)
	}
	if err != nil || plan == nil || plan.catalogGeneration != intent.SourceGeneration ||
		[32]byte(plan.OperationID()) != intent.Operation {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	return plan, nil
}

// OpenReplicaMoveIntent reconstructs the immutable journal intent against one
// observed controller cut. Before snapshot creation, membership is sufficient
// to rebuild the plan. Afterwards the observer supplies the authenticated
// certificate retained by the shard runtime, allowing recovery after
// promotion, catalog cutover, source removal, and controller restart without
// copying the certificate through the catalog operation record.
func OpenReplicaMoveIntent(
	raw []byte,
	catalog *gateway.Snapshot,
	publication raftmodel.Publication,
	certificate *replicatedstate.SnapshotBaseCertificate,
) (*Plan, error) {
	if catalog == nil {
		return nil, ErrPlanIntent
	}
	intent, request, err := openPersistedPlanIntent(raw)
	if err != nil || len(intent.Certificate) != 0 {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	var plan *Plan
	if certificate == nil {
		if catalog.Generation() != intent.SourceGeneration {
			return nil, ErrPlanIntent
		}
		plan, err = PlanReplicaMove(catalog, publication, request)
	} else {
		plan, err = recoverReplicaMoveCertificate(catalog, publication, request, *certificate)
	}
	if err != nil || plan == nil || plan.catalogGeneration != intent.SourceGeneration ||
		[32]byte(plan.OperationID()) != intent.Operation {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	return plan, nil
}

func appendPersistedPlanIntent(dst []byte, intent persistedPlanIntent) ([]byte, error) {
	raw, err := vibejson.Marshal(&intent)
	if err != nil {
		return dst, errors.Join(err, ErrPlanIntent)
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxPlanIntentBytes {
		return dst[:start], errors.Join(err, ErrPlanIntent)
	}
	return dst, nil
}

func openPersistedPlanIntent(raw []byte) (persistedPlanIntent, MoveRequest, error) {
	if len(raw) == 0 || len(raw) > MaxPlanIntentBytes {
		return persistedPlanIntent{}, MoveRequest{}, ErrPlanIntent
	}
	var intent persistedPlanIntent
	if err := vibejson.Unmarshal(raw, &intent); err != nil {
		return persistedPlanIntent{}, MoveRequest{}, errors.Join(err, ErrPlanIntent)
	}
	canonical, err := vibejson.Marshal(&intent)
	if err == nil {
		canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	}
	request := openMoveRequest(intent.Request)
	if err != nil || !bytes.Equal(raw, canonical) || intent.Operation == ([32]byte{}) ||
		intent.SourceGeneration == 0 || invalidMoveRequest(request) ||
		len(intent.Certificate) == 0 && intent.Certificate != nil {
		return persistedPlanIntent{}, MoveRequest{}, errors.Join(err, ErrPlanIntent)
	}
	return intent, request, nil
}

func persistMoveRequest(request MoveRequest) persistedMoveRequest {
	return persistedMoveRequest{
		Distribution: request.Distribution, Shard: request.Shard,
		ClusterID: request.Group.ClusterID, Incarnation: request.Group.ClusterIncarnation,
		Recovery: request.Group.TopologyRecoveryEpoch, ShardID: request.Group.ShardIncarnation,
		GroupID: request.Group.GroupID, RetiringMember: request.RetiringMember,
		SnapshotSourceMember: request.SnapshotSourceMember,
		TargetMember:         request.TargetMember, Source: request.Source, Target: request.Target,
	}
}

func openMoveRequest(request persistedMoveRequest) MoveRequest {
	return MoveRequest{
		Distribution: request.Distribution, Shard: request.Shard,
		Group: raftmember.GroupKey{
			ClusterID: request.ClusterID, ClusterIncarnation: request.Incarnation,
			TopologyRecoveryEpoch: request.Recovery, ShardIncarnation: request.ShardID,
			GroupID: request.GroupID,
		},
		RetiringMember:       request.RetiringMember,
		SnapshotSourceMember: request.SnapshotSourceMember,
		TargetMember:         request.TargetMember,
		Source:               request.Source, Target: request.Target,
	}
}
