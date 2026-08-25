package snapshottransfer

import (
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var ErrLearnerInstall = errors.New("snapshottransfer: learner install proof mismatch")

// LearnerInstallPlan contains only independently retained cold control-plane
// facts and already-open exclusive local owners. InstallPublishedLearner adds
// the runtime to Host only after artifact authentication, checkpoint-group
// activation, exact WAL-base settlement, and node-incarnation minting.
type LearnerInstallPlan struct {
	Repository *Repository
	Descriptor Descriptor
	Cursor     *replicatedstate.SnapshotCursorStore
	Database   *sqldriver.Database

	SQLIdentity       sqldriver.ReplicatedShardStoreIdentity
	ApplyOptions      sqldriver.ReplicatedApplyOptions
	StageOptions      replicatedstate.SnapshotArtifactStageOptions
	StaticBootstrap   *pb.Snapshot
	ExpectedConfState *pb.ConfState

	WALPath     string
	WALIdentity raftstore.Identity
	WALKey      raftstore.Key
	WALOptions  raftstore.Options
	Authority   sqldriver.ReplicatedAuthorityProfile
	Host        *multiraft.Host
}

// InstallPublishedLearner streams one already authenticated repository object
// into an empty non-serving replica, crash-settles its exact immutable WAL
// base, mints the requested node incarnation, and transfers the runtime to the
// multiraft host. It does not construct a shard service or serving Owner;
// learner catch-up and promotion remain explicit later barriers.
func InstallPublishedLearner(plan LearnerInstallPlan) (raftmember.RuntimeIdentity, error) {
	if err := validateLearnerInstallPlan(plan); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	manifest, err := plan.Repository.Manifest(plan.Descriptor)
	if err != nil || !proto.Equal(manifest.State.ConfState, plan.ExpectedConfState) {
		return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, err)
	}
	// BuildSnapshotBase authenticates the independently retained static
	// bootstrap against State.BootstrapDigest. Current membership is checked
	// separately above because it legitimately advances beyond that bootstrap.
	if _, err = replicatedstate.BuildSnapshotBase(manifest, plan.StaticBootstrap); err != nil {
		return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, err)
	}
	cursor, err := plan.Cursor.Load()
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	stage, _, err := plan.Database.OpenReplicatedSnapshotStage(
		plan.SQLIdentity, manifest, cursor, plan.ApplyOptions, plan.StageOptions,
	)
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	activationOwned := false
	defer func() {
		if !activationOwned {
			_ = stage.Close()
		}
	}()
	artifact, err := plan.Repository.OpenPublished(plan.Descriptor, stage.Offset())
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	_, receiveErr := stage.Receive(artifact, plan.Cursor.Persist)
	closeErr := artifact.Close()
	if receiveErr != nil || closeErr != nil {
		return raftmember.RuntimeIdentity{}, errors.Join(receiveErr, closeErr)
	}
	activation, err := stage.Activate(plan.StaticBootstrap)
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	activationOwned = true
	wal, err := raftmember.OpenOrCreateStagedChildWAL(
		plan.WALPath, plan.WALIdentity, plan.WALKey,
		plan.Descriptor.Group.TopologyRecoveryEpoch, plan.Authority,
		plan.SQLIdentity, activation, plan.WALOptions,
	)
	if err != nil {
		_ = activation.Apply.Close()
		return raftmember.RuntimeIdentity{}, err
	}
	currentIncarnation := wal.CurrentIncarnation()
	if currentIncarnation == math.MaxUint64 ||
		currentIncarnation+1 != plan.Descriptor.TargetIncarnation {
		_ = wal.Close()
		_ = activation.Apply.Close()
		return raftmember.RuntimeIdentity{}, ErrLearnerInstall
	}
	runtime, err := raftmember.AdoptRuntime(wal, plan.Database, activation.Apply)
	if err != nil {
		if runtime == nil {
			_ = wal.Close()
		}
		return raftmember.RuntimeIdentity{}, err
	}
	identity := runtime.Identity()
	if identity.NodeIncarnation != plan.Descriptor.TargetIncarnation {
		return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, runtime.Close())
	}
	if err := plan.Host.Add(runtime); err != nil {
		return raftmember.RuntimeIdentity{}, errors.Join(err, runtime.Close())
	}
	return identity, nil
}

func validateLearnerInstallPlan(plan LearnerInstallPlan) error {
	d := plan.Descriptor
	b := plan.SQLIdentity.Binding
	w := plan.WALIdentity
	conf := plan.ExpectedConfState
	if plan.Repository == nil || plan.Cursor == nil || plan.Database == nil || plan.Host == nil ||
		!d.Valid() || plan.WALPath == "" || plan.StaticBootstrap == nil || conf == nil ||
		d.Group.ClusterID != b.ClusterID || d.Group.ClusterIncarnation != b.ClusterIncarnation ||
		d.Group.TopologyRecoveryEpoch != b.TopologyRecoveryEpoch ||
		d.Group.ShardIncarnation != b.ShardIncarnation || d.Group.GroupID != b.GroupID ||
		d.TargetMember != b.MemberID || d.TargetStore != b.StoreID ||
		d.SchemaGeneration != b.Authority.SchemaGeneration ||
		w.ClusterID != b.ClusterID || w.ClusterIncarnation != b.ClusterIncarnation ||
		w.Distribution != b.Distribution || w.Shard != b.Shard ||
		w.AllocationGeneration != b.AllocationGeneration ||
		w.ShardIncarnation != b.ShardIncarnation || w.GroupID != b.GroupID ||
		w.MemberID != b.MemberID || w.StoreID != b.StoreID ||
		!exactLearnerConfState(conf, d.SourceMember, d.TargetMember) {
		return ErrLearnerInstall
	}
	meta := plan.StaticBootstrap.GetMetadata()
	if meta.GetIndex() == 0 || meta.GetTerm() == 0 || meta.GetConfState() == nil {
		return fmt.Errorf("%w: static bootstrap", ErrLearnerInstall)
	}
	return nil
}

func exactLearnerConfState(conf *pb.ConfState, source, target uint64) bool {
	if conf == nil || source == 0 || target == 0 || source == target ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return false
	}
	var sourceVoter, targetLearner bool
	for _, member := range conf.GetVoters() {
		if member == target {
			return false
		}
		sourceVoter = sourceVoter || member == source
	}
	for _, member := range conf.GetLearners() {
		if member == source {
			return false
		}
		targetLearner = targetLearner || member == target
	}
	return sourceVoter && targetLearner
}
