package snapshottransfer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var ErrLearnerInstall = errors.New("snapshottransfer: learner install proof mismatch")

type learnerInstallFaultPoint uint8

const (
	learnerInstallAfterActivation learnerInstallFaultPoint = iota + 1
	learnerInstallAfterWAL
	learnerInstallAfterAdopt
	learnerInstallBeforeHostAdd
)

var learnerInstallFaultHook func(learnerInstallFaultPoint) error

var (
	learnerInstallCloseStage    = (*sqldriver.ReplicatedSnapshotStage).Close
	learnerInstallCloseApply    = (*sqldriver.ReplicatedApply).Close
	learnerInstallCloseDatabase = (*sqldriver.Database).Close
	learnerInstallCloseWAL      = (*raftstore.Store).Close
	learnerInstallCloseRuntime  = (*raftmember.Runtime).Close
)

func learnerInstallFault(point learnerInstallFaultPoint) error {
	if learnerInstallFaultHook == nil {
		return nil
	}
	return learnerInstallFaultHook(point)
}

// LearnerInstallSettlement retains every exclusive owner until cleanup has
// succeeded or Host.Add has accepted the Runtime. Callers must retain one per
// install operation and retry InstallPublishedLearner after any error; no
// failed Close can make an Apply, Database, WAL, or Runtime unreachable.
type LearnerInstallSettlement struct {
	mu       sync.Mutex
	stage    *sqldriver.ReplicatedSnapshotStage
	apply    *sqldriver.ReplicatedApply
	database *sqldriver.Database
	wal      *raftstore.Store
	runtime  *raftmember.Runtime
}

// NodeLearnerInstallFunc is the physical-node adoption hook. It is called
// after the streamed artifact has activated the SQL apply image but before a
// child WAL or a standalone multiraft Host is created. The callback must
// durably publish the node-log checkpoint, adopt the supplied database/apply,
// and publish the runtime into its shared node owner. On error it retains no
// caller-owned handles; the caller is free to retry from the durable cursor.
// Returning the exact runtime identity is the only result exposed to the
// bootstrap journal.
type NodeLearnerInstallFunc func(
	context.Context, Descriptor, replicatedstate.SnapshotArtifactManifest, *pb.Snapshot,
	*sqldriver.Database, *sqldriver.ReplicatedApply,
) (raftmember.RuntimeIdentity, error)

// Close retries cleanup of the exact retained owner. It is monotonic: a
// successfully closed component is cleared, while the first component whose
// Close fails and every dependent owner remain reachable for the next call.
func (s *LearnerInstallSettlement) Close() error { return s.settle() }

func (s *LearnerInstallSettlement) settle() error {
	if s == nil {
		return ErrLearnerInstall
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settleLocked()
}

func (s *LearnerInstallSettlement) settleLocked() error {
	if s.runtime != nil {
		if err := learnerInstallCloseRuntime(s.runtime); err != nil {
			return err
		}
		s.runtime = nil
	}
	if s.stage != nil {
		if err := learnerInstallCloseStage(s.stage); err != nil {
			return err
		}
		s.stage = nil
	}
	if s.apply != nil {
		if err := learnerInstallCloseApply(s.apply); err != nil {
			return err
		}
		s.apply = nil
	}
	if s.database != nil {
		if err := learnerInstallCloseDatabase(s.database); err != nil {
			return err
		}
		s.database = nil
	}
	if s.wal != nil {
		if err := learnerInstallCloseWAL(s.wal); err != nil {
			return err
		}
		s.wal = nil
	}
	return nil
}

// LearnerInstallPlan contains only independently retained cold control-plane
// facts and already-open exclusive local owners. InstallPublishedLearner adds
// the runtime to Host only after artifact authentication, checkpoint-group
// activation, exact WAL-base settlement, and node-incarnation minting.
type LearnerInstallPlan struct {
	Repository *Repository
	Descriptor Descriptor
	Cursor     *replicatedstate.SnapshotCursorStore
	Database   *sqldriver.Database
	// Context and Budget carry cancellation and node-wide pacing into the
	// target-side artifact stage. Context is optional for legacy callers.
	Context context.Context
	Budget  *migrationbudget.Budget

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
	Settlement  *LearnerInstallSettlement
	// NodeInstall selects the node-wide durability/adoption path. It is
	// mutually exclusive with the legacy child-WAL Host path and is used by a
	// running empty physical node so a new group can be adopted without a
	// process restart.
	NodeInstall NodeLearnerInstallFunc
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
	plan.Settlement.mu.Lock()
	defer plan.Settlement.mu.Unlock()
	if err := plan.Settlement.settleLocked(); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	ctx := budgetContext(plan.Context)
	if err := plan.Repository.AttachBudget(plan.Budget); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	manifest, err := plan.Repository.ManifestContext(ctx, plan.Descriptor)
	if err != nil || !proto.Equal(manifest.State.ConfState, plan.ExpectedConfState) {
		return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, err)
	}
	// BuildSnapshotBase authenticates the independently retained static
	// bootstrap against State.BootstrapDigest. Current membership is checked
	// separately above because it legitimately advances beyond that bootstrap.
	var snapshotBase *pb.Snapshot
	if snapshotBase, err = replicatedstate.BuildSnapshotBase(manifest, plan.StaticBootstrap); err != nil {
		return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, err)
	}
	cursor, err := plan.Cursor.Load()
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	var activation sqldriver.ReplicatedChildActivation
	resumed := false
	if len(cursor) != 0 {
		activation, resumed, err = plan.Database.ResumeReplicatedSnapshotActivation(
			plan.SQLIdentity, manifest, plan.StaticBootstrap, plan.ApplyOptions,
		)
		if err != nil {
			return raftmember.RuntimeIdentity{}, err
		}
	}
	if !resumed {
		stage, _, stageErr := plan.Database.OpenReplicatedSnapshotStage(
			plan.SQLIdentity, manifest, cursor, plan.ApplyOptions, plan.StageOptions,
		)
		if stageErr != nil {
			return raftmember.RuntimeIdentity{}, stageErr
		}
		plan.Settlement.stage = stage
		artifact, openErr := plan.Repository.OpenPublished(plan.Descriptor, stage.Offset())
		if openErr != nil {
			return raftmember.RuntimeIdentity{}, openErr
		}
		var lease *migrationbudget.Lease
		if plan.Budget != nil {
			lease, err = plan.Budget.Acquire(ctx)
			if err != nil {
				_ = artifact.Close()
				return raftmember.RuntimeIdentity{}, err
			}
		}
		reader := budgetedReader{ctx: ctx, budget: plan.Budget, lease: lease, reader: artifact}
		_, receiveErr := stage.Receive(reader, plan.Cursor.Persist)
		closeErr := artifact.Close()
		if lease != nil {
			lease.Release()
		}
		if receiveErr != nil || closeErr != nil {
			return raftmember.RuntimeIdentity{}, errors.Join(receiveErr, closeErr)
		}
		activation, err = stage.Activate(plan.StaticBootstrap)
		if err != nil {
			return raftmember.RuntimeIdentity{}, err
		}
	}
	plan.Settlement.stage = nil
	plan.Settlement.apply = activation.Apply
	plan.Settlement.database = plan.Database
	if err = learnerInstallFault(learnerInstallAfterActivation); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	if plan.NodeInstall != nil {
		identity, installErr := plan.NodeInstall(ctx, plan.Descriptor, manifest, snapshotBase,
			plan.Database, activation.Apply)
		if installErr != nil {
			return raftmember.RuntimeIdentity{}, errors.Join(installErr, plan.Settlement.settleLocked())
		}
		if !runtimeMatchesDescriptor(identity, plan.Descriptor) {
			return raftmember.RuntimeIdentity{}, errors.Join(ErrLearnerInstall, plan.Settlement.settleLocked())
		}
		// NodeInstall owns the SQL/apply pair after a successful adoption and
		// has already published it into the shared execution owner.
		plan.Settlement.apply, plan.Settlement.database = nil, nil
		return identity, nil
	}
	wal, err := raftmember.OpenOrCreateStagedChildWAL(
		plan.WALPath, plan.WALIdentity, plan.WALKey,
		plan.Descriptor.Group.TopologyRecoveryEpoch, plan.Authority,
		plan.SQLIdentity, activation, plan.WALOptions,
	)
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	plan.Settlement.wal = wal
	if err = learnerInstallFault(learnerInstallAfterWAL); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	runtime, err := raftmember.AdoptPipelinedStagedRuntime(
		wal, plan.Database, activation.Apply, plan.Descriptor.TargetIncarnation,
	)
	if err != nil {
		if runtime != nil {
			plan.Settlement.apply, plan.Settlement.database, plan.Settlement.wal = nil, nil, nil
			plan.Settlement.runtime = runtime
		}
		return raftmember.RuntimeIdentity{}, err
	}
	plan.Settlement.apply, plan.Settlement.database, plan.Settlement.wal = nil, nil, nil
	plan.Settlement.runtime = runtime
	if err = learnerInstallFault(learnerInstallAfterAdopt); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	identity := runtime.Identity()
	if identity.NodeIncarnation != plan.Descriptor.TargetIncarnation {
		return raftmember.RuntimeIdentity{}, ErrLearnerInstall
	}
	if err = learnerInstallFault(learnerInstallBeforeHostAdd); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	if err := plan.Host.Add(runtime); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	plan.Settlement.runtime = nil
	return identity, nil
}

func validateLearnerInstallPlan(plan LearnerInstallPlan) error {
	d := plan.Descriptor
	b := plan.SQLIdentity.Binding
	w := plan.WALIdentity
	conf := plan.ExpectedConfState
	if plan.Repository == nil || plan.Cursor == nil || plan.Database == nil ||
		plan.Settlement == nil ||
		!d.Valid() || plan.StaticBootstrap == nil || conf == nil ||
		d.Group.ClusterID != b.ClusterID || d.Group.ClusterIncarnation != b.ClusterIncarnation ||
		d.Group.TopologyRecoveryEpoch != b.TopologyRecoveryEpoch ||
		d.Group.ShardIncarnation != b.ShardIncarnation || d.Group.GroupID != b.GroupID ||
		d.TargetMember != b.MemberID || d.TargetStore != b.StoreID ||
		d.SchemaGeneration != b.Authority.SchemaGeneration ||
		!exactLearnerConfState(conf, d.SourceMember, d.TargetMember) {
		return ErrLearnerInstall
	}
	if plan.NodeInstall == nil {
		if plan.Host == nil || plan.WALPath == "" ||
			w.ClusterID != b.ClusterID || w.ClusterIncarnation != b.ClusterIncarnation ||
			w.Distribution != b.Distribution || w.Shard != b.Shard ||
			w.AllocationGeneration != b.AllocationGeneration ||
			w.ShardIncarnation != b.ShardIncarnation || w.GroupID != b.GroupID ||
			w.MemberID != b.MemberID || w.StoreID != b.StoreID {
			return ErrLearnerInstall
		}
	} else if plan.Host != nil {
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
