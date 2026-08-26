package splitcontroller

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ChildRuntimeAdopter is the live multi-Raft ownership boundary. AdoptChild
// must take ownership of runtime only when it returns nil. A failed call leaves
// ownership with the caller so it can close the runtime before recovery.
type ChildRuntimeAdopter interface {
	AdoptSplitChild(context.Context, OperationID, uint8, PreparedChildRuntime) error
}

// PreparedChildRuntime keeps the adopted Runtime and its exact activated
// state-machine read capabilities in one ownership handoff. These references
// all originate from the same certified child activation.
type PreparedChildRuntime struct {
	Runtime *raftmember.Runtime
	Apply   *sqldriver.ReplicatedApply
}

// LocalChildLifecycleOptions freezes every local resource needed to transfer a
// sealed, non-serving child into its final Raft group. The SQL stage and
// database are the exact exclusive handles opened for the child's final store;
// no row image is copied at this boundary.
type LocalChildLifecycleOptions struct {
	Child    uint8
	Replica  *ChildReplicaTarget
	Stage    *sqldriver.ReplicatedChildStage
	Database *sqldriver.Database

	StaticBootstrap *pb.Snapshot
	ArtifactOptions replicatedstate.SnapshotArtifactOptions

	WALPath               string
	WALIdentity           raftstore.Identity
	WALKey                raftstore.Key
	WALOptions            raftstore.Options
	TopologyRecoveryEpoch uint64
	Authority             sqldriver.ReplicatedAuthorityProfile
	SQL                   sqldriver.ReplicatedShardStoreIdentity

	Adopter ChildRuntimeAdopter
}

// LocalChildLifecycle executes the three ownership-changing child actions in
// order. Durable SQL and WAL authorities remain the recovery journal; these
// process-local pointers only prevent duplicate ownership inside one process.
type LocalChildLifecycle struct {
	mu sync.Mutex

	options    LocalChildLifecycleOptions
	activation sqldriver.ReplicatedChildActivation
	wal        *raftstore.Store
	adopted    raftmember.RuntimeIdentity
}

func NewLocalChildLifecycle(options LocalChildLifecycleOptions) (*LocalChildLifecycle, error) {
	if options.Stage == nil || options.Database == nil || options.StaticBootstrap == nil ||
		options.Adopter == nil || options.Child >= autosplit.MaxSplitChildren || options.WALPath == "" ||
		!filepath.IsAbs(options.WALPath) || filepath.Clean(options.WALPath) != options.WALPath ||
		options.TopologyRecoveryEpoch == 0 || options.SQL.LogID == ([16]byte{}) {
		return nil, ErrRuntimeStore
	}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(options.ArtifactOptions); err != nil {
		return nil, errors.Join(ErrRuntimeStore, err)
	}
	options.StaticBootstrap = proto.Clone(options.StaticBootstrap).(*pb.Snapshot)
	if options.Replica != nil {
		copy := *options.Replica
		copy.SQL = options.Replica.SQL.Clone()
		options.Replica = &copy
	}
	options.WALPath = strings.Clone(options.WALPath)
	options.WALIdentity.Distribution = strings.Clone(options.WALIdentity.Distribution)
	options.WALIdentity.Shard = strings.Clone(options.WALIdentity.Shard)
	options.WALKey.ID = strings.Clone(options.WALKey.ID)
	options.WALKey.Wrapped = bytes.Clone(options.WALKey.Wrapped)
	options.SQL = options.SQL.Clone()
	return &LocalChildLifecycle{options: options}, nil
}

// ExecuteActivateChild performs the O(1) certified-image handoff into normal
// replicated apply. An exact retry in the same process is a read-only proof;
// after a crash OpenReplicatedChildStage reconstructs the durable activation
// intent and Activate settles the same state.
func (l *LocalChildLifecycle) ExecuteActivateChild(
	plan *Plan,
	certificate rangesplit.CutoverCertificate,
) error {
	if l == nil || plan == nil {
		return ErrInvalidPlan
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	target, ok := plan.Target(l.options.Child)
	if !ok || !l.matchesTarget(target) ||
		plan.partitioner.VerifyCutoverCertificate(certificate) != nil {
		return ErrTopologyConflict
	}
	if l.adopted != (raftmember.RuntimeIdentity{}) || l.wal != nil {
		return ErrTopologyConflict
	}
	if l.activation.Apply != nil {
		return l.validateActivation(target, certificate)
	}
	activation, err := l.options.Stage.Activate(
		certificate, l.options.StaticBootstrap, l.options.ArtifactOptions,
	)
	if err != nil {
		return err
	}
	l.activation = activation
	return l.validateActivation(target, certificate)
}

// ExecuteCreateChildWAL publishes or reopens the exact immutable WAL base. It
// never mints a runtime incarnation and therefore remains non-serving.
func (l *LocalChildLifecycle) ExecuteCreateChildWAL(
	plan *Plan,
	certificate rangesplit.CutoverCertificate,
) error {
	if l == nil || plan == nil {
		return ErrInvalidPlan
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	target, ok := plan.Target(l.options.Child)
	if !ok || !l.matchesTarget(target) || l.adopted != (raftmember.RuntimeIdentity{}) ||
		l.validateActivation(target, certificate) != nil {
		return ErrTopologyConflict
	}
	if l.wal != nil {
		binding, err := raftmember.BindingFromWAL(l.wal, l.options.Authority)
		if err != nil || binding != target.SQL.Binding {
			return errors.Join(ErrTopologyConflict, err)
		}
		return nil
	}
	wal, err := raftmember.OpenOrCreateStagedChildWAL(
		l.options.WALPath, l.options.WALIdentity, l.options.WALKey,
		l.options.TopologyRecoveryEpoch, l.options.Authority, l.options.SQL,
		l.activation, l.options.WALOptions,
	)
	if err != nil {
		return err
	}
	l.wal = wal
	return nil
}

// ExecuteAdoptChildRuntime mints one runtime incarnation and transfers it to
// the serving multi-Raft owner. Publication is still forbidden until
// Reconcile observes an applied quorum for the exact child group.
func (l *LocalChildLifecycle) ExecuteAdoptChildRuntime(
	ctx context.Context,
	plan *Plan,
) error {
	if l == nil || ctx == nil || plan == nil {
		return ErrInvalidPlan
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	target, ok := plan.Target(l.options.Child)
	if !ok || !l.matchesTarget(target) {
		return ErrTopologyConflict
	}
	if l.adopted != (raftmember.RuntimeIdentity{}) {
		if l.runtimeIdentityMatches(target, l.adopted) {
			return nil
		}
		return ErrTopologyConflict
	}
	if l.wal == nil || l.activation.Apply == nil {
		return ErrTopologyConflict
	}
	runtime, err := raftmember.AdoptRuntime(l.wal, l.options.Database, l.activation.Apply)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		return err
	}
	identity := runtime.Identity()
	if !l.runtimeIdentityMatches(target, identity) {
		return errors.Join(ErrTopologyConflict, runtime.Close())
	}
	if err = l.options.Adopter.AdoptSplitChild(
		ctx, plan.OperationID(), l.options.Child, PreparedChildRuntime{
			Runtime: runtime, Apply: l.activation.Apply,
		},
	); err != nil {
		return errors.Join(err, runtime.Close())
	}
	// Ownership moved to Adopter. Clear every transferred handle before
	// publishing the process-local observation.
	l.adopted = identity
	l.wal = nil
	l.activation.Apply = nil
	l.options.Database = nil
	return nil
}

func (l *LocalChildLifecycle) matchesTarget(target ChildTarget) bool {
	if l == nil || target.Child != l.options.Child ||
		target.TopologyRecoveryEpoch != l.options.TopologyRecoveryEpoch ||
		target.Authority != l.options.Authority {
		return false
	}
	if l.options.Replica == nil {
		return target.WAL == l.options.WALIdentity && target.SQL.Equal(l.options.SQL)
	}
	return l.options.Replica.WAL == l.options.WALIdentity &&
		l.options.Replica.SQL.Equal(l.options.SQL) &&
		targetMatchesPreparedReplica(target, *l.options.Replica)
}

func (l *LocalChildLifecycle) runtimeIdentityMatches(
	target ChildTarget, identity raftmember.RuntimeIdentity,
) bool {
	if l.options.Replica == nil {
		return runtimeIdentityMatches(target, identity)
	}
	local := cloneChildTarget(target)
	local.WAL = l.options.Replica.WAL
	local.SQL = l.options.Replica.SQL.Clone()
	return runtimeIdentityMatches(local, identity)
}

func (l *LocalChildLifecycle) validateActivation(
	target ChildTarget,
	certificate rangesplit.CutoverCertificate,
) error {
	activation := l.activation
	if activation.Apply == nil || activation.SnapshotBase == nil ||
		activation.ApplyIdentity == (sqldriver.ReplicatedApplyIdentity{}) {
		return ErrTopologyConflict
	}
	identity, identityErr := activation.Apply.Identity()
	profile, profileErr := activation.Apply.CapacityQualificationProfile()
	base, baseErr := replicatedstate.OpenSnapshotBase(activation.SnapshotBase)
	if identityErr != nil || profileErr != nil || baseErr != nil ||
		identity != activation.ApplyIdentity || profile.Binding != l.options.SQL.Binding ||
		!profile.Initialized || profile.Applied != certificate.SourceCut().Applied ||
		profile.SessionEpochHighWater != certificate.SourceCut().Applied ||
		profile.SessionCount != 0 || profile.SessionSlotCount != 0 ||
		base.Manifest.State.Applied != certificate.SourceCut().Applied ||
		activation.ArtifactManifest.Digest != base.Manifest.Digest {
		return errors.Join(ErrTopologyConflict, identityErr, profileErr, baseErr)
	}
	return nil
}
