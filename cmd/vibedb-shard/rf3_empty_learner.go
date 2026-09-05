package main

// This file composes the certified snapshot path for an already running
// empty physical node. Preparation reserves only SQL/schema state. Once the
// source publishes a descriptor after AddLearner, this factory opens the
// durable receiver, streams the artifact, registers the exact checkpoint in
// the node log, and hands the adopted runtime to the shared execution peer.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const rf3DynamicRepositoryMaxBytes = uint64(1) << 50

type rf3DynamicLearnerFactory struct {
	mu       sync.Mutex
	runtime  *rf3EmptyNodeRuntime
	owner    *rf3NodeOwner
	root     string
	profile  *rafttransport.PeerTLS
	policy   *serviceauthz.Policy
	gate     *serviceauthz.Gate
	budget   *migrationbudget.Budget
	deadline rafttransport.DeadlineFunc
	services map[raftmember.GroupKey]*rf3DynamicLearnerService
	closed   bool
}

type rf3DynamicLearnerService struct {
	service     *snapshottransfer.BootstrapControlService
	repository  *snapshottransfer.Repository
	cursor      *replicatedstate.SnapshotCursorStore
	journal     *snapshottransfer.BootstrapFileJournal
	descriptor  snapshottransfer.Descriptor
	reservation string
	installer   *rf3DynamicLearnerInstaller
}

func newRF3DynamicLearnerFactory(
	runtime *rf3EmptyNodeRuntime, owner *rf3NodeOwner, manifest rf3Manifest,
	profile *rafttransport.PeerTLS, policy *serviceauthz.Policy, gate *serviceauthz.Gate,
	budget *migrationbudget.Budget, deadline rafttransport.DeadlineFunc,
) (*rf3DynamicLearnerFactory, error) {
	if runtime == nil || owner == nil || manifest.ReplicaControl.SourceDataRoot == "" ||
		profile == nil || policy == nil || gate == nil || budget == nil || deadline == nil {
		return nil, nodecontrol.ErrControl
	}
	return &rf3DynamicLearnerFactory{
		runtime: runtime, owner: owner, root: manifest.ReplicaControl.SourceDataRoot,
		profile: profile, policy: policy, gate: gate, budget: budget, deadline: deadline,
		services: make(map[raftmember.GroupKey]*rf3DynamicLearnerService),
	}, nil
}

func (factory *rf3DynamicLearnerFactory) Close() error {
	if factory == nil {
		return nil
	}
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.closed {
		return nil
	}
	factory.closed = true
	var result error
	for group, item := range factory.services {
		if item == nil {
			continue
		}
		result = errors.Join(result, item.journal.Close(), item.cursor.Close(), item.repository.Close())
		delete(factory.services, group)
	}
	return result
}

// Register publishes one real bootstrap service for the exact currently
// activated reservation. It is called by the post-AddLearner descriptor
// path; Prepare/Adopt only activate the receiver reservation and cannot use
// this method without a certified descriptor.
func (factory *rf3DynamicLearnerFactory) Register(
	ctx context.Context, intent gateway.GroupEnrollmentIntent,
	proof gateway.PreparedReplicaProof,
	descriptor snapshottransfer.Descriptor,
) error {
	if factory == nil || ctx == nil || !rf3BootstrapIntentProofMatches(intent, proof) ||
		descriptor.Group != intent.Group || descriptor.TargetMember != intent.Target.Member ||
		descriptor.TargetStore != intent.Target.StoreID || descriptor.TargetIncarnation != intent.Target.NodeIncarnation {
		return nodecontrol.ErrInvalidProof
	}
	factory.mu.Lock()
	if factory.closed {
		factory.mu.Unlock()
		return nodecontrol.ErrControl
	}
	if prior := factory.services[intent.Group]; prior != nil {
		if prior.descriptor != descriptor {
			factory.mu.Unlock()
			return nodecontrol.ErrConflict
		}
		factory.mu.Unlock()
		return factory.runtime.receivers.Register(ctx, intent, proof, prior.service)
	}
	factory.mu.Unlock()

	service, resources, err := factory.openService(ctx, intent, proof, descriptor)
	if err != nil {
		return err
	}
	if err = persistRF3EnrollmentDescriptor(
		rf3EnrollmentReservationPath(factory.root, intent.IntentID), intent, descriptor,
	); err != nil {
		_ = resources.close()
		return err
	}
	if err = factory.runtime.receivers.Register(ctx, intent, proof, service); err != nil {
		_ = resources.close()
		return err
	}
	factory.mu.Lock()
	if factory.closed {
		factory.mu.Unlock()
		_ = resources.close()
		return nodecontrol.ErrControl
	}
	if prior := factory.services[intent.Group]; prior != nil {
		factory.mu.Unlock()
		_ = resources.close()
		return nil
	}
	factory.services[intent.Group] = resources
	factory.mu.Unlock()
	return nil
}

// Recover reopens only descriptor-backed enrollments whose current authority
// still says Enrolled (or Moving).  Reserved/Prepared rows remain cold, and a
// Complete row is deliberately ignored so an old runtime receipt can never
// resurrect a retired group.  The descriptor and runtime receipts are
// evidence for restart repair; the remote reader remains the authority.
func (factory *rf3DynamicLearnerFactory) Recover(ctx context.Context) error {
	if factory == nil || ctx == nil || factory.runtime == nil || factory.runtime.reader == nil {
		return nodecontrol.ErrControl
	}
	entries, err := os.ReadDir(filepath.Join(factory.root, "enrollments"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > maxRF3ManifestGroups {
		return nodecontrol.ErrBound
	}
	for _, entry := range entries {
		if err = context.Cause(ctx); err != nil {
			return err
		}
		if !entry.IsDir() || len(entry.Name()) != 64 {
			return nodecontrol.ErrJournalCorrupt
		}
		intentRoot := filepath.Join(factory.root, "enrollments", entry.Name())
		raw, readErr := readRF3BoundedFile(filepath.Join(intentRoot, rf3EnrollmentDescriptorFile), 256<<10)
		if errors.Is(readErr, os.ErrNotExist) {
			// Preparation has not crossed the descriptor fence yet.
			continue
		}
		if readErr != nil {
			return readErr
		}
		var descriptorReceipt rf3EnrollmentDescriptorReceipt
		if readErr = vibejson.Unmarshal(raw, &descriptorReceipt); readErr != nil || descriptorReceipt.Kind != rf3EnrollmentPayloadKind ||
			len(descriptorReceipt.Descriptor) != snapshottransfer.DescriptorBytes {
			return errors.Join(nodecontrol.ErrJournalCorrupt, readErr)
		}
		var intentID [32]byte
		copy(intentID[:], descriptorReceipt.IntentID[:])
		intent, readIntentErr := factory.runtime.reader.ReadEnrollmentIntent(ctx, intentID)
		if readIntentErr != nil {
			return errors.Join(nodecontrol.ErrStale, readIntentErr)
		}
		if intent.State < gateway.EnrollmentEnrolled || intent.State == gateway.EnrollmentComplete || intent.Proof == nil {
			continue
		}
		descriptor, found, descriptorErr := readRF3EnrollmentDescriptor(intentRoot, intent)
		if descriptorErr != nil {
			return descriptorErr
		}
		if !found {
			return nodecontrol.ErrJournalCorrupt
		}
		if err = factory.runtime.receivers.Activate(ctx, intent, *intent.Proof); err != nil {
			return err
		}
		if err = factory.Register(ctx, intent, *intent.Proof, descriptor); err != nil {
			return err
		}
		factory.mu.Lock()
		resources := factory.services[intent.Group]
		factory.mu.Unlock()
		if resources != nil && resources.installer != nil {
			if err = resources.installer.RecoverInstalled(ctx, descriptor); err != nil {
				return err
			}
		}
	}
	return nil
}

func (factory *rf3DynamicLearnerFactory) openService(
	ctx context.Context, intent gateway.GroupEnrollmentIntent,
	proof gateway.PreparedReplicaProof,
	descriptor snapshottransfer.Descriptor,
) (*snapshottransfer.BootstrapControlService, *rf3DynamicLearnerService, error) {
	reservationRoot := rf3EnrollmentReservationPath(factory.root, intent.IntentID)
	reservation, found, err := readRF3EnrollmentReservation(reservationRoot)
	if err != nil || !found {
		return nil, nil, nodecontrol.ErrNotPrepared
	}
	if reservation.IntentID != intent.IntentID || reservation.IntentDigest != intent.Digest() ||
		reservation.Group != intent.Group || reservation.TargetMember != intent.Target.Member ||
		reservation.TargetNode != intent.Target.Node || reservation.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		reservation.TargetStoreID != intent.Target.StoreID || proof.EnrollmentDigest != proof.ComputedEnrollmentDigest() {
		return nil, nil, nodecontrol.ErrConflict
	}
	rawSpec, err := readRF3BoundedFile(filepath.Join(reservationRoot, rf3EnrollmentSpecFile), maxRF3EnrollmentPayloadBytes)
	if err != nil {
		return nil, nil, err
	}
	spec, err := nodecontrol.OpenPreparationSpec(rawSpec)
	if err != nil {
		return nil, nil, err
	}
	if err = spec.ValidateAgainst(intent); err != nil {
		return nil, nil, err
	}
	if len(spec.SourceBootstrap) == 0 || sha256.Sum256(spec.SourceBootstrap) != spec.SourceBootstrapDigest {
		return nil, nil, nodecontrol.ErrStale
	}
	var staticBootstrap pb.Snapshot
	if err = proto.Unmarshal(spec.SourceBootstrap, &staticBootstrap); err != nil ||
		staticBootstrap.GetMetadata() == nil || staticBootstrap.GetMetadata().GetConfState() == nil {
		return nil, nil, errors.Join(nodecontrol.ErrControl, err)
	}
	canonicalBootstrap, err := proto.MarshalOptions{Deterministic: true}.Marshal(&staticBootstrap)
	if err != nil || !bytes.Equal(canonicalBootstrap, spec.SourceBootstrap) {
		return nil, nil, nodecontrol.ErrControl
	}
	sourceNode, sourceAddress, ok := rf3DynamicSource(spec, descriptor.SourceMember)
	if !ok || sourceAddress == "" {
		// Snapshot addresses are explicit input. A peer/control address is not
		// silently reused for bulk transfer.
		return nil, nil, snapshottransfer.ErrBootstrapUnauthorized
	}
	if descriptor.SchemaGeneration != reservation.SQL.Binding.Authority.SchemaGeneration ||
		descriptor.TargetMember != reservation.SQL.Binding.MemberID ||
		descriptor.TargetStore != reservation.SQL.Binding.StoreID {
		return nil, nil, nodecontrol.ErrConflict
	}
	openOptions := sqldriver.ReplicatedOpenOptions{WriterLockContext: ctx, WriterLockDeadline: factory.deadline()}
	database, err := sqldriver.OpenReplicatedSnapshotTarget(filepath.Join(reservationRoot, "member.vdb"),
		reservation.SQL, reservation.Apply, openOptions)
	if err != nil {
		return nil, nil, err
	}
	closeDatabase := func(cause error) (*snapshottransfer.BootstrapControlService, *rf3DynamicLearnerService, error) {
		return nil, nil, errors.Join(cause, database.Close())
	}
	repository, err := snapshottransfer.OpenRepository(filepath.Join(reservationRoot, "snapshot-repository"), snapshottransfer.Limits{
		MaxArtifacts: 1, MaxArtifactBytes: rf3DynamicRepositoryMaxBytes,
		MaxDiskBytes: rf3DynamicRepositoryMaxBytes + snapshottransfer.DescriptorBytes + 2<<20,
		Budget:       factory.budget,
	})
	if err != nil {
		return closeDatabase(err)
	}
	cursor, err := replicatedstate.OpenSnapshotCursorStore(filepath.Join(reservationRoot, "snapshot.cursor"))
	if err != nil {
		_ = repository.Close()
		return closeDatabase(err)
	}
	journal, err := snapshottransfer.OpenBootstrapFileJournal(filepath.Join(reservationRoot, "bootstrap-journal"), 4)
	if err != nil {
		_ = cursor.Close()
		_ = repository.Close()
		return closeDatabase(err)
	}
	opener := rafttransport.TLSSnapshotStreamOpener{
		TLS: factory.profile,
		Open: func(openCtx context.Context, node rafttransport.NodeID) (net.Conn, error) {
			if node != sourceNode {
				return nil, snapshottransfer.ErrBootstrapUnauthorized
			}
			return (&net.Dialer{}).DialContext(openCtx, "tcp", sourceAddress)
		},
		HandshakeDeadline: factory.deadline,
	}
	receiver := &snapshottransfer.Receiver{Repository: repository, Opener: opener, Budget: factory.budget,
		ReadDeadline: factory.deadline, WriteDeadline: factory.deadline}
	installer := &rf3DynamicLearnerInstaller{
		factory: factory, intent: intent, proof: proof, spec: spec,
		reservationRoot: reservationRoot, repository: repository, cursor: cursor,
		base: reservation.SQL, applyIdentity: reservation.Apply, staticBootstrap: &staticBootstrap,
	}
	service, err := snapshottransfer.NewBootstrapControlService(snapshottransfer.BootstrapControlOptions{
		Journal: journal, Receiver: receiver, Installer: installer, Releaser: repository,
		Authorize: func(identity rafttransport.PeerIdentity, request snapshottransfer.BootstrapRequest) bool {
			return identity.TrustDomain == factory.profile.LocalIdentity().TrustDomain &&
				factory.gate.Check(identity.Node, factory.gate.Generation(), serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow &&
				request.Descriptor.Group == intent.Group && request.Descriptor.TargetMember == intent.Target.Member &&
				request.Descriptor.TargetStore == intent.Target.StoreID && request.Descriptor.TargetIncarnation == intent.Target.NodeIncarnation
		},
		SourceNode: func(candidate snapshottransfer.Descriptor) (rafttransport.NodeID, bool) {
			return sourceNode, candidate.Group == descriptor.Group && candidate.SourceMember == descriptor.SourceMember
		},
		ReadDeadline: factory.deadline, WriteDeadline: factory.deadline, MaxConcurrent: 1,
	})
	if err != nil {
		_ = journal.Close()
		_ = cursor.Close()
		_ = repository.Close()
		return closeDatabase(err)
	}
	resources := &rf3DynamicLearnerService{service: service, repository: repository, cursor: cursor, journal: journal,
		descriptor: descriptor, reservation: reservationRoot, installer: installer}
	return service, resources, nil
}

func rf3DynamicSource(spec nodecontrol.PreparationSpec, member uint64) (rafttransport.NodeID, string, bool) {
	for _, candidate := range spec.InitialVoters {
		if candidate.MemberID == member {
			return candidate.Node, candidate.SnapshotAddress, true
		}
	}
	return rafttransport.NodeID{}, "", false
}

func (resources *rf3DynamicLearnerService) close() error {
	if resources == nil {
		return nil
	}
	return errors.Join(resources.journal.Close(), resources.cursor.Close(), resources.repository.Close())
}

type rf3DynamicLearnerInstaller struct {
	mu              sync.Mutex
	factory         *rf3DynamicLearnerFactory
	intent          gateway.GroupEnrollmentIntent
	proof           gateway.PreparedReplicaProof
	spec            nodecontrol.PreparationSpec
	reservationRoot string
	repository      *snapshottransfer.Repository
	cursor          *replicatedstate.SnapshotCursorStore
	base            sqldriver.ReplicatedShardStoreIdentity
	applyIdentity   sqldriver.ReplicatedApplyIdentity
	staticBootstrap *pb.Snapshot
	installed       *raftmember.RuntimeIdentity
}

func (installer *rf3DynamicLearnerInstaller) ObserveInstalled(
	_ context.Context, descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, bool, error) {
	installer.mu.Lock()
	defer installer.mu.Unlock()
	if installer.installed == nil || installer.installed.Group != descriptor.Group ||
		installer.installed.MemberID != descriptor.TargetMember || installer.installed.StoreID != descriptor.TargetStore ||
		installer.installed.NodeIncarnation != descriptor.TargetIncarnation {
		return raftmember.RuntimeIdentity{}, false, nil
	}
	return *installer.installed, true, nil
}

// RecoverInstalled repairs the process-local runtime after a crash that
// happened after the node-log checkpoint and shared peer publication.  It
// reopens the exact reserved SQL/apply identities, verifies the node-log
// checkpoint against the durable descriptor, and publishes the runtime only
// through the same shared peer path used by a first install.
func (installer *rf3DynamicLearnerInstaller) RecoverInstalled(
	ctx context.Context, descriptor snapshottransfer.Descriptor,
) error {
	if installer == nil || installer.factory == nil || ctx == nil {
		return nodecontrol.ErrControl
	}
	installer.mu.Lock()
	defer installer.mu.Unlock()
	if installer.installed != nil {
		if installer.installed.Group == descriptor.Group && installer.installed.MemberID == descriptor.TargetMember &&
			installer.installed.StoreID == descriptor.TargetStore && installer.installed.NodeIncarnation == descriptor.TargetIncarnation {
			return nil
		}
		return nodecontrol.ErrConflict
	}
	identity, found, err := readRF3EnrollmentRuntime(installer.reservationRoot, installer.intent, installer.proof, descriptor)
	if err != nil || !found {
		return err
	}
	database, err := sqldriver.OpenReplicatedSnapshotTarget(filepath.Join(installer.reservationRoot, "member.vdb"),
		installer.base, installer.applyIdentity, sqldriver.ReplicatedOpenOptions{
			WriterLockContext: ctx, WriterLockDeadline: installer.factory.deadline(),
		})
	if err != nil {
		return err
	}
	closeDatabase := func(cause error) error { return errors.Join(cause, database.Close()) }
	apply, applyIdentity, err := database.OpenReplicatedApply(installer.base, installer.staticBootstrap,
		replicatedApplyOptions(installer.applyIdentity))
	if err != nil {
		return closeDatabase(err)
	}
	if applyIdentity != installer.applyIdentity {
		_ = apply.Close()
		return closeDatabase(nodecontrol.ErrConflict)
	}
	group, err := installer.factory.owner.group(installer.base.Binding)
	if err != nil {
		return errors.Join(closeDatabase(err), apply.Close())
	}
	checkpoint, err := group.Snapshot()
	if err != nil {
		_ = apply.Close()
		return closeDatabase(err)
	}
	expectedConfState := &pb.ConfState{Voters: []uint64{
		installer.spec.InitialVoters[0].MemberID, installer.spec.InitialVoters[1].MemberID,
		installer.spec.InitialVoters[2].MemberID,
	}, Learners: []uint64{descriptor.TargetMember}}
	if checkpoint.GetMetadata() == nil || checkpoint.GetMetadata().GetIndex() != descriptor.SnapshotIndex ||
		checkpoint.GetMetadata().GetTerm() != descriptor.SnapshotTerm ||
		!proto.Equal(checkpoint.GetMetadata().GetConfState(), expectedConfState) {
		_ = apply.Close()
		return closeDatabase(nodecontrol.ErrStale)
	}
	runtime, err := installer.factory.owner.adoptRegistered(group, database, apply)
	if err != nil {
		_ = apply.Close()
		return closeDatabase(err)
	}
	actual := runtime.Identity()
	if actual != identity {
		_ = runtime.Close()
		return nodecontrol.ErrConflict
	}
	if err = installer.factory.runtime.RegisterExecutionGroup(rf3DynamicRoster(installer.spec, descriptor), raftservice.ExecutionGroup{
		Runtime: runtime, Identity: actual, Command: installer.intent.ExpectedCommand, Read: apply, Recovery: apply,
	}); err != nil {
		_ = runtime.Close()
		return err
	}
	installer.installed = &actual
	return nil
}

func (installer *rf3DynamicLearnerInstaller) InstallPublishedLearner(
	ctx context.Context, descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, error) {
	if installer == nil || installer.factory == nil || ctx == nil {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrControl
	}
	installer.mu.Lock()
	defer installer.mu.Unlock()
	if installer.installed != nil {
		return *installer.installed, nil
	}
	if descriptor.Group != installer.intent.Group || descriptor.TargetMember != installer.intent.Target.Member ||
		descriptor.TargetStore != installer.intent.Target.StoreID || descriptor.TargetIncarnation != installer.intent.Target.NodeIncarnation {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrStale
	}
	planManifest, err := installer.repository.ManifestContext(ctx, descriptor)
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	database, err := sqldriver.OpenReplicatedSnapshotTarget(filepath.Join(installer.reservationRoot, "member.vdb"),
		installer.base, installer.applyIdentity, sqldriver.ReplicatedOpenOptions{
			WriterLockContext: ctx, WriterLockDeadline: installer.factory.deadline(),
		})
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	plan := snapshottransfer.LearnerInstallPlan{
		Repository: installer.repository, Descriptor: descriptor, Cursor: installer.cursor,
		Database: database, Context: ctx, Budget: installer.factory.budget,
		SQLIdentity: installer.base, ApplyOptions: replicatedApplyOptions(installer.applyIdentity),
		StageOptions: replicatedstate.SnapshotArtifactStageOptions{}, StaticBootstrap: installer.staticBootstrap,
		ExpectedConfState: planManifest.State.ConfState,
		Settlement:        new(snapshottransfer.LearnerInstallSettlement),
		NodeInstall:       installer.installNode,
	}
	identity, err := snapshottransfer.InstallPublishedLearner(plan)
	if err != nil {
		_ = database.Close()
		return raftmember.RuntimeIdentity{}, err
	}
	installer.installed = &identity
	return identity, nil
}

func (installer *rf3DynamicLearnerInstaller) installNode(
	ctx context.Context, descriptor snapshottransfer.Descriptor,
	manifest replicatedstate.SnapshotArtifactManifest, snapshot *pb.Snapshot,
	database *sqldriver.Database, apply *sqldriver.ReplicatedApply,
) (raftmember.RuntimeIdentity, error) {
	// The callback is entered after snapshottransfer has activated the exact
	// SQL apply image. Build the node descriptor from that authenticated apply
	// identity; no path or key is accepted from the controller payload.
	if installer == nil || ctx == nil || database == nil || apply == nil || snapshot == nil {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrControl
	}
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	descriptorForNode := raftstore.GroupDescriptor{
		TopologyRecoveryEpoch: profile.Binding.TopologyRecoveryEpoch,
		AllocationGeneration:  profile.Binding.AllocationGeneration, MemberID: profile.Binding.MemberID,
		GroupID: profile.Binding.GroupID, ShardIncarnation: profile.Binding.ShardIncarnation,
		StoreID: profile.Binding.StoreID, Distribution: profile.Binding.Distribution, Shard: profile.Binding.Shard,
	}
	if descriptorForNode.GroupID != descriptor.Group.GroupID || descriptorForNode.StoreID != descriptor.TargetStore ||
		descriptorForNode.MemberID != descriptor.TargetMember {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrConflict
	}
	if snapshot.GetMetadata() == nil || !proto.Equal(snapshot.GetMetadata().GetConfState(), manifest.State.ConfState) ||
		descriptor.SnapshotIndex != manifest.State.Applied || descriptor.SnapshotTerm != manifest.State.LastTerm {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrStale
	}
	runtime, err := installer.factory.owner.registerAndAdoptDynamic(
		descriptorForNode, snapshot, descriptor.TargetIncarnation, database, apply,
	)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		return raftmember.RuntimeIdentity{}, err
	}
	identity := runtime.Identity()
	if identity.Group != descriptor.Group || identity.MemberID != descriptor.TargetMember ||
		identity.StoreID != descriptor.TargetStore || identity.NodeIncarnation != descriptor.TargetIncarnation {
		_ = runtime.Close()
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrConflict
	}
	roster := rf3DynamicRoster(installer.spec, descriptor)
	if len(roster) != 4 {
		_ = runtime.Close()
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrControl
	}
	if err = installer.factory.runtime.RegisterExecutionGroup(roster, raftservice.ExecutionGroup{
		Runtime: runtime, Identity: identity, Command: installer.intent.ExpectedCommand,
		Read: apply, Recovery: apply,
	}); err != nil {
		_ = runtime.Close()
		return raftmember.RuntimeIdentity{}, err
	}
	if err = persistRF3EnrollmentRuntime(installer.reservationRoot, installer.intent, installer.proof, descriptor, identity); err != nil {
		if unregisterErr := installer.factory.runtime.UnregisterExecutionGroup(identity); unregisterErr != nil {
			return raftmember.RuntimeIdentity{}, errors.Join(err, unregisterErr)
		}
		return raftmember.RuntimeIdentity{}, err
	}
	return identity, nil
}

func rf3DynamicRoster(spec nodecontrol.PreparationSpec, descriptor snapshottransfer.Descriptor) []rafttransport.Member {
	roster := make([]rafttransport.Member, 0, len(spec.InitialVoters)+1)
	for _, member := range spec.InitialVoters {
		roster = append(roster, rafttransport.Member{Group: descriptor.Group,
			ReplicaSetVersion: descriptor.ReplicaSetVersion, MemberID: member.MemberID,
			Node: member.Node, Role: rafttransport.MemberVoter})
	}
	roster = append(roster, rafttransport.Member{Group: descriptor.Group,
		ReplicaSetVersion: descriptor.ReplicaSetVersion, MemberID: descriptor.TargetMember,
		Node: spec.Target.Node, Role: rafttransport.MemberLearner})
	return roster
}

var _ snapshottransfer.BootstrapInstaller = (*rf3DynamicLearnerInstaller)(nil)
