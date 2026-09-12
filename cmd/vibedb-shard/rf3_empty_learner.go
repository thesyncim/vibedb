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
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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

// Recover reopens descriptor-backed enrollments. Completed enrollments need a
// fresh catalog placement proof: completion means the target joined, while a
// later move may have removed that same target. Local receipts alone cannot
// distinguish those cases.
func (factory *rf3DynamicLearnerFactory) Recover(ctx context.Context) error {
	if factory == nil || ctx == nil || factory.runtime == nil || factory.runtime.reader == nil {
		return nodecontrol.ErrControl
	}
	directory, err := os.Open(filepath.Join(factory.root, "enrollments"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	// Historical reservations may outlive their catalog rows. Bound each
	// directory batch and the active services, rather than all past moves.
	for {
		entries, readErr := directory.ReadDir(maxRF3ManifestGroups)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		for _, entry := range entries {
			if err = factory.recoverEnrollment(ctx, entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
	}
}

func (factory *rf3DynamicLearnerFactory) recoverEnrollment(ctx context.Context, entry os.DirEntry) error {
	var err error
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
		return nil
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
	cut, readErr := factory.runtime.reader.ReadEnrollmentRecovery(ctx, intentID)
	if readErr != nil {
		return errors.Join(nodecontrol.ErrStale, readErr)
	}
	if cut.EnrollmentMissing() {
		if cut.IntentID != intentID || cut.PhysicalNode != descriptorReceipt.TargetNode || cut.Incarnation != descriptorReceipt.TargetIncarnation {
			return nodecontrol.ErrStale
		}
		// The catalog pins completed enrollments for every live target. A
		// witnessed absence therefore retires only this old local reservation.
		return nil
	}
	intent := cut.Intent
	if !intent.Valid() || intent.IntentID != intentID {
		return nodecontrol.ErrStale
	}
	if intent.State < gateway.EnrollmentEnrolled || intent.State > gateway.EnrollmentComplete || intent.Proof == nil {
		return nil
	}
	var recovery *nodecontrol.BootstrapReadReply
	if intent.State == gateway.EnrollmentComplete {
		if !cut.TargetServing() {
			return nil
		}
		recovery = &cut
	}
	descriptor, found, descriptorErr := readRF3EnrollmentDescriptor(intentRoot, intent)
	if descriptorErr != nil {
		return descriptorErr
	}
	if !found {
		return nodecontrol.ErrJournalCorrupt
	}
	factory.mu.Lock()
	bounded := len(factory.services) >= maxRF3ManifestGroups && factory.services[intent.Group] == nil
	factory.mu.Unlock()
	if bounded {
		return nodecontrol.ErrBound
	}
	var resources *rf3DynamicLearnerService
	if recovery != nil {
		// Completed replicas need their retained runtime, not another
		// bootstrap receiver capable of replaying the original artifact.
		_, resources, err = factory.openService(ctx, intent, *intent.Proof, descriptor)
		if err != nil {
			return err
		}
		resources.installer.recovery = recovery
		factory.mu.Lock()
		factory.services[intent.Group] = resources
		factory.mu.Unlock()
	} else {
		if err = factory.runtime.receivers.Activate(ctx, intent, *intent.Proof); err != nil {
			return err
		}
		if err = factory.Register(ctx, intent, *intent.Proof, descriptor); err != nil {
			return err
		}
		factory.mu.Lock()
		resources = factory.services[intent.Group]
		factory.mu.Unlock()
	}
	if resources != nil && resources.installer != nil {
		if err = resources.installer.RecoverInstalled(ctx, descriptor); err != nil {
			return err
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
	repository, err := snapshottransfer.OpenRepository(filepath.Join(reservationRoot, "snapshot-repository"), snapshottransfer.Limits{
		MaxArtifacts: 1, MaxArtifactBytes: rf3DynamicRepositoryMaxBytes,
		MaxDiskBytes: rf3DynamicRepositoryMaxBytes + snapshottransfer.DescriptorBytes + 2<<20,
		Budget:       factory.budget,
	})
	if err != nil {
		return nil, nil, err
	}
	cursor, err := replicatedstate.OpenSnapshotCursorStore(filepath.Join(reservationRoot, "snapshot.cursor"))
	if err != nil {
		_ = repository.Close()
		return nil, nil, err
	}
	journal, err := snapshottransfer.OpenBootstrapFileJournal(filepath.Join(reservationRoot, "bootstrap-journal"), 4)
	if err != nil {
		_ = cursor.Close()
		_ = repository.Close()
		return nil, nil, err
	}
	snapshotIO := func() time.Time { return time.Now().Add(rf3SnapshotBootstrapTimeout) }
	opener := rafttransport.TLSSnapshotStreamOpener{
		TLS: factory.profile,
		Open: func(openCtx context.Context, node rafttransport.NodeID) (net.Conn, error) {
			if node != sourceNode {
				return nil, snapshottransfer.ErrBootstrapUnauthorized
			}
			conn, err := (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(openCtx, "tcp", sourceAddress)
			if err != nil {
				return nil, fmt.Errorf("snapshot source %s: %w", sourceAddress, err)
			}
			return conn, nil
		},
		HandshakeDeadline: factory.deadline,
	}
	receiver := &snapshottransfer.Receiver{Repository: repository, Opener: opener, Budget: factory.budget,
		ReadDeadline: snapshotIO, WriteDeadline: snapshotIO}
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
		ReadDeadline: snapshotIO, WriteDeadline: snapshotIO, MaxConcurrent: 1,
	})
	if err != nil {
		_ = journal.Close()
		_ = cursor.Close()
		_ = repository.Close()
		return nil, nil, err
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
	recovery        *nodecontrol.BootstrapReadReply
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
	return installer.recoverInstalledLocked(ctx, descriptor)
}

func (installer *rf3DynamicLearnerInstaller) recoverInstalledLocked(ctx context.Context, descriptor snapshottransfer.Descriptor) error {
	if installer.installed != nil {
		if installer.installed.Group == descriptor.Group && installer.installed.MemberID == descriptor.TargetMember &&
			installer.installed.StoreID == descriptor.TargetStore && installer.installed.NodeIncarnation == descriptor.TargetIncarnation {
			return nil
		}
		return nodecontrol.ErrConflict
	}
	runtime, apply, found, err := installer.recoverRuntime(ctx, descriptor)
	if err != nil || !found {
		return err
	}
	actual := runtime.Identity()
	grant, _, err := installer.factory.runtime.grants.Register(descriptor.Group, filepath.Join(installer.reservationRoot, "membership-grant"))
	if err != nil {
		return errors.Join(err, runtime.Close())
	}
	publication, err := runtime.Publication()
	if err != nil {
		return errors.Join(err, runtime.Close())
	}
	roster, command, err := rf3RecoveredRoster(installer.spec, descriptor, publication, installer.intent.ExpectedCommand, installer.recovery, grant)
	if err != nil {
		return errors.Join(err, runtime.Close())
	}
	profile, err := apply.CapacityQualificationProfile()
	if err == nil {
		command, err = rf3RecoveredCommand(command, profile.Binding.Authority.SchemaGeneration, profile.RelationManifestDigest)
	}
	if err != nil {
		return errors.Join(err, runtime.Close())
	}
	// The retained roster supplies historical member mappings needed to
	// replay an already committed removal. Current placement supplies any
	// later peers and the current command fence.
	err = installer.enrollCertifiedRosterPeers(ctx, roster)
	if installer.recovery != nil {
		if err == nil {
			err = installer.enrollRecoveryPeers(ctx)
		}
	}
	if err != nil {
		_ = runtime.Close()
		return err
	}
	if err = installer.factory.runtime.RegisterExecutionGroupWithGrant(roster, raftservice.ExecutionGroup{
		Runtime: runtime, Identity: actual, Command: command, Read: apply, Recovery: apply,
	}, grant); err != nil {
		_ = runtime.Close()
		return err
	}
	installer.installed = &actual
	return nil
}

// recoverRuntime uses the authenticated node registration as the installation
// commit point. The local receipt may be absent if the process died before
// publication; in that case only the original certified checkpoint can prove
// the install. A receipt permits later checkpoints, which must never be reset
// to the snapshot that originally enrolled this member.
func (installer *rf3DynamicLearnerInstaller) recoverRuntime(
	ctx context.Context, descriptor snapshottransfer.Descriptor,
) (*raftmember.Runtime, *sqldriver.ReplicatedApply, bool, error) {
	identity, receiptFound, err := readRF3EnrollmentRuntime(installer.reservationRoot, installer.intent, installer.proof, descriptor)
	if err != nil {
		return nil, nil, false, fmt.Errorf("read recovered enrollment receipt: %w", err)
	}
	if _, found := installer.factory.owner.store.GroupByID(descriptor.Group.GroupID); !found {
		if receiptFound || installer.recovery != nil {
			return nil, nil, false, nodecontrol.ErrJournalCorrupt
		}
		return nil, nil, false, nil
	}
	group, err := installer.factory.owner.group(installer.base.Binding)
	if err != nil {
		return nil, nil, false, err
	}
	incarnation, err := group.NodeIncarnation()
	if err != nil || incarnation != descriptor.TargetIncarnation {
		return nil, nil, false, errors.Join(nodecontrol.ErrConflict, err)
	}
	checkpoint, err := group.Snapshot()
	if err != nil {
		return nil, nil, false, err
	}
	expectedConfState := &pb.ConfState{Voters: []uint64{
		installer.spec.InitialVoters[0].MemberID, installer.spec.InitialVoters[1].MemberID,
		installer.spec.InitialVoters[2].MemberID,
	}, Learners: []uint64{descriptor.TargetMember}}
	if !rf3RecoveredCheckpointMatches(checkpoint, descriptor, expectedConfState, receiptFound) {
		return nil, nil, false, nodecontrol.ErrStale
	}
	// Snapshot-target opens are for incomplete transfer staging. Once the
	// registration is durable the ordinary retained-log opener must recover
	// SQL progress, including checkpoint rotation and schema settlement.
	_, _, database, apply, err := openRF3RetainedApply(filepath.Join(installer.reservationRoot, "member.vdb"),
		group, installer.base, installer.applyIdentity, sqldriver.ReplicatedOpenOptions{
			WriterLockContext: ctx, WriterLockDeadline: installer.factory.deadline(),
		})
	if err != nil {
		return nil, nil, false, err
	}
	runtime, err := installer.factory.owner.adoptRegistered(group, database, apply)
	if err != nil {
		if runtime != nil {
			return nil, nil, false, errors.Join(err, runtime.Close())
		}
		return nil, nil, false, errors.Join(err, apply.Close(), database.Close())
	}
	actual := runtime.Identity()
	expectedIdentity := identity
	// The retained opener authenticates any published schema successor. Its
	// local generation can still lag the current catalog while Raft catches up;
	// the enrollment receipt fences the immutable replica identity, not schema.
	expectedIdentity.RelationManifestDigest = actual.RelationManifestDigest
	if actual.Group != descriptor.Group || actual.MemberID != descriptor.TargetMember ||
		actual.StoreID != descriptor.TargetStore || actual.NodeIncarnation != descriptor.TargetIncarnation ||
		receiptFound && actual != expectedIdentity {
		return nil, nil, false, errors.Join(nodecontrol.ErrConflict, runtime.Close())
	}
	if !receiptFound {
		if err = persistRF3EnrollmentRuntime(installer.reservationRoot, installer.intent, installer.proof, descriptor, actual); err != nil {
			return nil, nil, false, errors.Join(fmt.Errorf("persist recovered enrollment receipt: %w", err), runtime.Close())
		}
	}
	return runtime, apply, true, nil
}

func rf3RecoveredCheckpointMatches(checkpoint *pb.Snapshot, descriptor snapshottransfer.Descriptor, initial *pb.ConfState, receiptFound bool) bool {
	metadata := checkpoint.GetMetadata()
	if metadata == nil || metadata.GetConfState() == nil || metadata.GetIndex() < descriptor.SnapshotIndex || metadata.GetTerm() < descriptor.SnapshotTerm {
		return false
	}
	if metadata.GetIndex() == descriptor.SnapshotIndex {
		return metadata.GetTerm() == descriptor.SnapshotTerm && metadata.GetConfState().Equivalent(initial) == nil
	}
	return receiptFound
}

func rf3RecoveredCommand(command raftservice.CommandFence, schema uint64, manifest replication.Digest) (raftservice.CommandFence, error) {
	if !command.Valid() || schema == 0 || schema > command.SchemaGeneration || manifest == (replication.Digest{}) ||
		schema == command.SchemaGeneration && manifest != command.RelationManifestDigest {
		return command, nodecontrol.ErrStale
	}
	// Retain current placement fences, but expose only the schema durably
	// selected by this replica. The owner advances it after Raft schema replay.
	command.SchemaGeneration, command.RelationManifestDigest = schema, manifest
	return command, nil
}

func (installer *rf3DynamicLearnerInstaller) InstallPublishedLearner(
	ctx context.Context, descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, error) {
	if installer == nil || installer.factory == nil || ctx == nil {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrControl
	}
	installer.mu.Lock()
	defer installer.mu.Unlock()
	if descriptor.Group != installer.intent.Group || descriptor.TargetMember != installer.intent.Target.Member ||
		descriptor.TargetStore != installer.intent.Target.StoreID || descriptor.TargetIncarnation != installer.intent.Target.NodeIncarnation {
		return raftmember.RuntimeIdentity{}, nodecontrol.ErrStale
	}
	if installer.installed != nil {
		return *installer.installed, nil
	}
	if err := installer.recoverInstalledLocked(ctx, descriptor); err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	if installer.installed != nil {
		return *installer.installed, nil
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
	// Persist installation evidence before the runtime can advance its Raft
	// checkpoint. Recovery can then distinguish a completed install from an
	// interrupted transfer even if publication or the response fails.
	if err = persistRF3EnrollmentRuntime(installer.reservationRoot, installer.intent, installer.proof, descriptor, identity); err != nil {
		_ = runtime.Close()
		return raftmember.RuntimeIdentity{}, err
	}
	if err = installer.enrollCertifiedRosterPeers(ctx); err != nil {
		_ = runtime.Close()
		return raftmember.RuntimeIdentity{}, err
	}
	grant, _, err := installer.factory.runtime.grants.Register(descriptor.Group, filepath.Join(installer.reservationRoot, "membership-grant"))
	if err != nil {
		return raftmember.RuntimeIdentity{}, errors.Join(err, runtime.Close())
	}
	if err = installer.factory.runtime.RegisterExecutionGroupWithGrant(roster, raftservice.ExecutionGroup{
		Runtime: runtime, Identity: identity, Command: installer.intent.ExpectedCommand,
		Read: apply, Recovery: apply,
	}, grant); err != nil {
		_ = runtime.Close()
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

func rf3RecoveredRoster(spec nodecontrol.PreparationSpec, descriptor snapshottransfer.Descriptor, publication raftmodel.Publication,
	command raftservice.CommandFence, cut *nodecontrol.BootstrapReadReply, grant membershipgrant.Grant,
) ([]rafttransport.Member, raftservice.CommandFence, error) {
	conf := publication.ConfState
	if conf == nil || publication.ReplicaSetVersion == 0 || len(conf.VotersOutgoing) != 0 || len(conf.LearnersNext) != 0 || conf.GetAutoLeave() {
		return nil, command, nodecontrol.ErrStale
	}
	roster := rf3DynamicRoster(spec, descriptor)
	for index := range roster {
		roster[index].ReplicaSetVersion = publication.ReplicaSetVersion
		roster[index].Role = rafttransport.MemberEnrolled
	}
	if cut != nil {
		if !cut.TargetServing() {
			return nil, command, nodecontrol.ErrStale
		}
		command = cut.CurrentRoute.Serving.Command
		members := slices.Clone(cut.CurrentRoute.Serving.Replicas)
		if cut.CurrentRoute.HasEnrolledTarget {
			members = append(members, cut.CurrentRoute.EnrolledTarget)
		}
		for _, replica := range members {
			index := slices.IndexFunc(roster, func(member rafttransport.Member) bool { return member.MemberID == replica.Member })
			if index >= 0 {
				if roster[index].Node != replica.Node {
					return nil, command, nodecontrol.ErrConflict
				}
				continue
			}
			roster = append(roster, rafttransport.Member{Group: descriptor.Group, ReplicaSetVersion: publication.ReplicaSetVersion,
				MemberID: replica.Member, Node: replica.Node, Role: rafttransport.MemberEnrolled})
		}
	}
	for _, members := range []struct {
		ids  []uint64
		role rafttransport.MemberRole
	}{{conf.Voters, rafttransport.MemberVoter}, {conf.Learners, rafttransport.MemberLearner}} {
		for _, id := range members.ids {
			index := slices.IndexFunc(roster, func(member rafttransport.Member) bool { return member.MemberID == id })
			if index < 0 || roster[index].Role != rafttransport.MemberEnrolled {
				return nil, command, nodecontrol.ErrConflict
			}
			roster[index].Role = members.role
		}
	}
	// The original mappings are a bounded historical proof. Remove mappings
	// that neither the current route nor a retained grant needs once Raft has
	// durably stopped referring to them.
	if cut != nil && grant == (membershipgrant.Grant{}) {
		roster = slices.DeleteFunc(roster, func(member rafttransport.Member) bool {
			return member.Role == rafttransport.MemberEnrolled &&
				!slices.ContainsFunc(cut.CurrentRoute.Serving.Replicas, func(replica gateway.ReplicatedEndpoint) bool { return replica.Member == member.MemberID }) &&
				(!cut.CurrentRoute.HasEnrolledTarget || cut.CurrentRoute.EnrolledTarget.Member != member.MemberID)
		})
	}
	return roster, command, nil
}

func (installer *rf3DynamicLearnerInstaller) enrollRecoveryPeers(ctx context.Context) error {
	if installer.recovery == nil || !installer.recovery.TargetServing() {
		return nodecontrol.ErrStale
	}
	cut := installer.recovery
	registry := installer.factory.runtime.registry
	enroller := installer.factory.runtime.peer.Transport()
	verifier := rafttransport.EnrollmentVerifierFunc(func(intent rafttransport.EnrollmentIntent) error {
		if intent.Group != (raftmember.GroupKey{}) {
			return rafttransport.ErrInvalidGroup
		}
		for _, node := range cut.CurrentNodes {
			if node.NodeID == intent.Peer.NodeID && node.Incarnation == intent.Peer.Incarnation &&
				node.Revision == intent.Peer.Revision && node.DataAddress == intent.Peer.Endpoint &&
				node.ServiceKeyDigest == intent.Peer.ServiceKeyDigest {
				return nil
			}
		}
		return nodecontrol.ErrStale
	})
	for _, node := range cut.CurrentNodes {
		if node.NodeID == registry.LocalNode() {
			continue
		}
		intent := rafttransport.EnrollmentIntent{
			Domain: installer.factory.profile.LocalIdentity().TrustDomain,
			Digest: rf3EmptyNodePeerEnrollmentDigest(cut.DirectoryCutDigest, node.NodeID),
			Peer: rafttransport.PhysicalPeer{NodeID: node.NodeID, TrustDomain: installer.factory.profile.LocalIdentity().TrustDomain,
				Incarnation: node.Incarnation, Revision: node.Revision, ServiceKeyDigest: node.ServiceKeyDigest,
				Endpoint: node.DataAddress, State: rafttransport.PeerEnrolled}, DirectoryRevision: registry.PeerDirectoryRevision()}
		if err := rf3EnrollPhysicalPeer(ctx, enroller, registry, intent, verifier); err != nil {
			return err
		}
	}
	return nil
}

type rf3PhysicalPeerEnroller interface {
	EnrollPeerContext(context.Context, rafttransport.EnrollmentIntent, rafttransport.EnrollmentVerifier) error
}

// Independent group preparations may certify the same physical peer. Keep
// its first directory proof when the complete physical record is unchanged;
// the caller still verifies its own committed preparation or recovery cut,
// and the enroller still restores the queue before publishing membership.
func rf3EnrollPhysicalPeer(
	ctx context.Context,
	enroller rf3PhysicalPeerEnroller,
	registry *rafttransport.StaticRegistry,
	intent rafttransport.EnrollmentIntent,
	verifier rafttransport.EnrollmentVerifier,
) error {
	peer := intent.Peer
	if existing, err := registry.PhysicalPeer(peer.NodeID); err == nil &&
		existing.EnrollmentDigest != ([32]byte{}) && existing.NodeID == peer.NodeID &&
		existing.TrustDomain == peer.TrustDomain && existing.State == peer.State &&
		existing.Incarnation == peer.Incarnation && existing.Revision == peer.Revision &&
		existing.ServiceKeyDigest == peer.ServiceKeyDigest && existing.Endpoint == peer.Endpoint {
		intent.Digest = existing.EnrollmentDigest
	}
	return enroller.EnrollPeerContext(ctx, intent, verifier)
}

func (installer *rf3DynamicLearnerInstaller) enrollCertifiedRosterPeers(ctx context.Context, rosters ...[]rafttransport.Member) error {
	if installer == nil || installer.factory == nil || installer.factory.runtime == nil ||
		installer.factory.runtime.peer == nil || installer.factory.profile == nil || len(rosters) > 1 {
		return nodecontrol.ErrControl
	}
	include := func(node rafttransport.NodeID) bool {
		if installer.recovery != nil {
			for _, current := range installer.recovery.CurrentNodes {
				if current.NodeID == node {
					return false // The authenticated current directory supplies it.
				}
			}
		}
		if len(rosters) == 0 {
			return true
		}
		for _, member := range rosters[0] {
			if member.Node == node {
				return true
			}
		}
		return false
	}
	return rf3EnrollCertifiedRosterPeersFiltered(
		ctx,
		installer.factory.runtime.peer.Transport(),
		installer.factory.runtime.registry,
		installer.spec,
		installer.intent.ExpectedManifestDigest,
		installer.factory.profile.LocalIdentity().TrustDomain,
		include,
	)
}

// rf3EnrollCertifiedRosterPeers publishes the source-certified voter identities
// into an empty-node directory. InstallGroup refuses any roster node that is
// not already physically enrolled, so snapshot install cannot publish the
// learner group until these peers exist.
func rf3EnrollCertifiedRosterPeers(
	ctx context.Context,
	enroller rf3PhysicalPeerEnroller,
	registry *rafttransport.StaticRegistry,
	spec nodecontrol.PreparationSpec,
	certified replication.Digest,
	domain rafttransport.TrustDomain,
) error {
	return rf3EnrollCertifiedRosterPeersFiltered(ctx, enroller, registry, spec, certified, domain, nil)
}

func rf3EnrollCertifiedRosterPeersFiltered(
	ctx context.Context,
	enroller rf3PhysicalPeerEnroller,
	registry *rafttransport.StaticRegistry,
	spec nodecontrol.PreparationSpec,
	certified replication.Digest,
	domain rafttransport.TrustDomain,
	include func(rafttransport.NodeID) bool,
) error {
	if ctx == nil || enroller == nil || registry == nil || certified == (replication.Digest{}) ||
		domain == (rafttransport.TrustDomain{}) {
		return nodecontrol.ErrControl
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	local := registry.LocalNode()
	if spec.Target.Node == local && spec.Target.ServiceKeyDigest != (replication.Digest{}) {
		peer, err := registry.PhysicalPeer(local)
		if err == nil && peer.ServiceKeyDigest != ([32]byte{}) &&
			peer.ServiceKeyDigest != [32]byte(spec.Target.ServiceKeyDigest) {
			return nodecontrol.ErrConflict
		}
	}
	verifier := rafttransport.EnrollmentVerifierFunc(func(intent rafttransport.EnrollmentIntent) error {
		if intent.Group != (raftmember.GroupKey{}) {
			return rafttransport.ErrInvalidGroup
		}
		for _, voter := range spec.InitialVoters {
			if voter.Node != intent.Peer.NodeID {
				continue
			}
			if [32]byte(voter.ServiceKeyDigest) != intent.Peer.ServiceKeyDigest ||
				voter.PeerAddress != intent.Peer.Endpoint ||
				voter.NodeIncarnation != intent.Peer.Incarnation ||
				voter.NodeRevision != intent.Peer.Revision {
				return nodecontrol.ErrStale
			}
			return nil
		}
		return nodecontrol.ErrConflict
	})
	for _, voter := range spec.InitialVoters {
		if voter.Node == local || include != nil && !include(voter.Node) {
			continue
		}
		if voter.ServiceKeyDigest == (replication.Digest{}) || voter.NodeIncarnation == 0 ||
			voter.NodeRevision == 0 || voter.PeerAddress == "" {
			return nodecontrol.ErrControl
		}
		intent := rafttransport.EnrollmentIntent{
			Digest: rf3EmptyNodePeerEnrollmentDigest(certified, voter.Node),
			Domain: domain,
			Peer: rafttransport.PhysicalPeer{
				NodeID: voter.Node, TrustDomain: domain,
				Incarnation: voter.NodeIncarnation, Revision: voter.NodeRevision,
				ServiceKeyDigest: [32]byte(voter.ServiceKeyDigest),
				Endpoint:         voter.PeerAddress,
				State:            rafttransport.PeerEnrolled,
			},
			DirectoryRevision: registry.PeerDirectoryRevision(),
		}
		if err := rf3EnrollPhysicalPeer(ctx, enroller, registry, intent, verifier); err != nil {
			return fmt.Errorf("RF3 empty-node roster enrollment: %w", err)
		}
	}
	return nil
}

func rf3EmptyNodePeerEnrollmentDigest(certified replication.Digest, node rafttransport.NodeID) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/rf3-empty-node/physical-peer/v1\x00"))
	_, _ = hash.Write(certified[:])
	_, _ = hash.Write(node[:])
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

var _ snapshottransfer.BootstrapInstaller = (*rf3DynamicLearnerInstaller)(nil)
