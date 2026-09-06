package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	bootstrapRF3NetworkTimeout   = 15 * time.Second
	bootstrapRF3MaxArtifactBytes = uint64(1) << 50
)

func runBootstrapRF3(args []string) int {
	flags := flag.NewFlagSet("bootstrap-rf3", flag.ContinueOnError)
	manifestPath := flags.String("manifest", "", "canonical cold RF3 learner manifest")
	if err := flags.Parse(args); err != nil || *manifestPath == "" || flags.NArg() != 0 {
		usage()
		return 2
	}
	manifest, err := loadBootstrapRF3Manifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error RF3 bootstrap manifest: %v\n", err)
		return 2
	}
	bundles := manifest.groupBundles()
	members := make([]rf3Manifest, len(bundles))
	for index, bundle := range bundles {
		members[index], err = loadRF3Manifest(bundle.MemberManifest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error RF3 member manifest group %d: %v\n", index, err)
			return 2
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(members) == 1 {
		err = bootstrapPreparedRF3(ctx, manifest.withGroup(bundles[0]), members[0])
	} else {
		err = bootstrapPreparedRF3Groups(ctx, manifest, members)
	}
	err = componentShutdownError(err, context.Cause(ctx))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error bootstrap RF3: %v\n", err)
		return 1
	}
	return 0
}

// bootstrapPreparedRF3 owns the non-serving cold target until one exact
// snapshot is installed. It then closes every cold owner and reopens through
// the ordinary RF3 serving path, where learner membership keeps native ingress
// fenced until promotion and the current catalog cut authorize it.
func bootstrapPreparedRF3(
	parent context.Context,
	bootstrap bootstrapRF3Manifest,
	member rf3Manifest,
) error {
	if parent == nil || bootstrap.MaxArtifactBytes == 0 {
		return errInvalidBootstrapRF3Manifest
	}
	if _, err := os.Stat(member.WAL.Path); err == nil {
		return servePreparedRF3(parent, member)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := validateBootstrapRF3Topology(bootstrap, member); err != nil {
		return err
	}
	migrationBudget, err := openRF3MigrationBudget(member)
	if err != nil {
		return err
	}
	defer migrationBudget.Close()
	profile, err := servicetls.LoadProfile(
		member.TLS.Certificate, member.TLS.Key, member.TLS.Roots,
		member.TLS.IdentityOID, time.Now,
	)
	if err != nil {
		return err
	}
	policy, err := serviceauthz.LoadFile(member.AuthorizationPolicy)
	if err != nil {
		return err
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		return err
	}
	topologyNodes := policy.NodesWith(serviceauthz.CapabilityTopology)
	authorizer, err := servicetls.NewNodeAuthorizer(topologyNodes)
	if err != nil {
		return err
	}
	controlTLS, err := servicetls.NewServer(
		profile, rafttransport.TrafficShardControl, authorizer,
	)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", bootstrap.ControlListener)
	if err != nil {
		return err
	}
	defer listener.Close()

	base, applyIdentity, err := loadRF3RetainedIdentities(member)
	if err != nil {
		return err
	}
	target := member.EnrolledTarget
	wantDomain := rafttransport.TrustDomain{
		ClusterID: base.Binding.ClusterID, ClusterIncarnation: base.Binding.ClusterIncarnation,
	}
	if target == nil || profile.LocalIdentity().Node != target.NodeID ||
		profile.LocalIdentity().TrustDomain != wantDomain {
		return errInvalidBootstrapRF3Manifest
	}
	grantInstaller, err := openDurableRF3GrantInstaller(
		rf3MembershipGrantPath(member), coldRF3GrantAuthority{
			group: groupFromBinding(base.Binding), members: member.Members, target: *target,
		},
	)
	if err != nil {
		return err
	}
	deadline := func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) }
	membershipControl, err := shardservice.NewMembershipGrantControlService(
		grantInstaller, policy, deadline, deadline,
	)
	if err != nil {
		return err
	}
	database, err := sqldriver.OpenReplicatedSnapshotTarget(member.SQL.Path, base, applyIdentity,
		sqldriver.ReplicatedOpenOptions{WriterLockContext: parent, WriterLockDeadline: time.Now().Add(rf3StartupWriterLockWait)})
	if err != nil {
		return err
	}
	databaseOwned := true
	defer func() {
		if databaseOwned {
			_ = database.Close()
		}
	}()
	if _, err = database.RequireReplicatedShardStore(base); err != nil {
		return err
	}
	staticRaw, err := readRF3BoundedFile(
		bootstrap.StaticBootstrapPath, replicatedstate.MaxStaticBootstrapEnvelopeBytes,
	)
	if err != nil {
		return err
	}
	staticBootstrap := new(pb.Snapshot)
	if err = proto.Unmarshal(staticRaw, staticBootstrap); err != nil {
		return err
	}
	clear(staticRaw)

	repository, err := snapshottransfer.OpenRepository(
		bootstrap.RepositoryPath,
		snapshottransfer.Limits{
			MaxArtifacts: 1, MaxArtifactBytes: bootstrap.MaxArtifactBytes,
			MaxDiskBytes: bootstrap.MaxArtifactBytes + snapshottransfer.DescriptorBytes + 1<<20,
			Budget:       migrationBudget,
		},
	)
	if err != nil {
		return err
	}
	defer repository.Close()
	cursor, err := replicatedstate.OpenSnapshotCursorStore(bootstrap.CursorPath)
	if err != nil {
		return err
	}
	defer cursor.Close()
	journal, err := snapshottransfer.OpenBootstrapFileJournal(bootstrap.JournalPath, 1)
	if err != nil {
		return err
	}
	defer journal.Close()
	host, err := multiraft.NewHost(rf3HostLimits())
	if err != nil {
		return err
	}
	hostOwned := true
	defer func() {
		if hostOwned {
			_ = host.Close()
		}
	}()

	key, err := loadRF3WALKey(member.WAL.KeyID, member.WAL.KeyMaterialPath)
	if err != nil {
		return err
	}
	defer clear(key.Material[:])
	// Serving opens can recover provider metadata from an existing WAL. A cold
	// target has no WAL yet, so creation must use the provisioned exact bytes.
	key.Wrapped = bytes.Clone(bootstrap.WALWrappedKey)
	defer clear(key.Wrapped)
	opener := rafttransport.TLSSnapshotStreamOpener{
		TLS: profile,
		Open: func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
			if node != bootstrap.SourceNode {
				return nil, snapshottransfer.ErrBootstrapUnauthorized
			}
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", bootstrap.SourceSnapshotAddress)
		},
		HandshakeDeadline: func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
	}
	receiver := &snapshottransfer.Receiver{
		Repository: repository, Opener: opener,
		Budget:        migrationBudget,
		ReadDeadline:  func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
		WriteDeadline: func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
	}
	installer := &coldRF3Installer{
		repository: repository, cursor: cursor, database: database,
		base: base, apply: applyIdentity, staticBootstrap: staticBootstrap,
		walPath: member.WAL.Path, walKey: key, walOptions: member.WAL.Options,
		host: host, settlement: new(snapshottransfer.LearnerInstallSettlement),
		budget: migrationBudget,
	}
	coldCapacityGroup := &preparedColdRF3Group{group: groupFromBinding(base.Binding),
		authority:  coldRF3GrantAuthority{group: groupFromBinding(base.Binding), members: member.Members, target: *target},
		repository: repository, cursor: cursor, journal: journal, database: database, key: key, installer: installer}
	var capacityRevision atomic.Uint64
	capacityDirectory, err := newRF3CapacitySourceDirectory(nil, []*preparedColdRF3Group{coldCapacityGroup}, nil,
		func(ctx context.Context, _ replicacontrol.CapacityRequest, samples []replicacontrol.CapacitySourceSample) (replicacontrol.NodeCapacity, error) {
			return RF3CapacityNodeFromPhysical(ctx, profile.LocalIdentity().Node, target.NodeIncarnation,
				uint64(max(raftstore.DefaultMaxFileBytes, int64(member.WAL.Options.MaxFileBytes))), migrationBudget, &capacityRevision, samples)
		})
	if err != nil {
		return err
	}
	capacityProvider, err := replicacontrol.NewCapacityProvider(capacityDirectory)
	if err != nil {
		return err
	}
	capacityControl, err := replicacontrol.NewCapacityService(replicacontrol.CapacityServiceOptions{
		Observer: capacityProvider,
		Authorize: func(identity rafttransport.PeerIdentity, request replicacontrol.CapacityRequest) bool {
			return identity.TrustDomain == profile.LocalIdentity().TrustDomain &&
				policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow &&
				request.Group == groupFromBinding(base.Binding) && request.TargetMember == base.Binding.MemberID
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 4,
	})
	if err != nil {
		return err
	}
	service, err := snapshottransfer.NewBootstrapControlService(
		snapshottransfer.BootstrapControlOptions{
			Journal: journal, Receiver: receiver, Installer: installer, Releaser: repository,
			Authorize: func(identity rafttransport.PeerIdentity, request snapshottransfer.BootstrapRequest) bool {
				return identity.TrustDomain == profile.LocalIdentity().TrustDomain &&
					gate.Check(identity.Node, gate.Generation(), serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow &&
					request.Descriptor.TargetMember == base.Binding.MemberID &&
					request.Descriptor.TargetStore == base.Binding.StoreID
			},
			SourceNode: func(descriptor snapshottransfer.Descriptor) (rafttransport.NodeID, bool) {
				return bootstrap.SourceNode, descriptor.SourceMember == sourceMemberForBootstrap(bootstrap, member)
			},
			ReadDeadline:  func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
			WriteDeadline: func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
			MaxConcurrent: 1,
		},
	)
	if err != nil {
		return err
	}
	complete := make(chan struct{}, 1)
	controlMux, err := shardcontrol.New(
		shardcontrol.Route{
			Discriminator: shardservice.MembershipGrantRequestDiscriminator(),
			Handler:       membershipControl,
		},
		shardcontrol.Route{
			Discriminator: snapshottransfer.BootstrapRequestDiscriminator(),
			Handler: rf3BootstrapCompletionHandler{
				Handler: service, Complete: complete,
			},
		},
		shardcontrol.Route{Discriminator: replicacontrol.CapacityRequestDiscriminator(), Handler: capacityControl},
	)
	if err != nil {
		return err
	}

	controlCtx, stopControl := context.WithCancelCause(parent)
	defer stopControl(context.Canceled)
	done := make(chan error, 1)
	go func() {
		done <- controlTLS.Serve(controlCtx, listener, servicetls.Limits{
			MaxConnections: 8, MaxHandshakes: 4,
			HandshakeDeadline: func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) },
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			if err := controlMux.Serve(ctx, connection); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "RF3 cold bootstrap control failed: %v\n", err)
			}
		})
	}()
	fmt.Fprintf(os.Stderr, "vibedb-shard RF3 cold bootstrap ready member=%d control=%s\n",
		base.Binding.MemberID, listener.Addr())
	select {
	case <-parent.Done():
		stopControl(context.Cause(parent))
		return componentShutdownError(<-done)
	case err = <-done:
		return err
	case <-complete:
		stopControl(context.Canceled)
		_ = <-done
	}
	if err = host.Close(); err != nil {
		return err
	}
	hostOwned = false
	databaseOwned = false // ownership transferred to the installed runtime and closed by Host.
	if err = errors.Join(repository.Close(), cursor.Close(), journal.Close()); err != nil {
		return err
	}
	return servePreparedRF3(parent, member)
}

type rf3BootstrapCompletionHandler struct {
	shardcontrol.Handler
	Complete chan<- struct{}
}

func (handler rf3BootstrapCompletionHandler) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	err := handler.Handler.Serve(ctx, connection)
	if err == nil {
		select {
		case handler.Complete <- struct{}{}:
		default:
		}
	}
	return err
}

func validateBootstrapRF3Topology(bootstrap bootstrapRF3Manifest, member rf3Manifest) error {
	target := member.EnrolledTarget
	if target == nil || bootstrap.ControlListener != target.ControlAddress ||
		profileTargetMember(member) != target.MemberID ||
		len(bootstrap.WALWrappedKey) == 0 || len(bootstrap.WALWrappedKey) > raftstore.MaxWrappedKeyBytes {
		return errInvalidBootstrapRF3Manifest
	}
	localSource := sourceMemberForBootstrap(bootstrap, member)
	if localSource == 0 || bootstrap.SourceSnapshotAddress == "" ||
		bootstrap.SourceSnapshotAddress == bootstrap.ControlListener ||
		bootstrap.MaxArtifactBytes > bootstrapRF3MaxArtifactBytes ||
		bootstrap.MaxArtifactBytes > ^uint64(0)-snapshottransfer.DescriptorBytes-(1<<20) {
		return errInvalidBootstrapRF3Manifest
	}
	paths := [...]string{
		bootstrap.MemberManifest, bootstrap.RepositoryPath, bootstrap.CursorPath,
		bootstrap.JournalPath, bootstrap.StaticBootstrapPath, member.WAL.Path,
		member.SQL.Path, member.WAL.KeyMaterialPath,
	}
	for index := range paths {
		clean := filepath.Clean(paths[index])
		if clean == "." || clean == string(filepath.Separator) || clean != paths[index] {
			return errInvalidBootstrapRF3Manifest
		}
		for prior := 0; prior < index; prior++ {
			if clean == paths[prior] {
				return errInvalidBootstrapRF3Manifest
			}
		}
	}
	if err := validateRF3Address(bootstrap.ControlListener, true); err != nil {
		return err
	}
	return validateRF3Address(bootstrap.SourceSnapshotAddress, false)
}

func profileTargetMember(member rf3Manifest) uint64 {
	base, _, err := loadRF3RetainedIdentities(member)
	if err != nil {
		return 0
	}
	return base.Binding.MemberID
}

func sourceMemberForBootstrap(bootstrap bootstrapRF3Manifest, member rf3Manifest) uint64 {
	for _, candidate := range member.Members {
		if candidate.NodeID == bootstrap.SourceNode {
			return candidate.MemberID
		}
	}
	return 0
}

type coldRF3Installer struct {
	repository      *snapshottransfer.Repository
	cursor          *replicatedstate.SnapshotCursorStore
	database        *sqldriver.Database
	base            sqldriver.ReplicatedShardStoreIdentity
	apply           sqldriver.ReplicatedApplyIdentity
	staticBootstrap *pb.Snapshot
	walPath         string
	walKey          raftstore.Key
	walOptions      raftstore.Options
	host            *multiraft.Host
	settlement      *snapshottransfer.LearnerInstallSettlement
	budget          *migrationbudget.Budget
	installed       raftmember.RuntimeIdentity
}

func (installer *coldRF3Installer) ObserveInstalled(
	_ context.Context,
	descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, bool, error) {
	if installer.installed == (raftmember.RuntimeIdentity{}) {
		return raftmember.RuntimeIdentity{}, false, nil
	}
	status, err := installer.host.Status(descriptor.Group)
	return installer.installed, err == nil && status.MemberID == descriptor.TargetMember, err
}

func (installer *coldRF3Installer) InstallPublishedLearner(
	ctx context.Context,
	descriptor snapshottransfer.Descriptor,
) (raftmember.RuntimeIdentity, error) {
	manifest, err := installer.repository.Manifest(descriptor)
	if err != nil {
		return raftmember.RuntimeIdentity{}, err
	}
	identity, err := snapshottransfer.InstallPublishedLearner(snapshottransfer.LearnerInstallPlan{
		Repository: installer.repository, Descriptor: descriptor, Cursor: installer.cursor,
		Database: installer.database, SQLIdentity: installer.base,
		Context: ctx, Budget: installer.budget,
		ApplyOptions:      replicatedApplyOptions(installer.apply),
		StageOptions:      replicatedstate.SnapshotArtifactStageOptions{},
		StaticBootstrap:   installer.staticBootstrap,
		ExpectedConfState: proto.Clone(manifest.State.ConfState).(*pb.ConfState),
		WALPath:           installer.walPath, WALIdentity: walIdentityFromBinding(installer.base.Binding),
		WALKey: installer.walKey, WALOptions: installer.walOptions,
		Authority: installer.base.Binding.Authority, Host: installer.host,
		Settlement: installer.settlement,
	})
	if err == nil {
		installer.installed = identity
	}
	return identity, err
}

func replicatedApplyOptions(identity sqldriver.ReplicatedApplyIdentity) sqldriver.ReplicatedApplyOptions {
	return sqldriver.ReplicatedApplyOptions{
		MaxSessions: identity.MaxSessions, RetryWindow: identity.RetryWindow,
		TxnLimits: identity.TxnLimits, Placement: identity.Placement,
		RequestLedgerCapacityBytes:       identity.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: identity.RequestLedgerCleanupReserveBytes,
		RequestLedgerRangeStart:          identity.RequestLedgerRangeStart,
		RequestLedgerRangeEnd:            identity.RequestLedgerRangeEnd,
		RequestLedgerRangeIdentity:       identity.RequestLedgerRangeIdentity,
	}
}

var _ snapshottransfer.BootstrapInstaller = (*coldRF3Installer)(nil)
