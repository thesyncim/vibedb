package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type preparedColdRF3Group struct {
	group        raftmember.GroupKey
	service      *snapshottransfer.BootstrapControlService
	authority    coldRF3GrantAuthority
	repository   *snapshottransfer.Repository
	cursor       *replicatedstate.SnapshotCursorStore
	journal      *snapshottransfer.BootstrapFileJournal
	database     *sqldriver.Database
	key          raftstore.Key
	installer    *coldRF3Installer
	installedWAL bool
}

func (group *preparedColdRF3Group) close() error {
	if group == nil {
		return nil
	}
	var err error
	if group.database != nil && (group.installer == nil || group.installer.installed == (raftmember.RuntimeIdentity{})) {
		err = errors.Join(err, group.database.Close())
	}
	if group.repository != nil {
		err = errors.Join(err, group.repository.Close())
	}
	if group.cursor != nil {
		err = errors.Join(err, group.cursor.Close())
	}
	if group.journal != nil {
		err = errors.Join(err, group.journal.Close())
	}
	clear(group.key.Material[:])
	return err
}

type coldRF3GrantAuthorityRouter map[raftmember.GroupKey]coldRF3GrantAuthority

func (router coldRF3GrantAuthorityRouter) InstallTransitionGrant(grant membershipgrant.Grant) error {
	authority, ok := router[grant.Group]
	if !ok {
		return errRF3MembershipGrant
	}
	return authority.InstallTransitionGrant(grant)
}

func bootstrapPreparedRF3Groups(
	parent context.Context, bootstrap bootstrapRF3Manifest, members []rf3Manifest,
) (resultErr error) {
	if parent == nil || len(members) < 2 || len(members) != len(bootstrap.Groups) {
		return errInvalidBootstrapRF3Manifest
	}
	combined, err := combineColdRF3MemberManifests(members)
	if err != nil {
		return err
	}
	pending := make([]int, 0, len(members))
	for index, member := range members {
		if _, statErr := os.Stat(member.WAL.Path); errors.Is(statErr, os.ErrNotExist) {
			pending = append(pending, index)
		} else if statErr != nil {
			return statErr
		}
	}
	if len(pending) == 0 {
		return servePreparedRF3(parent, combined)
	}
	profile, err := servicetls.LoadProfile(combined.TLS.Certificate, combined.TLS.Key,
		combined.TLS.Roots, combined.TLS.IdentityOID, time.Now)
	if err != nil {
		return err
	}
	policy, err := serviceauthz.LoadFile(combined.AuthorizationPolicy)
	if err != nil {
		return err
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		return err
	}
	authorizer, err := servicetls.NewNodeAuthorizer(policy.NodesWith(serviceauthz.CapabilityTopology))
	if err != nil {
		return err
	}
	controlTLS, err := servicetls.NewServer(profile, rafttransport.TrafficShardControl, authorizer)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", bootstrap.ControlListener)
	if err != nil {
		return err
	}
	defer listener.Close()
	deadline := func() time.Time { return time.Now().Add(bootstrapRF3NetworkTimeout) }
	prepared := make([]*preparedColdRF3Group, 0, len(pending))
	host, err := multiraft.NewHost(rf3HostLimitsForGroups(len(pending)))
	if err != nil {
		return err
	}
	hostOwned := true
	defer func() {
		if hostOwned {
			resultErr = errors.Join(resultErr, host.Close())
		}
	}()
	defer func() {
		for _, group := range prepared {
			resultErr = errors.Join(resultErr, group.close())
		}
	}()
	authorities := make(coldRF3GrantAuthorityRouter, len(pending))
	services := make([]snapshottransfer.GroupBootstrapControlService, 0, len(pending))
	for _, index := range pending {
		group, prepareErr := prepareColdRF3Group(bootstrap.withGroup(bootstrap.Groups[index]),
			members[index], profile, gate, deadline, host)
		if prepareErr != nil {
			return prepareErr
		}
		prepared = append(prepared, group)
		authorities[group.group] = group.authority
		services = append(services, snapshottransfer.GroupBootstrapControlService{
			Group: group.group, Service: group.service,
		})
	}
	for _, member := range members {
		base, _, loadErr := loadRF3RetainedIdentities(member)
		if loadErr != nil {
			return loadErr
		}
		group := groupFromBinding(base.Binding)
		if _, found := authorities[group]; !found {
			target := member.EnrolledTarget
			if target == nil {
				return errInvalidBootstrapRF3Manifest
			}
			authorities[group] = coldRF3GrantAuthority{group: group, members: member.Members, target: *target}
		}
	}
	grantRouter, err := openDurableRF3GrantRouter(combined, authorities)
	if err != nil {
		return err
	}
	membershipControl, err := shardservice.NewMembershipGrantControlService(
		grantRouter, policy, deadline, deadline,
	)
	if err != nil {
		return err
	}
	metricsControl, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider: &coldRF3MetricsProvider{groups: prepared},
		Authorize: func(identity rafttransport.PeerIdentity) bool {
			return policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		}, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		return err
	}
	complete := make(chan struct{}, 1)
	completed := make(map[raftmember.GroupKey]struct{}, len(prepared))
	var completeMu sync.Mutex
	bootstrapControl, err := snapshottransfer.NewGroupBootstrapControlRegistry(
		snapshottransfer.GroupBootstrapControlRegistryOptions{
			TrustDomain: profile.LocalIdentity().TrustDomain, Services: services,
			ReadDeadline: deadline, MaxConnections: min(8, len(services)),
			Complete: func(group raftmember.GroupKey) {
				completeMu.Lock()
				completed[group] = struct{}{}
				all := len(completed) == len(services)
				completeMu.Unlock()
				if all {
					select {
					case complete <- struct{}{}:
					default:
					}
				}
			},
		},
	)
	if err != nil {
		return err
	}
	controlMux, err := shardcontrol.New(
		shardcontrol.Route{Discriminator: shardservice.MembershipGrantRequestDiscriminator(), Handler: membershipControl},
		shardcontrol.Route{Discriminator: servicemetrics.RequestDiscriminator(), Handler: metricsControl},
		shardcontrol.Route{Discriminator: snapshottransfer.BootstrapRequestDiscriminator(), Handler: bootstrapControl},
	)
	if err != nil {
		return err
	}
	controlCtx, stopControl := context.WithCancelCause(parent)
	defer stopControl(context.Canceled)
	done := make(chan error, 1)
	go func() {
		done <- controlTLS.Serve(controlCtx, listener, servicetls.Limits{
			MaxConnections: 8, MaxHandshakes: 4, HandshakeDeadline: deadline,
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			_ = controlMux.Serve(ctx, connection)
		})
	}()
	fmt.Fprintf(os.Stderr, "vibedb-shard RF3 cold bootstrap ready groups=%d control=%s\n", len(members), listener.Addr())
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
	for _, group := range prepared {
		if err = group.close(); err != nil {
			return err
		}
	}
	prepared = prepared[:0]
	return servePreparedRF3(parent, combined)
}

func prepareColdRF3Group(
	bootstrap bootstrapRF3Manifest, member rf3Manifest, profile *rafttransport.PeerTLS,
	gate *serviceauthz.Gate, deadline rafttransport.DeadlineFunc, host *multiraft.Host,
) (_ *preparedColdRF3Group, resultErr error) {
	if host == nil {
		return nil, errInvalidBootstrapRF3Manifest
	}
	if err := validateBootstrapRF3Topology(bootstrap, member); err != nil {
		return nil, err
	}
	base, applyIdentity, err := loadRF3RetainedIdentities(member)
	if err != nil {
		return nil, err
	}
	target := member.EnrolledTarget
	group := groupFromBinding(base.Binding)
	wantDomain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	if target == nil || profile.LocalIdentity().Node != target.NodeID || profile.LocalIdentity().TrustDomain != wantDomain {
		return nil, errInvalidBootstrapRF3Manifest
	}
	prepared := &preparedColdRF3Group{group: group,
		authority: coldRF3GrantAuthority{group: group, members: member.Members, target: *target}}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, prepared.close())
		}
	}()
	prepared.database, err = sqldriver.OpenReplicatedShardStoreWithApply(member.SQL.Path, base, applyIdentity)
	if err != nil {
		return nil, err
	}
	if _, err = prepared.database.RequireReplicatedShardStore(base); err != nil {
		return nil, err
	}
	staticRaw, err := readRF3BoundedFile(bootstrap.StaticBootstrapPath, replicatedstate.MaxStaticBootstrapEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	staticBootstrap := new(pb.Snapshot)
	if err = proto.Unmarshal(staticRaw, staticBootstrap); err != nil {
		return nil, err
	}
	clear(staticRaw)
	prepared.repository, err = snapshottransfer.OpenRepository(bootstrap.RepositoryPath,
		snapshottransfer.Limits{MaxArtifacts: 1, MaxArtifactBytes: bootstrap.MaxArtifactBytes,
			MaxDiskBytes: bootstrap.MaxArtifactBytes + snapshottransfer.DescriptorBytes + 1<<20})
	if err != nil {
		return nil, err
	}
	prepared.cursor, err = replicatedstate.OpenSnapshotCursorStore(bootstrap.CursorPath)
	if err != nil {
		return nil, err
	}
	prepared.journal, err = snapshottransfer.OpenBootstrapFileJournal(bootstrap.JournalPath, 1)
	if err != nil {
		return nil, err
	}
	prepared.key, err = loadRF3WALKey(member.WAL.KeyID, member.WAL.KeyMaterialPath)
	if err != nil {
		return nil, err
	}
	opener := rafttransport.TLSSnapshotStreamOpener{TLS: profile,
		Open: func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
			if node != bootstrap.SourceNode {
				return nil, snapshottransfer.ErrBootstrapUnauthorized
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", bootstrap.SourceSnapshotAddress)
		}, HandshakeDeadline: deadline}
	receiver := &snapshottransfer.Receiver{Repository: prepared.repository, Opener: opener,
		ReadDeadline: deadline, WriteDeadline: deadline,
		Workspace: make([]byte, snapshottransfer.AbsoluteMaxChunkBytes)}
	prepared.installer = &coldRF3Installer{repository: prepared.repository, cursor: prepared.cursor,
		database: prepared.database, base: base, apply: applyIdentity, staticBootstrap: staticBootstrap,
		walPath: member.WAL.Path, walKey: prepared.key, walOptions: member.WAL.Options,
		host: host, settlement: new(snapshottransfer.LearnerInstallSettlement)}
	prepared.service, err = snapshottransfer.NewBootstrapControlService(snapshottransfer.BootstrapControlOptions{
		Journal: prepared.journal, Receiver: receiver, Installer: prepared.installer, Releaser: prepared.repository,
		Authorize: func(identity rafttransport.PeerIdentity, request snapshottransfer.BootstrapRequest) bool {
			d := request.Descriptor
			return identity.TrustDomain == profile.LocalIdentity().TrustDomain &&
				gate.Check(identity.Node, gate.Generation(), serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow &&
				d.Group == group && d.TargetMember == base.Binding.MemberID && d.TargetStore == base.Binding.StoreID &&
				d.TargetIncarnation == target.NodeIncarnation
		}, SourceNode: func(descriptor snapshottransfer.Descriptor) (rafttransport.NodeID, bool) {
			return bootstrap.SourceNode, descriptor.Group == group &&
				descriptor.SourceMember == sourceMemberForBootstrap(bootstrap, member)
		}, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func combineColdRF3MemberManifests(members []rf3Manifest) (rf3Manifest, error) {
	if len(members) < 2 || len(members) > maxRF3ManifestGroups {
		return rf3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	combined := members[0]
	combined.Groups = make([]rf3ManifestGroup, 0, len(members))
	for _, member := range members {
		bundles := member.groupBundles()
		if len(bundles) != 1 || member.Listeners != combined.Listeners || member.TLS != combined.TLS ||
			member.AuthorizationPolicy != combined.AuthorizationPolicy || member.DevelopmentOnly != combined.DevelopmentOnly {
			return rf3Manifest{}, errInvalidBootstrapRF3Manifest
		}
		combined.Groups = append(combined.Groups, bundles[0])
	}
	return combined, nil
}
