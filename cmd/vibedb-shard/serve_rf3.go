package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterbackupservice"
	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitartifact"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	publicshardcontrol "github.com/thesyncim/vibedb/shardcontrol"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	rf3IdentityFileBytes                 = 256 << 10
	rf3TickInterval                      = 50 * time.Millisecond
	rf3DefaultWALGenerationIntervalTicks = uint64((10 * time.Minute) / rf3TickInterval)
	rf3NetworkTimeout                    = 10 * time.Second
	rf3RequestTimeout                    = 15 * time.Second
	rf3DefaultExecutionLanes             = 8
	rf3SchemaInstallRecords              = 256
	rf3SchemaInstallArtifacts            = 16
	rf3SchemaInstallDiskBytes            = 1 << 30
)

// A variable keeps the production default immutable in behavior while letting
// the external command qualification compress ten-minute maintenance windows
// without adding a serving flag or environment-controlled production path.
var rf3WALGenerationIntervalTicks = rf3DefaultWALGenerationIntervalTicks

var errRF3Serving = errors.New("vibedb-shard: invalid RF3 serving configuration")

func rf3ControlNodes(policy *serviceauthz.Policy) []rafttransport.NodeID {
	if policy == nil {
		return nil
	}
	nodes := append(policy.NodesWith(serviceauthz.CapabilityMembership),
		policy.NodesWith(serviceauthz.CapabilitySchema)...)
	nodes = append(nodes, policy.NodesWith(serviceauthz.CapabilityTopology)...)
	nodes = append(nodes, policy.NodesWith(serviceauthz.CapabilityBackup)...)
	nodes = append(nodes, policy.NodesWith(serviceauthz.CapabilityRestoreActivate)...)
	slices.SortFunc(nodes, func(a, b rafttransport.NodeID) int { return bytes.Compare(a[:], b[:]) })
	return slices.Compact(nodes)
}

func runServeRF3(args []string) int {
	fs := flag.NewFlagSet("serve-rf3", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "canonical prepared RF3 member manifest")
	executionLanes := fs.Int("execution-lanes", rf3DefaultExecutionLanes, "power-of-two Raft execution lanes")
	if err := fs.Parse(args); err != nil || *manifestPath == "" || fs.NArg() != 0 ||
		!validRF3ExecutionLanes(*executionLanes) {
		usage()
		return 2
	}
	manifest, err := loadRF3Manifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error RF3 manifest: %v\n", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := servePreparedRF3WithExecutionLanes(ctx, manifest, *executionLanes, net.Listen); err != nil {
		fmt.Fprintf(os.Stderr, "error serve RF3: %v\n", err)
		return 1
	}
	return 0
}

// servePreparedRF3 opens only previously prepared durable artifacts. It never
// creates a WAL, SQL root, apply namespace, identity, or bootstrap authority.
func servePreparedRF3(parent context.Context, manifest rf3Manifest) error {
	return servePreparedRF3WithExecutionLanes(parent, manifest, rf3DefaultExecutionLanes, net.Listen)
}

type rf3ListenFunc func(network, address string) (net.Listener, error)

type preparedRF3Group struct {
	manifest         rf3Manifest
	base             sqldriver.ReplicatedShardStoreIdentity
	applyIdentity    sqldriver.ReplicatedApplyIdentity
	key              raftstore.Key
	wal              *raftstore.Store
	database         *sqldriver.Database
	apply            *sqldriver.ReplicatedApply
	publication      raftmodel.Publication
	restoreOperation [32]byte
}

type preparedRF3Set struct {
	groups           []preparedRF3Group
	members          []rafttransport.Member
	remoteNodes      []rafttransport.NodeID
	dial             rafttransport.RawPeerDialFunc
	nativeConfigured bool
}

func (group *preparedRF3Group) close(cause error) error {
	if group == nil {
		return cause
	}
	clear(group.key.Material[:])
	return errors.Join(cause, group.apply.Close(), group.database.Close(), group.wal.Close())
}

func closePreparedRF3Groups(groups []preparedRF3Group, cause error) error {
	for index := range groups {
		cause = groups[index].close(cause)
	}
	return cause
}

func prepareRF3GroupSet(manifest rf3Manifest, profile *rafttransport.PeerTLS) (preparedRF3Set, error) {
	var result preparedRF3Set
	bundles := manifest.groupBundles()
	result.groups = make([]preparedRF3Group, 0, len(bundles))
	result.members = make([]rafttransport.Member, 0, len(bundles)*rf3ManifestMembers)
	seen := make(map[raftmember.GroupKey]struct{}, len(bundles))
	addresses := make(map[rafttransport.NodeID]string, rf3ManifestMembers)
	for index, bundle := range bundles {
		single := manifest.withGroup(bundle)
		base, applyIdentity, err := loadRF3RetainedIdentities(single)
		if err != nil {
			return result, closePreparedRF3Groups(result.groups, err)
		}
		if !rf3SplitChildTemplateMatchesRetained(
			manifest.SplitControl.ChildRegistry, base, applyIdentity,
		) {
			return result, closePreparedRF3Groups(result.groups,
				fmt.Errorf("%w: group %d split child template differs from retained SQL/apply", errRF3Serving, index))
		}
		group := groupFromBinding(base.Binding)
		if !rf3RouteMatchesBinding(bundle.Route, base.Binding) {
			return result, closePreparedRF3Groups(result.groups,
				fmt.Errorf("%w: group %d route differs from retained identity", errRF3Serving, index))
		}
		if _, duplicate := seen[group]; duplicate {
			return result, closePreparedRF3Groups(result.groups, fmt.Errorf("%w: duplicate retained group", errRF3Serving))
		}
		seen[group] = struct{}{}
		want := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
		if profile.LocalIdentity().TrustDomain != want {
			return result, closePreparedRF3Groups(result.groups, fmt.Errorf("%w: group %d trust domain differs from retained identity", errRF3Serving, index))
		}
		key, err := loadRF3WALKey(bundle.WAL.KeyID, bundle.WAL.KeyMaterialPath)
		if err != nil {
			return result, closePreparedRF3Groups(result.groups, err)
		}
		wal, err := raftstore.Open(bundle.WAL.Path, walIdentityFromBinding(base.Binding), base.Binding.TopologyRecoveryEpoch, key, bundle.WAL.Options)
		if err != nil {
			clear(key.Material[:])
			return result, closePreparedRF3Groups(result.groups, fmt.Errorf("open RF3 WAL group %d: %w", index, err))
		}
		database, apply, err := raftmember.OpenBoundSQLWithApplyRecoveringGeneration(bundle.SQL.Path, wal, base.Binding.Authority, base, applyIdentity)
		if err != nil {
			clear(key.Material[:])
			return result, closePreparedRF3Groups(result.groups, errors.Join(err, wal.Close()))
		}
		item := preparedRF3Group{manifest: single, base: base, applyIdentity: applyIdentity, key: key, wal: wal, database: database, apply: apply, publication: apply.Published()}
		item.restoreOperation, err = validateRestoredRF3Bootstrap(wal, bundle.SQL.Path, base.Binding.MemberID)
		if err != nil {
			return result, closePreparedRF3Groups(append(result.groups, item), err)
		}
		if err = rejectRF3UnappliedMembership(wal, item.publication.Applied); err != nil {
			return result, closePreparedRF3Groups(append(result.groups, item), err)
		}
		roster, _, _, native, err := buildRF3Roster(single, group, base.Binding.MemberID, item.publication)
		if err != nil {
			return result, closePreparedRF3Groups(append(result.groups, item), err)
		}
		for _, member := range roster {
			address := peerAddressForRF3Member(single, member.MemberID)
			if prior, found := addresses[member.Node]; found && prior != address {
				return result, closePreparedRF3Groups(append(result.groups, item), fmt.Errorf("%w: node address differs across groups", errRF3Serving))
			}
			addresses[member.Node] = address
		}
		result.groups = append(result.groups, item)
		result.members = append(result.members, roster...)
		result.nativeConfigured = result.nativeConfigured || native
	}
	for node := range addresses {
		if node != profile.LocalIdentity().Node {
			result.remoteNodes = append(result.remoteNodes, node)
		}
	}
	slices.SortFunc(result.remoteNodes, func(a, b rafttransport.NodeID) int { return bytes.Compare(a[:], b[:]) })
	dialer := net.Dialer{Timeout: rf3NetworkTimeout}
	result.dial = func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		address, found := addresses[node]
		if !found {
			return nil, rafttransport.ErrNodeNotFound
		}
		return dialer.DialContext(ctx, "tcp", address)
	}
	return result, nil
}

func rf3SplitChildTemplateMatchesRetained(
	registry rf3ManifestSplitChildRegistry,
	base sqldriver.ReplicatedShardStoreIdentity,
	apply sqldriver.ReplicatedApplyIdentity,
) bool {
	return registry.Table == base.UserTable &&
		registry.Apply.MaxSessions == apply.MaxSessions &&
		registry.Apply.RetryWindow == apply.RetryWindow &&
		registry.Apply.TxnLimits == apply.TxnLimits &&
		registry.Apply.RequestLedgerCapacityBytes == apply.RequestLedgerCapacityBytes &&
		registry.Apply.RequestLedgerCleanupReserveBytes == apply.RequestLedgerCleanupReserveBytes &&
		registry.Apply.RequestLedgerRangeStart == apply.RequestLedgerRangeStart &&
		registry.Apply.RequestLedgerRangeEnd == apply.RequestLedgerRangeEnd &&
		registry.Apply.RequestLedgerRangeIdentity == apply.RequestLedgerRangeIdentity &&
		registry.Apply.Format == apply.Placement.Format &&
		registry.Apply.ShardKey == apply.Placement.ShardKey &&
		registry.Apply.TupleVersion == apply.Placement.TupleVersion &&
		registry.Apply.MapperVersion == apply.Placement.MapperVersion
}

func rf3RouteMatchesBinding(
	route rf3ManifestGroupRoute,
	binding sqldriver.ReplicatedShardStoreBinding,
) bool {
	return route.Group == groupFromBinding(binding) &&
		route.Distribution == binding.Distribution && route.Shard == binding.Shard &&
		route.AllocationGeneration == binding.AllocationGeneration &&
		route.MemberID == binding.MemberID && route.StoreID == binding.StoreID
}

func peerAddressForRF3Member(manifest rf3Manifest, memberID uint64) string {
	for _, member := range manifest.memberRoster() {
		if member.MemberID == memberID {
			return member.PeerAddress
		}
	}
	if manifest.EnrolledTarget != nil && manifest.EnrolledTarget.MemberID == memberID {
		return manifest.EnrolledTarget.PeerAddress
	}
	return ""
}

func servePreparedRF3WithListen(
	parent context.Context,
	manifest rf3Manifest,
	listen rf3ListenFunc,
) error {
	return servePreparedRF3WithExecutionLanes(parent, manifest, rf3DefaultExecutionLanes, listen)
}

func servePreparedRF3WithExecutionLanes(
	parent context.Context,
	manifest rf3Manifest,
	executionLaneCount int,
	listen rf3ListenFunc,
) (resultErr error) {
	if parent == nil {
		return errRF3Serving
	}
	if listen == nil {
		return errRF3Serving
	}
	if !validRF3ExecutionLanes(executionLaneCount) {
		return fmt.Errorf("%w: execution lanes must be a power of two between 1 and %d", errRF3Serving, multiraft.AbsoluteMaxExecutionLanes)
	}
	if cause := context.Cause(parent); cause != nil {
		return componentShutdownError(cause)
	}
	if err := validateRF3Addresses(manifest); err != nil {
		return err
	}
	profile, err := servicetls.LoadProfile(
		manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots,
		manifest.TLS.IdentityOID, time.Now,
	)
	if err != nil {
		return fmt.Errorf("%w: TLS profile: %v", errRF3Serving, err)
	}
	policy, err := serviceauthz.LoadFile(manifest.AuthorizationPolicy)
	if err != nil {
		return fmt.Errorf("%w: authorization policy: %v", errRF3Serving, err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		return fmt.Errorf("%w: authorization gate: %v", errRF3Serving, err)
	}
	nativeTLS, err := shardservice.NewReplicatedServerTLS(
		profile, policy.NodesWith(serviceauthz.CapabilityDelegate),
	)
	if err != nil {
		return fmt.Errorf("%w: native TLS authority: %v", errRF3Serving, err)
	}
	controlAuthorizer, err := servicetls.NewNodeAuthorizer(
		rf3ControlNodes(policy),
	)
	if err != nil {
		return fmt.Errorf("%w: control TLS authority: %v", errRF3Serving, err)
	}
	controlTLS, err := servicetls.NewServer(
		profile, rafttransport.TrafficShardControl, controlAuthorizer,
	)
	if err != nil {
		return fmt.Errorf("%w: control TLS server: %v", errRF3Serving, err)
	}

	preparedSet, err := prepareRF3GroupSet(manifest, profile)
	if err != nil {
		return err
	}
	closePrepared := func(cause error) error { return closePreparedRF3Groups(preparedSet.groups, cause) }
	first := &preparedSet.groups[0]
	base := first.base
	group := groupFromBinding(base.Binding)
	members, remoteNodes, dial := preparedSet.members, preparedSet.remoteNodes, preparedSet.dial
	nativeConfigured := preparedSet.nativeConfigured
	transportRegistry, err := rafttransport.NewStaticRegistry(
		profile.LocalIdentity().Node, members,
		rafttransport.Limits{MaxGroups: len(preparedSet.groups), MaxMembers: len(members)},
	)
	if err != nil {
		return closePrepared(fmt.Errorf("%w: transport roster: %v", errRF3Serving, err))
	}
	grantInstaller, err := openDurableRF3GrantRouter(manifest, transportRegistry)
	if err != nil {
		return closePrepared(fmt.Errorf("%w: restore membership grant: %v", errRF3Serving, err))
	}
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	membershipControl, err := shardservice.NewMembershipGrantControlService(
		grantInstaller, policy, deadline, deadline,
	)
	if err != nil {
		return closePrepared(fmt.Errorf("%w: membership grant control: %v", errRF3Serving, err))
	}
	for index := range preparedSet.groups {
		item := &preparedSet.groups[index]
		localMember, localErr := transportRegistry.LocalMember(groupFromBinding(item.base.Binding))
		if localErr != nil || localMember != item.base.Binding.MemberID {
			return closePrepared(fmt.Errorf("%w: certificate node does not own retained member for group %d", errRF3Serving, index))
		}
	}

	// Reserve every enabled socket before AdoptRuntime durably advances the node
	// incarnation. A bind failure leaves the prepared member restartable without
	// an unexplained incarnation jump.
	peerListener, err := listen("tcp", manifest.Listeners.Peer)
	if err != nil {
		return closePrepared(fmt.Errorf("listen RF3 peer %q: %w", manifest.Listeners.Peer, err))
	}
	defer peerListener.Close()
	controlListener, err := listen("tcp", manifest.Listeners.Control)
	if err != nil {
		return errors.Join(
			closePrepared(fmt.Errorf("listen RF3 control %q: %w", manifest.Listeners.Control, err)),
			peerListener.Close(),
		)
	}
	defer controlListener.Close()
	snapshotListener, err := listen("tcp", manifest.Listeners.Snapshot)
	if err != nil {
		return errors.Join(
			closePrepared(fmt.Errorf("listen RF3 snapshot %q: %w", manifest.Listeners.Snapshot, err)),
			peerListener.Close(), controlListener.Close(),
		)
	}
	defer snapshotListener.Close()
	var nativeListener net.Listener
	if nativeConfigured {
		nativeListener, err = listen("tcp", manifest.Listeners.Native)
		if err != nil {
			return errors.Join(
				closePrepared(fmt.Errorf("listen RF3 native %q: %w", manifest.Listeners.Native, err)),
				peerListener.Close(),
			)
		}
		defer nativeListener.Close()
	}
	if cause := context.Cause(parent); cause != nil {
		return closePrepared(componentShutdownError(cause))
	}

	runtimes := make([]*raftmember.Runtime, 0, len(preparedSet.groups))
	identities := make([]raftmember.RuntimeIdentity, 0, len(preparedSet.groups))
	commands := make([]raftservice.CommandFence, 0, len(preparedSet.groups))
	readSources := make([]raftservice.ReadSource, 0, len(preparedSet.groups))
	recoverySources := make([]raftservice.TransactionRecoverySource, 0, len(preparedSet.groups))
	publications := make([]raftmodel.Publication, 0, len(preparedSet.groups))
	closeAdopted := func(cause error) error {
		for _, runtime := range runtimes {
			cause = errors.Join(cause, runtime.Close())
		}
		return cause
	}
	for index := range preparedSet.groups {
		item := &preparedSet.groups[index]
		runtime, adoptErr := raftmember.AdoptRuntime(item.wal, item.database, item.apply)
		if adoptErr != nil {
			remaining := preparedSet.groups[index:]
			if runtime != nil {
				adoptErr = errors.Join(adoptErr, runtime.Close())
				remaining = preparedSet.groups[index+1:]
			}
			return errors.Join(closeAdopted(adoptErr), closePreparedRF3Groups(remaining, nil))
		}
		runtimes = append(runtimes, runtime)
		if adoptErr = runtime.ConfigureWALGeneration(raftmember.WALGenerationDriverOptions{
			IntervalTicks: rf3WALGenerationIntervalTicks, Key: item.key,
			OnError: func(err error) { fmt.Fprintf(os.Stderr, "vibedb-shard RF3 WAL generation deferred: %v\n", err) },
		}); adoptErr != nil {
			return errors.Join(closeAdopted(adoptErr), closePreparedRF3Groups(preparedSet.groups[index+1:], nil))
		}
		clear(item.key.Material[:])
		runtimePublication, publicationErr := runtime.Publication()
		if publicationErr != nil || runtimePublication.ReplicaSetVersion != item.publication.ReplicaSetVersion || !proto.Equal(runtimePublication.ConfState, item.publication.ConfState) {
			return errors.Join(closeAdopted(errors.Join(fmt.Errorf("%w: group %d publication changed during adoption", errRF3Serving, index), publicationErr)), closePreparedRF3Groups(preparedSet.groups[index+1:], nil))
		}
		identity := runtime.Identity()
		identities = append(identities, identity)
		publications = append(publications, runtimePublication)
		commands = append(commands, commandFenceFromPublication(item.base.Binding.Authority, identity, runtimePublication.ReplicaSetVersion))
		readSources = append(readSources, item.apply)
		recoverySources = append(recoverySources, item.apply)
	}
	restoreGates := make(map[raftmember.GroupKey]*shardservice.RestoreServingGate)
	restoreOperations := make(map[raftmember.GroupKey][32]byte)
	restoreGateList := make([]*shardservice.RestoreServingGate, 0, len(preparedSet.groups))
	for index := range preparedSet.groups {
		item := &preparedSet.groups[index]
		operation := item.restoreOperation
		if operation == ([32]byte{}) {
			continue
		}
		itemGroup := groupFromBinding(item.base.Binding)
		restoreGate, gateErr := shardservice.NewRestoreServingGate(
			identities[index], profile.LocalIdentity().Node, operation,
		)
		if gateErr != nil {
			return closeAdopted(gateErr)
		}
		restoreGates[itemGroup] = restoreGate
		restoreOperations[itemGroup] = operation
		restoreGateList = append(restoreGateList, restoreGate)
	}
	runtimePublication, _ := runtimes[0].Publication()

	servingRegistry, err := raftserve.NewRegistry(rf3RegistryLimitsForGroups(len(runtimes)))
	if err != nil {
		return closeAdopted(err)
	}
	lanes, err := servingRegistry.NewExecutionLanes(executionLaneCount, rf3HostLimitsForGroups(len(runtimes)))
	if err != nil {
		return errors.Join(closeAdopted(err), servingRegistry.Close())
	}
	for _, runtime := range runtimes {
		if err := lanes.Add(runtime); err != nil {
			return errors.Join(err, lanes.Close(), servingRegistry.Close())
		}
	}

	pulse := make(chan struct{}, 1)
	progressMetrics := new(raftservice.ProgressMetrics)
	if err := progressMetrics.ConfigureGroups(identities); err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	peer, err := raftservice.NewAuthenticatedExecutionPeerRuntime(raftservice.AuthenticatedExecutionPeerOptions{
		Registry: transportRegistry, TLS: profile, Dial: dial, Listener: peerListener,
		HandshakeDeadline: deadline, MaxInboundStreams: 8,
		Execution: raftservice.ExecutionOptions{
			Registry: servingRegistry, Lanes: lanes,
			Members:                    identities,
			CommandFences:              commands,
			ReadSources:                readSources,
			TransactionRecoverySources: recoverySources,
			Pulse:                      pulse,
			Limits:                     rf3OwnerLimits(),
			ProgressMetrics:            progressMetrics,
		},
		Transport: rf3TransportOptions(remoteNodes, deadline),
		Receiver: rafttransport.OrdinaryReceiverOptions{
			ReadDeadline:       deadline,
			RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
		},
	})
	if err != nil {
		return errors.Join(err, lanes.Close(), servingRegistry.Close())
	}
	servedGroups := make(map[raftmember.GroupKey]raftmember.RuntimeIdentity, len(identities))
	for _, identity := range identities {
		servedGroups[identity.Group] = identity
	}
	observationControl, err := replicacontrol.NewService(replicacontrol.ServiceOptions{
		Observer: peer.Owners(),
		Authorize: func(identity rafttransport.PeerIdentity, request replicacontrol.Request) bool {
			_, served := servedGroups[request.Group]
			return served &&
				policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	backupControl, err := clusterbackupservice.New(clusterbackupservice.Options{
		Owner: peer.Owners(),
		Authorize: func(identity rafttransport.PeerIdentity, request clusterbackup.LiveRequest) bool {
			local, served := servedGroups[request.Group]
			return served && local.MemberID == request.SourceMember &&
				policy.Check(identity.Node, serviceauthz.CapabilityBackup) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
		ChunkBytes:    int(manifest.ReplicaControl.SourceChunkBytes),
		MaxConcurrent: manifest.ReplicaControl.MaxSourceConcurrent,
	})
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	actionJournal, err := replicaaction.OpenFileJournal(
		manifest.ReplicaControl.ActionJournalPath,
		manifest.ReplicaControl.MaxActionRecords,
	)
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, actionJournal.Close()) }()
	actionControl, err := replicaaction.NewService(replicaaction.Options{
		Journal: actionJournal, Owner: peer.Owners(),
		Authorize: func(identity rafttransport.PeerIdentity, request replicaaction.Request) bool {
			local, served := servedGroups[request.Fence.Group]
			return served && request.Fence.MemberID == local.MemberID &&
				policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 32,
	})
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	schemaActivator, err := newRF3SchemaActivator(peer.Owners(), preparedSet.groups)
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	schemaRoot := manifest.ReplicaControl.SourceDataRoot
	schemaJournal, err := schemainstall.OpenFileJournal(
		filepath.Join(schemaRoot, "schema-rollout-journal"), rf3SchemaInstallRecords,
	)
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, schemaJournal.Close()) }()
	schemaArtifacts, err := schemainstall.OpenDirectoryBackend(schemainstall.DirectoryOptions{
		Path:         filepath.Join(schemaRoot, "schema-rollout-artifacts"),
		MaxArtifacts: rf3SchemaInstallArtifacts, MaxDiskBytes: rf3SchemaInstallDiskBytes,
		Activator: schemaActivator,
	})
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, schemaArtifacts.Close()) }()
	if err = schemaActivator.bindArtifacts(schemaArtifacts); err != nil {
		return err
	}
	schemaInstaller, err := schemainstall.New(schemainstall.Options{
		Journal: schemaJournal, Backend: schemaArtifacts, MaxConcurrent: 8,
	})
	if err != nil {
		return err
	}
	schemaControl, err := schemainstall.NewControlService(schemainstall.ControlOptions{
		Installer: schemaInstaller,
		Authorize: func(identity rafttransport.PeerIdentity, request schemainstall.Request, _ schemainstall.Command) bool {
			_, served := servedGroups[request.Group]
			return served && policy.Check(identity.Node, serviceauthz.CapabilitySchema) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
		MaxBundleBytes: schemainstall.AbsoluteMaxBundleBytes,
	})
	if err != nil {
		return err
	}
	var sourceControl shardcontrol.Handler
	var sourceData shardcontrol.Handler
	var snapshotTLS *servicetls.Server
	dataServices := make([]snapshottransfer.GroupDataService, 0, len(preparedSet.groups))
	controlServices := make([]snapshottransfer.GroupSourceControlService, 0, len(preparedSet.groups))
	targetNodes := make([]rafttransport.NodeID, 0, len(preparedSet.groups))
	for index := range preparedSet.groups {
		item := &preparedSet.groups[index]
		target := item.manifest.EnrolledTarget
		groupIdentity := identities[index]
		publication := publications[index]
		itemGroup := groupIdentity.Group
		if target == nil || groupIdentity.MemberID == target.MemberID {
			continue
		}
		if policy.Check(target.NodeID, serviceauthz.CapabilityMembership) != serviceauthz.DecisionAllow {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(errRF3Serving, errors.New("enrolled target lacks membership capability"),
				componentShutdownError(peerErr), servingRegistry.Close())
		}
		sourceJournal, openErr := snapshottransfer.OpenSourceFileJournal(
			rf3SnapshotGroupPath(manifest.ReplicaControl.SourceJournalPath, itemGroup, len(preparedSet.groups) > 1),
			manifest.ReplicaControl.MaxSourceRecords,
		)
		if openErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(openErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		defer func(journal *snapshottransfer.SourceFileJournal) {
			resultErr = errors.Join(resultErr, journal.Close())
		}(sourceJournal)
		provider, providerErr := snapshottransfer.OpenRetainedSourceExportProvider(
			snapshottransfer.RetainedSourceExportOptions{
				DataRoot:       manifest.ReplicaControl.SourceDataRoot,
				RepositoryPath: rf3SnapshotGroupPath(manifest.ReplicaControl.SourceRepositoryPath, itemGroup, len(preparedSet.groups) > 1),
				Limits: snapshottransfer.Limits{
					MaxArtifacts:     manifest.ReplicaControl.MaxSourceArtifacts,
					MaxArtifactBytes: manifest.ReplicaControl.MaxSourceArtifactBytes,
					MaxDiskBytes:     manifest.ReplicaControl.MaxSourceDiskBytes,
				},
				ChunkBytes:      manifest.ReplicaControl.SourceChunkBytes,
				MaxConcurrent:   manifest.ReplicaControl.MaxSourceConcurrent,
				RuntimeIdentity: groupIdentity,
				SourceNode:      profile.LocalIdentity().Node,
				TargetMember:    target.MemberID, TargetStore: target.StoreID,
				TargetIncarnation: target.NodeIncarnation, Cut: item.apply,
			},
		)
		if providerErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(providerErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		if phase := os.Getenv("VIBEDB_QUALIFICATION_ABANDON_CRASH"); phase != "" {
			if os.Getenv("VIBEDB_REPLICA_REPLACEMENT_E2E") != "1" ||
				!provider.InstallAbandonmentExitFaultForQualification(phase, func() { os.Exit(97) }) {
				return errors.Join(errRF3Serving, errors.New("invalid abandonment qualification crash cut"))
			}
		}
		defer func(provider *snapshottransfer.RetainedSourceExportProvider) {
			resultErr = errors.Join(resultErr, provider.Close())
		}(provider)
		sourceService, serviceErr := snapshottransfer.NewSourceControlService(
			snapshottransfer.SourceControlOptions{
				Journal:  sourceJournal,
				Exporter: snapshottransfer.PinnedSourceControlExporter{Provider: provider},
				Authorize: func(peerIdentity rafttransport.PeerIdentity, request snapshottransfer.SourceControlRequest) bool {
					return request.Group == itemGroup && request.SourceMember == groupIdentity.MemberID &&
						request.SourceNode == profile.LocalIdentity().Node &&
						request.TargetMember == target.MemberID && request.TargetStore == target.StoreID &&
						request.TargetIncarnation == target.NodeIncarnation &&
						policy.Check(peerIdentity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
				},
				ReadDeadline: deadline, WriteDeadline: deadline,
				MaxConcurrent: manifest.ReplicaControl.MaxSourceConcurrent,
			},
		)
		if serviceErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(serviceErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		controlServices = append(controlServices, snapshottransfer.GroupSourceControlService{
			Group: itemGroup, Service: sourceService,
		})
		dataService, serviceErr := provider.NewDataService(snapshottransfer.ServiceOptions{
			Registry: transportRegistry,
			Authorize: func(descriptor snapshottransfer.Descriptor) bool {
				return descriptor.Group == itemGroup &&
					descriptor.SourceMember == groupIdentity.MemberID &&
					descriptor.TargetMember == target.MemberID &&
					descriptor.TargetStore == target.StoreID &&
					descriptor.TargetIncarnation == target.NodeIncarnation &&
					descriptor.SchemaGeneration == item.base.Binding.Authority.SchemaGeneration &&
					descriptor.ReplicaSetVersion == publication.ReplicaSetVersion
			},
			ReadDeadline: deadline, WriteDeadline: deadline,
			MaxConnections: manifest.ReplicaControl.MaxSourceConcurrent,
			MaxChunkBytes:  manifest.ReplicaControl.SourceChunkBytes,
			MaxInflightBytes: int64(manifest.ReplicaControl.SourceChunkBytes) *
				int64(manifest.ReplicaControl.MaxSourceConcurrent),
		})
		if serviceErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(serviceErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		dataServices = append(dataServices, snapshottransfer.GroupDataService{
			Group: itemGroup, Service: dataService,
		})
		targetNodes = append(targetNodes, target.NodeID)
	}
	if len(dataServices) != 0 {
		var serviceErr error
		sourceControl, serviceErr = snapshottransfer.NewGroupSourceControlRegistry(
			snapshottransfer.GroupSourceControlRegistryOptions{
				Registry: transportRegistry, Services: controlServices,
				ReadDeadline: deadline, MaxConnections: manifest.ReplicaControl.MaxSourceConcurrent,
			},
		)
		if serviceErr == nil {
			sourceData, serviceErr = snapshottransfer.NewGroupDataRegistry(
				snapshottransfer.GroupDataRegistryOptions{
					Registry: transportRegistry, Services: dataServices,
					ReadDeadline: deadline, MaxConnections: manifest.ReplicaControl.MaxSourceConcurrent,
					MaxInflightBytes: int64(manifest.ReplicaControl.SourceChunkBytes) *
						int64(manifest.ReplicaControl.MaxSourceConcurrent),
				},
			)
		}
		slices.SortFunc(targetNodes, func(a, b rafttransport.NodeID) int { return bytes.Compare(a[:], b[:]) })
		targetNodes = slices.Compact(targetNodes)
		if serviceErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(serviceErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
	}
	snapshotNodes := policy.NodesWith(serviceauthz.CapabilityMembership)
	snapshotAuthorizer, err := servicetls.NewNodeAuthorizer(snapshotNodes)
	if err == nil {
		snapshotTLS, err = servicetls.NewServer(
			profile, rafttransport.TrafficSnapshot, snapshotAuthorizer,
		)
	}
	if err != nil {
		return errors.Join(errRF3Serving, err)
	}
	var childPrepareControl shardcontrol.Handler
	if nativeListener != nil {
		childPaths, childErr := newRF3SplitChildPathRegistry(manifest.SplitControl.ChildRegistry)
		if childErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(childErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		childPreparer, childErr := newRF3ChildPreparer(
			childPaths, profile.LocalIdentity().Node,
			peerListener.Addr(), nativeListener.Addr(), controlListener.Addr(), snapshotListener.Addr(),
		)
		if childErr == nil {
			concurrency := min(manifest.SplitControl.ChildRegistry.MaxOperations, 8)
			childPrepareControl, childErr = splitcontroller.NewChildPrepareService(
				splitcontroller.ChildPrepareServiceOptions{
					Preparer: childPreparer,
					Authorize: func(identity rafttransport.PeerIdentity, request splitcontroller.ChildPreparation) bool {
						return request.ReplicaTarget().Node == profile.LocalIdentity().Node &&
							policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
					},
					ReadDeadline: deadline, WriteDeadline: deadline,
					MaxConcurrent:    concurrency,
					MaxInflightBytes: uint64(splitcontroller.MaxChildPrepareWireBytes) * uint64(concurrency),
				},
			)
		}
		if childErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(childErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
	}
	splitRuntime, splitRuntimeErr := newRF3SplitServingRuntime(rf3SplitServingOptions{
		manifest: manifest, prepared: preparedSet.groups, identities: identities, commands: commands,
		owners: peer.Owners(), registrar: peer, profile: profile, policy: policy, deadline: deadline,
	})
	if splitRuntimeErr != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(splitRuntimeErr, componentShutdownError(peerErr), servingRegistry.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, splitRuntime.Close()) }()
	metricsControl, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider: &rf3MetricsProvider{owners: peer.Owners(), groups: preparedSet.groups,
			backup: backupControl, action: actionControl, data: dataServices, split: splitRuntime.action},
		Authorize: func(identity rafttransport.PeerIdentity) bool {
			return policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	var restoreServingControl shardcontrol.Handler
	if len(restoreGateList) != 0 {
		restoreServingControl, err = shardservice.NewRestoreServingControlRegistryService(
			restoreGateList, policy, deadline, deadline,
		)
		if err != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
		}
	}
	controlMux, err := newRF3ControlMux(
		membershipControl, observationControl, metricsControl, backupControl, sourceControl, actionControl,
		splitRuntime.action, schemaControl, splitRuntime.observation.service,
		splitRuntime.admission, splitRuntime.tail, splitRuntime.terminal, childPrepareControl,
		restoreServingControl,
	)
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	snapshotMux, err := newRF3SnapshotMux(sourceData, splitRuntime.artifact)
	if err != nil {
		return errors.Join(err, servingRegistry.Close())
	}
	var server *shardservice.ReplicatedServer
	if nativeConfigured {
		baseServing := rf3NativeServingAuthority(transportRegistry, manifest, group, base)
		server, err = shardservice.NewReplicatedServer(peer.Owners(), 64<<20, rf3RequestTimeout)
		if err == nil {
			err = server.BindAuthorization(gate, nil)
		}
		if err == nil {
			err = server.BindServingAuthority(func(state raftservice.ServingState) bool {
				if !baseServing(state) {
					return false
				}
				restoreGate, restored := restoreGates[state.Identity.Group]
				return !restored || restoreGate.Allows(state)
			})
		}
		if err == nil {
			membershipPreparing := rf3NativeMembershipAuthority(transportRegistry, manifest, group, base)
			restorePreparing := rf3RestoreCatalogPreparingAuthority(
				gate, restoreOperations[group], group, base, baseServing,
			)
			err = server.BindTransitionalServingAuthority(func(state raftservice.ServingState,
				request *shardservice.ReplicatedRequest,
			) bool {
				return restorePreparing(state, request) ||
					(manifest.EnrolledTarget != nil && base.Binding.MemberID == manifest.EnrolledTarget.MemberID &&
						membershipPreparing(state, request))
			})
		}
		if err != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
		}
	}

	peerCtx, stopPeer := context.WithCancelCause(context.Background())
	controlCtx, stopControl := context.WithCancelCause(context.Background())
	snapshotCtx, stopSnapshot := context.WithCancelCause(context.Background())
	nativeCtx, stopNative := context.WithCancelCause(context.Background())
	defer stopPeer(context.Canceled)
	defer stopControl(context.Canceled)
	defer stopSnapshot(context.Canceled)
	defer stopNative(context.Canceled)
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Run(peerCtx) }()
	select {
	case <-peer.Started():
	case <-parent.Done():
		stopPeer(context.Cause(parent))
		peerErr := <-peerDone
		return finishRF3Serving(componentShutdownError(peerErr), lanes, servingRegistry)
	}
	if !peer.Running() || !peer.Owners().Running() {
		peerErr := <-peerDone
		return finishRF3Serving(
			errors.Join(errors.New("RF3 peer failed before readiness"), peerErr),
			lanes, servingRegistry,
		)
	}

	pulseDone := make(chan struct{})
	go runRF3Pulse(peerCtx, pulse, pulseDone)
	controlDone := make(chan error, 1)
	go func() {
		controlDone <- controlTLS.Serve(controlCtx, controlListener, servicetls.Limits{
			MaxConnections: 32, MaxHandshakes: 8, HandshakeDeadline: deadline,
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			_ = controlMux.Serve(ctx, connection)
		})
	}()
	snapshotDone := make(chan error, 1)
	snapshotAddress := snapshotListener.Addr().String()
	snapshotConcurrency := max(manifest.ReplicaControl.MaxSourceConcurrent,
		min(manifest.SplitControl.ChildRegistry.MaxOperations, 8))
	go func() {
		snapshotDone <- snapshotTLS.Serve(snapshotCtx, snapshotListener, servicetls.Limits{
			MaxConnections: snapshotConcurrency, MaxHandshakes: snapshotConcurrency,
			HandshakeDeadline: deadline,
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			_ = snapshotMux.Serve(ctx, connection)
		})
	}()
	var nativeDone chan error
	nativeAddress := "fenced"
	if nativeConfigured {
		nativeDone = make(chan error, 1)
		nativeAddress = nativeListener.Addr().String()
		go func() {
			nativeDone <- server.ServeAuthenticated(
				nativeCtx, nativeListener, nativeTLS, deadline, 64, 16,
			)
		}()
	}
	topology := "RF3"
	if manifest.DevelopmentOnly {
		topology = "RF1-development-only-no-HA"
	}
	fmt.Fprintf(os.Stderr,
		"vibedb-shard %s ready distribution=%q shard=%q member=%d replica-set=%d peer=%s native=%s snapshot=%s control=%s\n",
		topology,
		base.Binding.Distribution, base.Binding.Shard, base.Binding.MemberID,
		runtimePublication.ReplicaSetVersion, peerListener.Addr(), nativeAddress,
		snapshotAddress, controlListener.Addr(),
	)

	var primary error
	peerFinished, controlFinished, snapshotFinished, nativeFinished := false, false, false, false
	select {
	case <-parent.Done():
		// A requested shutdown is not an error. Component failures observed below
		// remain visible unless they are the expected cancellation result.
	case err := <-peerDone:
		primary, peerFinished = fmt.Errorf("RF3 peer stopped: %w", err), true
	case err := <-controlDone:
		primary, controlFinished = fmt.Errorf("RF3 control listener stopped: %w", err), true
	case err := <-snapshotDone:
		primary, snapshotFinished = fmt.Errorf("RF3 snapshot listener stopped: %w", err), true
	case err := <-nativeDone:
		primary, nativeFinished = fmt.Errorf("RF3 native listener stopped: %w", err), true
	}

	// Fence and join client ingress before retiring Owner/Host/runtime.
	if nativeConfigured {
		stopNative(context.Canceled)
		if !nativeFinished {
			primary = errors.Join(primary, componentShutdownError(<-nativeDone))
		}
	}
	stopSnapshot(context.Canceled)
	if !snapshotFinished {
		primary = errors.Join(primary, componentShutdownError(<-snapshotDone))
	}
	stopControl(context.Canceled)
	if !controlFinished {
		primary = errors.Join(primary, componentShutdownError(<-controlDone))
	}
	stopPeer(context.Canceled)
	if !peerFinished {
		primary = errors.Join(primary, componentShutdownError(<-peerDone))
	}
	<-pulseDone
	return finishRF3Serving(primary, lanes, servingRegistry)
}

// newRF3ControlMux is the stable composition boundary for all authenticated
// shard-control operations. Snapshot source export and mutating replica
// actions remain optional until their durable local journals are opened; when
// supplied they share the same TLS listener and connection concurrency bound.
func newRF3ControlMux(
	membership, observation, metrics, backup, source, action, split, schema, planObservation, admission, tail,
	terminal, childPrepare, restoreServing shardcontrol.Handler,
) (*shardcontrol.Mux, error) {
	routes := make([]shardcontrol.Route, 0, 14)
	routes = append(routes,
		shardcontrol.Route{
			Discriminator: shardservice.MembershipGrantRequestDiscriminator(),
			Handler:       membership,
		},
		shardcontrol.Route{
			Discriminator: replicacontrol.RequestDiscriminator(),
			Handler:       observation,
		},
		shardcontrol.Route{
			Discriminator: servicemetrics.RequestDiscriminator(),
			Handler:       metrics,
		},
	)
	if backup != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: clusterbackup.LiveRequestDiscriminator(),
			Handler:       backup,
		})
	}
	if source != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: snapshottransfer.SourceControlRequestDiscriminator(),
			Handler:       source,
		})
	}
	if action != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: replicaaction.RequestDiscriminator(),
			Handler:       action,
		})
	}
	if split != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: publicshardcontrol.RequestDiscriminator(),
			Handler:       split,
		})
	}
	if schema != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: schemainstall.RequestDiscriminator(),
			Handler:       schema,
		})
	}
	if planObservation != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitcontroller.PlanObservationRequestDiscriminator(),
			Handler:       planObservation,
		})
	}
	if admission != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitcontroller.PlanAdmissionRequestDiscriminator(),
			Handler:       admission,
		})
	}
	if tail != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitcontroller.TailStreamRequestDiscriminator(),
			Handler:       tail,
		})
	}
	if terminal != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitcontroller.TerminalRetirementRequestDiscriminator(), Handler: terminal,
		})
	}
	if childPrepare != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitcontroller.ChildPrepareRequestDiscriminator(),
			Handler:       childPrepare,
		})
	}
	if restoreServing != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: shardservice.RestoreServingRequestDiscriminator(), Handler: restoreServing,
		})
	}
	return shardcontrol.New(routes...)
}

func hasRestoredRF3PreparingMarker(sqlPath string) (bool, error) {
	_, found, err := restoredRF3PreparingOperation(sqlPath)
	return found, err
}

func validateRestoredRF3Bootstrap(wal *raftstore.Store, sqlPath string, member uint64) ([32]byte, error) {
	base, err := wal.Snapshot()
	if err != nil {
		return [32]byte{}, err
	}
	operation, ordinal, _, restored, err := kubeoperator.RestoreBootstrapOperation(base)
	if err != nil {
		return [32]byte{}, err
	}
	marker, err := readRF3BoundedFile(filepath.Join(filepath.Dir(sqlPath), "restore_preparing"), 37)
	if !restored {
		if errors.Is(err, os.ErrNotExist) {
			return [32]byte{}, nil
		}
		return [32]byte{}, errors.Join(errRF3Serving, err)
	}
	if err != nil || len(marker) != 37 || member == 0 || member > 3 ||
		[32]byte(marker[:32]) != operation || binary.BigEndian.Uint32(marker[32:36]) != ordinal ||
		uint64(marker[36])+1 != member {
		return [32]byte{}, errors.Join(errRF3Serving, err)
	}
	return operation, nil
}

func restoredRF3PreparingOperation(sqlPath string) ([32]byte, bool, error) {
	if sqlPath == "" {
		return [32]byte{}, false, errRF3Serving
	}
	path := filepath.Join(filepath.Dir(sqlPath), "restore_preparing")
	raw, err := readRF3BoundedFile(path, 37)
	if errors.Is(err, os.ErrNotExist) {
		return [32]byte{}, false, nil
	}
	if err != nil || len(raw) != 37 {
		return [32]byte{}, false, errors.Join(errRF3Serving, err)
	}
	var digest [32]byte
	copy(digest[:], raw[:32])
	if digest == ([32]byte{}) || raw[36] >= 3 {
		return [32]byte{}, false, errRF3Serving
	}
	return digest, true, nil
}

func newRF3SnapshotMux(source, artifact shardcontrol.Handler) (*shardcontrol.Mux, error) {
	routes := make([]shardcontrol.Route, 0, 2)
	if source != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: snapshottransfer.RequestDiscriminator(), Handler: source,
		})
	}
	if artifact != nil {
		routes = append(routes, shardcontrol.Route{
			Discriminator: splitartifact.RequestDiscriminator(), Handler: artifact,
		})
	}
	return shardcontrol.NewForTraffic(rafttransport.TrafficSnapshot, routes...)
}

func loadRF3RetainedIdentities(manifest rf3Manifest) (
	sqldriver.ReplicatedShardStoreIdentity,
	sqldriver.ReplicatedApplyIdentity,
	error,
) {
	var base sqldriver.ReplicatedShardStoreIdentity
	if err := loadRF3IdentityFile(manifest.SQL.IdentityPath, &base); err != nil {
		return base, sqldriver.ReplicatedApplyIdentity{}, fmt.Errorf(
			"%w: SQL identity: %v", errRF3Serving, err,
		)
	}
	var apply sqldriver.ReplicatedApplyIdentity
	if err := loadRF3IdentityFile(manifest.SQL.ApplyIdentityPath, &apply); err != nil {
		return base, apply, fmt.Errorf("%w: apply identity: %v", errRF3Serving, err)
	}
	publishedBase, publishedApply, published, err :=
		sqldriver.PublishedReplicatedSchemaActivationIdentity(manifest.SQL.Path)
	if err != nil {
		return base, apply, fmt.Errorf("%w: published schema identity: %v", errRF3Serving, err)
	}
	if published {
		if !rf3SchemaSuccessorMatchesRetained(base, apply, publishedBase, publishedApply) {
			return base, apply, fmt.Errorf("%w: published schema identity diverges from retained source", errRF3Serving)
		}
		base, apply = publishedBase, publishedApply
	}
	return base, apply, nil
}

func rf3SchemaSuccessorMatchesRetained(
	retained sqldriver.ReplicatedShardStoreIdentity,
	retainedApply sqldriver.ReplicatedApplyIdentity,
	published sqldriver.ReplicatedShardStoreIdentity,
	publishedApply sqldriver.ReplicatedApplyIdentity,
) bool {
	if retained.Equal(published) && retainedApply == publishedApply {
		return true
	}
	if retained.Binding.Authority.SchemaGeneration == ^uint64(0) ||
		published.Binding.Authority.SchemaGeneration !=
			retained.Binding.Authority.SchemaGeneration+1 {
		return false
	}
	wantBinding := retained.Binding
	wantBinding.Authority.SchemaGeneration++
	wantApply := retainedApply
	wantApply.ValidationDigest = publishedApply.ValidationDigest
	return published.Binding == wantBinding && published.LogID == retained.LogID &&
		published.UserTable == retained.UserTable && publishedApply == wantApply
}

type rf3IdentityDecoder interface{ UnmarshalJSON([]byte) error }

func loadRF3IdentityFile(path string, target rf3IdentityDecoder) error {
	data, err := readRF3BoundedFile(path, rf3IdentityFileBytes)
	if err != nil {
		return err
	}
	return target.UnmarshalJSON(data)
}

func loadRF3WALKey(id, path string) (raftstore.Key, error) {
	if id == "" || len(id) > raftstore.MaxKeyIDBytes {
		return raftstore.Key{}, fmt.Errorf("%w: invalid WAL key ID", errRF3Serving)
	}
	material, err := readRF3BoundedFile(path, 32)
	if err != nil || len(material) != 32 {
		clear(material)
		return raftstore.Key{}, fmt.Errorf(
			"%w: WAL key material must be exactly 32 bytes", errRF3Serving,
		)
	}
	key := raftstore.Key{ID: id}
	copy(key.Material[:], material)
	clear(material)
	return key, nil
}

func readRF3BoundedFile(path string, maximum int) ([]byte, error) {
	if path == "" || maximum <= 0 {
		return nil, errRF3Serving
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(maximum) {
		return nil, errors.Join(errRF3Serving, err)
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, errRF3Serving
	}
	return data, nil
}

func validateRF3Addresses(manifest rf3Manifest) error {
	if manifest.Listeners.Peer == manifest.Listeners.Native ||
		manifest.Listeners.Peer == manifest.Listeners.Snapshot ||
		manifest.Listeners.Peer == manifest.Listeners.Control ||
		manifest.Listeners.Native == manifest.Listeners.Snapshot ||
		manifest.Listeners.Native == manifest.Listeners.Control ||
		manifest.Listeners.Snapshot == manifest.Listeners.Control {
		return fmt.Errorf("%w: peer, native, snapshot, and control listeners must differ", errRF3Serving)
	}
	if err := validateRF3Address(manifest.Listeners.Peer, true); err != nil {
		return err
	}
	if err := validateRF3Address(manifest.Listeners.Native, true); err != nil {
		return err
	}
	if err := validateRF3Address(manifest.Listeners.Snapshot, true); err != nil {
		return err
	}
	if err := validateRF3Address(manifest.Listeners.Control, true); err != nil {
		return err
	}
	for _, bundle := range manifest.groupBundles() {
		for _, member := range bundle.Members[:bundle.MemberCount] {
			if err := validateRF3Address(member.PeerAddress, false); err != nil {
				return err
			}
		}
		if target := bundle.EnrolledTarget; target != nil {
			for _, address := range [...]string{target.PeerAddress, target.NativeAddress, target.SnapshotAddress, target.ControlAddress} {
				if err := validateRF3Address(address, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateRF3Address(address string, listener bool) error {
	host, port, err := net.SplitHostPort(address)
	value, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || portErr != nil || value == 0 || !listener && host == "" {
		return fmt.Errorf("%w: invalid network address %q", errRF3Serving, address)
	}
	return nil
}

func groupFromBinding(binding sqldriver.ReplicatedShardStoreBinding) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}
}

func walIdentityFromBinding(binding sqldriver.ReplicatedShardStoreBinding) raftstore.Identity {
	return raftstore.Identity{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		Distribution: binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		MemberID: binding.MemberID, StoreID: binding.StoreID,
	}
}

// rejectRF3UnappliedMembership closes the fixed-roster restart gap. Normal
// committed entries may replay after adoption, but a committed configuration
// entry would advance the state machine beyond the immutable transport roster.
func rejectRF3UnappliedMembership(wal *raftstore.Store, applied uint64) error {
	commit, err := wal.DurableCommit()
	if err != nil {
		return err
	}
	if commit < applied || commit == ^uint64(0) {
		return fmt.Errorf("%w: durable commit is inconsistent with applied state", errRF3Serving)
	}
	for next := applied + 1; next <= commit; {
		entries, err := wal.Entries(next, commit+1, 16<<20)
		if err != nil || len(entries) == 0 {
			return errors.Join(fmt.Errorf("%w: inspect committed suffix", errRF3Serving), err)
		}
		for _, entry := range entries {
			if entry.GetType() == pb.EntryConfChange || entry.GetType() == pb.EntryConfChangeV2 {
				return fmt.Errorf(
					"%w: unapplied membership entry at index %d", errRF3Serving, entry.GetIndex(),
				)
			}
		}
		next = entries[len(entries)-1].GetIndex() + 1
	}
	return nil
}

func rf3NativeServingAuthority(
	registry *rafttransport.StaticRegistry,
	manifest rf3Manifest,
	group raftmember.GroupKey,
	base sqldriver.ReplicatedShardStoreIdentity,
) func(raftservice.ServingState) bool {
	return func(state raftservice.ServingState) bool {
		if registry == nil || state.Identity.Group != group ||
			state.Identity.MemberID != base.Binding.MemberID ||
			state.Identity.StoreID != base.Binding.StoreID || !state.Command.Valid() {
			return false
		}
		version, found := registry.ReplicaSetVersion(group)
		if !found || version != state.Command.ReplicaSetVersion {
			return false
		}
		voters := 0
		for _, member := range manifest.memberRoster() {
			if role, err := registry.Role(group, member.MemberID); err == nil &&
				role == rafttransport.MemberVoter {
				voters++
			}
		}
		if target := manifest.EnrolledTarget; target != nil {
			if role, err := registry.Role(group, target.MemberID); err == nil &&
				role == rafttransport.MemberVoter {
				voters++
			}
		}
		role, err := registry.Role(group, base.Binding.MemberID)
		if err != nil || role != rafttransport.MemberVoter {
			return false
		}
		if target := manifest.EnrolledTarget; target != nil &&
			base.Binding.MemberID == target.MemberID {
			initial := base.Binding.Authority
			return voters == rf3ManifestMembers &&
				state.Command.OwnershipEpoch > initial.OwnershipEpoch &&
				state.Command.RoutingVersion > initial.RoutingVersion &&
				state.Command.RouteGeneration > initial.RouteGeneration
		}
		return true
	}
}

func rf3NativeMembershipAuthority(
	registry *rafttransport.StaticRegistry,
	manifest rf3Manifest,
	group raftmember.GroupKey,
	base sqldriver.ReplicatedShardStoreIdentity,
) func(raftservice.ServingState, *shardservice.ReplicatedRequest) bool {
	return func(state raftservice.ServingState, request *shardservice.ReplicatedRequest) bool {
		target := manifest.EnrolledTarget
		if registry == nil || target == nil || request == nil ||
			state.Identity.Group != group || state.Identity.MemberID != target.MemberID ||
			state.Identity.MemberID != base.Binding.MemberID ||
			state.Identity.StoreID != target.StoreID || state.Identity.StoreID != base.Binding.StoreID ||
			request.Operation != shardservice.ReplicatedMembership ||
			request.Capability != serviceauthz.CapabilityMembership || !state.Command.Valid() ||
			request.Fence.Group != group ||
			request.Fence.AllocationGeneration != state.Identity.AllocationGeneration ||
			request.Membership.ExpectedReplicaSetVersion != state.Command.ReplicaSetVersion {
			return false
		}
		version, found := registry.ReplicaSetVersion(group)
		if !found || version != state.Command.ReplicaSetVersion {
			return false
		}
		role, err := registry.Role(group, target.MemberID)
		if err != nil || role != rafttransport.MemberVoter {
			return false
		}
		voters := 0
		for _, member := range manifest.memberRoster() {
			if role, roleErr := registry.Role(group, member.MemberID); roleErr == nil &&
				role == rafttransport.MemberVoter {
				voters++
			}
		}
		voters++
		if voters != rf3ManifestMembers+1 {
			return false
		}
		grant, found, err := registry.CurrentTransitionGrant(group)
		membership := request.Membership
		return err == nil && found && grant.Valid() && grant.Group == group &&
			grant.TargetMember == target.MemberID && grant.TargetNode == [16]byte(target.NodeID) &&
			membership.TransitionID == grant.TransitionID &&
			membership.MetadataEpoch == grant.MetadataEpoch &&
			membership.CatalogGeneration == grant.CatalogGeneration &&
			membership.SourceMember == grant.SourceMember &&
			membership.TargetMember == grant.TargetMember
	}
}

func buildRF3Roster(
	manifest rf3Manifest,
	group raftmember.GroupKey,
	localMember uint64,
	publication raftmodel.Publication,
) ([]rafttransport.Member, []rafttransport.NodeID, rafttransport.RawPeerDialFunc, bool, error) {
	conf := publication.ConfState
	if publication.ReplicaSetVersion == 0 ||
		raftmodel.ValidateConfState(conf, publication.ReplicaSetVersion) != nil ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return nil, nil, nil, false, fmt.Errorf("%w: unsupported durable membership cut", errRF3Serving)
	}
	voters, learners := conf.GetVoters(), conf.GetLearners()
	configured := make([]rf3ManifestMember, 0, rf3ManifestMembers+1)
	configured = append(configured, manifest.memberRoster()...)
	if target := manifest.EnrolledTarget; target != nil {
		configured = append(configured, rf3ManifestMember{
			MemberID: target.MemberID, NodeID: target.NodeID, PeerAddress: target.PeerAddress,
		})
	}
	if !supportedRF3MembershipCut(manifest, voters, learners) {
		return nil, nil, nil, false, fmt.Errorf("%w: durable membership differs from enrolled roster", errRF3Serving)
	}
	members := make([]rafttransport.Member, len(configured))
	remote := make([]rafttransport.NodeID, 0, len(configured)-1)
	localFound := false
	localNativeAuthorized := false
	nodes := make([]rafttransport.NodeID, len(configured))
	addresses := make([]string, len(configured))
	for index, configured := range configured {
		role := rafttransport.MemberEnrolled
		if slices.Contains(voters, configured.MemberID) {
			role = rafttransport.MemberVoter
		} else if slices.Contains(learners, configured.MemberID) {
			role = rafttransport.MemberLearner
		}
		members[index] = rafttransport.Member{
			Group: group, ReplicaSetVersion: publication.ReplicaSetVersion,
			MemberID: configured.MemberID, Node: configured.NodeID,
			Role: role,
		}
		nodes[index], addresses[index] = configured.NodeID, configured.PeerAddress
		if configured.MemberID == localMember {
			localFound = true
			localNativeAuthorized = role == rafttransport.MemberVoter ||
				manifest.EnrolledTarget != nil &&
					configured.MemberID == manifest.EnrolledTarget.MemberID
		} else {
			remote = append(remote, configured.NodeID)
		}
	}
	if !localFound {
		return nil, nil, nil, false, fmt.Errorf("%w: retained member is absent from enrolled roster", errRF3Serving)
	}
	dialer := net.Dialer{Timeout: rf3NetworkTimeout}
	dial := func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		for index := range nodes {
			if nodes[index] == node {
				return dialer.DialContext(ctx, "tcp", addresses[index])
			}
		}
		return nil, rafttransport.ErrNodeNotFound
	}
	return members, remote, dial, localNativeAuthorized, nil
}

func supportedRF3MembershipCut(
	manifest rf3Manifest,
	voters, learners []uint64,
) bool {
	base := make([]uint64, len(manifest.memberRoster()))
	for index, member := range manifest.memberRoster() {
		base[index] = member.MemberID
	}
	if manifest.DevelopmentOnly {
		return manifest.EnrolledTarget == nil && len(learners) == 0 && slices.Equal(voters, base)
	}
	if len(base) != rf3ManifestMembers {
		return false
	}
	if target := manifest.EnrolledTarget; target != nil {
		if len(learners) == 1 && learners[0] == target.MemberID && slices.Equal(voters, base) {
			return true
		}
		if len(learners) != 0 {
			return false
		}
		all := append(slices.Clone(base), target.MemberID)
		slices.Sort(all)
		if slices.Equal(voters, base) || slices.Equal(voters, all) {
			return true
		}
		if len(voters) != rf3ManifestMembers || !slices.Contains(voters, target.MemberID) {
			return false
		}
		for _, voter := range voters {
			if voter != target.MemberID && !slices.Contains(base, voter) {
				return false
			}
		}
		return true
	}
	return len(learners) == 0 && slices.Equal(voters, base)
}

func commandFenceFromPublication(
	authority sqldriver.ReplicatedAuthorityProfile,
	identity raftmember.RuntimeIdentity,
	replicaSetVersion uint64,
) raftservice.CommandFence {
	return raftservice.CommandFence{
		ReplicaSetVersion:      replicaSetVersion,
		ActivePolicyGeneration: authority.ActivePolicyGeneration,
		ProtectionEpoch:        authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
		SchemaGeneration:       authority.SchemaGeneration,
		RelationManifestDigest: identity.RelationManifestDigest,
		RoutingVersion:         authority.RoutingVersion, RouteGeneration: authority.RouteGeneration,
	}
}

func rf3RegistryLimits() raftserve.Limits {
	return rf3RegistryLimitsForGroups(1)
}

func rf3RegistryLimitsForGroups(groups int) raftserve.Limits {
	return raftserve.Limits{
		MaxGroups: groups, MaxOutstandingIdentities: 32 * groups,
		MaxOutstandingAttempts: 64 * groups, MaxWaiters: 64 * groups, MaxAttemptsPerIdentity: 4,
		MaxRetainedCompletionBytes: int64(32*groups) * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	}
}

func rf3HostLimits() multiraft.Limits {
	return rf3HostLimitsForGroups(1)
}

func rf3HostLimitsForGroups(groups int) multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: groups, MaxQueueItems: 256, MaxQueueBytes: 128 << 20,
		MaxGroupItems: 256, MaxGroupBytes: 128 << 20,
		MaxOutboxItems: 256, MaxOutboxBytes: 128 << 20, MaxPendingTicks: 16,
	}
}

func rf3OwnerLimits() raftservice.Limits {
	return raftservice.Limits{
		MaxIngressItems: 128, MaxIngressBytes: 64 << 20,
		MaxPendingProposalItems: 64, MaxPendingProposalBytes: 64 << 20,
		MaxPendingReadItems: 64, MaxPendingReadBytes: 64 << 20,
		MaxPendingOutboundBytes: 64 << 20,
	}
}

func rf3TransportOptions(
	peers []rafttransport.NodeID,
	deadline rafttransport.DeadlineFunc,
) rafttransport.OrdinaryTransportOptions {
	return rafttransport.OrdinaryTransportOptions{
		Peers: peers,
		Queue: rafttransport.QueueLimits{
			// One maximum command rounds to a 32 MiB owned frame. Each
			// follower can retain one without making a valid proposal fatal.
			PerPeerFrames: 32, PerPeerBytes: 32 << 20,
			GlobalFrames: 64, GlobalBytes: 64 << 20,
		},
		Coalesce: rafttransport.CoalesceLimits{
			MaxFrames: 8,
			MaxBytes: rafttransport.StreamRecordHeaderBytes +
				rafttransport.MaxFrameBytes,
			RetainedBytes: rafttransport.DefaultRetainedFrameBytes,
		},
		Wait: rafttransport.WaitWithTimer, Backoff: rf3ReconnectBackoff,
		MaxReconnectDelay: time.Second, WriteDeadline: deadline,
		RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
	}
}

func rf3ReconnectBackoff(failures uint32) time.Duration {
	shift := min(failures, 10)
	return time.Duration(uint64(1)<<shift) * time.Millisecond
}

func runRF3Pulse(ctx context.Context, pulse chan<- struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(rf3TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case pulse <- struct{}{}:
			default:
			}
		}
	}
}

func componentShutdownError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func validRF3ExecutionLanes(count int) bool {
	return count > 0 && count <= multiraft.AbsoluteMaxExecutionLanes && count&(count-1) == 0
}

func finishRF3Serving(primary error, lanes *multiraft.ExecutionLanes, registry *raftserve.Registry) error {
	return errors.Join(primary, lanes.Close(), registry.Close())
}
