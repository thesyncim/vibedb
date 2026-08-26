package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

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
	rf3IdentityFileBytes = 256 << 10
	rf3TickInterval      = 50 * time.Millisecond
	rf3NetworkTimeout    = 10 * time.Second
	rf3RequestTimeout    = 15 * time.Second
)

var errRF3Serving = errors.New("vibedb-shard: invalid RF3 serving configuration")

func runServeRF3(args []string) int {
	fs := flag.NewFlagSet("serve-rf3", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "canonical prepared RF3 member manifest")
	if err := fs.Parse(args); err != nil || *manifestPath == "" || fs.NArg() != 0 {
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
	if err := servePreparedRF3(ctx, manifest); err != nil {
		fmt.Fprintf(os.Stderr, "error serve RF3: %v\n", err)
		return 1
	}
	return 0
}

// servePreparedRF3 opens only previously prepared durable artifacts. It never
// creates a WAL, SQL root, apply namespace, identity, or bootstrap authority.
func servePreparedRF3(parent context.Context, manifest rf3Manifest) error {
	return servePreparedRF3WithListen(parent, manifest, net.Listen)
}

type rf3ListenFunc func(network, address string) (net.Listener, error)

func servePreparedRF3WithListen(
	parent context.Context,
	manifest rf3Manifest,
	listen rf3ListenFunc,
) (resultErr error) {
	if parent == nil {
		return errRF3Serving
	}
	if listen == nil {
		return errRF3Serving
	}
	if cause := context.Cause(parent); cause != nil {
		return componentShutdownError(cause)
	}
	if err := validateRF3Addresses(manifest); err != nil {
		return err
	}
	base, applyIdentity, err := loadRF3RetainedIdentities(manifest)
	if err != nil {
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
		policy.NodesWith(serviceauthz.CapabilityMembership),
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

	group := groupFromBinding(base.Binding)
	wantDomain := rafttransport.TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
	}
	if profile.LocalIdentity().TrustDomain != wantDomain {
		return fmt.Errorf("%w: certificate trust domain differs from retained SQL identity", errRF3Serving)
	}

	key, err := loadRF3WALKey(manifest.WAL.KeyID, manifest.WAL.KeyMaterialPath)
	if err != nil {
		return err
	}
	wal, err := raftstore.Open(
		manifest.WAL.Path, walIdentityFromBinding(base.Binding),
		base.Binding.TopologyRecoveryEpoch, key, manifest.WAL.Options,
	)
	clear(key.Material[:])
	if err != nil {
		return fmt.Errorf("open RF3 WAL: %w", err)
	}
	database, apply, err := raftmember.OpenBoundSQLWithApply(
		manifest.SQL.Path, wal, base.Binding.Authority, base, applyIdentity,
	)
	if err != nil {
		return errors.Join(fmt.Errorf("open bound RF3 SQL/apply: %w", err), wal.Close())
	}
	closePrepared := func(cause error) error {
		return errors.Join(cause, apply.Close(), database.Close(), wal.Close())
	}

	publication := apply.Published()
	if err := rejectRF3UnappliedMembership(wal, publication.Applied); err != nil {
		return closePrepared(err)
	}
	members, remoteNodes, dial, nativeAuthorized, err := buildRF3Roster(
		manifest, group, base.Binding.MemberID, publication,
	)
	if err != nil {
		return closePrepared(err)
	}
	transportRegistry, err := rafttransport.NewStaticRegistry(
		profile.LocalIdentity().Node, members,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: len(members)},
	)
	if err != nil {
		return closePrepared(fmt.Errorf("%w: transport roster: %v", errRF3Serving, err))
	}
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	membershipControl, err := shardservice.NewMembershipGrantControlService(
		transportRegistry, policy, deadline, deadline,
	)
	if err != nil {
		return closePrepared(fmt.Errorf("%w: membership grant control: %v", errRF3Serving, err))
	}
	localMember, err := transportRegistry.LocalMember(group)
	if err != nil || localMember != base.Binding.MemberID {
		return closePrepared(fmt.Errorf("%w: certificate node does not own retained member", errRF3Serving))
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
	var nativeListener net.Listener
	if nativeAuthorized {
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

	runtime, err := raftmember.AdoptRuntime(wal, database, apply)
	if err != nil {
		if runtime != nil {
			return errors.Join(err, runtime.Close())
		}
		return closePrepared(err)
	}
	runtimePublication, err := runtime.Publication()
	if err != nil || runtimePublication.ReplicaSetVersion != publication.ReplicaSetVersion ||
		!proto.Equal(runtimePublication.ConfState, publication.ConfState) {
		return errors.Join(
			fmt.Errorf("%w: publication changed during adoption", errRF3Serving),
			err, runtime.Close(),
		)
	}
	runtimeIdentity := runtime.Identity()
	command := commandFenceFromPublication(
		base.Binding.Authority, runtimeIdentity, runtimePublication.ReplicaSetVersion,
	)

	servingRegistry, err := raftserve.NewRegistry(rf3RegistryLimits())
	if err != nil {
		return errors.Join(err, runtime.Close())
	}
	host, err := servingRegistry.NewHost(rf3HostLimits())
	if err != nil {
		return errors.Join(err, runtime.Close(), servingRegistry.Close())
	}
	if err := host.Add(runtime); err != nil {
		return errors.Join(err, runtime.Close(), host.Close(), servingRegistry.Close())
	}

	pulse := make(chan struct{}, 1)
	peer, err := raftservice.NewAuthenticatedPeerRuntime(raftservice.AuthenticatedPeerOptions{
		Registry: transportRegistry, TLS: profile, Dial: dial, Listener: peerListener,
		HandshakeDeadline: deadline, MaxInboundStreams: 8,
		Owner: raftservice.Options{
			Registry: servingRegistry, Host: host,
			Members:                    []raftmember.RuntimeIdentity{runtimeIdentity},
			CommandFences:              []raftservice.CommandFence{command},
			ReadSources:                []raftservice.ReadSource{apply},
			TransactionRecoverySources: []raftservice.TransactionRecoverySource{apply},
			Pulse:                      pulse,
			Limits:                     rf3OwnerLimits(),
		},
		Transport: rf3TransportOptions(remoteNodes, deadline),
		Receiver: rafttransport.OrdinaryReceiverOptions{
			ReadDeadline:       deadline,
			RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
		},
	})
	if err != nil {
		return errors.Join(err, host.Close(), servingRegistry.Close())
	}
	observationControl, err := replicacontrol.NewService(replicacontrol.ServiceOptions{
		Observer: peer.Owner(),
		Authorize: func(identity rafttransport.PeerIdentity, request replicacontrol.Request) bool {
			return request.Group == group &&
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
		Journal: actionJournal, Owner: peer.Owner(),
		Authorize: func(identity rafttransport.PeerIdentity, request replicaaction.Request) bool {
			return request.Fence.Group == group &&
				request.Fence.MemberID == runtimeIdentity.MemberID &&
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
	var sourceControl shardcontrol.Handler
	if target := manifest.EnrolledTarget; target != nil &&
		runtimeIdentity.MemberID != target.MemberID {
		sourceJournal, openErr := snapshottransfer.OpenSourceFileJournal(
			manifest.ReplicaControl.SourceJournalPath,
			manifest.ReplicaControl.MaxSourceRecords,
		)
		if openErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(openErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		defer func() { resultErr = errors.Join(resultErr, sourceJournal.Close()) }()
		provider, providerErr := snapshottransfer.OpenRetainedSourceExportProvider(
			snapshottransfer.RetainedSourceExportOptions{
				DataRoot:       manifest.ReplicaControl.SourceDataRoot,
				RepositoryPath: manifest.ReplicaControl.SourceRepositoryPath,
				Limits: snapshottransfer.Limits{
					MaxArtifacts:     manifest.ReplicaControl.MaxSourceArtifacts,
					MaxArtifactBytes: manifest.ReplicaControl.MaxSourceArtifactBytes,
					MaxDiskBytes:     manifest.ReplicaControl.MaxSourceDiskBytes,
				},
				ChunkBytes:      manifest.ReplicaControl.SourceChunkBytes,
				MaxConcurrent:   manifest.ReplicaControl.MaxSourceConcurrent,
				RuntimeIdentity: runtimeIdentity,
				SourceNode:      profile.LocalIdentity().Node,
				TargetMember:    target.MemberID, TargetStore: target.StoreID,
				TargetIncarnation: target.NodeIncarnation, Cut: apply,
			},
		)
		if providerErr != nil {
			retireCtx, retire := context.WithCancelCause(context.Background())
			retire(context.Canceled)
			peerErr := peer.Run(retireCtx)
			return errors.Join(providerErr, componentShutdownError(peerErr), servingRegistry.Close())
		}
		defer func() { resultErr = errors.Join(resultErr, provider.Close()) }()
		sourceService, serviceErr := snapshottransfer.NewSourceControlService(
			snapshottransfer.SourceControlOptions{
				Journal:  sourceJournal,
				Exporter: snapshottransfer.PinnedSourceControlExporter{Provider: provider},
				Authorize: func(identity rafttransport.PeerIdentity, request snapshottransfer.SourceControlRequest) bool {
					return request.Group == group && request.SourceMember == runtimeIdentity.MemberID &&
						request.SourceNode == profile.LocalIdentity().Node &&
						request.TargetMember == target.MemberID && request.TargetStore == target.StoreID &&
						request.TargetIncarnation == target.NodeIncarnation &&
						policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
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
		sourceControl = sourceService
	}
	controlMux, err := newRF3ControlMux(membershipControl, observationControl, sourceControl, actionControl)
	if err != nil {
		retireCtx, retire := context.WithCancelCause(context.Background())
		retire(context.Canceled)
		peerErr := peer.Run(retireCtx)
		return errors.Join(err, componentShutdownError(peerErr), servingRegistry.Close())
	}
	var server *shardservice.ReplicatedServer
	if nativeAuthorized {
		server, err = shardservice.NewReplicatedServer(peer.Owner(), 64<<20, rf3RequestTimeout)
		if err == nil {
			err = server.BindAuthorization(gate, nil)
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
	nativeCtx, stopNative := context.WithCancelCause(context.Background())
	defer stopPeer(context.Canceled)
	defer stopControl(context.Canceled)
	defer stopNative(context.Canceled)
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Run(peerCtx) }()
	select {
	case <-peer.Started():
	case <-parent.Done():
		stopPeer(context.Cause(parent))
		peerErr := <-peerDone
		return finishRF3Serving(componentShutdownError(peerErr), host, servingRegistry)
	}
	if !peer.Running() || !peer.Owner().Running() {
		peerErr := <-peerDone
		return finishRF3Serving(
			errors.Join(errors.New("RF3 peer failed before readiness"), peerErr),
			host, servingRegistry,
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
	var nativeDone chan error
	nativeAddress := "fenced"
	if nativeAuthorized {
		nativeDone = make(chan error, 1)
		nativeAddress = nativeListener.Addr().String()
		go func() {
			nativeDone <- server.ServeAuthenticated(
				nativeCtx, nativeListener, nativeTLS, deadline, 64, 16,
			)
		}()
	}
	fmt.Fprintf(os.Stderr,
		"vibedb-shard RF3 ready distribution=%q shard=%q member=%d replica-set=%d peer=%s native=%s control=%s\n",
		base.Binding.Distribution, base.Binding.Shard, base.Binding.MemberID,
		runtimePublication.ReplicaSetVersion, peerListener.Addr(), nativeAddress, controlListener.Addr(),
	)

	var primary error
	peerFinished, controlFinished, nativeFinished := false, false, false
	select {
	case <-parent.Done():
		// A requested shutdown is not an error. Component failures observed below
		// remain visible unless they are the expected cancellation result.
	case err := <-peerDone:
		primary, peerFinished = fmt.Errorf("RF3 peer stopped: %w", err), true
	case err := <-controlDone:
		primary, controlFinished = fmt.Errorf("RF3 control listener stopped: %w", err), true
	case err := <-nativeDone:
		primary, nativeFinished = fmt.Errorf("RF3 native listener stopped: %w", err), true
	}

	// Fence and join client ingress before retiring Owner/Host/runtime.
	if nativeAuthorized {
		stopNative(context.Canceled)
		if !nativeFinished {
			primary = errors.Join(primary, componentShutdownError(<-nativeDone))
		}
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
	return finishRF3Serving(primary, host, servingRegistry)
}

// newRF3ControlMux is the stable composition boundary for all authenticated
// shard-control operations. Snapshot source export and mutating replica
// actions remain optional until their durable local journals are opened; when
// supplied they share the same TLS listener and connection concurrency bound.
func newRF3ControlMux(
	membership, observation, source, action shardcontrol.Handler,
) (*shardcontrol.Mux, error) {
	routes := make([]shardcontrol.Route, 0, 4)
	routes = append(routes,
		shardcontrol.Route{
			Discriminator: shardservice.MembershipGrantRequestDiscriminator(),
			Handler:       membership,
		},
		shardcontrol.Route{
			Discriminator: replicacontrol.RequestDiscriminator(),
			Handler:       observation,
		},
	)
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
	return shardcontrol.New(routes...)
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
	return base, apply, nil
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
		manifest.Listeners.Peer == manifest.Listeners.Control ||
		manifest.Listeners.Native == manifest.Listeners.Control {
		return fmt.Errorf("%w: peer, native, and control listeners must differ", errRF3Serving)
	}
	if err := validateRF3Address(manifest.Listeners.Peer, true); err != nil {
		return err
	}
	if err := validateRF3Address(manifest.Listeners.Native, true); err != nil {
		return err
	}
	if err := validateRF3Address(manifest.Listeners.Control, true); err != nil {
		return err
	}
	for _, member := range manifest.Members {
		if err := validateRF3Address(member.PeerAddress, false); err != nil {
			return err
		}
	}
	if target := manifest.EnrolledTarget; target != nil {
		for _, address := range [...]string{
			target.PeerAddress, target.NativeAddress,
			target.SnapshotAddress, target.ControlAddress,
		} {
			if err := validateRF3Address(address, false); err != nil {
				return err
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
	configured = append(configured, manifest.Members[:]...)
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
			localNativeAuthorized = role == rafttransport.MemberVoter &&
				(manifest.EnrolledTarget == nil ||
					configured.MemberID != manifest.EnrolledTarget.MemberID ||
					len(voters) == rf3ManifestMembers)
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
	base := []uint64{
		manifest.Members[0].MemberID,
		manifest.Members[1].MemberID,
		manifest.Members[2].MemberID,
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
	return raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 32,
		MaxOutstandingAttempts: 64, MaxWaiters: 64, MaxAttemptsPerIdentity: 4,
		MaxRetainedCompletionBytes: 32 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	}
}

func rf3HostLimits() multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 256, MaxQueueBytes: 128 << 20,
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

func finishRF3Serving(primary error, host *multiraft.Host, registry *raftserve.Registry) error {
	return errors.Join(primary, host.Close(), registry.Close())
}
