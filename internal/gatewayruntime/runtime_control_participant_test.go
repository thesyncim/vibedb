package gatewayruntime

import (
	"bytes"
	"context"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

func runtimeControlTLSFixture(t *testing.T, entries []serviceauthz.Entry) ([]*rafttransport.PeerTLS, *serviceauthz.Policy) {
	t.Helper()
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	nodes := make([]rafttransport.NodeID, len(entries))
	for index, entry := range entries {
		nodes[index] = entry.Node
	}
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), oid, domain, nodes)
	if err != nil {
		t.Fatal(err)
	}
	profiles := make([]*rafttransport.PeerTLS, len(nodes))
	for index := range profiles {
		profiles[index], err = servicetls.LoadProfile(credentials[index].Certificate, credentials[index].Key, roots, oid.String(), time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	policy, err := serviceauthz.NewPolicy(1, entries)
	if err != nil {
		t.Fatal(err)
	}
	return profiles, policy
}

func runtimeParticipantForTest(t *testing.T, profile *rafttransport.PeerTLS, policy *serviceauthz.Policy,
	shardDials ...gateway.DialFunc,
) *Runtime {
	t.Helper()
	if len(shardDials) > 1 {
		t.Fatal("multiple shard dials")
	}
	var shardDial gateway.DialFunc
	if len(shardDials) == 1 {
		shardDial = shardDials[0]
	}
	runtime, err := Open(context.Background(), Config{CatalogPath: runtimeLifecycleCatalog(t),
		DevStaticCatalog: true, DevPlaintext: true, Listener: newBlockingRuntimeListener(),
		ShardDial: shardDial, Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	// Replace catalog recovery with a prepared holder in this service-lifetime
	// test. Drain traffic, policy, TLS handshakes, public Serve, and Close are real.
	runtime.config.ControlParticipantOnly = true
	runtime.config.DevStaticCatalog, runtime.config.DevPlaintext = false, false
	runtime.config.TLSProfile, runtime.config.Authorization = profile, policy
	runtime.config.InternalAuthority = serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: policy.Generation()}
	return runtime
}

type participantControlDirectoryFixture struct {
	gateway.DirectoryReader
	cut gateway.NodeDirectoryCut
}

func (fixture participantControlDirectoryFixture) ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error) {
	return fixture.cut, nil
}

func (fixture participantControlDirectoryFixture) CatalogServiceFences(context.Context) ([]serviceauthz.ServiceFence, uint64, error) {
	return nil, fixture.cut.CatalogGeneration, nil
}

func (fixture participantControlDirectoryFixture) ReadCompleteServiceDirectoryCut(context.Context) (serviceDirectoryCompleteCut, error) {
	return serviceDirectoryCompleteCut{
		Revision:          fixture.cut.Revision,
		CatalogGeneration: fixture.cut.CatalogGeneration,
		ScopesGeneration:  fixture.cut.CatalogGeneration,
	}, nil
}

// TestOpenReplicaControlParticipantInitializesGatewayControlOpener exercises
// the real participant-only startup branch. The opener must be constructed
// from the authenticated manifest before that branch returns; injecting one
// into Runtime would miss the wiring regression that caused remote scans to
// fail during decommission.
func TestOpenReplicaControlParticipantInitializesGatewayControlOpener(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{11}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{21}, Capabilities: serviceauthz.AllCapabilities},
	})
	snapshot := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	local := profiles[0].LocalIdentity().Node
	physical := profiles[1].LocalIdentity().Node
	controlAddress := "127.0.0.1:0"
	record := gateway.NodeRecord{
		NodeID: physical, Incarnation: 1,
		ServiceKeyDigest: replication.Digest(profiles[1].LocalServiceKeyDigest()),
		DataEndpoint:     "physical-data", NativeEndpoint: "physical-native", ControlEndpoint: "physical-control",
		GatewayEndpoint: "physical-gateway", DataAddress: "127.0.0.1:7401", NativeAddress: "127.0.0.1:7402",
		ControlAddress: "127.0.0.1:7403", GatewayAddress: controlAddress, FailureDomain: "worker",
		Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway, Lifecycle: gateway.NodeActive,
		Revision: 1, CatalogGeneration: snapshot.Generation(),
		Gateway: gateway.GatewayIdentity{NodeID: local, Incarnation: 1,
			ServiceKeyDigest: replication.Digest(profiles[0].LocalServiceKeyDigest()), ServiceID: [16]byte{1},
			SessionID: [16]byte{2}, SessionRevision: 1, ParticipantDigest: replication.Digest{3}},
	}
	if !record.Valid() {
		t.Fatal("participant directory fixture is invalid")
	}
	directory := participantControlDirectoryFixture{cut: gateway.NodeDirectoryCut{
		Revision: 1, Digest: replication.Digest{4}, CatalogGeneration: snapshot.Generation(), Nodes: []gateway.NodeRecord{record},
	}}
	manifest := persistedGatewayReplicaControlManifest{
		Generation:   1,
		LocalGateway: persistedGatewayControlEndpoint{Node: fmt.Sprintf("%x", local), Incarnation: 1, ControlAddress: controlAddress},
		TLS: persistedGatewayReplicaTLS{Certificate: "/tls/cert", Key: "/tls/key", Roots: "/tls/roots",
			IdentityOID: "1.2.3.4", AuthorizationPolicy: "/tls/policy"},
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 8, MaxHandshakes: 4, MaxConcurrentDrains: 2,
			ControllerInterval: 100, ReadTimeout: 1000, WriteTimeout: 1000},
		GatewayEndpoints: []persistedGatewayControlEndpoint{{Node: fmt.Sprintf("%x", local), Incarnation: 1, ControlAddress: controlAddress}},
	}
	for index, replica := range snapshot.ReplicatedShardDescriptors()[0].Replicas {
		address, err := snapshot.Address(replica.ControlEndpoint)
		if err != nil {
			t.Fatal(err)
		}
		manifest.ShardEndpoints = append(manifest.ShardEndpoints, persistedGatewayShardControlEndpoint{
			Node: fmt.Sprintf("%x", replica.Node), ControlAddress: address,
			SplitSnapshotAddress: fmt.Sprintf("127.0.0.1:%d", 7501+index),
		})
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "replica-control.vibejson")
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		config: Config{ControlParticipantOnly: true, ReplicaControlManifestPath: manifestPath,
			TLSProfile: profiles[0], Authorization: policy, InternalAuthority: serviceauthz.Authority{Node: local, Generation: policy.Generation()},
			TLSCertificate: "/tls/cert", TLSKey: "/tls/key", TLSRoots: "/tls/roots", TLSIdentityOID: "1.2.3.4",
			AuthorizationPolicy: "/tls/policy", TLSHandshakeTimeout: time.Second, ControlDirectory: directory,
			Transport: new(sourceTopologyTestNative)},
		ctx: ctx, cancel: cancel, holder: gateway.NewCatalogHolder(snapshot),
		authority: &gateway.ReplicatedCatalogAuthority{}, frontend: newFrontendAdmission(FrontendDrainIdentity{}, false, false),
		ready: make(chan struct{}), serveDone: make(chan struct{}), drainDone: make(chan struct{}),
	}
	defer runtime.Close()
	if err := runtime.openReplicaControl(); err != nil {
		t.Fatal(err)
	}
	if runtime.clusterControlOpener == nil || runtime.clusterControlOpener.DirectoryRevision() != 1 {
		t.Fatal("participant startup did not retain authenticated gateway control opener")
	}
	if runtime.drainCoordinator == nil {
		t.Fatal("participant startup did not retain catalog drain coordinator")
	}
	if runtime.replicaControllersDone != nil || runtime.splitControllerDone != nil || runtime.hotShardDone != nil {
		t.Fatal("participant startup started an autonomous controller")
	}
}

func TestRuntimeParticipantDrainWaitsForNonControllerRead(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{11}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{12}, Capabilities: serviceauthz.CapabilityTopology},
	})
	startupRecoveryEntered, startupRecoveryRelease := make(chan struct{}), make(chan struct{})
	var enterRecovery, releaseRecovery sync.Once
	unblockStartupRecovery := func() { releaseRecovery.Do(func() { close(startupRecoveryRelease) }) }
	t.Cleanup(unblockStartupRecovery)
	startupRecoveryDial := func(ctx context.Context, _ string) (net.Conn, error) {
		enterRecovery.Do(func() { close(startupRecoveryEntered) })
		select {
		case <-startupRecoveryRelease:
			return nil, errors.New("startup recovery released")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	runtimes := []*Runtime{
		runtimeParticipantForTest(t, profiles[0], policy),
		runtimeParticipantForTest(t, profiles[1], policy, startupRecoveryDial),
	}
	old := runtimes[1].holder.Current()
	raw, err := gateway.AppendSnapshotDocument(nil, old)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte(`"generation":1`), []byte(`"generation":2`), 1)
	next, err := gateway.OpenSnapshotDocument(raw)
	if err != nil || next.Generation() != 2 {
		t.Fatalf("next snapshot: %v", err)
	}
	roster := make([]gatewayControlEndpoint, len(runtimes))
	// Build the complete identity roster before opening either service.
	for index := range roster {
		roster[index] = gatewayControlEndpoint{Member: gateway.ClusterCatalogDrainMember{Node: profiles[index].LocalIdentity().Node, Incarnation: uint64(index + 1)}, Address: "127.0.0.1:0"}
	}
	served := make([]chan error, len(runtimes))
	for index, runtime := range runtimes {
		if err := runtime.openCatalogDrainService(gatewayReplicaControlManifest{Local: roster[index], Gateways: roster,
			Bounds: persistedGatewayReplicaBounds{MaxConnections: 8, MaxHandshakes: 4, MaxConcurrentDrains: 2, ReadTimeout: 5000, WriteTimeout: 5000}}, testReplicaHealthCatalog{next}); err != nil {
			t.Fatal(err)
		}
		roster[index].Address = runtime.controlListener.Addr().String()
		served[index] = make(chan error, 1)
		go func() { served[index] <- runtime.Serve(context.Background()) }()
		awaitRuntimeSignal(t, runtime.Ready(), "participant control readiness")
		if runtime.replicaControllersDone != nil || runtime.splitControllerDone != nil || runtime.hotShardDone != nil ||
			runtime.metricsDone != nil || runtime.schemaDDL != nil || runtime.backupOperator != nil {
			t.Fatal("participant started an autonomous controller")
		}
	}
	// Ready closes before the public runtime starts its immediate transaction
	// recovery pass. Let that pass acquire and release its catalog lease before
	// attributing the exact generation-1 lease below to the blocked read.
	awaitRuntimeSignal(t, startupRecoveryEntered, "startup recovery catalog pin")
	unblockStartupRecovery()
	waitForCatalogLeaseQuiescence(t, runtimes[1].holder, 2)
	blocked := &runtimePinnedRead{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(blocked.release) }) }
	t.Cleanup(unblock)
	executor := gateway.NewExecutor(blocked, runtimes[1].holder, gateway.Options{})
	readDone := make(chan error, 1)
	go func() {
		_, err := executor.Query(t.Context(), gateway.Query{SQL: "SELECT tenant_id FROM messages", Class: gateway.ClassBatch})
		readDone <- err
	}()
	select {
	case <-blocked.entered:
	case err := <-readDone:
		t.Fatalf("read failed before pinning: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("read did not reach pinned transport")
	}
	for _, runtime := range runtimes {
		if !runtime.holder.Publish(next) {
			t.Fatal("publish new generation")
		}
	}
	if status := runtimes[1].holder.DrainStatus(2); status.CurrentGeneration != 2 ||
		status.OldestActiveGeneration != 1 || status.ActiveOlderOperations != 1 {
		t.Fatalf("blocked read catalog pin = %+v, want current=2 oldest=1 active=1", status)
	}
	digest, err := gateway.CatalogSnapshotDigest(next)
	if err != nil {
		t.Fatal(err)
	}
	deadline := servicetls.FixedDeadline(5 * time.Second)
	coordinator, err := newGatewayClusterDrainCertifier(profiles[0].LocalIdentity().TrustDomain, profiles[0], deadline, deadline, deadline,
		func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, roster, 2)
	if err != nil {
		t.Fatal(err)
	}
	request := gateway.ClusterCatalogDrainRequest{Operation: [32]byte{4}, Step: [32]byte{5}, Generation: 2, CatalogDigest: digest}
	drained := make(chan error, 1)
	go func() {
		certificate, err := coordinator.CertifyClusterCatalogDrain(t.Context(), request)
		if err == nil && !certificate.ValidFor(request) {
			err = errors.New("invalid drain certificate")
		}
		drained <- err
	}()
	assertRuntimeWaiting(t, drained, "noncontroller read pin")
	unblock()
	if err := awaitRuntimeError(t, drained, "complete authenticated roster drain"); err != nil {
		t.Fatal(err)
	}
	_ = awaitRuntimeError(t, readDone, "pinned read")
	for index, runtime := range runtimes {
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
		if err := awaitRuntimeError(t, served[index], "participant Serve join"); err != nil {
			t.Fatal(err)
		}
		if runtime.controlTLS.Stats().Active != 0 {
			t.Fatal("participant retained authenticated control stream")
		}
	}
}

func waitForCatalogLeaseQuiescence(t *testing.T, holder *gateway.CatalogHolder, generation uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := holder.DrainStatus(generation)
		if status.ActiveOlderOperations == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("catalog generation %d retained startup leases: %+v: %v", generation, status, ctx.Err())
		case <-ticker.C:
		}
	}
}

type runtimePinnedRead struct {
	entered, release chan struct{}
	once             sync.Once
}

func (read *runtimePinnedRead) Do(ctx context.Context, _ string, _ *shardservice.ShardRequest) (*shardservice.ShardResponse, error) {
	read.once.Do(func() { close(read.entered) })
	select {
	case <-read.release:
		return nil, errors.New("read released")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (read *runtimePinnedRead) DoBatches(ctx context.Context, address string, request *shardservice.ShardRequest, _ func(*shardservice.ShardResponse) error) error {
	_, err := read.Do(ctx, address, request)
	return err
}

func TestRuntimeParticipantServicesJoinAfterStartupFailure(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{31}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{32}, Capabilities: serviceauthz.AllCapabilities},
	})
	runtime := runtimeParticipantForTest(t, profiles[1], policy)
	runtime.clientTLS, _ = gateway.NewAuthorizedClientTLS(profiles[1], policy)
	roster := []gatewayControlEndpoint{
		{Member: gateway.ClusterCatalogDrainMember{Node: profiles[0].LocalIdentity().Node, Incarnation: 1}, Address: "127.0.0.1:1"},
		{Member: gateway.ClusterCatalogDrainMember{Node: profiles[1].LocalIdentity().Node, Incarnation: 1}, Address: "127.0.0.1:0"},
	}
	if err := runtime.openCatalogDrainService(gatewayReplicaControlManifest{Local: roster[1], Gateways: roster,
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 4, MaxHandshakes: 2, ReadTimeout: 1000, WriteTimeout: 1000}},
		testReplicaHealthCatalog{runtime.holder.Current()}); err != nil {
		t.Fatal(err)
	}
	address := runtime.controlListener.Addr().String()
	runtime.config.PGListenAddress = "127.0.0.1:0"
	runtime.config.DDLOwnerAddress, runtime.config.DDLOwnerNode = "127.0.0.1:1", profiles[0].LocalIdentity().Node
	if err := runtime.openDDL(); err != nil {
		t.Fatal(err)
	}
	// Missing durable PG writers fails after the authenticated control service
	// started. Serve must cancel and join that service without advertising ready.
	if err := runtime.Serve(t.Context()); err == nil {
		t.Fatal("startup failure was hidden")
	}
	select {
	case <-runtime.Ready():
		t.Fatal("failed participant advertised readiness")
	default:
	}
	if runtime.controlDone == nil || runtime.controlTLS.Stats().Active != 0 {
		t.Fatal("control service did not start and join")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ddlForwardTLS.Dial(t.Context(), runtime.config.DDLOwnerAddress); !errors.Is(err, servicetls.ErrUnauthorized) {
		t.Fatalf("forwarding client retained after failure: %v", err)
	}
	reopened, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed startup retained control listener: %v", err)
	}
	_ = reopened.Close()
}
