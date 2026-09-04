package gatewayruntime

import (
	"bytes"
	"context"
	"encoding/asn1"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
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

func runtimeParticipantForTest(t *testing.T, profile *rafttransport.PeerTLS, policy *serviceauthz.Policy) *Runtime {
	t.Helper()
	runtime, err := Open(context.Background(), Config{CatalogPath: runtimeLifecycleCatalog(t),
		DevStaticCatalog: true, DevPlaintext: true, Listener: newBlockingRuntimeListener(), Logf: func(string, ...any) {}})
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

func TestRuntimeParticipantDrainWaitsForNonControllerRead(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{11}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{12}, Capabilities: serviceauthz.CapabilityTopology},
	})
	runtimes := []*Runtime{runtimeParticipantForTest(t, profiles[0], policy), runtimeParticipantForTest(t, profiles[1], policy)}
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
	if runtimes[1].holder.DrainStatus(2).ActiveOlderOperations != 1 {
		t.Fatal("read did not retain the old catalog pin")
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
