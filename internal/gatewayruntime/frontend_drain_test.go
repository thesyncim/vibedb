package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type frontendDrainTestListener struct {
	accepted chan net.Conn
	closed   chan struct{}
	once     sync.Once
}

func newFrontendDrainTestListener() *frontendDrainTestListener {
	return &frontendDrainTestListener{accepted: make(chan net.Conn, 1), closed: make(chan struct{})}
}

func (listener *frontendDrainTestListener) Accept() (net.Conn, error) {
	select {
	case conn := <-listener.accepted:
		return conn, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *frontendDrainTestListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (listener *frontendDrainTestListener) Addr() net.Addr { return frontendDrainTestAddr{} }

type frontendDrainTestAddr struct{}

func (frontendDrainTestAddr) Network() string { return "frontend-drain-test" }
func (frontendDrainTestAddr) String() string  { return "frontend-drain-test" }

func testFrontendDrainIdentity() FrontendDrainIdentity {
	var identity FrontendDrainIdentity
	identity.NodeID[0] = 1
	identity.Incarnation = 2
	identity.GatewayNodeID[0] = 9
	identity.GatewayIncarnation = 3
	identity.GatewayServiceKeyDigest[0] = 8
	identity.SessionID[0] = 3
	identity.SessionRevision = 4
	identity.NodeRevision = 5
	identity.CatalogGeneration = 6
	identity.DirectoryRevision = 7
	return identity
}

func TestFrontendDrainKeepsAcceptedNativeConnection(t *testing.T) {
	identity := testFrontendDrainIdentity()
	frontend := newFrontendAdmission(identity, false, false)
	listener := newFrontendDrainTestListener()
	wrapped := &frontendAdmissionListener{Listener: listener, frontend: frontend}

	client, peer := net.Pipe()
	listener.accepted <- peer
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := wrapped.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	var conn net.Conn
	select {
	case err := <-acceptErr:
		t.Fatalf("accepted connection rejected before drain: %v", err)
	case conn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept the native connection")
	}
	defer client.Close()

	runtime := &Runtime{listener: listener, frontend: frontend}
	ack := runtime.BeginFrontendDrain()
	if !ack.AdmissionDrained || ack.ActiveNativeConnections != 1 || ack.SafeToStop {
		t.Fatalf("drain acknowledgement = %+v", ack)
	}
	if !frontend.isDraining() {
		t.Fatal("frontend did not retain the admission-drained state")
	}

	// Closing the public listener does not close an already accepted stream.
	if err := conn.Close(); err != nil {
		t.Fatalf("close accepted connection: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		ack = runtime.FrontendDrainStatus()
		if ack.ActiveNativeConnections == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("accepted connection remained counted: %+v", ack)
		}
		time.Sleep(time.Millisecond)
	}
	if !ack.SafeToStop {
		t.Fatalf("zero-count, identity-bound drain was not safe: %+v", ack)
	}

	afterDrain := make(chan error, 1)
	go func() {
		_, err := wrapped.Accept()
		afterDrain <- err
	}()
	select {
	case err := <-afterDrain:
		if !errors.Is(err, errFrontendAdmissionDrained) {
			t.Fatalf("Accept after drain = %v, want frontend drain sentinel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed listener did not acknowledge admission drain")
	}
}

func TestFrontendAdmissionDrainIsIdempotent(t *testing.T) {
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	listener := newFrontendDrainTestListener()
	runtime := &Runtime{listener: listener, frontend: frontend}
	first := runtime.BeginFrontendDrain()
	second := runtime.BeginFrontendDrain()
	if first.Revision == 0 || second.Revision != first.Revision {
		t.Fatalf("drain revision changed across retry: first=%+v second=%+v", first, second)
	}
	if !second.AdmissionDrained || !second.Identity.Valid() {
		t.Fatalf("idempotent drain lost its fence: %+v", second)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFrontendDrainDirectoryBindsDistinctGatewayIdentity(t *testing.T) {
	physical := rafttransport.NodeID{1}
	gatewayNode := rafttransport.NodeID{2}
	record := gateway.NodeRecord{
		NodeID: physical, Incarnation: 1, ServiceKeyDigest: replication.Digest{1},
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", GatewayAddress: "localhost:4",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{NodeID: gatewayNode, Incarnation: 1,
			ServiceKeyDigest: replication.Digest{2}, ServiceID: [16]byte{3}, SessionID: [16]byte{4},
			SessionRevision: 1, ParticipantDigest: replication.Digest{5}},
	}
	if !record.Valid() {
		t.Fatal("distinct physical and gateway identities produced an invalid node record")
	}
	directory, err := gateway.NewReplicatedControlDirectory(gateway.ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{record},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		config:           Config{InternalAuthority: serviceauthz.Authority{Node: gatewayNode, Generation: 1}},
		controlDirectory: directory, frontend: newFrontendAdmission(FrontendDrainIdentity{}, false, false),
	}
	if err := runtime.restoreFrontendDrainFromDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}
	ack := runtime.FrontendDrainStatus()
	if ack.Identity.NodeID != physical || ack.Identity.SessionID != record.Gateway.SessionID ||
		ack.Identity.NodeRevision != record.Revision || ack.Identity.DirectoryRevision != 1 {
		t.Fatalf("frontend identity was not bound to the physical record: %+v", ack.Identity)
	}
	if runtime.frontend.isDraining() {
		t.Fatal("active directory record unexpectedly drained frontend")
	}

	record.Lifecycle = gateway.NodeDraining
	record.Revision++
	if !runtime.syncFrontendDrainFromDirectory([]gateway.NodeRecord{record}, 2) {
		t.Fatal("draining directory record was not applied")
	}
	ack = runtime.FrontendDrainStatus()
	if !runtime.frontend.isDraining() || !ack.AdmissionDrained || ack.Identity.NodeID != physical {
		t.Fatalf("durable draining lifecycle did not close frontend admission: %+v", ack)
	}
}

type frontendDrainDirectoryReader struct {
	gateway.DirectoryReader
	cut     gateway.NodeDirectoryCut
	records []gateway.FrontendDrainRecord
}

func (reader frontendDrainDirectoryReader) ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error) {
	return reader.cut, nil
}

func (reader frontendDrainDirectoryReader) CatalogServiceFences(context.Context) ([]serviceauthz.ServiceFence, uint64, error) {
	return nil, reader.cut.CatalogGeneration, nil
}

func (reader frontendDrainDirectoryReader) ReadFrontendDrainRecord(context.Context, [32]byte) (gateway.FrontendDrainRecord, error) {
	return gateway.FrontendDrainRecord{}, gateway.ErrReplicatedCatalogMissing
}

func (reader frontendDrainDirectoryReader) ReadFrontendDrainRecordCut(context.Context) (uint64, []gateway.FrontendDrainRecord, error) {
	return 1, append([]gateway.FrontendDrainRecord(nil), reader.records...), nil
}

func TestFrontendDrainPreflightRejectsInvalidAndOversizedProofBeforeAdmission(t *testing.T) {
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	runtime := &Runtime{frontend: frontend}
	if _, _, _, _, _, err := runtime.preflightFrontendDrainProof(context.Background(), gateway.NodeRecord{}); err == nil {
		t.Fatal("invalid preflight unexpectedly succeeded")
	}
	if frontend.isDraining() {
		t.Fatal("invalid preflight closed Active frontend admission")
	}
	if _, admitted := frontend.admitNative(); !admitted {
		t.Fatal("invalid preflight rejected a new native admission")
	}
	if frontendDrainScopesFitRecord(make([]serviceauthz.FrontendContinuationScopeRecord, maxFrontendContinuationScopes+1)) {
		t.Fatal("oversized continuation scope cut was accepted")
	}
	if frontend.isDraining() {
		t.Fatal("oversized proof validation closed Active frontend admission")
	}
}

func TestFrontendDrainRejectsOversizedProjectedSourceCutBeforeAdmission(t *testing.T) {
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	group := raftmember.GroupKey{ClusterID: [16]byte{3}, ClusterIncarnation: [16]byte{4},
		ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6}}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead,
		Group: group,
	}
	tokens := make([]serviceauthz.FrontendConnToken, maxFrontendContinuationTokens)
	protocols := make([]serviceauthz.FrontendContinuationScope, len(tokens))
	for index := range tokens {
		ordinal := index + 1
		tokens[index][30], tokens[index][31] = byte(ordinal>>8), byte(ordinal)
		protocols[index] = serviceauthz.FrontendScopeNative
	}
	const grantCount = 24
	bindings := make([]serviceauthz.ServiceBinding, 0, grantCount)
	grants := make([]serviceauthz.CommittedFrontendContinuationGrant, 0, grantCount)
	for index := 0; index < grantCount; index++ {
		physical := rafttransport.NodeID{1, byte(index + 1)}
		principal := rafttransport.NodeID{2, byte(index + 1)}
		peerKey := [32]byte{3, byte(index + 1)}
		session := [16]byte{4, byte(index + 1)}
		drainID := [32]byte{5, byte(index + 1)}
		grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
			TrustDomain: trust, PhysicalNode: physical, PhysicalIncarnation: 1,
			PeerKeyDigest: peerKey, GatewayServiceID: principal, GatewaySessionID: session,
			GatewaySessionRevision: 1, DrainID: drainID, AdmissionEpoch: 1,
			AcceptedConnectionTokens: tokens, AcceptedConnectionProtocols: protocols,
			AdmissionClosedProofDigest: [32]byte{6, byte(index + 1)}, Revision: 1,
			State: serviceauthz.ContinuationGrantPrepared,
		})
		if err != nil {
			t.Fatalf("grant %d: %v", index, err)
		}
		grants = append(grants, grant)
		bindings = append(bindings, serviceauthz.ServiceBinding{
			Principal: principal, PhysicalNode: physical, PhysicalIncarnation: 1,
			KeyDigest: peerKey, Roles: serviceauthz.ServiceRoleGateway,
			Lifecycle: serviceauthz.ServiceActive, GatewayIncarnation: 1,
			SessionID: session, SessionRevision: 1, ParticipantDigest: [32]byte{7, byte(index + 1)},
		})
	}
	slices.SortFunc(grants, func(left, right serviceauthz.CommittedFrontendContinuationGrant) int {
		return bytes.Compare(left.GrantDigest[:], right.GrantDigest[:])
	})
	slices.SortFunc(bindings, func(left, right serviceauthz.ServiceBinding) int {
		return bytes.Compare(left.Principal[:], right.Principal[:])
	})
	serviceCut := serviceauthz.ServiceDirectoryCut{
		CatalogGeneration: 1, Revision: 1, TrustDomain: trust, PolicyGeneration: 1,
		Bindings: bindings, ForwardedScopes: []serviceauthz.FrontendContinuationScopeRecord{scope},
		ContinuationGrants: grants,
	}
	if !serviceCut.Valid() {
		t.Fatal("oversized source service cut fixture is invalid")
	}
	request := frontenddrain.PreparedAckRequest{
		Nonce: [16]byte{8}, DrainID: [32]byte{9}, GrantDigest: grants[0].GrantDigest,
		SourcePrincipal: rafttransport.NodeID{10}, SourcePrincipalKeyDigest: [32]byte{11},
		ReceiverNode: bindings[0].PhysicalNode, ReceiverIncarnation: 1,
		ReceiverServiceKeyDigest: bindings[0].KeyDigest, ReceiverNodeRevision: 1,
		SourceCut: frontenddrain.PreparedAckCut{
			DirectoryRevision: 1, DirectoryDigest: [32]byte{12}, CatalogGeneration: 1,
			CatalogHeadDigest: [32]byte{13}, ServiceDirectoryRevision: 1,
			ServiceDirectory: serviceCut,
		},
	}
	if !request.Valid() {
		t.Fatal("oversized source request fixture is invalid")
	}
	if frontendDrainProjectedCutFitsStorage(request) {
		t.Fatal("projected source cut over the PreparedAck wire limit was accepted")
	}
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	if _, admitted := frontend.admitNative(); !admitted || frontend.isDraining() {
		t.Fatal("projected source cut rejection closed Active admission")
	}
}

func TestPreparedFrontendDrainRestartClosesAdmissionBeforeListenerAccept(t *testing.T) {
	physical := rafttransport.NodeID{1}
	gatewayNode := rafttransport.NodeID{2}
	active := gateway.NodeRecord{
		NodeID: physical, Incarnation: 1, ServiceKeyDigest: replication.Digest{1},
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", GatewayAddress: "localhost:4",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{NodeID: gatewayNode, Incarnation: 1,
			ServiceKeyDigest: replication.Digest{2}, ServiceID: [16]byte{3}, SessionID: [16]byte{4},
			SessionRevision: 1, ParticipantDigest: replication.Digest{5}},
	}
	if !active.Valid() {
		t.Fatal("active gateway fixture is invalid")
	}
	intent := [32]byte{6}
	record := gateway.FrontendDrainRecord{
		IntentID: intent, DecommissionIntentID: intent,
		DrainID:      gateway.NewFrontendDrainID(intent, gateway.NodeReference{NodeID: active.NodeID, Incarnation: active.Incarnation}),
		TrustDomain:  rafttransport.TrustDomain{ClusterID: [16]byte{7}, ClusterIncarnation: [16]byte{8}},
		PhysicalNode: active.NodeID, PhysicalIncarnation: active.Incarnation,
		GatewayServiceID: active.Gateway.NodeID, GatewayIncarnation: active.Gateway.Incarnation,
		PeerKeyDigest: active.Gateway.ServiceKeyDigest, GatewayIdentityServiceID: active.Gateway.ServiceID,
		GatewaySessionID: active.Gateway.SessionID, GatewaySessionRevision: active.Gateway.SessionRevision,
		NodeRevision: active.Revision, AdmissionEpoch: 1, AdmissionClosedProofDigest: replication.Digest{9},
		DrainFence: serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
			Operation: serviceauthz.ServiceOperationCatalogRead, Group: raftmember.GroupKey{ClusterID: [16]byte{1}}, Relation: [16]byte{10},
			SessionID: active.Gateway.SessionID, SessionRevision: active.Gateway.SessionRevision, IntentID: [32]byte{11}, FenceDigest: [32]byte{12}},
		Lifecycle: gateway.FrontendDrainPrepared, Revision: 1,
	}
	if !record.ValidForNode(active) {
		t.Fatal("prepared frontend drain fixture is invalid")
	}
	directory, err := gateway.NewReplicatedControlDirectory(gateway.ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{active},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener := newFrontendDrainTestListener()
	frontend := newFrontendAdmission(FrontendDrainIdentity{}, false, false)
	runtime := &Runtime{config: Config{ControlDirectory: frontendDrainDirectoryReader{cut: gateway.NodeDirectoryCut{
		Revision: 1, Digest: replication.Digest{13}, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{active},
	}, records: []gateway.FrontendDrainRecord{record}}, InternalAuthority: serviceauthz.Authority{Node: gatewayNode, Generation: 1}},
		controlDirectory: directory, listener: listener, frontend: frontend}
	if err := runtime.restoreFrontendDrainFromDirectory(context.Background()); err != nil {
		t.Fatalf("restore prepared frontend drain: %v", err)
	}
	if !frontend.isDraining() || !runtime.FrontendDrainStatus().AdmissionDrained {
		t.Fatal("Prepared restart left frontend admission open")
	}
	wrapped := &frontendAdmissionListener{Listener: listener, frontend: frontend}
	client, peer := net.Pipe()
	defer client.Close()
	listener.accepted <- peer
	if _, err := wrapped.Accept(); !errors.Is(err, errFrontendAdmissionDrained) {
		t.Fatalf("listener accepted after Prepared restore: %v", err)
	}
}

func TestApplyLiveDirectoryBindsDrainBeforeServiceCutValidation(t *testing.T) {
	physical := rafttransport.NodeID{1}
	gatewayNode := rafttransport.NodeID{2}
	active := gateway.NodeRecord{
		NodeID: physical, Incarnation: 1, ServiceKeyDigest: replication.Digest{1},
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", GatewayAddress: "localhost:4",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{NodeID: gatewayNode, Incarnation: 1,
			ServiceKeyDigest: replication.Digest{2}, ServiceID: [16]byte{3}, SessionID: [16]byte{4},
			SessionRevision: 1, ParticipantDigest: replication.Digest{5}},
	}
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{{Node: gatewayNode, Capabilities: serviceauthz.AllCapabilities}})
	active.Gateway.ServiceKeyDigest = replication.Digest(profiles[0].LocalServiceKeyDigest())
	if !active.Valid() {
		t.Fatal("active directory record is invalid")
	}
	activeCut := gateway.ReplicatedControlDirectorySnapshot{Revision: 1, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{active}}
	directory, err := gateway.NewReplicatedControlDirectory(activeCut)
	if err != nil {
		t.Fatal(err)
	}
	draining := active
	draining.Lifecycle = gateway.NodeDraining
	draining.Revision = 2
	drainingCut := gateway.ReplicatedControlDirectorySnapshot{Revision: 2, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{draining}}
	reader := frontendDrainDirectoryReader{cut: gateway.NodeDirectoryCut{
		Revision: 2, Digest: replication.Digest{9}, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{draining},
	}}
	runtime := &Runtime{
		config: Config{ControlDirectory: reader, TLSProfile: profiles[0], Authorization: policy,
			InternalAuthority: serviceauthz.Authority{Node: gatewayNode, Generation: policy.Generation()}},
		controlDirectory: directory, frontend: newFrontendAdmission(FrontendDrainIdentity{}, false, false),
	}
	if err := runtime.applyLiveControlDirectory(context.Background(), drainingCut); err == nil {
		t.Fatal("draining cut without a continuation grant unexpectedly passed service validation")
	}
	ack := runtime.FrontendDrainStatus()
	if !runtime.frontend.isDraining() || !ack.AdmissionDrained || ack.Identity.NodeID != physical ||
		ack.Identity.NodeRevision != draining.Revision || ack.Identity.DirectoryRevision != drainingCut.Revision {
		t.Fatalf("local drain identity was not published before service validation: %+v", ack)
	}
	evidence, err := runtime.ScanGatewayParticipant(context.Background(), draining)
	if err != nil {
		t.Fatalf("drain scan rejected the identity published by the directory update: %v", err)
	}
	if !evidence.ValidFor(draining) || evidence.Active {
		t.Fatalf("drain scan evidence = %+v", evidence)
	}
	for _, test := range []struct {
		name   string
		mutate func(*gateway.NodeRecord)
	}{
		{name: "foreign physical identity", mutate: func(record *gateway.NodeRecord) { record.NodeID[0]++ }},
		{name: "foreign physical incarnation", mutate: func(record *gateway.NodeRecord) { record.Incarnation++ }},
		{name: "foreign physical key", mutate: func(record *gateway.NodeRecord) { record.ServiceKeyDigest[0]++ }},
		{name: "foreign gateway identity", mutate: func(record *gateway.NodeRecord) { record.Gateway.NodeID[0]++ }},
		{name: "foreign gateway incarnation", mutate: func(record *gateway.NodeRecord) { record.Gateway.Incarnation++ }},
		{name: "foreign gateway key", mutate: func(record *gateway.NodeRecord) { record.Gateway.ServiceKeyDigest[0]++ }},
		{name: "stale node revision", mutate: func(record *gateway.NodeRecord) { record.Revision++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := draining
			test.mutate(&candidate)
			if evidence.ValidFor(candidate) {
				t.Fatalf("evidence unexpectedly accepted mutated record: %+v", candidate)
			}
		})
	}
}

func TestFrontendContinuationCredentialSnapshotsOpenNativeSocket(t *testing.T) {
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	listener := newFrontendDrainTestListener()
	wrapped := &frontendAdmissionListener{Listener: listener, frontend: frontend}
	pgToken, ok := frontend.admitPG()
	if !ok {
		t.Fatal("failed to admit PostgreSQL token fixture")
	}
	client, peer := net.Pipe()
	defer client.Close()
	listener.accepted <- peer
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := wrapped.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept native socket")
	}
	defer conn.Close()

	base := serviceauthz.FrontendConnectionContextFromConn(context.Background(), conn)
	if _, ok := serviceauthz.FrontendContinuationFromContext(base); ok {
		t.Fatal("active socket exposed a continuation before drain publication")
	}
	frontend.begin(listener)
	var digest [32]byte
	digest[0] = 9
	if !frontend.installGrant(digest, serviceauthz.FrontendScopeNative) {
		t.Fatal("continuation grant was not installed for an open socket")
	}
	credential, ok := serviceauthz.FrontendContinuationFromContext(base)
	if !ok || credential.GrantDigest != digest || credential.Protocol != serviceauthz.FrontendScopeNative {
		t.Fatalf("credential = %+v, ok=%v", credential, ok)
	}
	if _, ok := serviceauthz.FrontendContinuationFromContext(
		serviceauthz.FrontendConnectionContextFromConn(context.Background(), &frontendTrackedConn{
			Conn: conn, token: credential.ConnToken, scope: serviceauthz.FrontendScopePostgreSQL, frontend: frontend,
		}),
	); ok {
		t.Fatal("native token was replayable under PostgreSQL scope")
	}
	if !frontend.installGrant(digest, serviceauthz.FrontendScopePostgreSQL) {
		t.Fatal("second protocol grant should be independently installable")
	}
	pgContextConn := &frontendTrackedConn{Conn: conn, token: pgToken,
		scope: serviceauthz.FrontendScopePostgreSQL, frontend: frontend}
	pgContext := serviceauthz.FrontendConnectionContextFromConn(context.Background(), pgContextConn)
	pgCredential, ok := serviceauthz.FrontendContinuationFromContext(pgContext)
	if !ok || pgCredential.ConnToken != pgToken || pgCredential.Protocol != serviceauthz.FrontendScopePostgreSQL {
		t.Fatalf("PostgreSQL credential = %+v, ok=%v", pgCredential, ok)
	}
	frontend.releasePG(pgToken)

	// A restarted draining frontend carries only the durable fence state. It
	// must not mint or revive a token for a socket that was never accepted by
	// this process.
	restarted := newFrontendAdmission(testFrontendDrainIdentity(), true, false)
	if !restarted.installGrant(digest, serviceauthz.FrontendScopeNative) {
		t.Fatal("restarted drain rejected durable grant publication")
	}
	if _, ok := restarted.FrontendContinuationCredential(credential.ConnToken, serviceauthz.FrontendScopeNative); ok {
		t.Fatal("restarted drain revived an old accepted-socket token")
	}
}

func TestFrontendDrainKeepsRealStoredDataHeldPGSession(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "held-pg.vdb")
	database, err := sqldriver.Open(databasePath)
	if err != nil {
		t.Fatalf("open stored database: %v", err)
	}
	setup, err := database.NewSession(t.Context())
	if err != nil {
		_ = database.Close()
		t.Fatalf("open setup session: %v", err)
	}
	create, err := setup.Prepare(t.Context(), "CREATE TABLE held_rows (PRIMARY KEY (_pgwire_key))")
	if err != nil {
		_ = setup.Close()
		_ = database.Close()
		t.Fatalf("prepare stored table: %v", err)
	}
	if _, err := create.Exec(t.Context(), nil); err != nil {
		_ = create.Close()
		_ = setup.Close()
		_ = database.Close()
		t.Fatalf("create stored table: %v", err)
	}
	_ = create.Close()
	insert, err := setup.Prepare(t.Context(), "INSERT INTO held_rows VALUES (?)")
	if err != nil {
		_ = setup.Close()
		_ = database.Close()
		t.Fatalf("prepare stored row: %v", err)
	}
	if _, err := insert.Exec(t.Context(), []any{[]byte(`{"_pgwire_key":"held-1","value":"durable"}`)}); err != nil {
		_ = insert.Close()
		_ = setup.Close()
		_ = database.Close()
		t.Fatalf("insert stored row: %v", err)
	}
	_ = insert.Close()
	if err := setup.Close(); err != nil {
		_ = database.Close()
		t.Fatalf("close setup session: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen PostgreSQL: %v", err)
	}
	identity := testFrontendDrainIdentity()
	frontend := newFrontendAdmission(identity, false, true)
	server, err := pgwire.NewServer(database, pgwire.Options{Auth: pgwire.Trust(), Database: "vibedb"})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create PostgreSQL server: %v", err)
	}
	wrapped := &frontendPGAdmissionListener{Listener: listener, frontend: frontend}
	frontend.bindPG(server)
	served := make(chan error, 1)
	go func() { served <- server.Serve(wrapped) }()
	runtime := &Runtime{frontend: frontend}
	connection := openDDLWire(t, t.Context(), listener.Addr().String())
	defer connection.Close()
	if result := ddlWireQuery(t, connection, "SELECT COUNT(*) FROM held_rows", false); result.code != "" || len(result.rows) != 1 || len(result.rows[0]) != 1 || result.rows[0][0] != "1" {
		_ = server.Close()
		t.Fatalf("stored row before drain: result=%+v", result)
	}
	if result := ddlWireQuery(t, connection, "BEGIN", false); result.code != "" {
		_ = server.Close()
		t.Fatalf("hold PostgreSQL transaction: result=%+v", result)
	}

	ack := runtime.BeginFrontendDrain()
	if !ack.AdmissionDrained || !ack.PGAdmissionDrained || ack.ActivePGConnections != 1 || ack.ActivePGSessions != 1 || ack.SafeToStop {
		_ = connection.Close()
		_ = server.Close()
		t.Fatalf("held PostgreSQL session was not retained by drain: %+v", ack)
	}
	if result := ddlWireQuery(t, connection, "SELECT value FROM held_rows", false); result.code != "" || len(result.rows) != 1 || len(result.rows[0]) != 1 || result.rows[0][0] != `"durable"` {
		_ = server.Close()
		t.Fatalf("stored row after admission drain: result=%+v", result)
	}
	if result := ddlWireQuery(t, connection, `INSERT INTO held_rows VALUES ('{"_pgwire_key":"held-2","value":"continued"}')`, false); result.code != "" || result.tag != "INSERT 0 1" {
		_ = server.Close()
		t.Fatalf("stored write after admission drain: result=%+v", result)
	}
	if result := ddlWireQuery(t, connection, "COMMIT", false); result.code != "" {
		_ = server.Close()
		t.Fatalf("commit held PostgreSQL transaction after drain: result=%+v", result)
	}
	_ = connection.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ack = runtime.FrontendDrainStatus()
		if ack.ActivePGConnections == 0 && ack.ActivePGSessions == 0 && ack.SafeToStop {
			break
		}
		if time.Now().After(deadline) {
			_ = server.Close()
			t.Fatalf("held PostgreSQL session did not release: %+v", ack)
		}
		time.Sleep(time.Millisecond)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close PostgreSQL server: %v", err)
	}
	select {
	case serveErr := <-served:
		if !errors.Is(serveErr, pgwire.ErrServerDraining) && !errors.Is(serveErr, pgwire.ErrServerClosed) {
			t.Fatalf("PostgreSQL serve after drain: %v", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PostgreSQL accept loop did not stop after drain")
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close stored database: %v", err)
	}

	// Reopen the same on-disk catalog after the held session and drain have
	// completed. This keeps the test tied to persisted data rather than a
	// backend fixture that merely echoes a row from memory.
	reopened, err := sqldriver.Open(databasePath)
	if err != nil {
		t.Fatalf("reopen stored database: %v", err)
	}
	defer reopened.Close()
	reader, err := reopened.NewSession(t.Context())
	if err != nil {
		t.Fatalf("open verification session: %v", err)
	}
	defer reader.Close()
	query, err := reader.Prepare(t.Context(), "SELECT value FROM held_rows")
	if err != nil {
		t.Fatalf("prepare persisted verification: %v", err)
	}
	defer query.Close()
	cursor, err := query.Query(t.Context(), nil)
	if err != nil {
		t.Fatalf("query persisted row: %v", err)
	}
	defer cursor.Close()
	if !cursor.Next() || cursor.Cell(0).String() != `"durable"` {
		t.Fatalf("persisted row after restart = %q", cursor.Cell(0).String())
	}
	if !cursor.Next() || cursor.Cell(0).String() != `"continued"` {
		t.Fatalf("continued stored row after restart = %q", cursor.Cell(0).String())
	}
}
