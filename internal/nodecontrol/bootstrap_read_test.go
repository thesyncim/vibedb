package nodecontrol

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestBootstrapReadWireIsFixedAndCanonical(t *testing.T) {
	intent := testIntent([]byte(`{"root":"/node/group"}`), gateway.EnrollmentReserved)
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeJoining, replication.Digest{0xa1})
	reply := bootstrapReadTestReply(intent, node)
	raw, err := AppendBootstrapReadReply(nil, reply)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenBootstrapReadReply(raw)
	if err != nil || opened.Intent != intent || opened.Node != node {
		t.Fatalf("reply round trip=%+v err=%v", opened, err)
	}
	if _, err := OpenBootstrapReadReply(append(append([]byte(nil), raw...), ' ')); err == nil {
		t.Fatal("trailing response bytes accepted")
	}
	request := BootstrapReadRequest{Nonce: [bootstrapReadNonceBytes]byte{1}, Operation: OpReadOwnEnrollment,
		PhysicalNode: intent.Target.Node, Incarnation: intent.Target.NodeIncarnation, IntentID: intent.IntentID}
	requestRaw, err := AppendBootstrapReadRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	if openedRequest, openErr := OpenBootstrapReadRequest(requestRaw); openErr != nil || openedRequest != request {
		t.Fatalf("request round trip=%+v err=%v", openedRequest, openErr)
	}
	requestRaw[10] = 1
	if _, err := OpenBootstrapReadRequest(requestRaw); err == nil {
		t.Fatal("nonzero reserved request bytes accepted")
	}
	if !bytes.Equal(raw[:8], bootstrapReadResponseMagic[:]) {
		t.Fatal("response did not use the dedicated bootstrap-read discriminator")
	}
}

func TestBootstrapReadServiceRequiresExactCommittedIdentityAndStableCut(t *testing.T) {
	intent := testIntent([]byte(`{"root":"/node/group"}`), gateway.EnrollmentReserved)
	key := replication.Digest{0xa1}
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeJoining, key)
	authority := &bootstrapReadTestAuthority{intent: intent, node: node, cut: bootstrapReadTestCut(node), evidence: bootstrapReadTestEvidence(node)}
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	service, err := NewBootstrapReadService(BootstrapReadServiceOptions{
		Authority: authority, TrustDomain: domain,
		Authorize: func(identity rafttransport.PeerIdentity, record gateway.NodeRecord) bool {
			return identity.Node == record.NodeID
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	serverConn := &bootstrapReadTestConn{Conn: server, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node.NodeID}, key: [32]byte(key), class: rafttransport.TrafficGatewayControl}
	clientConn := &bootstrapReadTestConn{Conn: client, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node.NodeID}, key: [32]byte(key), class: rafttransport.TrafficGatewayControl}
	serverDone := make(chan error, 1)
	go func() { serverDone <- service.Serve(context.Background(), serverConn) }()
	request := BootstrapReadRequest{Nonce: [bootstrapReadNonceBytes]byte{3}, Operation: OpReadOwnEnrollment,
		PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: intent.IntentID}
	if err := WriteBootstrapReadRequest(clientConn, request); err != nil {
		t.Fatal(err)
	}
	if reply, err := ReadBootstrapReadReply(clientConn); err != nil || reply.Intent != intent {
		t.Fatalf("service reply=%+v err=%v", reply, err)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}

	wrongClient, wrongServer := net.Pipe()
	wrongServerConn := &bootstrapReadTestConn{Conn: wrongServer, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node.NodeID}, key: [32]byte{0xff}, class: rafttransport.TrafficGatewayControl}
	go func() { serverDone <- service.Serve(context.Background(), wrongServerConn) }()
	if err := WriteBootstrapReadRequest(wrongClient, request); err != nil {
		t.Fatal(err)
	}
	if err := wrongClient.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := wrongClient.Read(one[:]); err == nil {
		t.Fatal("wrong committed SPKI was not rejected")
	}
	_ = wrongClient.Close()
	if err := <-serverDone; !errors.Is(err, ErrBootstrapReadUnauthorized) {
		t.Fatalf("wrong SPKI service error=%v", err)
	}
}

func TestBootstrapReadServiceRejectsEnrollmentRowChangedDuringScan(t *testing.T) {
	intent := testIntent([]byte(`{"root":"/node/group"}`), gateway.EnrollmentReserved)
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeJoining, replication.Digest{0xa1})
	authority := &bootstrapReadTestAuthority{
		intent: intent, node: node, cut: bootstrapReadTestCut(node), evidence: bootstrapReadTestEvidence(node),
	}
	authority.onIntentRead = func(reads int, state *bootstrapReadTestAuthority) {
		if reads != 2 {
			return
		}
		cancelled := state.intent
		cancelled.State = gateway.EnrollmentCancelled
		cancelled.Revision++
		state.intent = cancelled
	}
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	service, err := NewBootstrapReadService(BootstrapReadServiceOptions{
		Authority: authority, TrustDomain: domain, Authorize: func(rafttransport.PeerIdentity, gateway.NodeRecord) bool { return true },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	serverConn := &bootstrapReadTestConn{Conn: server, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node.NodeID}, key: [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficGatewayControl}
	clientConn := &bootstrapReadTestConn{Conn: client, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node.NodeID}, key: [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficGatewayControl}
	serverDone := make(chan error, 1)
	go func() { serverDone <- service.Serve(context.Background(), serverConn) }()
	request := BootstrapReadRequest{Nonce: [bootstrapReadNonceBytes]byte{0x44}, Operation: OpReadOwnEnrollment,
		PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: intent.IntentID}
	if err := WriteBootstrapReadRequest(clientConn, request); err != nil {
		t.Fatal(err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBootstrapReadReply(clientConn); err == nil {
		t.Fatal("service returned an enrollment row that changed during the scan")
	}
	_ = clientConn.Close()
	if err := <-serverDone; !errors.Is(err, ErrBootstrapReadStale) {
		t.Fatalf("changed enrollment row service error=%v", err)
	}
}

func TestBootstrapReadClientFailsOverOnlyConfiguredSeedsAndFreshensNonce(t *testing.T) {
	intent := testIntent([]byte(`{"root":"/node/group"}`), gateway.EnrollmentReserved)
	targetKey := replication.Digest{0xa1}
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeJoining, targetKey)
	authority := &bootstrapReadTestAuthority{intent: intent, node: node, cut: bootstrapReadTestCut(node), evidence: bootstrapReadTestEvidence(node)}
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	service, err := NewBootstrapReadService(BootstrapReadServiceOptions{
		Authority: authority, TrustDomain: domain, Authorize: func(rafttransport.PeerIdentity, gateway.NodeRecord) bool { return true },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	seedA := BootstrapGatewaySeed{NodeID: rafttransport.NodeID{0x20}, Incarnation: 1, ControlAddress: "127.0.0.1:20001", SPKIPinDigest: replication.Digest{0x20}}
	seedB := BootstrapGatewaySeed{NodeID: rafttransport.NodeID{0x21}, Incarnation: 1, ControlAddress: "127.0.0.1:20002", SPKIPinDigest: replication.Digest{0x21}}
	opener := &bootstrapReadTestOpener{service: service, domain: domain, target: node, seeds: []BootstrapGatewaySeed{seedA, seedB}, failFirst: true}
	var nonce byte
	client, err := NewBootstrapReadClient(BootstrapReadClientOptions{
		Opener: opener, Seeds: []BootstrapGatewaySeed{seedA, seedB}, TrustDomain: domain,
		PhysicalNode: node.NodeID, Incarnation: node.Incarnation,
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
		Nonce: func() ([bootstrapReadNonceBytes]byte, error) {
			nonce++
			return [bootstrapReadNonceBytes]byte{nonce}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	read, err := client.ReadEnrollmentIntent(context.Background(), intent.IntentID)
	if err != nil || read != intent {
		t.Fatalf("fresh seed read=%+v err=%v", read, err)
	}
	if opener.calls != 2 || nonce != 2 {
		t.Fatalf("seed attempts=%d nonce calls=%d, want one fresh nonce per query and configured failover", opener.calls, nonce)
	}
	if _, err := client.ReadEnrollmentIntent(context.Background(), intent.IntentID); err != nil {
		t.Fatal(err)
	}
	if nonce != 3 {
		t.Fatalf("reader reused nonce/cache: calls=%d", nonce)
	}
}

type bootstrapReadTestAuthority struct {
	intent       gateway.GroupEnrollmentIntent
	node         gateway.NodeRecord
	cut          gateway.NodeDirectoryCut
	evidence     gateway.NodeReferenceEvidence
	intentReads  int
	onIntentRead func(int, *bootstrapReadTestAuthority)
}

func (authority *bootstrapReadTestAuthority) ReadNode(_ context.Context, node rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
	if node != authority.node.NodeID || incarnation != authority.node.Incarnation {
		return gateway.NodeRecord{}, gateway.ErrScalingNodeMissing
	}
	return authority.node, nil
}

func (authority *bootstrapReadTestAuthority) ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error) {
	return authority.cut, nil
}

func (authority *bootstrapReadTestAuthority) ReadEnrollmentIntent(_ context.Context, id [32]byte) (gateway.GroupEnrollmentIntent, error) {
	if id != authority.intent.IntentID {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrEnrollmentIntentMissing
	}
	authority.intentReads++
	if authority.onIntentRead != nil {
		authority.onIntentRead(authority.intentReads, authority)
	}
	return authority.intent, nil
}

func (authority *bootstrapReadTestAuthority) ScanNodeReferences(_ context.Context, node rafttransport.NodeID, incarnation uint64) (gateway.NodeReferenceEvidence, error) {
	if node != authority.node.NodeID || incarnation != authority.node.Incarnation {
		return gateway.NodeReferenceEvidence{}, gateway.ErrScalingNodeMissing
	}
	return authority.evidence, nil
}

func bootstrapReadTestNode(node rafttransport.NodeID, incarnation uint64, lifecycle gateway.NodeLifecycle, key replication.Digest) gateway.NodeRecord {
	capacity := autosplit.CapacityVector{}
	for index := range capacity {
		capacity[index] = 100
	}
	return gateway.NodeRecord{NodeID: node, Incarnation: incarnation, ServiceKeyDigest: key,
		DataEndpoint: distribution.EndpointID("data"), NativeEndpoint: distribution.EndpointID("native"), ControlEndpoint: distribution.EndpointID("control"),
		DataAddress: "127.0.0.1:8001", NativeAddress: "127.0.0.1:8002", ControlAddress: "127.0.0.1:8003",
		FailureDomain: "zone-a", Roles: gateway.NodeRoleStorage, Capacity: capacity, MigrationCapacity: 1 << 20, MaxReceives: 4,
		Lifecycle: lifecycle, Revision: 2, CatalogGeneration: 12}
}

func bootstrapReadTestCut(node gateway.NodeRecord) gateway.NodeDirectoryCut {
	return gateway.NodeDirectoryCut{Revision: 7, Digest: replication.Digest{0x07}, CatalogGeneration: 12, Nodes: []gateway.NodeRecord{node}}
}

func bootstrapReadTestEvidence(node gateway.NodeRecord) gateway.NodeReferenceEvidence {
	return gateway.NodeReferenceEvidence{NodeID: node.NodeID, Incarnation: node.Incarnation, CatalogGeneration: 12,
		DirectoryRevision: node.Revision, DirectoryCutRevision: 7, DirectoryCutDigest: replication.Digest{0x07},
		CatalogHeadDigest: replication.Digest{0x08}, EnrollmentDirectoryDigest: replication.Digest{0x09}, Digest: replication.Digest{0x0a}}
}

func bootstrapReadTestReply(intent gateway.GroupEnrollmentIntent, node gateway.NodeRecord) BootstrapReadReply {
	evidence := bootstrapReadTestEvidence(node)
	cut := bootstrapReadTestCut(node)
	return BootstrapReadReply{Nonce: [bootstrapReadNonceBytes]byte{1}, Operation: OpReadOwnEnrollment,
		PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: intent.IntentID, Intent: intent, IntentDigest: intent.Digest(), Node: node,
		DirectoryCutRevision: cut.Revision, DirectoryCutDigest: cut.Digest, CatalogGeneration: evidence.CatalogGeneration,
		CatalogHeadDigest: evidence.CatalogHeadDigest, EnrollmentDirectoryDigest: evidence.EnrollmentDirectoryDigest}
}

type bootstrapReadTestConn struct {
	net.Conn
	identity rafttransport.PeerIdentity
	key      [32]byte
	class    rafttransport.TrafficClass
}

func (connection *bootstrapReadTestConn) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (connection *bootstrapReadTestConn) PeerKeyDigest() [32]byte { return connection.key }
func (connection *bootstrapReadTestConn) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type bootstrapReadTestOpener struct {
	service   *BootstrapReadService
	domain    rafttransport.TrustDomain
	target    gateway.NodeRecord
	seeds     []BootstrapGatewaySeed
	failFirst bool
	calls     int
}

func (opener *bootstrapReadTestOpener) OpenBootstrapGatewayControl(_ context.Context, seed BootstrapGatewaySeed) (rafttransport.PeerConnection, error) {
	opener.calls++
	if opener.failFirst && opener.calls == 1 {
		return nil, errors.New("seed unavailable")
	}
	left, right := net.Pipe()
	client := &bootstrapReadTestConn{Conn: left, identity: rafttransport.PeerIdentity{TrustDomain: opener.domain, Node: seed.NodeID}, key: [32]byte(seed.SPKIPinDigest), class: rafttransport.TrafficGatewayControl}
	server := &bootstrapReadTestConn{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: opener.domain, Node: opener.target.NodeID}, key: [32]byte(opener.target.ServiceKeyDigest), class: rafttransport.TrafficGatewayControl}
	go func() { _ = opener.service.Serve(context.Background(), server) }()
	return client, nil
}
