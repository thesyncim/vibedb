package nodecontrol

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestNodeInfoWireIsBoundedAndCanonical(t *testing.T) {
	request := NodeInfoRequest{Nonce: [nodeInfoNonceBytes]byte{1}, Operation: OpNodeInfo,
		NodeID: rafttransport.NodeID{1}, Incarnation: 7, MinimumInventoryRevision: 4}
	raw, err := AppendNodeInfoRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != nodeInfoRequestHeaderBytes || !bytes.Equal(raw[:8], nodeInfoRequestMagic[:]) {
		t.Fatalf("request wire length/magic=%d/%q", len(raw), raw[:8])
	}
	opened, err := OpenNodeInfoRequest(raw)
	if err != nil || opened != request {
		t.Fatalf("request round trip=%+v err=%v", opened, err)
	}
	raw[60] = 1
	if _, err := OpenNodeInfoRequest(raw); err == nil {
		t.Fatal("request reserved bytes accepted")
	}

	observation := testNodeInfoObservation(request)
	reply, err := AppendNodeInfoReply(nil, observation)
	if err != nil {
		t.Fatal(err)
	}
	openedReply, err := OpenNodeInfoReply(reply)
	if err != nil || openedReply != observation {
		t.Fatalf("reply round trip=%+v err=%v", openedReply, err)
	}
	if _, err := OpenNodeInfoReply(append(append([]byte(nil), reply...), ' ')); err == nil {
		t.Fatal("trailing node-info response accepted")
	}
	reply[10] = 1
	if _, err := OpenNodeInfoReply(reply); err == nil {
		t.Fatal("response reserved bytes accepted")
	}
}

func TestNodeInfoServiceAndClientBindPhysicalIdentityAndReadinessFacts(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	target := rafttransport.NodeID{1}
	caller := rafttransport.NodeID{0xaa}
	request := NodeInfoRequest{Nonce: [nodeInfoNonceBytes]byte{2}, Operation: OpNodeInfo,
		NodeID: target, Incarnation: 7, MinimumInventoryRevision: 4}
	provider := NodeInfoProviderFunc(func(_ context.Context, got NodeInfoRequest) (NodeInfoObservation, error) {
		if got != request {
			return NodeInfoObservation{}, ErrNodeInfoConflict
		}
		return testNodeInfoObservation(got), nil
	})
	service, err := NewNodeInfoService(NodeInfoServiceOptions{
		Provider: provider, TrustDomain: domain, LocalNode: target, Incarnation: request.Incarnation,
		Authorize: func(identity rafttransport.PeerIdentity, got NodeInfoRequest) bool {
			return identity.Node == caller && got.NodeID == target
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	opener := &nodeInfoTestOpener{service: service, domain: domain, target: target, caller: caller}
	client, err := NewNodeInfoClient(NodeInfoClientOptions{
		Opener: opener, TrustDomain: domain,
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Observe(context.Background(), target, request)
	if err != nil || got != testNodeInfoObservation(request) {
		t.Fatalf("node-info observation=%+v err=%v", got, err)
	}
	if !got.ReadyForEnrollment() || got.ServingGroups != 0 || got.ReservedGroups != 2 {
		t.Fatalf("readiness facts lost empty-node reservation state: %+v", got)
	}
	if opener.calls != 1 {
		t.Fatalf("observation was cached/reused: calls=%d", opener.calls)
	}
	if _, err := client.Observe(context.Background(), rafttransport.NodeID{2}, request); !errors.Is(err, ErrNodeInfo) {
		t.Fatalf("wrong target request=%v", err)
	}
}

func TestNodeInfoServiceRejectsStaleInventoryAndUnauthorizedCaller(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{3}, ClusterIncarnation: [16]byte{4}}
	target := rafttransport.NodeID{3}
	request := NodeInfoRequest{Nonce: [nodeInfoNonceBytes]byte{3}, Operation: OpNodeInfo,
		NodeID: target, Incarnation: 9, MinimumInventoryRevision: 8}
	service, err := NewNodeInfoService(NodeInfoServiceOptions{
		Provider: NodeInfoProviderFunc(func(_ context.Context, got NodeInfoRequest) (NodeInfoObservation, error) {
			observation := testNodeInfoObservation(got)
			observation.InventoryRevision = got.MinimumInventoryRevision - 1
			observation.ObservationDigest = observation.computedDigest()
			return observation, nil
		}),
		TrustDomain: domain, LocalNode: target, Incarnation: request.Incarnation,
		Authorize:     func(rafttransport.PeerIdentity, NodeInfoRequest) bool { return false },
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	server := &nodeInfoTestConn{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{0x44}}, class: rafttransport.TrafficShardControl}
	client := &nodeInfoTestConn{Conn: left, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{0x44}}, class: rafttransport.TrafficShardControl}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Serve(context.Background(), server) }()
	if err := WriteNodeInfoRequest(client, request); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := client.Read(one[:]); err == nil {
		t.Fatal("unauthorized node-info request unexpectedly returned bytes")
	}
	_ = client.Close()
	if err := <-done; !errors.Is(err, ErrNodeInfoUnauthorized) {
		t.Fatalf("unauthorized service error=%v", err)
	}
}

func testNodeInfoObservation(request NodeInfoRequest) NodeInfoObservation {
	var actual, declared autosplit.CapacityVector
	for index := range actual {
		actual[index] = 100 + uint64(index)
		declared[index] = 200 + uint64(index)
	}
	facts := NodeInfoStoreFacts{
		Identity:      NodeInfoStoreIdentity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, NodeID: [16]byte(request.NodeID)},
		SPKIPinDigest: replication.Digest{0xa1}, Endpoints: NodeInfoEndpoints{
			Peer: "127.0.0.1:21001", Native: "127.0.0.1:21002", Snapshot: "127.0.0.1:21003", Control: "127.0.0.1:21004",
		}, Readiness: NodeInfoReadiness{NodeJournalReady: true, PhysicalStoreReady: true, BoundListenersReady: true},
		ServingGroups: 0, ReservedGroups: 2, InventoryRevision: 8,
		ActualCapacity: actual, ActualUsage: autosplit.CapacityVector{}, DeclaredCapacity: declared,
		ActualMigrationCapacity: 1000, ActualMigrationUsed: 10, DeclaredMigrationCapacity: 2000,
		ActualActiveReceives: 1, DeclaredMaxReceives: 4,
	}
	result, err := facts.Observation(request)
	if err != nil {
		panic(err)
	}
	return result
}

type nodeInfoTestConn struct {
	net.Conn
	identity rafttransport.PeerIdentity
	class    rafttransport.TrafficClass
}

func (connection *nodeInfoTestConn) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (connection *nodeInfoTestConn) PeerKeyDigest() [32]byte { return [32]byte{0xa1} }
func (connection *nodeInfoTestConn) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type nodeInfoTestOpener struct {
	service *NodeInfoService
	domain  rafttransport.TrustDomain
	target  rafttransport.NodeID
	caller  rafttransport.NodeID
	calls   int
}

func (opener *nodeInfoTestOpener) OpenShardControl(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	opener.calls++
	if node != opener.target {
		return nil, ErrNodeInfoUnavailable
	}
	left, right := net.Pipe()
	client := &nodeInfoTestConn{Conn: left, identity: rafttransport.PeerIdentity{TrustDomain: opener.domain, Node: opener.target}, class: rafttransport.TrafficShardControl}
	server := &nodeInfoTestConn{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: opener.domain, Node: opener.caller}, class: rafttransport.TrafficShardControl}
	go func() { _ = opener.service.Serve(context.Background(), server) }()
	return client, nil
}
