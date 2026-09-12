package nodecontrol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
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
	// Set the bound before the server can reject and close the pipe. Setting
	// a deadline after the request races the expected authorization refusal.
	if err := wrongClient.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() { serverDone <- service.Serve(context.Background(), wrongServerConn) }()
	if err := WriteBootstrapReadRequest(wrongClient, request); err != nil {
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
	scanReads    int
	onScan       func(int, *bootstrapReadTestAuthority)
	intentErr    error
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
	if authority.intentErr != nil {
		return gateway.GroupEnrollmentIntent{}, authority.intentErr
	}
	copyOf := authority.intent
	if copyOf.Proof != nil {
		proof := *copyOf.Proof
		copyOf.Proof = &proof
	}
	if copyOf.Receipt != nil {
		receipt := *copyOf.Receipt
		copyOf.Receipt = &receipt
	}
	return copyOf, nil
}

func (authority *bootstrapReadTestAuthority) ScanNodeReferences(_ context.Context, node rafttransport.NodeID, incarnation uint64) (gateway.NodeReferenceEvidence, error) {
	if node != authority.node.NodeID || incarnation != authority.node.Incarnation {
		return gateway.NodeReferenceEvidence{}, gateway.ErrScalingNodeMissing
	}
	authority.scanReads++
	if authority.onScan != nil {
		authority.onScan(authority.scanReads, authority)
	}
	return authority.evidence, nil
}

type bootstrapRecoveryTestAuthority struct {
	*bootstrapReadTestAuthority
	snapshot *gateway.Snapshot
	digest   replication.Digest
	err      error
}

func (authority *bootstrapRecoveryTestAuthority) ReadReplicatedCatalogHead(context.Context) (*gateway.Snapshot, replication.Digest, error) {
	return authority.snapshot, authority.digest, authority.err
}

func bootstrapRecoveryTestSnapshot(t *testing.T, intent gateway.GroupEnrollmentIntent, targetServing bool) *gateway.Snapshot {
	t.Helper()
	replicas := []gateway.ReplicatedReplicaDescriptor{
		{Member: 1, Node: rafttransport.NodeID{1}, StoreID: [16]byte{1}, NodeIncarnation: 1, Endpoint: "one", NativeEndpoint: "one-native", ControlEndpoint: "one-control"},
		{Member: 2, Node: rafttransport.NodeID{2}, StoreID: [16]byte{2}, NodeIncarnation: 1, Endpoint: "two", NativeEndpoint: "two-native", ControlEndpoint: "two-control"},
		{Member: 3, Node: rafttransport.NodeID{3}, StoreID: [16]byte{3}, NodeIncarnation: 1, Endpoint: "three", NativeEndpoint: "three-native", ControlEndpoint: "three-control"},
	}
	if targetServing {
		target := intent.Target
		replicas[2] = gateway.ReplicatedReplicaDescriptor{Member: target.Member, Node: target.Node, StoreID: target.StoreID,
			NodeIncarnation: target.NodeIncarnation, Endpoint: target.Endpoint, NativeEndpoint: target.NativeEndpoint, ControlEndpoint: target.ControlEndpoint}
	}
	leaders := make([]distribution.EndpointID, len(replicas))
	endpoints := make(map[distribution.EndpointID]string)
	for index, replica := range replicas {
		leaders[index] = replica.Endpoint
		endpoints[replica.Endpoint] = fmt.Sprintf("127.0.0.1:%d", 1001+index)
		endpoints[replica.NativeEndpoint] = fmt.Sprintf("127.0.0.1:%d", 2001+index)
		endpoints[replica.ControlEndpoint] = fmt.Sprintf("127.0.0.1:%d", 3001+index)
	}
	manifest, err := distribution.NewManifest(intent.Distribution, distribution.RoutingVersion(intent.ExpectedCommand.RoutingVersion), []distribution.Shard{{
		ID: intent.Shard, AllocationGeneration: intent.AllocationGeneration, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: leaders, Epoch: distribution.OwnershipEpoch(intent.ExpectedCommand.OwnershipEpoch),
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: intent.Distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, intent.CatalogGeneration, nil, nil, []gateway.ReplicatedShardDescriptor{{
		Distribution: intent.Distribution, Shard: intent.Shard, Group: intent.Group, AllocationGeneration: intent.AllocationGeneration,
		Command: intent.ExpectedCommand, RangeIdentity: replication.Digest{1}, LineageDigest: replication.Digest{2}, ForwardingRuleDigest: replication.Digest{3}, Replicas: replicas,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func bootstrapRecoveryTestDirectory(t *testing.T, snapshot *gateway.Snapshot, target gateway.NodeRecord) (gateway.NodeRecord, gateway.NodeDirectoryCut) {
	t.Helper()
	cut := bootstrapReadTestCut(target)
	route, ok := snapshot.ReplicatedRouteAt(0, nil)
	if !ok {
		t.Fatal("missing fixture route")
	}
	cut.Nodes = nil
	foundTarget := false
	for _, replica := range route.Replicas {
		node := bootstrapReadTestNode(replica.Node, replica.NodeIncarnation, gateway.NodeActive, replication.Digest{0xa1})
		node.DataEndpoint, node.NativeEndpoint, node.ControlEndpoint = distribution.EndpointID(replica.Endpoint), distribution.EndpointID(replica.NativeEndpoint), distribution.EndpointID(replica.ControlEndpoint)
		node.DataAddress, node.NativeAddress, node.ControlAddress = replica.DataAddress, replica.Address, replica.ControlAddress
		if replica.Node == target.NodeID {
			target = node
			foundTarget = true
		}
		cut.Nodes = append(cut.Nodes, node)
	}
	if !foundTarget {
		cut.Nodes = append(cut.Nodes, target)
	}
	slices.SortFunc(cut.Nodes, func(left, right gateway.NodeRecord) int { return bytes.Compare(left.NodeID[:], right.NodeID[:]) })
	return target, cut
}

func TestBootstrapRecoveryReadUsesCurrentServingPlacement(t *testing.T) {
	for _, targetServing := range []bool{true, false} {
		t.Run(fmt.Sprintf("target-serving-%t", targetServing), func(t *testing.T) {
			intent := testIntent([]byte(`{"schema":"orders"}`), gateway.EnrollmentComplete)
			intent.MoveOperationID = [32]byte{1}
			node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeActive, replication.Digest{0xa1})
			snapshot := bootstrapRecoveryTestSnapshot(t, intent, targetServing)
			node, cut := bootstrapRecoveryTestDirectory(t, snapshot, node)
			authority := &bootstrapRecoveryTestAuthority{bootstrapReadTestAuthority: &bootstrapReadTestAuthority{
				intent: intent, node: node, cut: cut, evidence: bootstrapReadTestEvidence(node),
			}, snapshot: snapshot, digest: replication.Digest{8}}
			domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			service, err := NewBootstrapReadService(BootstrapReadServiceOptions{Authority: authority, TrustDomain: domain,
				Authorize: func(rafttransport.PeerIdentity, gateway.NodeRecord) bool { return true }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			seed := BootstrapGatewaySeed{NodeID: rafttransport.NodeID{10}, Incarnation: 1, ControlAddress: "127.0.0.1:9999", SPKIPinDigest: replication.Digest{10}}
			client, err := NewBootstrapReadClient(BootstrapReadClientOptions{Opener: &bootstrapReadTestOpener{service: service, domain: domain, target: node},
				Seeds: []BootstrapGatewaySeed{seed}, TrustDomain: domain, PhysicalNode: node.NodeID, Incarnation: node.Incarnation,
				ReadDeadline: deadline, WriteDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			slot := new(IntentReaderSlot)
			if err := slot.Set(client); err != nil {
				t.Fatal(err)
			}
			reply, err := slot.ReadEnrollmentRecovery(t.Context(), intent.IntentID)
			if err != nil || reply.CurrentRoute == nil || len(reply.CurrentNodes) != 3 || reply.TargetServing() != targetServing || !sameBootstrapReadIntent(reply.Intent, intent) {
				t.Fatalf("current placement=%+v serving=%v err=%v", reply.CurrentRoute, reply.TargetServing(), err)
			}
		})
	}
}

func TestBootstrapRecoveryReadRejectsCatalogChangeAndUnavailableAuthority(t *testing.T) {
	intent := testIntent([]byte(`{"schema":"orders"}`), gateway.EnrollmentReserved)
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeJoining, replication.Digest{0xa1})
	request := BootstrapReadRequest{Nonce: [16]byte{1}, Operation: OpReadOwnEnrollmentRecovery, PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: intent.IntentID}
	snapshot := bootstrapRecoveryTestSnapshot(t, intent, true)
	node, cut := bootstrapRecoveryTestDirectory(t, snapshot, node)
	for _, name := range []string{"unsupported", "unavailable", "first scan changed", "second scan changed", "missing physical peer", "group absent"} {
		t.Run(name, func(t *testing.T) {
			base := &bootstrapReadTestAuthority{intent: intent, node: node, cut: cut, evidence: bootstrapReadTestEvidence(node)}
			authority := &bootstrapRecoveryTestAuthority{bootstrapReadTestAuthority: base, snapshot: snapshot, digest: base.evidence.CatalogHeadDigest}
			service := &BootstrapReadService{authority: authority, trustDomain: rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}}
			switch name {
			case "unsupported":
				service.authority = base
			case "unavailable":
				authority.err = errors.New("authority unavailable")
			case "first scan changed":
				base.evidence.CatalogHeadDigest[0]++
			case "second scan changed":
				base.onScan = func(count int, state *bootstrapReadTestAuthority) {
					if count == 2 {
						state.evidence.CatalogHeadDigest[0]++
					}
				}
			case "missing physical peer":
				base.cut.Nodes = base.cut.Nodes[1:]
			case "group absent":
				other := intent
				other.Group.GroupID[0]++
				authority.snapshot = bootstrapRecoveryTestSnapshot(t, other, true)
			}
			reply, err := service.readStable(t.Context(), request, node)
			if name == "group absent" {
				if err != nil || reply.CurrentRoute != nil || reply.TargetServing() {
					t.Fatalf("absent group inferred serving: %+v, %v", reply, err)
				}
			} else if err == nil {
				t.Fatal("unverified recovery cut accepted")
			}
		})
	}
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

func TestBootstrapStableReadAcceptsIndependentlyDecodedPreparedProofs(t *testing.T) {
	for _, phase := range []gateway.EnrollmentState{gateway.EnrollmentPrepared, gateway.EnrollmentEnrolled} {
		intent := testIntent([]byte(`{"schema":"orders"}`), phase)
		node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeActive, replication.Digest{0xa1})
		authority := &bootstrapReadTestAuthority{intent: intent, node: node, cut: bootstrapReadTestCut(node), evidence: bootstrapReadTestEvidence(node)}
		service := &BootstrapReadService{authority: authority, trustDomain: rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}}
		request := BootstrapReadRequest{Nonce: [bootstrapReadNonceBytes]byte{1}, Operation: OpReadOwnEnrollment, PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: intent.IntentID}
		reply, err := service.readStable(t.Context(), request, node)
		if err != nil || !sameBootstrapReadIntent(reply.Intent, intent) {
			t.Fatalf("phase %d stable proof: %v", phase, err)
		}
		altered := reply.Intent
		altered.Proof.ManifestDigest[0] ^= 1
		if sameBootstrapReadIntent(altered, intent) {
			t.Fatal("altered proof accepted")
		}
	}
}

func TestBootstrapReadFailureReplyIsBoundedAndNonceBound(t *testing.T) {
	var wire bytes.Buffer
	nonce := [bootstrapReadNonceBytes]byte{9}
	if err := writeBootstrapReadFailure(&wire, nonce, errors.New("directory changed\nduring scan")); err != nil {
		t.Fatal(err)
	}
	reply, err := ReadBootstrapReadReply(bytes.NewReader(wire.Bytes()))
	if !errors.Is(err, ErrBootstrapRead) || reply.Nonce != nonce || err.Error() != "nodecontrol: invalid bootstrap enrollment read: gateway bootstrap read: directory changed during scan" {
		t.Fatalf("error reply: nonce=%x err=%v", reply.Nonce, err)
	}
	raw := bytes.Clone(wire.Bytes()[:bootstrapReadResponseHeader])
	binary.BigEndian.PutUint32(raw[28:32], maxBootstrapReadErrorBytes+1)
	if _, err := ReadBootstrapReadReply(bytes.NewReader(raw)); !errors.Is(err, ErrBootstrapReadBound) {
		t.Fatalf("unbounded error: %v", err)
	}
}

func TestBootstrapRecoveryMissingIntentRequiresStableAuthoritativeAbsence(t *testing.T) {
	intent := testIntent([]byte(`{"schema":"orders"}`), gateway.EnrollmentComplete)
	intent.MoveOperationID = [32]byte{1}
	node := bootstrapReadTestNode(intent.Target.Node, intent.Target.NodeIncarnation, gateway.NodeActive, replication.Digest{0xa1})
	snapshot := bootstrapRecoveryTestSnapshot(t, intent, false)
	node, cut := bootstrapRecoveryTestDirectory(t, snapshot, node)
	domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
	for _, name := range []string{"stable", "normal read", "unavailable", "mixed error", "reappeared", "second read unavailable", "enrollment changed", "catalog changed"} {
		t.Run(name, func(t *testing.T) {
			base := &bootstrapReadTestAuthority{intent: intent, node: node, cut: cut, evidence: bootstrapReadTestEvidence(node), intentErr: gateway.ErrEnrollmentIntentMissing}
			authority := &bootstrapRecoveryTestAuthority{bootstrapReadTestAuthority: base, snapshot: snapshot, digest: base.evidence.CatalogHeadDigest}
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			service, err := NewBootstrapReadService(BootstrapReadServiceOptions{Authority: authority, TrustDomain: domain,
				Authorize: func(rafttransport.PeerIdentity, gateway.NodeRecord) bool { return true }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "unavailable":
				base.intentErr = ErrBootstrapReadUnavailable
			case "mixed error":
				base.intentErr = errors.Join(gateway.ErrEnrollmentIntentMissing, ErrBootstrapReadUnavailable)
			case "reappeared", "second read unavailable":
				base.onIntentRead = func(count int, state *bootstrapReadTestAuthority) {
					if count == 2 {
						state.intentErr = nil
						if name == "second read unavailable" {
							state.intentErr = ErrBootstrapReadUnavailable
						}
					}
				}
			case "enrollment changed", "catalog changed":
				base.onScan = func(count int, state *bootstrapReadTestAuthority) {
					if count == 2 {
						if name == "enrollment changed" {
							state.evidence.EnrollmentDirectoryDigest[0]++
						} else {
							state.evidence.CatalogHeadDigest[0]++
						}
					}
				}
			}
			seed := BootstrapGatewaySeed{NodeID: rafttransport.NodeID{10}, Incarnation: 1, ControlAddress: "127.0.0.1:9999", SPKIPinDigest: replication.Digest{10}}
			client, err := NewBootstrapReadClient(BootstrapReadClientOptions{Opener: &bootstrapReadTestOpener{service: service, domain: domain, target: node},
				Seeds: []BootstrapGatewaySeed{seed}, TrustDomain: domain, PhysicalNode: node.NodeID, Incarnation: node.Incarnation,
				ReadDeadline: deadline, WriteDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			if name == "normal read" {
				if _, err := client.ReadEnrollmentIntent(t.Context(), intent.IntentID); err == nil {
					t.Fatal("normal enrollment read accepted absence")
				}
				return
			}
			reply, err := client.ReadEnrollmentRecovery(t.Context(), intent.IntentID)
			if name != "stable" {
				if err == nil || reply.EnrollmentMissing() {
					t.Fatalf("unverified absence accepted: %+v, %v", reply, err)
				}
				return
			}
			if err != nil || !reply.EnrollmentMissing() || reply.TargetServing() || reply.IntentID != intent.IntentID || base.intentReads != 2 {
				t.Fatalf("stable absence not authenticated: %+v reads=%d err=%v", reply, base.intentReads, err)
			}
			for _, mutate := range []func(*BootstrapReadReply){
				func(reply *BootstrapReadReply) { reply.CatalogHeadDigest = replication.Digest{} },
				func(reply *BootstrapReadReply) { reply.EnrollmentDirectoryDigest = replication.Digest{} },
				func(reply *BootstrapReadReply) { reply.Operation = OpReadOwnEnrollment },
				func(reply *BootstrapReadReply) { reply.Intent = intent },
				func(reply *BootstrapReadReply) { reply.IntentDigest = intent.Digest() },
			} {
				invalid := reply
				mutate(&invalid)
				if invalid.EnrollmentMissing() {
					t.Fatal("incomplete or conflicting absence witness accepted")
				}
			}
		})
	}
}

func TestBootstrapRecoveryReplyFitsMaximumEscapedEndpointIdentifiers(t *testing.T) {
	endpoint := func(prefix string) distribution.EndpointID {
		return distribution.EndpointID(prefix + strings.Repeat(`"`, gateway.MaxScalingStringBytes-len(prefix)))
	}
	intent := testIntent([]byte(`{"schema":"orders"}`), gateway.EnrollmentComplete)
	intent.MoveOperationID = [32]byte{1}
	intent.Source.Endpoint, intent.Source.NativeEndpoint, intent.Source.ControlEndpoint = endpoint("source-data"), endpoint("source-native"), endpoint("source-control")
	intent.Target.Endpoint, intent.Target.NativeEndpoint, intent.Target.ControlEndpoint = endpoint("target-data"), endpoint("target-native"), endpoint("target-control")
	proof := testProof(intent)
	intent.Proof = &proof
	intent.Receipt.Target = intent.Target
	intent.Receipt.IntentDigest = intent.Digest()
	intent.Receipt.TransitionID = gateway.EnrollmentTransitionDigest(intent)
	if !intent.Valid() {
		t.Fatal("maximal endpoint intent fixture invalid")
	}
	intentBytes, err := vibejson.Marshal(&intent)
	if err != nil || len(intentBytes) > 128<<10 {
		t.Fatalf("intent exceeds durable row bound: %d, %v", len(intentBytes), err)
	}
	var nodes []gateway.NodeRecord
	var members []gateway.ReplicatedEndpoint
	for _, id := range []byte{1, 2, 6, 7} {
		node := bootstrapReadTestNode(rafttransport.NodeID{id}, 1, gateway.NodeActive, replication.Digest{id})
		member := uint64(id)
		store := [16]byte{id}
		if id == intent.Target.Node[0] {
			node.Incarnation, member, store = intent.Target.NodeIncarnation, intent.Target.Member, intent.Target.StoreID
			node.DataEndpoint, node.NativeEndpoint, node.ControlEndpoint = intent.Target.Endpoint, intent.Target.NativeEndpoint, intent.Target.ControlEndpoint
		} else {
			node.DataEndpoint, node.NativeEndpoint, node.ControlEndpoint = endpoint(fmt.Sprintf("%d-data", id)), endpoint(fmt.Sprintf("%d-native", id)), endpoint(fmt.Sprintf("%d-control", id))
		}
		nodeBytes, err := vibejson.Marshal(&node)
		if err != nil || !node.Valid() || len(nodeBytes) > 32<<10 {
			t.Fatalf("node exceeds durable row bound: %d, %v", len(nodeBytes), err)
		}
		nodes = append(nodes, node)
		members = append(members, gateway.ReplicatedEndpoint{Member: member, Node: node.NodeID, NodeIncarnation: node.Incarnation, StoreID: store,
			Endpoint: string(node.DataEndpoint), NativeEndpoint: string(node.NativeEndpoint), ControlEndpoint: string(node.ControlEndpoint),
			DataAddress: node.DataAddress, Address: node.NativeAddress, ControlAddress: node.ControlAddress})
	}
	reply := bootstrapReadTestReply(intent, nodes[2])
	reply.Operation = OpReadOwnEnrollmentRecovery
	reply.CurrentNodes = nodes
	reply.CurrentRoute = &gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{Group: intent.Group, Distribution: intent.Distribution,
		Shard: intent.Shard, AllocationGeneration: uint64(intent.AllocationGeneration), Command: intent.ExpectedCommand, Replicas: members[:3]},
		HasEnrolledTarget: true, EnrolledTarget: members[3]}
	raw, err := AppendBootstrapReadReply(nil, reply)
	if err != nil || len(raw) <= 128<<10 || len(raw) > MaxBootstrapReadReplyBytes+bootstrapReadResponseHeader {
		t.Fatalf("bounded valid recovery reply: %d bytes, %v", len(raw), err)
	}
	opened, err := ReadBootstrapReadReply(bytes.NewReader(raw))
	if err != nil || !opened.TargetServing() || len(opened.CurrentNodes) != 4 {
		t.Fatalf("maximal escaped identifiers did not round trip: %v", err)
	}
	// Per-group catalog handles may alias the same physical-node endpoints.
	// Exercise six-byte JSON escaping on maximal aliases independently from
	// the physical row's identifiers, keeping every durable node under 32 KiB.
	for index := range reply.CurrentNodes {
		node := &reply.CurrentNodes[index]
		node.DataEndpoint, node.NativeEndpoint, node.ControlEndpoint = distribution.EndpointID(fmt.Sprintf("physical-%d-data", index)), distribution.EndpointID(fmt.Sprintf("physical-%d-native", index)), distribution.EndpointID(fmt.Sprintf("physical-%d-control", index))
		node.FailureDomain = strings.Repeat("\x01", gateway.MaxScalingStringBytes)
		nodeBytes, err := vibejson.Marshal(node)
		if err != nil || !node.Valid() || len(nodeBytes) > 32<<10 {
			t.Fatalf("escaped node exceeds durable row bound: %d, %v", len(nodeBytes), err)
		}
		if index == 2 {
			reply.Node = *node
			continue
		}
		member := &reply.CurrentRoute.EnrolledTarget
		if index < gateway.ServingReplicaCount {
			member = &reply.CurrentRoute.Serving.Replicas[index]
		}
		alias := func(label string) string {
			return label + strings.Repeat("\x01", gateway.MaxScalingStringBytes-len(label))
		}
		member.Endpoint, member.NativeEndpoint, member.ControlEndpoint = alias(fmt.Sprintf("%d-data", index)), alias(fmt.Sprintf("%d-native", index)), alias(fmt.Sprintf("%d-control", index))
	}
	raw, err = AppendBootstrapReadReply(nil, reply)
	if err != nil {
		t.Fatalf("valid independently escaped catalog aliases rejected: %v", err)
	}
	opened, err = ReadBootstrapReadReply(bytes.NewReader(raw))
	if err != nil || !opened.TargetServing() {
		t.Fatalf("catalog aliases lost physical identity binding: %v", err)
	}
	header := bytes.Clone(raw[:bootstrapReadResponseHeader])
	binary.BigEndian.PutUint32(header[28:32], MaxBootstrapReadReplyBytes+1)
	if _, err := ReadBootstrapReadReply(bytes.NewReader(header)); !errors.Is(err, ErrBootstrapRead) {
		t.Fatalf("oversized reply reached payload read: %v", err)
	}
}
