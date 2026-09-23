package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func preparedAckRosterNode(id byte, incarnation uint64, lifecycle gateway.NodeLifecycle, roles gateway.NodeRole) gateway.NodeRecord {
	name := fmt.Sprintf("node-%d", id)
	node := gateway.NodeRecord{
		NodeID:            rafttransport.NodeID{id},
		Incarnation:       incarnation,
		ServiceKeyDigest:  replication.Digest{100 + id},
		DataEndpoint:      gatewayEndpointID(name + "-data"),
		NativeEndpoint:    gatewayEndpointID(name + "-native"),
		ControlEndpoint:   gatewayEndpointID(name + "-control"),
		DataAddress:       name + ":7000",
		NativeAddress:     name + ":7100",
		ControlAddress:    name + ":7200",
		FailureDomain:     "prepared-ack-test",
		Roles:             roles,
		Lifecycle:         lifecycle,
		Revision:          uint64(id) + 10,
		CatalogGeneration: 1,
	}
	if roles&gateway.NodeRoleGateway != 0 {
		node.GatewayEndpoint = gatewayEndpointID(name + "-gateway")
		node.GatewayAddress = name + ":7300"
		node.Gateway = gateway.GatewayIdentity{
			NodeID:            rafttransport.NodeID{200 + id},
			Incarnation:       1,
			ServiceKeyDigest:  replication.Digest{150 + id},
			ServiceID:         [16]byte{160 + id},
			SessionID:         [16]byte{170 + id},
			SessionRevision:   1,
			ParticipantDigest: replication.Digest{180 + id},
		}
	}
	return node
}

func gatewayEndpointID(value string) distribution.EndpointID { return distribution.EndpointID(value) }

type preparedAckPhysicalOpenerTest struct {
	connection rafttransport.PeerConnection
}

func (opener preparedAckPhysicalOpenerTest) OpenShardControlEndpoint(
	context.Context, gateway.ReplicatedEndpoint,
) (rafttransport.PeerConnection, error) {
	return opener.connection, nil
}

type preparedAckSequenceOpenerTest struct {
	mu          sync.Mutex
	connections []rafttransport.PeerConnection
	calls       int
}

func (opener *preparedAckSequenceOpenerTest) OpenShardControlEndpoint(
	context.Context, gateway.ReplicatedEndpoint,
) (rafttransport.PeerConnection, error) {
	opener.mu.Lock()
	defer opener.mu.Unlock()
	index := opener.calls
	opener.calls++
	if index >= len(opener.connections) || opener.connections[index] == nil {
		return nil, errors.New("prepared acknowledgement connection unavailable")
	}
	return opener.connections[index], nil
}

type preparedAckGateInstallerTest struct {
	applied     uint64
	coordinates frontenddrain.PreparedAckCutReadFloor
	set         bool
}

func (installer *preparedAckGateInstallerTest) InstallFrontendDrainServiceCut(
	_ context.Context, cut frontenddrain.PreparedAckCut,
) (uint64, error) {
	if !cut.Valid() {
		return 0, errors.New("invalid prepared-ack service cut")
	}
	floor := cut.ReadFloor()
	if !floor.Valid() || floor == (frontenddrain.PreparedAckCutReadFloor{}) {
		return 0, errors.New("invalid prepared-ack service cut floor")
	}
	if installer.set && !cut.AtLeastFloor(installer.coordinates) {
		return 0, serviceauthz.ErrServiceDirectoryStale
	}
	installer.coordinates = floor
	installer.set = true
	installer.applied = cut.ServiceDirectoryRevision
	return installer.applied, nil
}

func (installer *preparedAckGateInstallerTest) ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool) {
	if installer == nil {
		return frontenddrain.PreparedAckCutReadFloor{}, false
	}
	return installer.coordinates, installer.set
}

func startPreparedAckWireService(
	t testing.TB, ctx context.Context, profile *rafttransport.PeerTLS, node gateway.NodeRecord,
	request frontenddrain.PreparedAckRequest, installer *preparedAckGateInstallerTest,
) (*preparedAckWirePeer, net.Conn, <-chan error) {
	t.Helper()
	clientRaw, serverRaw := net.Pipe()
	client := &preparedAckWirePeer{Conn: clientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	server := &preparedAckWirePeer{Conn: serverRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.Gateway.NodeID},
		key:  [32]byte(node.Gateway.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: preparedAckServiceReaderTest{}, Installer: installer, TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer.Node == request.SourcePrincipal && got.SourcePrincipal == peer.Node &&
				got.SourcePrincipalKeyDigest == request.SourcePrincipalKeyDigest
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- service.Serve(ctx, server) }()
	return client, serverRaw, serverErr
}

func TestFrontendDrainPreparedAckServingReceiversUsesServingRoster(t *testing.T) {
	snapshot := catalogRouteSeedSnapshot(t, 1, "node-1:7100")
	cutNodes := []gateway.NodeRecord{
		preparedAckRosterNode(1, 21, gateway.NodeActive, gateway.NodeRoleStorage|gateway.NodeRoleGateway),
		preparedAckRosterNode(2, 22, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(3, 23, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(4, 24, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(5, 25, gateway.NodeJoining, gateway.NodeRoleStorage),
	}
	// Match the three catalog route replicas exactly. The fourth active
	// storage node is joined into the serving service directory but is not a
	// current route replica; the Joining node is deliberately non-serving.
	cutNodes[0].DataEndpoint, cutNodes[0].NativeEndpoint, cutNodes[0].ControlEndpoint = "one", "one-native", "one-control"
	cutNodes[0].DataAddress, cutNodes[0].NativeAddress, cutNodes[0].ControlAddress = "127.0.0.1:7001", "node-1:7100", "127.0.0.1:7201"
	cutNodes[0].DataEndpoint, cutNodes[0].NativeEndpoint, cutNodes[0].ControlEndpoint = "physical-one-data", "physical-one-native", "physical-one-control"
	cutNodes[1].DataEndpoint, cutNodes[1].NativeEndpoint, cutNodes[1].ControlEndpoint = "two", "two-native", "two-control"
	cutNodes[1].DataAddress, cutNodes[1].NativeAddress, cutNodes[1].ControlAddress = "127.0.0.1:7002", "127.0.0.1:7102", "127.0.0.1:7202"
	cutNodes[1].DataEndpoint, cutNodes[1].NativeEndpoint, cutNodes[1].ControlEndpoint = "physical-two-data", "physical-two-native", "physical-two-control"
	// A route replica that is already Draining remains a mandatory native
	// receiver until its routes leave the catalog. This is the lifecycle
	// barrier case that cannot be replaced by the survivor gateway alone.
	cutNodes[1].Lifecycle = gateway.NodeDraining
	cutNodes[2].DataEndpoint, cutNodes[2].NativeEndpoint, cutNodes[2].ControlEndpoint = "three", "three-native", "three-control"
	cutNodes[2].DataAddress, cutNodes[2].NativeAddress, cutNodes[2].ControlAddress = "127.0.0.1:7003", "127.0.0.1:7103", "127.0.0.1:7203"
	cutNodes[2].DataEndpoint, cutNodes[2].NativeEndpoint, cutNodes[2].ControlEndpoint = "physical-three-data", "physical-three-native", "physical-three-control"
	for index := range cutNodes {
		if !cutNodes[index].Valid() {
			t.Fatalf("node %d invalid: %+v", index, cutNodes[index])
		}
	}
	nodeCut := gateway.NodeDirectoryCut{
		Revision: 9, Digest: replication.Digest{90}, CatalogGeneration: 1, Nodes: cutNodes,
	}
	if !nodeCut.Valid() {
		t.Fatal("invalid receiver directory cut")
	}
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	serviceCut := serviceauthz.ServiceDirectoryCut{
		Revision: 5, CatalogGeneration: 1, TrustDomain: trust, PolicyGeneration: 1,
		Bindings: []serviceauthz.ServiceBinding{
			{Principal: cutNodes[3].NodeID, PhysicalNode: cutNodes[3].NodeID, PhysicalIncarnation: cutNodes[3].Incarnation,
				KeyDigest: [32]byte(cutNodes[3].ServiceKeyDigest), Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive},
			{Principal: cutNodes[4].NodeID, PhysicalNode: cutNodes[4].NodeID, PhysicalIncarnation: cutNodes[4].Incarnation,
				KeyDigest: [32]byte(cutNodes[4].ServiceKeyDigest), Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceJoining},
		},
	}
	if !serviceCut.Valid() {
		t.Fatal("invalid service directory cut")
	}
	runtime := &Runtime{holder: gateway.NewCatalogHolder(snapshot)}
	receivers, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(nodeCut, snapshot, &serviceCut)
	if err != nil {
		t.Fatalf("serving receiver projection: %v", err)
	}
	if len(receivers) != 4 {
		t.Fatalf("receiver count=%d, want route RF3 plus active storage-only node", len(receivers))
	}
	for index, receiver := range receivers {
		want := rafttransport.NodeID{byte(index + 1)}
		if receiver.node.NodeID != want {
			t.Fatalf("receiver[%d]=%s, want node %d", index, receiver.node.NodeID, index+1)
		}
		if receiver.endpoint.Node != receiver.node.NodeID || receiver.endpoint.NodeIncarnation != receiver.node.Incarnation ||
			receiver.endpoint.Endpoint != string(receiver.node.DataEndpoint) || receiver.endpoint.DataAddress != receiver.node.DataAddress ||
			receiver.endpoint.NativeEndpoint != string(receiver.node.NativeEndpoint) || receiver.endpoint.Address != receiver.node.NativeAddress ||
			receiver.endpoint.ControlEndpoint != string(receiver.node.ControlEndpoint) || receiver.endpoint.ControlAddress != receiver.node.ControlAddress {
			t.Fatalf("receiver[%d] endpoint=%+v was not projected solely from NodeRecord=%+v", index, receiver.endpoint, receiver.node)
		}
	}

	published, err := runtime.frontendDrainPreparedAckPublicationReceiversFromServiceCut(nodeCut, snapshot, &serviceCut)
	if err != nil {
		t.Fatalf("publication receiver projection: %v", err)
	}
	if len(published) != 5 {
		t.Fatalf("publication receiver count=%d, want serving roster plus joining storage", len(published))
	}
	if published[4].node.NodeID != (rafttransport.NodeID{5}) || published[4].node.Lifecycle != gateway.NodeJoining {
		t.Fatalf("publication receivers=%+v, want joining node 5 last", published)
	}
}

func preparedAckAliasRouteSnapshot(t *testing.T, conflicting bool, reverse bool) *gateway.Snapshot {
	t.Helper()
	const (
		firstDistribution  = distribution.DistributionName("alias-catalog")
		secondDistribution = distribution.DistributionName("alias-ledger")
	)
	dataAddresses := []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}
	nativeAddresses := []string{"127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"}
	controlAddresses := []string{"127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"}
	endpoints := make(map[distribution.EndpointID]string, 18)
	makeNames := func(prefix string) (data, native, control []distribution.EndpointID) {
		data = make([]distribution.EndpointID, 3)
		native = make([]distribution.EndpointID, 3)
		control = make([]distribution.EndpointID, 3)
		for index := range data {
			data[index] = distribution.EndpointID(fmt.Sprintf("%s-node-%d-data", prefix, index+1))
			native[index] = distribution.EndpointID(fmt.Sprintf("%s-node-%d-native", prefix, index+1))
			control[index] = distribution.EndpointID(fmt.Sprintf("%s-node-%d-control", prefix, index+1))
			endpoints[data[index]] = dataAddresses[index]
			endpoints[native[index]] = nativeAddresses[index]
			endpoints[control[index]] = controlAddresses[index]
		}
		return data, native, control
	}
	firstData, firstNative, firstControl := makeNames("catalog")
	secondData, secondNative, secondControl := makeNames("ledger")
	if conflicting {
		endpoints[secondData[0]] = "127.0.0.1:7991"
	}
	firstManifest, err := distribution.NewManifest(firstDistribution, 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: firstData, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	secondManifest, err := distribution.NewManifest(secondDistribution, 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: secondData, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	makeGroup := func(seed byte) raftmember.GroupKey {
		return raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
			TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{seed}, GroupID: [16]byte{seed + 10}}
	}
	makeDescriptor := func(name distribution.DistributionName, group raftmember.GroupKey, data, native, control []distribution.EndpointID, seed byte) gateway.ReplicatedShardDescriptor {
		replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
		for index := range replicas {
			member := uint64(index + 1)
			replicas[index] = gateway.ReplicatedReplicaDescriptor{
				Member: member, Node: rafttransport.NodeID{byte(member)}, StoreID: [16]byte{byte(member + 30)},
				NodeIncarnation: member + 20, Endpoint: data[index], NativeEndpoint: native[index], ControlEndpoint: control[index],
			}
		}
		return gateway.ReplicatedShardDescriptor{Distribution: name, Shard: "all", Group: group,
			AllocationGeneration: 1, Command: raftservice.CommandFence{ReplicaSetVersion: 1,
				ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
				RelationManifestDigest: replication.Digest{seed}, RoutingVersion: 1, RouteGeneration: 1},
			LogicalSchemaDigest: replication.Digest{seed + 1}, RangeIdentity: replication.Digest{seed + 2},
			LineageDigest: replication.Digest{seed + 3}, ForwardingRuleDigest: replication.Digest{seed + 4}, Replicas: replicas}
	}
	firstDescriptor := makeDescriptor(firstDistribution, makeGroup(3), firstData, firstNative, firstControl, 3)
	secondDescriptor := makeDescriptor(secondDistribution, makeGroup(4), secondData, secondNative, secondControl, 4)
	distributions := []distribution.DistributionSpec{
		{Name: firstDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
		{Name: secondDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
	}
	manifests := []*distribution.Manifest{firstManifest, secondManifest}
	descriptors := []gateway.ReplicatedShardDescriptor{firstDescriptor, secondDescriptor}
	if reverse {
		manifests[0], manifests[1] = manifests[1], manifests[0]
		descriptors[0], descriptors[1] = descriptors[1], descriptors[0]
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: distributions, Manifests: manifests,
	}, endpoints, 1, nil, nil, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func preparedAckAliasRouteNodes() []gateway.NodeRecord {
	nodes := make([]gateway.NodeRecord, 3)
	for index := range nodes {
		nodes[index] = preparedAckRosterNode(byte(index+1), uint64(index+21), gateway.NodeActive, gateway.NodeRoleStorage)
		nodes[index].DataEndpoint = gatewayEndpointID(fmt.Sprintf("physical-node-%d-data", index+1))
		nodes[index].NativeEndpoint = gatewayEndpointID(fmt.Sprintf("physical-node-%d-native", index+1))
		nodes[index].ControlEndpoint = gatewayEndpointID(fmt.Sprintf("physical-node-%d-control", index+1))
		nodes[index].DataAddress = fmt.Sprintf("127.0.0.1:700%d", index+1)
		nodes[index].NativeAddress = fmt.Sprintf("127.0.0.1:710%d", index+1)
		nodes[index].ControlAddress = fmt.Sprintf("127.0.0.1:720%d", index+1)
	}
	return nodes
}

func preparedAckAliasRouteCut(nodes []gateway.NodeRecord) gateway.NodeDirectoryCut {
	return gateway.NodeDirectoryCut{Revision: 40, Digest: replication.Digest{40}, CatalogGeneration: 1, Nodes: nodes}
}

func TestFrontendDrainPreparedAckServingReceiversAcceptsResolvedHandleAliasesAndOrder(t *testing.T) {
	nodes := preparedAckAliasRouteNodes()
	nodeCut := preparedAckAliasRouteCut(nodes)
	if !nodeCut.Valid() {
		t.Fatal("invalid alias receiver directory cut")
	}
	first := preparedAckAliasRouteSnapshot(t, false, false)
	second := preparedAckAliasRouteSnapshot(t, false, true)
	runtime := &Runtime{}
	got, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(nodeCut, first, nil)
	if err != nil {
		t.Fatalf("alias receiver projection: %v", err)
	}
	want, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(nodeCut, second, nil)
	if err != nil {
		t.Fatalf("reordered alias receiver projection: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("receiver projection changed with route order:\nfirst=%+v\nsecond=%+v", got, want)
	}
	if len(got) != len(nodes) {
		t.Fatalf("receiver count=%d, want %d physical nodes", len(got), len(nodes))
	}
	byNode := make(map[rafttransport.NodeID]gateway.NodeRecord, len(nodes))
	for _, node := range nodes {
		byNode[node.NodeID] = node
	}
	for _, receiver := range got {
		node := byNode[receiver.node.NodeID]
		if receiver.endpoint.Endpoint != string(node.DataEndpoint) || receiver.endpoint.DataAddress != node.DataAddress ||
			receiver.endpoint.NativeEndpoint != string(node.NativeEndpoint) || receiver.endpoint.Address != node.NativeAddress ||
			receiver.endpoint.ControlEndpoint != string(node.ControlEndpoint) || receiver.endpoint.ControlAddress != node.ControlAddress {
			t.Fatalf("receiver endpoint=%+v does not come solely from physical NodeRecord=%+v", receiver.endpoint, node)
		}
	}
}

func TestFrontendDrainPreparedAckServingReceiversRejectsPhysicalAddressAndIdentityConflicts(t *testing.T) {
	baseNodes := preparedAckAliasRouteNodes()
	baseSnapshot := preparedAckAliasRouteSnapshot(t, false, false)
	baseCut := preparedAckAliasRouteCut(baseNodes)
	runtime := &Runtime{}
	if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(baseCut, baseSnapshot, nil); err != nil {
		t.Fatalf("baseline alias projection: %v", err)
	}
	for _, test := range []struct {
		name     string
		edit     func([]gateway.NodeRecord)
		dropLast bool
	}{
		{name: "data address", edit: func(nodes []gateway.NodeRecord) { nodes[0].DataAddress = "127.0.0.1:7991" }},
		{name: "native address", edit: func(nodes []gateway.NodeRecord) { nodes[0].NativeAddress = "127.0.0.1:7992" }},
		{name: "control address", edit: func(nodes []gateway.NodeRecord) { nodes[0].ControlAddress = "127.0.0.1:7993" }},
		{name: "missing node", dropLast: true},
		{name: "wrong physical incarnation", edit: func(nodes []gateway.NodeRecord) { nodes[2].Incarnation = 99 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			nodes := append([]gateway.NodeRecord(nil), baseNodes...)
			if test.dropLast {
				nodes = nodes[:len(nodes)-1]
			} else {
				test.edit(nodes)
			}
			cut := preparedAckAliasRouteCut(nodes)
			if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(cut, baseSnapshot, nil); !errors.Is(err, gateway.ErrScalingRevision) {
				t.Fatalf("conflict accepted: %v", err)
			}
		})
	}
	conflicting := preparedAckAliasRouteSnapshot(t, true, false)
	if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(baseCut, conflicting, nil); !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("second same-identity route with conflicting resolved address accepted: %v", err)
	}
}

type preparedAckServiceReaderTest struct{}

func (preparedAckServiceReaderTest) ReadFrontendDrainPreparedAckCut(
	_ context.Context, request frontenddrain.PreparedAckRequest,
) (frontenddrain.PreparedAckCut, error) {
	return request.SourceCut, nil
}

type preparedAckWirePeer struct {
	net.Conn
	peer  rafttransport.PeerIdentity
	key   [32]byte
	class rafttransport.TrafficClass
}

func (peer *preparedAckWirePeer) PeerIdentity() rafttransport.PeerIdentity { return peer.peer }
func (peer *preparedAckWirePeer) PeerKeyDigest() [32]byte                  { return peer.key }
func (peer *preparedAckWirePeer) TrafficClass() rafttransport.TrafficClass { return peer.class }

func TestFrontendDrainPreparedAckPhysicalReceiverUsesRealShardWire(t *testing.T) {
	profile, node, _, sourceCut, _ := frontendDrainSourceTestFixture(t)
	grant := sourceCut.ServiceDirectory.ContinuationGrants[0]
	request := frontenddrain.PreparedAckRequest{
		Nonce: [16]byte{40}, DrainID: grant.DrainID, GrantDigest: grant.GrantDigest,
		SourcePrincipal: node.Gateway.NodeID, SourcePrincipalKeyDigest: [32]byte(node.Gateway.ServiceKeyDigest),
		ReceiverNode: node.NodeID, ReceiverIncarnation: node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(node.ServiceKeyDigest), ReceiverNodeRevision: node.Revision,
		SourceCut: sourceCut,
	}
	if !request.Valid() {
		t.Fatal("invalid prepared acknowledgement request")
	}
	clientRaw, serverRaw := net.Pipe()
	client := &preparedAckWirePeer{Conn: clientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	server := &preparedAckWirePeer{Conn: serverRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.Gateway.NodeID},
		key:  [32]byte(node.Gateway.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	installer := new(preparedAckGateInstallerTest)
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: preparedAckServiceReaderTest{}, Installer: installer,
		TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer.Node == request.SourcePrincipal && got.SourcePrincipal == peer.Node &&
				got.SourcePrincipalKeyDigest == request.SourcePrincipalKeyDigest
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- service.Serve(t.Context(), server) }()
	runtime := &Runtime{
		config:                    Config{TLSProfile: profile},
		preparedAckPhysicalOpener: preparedAckPhysicalOpenerTest{connection: client},
		controlReadDeadline:       func() time.Time { return time.Now().Add(time.Second) },
		controlWriteDeadline:      func() time.Time { return time.Now().Add(time.Second) },
	}
	receiver := frontendDrainPreparedAckReceiver{node: node, endpoint: gateway.ReplicatedEndpoint{
		Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
	}}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("physical prepared acknowledgement: %v", err)
	}
	_ = clientRaw.Close()
	_ = serverRaw.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("real shard-control service: %v", err)
	}
	if installer.applied != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("installed revision=%d, want %d", installer.applied, request.SourceCut.ServiceDirectoryRevision)
	}
}

// canonicalPreparedAckSourceReaderTest drives the actual authority-backed
// source-cut service before the real shard ACK service installs the returned
// cut. It deliberately does not echo request.SourceCut: the source endpoint
// reconstructs the canonical projection from its own runtime cut and binds it
// to the authenticated physical receiver.
type canonicalPreparedAckSourceReaderTest struct {
	profile *rafttransport.PeerTLS
	node    gateway.NodeRecord
	source  gateway.FrontendDrainRuntimeCut
	calls   int
}

func (reader *canonicalPreparedAckSourceReaderTest) ReadFrontendDrainPreparedAckCut(
	ctx context.Context, request frontenddrain.PreparedAckRequest,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || reader.profile == nil {
		return frontenddrain.PreparedAckCut{}, errors.New("canonical source reader unavailable")
	}
	reader.calls++
	query := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationInstallExact, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest, ReceiverNodeRevision: request.ReceiverNodeRevision,
		SourceFloor: request.SourceCut.ReadFloor(), SourceCutDigest: request.SourceCut.Digest(),
	}
	raw := query.Marshal()
	connection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(raw),
		peer:  rafttransport.PeerIdentity{TrustDomain: reader.profile.LocalIdentity().TrustDomain, Node: reader.node.NodeID},
		key:   [32]byte(reader.node.ServiceKeyDigest), class: rafttransport.TrafficGatewayControl,
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	authorize := func(peer rafttransport.PeerConnection) bool {
		return peer.TrafficClass() == rafttransport.TrafficGatewayControl &&
			peer.PeerIdentity().TrustDomain == reader.profile.LocalIdentity().TrustDomain &&
			peer.PeerIdentity().Node == reader.node.NodeID &&
			peer.PeerKeyDigest() == [32]byte(reader.node.ServiceKeyDigest)
	}
	readNode := func(_ context.Context, node rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
		if node != reader.node.NodeID || incarnation != reader.node.Incarnation {
			return gateway.NodeRecord{}, gateway.ErrScalingIdentity
		}
		return reader.node, nil
	}
	readCut := func(context.Context) (gateway.FrontendDrainRuntimeCut, error) { return reader.source, nil }
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(
		ctx, connection, authorize, readNode, readCut, reader.profile, 1, deadline, deadline,
	); err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	response, err := frontenddrain.OpenPreparedAckCutReadResponse(connection.output.Bytes(), query)
	if err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	return response.Cut, nil
}

func TestFrontendDrainPreparedAckPhysicalReceiverUsesCanonicalSourceService(t *testing.T) {
	profile, node, source, sourceCut, _ := frontendDrainSourceTestFixture(t)
	source.Catalog = catalogRouteSeedSnapshot(t, 1, node.NativeAddress, node)
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(t.Context(), source, profile, 1)
	if err != nil {
		t.Fatalf("project canonical source service cut: %v", err)
	}
	canonicalCut, err := frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
	if err != nil {
		t.Fatalf("project canonical source cut: %v", err)
	}
	if canonicalCut.Digest() == sourceCut.Digest() {
		t.Fatal("canonical source fixture did not change after catalog placement")
	}
	grant := serviceCut.ContinuationGrants[0]
	request := frontenddrain.PreparedAckRequest{
		Nonce: [16]byte{66}, DrainID: grant.DrainID, GrantDigest: grant.GrantDigest,
		SourcePrincipal: profile.LocalIdentity().Node, SourcePrincipalKeyDigest: profile.LocalServiceKeyDigest(),
		ReceiverNode: node.NodeID, ReceiverIncarnation: node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(node.ServiceKeyDigest), ReceiverNodeRevision: node.Revision,
		RequirePrepared: true, SourceCut: canonicalCut,
	}
	if !request.Valid() {
		t.Fatal("invalid canonical source request")
	}
	reader := &canonicalPreparedAckSourceReaderTest{profile: profile, node: node, source: source}
	clientRaw, serverRaw := net.Pipe()
	client := &preparedAckWirePeer{Conn: clientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	server := &preparedAckWirePeer{Conn: serverRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: profile.LocalIdentity().Node},
		key:  profile.LocalServiceKeyDigest(), class: rafttransport.TrafficShardControl}
	installer := new(preparedAckGateInstallerTest)
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: reader, Installer: installer, TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer.Node == request.SourcePrincipal &&
				got.SourcePrincipal == peer.Node && got.SourcePrincipalKeyDigest == request.SourcePrincipalKeyDigest
		},
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- service.Serve(t.Context(), server) }()
	runtime := &Runtime{
		config: Config{TLSProfile: profile}, preparedAckPhysicalOpener: preparedAckPhysicalOpenerTest{connection: client},
		controlReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		controlWriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	}
	receiver := frontendDrainPreparedAckReceiver{node: node, endpoint: gateway.ReplicatedEndpoint{
		Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
	}}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("canonical source + real ACK service: %v", err)
	}
	_ = clientRaw.Close()
	_ = serverRaw.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("canonical ACK service: %v", err)
	}
	if reader.calls != 1 || installer.applied != canonicalCut.ServiceDirectoryRevision {
		t.Fatalf("canonical source calls=%d installed=%d want calls=1 revision=%d", reader.calls, installer.applied, canonicalCut.ServiceDirectoryRevision)
	}
	coordinates, present := installer.ServiceCutCoordinates()
	if !present || coordinates != canonicalCut.ReadFloor() {
		t.Fatalf("installed floor=%+v present=%t want=%+v", coordinates, present, canonicalCut.ReadFloor())
	}
}

func TestFrontendDrainPreparedAckPhysicalReceiverInstallsGenericNoDrainCut(t *testing.T) {
	profile, node, _, sourceCut, _ := frontendDrainSourceTestFixture(t)
	request := frontenddrain.PreparedAckRequest{
		Nonce:           [16]byte{45},
		SourcePrincipal: node.Gateway.NodeID, SourcePrincipalKeyDigest: [32]byte(node.Gateway.ServiceKeyDigest),
		ReceiverNode: node.NodeID, ReceiverIncarnation: node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(node.ServiceKeyDigest), ReceiverNodeRevision: node.Revision,
		SourceCut: sourceCut,
	}
	if !request.Valid() || request.RequirePrepared || request.DrainID != ([32]byte{}) || request.GrantDigest != ([32]byte{}) {
		t.Fatal("invalid generic no-drain InstallExact request")
	}
	clientRaw, serverRaw := net.Pipe()
	client := &preparedAckWirePeer{Conn: clientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	server := &preparedAckWirePeer{Conn: serverRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.Gateway.NodeID},
		key:  [32]byte(node.Gateway.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	installer := new(preparedAckGateInstallerTest)
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: preparedAckServiceReaderTest{}, Installer: installer,
		TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer.Node == request.SourcePrincipal && got.SourcePrincipal == peer.Node &&
				got.SourcePrincipalKeyDigest == request.SourcePrincipalKeyDigest &&
				got.DrainID == ([32]byte{}) && got.GrantDigest == ([32]byte{}) && !got.RequirePrepared
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- service.Serve(t.Context(), server) }()
	runtime := &Runtime{
		config: Config{TLSProfile: profile}, preparedAckPhysicalOpener: preparedAckPhysicalOpenerTest{connection: client},
		controlReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		controlWriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	}
	receiver := frontendDrainPreparedAckReceiver{node: node, endpoint: gateway.ReplicatedEndpoint{
		Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
	}}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("generic no-drain physical acknowledgement: %v", err)
	}
	_ = clientRaw.Close()
	_ = serverRaw.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("generic no-drain receiver service: %v", err)
	}
	if installer.applied != sourceCut.ServiceDirectoryRevision {
		t.Fatalf("installed revision=%d, want %d", installer.applied, sourceCut.ServiceDirectoryRevision)
	}
}

func TestFrontendDrainPreparedAckRetriesDelayedReceiverAndRejectsRestart(t *testing.T) {
	profile, node, _, sourceCut, _ := frontendDrainSourceTestFixture(t)
	grant := sourceCut.ServiceDirectory.ContinuationGrants[0]
	record := gateway.FrontendDrainRecord{
		DrainID: grant.DrainID, GatewayServiceID: node.Gateway.NodeID,
		PeerKeyDigest: node.Gateway.ServiceKeyDigest, GatewayServiceKeyDigest: node.Gateway.ServiceKeyDigest,
		ContinuationGrant: &grant,
	}
	receiver := frontendDrainPreparedAckReceiver{node: node, endpoint: gateway.ReplicatedEndpoint{
		Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
	}}
	runtime := &Runtime{
		config:               Config{TLSProfile: profile},
		controlReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		controlWriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	}
	request, err := runtime.frontendDrainPreparedAckRequest(t.Context(), node, record, receiver, sourceCut)
	if err != nil {
		t.Fatalf("build prepared acknowledgement request: %v", err)
	}

	// The first stream is deliberately left without a receiver response. The
	// context deadline closes it, modeling a lost/delayed ACK; the next call
	// gets a fresh physical connection and must complete normally.
	firstClientRaw, firstServerRaw := net.Pipe()
	firstClient := &preparedAckWirePeer{Conn: firstClientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	secondClientRaw, secondServerRaw := net.Pipe()
	secondClient := &preparedAckWirePeer{Conn: secondClientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:  [32]byte(node.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	secondServer := &preparedAckWirePeer{Conn: secondServerRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.Gateway.NodeID},
		key:  [32]byte(node.Gateway.ServiceKeyDigest), class: rafttransport.TrafficShardControl}
	installer := new(preparedAckGateInstallerTest)
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: preparedAckServiceReaderTest{}, Installer: installer, TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer.Node == request.SourcePrincipal && got.SourcePrincipal == peer.Node &&
				got.SourcePrincipalKeyDigest == request.SourcePrincipalKeyDigest
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	secondServerErr := make(chan error, 1)
	go func() { secondServerErr <- service.Serve(t.Context(), secondServer) }()
	opener := &preparedAckSequenceOpenerTest{connections: []rafttransport.PeerConnection{firstClient, secondClient}}
	runtime.preparedAckPhysicalOpener = opener
	firstCtx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	firstErr := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(firstCtx, receiver, request)
	cancel()
	_ = firstClientRaw.Close()
	_ = firstServerRaw.Close()
	if firstErr == nil {
		t.Fatal("delayed receiver unexpectedly acknowledged before its deadline")
	}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("retry after delayed receiver: %v", err)
	}
	_ = secondClientRaw.Close()
	_ = secondServerRaw.Close()
	if err := <-secondServerErr; err != nil {
		t.Fatalf("retry receiver service: %v", err)
	}
	if opener.calls != 2 || installer.applied != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("retry calls=%d applied=%d, want calls=2 revision=%d", opener.calls, installer.applied, request.SourceCut.ServiceDirectoryRevision)
	}

	// A restarted process cannot satisfy the old source cut: both its
	// incarnation and service key are outside the active receiver binding.
	restarted := receiver
	restarted.node.Incarnation++
	restarted.node.ServiceKeyDigest[0]++
	if _, err := runtime.frontendDrainPreparedAckRequest(t.Context(), node, record, restarted, sourceCut); err == nil {
		t.Fatal("restarted receiver was accepted by the old source cut")
	}
}

func TestFrontendDrainPreparedAckRejectsRosterAndCatalogChange(t *testing.T) {
	snapshot := catalogRouteSeedSnapshot(t, 1, "node-1:7100")
	nodes := []gateway.NodeRecord{
		preparedAckRosterNode(1, 21, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(2, 22, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(3, 23, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(4, 24, gateway.NodeActive, gateway.NodeRoleStorage),
	}
	nodes[0].DataEndpoint, nodes[0].NativeEndpoint, nodes[0].ControlEndpoint = "one", "one-native", "one-control"
	nodes[0].DataAddress, nodes[0].NativeAddress, nodes[0].ControlAddress = "127.0.0.1:7001", "node-1:7100", "127.0.0.1:7201"
	nodes[1].DataEndpoint, nodes[1].NativeEndpoint, nodes[1].ControlEndpoint = "two", "two-native", "two-control"
	nodes[1].DataAddress, nodes[1].NativeAddress, nodes[1].ControlAddress = "127.0.0.1:7002", "127.0.0.1:7102", "127.0.0.1:7202"
	nodes[2].DataEndpoint, nodes[2].NativeEndpoint, nodes[2].ControlEndpoint = "three", "three-native", "three-control"
	nodes[2].DataAddress, nodes[2].NativeAddress, nodes[2].ControlAddress = "127.0.0.1:7003", "127.0.0.1:7103", "127.0.0.1:7203"
	for index := range nodes {
		if !nodes[index].Valid() {
			t.Fatalf("invalid node %d", index)
		}
	}
	nodeCut := gateway.NodeDirectoryCut{Revision: 9, Digest: replication.Digest{90}, CatalogGeneration: 1, Nodes: nodes}
	if !nodeCut.Valid() {
		t.Fatal("invalid receiver cut")
	}
	serviceCut := serviceauthz.ServiceDirectoryCut{
		Revision: 5, CatalogGeneration: 1,
		TrustDomain: rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}, PolicyGeneration: 1,
		Bindings: []serviceauthz.ServiceBinding{{Principal: nodes[3].NodeID, PhysicalNode: nodes[3].NodeID,
			PhysicalIncarnation: nodes[3].Incarnation, KeyDigest: [32]byte(nodes[3].ServiceKeyDigest),
			Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive}},
	}
	if !serviceCut.Valid() {
		t.Fatal("invalid service cut")
	}
	runtime := &Runtime{holder: gateway.NewCatalogHolder(snapshot)}
	if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(nodeCut, snapshot, &serviceCut); err != nil {
		t.Fatalf("baseline receiver projection: %v", err)
	}
	changedRoster := nodeCut
	changedRoster.Nodes = append([]gateway.NodeRecord(nil), nodeCut.Nodes...)
	changedRoster.Nodes[3].Incarnation++
	changedRoster.Nodes[3].ServiceKeyDigest[0]++
	if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(changedRoster, snapshot, &serviceCut); !errors.Is(err, gateway.ErrScalingIdentity) {
		t.Fatalf("changed receiver roster error=%v, want identity fence", err)
	}
	changedCatalog := nodeCut
	changedCatalog.CatalogGeneration++
	if _, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(changedCatalog, snapshot, &serviceCut); !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("changed catalog generation error=%v, want revision fence", err)
	}
}

func TestFrontendDrainPreparedAckSurvivesSameIdentityReceiverRestart(t *testing.T) {
	profile, node, _, sourceCut, _ := frontendDrainSourceTestFixture(t)
	grant := sourceCut.ServiceDirectory.ContinuationGrants[0]
	request := frontenddrain.PreparedAckRequest{
		Nonce: [16]byte{44}, DrainID: grant.DrainID, GrantDigest: grant.GrantDigest,
		SourcePrincipal: node.Gateway.NodeID, SourcePrincipalKeyDigest: [32]byte(node.Gateway.ServiceKeyDigest),
		ReceiverNode: node.NodeID, ReceiverIncarnation: node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(node.ServiceKeyDigest), ReceiverNodeRevision: node.Revision,
		SourceCut: sourceCut,
	}
	if !request.Valid() {
		t.Fatal("invalid prepared acknowledgement request")
	}
	firstInstaller := new(preparedAckGateInstallerTest)
	firstClient, firstServer, firstErr := startPreparedAckWireService(t, t.Context(), profile, node, request, firstInstaller)
	secondInstaller := new(preparedAckGateInstallerTest)
	secondClient, secondServer, secondErr := startPreparedAckWireService(t, t.Context(), profile, node, request, secondInstaller)
	opener := &preparedAckSequenceOpenerTest{connections: []rafttransport.PeerConnection{firstClient, secondClient}}
	runtime := &Runtime{
		config:                    Config{TLSProfile: profile},
		preparedAckPhysicalOpener: opener,
		controlReadDeadline:       func() time.Time { return time.Now().Add(time.Second) },
		controlWriteDeadline:      func() time.Time { return time.Now().Add(time.Second) },
	}
	receiver := frontendDrainPreparedAckReceiver{node: node, endpoint: gateway.ReplicatedEndpoint{
		Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
	}}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("first receiver acknowledgement: %v", err)
	}
	_ = firstClient.Close()
	_ = firstServer.Close()
	if err := <-firstErr; err != nil {
		t.Fatalf("first receiver service: %v", err)
	}
	if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(t.Context(), receiver, request); err != nil {
		t.Fatalf("same-identity restart acknowledgement: %v", err)
	}
	_ = secondClient.Close()
	_ = secondServer.Close()
	if err := <-secondErr; err != nil {
		t.Fatalf("restarted receiver service: %v", err)
	}
	if opener.calls != 2 || firstInstaller.applied != request.SourceCut.ServiceDirectoryRevision ||
		secondInstaller.applied != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("restart calls=%d revisions=%d/%d, want calls=2 revision=%d", opener.calls,
			firstInstaller.applied, secondInstaller.applied, request.SourceCut.ServiceDirectoryRevision)
	}
}
