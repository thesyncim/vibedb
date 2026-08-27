package snapshottransfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestGroupDataRegistryRoutesExactGroupAndRejectsCrossGroupArtifact(t *testing.T) {
	payload1 := bytes.Repeat([]byte("group-one"), 700)
	payload2 := bytes.Repeat([]byte("group-two"), 700)
	d1 := testDescriptor(payload1)
	d2 := testDescriptor(payload2)
	d2.Group.GroupID[0]++
	d2.Group.ShardIncarnation[0]++
	d2.TargetStore[0]++
	d2.TargetIncarnation++

	registry, source, target1, target2 := twoGroupRegistry(t, d1, d2)
	repo1 := openTestRepository(t, filepath.Join(t.TempDir(), "one"))
	repo2 := openTestRepository(t, filepath.Join(t.TempDir(), "two"))
	appendAll(t, repo1, d1, payload1, 0)
	appendAll(t, repo2, d2, payload2, 0)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service := func(repository *Repository, descriptor Descriptor) *Service {
		result, err := NewService(ServiceOptions{
			Repository: repository, Registry: registry,
			Authorize:    func(got Descriptor) bool { return got == descriptor },
			ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
			MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes,
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	router, err := NewGroupDataRegistry(GroupDataRegistryOptions{
		Registry: registry, ReadDeadline: deadline, MaxConnections: 2,
		MaxInflightBytes: 2 * MinChunkBytes,
		Services:         []GroupDataService{{Group: d1.Group, Service: service(repo1, d1)}, {Group: d2.Group, Service: service(repo2, d2)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, serveErr := requestGroupChunk(t, router, target1, d1); serveErr != nil || !bytes.Equal(got, payload1[:MinChunkBytes]) {
		t.Fatalf("group one bytes=%d err=%v", len(got), serveErr)
	}
	if got, serveErr := requestGroupChunk(t, router, target2, d2); serveErr != nil || !bytes.Equal(got, payload2[:MinChunkBytes]) {
		t.Fatalf("group two bytes=%d err=%v", len(got), serveErr)
	}

	// Selecting group two while naming group one's artifact must never make
	// group one's repository reachable through the shared listener.
	forged := d1
	forged.Group = d2.Group
	forged.TargetMember = d2.TargetMember
	forged.TargetStore = d2.TargetStore
	forged.TargetIncarnation = d2.TargetIncarnation
	if _, serveErr := requestGroupChunk(t, router, target2, forged); !errors.Is(serveErr, ErrStaleFence) {
		t.Fatalf("cross-group artifact err=%v", serveErr)
	}
	if _, serveErr := requestGroupChunk(t, router, target2, d1); !errors.Is(serveErr, ErrStaleFence) {
		t.Fatalf("wrong target incarnation err=%v", serveErr)
	}
	_ = source
}

func TestGroupSourceControlRegistryRoutesExactGroup(t *testing.T) {
	request1, descriptor1 := sourceControlFixture()
	request2 := request1
	request2.Group.GroupID[0]++
	request2.Group.ShardIncarnation[0]++
	request2.Operation[0]++
	request2.Step[0]++
	request2.TargetStore[0]++
	request2.TargetIncarnation++
	descriptor2 := descriptor1
	descriptor2.Group = request2.Group
	descriptor2.TargetStore = request2.TargetStore
	descriptor2.TargetIncarnation = request2.TargetIncarnation

	registry, _, target1, target2 := twoGroupRegistry(t, descriptor1, descriptor2)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	exporter1 := &testSourceExporter{descriptor: descriptor1}
	exporter2 := &testSourceExporter{descriptor: descriptor2}
	newService := func(request SourceControlRequest, exporter *testSourceExporter) *SourceControlService {
		service, err := NewSourceControlService(SourceControlOptions{
			Journal:  &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)},
			Exporter: exporter, Authorize: func(_ rafttransport.PeerIdentity, got SourceControlRequest) bool { return got == request },
			ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	router, err := NewGroupSourceControlRegistry(GroupSourceControlRegistryOptions{
		Registry: registry, ReadDeadline: deadline, MaxConnections: 2,
		Services: []GroupSourceControlService{{Group: request1.Group, Service: newService(request1, exporter1)}, {Group: request2.Group, Service: newService(request2, exporter2)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if record, serveErr := requestGroupControl(t, router, target2, request2); serveErr != nil || record.Descriptor != descriptor2 {
		t.Fatalf("group two record=%+v err=%v", record, serveErr)
	}
	if exporter1.exportCalls != 0 || exporter2.exportCalls != 1 {
		t.Fatalf("cross-route exporter calls one=%d two=%d", exporter1.exportCalls, exporter2.exportCalls)
	}
	if _, serveErr := requestGroupControl(t, router, target1, request2); !errors.Is(serveErr, ErrSourceUnauthorized) {
		t.Fatalf("wrong target identity err=%v", serveErr)
	}
}

func twoGroupRegistry(
	t testing.TB, d1, d2 Descriptor,
) (*rafttransport.StaticRegistry, rafttransport.PeerIdentity, rafttransport.PeerIdentity, rafttransport.PeerIdentity) {
	t.Helper()
	var sourceNode, targetNode1, targetNode2 rafttransport.NodeID
	copy(sourceNode[:], d1.Group.ClusterID[:])
	copy(targetNode1[:], d1.Group.ClusterIncarnation[:])
	targetNode2 = targetNode1
	targetNode2[0]++
	members := []rafttransport.Member{
		{Group: d1.Group, ReplicaSetVersion: d1.ReplicaSetVersion, MemberID: d1.SourceMember, Node: sourceNode, Role: rafttransport.MemberVoter},
		{Group: d1.Group, ReplicaSetVersion: d1.ReplicaSetVersion, MemberID: d1.TargetMember, Node: targetNode1, Role: rafttransport.MemberEnrolled},
		{Group: d2.Group, ReplicaSetVersion: d2.ReplicaSetVersion, MemberID: d2.SourceMember, Node: sourceNode, Role: rafttransport.MemberVoter},
		{Group: d2.Group, ReplicaSetVersion: d2.ReplicaSetVersion, MemberID: d2.TargetMember, Node: targetNode2, Role: rafttransport.MemberEnrolled},
	}
	registry, err := rafttransport.NewStaticRegistry(sourceNode, members, rafttransport.Limits{MaxGroups: 2, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	domain := registry.TrustDomain()
	return registry,
		rafttransport.PeerIdentity{TrustDomain: domain, Node: sourceNode},
		rafttransport.PeerIdentity{TrustDomain: domain, Node: targetNode1},
		rafttransport.PeerIdentity{TrustDomain: domain, Node: targetNode2}
}

func requestGroupChunk(
	t testing.TB, handler interface {
		Serve(context.Context, rafttransport.PeerConnection) error
	},
	peer rafttransport.PeerIdentity, descriptor Descriptor,
) ([]byte, error) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- handler.Serve(context.Background(), &testPeerConn{Conn: server, identity: peer, class: rafttransport.TrafficSnapshot})
	}()
	var request [requestBytes]byte
	copy(request[:8], requestMagic[:])
	if _, err := AppendDescriptor(request[8:8], descriptor); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(request[8+DescriptorBytes:], 0)
	if err := writeFull(client, request[:]); err != nil {
		t.Fatal(err)
	}
	var response [responseBytes]byte
	_, readErr := io.ReadFull(client, response[:])
	var payload []byte
	if readErr == nil && response[8] == responseChunk {
		payload = make([]byte, binary.BigEndian.Uint32(response[32:36]))
		_, readErr = io.ReadFull(client, payload)
	}
	_ = client.Close()
	serveErr := <-done
	if readErr != nil && serveErr == nil {
		serveErr = readErr
	}
	return payload, serveErr
}

func requestGroupControl(
	t testing.TB, handler interface {
		Serve(context.Context, rafttransport.PeerConnection) error
	},
	peer rafttransport.PeerIdentity, request SourceControlRequest,
) (SourceControlRecord, error) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- handler.Serve(context.Background(), &testPeerConn{Conn: server, identity: peer, class: rafttransport.TrafficShardControl})
	}()
	encoded, err := AppendSourceControlRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeFull(client, encoded); err != nil {
		t.Fatal(err)
	}
	var response [SourceControlResponseBytes]byte
	_, readErr := io.ReadFull(client, response[:])
	_ = client.Close()
	serveErr := <-done
	if serveErr != nil {
		return SourceControlRecord{}, serveErr
	}
	if readErr != nil {
		return SourceControlRecord{}, readErr
	}
	record, err := OpenSourceControlResponse(response[:])
	return record, err
}
