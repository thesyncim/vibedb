package replicacontrol

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type observerFunc func(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error)

func (function observerFunc) ObserveReplica(
	ctx context.Context, group raftmember.GroupKey, target uint64,
) (raftservice.ReplicaObservation, error) {
	return function(ctx, group, target)
}

type testConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
	class    rafttransport.TrafficClass
}

func (connection *testConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (connection *testConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type openerFunc func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (function openerFunc) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return function(ctx, node)
}

func TestCodecRoundTripIsCanonicalBoundedAndStrict(t *testing.T) {
	request, cut := controlFixture()
	observation := Observation{Request: request, Publication: cut.Publication,
		Status: cut.Status, Progress: cut.TargetProgress, ProgressFound: true, State: cut.State}
	requestBytes, err := AppendRequest(nil, request)
	if err != nil || len(requestBytes) != RequestBytes {
		t.Fatalf("request bytes=%d err=%v", len(requestBytes), err)
	}
	openedRequest, err := OpenRequest(requestBytes)
	if err != nil || openedRequest != request {
		t.Fatalf("request=%+v err=%v", openedRequest, err)
	}
	encoded, err := AppendResponse(nil, observation)
	if err != nil || len(encoded) > MaxResponseBytes {
		t.Fatalf("response bytes=%d err=%v", len(encoded), err)
	}
	opened, err := OpenResponse(encoded)
	if err != nil || opened.Request != request || opened.Status != observation.Status ||
		opened.Progress != observation.Progress || !opened.ProgressFound ||
		!proto.Equal(opened.State.ConfState, observation.State.ConfState) ||
		opened.State.Applied != observation.State.Applied {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	reencoded, err := AppendResponse(nil, opened)
	if err != nil || string(reencoded) != string(encoded) {
		t.Fatal("decode/re-encode did not preserve the sole canonical form")
	}
	for length := 0; length < len(requestBytes); length++ {
		if _, err = OpenRequest(requestBytes[:length]); err == nil {
			t.Fatalf("accepted request prefix %d", length)
		}
	}
	for length := 0; length < len(encoded); length++ {
		if _, err = OpenResponse(encoded[:length]); err == nil {
			t.Fatalf("accepted response prefix %d", length)
		}
	}
	if _, err = OpenRequest(append(append([]byte(nil), requestBytes...), 0)); err == nil {
		t.Fatal("accepted trailing request byte")
	}
	if _, err = OpenResponse(append(append([]byte(nil), encoded...), 0)); err == nil {
		t.Fatal("accepted trailing response byte")
	}
	reserved := append([]byte(nil), encoded...)
	reserved[276] = 1
	if _, err = OpenResponse(reserved); err == nil {
		t.Fatal("accepted noncanonical response tail")
	}
	oversized := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint32(oversized[268:272], MaxSnapshotBaseEnvelopeBytes+1)
	if _, err = OpenResponse(oversized); err == nil {
		t.Fatal("accepted oversized snapshot-base envelope")
	}
	malformedCertificate := append(append([]byte(nil), encoded...), 0)
	binary.BigEndian.PutUint32(malformedCertificate[268:272], 1)
	binary.BigEndian.PutUint32(malformedCertificate[272:276], uint32(len(malformedCertificate)))
	if _, err = OpenResponse(malformedCertificate); err == nil {
		t.Fatal("accepted malformed snapshot-base envelope")
	}
}

func TestAuthenticatedServiceAndClientReturnExactLeaderAndTargetWitnesses(t *testing.T) {
	request, cut := controlFixture()
	node := rafttransport.NodeID{9}
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	controller := rafttransport.NodeID{7}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	observations := 0
	service, err := NewService(ServiceOptions{
		Observer: observerFunc(func(_ context.Context, group raftmember.GroupKey, target uint64) (raftservice.ReplicaObservation, error) {
			observations++
			if group != request.Group || target != request.TargetMember {
				t.Fatal("observer received changed identity")
			}
			return cut, nil
		}),
		Authorize: func(peer rafttransport.PeerIdentity, got Request) bool {
			return peer.Node == controller && got == request
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	opener := openerFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		clientRaw, serverRaw := net.Pipe()
		client := &testConnection{Conn: clientRaw,
			identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node},
			class:    rafttransport.TrafficShardControl}
		server := &testConnection{Conn: serverRaw,
			identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: controller},
			class:    rafttransport.TrafficShardControl}
		go func() {
			if serveErr := service.Serve(context.Background(), server); serveErr != nil {
				t.Errorf("Serve: %v", serveErr)
			}
		}()
		return client, nil
	})
	client, err := NewClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := client.Observe(context.Background(), node, request)
	if err != nil || observations != 1 || observed.Status.LeaderID != request.TargetMember ||
		observed.Progress.Match != cut.Status.Commit || observed.State.Applied != cut.State.Applied {
		t.Fatalf("observation=%+v calls=%d err=%v", observed, observations, err)
	}
	settled, inFlight := observed.TransferWitness(request.TargetMember)
	if !settled || inFlight {
		t.Fatalf("transfer witness settled=%t inflight=%t", settled, inFlight)
	}
	// The catalog cannot know the next membership apply index before the
	// ConfChange commits. Read-only discovery must return that authenticated
	// version; subsequent mutations still use the exact discovered fence.
	request.ExpectedReplicaSetVersion = 0
	observed, err = client.Observe(context.Background(), node, request)
	if err != nil || observed.Publication.ReplicaSetVersion != cut.Publication.ReplicaSetVersion ||
		observed.Request.ExpectedReplicaSetVersion != cut.Publication.ReplicaSetVersion {
		t.Fatalf("current membership discovery=%+v err=%v", observed, err)
	}
}

func TestServiceRejectsWrongTrafficAndStaleReplicaSetBeforeResponse(t *testing.T) {
	request, cut := controlFixture()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewService(ServiceOptions{Observer: observerFunc(func(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error) {
		return cut, nil
	}), Authorize: func(rafttransport.PeerIdentity, Request) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	wrong := &testConnection{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{1}},
		class: rafttransport.TrafficOrdinary}
	if err = service.Serve(context.Background(), wrong); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong traffic err=%v", err)
	}
	_ = left.Close()

	request.ExpectedReplicaSetVersion++
	left, right = net.Pipe()
	server := &testConnection{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{1}},
		class: rafttransport.TrafficShardControl}
	done := make(chan error, 1)
	go func() { done <- service.Serve(context.Background(), server) }()
	if err = WriteRequest(left, request); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, ErrStale) {
		t.Fatalf("stale replica set err=%v", err)
	}
	_ = left.Close()
}

func controlFixture() (Request, raftservice.ReplicaObservation) {
	id := func(seed byte) [16]byte {
		var value [16]byte
		for index := range value {
			value[index] = seed + byte(index)
		}
		return value
	}
	group := raftmember.GroupKey{ClusterID: id(1), ClusterIncarnation: id(2),
		TopologyRecoveryEpoch: 3, ShardIncarnation: id(4), GroupID: id(5)}
	digest := sha256.Sum256([]byte("control-cut"))
	state := replicatedstate.State{Binding: replicatedstate.Binding{
		ClusterID: replication.ID128(group.ClusterID), ClusterIncarnation: replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: "orders", Shard: "0000-ffff",
		AllocationGeneration: 6, ShardIncarnation: replication.ID128(group.ShardIncarnation),
		GroupID: replication.ID128(group.GroupID), ActivePolicyGeneration: 7, ProtectionEpoch: 8,
		OwnershipEpoch: 9, SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
	}, Applied: 19, LastTerm: 4, LastKind: replicatedstate.RecordNormal,
		LastEntryType: pb.EntryNormal, LastEntryDigest: digest, DataChainDigest: digest,
		ApplyContractDigest: digest, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		ReplicaSetVersion: 17, BootstrapDigest: digest, SnapshotBaseDigest: digest}
	request := Request{Operation: sha256.Sum256([]byte("operation")), Step: sha256.Sum256([]byte("step")),
		Group: group, TargetMember: 2, ExpectedReplicaSetVersion: state.ReplicaSetVersion}
	publication := raftmodel.Publication{Applied: state.Applied, DataChainDigest: state.DataChainDigest,
		ConfState: state.ConfState, ReplicaSetVersion: state.ReplicaSetVersion}
	status := raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4, Commit: state.Applied,
		Applied: state.Applied, CheckpointApplied: state.Applied, RaftState: raft.StateLeader}
	progress := raftmodel.MemberProgress{Match: status.Commit, Next: status.Commit + 1, RecentActive: true}
	return request, raftservice.ReplicaObservation{Publication: publication, Status: status,
		TargetProgress: progress, ProgressFound: true, State: state}
}
