package replicacontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
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

func TestHealthCodecIsFixedStrictAndDistinctFromFullCut(t *testing.T) {
	request, cut := controlFixture()
	request.HealthOnly = true
	health := HealthObservation{Request: request, MemberID: cut.Status.MemberID,
		LeaderID: cut.Status.LeaderID, Term: cut.Status.Term, Commit: cut.Status.Commit,
		Applied: cut.Status.Applied, ReplicaSetVersion: cut.Publication.ReplicaSetVersion}
	encoded, err := AppendHealthObservation(nil, health)
	if err != nil || len(encoded) != HealthObservationBytes {
		t.Fatalf("health bytes=%d err=%v", len(encoded), err)
	}
	opened, err := OpenHealthObservation(encoded)
	if err != nil || opened != health {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	reencoded, err := AppendHealthObservation(nil, opened)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatal("health decode/re-encode did not preserve the canonical form")
	}
	var buffer bytes.Buffer
	if err = WriteHealthObservation(&buffer, health); err != nil {
		t.Fatal(err)
	}
	if read, readErr := ReadHealthObservation(&buffer); readErr != nil || read != health {
		t.Fatalf("read=%+v err=%v", read, readErr)
	}
	for length := 0; length < len(encoded); length++ {
		if _, err = OpenHealthObservation(encoded[:length]); err == nil {
			t.Fatalf("accepted health prefix %d", length)
		}
	}
	if _, err = OpenHealthObservation(append(append([]byte(nil), encoded...), 0)); err == nil {
		t.Fatal("accepted trailing health byte")
	}
	badKind := append([]byte(nil), encoded...)
	badKind[8+176] = 0
	if _, err = OpenHealthObservation(badKind); err == nil {
		t.Fatal("accepted health response with full-request kind")
	}
	badKind[8+176] = 2
	if _, err = OpenHealthObservation(badKind); err == nil {
		t.Fatal("accepted health response with unknown request kind")
	}
	badReserved := append([]byte(nil), encoded...)
	badReserved[8+177] = 1
	if _, err = OpenHealthObservation(badReserved); err == nil {
		t.Fatal("accepted nonzero health request reserved byte")
	}
	fullRequest, fullCut := controlFixture()
	full, err := AppendResponse(nil, Observation{Request: fullRequest,
		Publication: fullCut.Publication, Status: fullCut.Status, Progress: fullCut.TargetProgress,
		ProgressFound: true, State: fullCut.State})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenHealthObservation(full); err == nil {
		t.Fatal("accepted full-cut response in health reader")
	}
}

func TestHealthClientRejectsEchoMismatchAndFullClientRejectsHealthRequest(t *testing.T) {
	request, cut := controlFixture()
	request.HealthOnly = true
	node := rafttransport.NodeID{9}
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	serverError := make(chan error, 1)
	opener := openerFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		clientRaw, serverRaw := net.Pipe()
		client := &testConnection{Conn: clientRaw,
			identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node},
			class:    rafttransport.TrafficShardControl}
		server := &testConnection{Conn: serverRaw,
			identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{7}},
			class:    rafttransport.TrafficShardControl}
		go func() {
			defer server.Close()
			var raw [RequestBytes]byte
			if _, readErr := io.ReadFull(server, raw[:]); readErr != nil {
				serverError <- readErr
				return
			}
			got, openErr := OpenRequest(raw[:])
			if openErr != nil {
				serverError <- openErr
				return
			}
			response := HealthObservation{Request: got, MemberID: got.TargetMember,
				LeaderID: got.TargetMember, Term: 4, Commit: cut.Status.Commit,
				Applied: cut.Status.Applied, ReplicaSetVersion: got.ExpectedReplicaSetVersion}
			response.Request.Step[0]++
			serverError <- WriteHealthObservation(server, response)
		}()
		return client, nil
	})
	client, err := NewClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Observe(context.Background(), node, request); !errors.Is(err, ErrControl) {
		t.Fatalf("full client accepted health request: %v", err)
	}
	if _, err = client.ObserveHealth(context.Background(), node, request); !errors.Is(err, ErrStale) {
		t.Fatalf("health echo mismatch err=%v", err)
	}
	if serveErr := <-serverError; serveErr != nil {
		t.Fatalf("health test server: %v", serveErr)
	}
}

type healthControlObserver struct {
	cut         raftservice.ReplicaHealthObservation
	healthCalls int
	fullCalls   int
}

func (observer *healthControlObserver) ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error) {
	observer.fullCalls++
	return raftservice.ReplicaObservation{}, errors.New("health service requested a full snapshot")
}

func (observer *healthControlObserver) ObserveReplicaHealth(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaHealthObservation, error) {
	observer.healthCalls++
	return observer.cut, nil
}

func TestHealthServiceUsesAuthorizedHealthObserverOnly(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutate      func(*healthControlObserver, *rafttransport.PeerIdentity)
		deny        bool
		unsupported bool
		busy        bool
		wrongClass  bool
		want        error
		calls       int
	}{
		{name: "healthy", calls: 1},
		{name: "wrong target", calls: 1, want: ErrStale, mutate: func(observer *healthControlObserver, _ *rafttransport.PeerIdentity) {
			observer.cut.Identity.MemberID++
			observer.cut.Status.MemberID++
		}},
		{name: "wrong group", calls: 1, want: ErrStale, mutate: func(observer *healthControlObserver, _ *rafttransport.PeerIdentity) {
			observer.cut.Identity.Group.GroupID[0]++
		}},
		{name: "stale version", calls: 1, want: ErrStale, mutate: func(observer *healthControlObserver, _ *rafttransport.PeerIdentity) {
			observer.cut.Publication.ReplicaSetVersion++
		}},
		{name: "denied principal", deny: true, want: ErrUnauthorized},
		{name: "wrong trust domain", want: ErrUnauthorized, mutate: func(_ *healthControlObserver, peer *rafttransport.PeerIdentity) {
			peer.TrustDomain.ClusterIncarnation[0]++
		}},
		{name: "wrong traffic class", wrongClass: true, want: ErrUnauthorized},
		{name: "unsupported health", unsupported: true, want: ErrControl},
		{name: "observer bound", busy: true, want: ErrBound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, cut := controlFixture()
			expected := request
			expected.HealthOnly = true
			domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
			node := rafttransport.NodeID{9}
			controller := rafttransport.NodeID{7}
			peer := rafttransport.PeerIdentity{TrustDomain: domain, Node: controller}
			observer := &healthControlObserver{cut: raftservice.ReplicaHealthObservation{
				Identity: raftmember.RuntimeIdentity{Group: request.Group, MemberID: request.TargetMember},
				Status:   cut.Status, Publication: cut.Publication,
			}}
			if test.mutate != nil {
				test.mutate(observer, &peer)
			}
			var source Observer = observer
			if test.unsupported {
				source = observerFunc(observer.ObserveReplica)
			}
			deadline := func() time.Time { return time.Now().Add(2 * time.Second) }
			service, err := NewService(ServiceOptions{Observer: source,
				Authorize: func(peer rafttransport.PeerIdentity, got Request) bool {
					return !test.deny && peer.Node == controller && got == expected
				}, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			if test.busy {
				service.slots <- struct{}{}
			}
			done := make(chan error, 1)
			client, err := NewClient(ClientOptions{Opener: openerFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
				clientRaw, serverRaw := net.Pipe()
				server := &testConnection{Conn: serverRaw, identity: peer, class: rafttransport.TrafficShardControl}
				if test.wrongClass {
					server.class = rafttransport.TrafficOrdinary
				}
				go func() { done <- service.Serve(t.Context(), server) }()
				return &testConnection{Conn: clientRaw, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node},
					class: rafttransport.TrafficShardControl}, nil
			}), ReadDeadline: deadline, WriteDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			observed, observeErr := client.ObserveHealth(t.Context(), node, request)
			if serveErr := <-done; !errors.Is(serveErr, test.want) {
				t.Fatalf("Serve=%v want=%v", serveErr, test.want)
			}
			if test.want == nil {
				if observeErr != nil || observed.Request != expected || observed.MemberID != cut.Status.MemberID ||
					observed.Commit != cut.Status.Commit || observed.Applied != cut.Publication.Applied {
					t.Fatalf("health=%+v err=%v", observed, observeErr)
				}
			} else if observeErr == nil {
				t.Fatal("rejected service request returned a health observation")
			}
			if observer.fullCalls != 0 || observer.healthCalls != test.calls {
				t.Fatalf("full calls=%d health calls=%d want=%d", observer.fullCalls, observer.healthCalls, test.calls)
			}
		})
	}
}
