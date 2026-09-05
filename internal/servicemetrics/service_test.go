package servicemetrics

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testProvider struct {
	snapshot raftservice.ProgressMetricsSnapshot
	group    raftmember.GroupKey
	member   uint64
	stages   StageMetricsSnapshot
}

type aggregateOnlyProvider struct {
	snapshot raftservice.ProgressMetricsSnapshot
}

func (provider aggregateOnlyProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.snapshot
}

func (provider testProvider) StageMetrics() StageMetricsSnapshot { return provider.stages }

func (provider testProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.snapshot
}

func (provider testProvider) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, raftservice.ProgressMetricsSnapshot, bool) {
	return raftmember.RuntimeIdentity{Group: provider.group, MemberID: provider.member}, provider.snapshot,
		group == provider.group && provider.member != 0
}

type testConnection struct {
	net.Conn
	peer rafttransport.PeerIdentity
}

func (connection *testConnection) PeerIdentity() rafttransport.PeerIdentity { return connection.peer }
func (*testConnection) PeerKeyDigest() [32]byte                             { return [32]byte{} }
func (*testConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

func TestAuthenticatedMetricsServiceRoundTripAndCorruption(t *testing.T) {
	want := raftservice.ProgressMetricsSnapshot{ProposalCommands: 1, ProposalBytes: 2,
		AppliedEntries: 3, ReadyPersisted: 4, SnapshotsFinished: 5,
		ReadCompletions: 6, Faults: 7, CommitAdvancements: 8, CommittedEntries: 9}
	identity := rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}}
	service, err := NewService(ServiceOptions{Provider: testProvider{snapshot: want},
		Authorize:     func(peer rafttransport.PeerIdentity) bool { return peer == identity },
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- service.Serve(t.Context(), &testConnection{Conn: server, peer: identity}) }()
	got, err := (Client{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return &testConnection{Conn: client, peer: rafttransport.PeerIdentity{Node: rafttransport.NodeID{2}}}, nil
	}}).Read(t.Context())
	if err != nil || got != want {
		t.Fatalf("metrics=%+v err=%v want=%+v", got, err, want)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}

	encoded := appendResponse(Snapshot{Metrics: want})
	encoded[17] ^= 1
	if _, err = OpenResponse(encoded[:]); err == nil {
		t.Fatal("corrupt response accepted")
	}
	if _, err = OpenResponse(encoded[:len(encoded)-1]); err == nil {
		t.Fatal("truncated response accepted")
	}
}

func TestMetricsServiceRejectsUnauthorizedBeforeReading(t *testing.T) {
	service, err := NewService(ServiceOptions{Provider: testProvider{}, Authorize: func(rafttransport.PeerIdentity) bool { return false },
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- service.Serve(t.Context(), &testConnection{Conn: server}) }()
	if err = <-done; err == nil {
		t.Fatal("unauthorized peer accepted")
	}
	_ = client.Close()
}

func TestAuthenticatedMetricsServiceReturnsOnlyExactLocalGroup(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	want := raftservice.ProgressMetricsSnapshot{ProposalCommands: 9, AppliedEntries: 8}
	identity := rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}}
	service, err := NewService(ServiceOptions{Provider: testProvider{snapshot: want, group: group, member: 7},
		Authorize:    func(peer rafttransport.PeerIdentity) bool { return peer == identity },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- service.Serve(t.Context(), &testConnection{Conn: server, peer: identity}) }()
	snapshot, err := (Client{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return &testConnection{Conn: client}, nil
	}}).ReadGroup(t.Context(), group)
	if err != nil || snapshot.Group != group || snapshot.Member != 7 || snapshot.Metrics != want {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	unknown := group
	unknown.GroupID[0]++
	server, client = net.Pipe()
	go func() { done <- service.Serve(t.Context(), &testConnection{Conn: server, peer: identity}) }()
	if _, err = (Client{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return &testConnection{Conn: client}, nil
	}}).ReadGroup(t.Context(), unknown); err == nil {
		t.Fatal("unknown group accepted")
	}
	if err = <-done; err == nil {
		t.Fatal("server accepted unknown group")
	}
}

func TestMetricsServiceRejectsGroupRequestWhenProviderHasNoGroupSupport(t *testing.T) {
	identity := rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}}
	service, err := NewService(ServiceOptions{Provider: aggregateOnlyProvider{},
		Authorize:    func(peer rafttransport.PeerIdentity) bool { return peer == identity },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- service.Serve(t.Context(), &testConnection{Conn: server, peer: identity}) }()
	if _, err = (Client{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return &testConnection{Conn: client}, nil
	}}).ReadGroup(t.Context(), group); err == nil {
		t.Fatal("group request succeeded without a GroupProvider")
	}
	if err = <-done; !errors.Is(err, ErrMetrics) {
		t.Fatalf("server error = %v, want ErrMetrics", err)
	}
}

func BenchmarkMetricsResponseCodec(b *testing.B) {
	metrics := raftservice.ProgressMetricsSnapshot{ProposalCommands: 1, AppliedEntries: 2}
	b.ReportAllocs()
	for b.Loop() {
		response := appendResponse(Snapshot{Metrics: metrics})
		if _, err := OpenResponse(response[:]); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMetricsNodeStageSnapshotIsAuthenticatedAndCanonical(t *testing.T) {
	stages := StageMetricsSnapshot{CheckpointApplied: 1, Checkpoints: 2, PhysicalCheckpoints: 3,
		CheckpointBarrierSyncs: 4, WALLiveBytes: 5, WALEntries: 6, WALSyncs: 7,
		BackupRequests: 8, BackupFaults: 9, BackupLogicalBytes: 10, BackupScanBytes: 11,
		SnapshotTransferChunks: 12, SnapshotTransferBytes: 13, SnapshotResidentBytes: 14,
		ReplicaActionRequests: 15, ReplicaActionCompletions: 16, ReplicaActionFaults: 17,
		SplitControlRequests: 18, SplitControlCompletions: 19, SplitControlFaults: 20,
		BootstrapRequests: 21, BootstrapChunks: 22, BootstrapBytes: 23, BootstrapCompletions: 24,
		BootstrapFaults: 25, BootstrapResidentBytes: 26, BootstrapInflight: 27}
	encoded := appendResponse(Snapshot{Stages: stages})
	opened, err := OpenResponse(encoded[:])
	if err != nil || opened.Stages != stages || opened.Group != (raftmember.GroupKey{}) || opened.Member != 0 {
		t.Fatalf("snapshot=%+v err=%v", opened, err)
	}
	encoded[200] ^= 1
	if _, err = OpenResponse(encoded[:]); err == nil {
		t.Fatal("corrupt stage frame accepted")
	}
	malformed := appendResponse(Snapshot{Member: 1})
	if _, err = OpenResponse(malformed[:]); err == nil {
		t.Fatal("node aggregate with member accepted")
	}
}
