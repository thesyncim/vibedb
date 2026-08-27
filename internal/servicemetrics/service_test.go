package servicemetrics

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testProvider struct {
	snapshot raftservice.ProgressMetricsSnapshot
}

func (provider testProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.snapshot
}

type testConnection struct {
	net.Conn
	peer rafttransport.PeerIdentity
}

func (connection *testConnection) PeerIdentity() rafttransport.PeerIdentity { return connection.peer }
func (*testConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

func TestAuthenticatedMetricsServiceRoundTripAndCorruption(t *testing.T) {
	want := raftservice.ProgressMetricsSnapshot{ProposalCommands: 1, ProposalBytes: 2,
		AppliedEntries: 3, ReadyPersisted: 4, SnapshotsFinished: 5,
		ReadCompletions: 6, Faults: 7}
	identity := rafttransport.PeerIdentity{Node: rafttransport.NodeID{1}}
	service, err := NewService(ServiceOptions{Provider: testProvider{want},
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

	encoded := appendResponse(want)
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

func BenchmarkMetricsResponseCodec(b *testing.B) {
	metrics := raftservice.ProgressMetricsSnapshot{ProposalCommands: 1, AppliedEntries: 2}
	b.ReportAllocs()
	for b.Loop() {
		response := appendResponse(metrics)
		if _, err := OpenResponse(response[:]); err != nil {
			b.Fatal(err)
		}
	}
}
