//go:build darwin || linux

package main

import (
	"context"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

// Only Probe is exercised. Other Owner methods remain uncalled, so this uses
// the real TLS/native listener without requiring platform-specific storage.
type rf3FaultWaveTestOwner struct {
	*raftservice.Owner
	state   raftservice.ServingState
	entered chan struct{}
	release <-chan struct{}
}

func (owner *rf3FaultWaveTestOwner) Probe(ctx context.Context, _ raftmember.GroupKey) (raftservice.ServingState, error) {
	if owner.entered != nil {
		select {
		case owner.entered <- struct{}{}:
		case <-ctx.Done():
			return raftservice.ServingState{}, context.Cause(ctx)
		}
	}
	if owner.release != nil {
		select {
		case <-owner.release:
		case <-ctx.Done():
			return raftservice.ServingState{}, context.Cause(ctx)
		}
	}
	return owner.state, nil
}

func newRF3FaultWaveTestServer(t testing.TB, connections, handshakes int, owner *rf3FaultWaveTestOwner) (
	*rf3FaultFixture, *shardservice.ReplicatedServer, *shardservice.ReplicatedServerTLS, *shardservice.ReplicatedRequest,
) {
	t.Helper()
	fixture := &rf3FaultFixture{
		nodes: [rf3CommandMembers]rafttransport.NodeID{{1}, {2}, {3}}, authority: rf3CommandAuthority(),
		profiles: make([]*rafttransport.PeerTLS, rf3CommandMembers),
	}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(),
		asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1},
		rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}, fixture.nodes[:])
	if err != nil {
		t.Fatal(err)
	}
	for i := range fixture.profiles {
		fixture.profiles[i], err = servicetls.LoadProfile(credentials[i].Certificate, credentials[i].Key,
			roots, "1.3.6.1.4.1.32473.1.1", time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	store := rf3CommandStoreIdentity(1)
	identity := raftmember.RuntimeIdentity{Group: rf3CommandGroup(), AllocationGeneration: store.AllocationGeneration,
		MemberID: store.MemberID, StoreID: store.StoreID, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
	owner.state = raftservice.ServingState{Identity: identity,
		Command: commandFenceFromPublication(fixture.authority, identity, 1),
		Status: raftmember.RuntimeStatus{MemberID: identity.MemberID, LeaderID: identity.MemberID,
			Term: 2, Commit: 9, Applied: 9, CheckpointApplied: 9}}
	server, err := shardservice.NewReplicatedServer(owner, 1<<20, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(fixture.authority.ActivePolicyGeneration, []serviceauthz.Entry{
		{Node: fixture.nodes[1], Capabilities: serviceauthz.CapabilityDelegate | serviceauthz.CapabilityTopology},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err = server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	tls, err := shardservice.NewReplicatedServerTLS(fixture.profiles[0], []rafttransport.NodeID{fixture.nodes[1]})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.nativeAddresses[0] = listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.ServeAuthenticated(ctx, listener, tls,
			func() time.Time { return time.Now().Add(10 * time.Second) }, connections, handshakes)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("wave preflight server did not close")
		}
	})
	request := &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe,
		Authority:  serviceauthz.Authority{Node: fixture.nodes[1], Generation: fixture.authority.ActivePolicyGeneration},
		Capability: serviceauthz.CapabilityTopology,
		Fence:      shardservice.ReplicatedFence{Group: identity.Group, AllocationGeneration: identity.AllocationGeneration}}
	return fixture, server, tls, request
}

func waitRF3FaultWaveStats(t testing.TB, server *shardservice.ReplicatedServer, active uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.Stats().Active == active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("native active connections = %d, want %d", server.Stats().Active, active)
}

func TestRF3FaultWaveAuthenticatesBeforeConcurrentRequests(t *testing.T) {
	for _, callers := range []int{32, 64} {
		t.Run(fmt.Sprintf("callers_%d", callers), func(t *testing.T) {
			release := make(chan struct{})
			owner := &rf3FaultWaveTestOwner{entered: make(chan struct{}, callers), release: release}
			fixture, server, tls, request := newRF3FaultWaveTestServer(t, 64, 16, owner)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			done := make(chan struct{})
			var results []rf3FaultRoundTrip
			var waveErr error
			go func() {
				results, waveErr = fixture.roundTripWave(ctx, 0, request, callers)
				close(done)
			}()
			for range callers {
				select {
				case <-owner.entered:
				case <-done:
					t.Fatalf("wave ended before every request was in flight: %v", waveErr)
				case <-ctx.Done():
					t.Fatal("wave did not preserve native request concurrency")
				}
			}
			if stats := tls.Stats(); stats.Authenticated != uint64(callers) || stats.HandshakeRejected != 0 || stats.AuthenticationRejected != 0 {
				t.Fatalf("TLS wave stats = %+v", stats)
			}
			close(release)
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("wave did not finish")
			}
			if waveErr != nil || len(results) != callers {
				t.Fatalf("wave results=%d err=%v", len(results), waveErr)
			}
			for _, result := range results {
				if result.err != nil || result.response.Kind != shardservice.ReplicatedHandshake {
					t.Fatalf("wave response=%+v err=%v", result.response, result.err)
				}
			}
			waitRF3FaultWaveStats(t, server, 0)
		})
	}
}

func TestRF3FaultWavePartialPreparationClosesEveryConnection(t *testing.T) {
	fixture, server, _, request := newRF3FaultWaveTestServer(t, 2, 2, &rf3FaultWaveTestOwner{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, err := fixture.roundTripWave(ctx, 0, request, 3)
	if !errors.Is(err, rafttransport.ErrPeerAuthentication) || results != nil {
		t.Fatalf("bounded setup results=%+v err=%v", results, err)
	}
	waitRF3FaultWaveStats(t, server, 0)
	if stats := server.Stats(); stats.Accepted != 2 || stats.Rejected != 1 {
		t.Fatalf("expected partial setup at connection bound: %+v", stats)
	}
	cancel()
	if _, err := fixture.roundTripWave(ctx, 0, request, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation = %v", err)
	}
}

func TestRF3FaultWaveDoesNotWeakenHandshakeOverloadRejection(t *testing.T) {
	fixture, server, tls, request := newRF3FaultWaveTestServer(t, 4, 1, &rf3FaultWaveTestOwner{})
	raw, err := net.Dial("tcp", fixture.nativeAddresses[0])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	// This socket deliberately never sends ClientHello, holding the single
	// handshake slot while leaving connection capacity for the next client.
	waitRF3FaultWaveStats(t, server, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if connection, err := fixture.dialNative(ctx, 0); !errors.Is(err, rafttransport.ErrPeerAuthentication) {
		if connection != nil {
			_ = connection.Close()
		}
		t.Fatalf("TLS handshake overload was not rejected: %v", err)
	}
	if stats := tls.Stats(); stats.HandshakeRejected != 1 || stats.Authenticated != 0 {
		t.Fatalf("handshake overload stats = %+v", stats)
	}
	_ = raw.Close()
	waitRF3FaultWaveStats(t, server, 0)
	if response, err := fixture.roundTripContext(ctx, 0, request); err != nil || response.Kind != shardservice.ReplicatedHandshake {
		t.Fatalf("handshake capacity was not reusable: %+v %v", response, err)
	}
}
