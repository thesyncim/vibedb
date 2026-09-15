package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestRF3RetirementDiscoveryPinsSurvivingPeer(t *testing.T) {
	group := rf3CommandGroup()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	local, survivor, other := rafttransport.NodeID{1}, rafttransport.NodeID{2}, rafttransport.NodeID{3}
	// The final credential has the same certified node identity and CA as the
	// survivor, but a different key. CA trust alone must not authorize it.
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID,
		domain, []rafttransport.NodeID{local, survivor, other, survivor})
	if err != nil {
		t.Fatal(err)
	}
	profiles := make([]*rafttransport.PeerTLS, len(credentials))
	for index, credential := range credentials {
		profiles[index], err = servicetls.LoadProfile(credential.Certificate, credential.Key, roots,
			rf3testfixture.ProcessIdentityOID, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	manifest := rf3Manifest{TLS: rf3ManifestTLS{PeerKeys: rf3CommandPeerKeys(credentials[0])[:3]}}
	members := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: survivor, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: other, Role: rafttransport.MemberVoter},
	}
	registry, err := newRF3PinnedStaticRegistry(manifest, profiles[0], members,
		map[rafttransport.NodeID]string{local: "127.0.0.1:21001", survivor: "127.0.0.1:21002", other: "127.0.0.1:21003"},
		rafttransport.Limits{MaxGroups: 1, MaxMembers: 3, MaxPeers: 3})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := registry.PhysicalPeer(survivor)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		server      int
		staleEpoch  bool
		staleKey    bool
		wrongTarget bool
		allowed     bool
	}{
		{name: "current certified survivor", server: 1, allowed: true},
		{name: "different certified node", server: 2},
		{name: "same node different certified key", server: 3},
		{name: "captured incarnation changed", server: 1, staleEpoch: true},
		{name: "captured key changed", server: 1, staleKey: true},
		{name: "different requested node", server: 1, wrongTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			bound, _ := ctx.Deadline()
			deadline := func() time.Time { return bound }
			captured := peer
			if tc.staleEpoch {
				captured.Incarnation++
			}
			if tc.staleKey {
				captured.ServiceKeyDigest[0] ^= 1
			}
			var calls int
			var raw *rf3RetirementTrackedConnection
			serverDone := make(chan error, 1)
			opener := &rf3RetirementStreamOpener{registry: registry, profile: profiles[0], node: survivor,
				peer: captured, address: "untrusted-discovery-hint", deadline: deadline,
				dial: func(context.Context, string) (net.Conn, error) {
					calls++
					left, right := net.Pipe()
					t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
					raw = &rf3RetirementTrackedConnection{Conn: left}
					go func() {
						connection, serveErr := profiles[tc.server].Server(ctx, right, rafttransport.TrafficShardControl, deadline)
						if connection != nil {
							_, _ = io.Copy(io.Discard, connection)
							_ = connection.Close()
						}
						serverDone <- serveErr
					}()
					return raw, nil
				}}
			requested := survivor
			if tc.wrongTarget {
				requested = other
			}
			connection, openErr := opener.OpenShardControl(ctx, requested)
			if tc.allowed {
				if openErr != nil || connection == nil || connection.PeerIdentity().Node != survivor {
					t.Fatalf("certified survivor rejected: %v", openErr)
				}
				_ = connection.Close()
			} else if openErr == nil || connection != nil {
				t.Fatal("untrusted retirement observation channel accepted")
			}
			if tc.wrongTarget {
				if calls != 0 || !errors.Is(openErr, replicaaction.ErrUnauthorized) {
					t.Fatalf("wrong survivor dialed: calls=%d err=%v", calls, openErr)
				}
				return
			}
			if calls != 1 || raw == nil || !raw.closed.Load() {
				t.Fatalf("observation connection leaked: calls=%d", calls)
			}
			select {
			case serverErr := <-serverDone:
				if tc.allowed && serverErr != nil {
					t.Fatal(serverErr)
				}
			case <-ctx.Done():
				t.Fatal("retirement TLS handshake exceeded shared deadline")
			}
		})
	}
}

type rf3RetirementTrackedConnection struct {
	net.Conn
	closed atomic.Bool
}

func (connection *rf3RetirementTrackedConnection) Close() error {
	connection.closed.Store(true)
	return connection.Conn.Close()
}
