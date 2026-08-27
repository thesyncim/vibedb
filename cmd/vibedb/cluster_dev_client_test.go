package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// This executes real TLS with the exact dev credential and policy producers on
// every host; strict-allocation-dependent process tests cannot hide self-auth.
func TestDevClientCredentialsAuthenticateWithoutServiceAuthority(t *testing.T) {
	root := t.TempDir()
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	gatewayNode, clientNode := rafttransport.NodeID{3}, rafttransport.NodeID{4}
	credentials, roots, err := writeDevCredentials(root, domain, []rafttransport.NodeID{gatewayNode, clientNode})
	if err != nil {
		t.Fatal(err)
	}
	profiles := make([]*rafttransport.PeerTLS, len(credentials))
	for i, credential := range credentials {
		profiles[i], err = servicetls.LoadProfile(credential[0], credential[1], roots, devClusterOID, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	if profiles[0].LocalIdentity().Node != gatewayNode || profiles[1].LocalIdentity().Node != clientNode {
		t.Fatal("generated credentials changed exact identities")
	}
	policyPath := filepath.Join(root, "policy.vibejson")
	if err := writeDevPolicy(policyPath, []rafttransport.NodeID{gatewayNode}, clientNode); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.LoadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	for capability := serviceauthz.CapabilityDataRead; capability <= serviceauthz.CapabilityRestoreActivate; capability <<= 1 {
		want := serviceauthz.DecisionDenyCapability
		if capability == serviceauthz.CapabilityDataRead || capability == serviceauthz.CapabilityDataWrite {
			want = serviceauthz.DecisionAllow
		}
		if got := policy.Check(clientNode, capability); got != want {
			t.Fatalf("client capability %d decision=%d want=%d", capability, got, want)
		}
	}
	if policy.Check(gatewayNode, serviceauthz.CapabilityTopology|serviceauthz.CapabilityDelegate|serviceauthz.CapabilityRequestLedger) != serviceauthz.DecisionAllow {
		t.Fatal("gateway lost internal catalog/coordinator authority")
	}
	for _, self := range []bool{false, true} {
		name := "distinct-client"
		client := profiles[1]
		if self {
			name, client = "self-rejected", profiles[0]
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			serverRaw, clientRaw := net.Pipe()
			defer serverRaw.Close()
			defer clientRaw.Close()
			type result struct {
				connection rafttransport.PeerConnection
				err        error
			}
			completed := make(chan result, 1)
			deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
			go func() {
				connection, err := profiles[0].Server(ctx, serverRaw, rafttransport.TrafficGatewayClient, deadline)
				completed <- result{connection, err}
			}()
			connection, clientErr := client.Client(ctx, clientRaw, gatewayNode, rafttransport.TrafficGatewayClient, deadline)
			server := <-completed
			// Close raw pipes before TLS connections to keep close_notify bounded.
			_ = serverRaw.Close()
			_ = clientRaw.Close()
			if connection != nil {
				_ = connection.Close()
			}
			if server.connection != nil {
				_ = server.connection.Close()
			}
			if self {
				if !errors.Is(clientErr, rafttransport.ErrPeerAuthentication) || server.err == nil {
					t.Fatalf("self-auth client=%v server=%v", clientErr, server.err)
				}
				return
			}
			if clientErr != nil || server.err != nil {
				t.Fatalf("distinct-client handshake client=%v server=%v", clientErr, server.err)
			}
			if connection.PeerIdentity().Node != gatewayNode || server.connection.PeerIdentity().Node != clientNode {
				t.Fatal("TLS authenticated wrong peer identity")
			}
		})
	}
}

func TestDevClientPolicyRejectsIdentityAliasesBeforePublication(t *testing.T) {
	for _, nodes := range [][]rafttransport.NodeID{{{1}, {1}}, {{2}}, {{}}} {
		path := filepath.Join(t.TempDir(), "policy.vibejson")
		if err := writeDevPolicy(path, nodes, rafttransport.NodeID{2}); !errors.Is(err, errDevCluster) {
			t.Fatalf("nodes=%v accepted: %v", nodes, err)
		}
	}
	if _, err := decodeDev16("AA000000000000000000000000000000"); !errors.Is(err, errDevCluster) {
		t.Fatalf("noncanonical identity alias accepted: %v", err)
	}
}
