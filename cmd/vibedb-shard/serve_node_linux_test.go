//go:build linux

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestServeNodeFrontendOpenFailureJoinsOwnersAndReleasesStorage(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	nodes, group := rf3CommandNodes(), rf3CommandGroup()
	frontend := rafttransport.NodeID{0xf1}
	root := t.TempDir()
	credentials, roots, err := rf3testfixture.WriteCredentials(root, rf3CommandIdentityOID,
		rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation},
		append(nodes[:], frontend))
	if err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(root, "policy.vibejson")
	if err := os.WriteFile(policy, rf3CommandPolicyWithTarget(nodes, frontend), 0o600); err != nil {
		t.Fatal(err)
	}
	var reservations []*rf3AcceptReadyListener
	byAddress := make(map[string]*rf3AcceptReadyListener)
	for range 4 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		admission := newRF3AcceptReadyListener(listener)
		reservations = append(reservations, admission)
		byAddress[listener.Addr().String()] = admission
		t.Cleanup(func() { _ = listener.Close() })
	}
	listeners := rf3ManifestListeners{Peer: reservations[0].Addr().String(), Native: reservations[1].Addr().String(),
		Snapshot: reservations[2].Addr().String(), Control: reservations[3].Addr().String()}
	for index := range input.Groups {
		member := &input.Groups[index]
		member.Listeners = listeners
		member.Members[0].PeerAddress = listeners.Peer
		member.TLS = rf3ManifestTLS{Certificate: credentials[0].Certificate, Key: credentials[0].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()}
		member.AuthorizationPolicy = policy
	}
	input.Gateway = &rf3ManifestGateway{
		CatalogPath: filepath.Join(root, "absent-catalog"), CatalogRouteSeedPath: filepath.Join(root, "absent-seed"),
		CatalogRelation: 1, CatalogAttempts: 1, CatalogAttemptTimeoutMillis: 1000, CatalogSessionLeaseMillis: 86400000,
		CatalogSessionJournal: filepath.Join(root, "absent-session"), CatalogClientID: strings.Repeat("a", 32),
		CatalogRetryHome: strings.Repeat("b", 16), DurableAckKeyPath: filepath.Join(root, "absent-ack-key"),
		ListenAddress: "127.0.0.1:0", AuthorizationPolicy: policy, TableCatalogs: []string{},
		TLS: rf3ManifestTLS{Certificate: credentials[3].Certificate, Key: credentials[3].Key, Roots: roots, IdentityOID: rf3CommandIdentityOID.String()},
	}
	for index, node := range nodes {
		address := fmt.Sprintf("127.0.0.1:%d", 28000+index)
		if index == 0 {
			address = listeners.Native
		}
		input.Gateway.ShardPeers = append(input.Gateway.ShardPeers, rf3ManifestGatewayPeer{Address: address, NodeID: fmt.Sprintf("%x", node)})
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = servePreparedRF3WithEmbeddedGateway(ctx, manifest, rf3DefaultExecutionLanes, func(_, address string) (net.Listener, error) {
		if listener := byAddress[address]; listener != nil {
			return listener, nil
		}
		return nil, fmt.Errorf("unexpected listener %q", address)
	})
	if err == nil || !strings.Contains(err.Error(), "open embedded gateway") {
		t.Fatalf("frontend Open failure was not returned: %v", err)
	}
	for _, listener := range reservations {
		select {
		case <-listener.accepting:
		default:
			t.Fatalf("frontend Open ran before %s accepted", listener.Addr())
		}
	}
	profile, err := servicetls.LoadProfile(manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots, manifest.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatalf("failed frontend retained node log ownership: %v", err)
	}
	defer owner.Close()
	prepared, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		t.Fatalf("failed frontend retained SQL ownership: %v", err)
	}
	if err := closePreparedRF3Groups(prepared.groups, nil); err != nil {
		t.Fatal(err)
	}
}
