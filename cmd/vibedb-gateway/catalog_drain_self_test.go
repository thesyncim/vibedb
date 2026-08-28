package main

import (
	"context"
	"encoding/asn1"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestGatewayCatalogDrainAuthenticatesOwnRosterMember(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	node := rafttransport.NodeID{3}
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), oid, domain, []rafttransport.NodeID{node})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots, oid.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testReplicaHealthSnapshot(t)
	digest, err := gateway.CatalogSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := gateway.ClusterCatalogDrainRequest{Operation: [32]byte{4}, Step: [32]byte{5},
		Generation: snapshot.Generation(), CatalogDigest: digest}
	member := gateway.ClusterCatalogDrainMember{Node: node, Incarnation: 1}
	deadline := servicetls.FixedDeadline(2 * time.Second)
	var authorized atomic.Bool
	authorized.Store(true)
	service, err := gateway.NewClusterCatalogDrainControlService(gateway.ClusterCatalogDrainControlOptions{
		Holder: gateway.NewCatalogHolder(snapshot), Member: member,
		Catalog: gatewayCatalogDigestVerifier{catalog: testReplicaHealthCatalog{snapshot}},
		Authorize: func(peer rafttransport.PeerIdentity, got gateway.ClusterCatalogDrainRequest) bool {
			return authorized.Load() && peer.Node == node && peer.TrustDomain == domain && got.Valid()
		}, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS := profile.WithLocalGatewayControlConnections()
	served := make(chan error, 1)
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		if address != "local-gateway-control" {
			return nil, errors.New("unexpected gateway address")
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			connection, serveErr := serverTLS.Server(ctx, server, rafttransport.TrafficGatewayControl, deadline)
			if serveErr == nil {
				serveErr = service.Serve(ctx, connection)
			}
			served <- serveErr
		}()
		return client, nil
	}
	coordinator, err := newGatewayClusterDrainCertifier(domain, profile, deadline, deadline, deadline, dial,
		[]gatewayControlEndpoint{{Member: member, Address: "local-gateway-control"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := coordinator.CertifyClusterCatalogDrain(t.Context(), request)
	if serveErr := <-served; err != nil || serveErr != nil || !first.ValidFor(request) {
		t.Fatalf("own roster member did not drain: client=%v server=%v", err, serveErr)
	}
	replay, err := coordinator.CertifyClusterCatalogDrain(t.Context(), request)
	if serveErr := <-served; err != nil || serveErr != nil || replay != first {
		t.Fatalf("drain replay differs: client=%v server=%v", err, serveErr)
	}
	wrong := request
	wrong.CatalogDigest[0]++
	_, err = coordinator.CertifyClusterCatalogDrain(t.Context(), wrong)
	if serveErr := <-served; err == nil || !errors.Is(serveErr, gateway.ErrClusterCatalogDrainUnknown) {
		t.Fatalf("self connection bypassed exact catalog digest: client=%v server=%v", err, serveErr)
	}
	authorized.Store(false)
	_, err = coordinator.CertifyClusterCatalogDrain(t.Context(), request)
	if serveErr := <-served; err == nil || !errors.Is(serveErr, gateway.ErrClusterCatalogDrainAuth) {
		t.Fatalf("self connection bypassed service authorization: client=%v server=%v", err, serveErr)
	}
}
