package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/gatewayruntime"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestRF3CanonicalSourceAdmitsOnlyCommittedReceiverWithoutSharedRaftGroup(t *testing.T) {
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID, trust, []rafttransport.NodeID{{1}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots, rf3CommandIdentityOID.String(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"committed", "self", "unknown", "wrong key", "wrong incarnation", "retired", "wrong domain", "wrong traffic"} {
		t.Run(name, func(t *testing.T) {
			cut := rf3CatalogGenesisExistingCutFixture(t, profile.LocalIdentity().Node, gateway.NodeActive)
			cut.Nodes.Nodes[0].ServiceKeyDigest = replication.Digest(profile.LocalServiceKeyDigest())
			receiver := &cut.Nodes.Nodes[1]
			receiver.Roles = gateway.NodeRoleStorage
			receiver.Gateway = gateway.GatewayIdentity{}
			receiver.GatewayEndpoint, receiver.GatewayAddress = "", ""
			if name == "self" {
				receiver = &cut.Nodes.Nodes[0]
			}
			query := frontenddrain.PreparedAckCutReadRequest{Operation: frontenddrain.CutOperationReadLatest,
				Nonce: [16]byte{1}, ReceiverNode: receiver.NodeID, ReceiverIncarnation: receiver.Incarnation,
				ReceiverServiceKeyDigest: [32]byte(receiver.ServiceKeyDigest)}
			peer := rafttransport.PeerIdentity{TrustDomain: trust, Node: receiver.NodeID}
			class := rafttransport.TrafficShardControl
			switch name {
			case "unknown":
				peer.Node, query.ReceiverNode = rafttransport.NodeID{9}, rafttransport.NodeID{9}
			case "wrong key":
				query.ReceiverServiceKeyDigest[0]++
			case "wrong incarnation":
				query.ReceiverIncarnation++
			case "retired":
				receiver.Lifecycle = gateway.NodeDecommissioned
				receiver.RetirementScanDigest = replication.Digest{1}
				receiver.RetirementScanDirectoryRevision, receiver.RetirementScanCutRevision = 3, 4
			case "wrong domain":
				peer.TrustDomain.ClusterID[0]++
			case "wrong traffic":
				class = rafttransport.TrafficGatewayControl
			}
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			service, err := gatewayruntime.NewFrontendDrainPreparedAckCutReadService(gatewayruntime.FrontendDrainPreparedAckCutReadServiceOptions{
				Authorize: rf3CanonicalSourcePeerAuthorizer(profile),
				ReadCut:   func(context.Context) (gateway.FrontendDrainRuntimeCut, error) { return cut, nil },
				Profile:   profile, PolicyGeneration: 1, TrafficClass: rafttransport.TrafficShardControl,
				SourceNode: profile.LocalIdentity().Node, SourceIncarnation: 1, SourceServiceKeyDigest: profile.LocalServiceKeyDigest(),
				ReadDeadline: deadline, WriteDeadline: deadline,
			})
			if err != nil {
				t.Fatal(err)
			}
			client, server := net.Pipe()
			defer client.Close()
			connection := &rf3PreparedAckReaderTestConnection{Conn: server, identity: peer, key: query.ReceiverServiceKeyDigest, class: class}
			done := make(chan error, 1)
			go func() {
				done <- service.Serve(t.Context(), connection)
				_ = connection.Close()
			}()
			_ = client.SetDeadline(deadline())
			_, writeErr := client.Write(query.Marshal())
			raw, readErr := io.ReadAll(client)
			serveErr := <-done
			if name != "committed" && name != "self" {
				if serveErr == nil || len(raw) != 0 {
					t.Fatalf("unauthorized receiver got %d bytes, error=%v", len(raw), serveErr)
				}
				return
			}
			if writeErr != nil || readErr != nil || serveErr != nil {
				t.Fatalf("source read: write=%v read=%v serve=%v", writeErr, readErr, serveErr)
			}
			if _, err := frontenddrain.OpenPreparedAckCutReadResponse(raw, query); err != nil {
				t.Fatal(err)
			}
		})
	}
}
