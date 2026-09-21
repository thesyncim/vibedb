package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

type recoveringGatewayScanner struct{ calls int }

func (scanner *recoveringGatewayScanner) ScanGatewayParticipant(context.Context, NodeRecord) (GatewayParticipantEvidence, error) {
	scanner.calls++
	return GatewayParticipantEvidence{}, ErrScalingState
}

func TestEnrollmentCatalogCutDoesNotRequireRecoveringGateway(t *testing.T) {
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	node := scalingTestNodeRecord([16]byte{0x31}, 1, NodeJoining, 1)
	node.Roles |= NodeRoleGateway
	node.GatewayEndpoint, node.GatewayAddress = distribution.EndpointID("recovering"), "127.0.0.1:8301"
	node.Gateway = GatewayIdentity{NodeID: node.NodeID, Incarnation: node.Incarnation,
		ServiceKeyDigest: node.ServiceKeyDigest, ServiceID: [16]byte{1}, SessionID: [16]byte{2},
		SessionRevision: 1, ParticipantDigest: replication.Digest{3}}
	if err := authority.PutNode(t.Context(), node, 0); err != nil {
		t.Fatal(err)
	}
	node.Lifecycle, node.Revision = NodeActive, 2
	if err := authority.PutNode(t.Context(), node, 1); err != nil {
		t.Fatal(err)
	}
	intent := scalingTestEnrollmentIntent(1, node.NodeID[0], node.Revision)
	if err := authority.SubmitEnrollmentIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	scanner := &recoveringGatewayScanner{}
	authority.gatewayParticipants = scanner
	if _, err := authority.ScanNodeReferences(t.Context(), node.NodeID, node.Incarnation); !errors.Is(err, ErrScalingState) || scanner.calls != 1 {
		t.Fatalf("drain scan must still require the live gateway: calls=%d err=%v", scanner.calls, err)
	}
	cut, err := authority.ReadEnrollmentCatalogCut(t.Context())
	expectedDigest, _ := ReplicatedCatalogHeadDigest(snapshot)
	if err != nil || cut.CatalogGeneration != snapshot.Generation() || cut.CatalogHeadDigest != expectedDigest ||
		cut.EnrollmentDirectoryDigest == (replication.Digest{}) || scanner.calls != 1 {
		t.Fatalf("recovery metadata depended on the recovering gateway: cut=%+v calls=%d err=%v", cut, scanner.calls, err)
	}
}
