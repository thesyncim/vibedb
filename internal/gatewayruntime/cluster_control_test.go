package gatewayruntime

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type clusterControlTestBackend struct {
	called   bool
	request  clustercontrol.Request
	response clustercontrol.Response
}

func (backend *clusterControlTestBackend) ExecuteClusterControl(_ context.Context, request clustercontrol.Request) clustercontrol.Response {
	backend.called = true
	backend.request = request
	return backend.response
}

func TestClusterControlServeLineUsesCanonicalNDJSONAndCapabilityFence(t *testing.T) {
	requestID, err := clustercontrol.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	request := clustercontrol.Request{Format: clustercontrol.Format, Op: clustercontrol.OpNodes, RequestID: requestID}
	line, err := clustercontrol.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	backend := &clusterControlTestBackend{response: clustercontrol.Response{
		Format: clustercontrol.Format, Op: clustercontrol.OpNodes, OK: true, RequestID: requestID,
	}}
	var required serviceauthz.Capability
	server := &clusterControlServer{backend: backend, authorize: func(_ context.Context, capability serviceauthz.Capability) bool {
		required = capability
		return true
	}}
	serverConn, clientConn := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.ServeLine(context.Background(), serverConn, line) }()
	raw, readErr := io.ReadAll(clientConn)
	_ = clientConn.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if serveErr := <-done; serveErr != nil {
		t.Fatal(serveErr)
	}
	response, err := clustercontrol.DecodeResponse(raw)
	if err != nil {
		t.Fatalf("DecodeResponse(%q): %v", raw, err)
	}
	if !response.OK || response.RequestID != requestID || !backend.called || backend.request != request {
		t.Fatalf("unexpected cluster control dispatch: response=%+v called=%v request=%+v", response, backend.called, backend.request)
	}
	if required != serviceauthz.CapabilityTopology {
		t.Fatalf("nodes capability = %v, want topology", required)
	}
}

func TestClusterControlServeLineRejectsMembershipWithoutCallingBackend(t *testing.T) {
	requestID, err := clustercontrol.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	request := clustercontrol.Request{Format: clustercontrol.Format, Op: clustercontrol.OpRebalance,
		RequestID: requestID, MaxMoves: 1, MaxMigrationBytes: 1}
	line, err := clustercontrol.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	backend := &clusterControlTestBackend{}
	var required serviceauthz.Capability
	server := &clusterControlServer{backend: backend, authorize: func(_ context.Context, capability serviceauthz.Capability) bool {
		required = capability
		return false
	}}
	serverConn, clientConn := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.ServeLine(context.Background(), serverConn, line) }()
	raw, readErr := io.ReadAll(clientConn)
	_ = clientConn.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if serveErr := <-done; serveErr != nil {
		t.Fatal(serveErr)
	}
	response, err := clustercontrol.DecodeResponse(raw)
	if err != nil {
		t.Fatalf("DecodeResponse(%q): %v", raw, err)
	}
	if response.OK || backend.called || response.RequestID != requestID || required != serviceauthz.CapabilityTopology|serviceauthz.CapabilityMembership {
		t.Fatalf("membership dispatch escaped auth fence: response=%+v called=%v required=%v", response, backend.called, required)
	}
}

func TestScalingEnrollmentIDIsStableAcrossDurableRecoveryPhases(t *testing.T) {
	var parent [32]byte
	parent[0] = 1
	row := gateway.GroupEnrollmentIntent{IntentID: [32]byte{2}, State: gateway.EnrollmentReserved, Revision: 1}
	reserved := scalingEnrollmentID(parent, row)
	row.PreparationClaim = [32]byte{9}
	if got := scalingEnrollmentID(parent, row); got != reserved {
		t.Fatalf("preparation claim changed enrollment owner: reserved=%x claimed=%x", reserved, got)
	}
	row.PreparationClaim = [32]byte{}
	row.State = gateway.EnrollmentPrepared
	row.Revision = 2
	row.Proof = &gateway.PreparedReplicaProof{IntentID: row.IntentID}
	row.MoveOperationID = [32]byte{3}
	if got := scalingEnrollmentID(parent, row); got != reserved {
		t.Fatalf("phase transition changed enrollment owner: reserved=%x prepared=%x", reserved, got)
	}
}
