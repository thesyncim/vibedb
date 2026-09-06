package clustercontrol

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

const testRequestID = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
const testOperationID = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"

func testDescriptor() NodeDescriptor {
	return NodeDescriptor{
		Format: Format, NodeID: "11111111111111111111111111111111", Incarnation: 7,
		ServiceKeyDigest: strings.Repeat("1", 64),
		FailureDomain:    "rack-a", Roles: []string{"control", "gateway", "storage"},
		DataEndpoint: "data-node-7", NativeEndpoint: "native-node-7", ControlEndpoint: "control-node-7",
		GatewayEndpoint: "gateway-node-7", DataAddress: "127.0.0.1:21001", NativeAddress: "127.0.0.1:21002",
		ControlAddress: "127.0.0.1:21003", GatewayAddress: "127.0.0.1:21004",
		Capacity: [7]uint64{64, 64, 64, 64, 64, 64, 64}, MigrationCapacity: 1 << 30, MaxReceives: 2,
	}
}

func TestRequestRoundTripUsesCanonicalBoundedGatewayLine(t *testing.T) {
	request := Request{Format: Format, Op: OpJoin, RequestID: testRequestID}
	descriptor := testDescriptor()
	request.NodeDescriptor = &descriptor
	raw, err := EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if raw[len(raw)-1] != '\n' || len(raw) > MaxRequestBytes {
		t.Fatalf("line=%q", raw)
	}
	decoded, err := DecodeRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Op != OpJoin || decoded.RequestID != testRequestID || decoded.NodeDescriptor == nil ||
		!decoded.NodeDescriptor.Valid() {
		t.Fatalf("decoded=%+v", decoded)
	}
	if _, err := DecodeRequest(append([]byte(`{"request_id":"`+testRequestID+`","format":1,"op":"cluster_nodes"}`), '\n')); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("noncanonical field order accepted: %v", err)
	}
	if _, err := DecodeRequest([]byte(`{"format":1,"op":"cluster_nodes","request_id":"` + testRequestID + `","unknown":true}`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown field accepted: %v", err)
	}
}

func TestRequestValidationRequiresOperationSpecificFields(t *testing.T) {
	cases := []Request{
		{Format: Format, Op: OpNodes, RequestID: testRequestID, NodeID: "11111111111111111111111111111111", NodeIncarnation: 1},
		{Format: Format, Op: OpJoin, RequestID: testRequestID},
		{Format: Format, Op: OpRebalance, RequestID: testRequestID, MaxMoves: 1},
		{Format: Format, Op: OpDecommission, RequestID: testRequestID, NodeID: "11111111111111111111111111111111"},
		{Format: Format, Op: OpStatus, RequestID: testRequestID},
		{Format: Format, Op: "cluster_unknown", RequestID: testRequestID},
	}
	for index, request := range cases {
		if request.Valid() {
			t.Errorf("case %d unexpectedly valid: %+v", index, request)
		}
	}
	valid := Request{Format: Format, Op: OpRebalance, RequestID: testRequestID, MaxMoves: 4, MaxMigrationBytes: 1 << 20}
	if !valid.Valid() {
		t.Fatal("valid rebalance rejected")
	}
	valid.Op, valid.OperationID = OpStatus, testOperationID
	valid.MaxMoves, valid.MaxMigrationBytes = 0, 0
	if !valid.Valid() {
		t.Fatal("valid status rejected")
	}
}

func TestNodeDescriptorRejectsSecretBearingUnknownFields(t *testing.T) {
	descriptor := testDescriptor()
	raw, err := vibejson.Marshal(&descriptor)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.TrimSuffix(raw, []byte("}"))
	raw = append(raw, []byte(`,"key":"-----BEGIN PRIVATE KEY-----"}`)...)
	path := filepath.Join(t.TempDir(), "node.vibejson")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNodeDescriptor(path); !errors.Is(err, ErrInvalidNodeDescriptor) {
		t.Fatalf("secret-bearing descriptor accepted: %v", err)
	}
}

func TestResponseRoundTripCarriesSafeToStopProofAndBudget(t *testing.T) {
	response := Response{
		Format: Format, Op: OpStatus, OK: true, RequestID: testRequestID, OperationID: testOperationID,
		State: "running", CatalogGeneration: 4, DirectoryRevision: 9,
		Nodes:    []NodeStatus{{NodeID: "11111111111111111111111111111111", Incarnation: 7, Lifecycle: "draining", Revision: 9, CatalogGeneration: 4, SafeToStop: false}},
		Blockers: []Blocker{{Code: "gateway_session", Detail: "retiring frontend still has one authenticated session", NodeID: "11111111111111111111111111111111", NodeIncarnation: 7, Revision: 9}},
		Evidence: &SafeToStopEvidence{NodeID: "11111111111111111111111111111111", NodeIncarnation: 7, CatalogGeneration: 4, DirectoryRevision: 9, Digest: testOperationID},
		Budget:   &BudgetStatus{ThrottledCalls: 2, ThrottledBytes: 4096, PeakActive: 2, MaxActive: 2},
	}
	raw, err := EncodeResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.OK || decoded.OperationID != testOperationID || len(decoded.Blockers) != 1 || decoded.Budget.ThrottledCalls != 2 {
		t.Fatalf("decoded=%+v", decoded)
	}
	failed := response
	failed.OK, failed.Error = false, "blocked by authenticated session"
	if _, err := EncodeResponse(failed); err != nil {
		t.Fatalf("failed response should encode: %v", err)
	}
	failed.Error = strings.Repeat("x", MaxErrorBytes+1)
	if _, err := EncodeResponse(failed); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("oversized error accepted: %v", err)
	}
}

func TestProfileIsCanonicalAndBounded(t *testing.T) {
	profile := Profile{Format: Format, Address: "127.0.0.1:17400", ServerNode: "11111111111111111111111111111111", Certificate: "/tmp/client-cert.pem", Key: "/tmp/client-key.pem", Roots: "/tmp/roots.pem", IdentityOID: "1.3.6.1.4.1.32473.1.1"}
	raw, err := vibejson.Marshal(&profile)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "profile.vibejson")
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProfile(path)
	if err != nil || loaded != profile {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if _, err := LoadProfile(filepath.Join("relative", "profile.vibejson")); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("relative profile accepted: %v", err)
	}
}

func TestNodesResponseRoundTrip(t *testing.T) {
	response := Response{Format: Format, Op: OpNodes, OK: true, RequestID: testRequestID, CatalogGeneration: 1, DirectoryRevision: 1,
		Nodes: []NodeStatus{
			{NodeID: "11111111111111111111111111111111", Incarnation: 1, Lifecycle: "active", Revision: 1, CatalogGeneration: 1},
			{NodeID: "22222222222222222222222222222222", Incarnation: 1, Lifecycle: "active", Revision: 1, CatalogGeneration: 1},
			{NodeID: "33333333333333333333333333333333", Incarnation: 1, Lifecycle: "active", Revision: 1, CatalogGeneration: 1},
		}}
	raw, err := EncodeResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeResponse(raw)
	if err != nil {
		t.Fatalf("nodes response: %v; encoded=%s decoded=%+v", err, raw, decoded)
	}
}
