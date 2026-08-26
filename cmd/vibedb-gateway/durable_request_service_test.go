package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

type durableRequestServiceStub struct {
	authority serviceauthz.Authority
	identity  durableExecBatchIdentity
	queries   int
	result    durableExecBatchExecuteResult
}

func (stub *durableRequestServiceStub) OpenIssuer(
	_ context.Context, authority serviceauthz.Authority, _ uint16,
) (durableIssuerOpenResult, error) {
	stub.authority = authority
	return durableIssuerOpenResult{}, errDurableExecBatchUnavailable
}

func (stub *durableRequestServiceStub) ExecBatch(
	_ context.Context,
	authority serviceauthz.Authority,
	identity durableExecBatchIdentity,
	queries []gateway.Query,
) (durableExecBatchExecuteResult, error) {
	stub.authority, stub.identity, stub.queries = authority, identity, len(queries)
	return stub.result, nil
}

func (stub *durableRequestServiceStub) AckExecBatch(
	_ context.Context, authority serviceauthz.Authority, request durableExecBatchAckWireRequest,
) (durableExecBatchAckWireResponse, error) {
	stub.authority = authority
	return durableExecBatchAckWireResponse{durableExecBatchAckWireRequest: request, Applied: 1}, nil
}

func TestIssuerOpenWireIsStrictCanonicalVibeJSON(t *testing.T) {
	var request issuerOpenWireRequest
	if err := decodeIssuerOpenRequest([]byte(`{"op":"issuer_open","lane":0}`), &request); err != nil || request.Lane != 0 {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	for _, raw := range []string{
		`{"lane":0,"op":"issuer_open"}`,
		`{"op":"issuer_open"}`,
		`{"op":"issuer_open","lane":65536}`,
		`{"op":"issuer_open","lane":0,"principal":"spoof"}`,
	} {
		if err := decodeIssuerOpenRequest([]byte(raw), &request); !errors.Is(err, errInvalidIssuerOpen) {
			t.Fatalf("raw=%s err=%v", raw, err)
		}
	}
	result := durableIssuerOpenResult{
		Installation: replication.ID128{1}, Epoch: 7,
		Lane: [8]byte{2}, Authenticator: replication.Digest{3},
	}
	var output bytes.Buffer
	if err := writeIssuerOpenResponse(vibejson.NewWriter(&output), result); err != nil {
		t.Fatal(err)
	}
	want := `{"ok":true,"op":"issuer_open","installation_id":"01000000000000000000000000000000","issuer_epoch":7,"issuer_lane":"0200000000000000","issuer_authenticator":"0300000000000000000000000000000000000000000000000000000000000000"}` + "\n"
	if output.String() != want || !vibejson.Valid(bytes.TrimSpace(output.Bytes())) {
		t.Fatalf("response=%q", output.String())
	}
}

func TestStructuredExecBatchBindsExactAuthorityAndReturnsAckHandle(t *testing.T) {
	request := serveRequest{
		Op: "exec_batch", RequestID: "01000000000000000000000000000000",
		IssuerEpoch: 7, IssuerLane: "0200000000000000", IssuerSequence: 9,
		IssuerAuthenticator: strings.Repeat("03", 32),
		Statements:          []serveStatement{{SQL: `DELETE FROM docs WHERE id = 1`}},
	}
	identity, ok := structuredExecBatchIdentity(&request)
	if !ok {
		t.Fatal("structured identity rejected")
	}
	ack := durableExecBatchAckWireRequest{
		Identity: durableExecBatchAckIdentity{
			RequestID: identity.RequestID, RequestDigest: replication.Digest{4},
			IssuerEpoch: identity.IssuerEpoch, IssuerLane: identity.IssuerLane,
			IssuerSequence: identity.IssuerSequence,
		},
		TerminalRevision: 11, ResultDigest: replication.Digest{5}, AckToken: [32]byte{6},
	}
	transactionID := replication.ID128{8}
	stub := &durableRequestServiceStub{result: durableExecBatchExecuteResult{
		Result: &gateway.Result{
			Kind: shardservice.ResponseCompletion, RouteKind: distribution.RouteTargeted,
			Generation: 3, ShardsFanned: 1, TransactionID: transactionID,
		},
		Ack: ack,
	}}
	authority := serviceauthz.Authority{Node: [16]byte{9}, Generation: 4}
	response := executeDurableExecBatch(t.Context(), stub, authority, request)
	if response.Error != "" || response.DurableAck == nil || !response.Committed ||
		stub.authority != authority || stub.identity != identity || stub.queries != 1 {
		t.Fatalf("response=%+v stub=%+v", response, stub)
	}
	var output bytes.Buffer
	if err := writeServeResponse(vibejson.NewWriter(&output), response); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"request_id":"01000000000000000000000000000000"`,
		`"request_digest":"0400000000000000000000000000000000000000000000000000000000000000"`,
		`"issuer_sequence":9`, `"terminal_revision":11`,
		`"ack_token":"0600000000000000000000000000000000000000000000000000000000000000"`,
	} {
		if !strings.Contains(output.String(), fragment) {
			t.Fatalf("response %q lacks %q", output.String(), fragment)
		}
	}
	if strings.Contains(output.String(), "issuer_authenticator") {
		t.Fatal("secret issuer grant leaked into terminal ACK handle")
	}
}

func TestStructuredExecBatchWireRejectsIdentityAmbiguityAndSpoofing(t *testing.T) {
	valid := `{"op":"exec_batch","request_id":"01000000000000000000000000000000","issuer_epoch":7,"issuer_lane":"0200000000000000","issuer_sequence":9,"issuer_authenticator":"` + strings.Repeat("03", 32) + `","class":"batch","statements":[{"sql":"DELETE FROM docs WHERE id = 1"}]}`
	if err := validateDurableExecBatchEnvelope([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	checks := []string{
		strings.Replace(valid, `"request_id":"01000000000000000000000000000000","issuer_epoch":7`, `"issuer_epoch":7,"request_id":"01000000000000000000000000000000"`, 1),
		strings.Replace(valid, `"issuer_epoch":7`, `"issuer_epoch":7,"issuer_epoch":7`, 1),
		strings.Replace(valid, `,"class":"batch"`, `,"tenant":"spoof","class":"batch"`, 1),
		strings.Replace(valid, `,"class":"batch"`, `,"principal":"spoof","class":"batch"`, 1),
		strings.Replace(valid, strings.Repeat("03", 32), strings.Repeat("03", 31), 1),
		strings.Replace(valid, strings.Repeat("03", 32), strings.Repeat("0A", 32), 1),
	}
	for index, raw := range checks {
		if err := validateDurableExecBatchEnvelope([]byte(raw)); !errors.Is(err, errInvalidDurableExecBatch) {
			t.Fatalf("case %d err=%v", index, err)
		}
	}
	oversized := append([]byte(valid), bytes.Repeat([]byte(" "), maxServeRequestBytes)...)
	if err := validateDurableExecBatchEnvelope(oversized); !errors.Is(err, errInvalidDurableExecBatch) {
		t.Fatalf("oversized err=%v", err)
	}
}

func TestArbitraryRequestIDOnlyExecBatchFailsClosed(t *testing.T) {
	request := serveRequest{
		Op: "exec_batch", RequestID: "01000000000000000000000000000000",
		Statements: []serveStatement{{SQL: `DELETE FROM docs WHERE id = 1`}},
	}
	if _, ok := structuredExecBatchIdentity(&request); ok {
		t.Fatal("request_id-only request became a structured identity")
	}
	response := execRequest(context.Background(), nil, request)
	if response.Error != errDurableExecBatchUnavailable.Error() {
		t.Fatalf("response=%+v", response)
	}
	request.IssuerEpoch = 7
	if _, ok := structuredExecBatchIdentity(&request); ok {
		t.Fatal("partial issuer identity accepted")
	}
}
