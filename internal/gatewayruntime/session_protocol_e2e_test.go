package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	errSessionProtocolGrant        = errors.New("session protocol test: issuer grant rejected")
	errSessionProtocolSequence     = errors.New("session protocol test: non-monotonic issuer sequence")
	errSessionProtocolConflict     = errors.New("session protocol test: request identity conflict")
	errSessionProtocolAck          = errors.New("session protocol test: acknowledgement rejected")
	errSessionProtocolAcknowledged = errors.New("session protocol test: result already acknowledged")
	errSessionProtocolOutcome      = errors.New("session protocol test: terminal response lost")
)

// sessionProtocolSharedState is a test-only protocol oracle shared by two
// handler instances. It verifies transport continuity and exact authority
// forwarding; it is deliberately not a substitute for the production RF3
// issuer-authority adapter.
type sessionProtocolSharedState struct {
	mu sync.Mutex

	open  gateway.ReplicatedIssuerOpen
	grant gateway.ReplicatedIssuerLaneGrant

	highwater uint64
	records   map[uint64]*sessionProtocolRecord

	openCalls        uint64
	openApplications uint64
	execCalls        uint64
	execApplications uint64
	ackCalls         uint64
	ackApplications  uint64

	failNextOpenResponse bool
	failNextExecResponse bool
	failNextAckResponse  bool
}

type sessionProtocolRecord struct {
	authority serviceauthz.Authority
	identity  durableExecBatchIdentity
	queries   replication.Digest
	result    gateway.Result
	ack       durableExecBatchAckWireRequest
	response  durableExecBatchAckWireResponse
	acked     bool
}

type sessionProtocolService struct {
	shared *sessionProtocolSharedState
}

func newSessionProtocolSharedState() *sessionProtocolSharedState {
	return &sessionProtocolSharedState{
		open: gateway.ReplicatedIssuerOpen{
			Installation: replication.ID128{0x11}, Epoch: 1, LaneOrdinal: 7,
		},
		grant: gateway.ReplicatedIssuerLaneGrant{
			Installation: replication.ID128{0x11}, Epoch: 1, LaneOrdinal: 7,
			Lane: requestledger.IssuerLane{0x21}, Scope: requestledger.ScopeAuthenticated,
			TenantDigest: requestledger.Digest{0x31}, Revision: 1,
			GrantDigest: replication.Digest{0x61},
		},
		records: make(map[uint64]*sessionProtocolRecord),
	}
}

func (state *sessionProtocolSharedState) OpenIssuer(
	_ context.Context,
	authority serviceauthz.Authority,
	open gateway.ReplicatedIssuerOpen,
) (gateway.ReplicatedIssuerLaneGrant, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.openCalls++
	if !authority.Valid() || open != state.open {
		return gateway.ReplicatedIssuerLaneGrant{}, errSessionProtocolGrant
	}
	principal := requestledger.PrincipalID(authority.Node)
	if state.grant.Principal == (requestledger.PrincipalID{}) {
		state.grant.Principal = principal
		state.openApplications++
	} else if state.grant.Principal != principal {
		return gateway.ReplicatedIssuerLaneGrant{}, errSessionProtocolGrant
	}
	if state.failNextOpenResponse {
		state.failNextOpenResponse = false
		return gateway.ReplicatedIssuerLaneGrant{}, errSessionProtocolOutcome
	}
	return state.grant, nil
}

func (state *sessionProtocolSharedState) ExecBatch(
	_ context.Context,
	authority serviceauthz.Authority,
	identity durableExecBatchIdentity,
	queries []gateway.Query,
) (durableExecBatchExecuteResult, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.execCalls++
	if !authority.Valid() || requestledger.PrincipalID(authority.Node) != state.grant.Principal ||
		identity.Reference != state.referenceLocked() {
		return durableExecBatchExecuteResult{}, errSessionProtocolGrant
	}
	digest := sessionProtocolQueryDigest(queries)
	if identity.IssuerSequence <= state.highwater {
		record := state.records[identity.IssuerSequence]
		if record == nil || record.authority != authority || record.identity != identity ||
			record.queries != digest {
			return durableExecBatchExecuteResult{}, errSessionProtocolConflict
		}
		if record.acked {
			return durableExecBatchExecuteResult{}, errSessionProtocolAcknowledged
		}
		return sessionProtocolExecuteResult(record), nil
	}
	if identity.IssuerSequence != state.highwater+1 {
		return durableExecBatchExecuteResult{}, errSessionProtocolSequence
	}
	resultDigest := sessionProtocolDomainDigest("result", digest, identity.IssuerSequence)
	tokenDigest := sessionProtocolDomainDigest("ack", digest, identity.IssuerSequence)
	record := &sessionProtocolRecord{
		authority: authority,
		identity:  identity,
		queries:   digest,
		result: gateway.Result{
			Kind:          shardservice.ResponseCompletion,
			RowsAffected:  1,
			TransactionID: identity.RequestID,
			RouteKind:     distribution.RouteScatter,
			Generation:    9,
			ShardsFanned:  2,
		},
		ack: durableExecBatchAckWireRequest{
			Identity: durableExecBatchAckIdentity{
				RequestID:      identity.RequestID,
				RequestDigest:  digest,
				Reference:      identity.Reference,
				IssuerSequence: identity.IssuerSequence,
			},
			TerminalRevision: 100 + identity.IssuerSequence,
			ResultDigest:     resultDigest,
			AckToken:         requestledger.AckToken(tokenDigest),
		},
	}
	record.response = durableExecBatchAckWireResponse{
		durableExecBatchAckWireRequest: record.ack,
		Applied:                        1000 + identity.IssuerSequence,
		CollectionRounds:               1,
	}
	state.records[identity.IssuerSequence] = record
	state.highwater = identity.IssuerSequence
	state.execApplications++
	if state.failNextExecResponse {
		state.failNextExecResponse = false
		return durableExecBatchExecuteResult{}, errors.Join(
			gateway.ErrDurableRequestUnresolved, errSessionProtocolOutcome,
		)
	}
	return sessionProtocolExecuteResult(record), nil
}

func (state *sessionProtocolSharedState) AckExecBatch(
	_ context.Context,
	authority serviceauthz.Authority,
	request durableExecBatchAckWireRequest,
) (durableExecBatchAckWireResponse, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.ackCalls++
	record := state.records[request.Identity.IssuerSequence]
	if record == nil || record.authority != authority || request != record.ack {
		return durableExecBatchAckWireResponse{}, errSessionProtocolAck
	}
	if !record.acked {
		record.acked = true
		state.ackApplications++
		if state.failNextAckResponse {
			state.failNextAckResponse = false
			return durableExecBatchAckWireResponse{}, errSessionProtocolOutcome
		}
	}
	return record.response, nil
}

func (service *sessionProtocolService) OpenIssuer(
	ctx context.Context,
	authority serviceauthz.Authority,
	open gateway.ReplicatedIssuerOpen,
) (gateway.ReplicatedIssuerLaneGrant, error) {
	return service.shared.OpenIssuer(ctx, authority, open)
}

func (service *sessionProtocolService) ExecBatch(
	ctx context.Context,
	authority serviceauthz.Authority,
	identity durableExecBatchIdentity,
	queries []gateway.Query,
) (durableExecBatchExecuteResult, error) {
	return service.shared.ExecBatch(ctx, authority, identity, queries)
}

func (service *sessionProtocolService) AckExecBatch(
	ctx context.Context,
	authority serviceauthz.Authority,
	request durableExecBatchAckWireRequest,
) (durableExecBatchAckWireResponse, error) {
	return service.shared.AckExecBatch(ctx, authority, request)
}

func (state *sessionProtocolSharedState) referenceLocked() gateway.ReplicatedIssuerReference {
	return gateway.ReplicatedIssuerReference{
		Installation: state.grant.Installation,
		Epoch:        state.grant.Epoch,
		LaneOrdinal:  state.grant.LaneOrdinal,
		GrantDigest:  state.grant.GrantDigest,
	}
}

func sessionProtocolQueryDigest(queries []gateway.Query) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("vibedb/session-protocol-test/query\x00"))
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], uint64(len(queries)))
	_, _ = hasher.Write(fixed[:])
	for index := range queries {
		binary.LittleEndian.PutUint64(fixed[:], uint64(len(queries[index].SQL)))
		_, _ = hasher.Write(fixed[:])
		_, _ = hasher.Write([]byte(queries[index].SQL))
		binary.LittleEndian.PutUint64(fixed[:], uint64(len(queries[index].Params)))
		_, _ = hasher.Write(fixed[:])
	}
	var digest replication.Digest
	_ = hasher.Sum(digest[:0])
	return digest
}

func sessionProtocolDomainDigest(
	domain string,
	digest replication.Digest,
	sequence uint64,
) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("vibedb/session-protocol-test/"))
	_, _ = hasher.Write([]byte(domain))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(digest[:])
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], sequence)
	_, _ = hasher.Write(fixed[:])
	var result replication.Digest
	_ = hasher.Sum(result[:0])
	return result
}

func sessionProtocolExecuteResult(record *sessionProtocolRecord) durableExecBatchExecuteResult {
	result := record.result
	return durableExecBatchExecuteResult{Result: &result, Ack: record.ack}
}

func TestDurableSessionProtocolRecoversAcrossGatewayAndAuthenticatesAck(t *testing.T) {
	shared := newSessionProtocolSharedState()
	gatewayA := &sessionProtocolService{shared: shared}
	gatewayB := &sessionProtocolService{shared: shared}
	authority := serviceauthz.Authority{Node: [16]byte{0x41}, Generation: 13}
	otherAuthority := serviceauthz.Authority{Node: [16]byte{0x42}, Generation: 13}

	openRequest := sessionProtocolIssuerOpenRequest(t, shared.open)
	shared.mu.Lock()
	shared.failNextOpenResponse = true
	shared.mu.Unlock()
	if lost := sessionProtocolRoundTrip(t, gatewayA, authority, openRequest, true); !bytes.Contains(lost, []byte(errSessionProtocolOutcome.Error())) {
		t.Fatalf("lost issuer-open response = %s", lost)
	}
	openB := sessionProtocolRoundTrip(t, gatewayB, authority, openRequest, true)
	openA := sessionProtocolRoundTrip(t, gatewayA, authority, openRequest, true)
	if !bytes.Equal(openA, openB) ||
		!bytes.Contains(openA, []byte(`"installation_id":"11000000000000000000000000000000"`)) ||
		!bytes.Contains(openA, []byte(`"issuer_epoch":1`)) ||
		!bytes.Contains(openA, []byte(`"lane_ordinal":7`)) ||
		!bytes.Contains(openA, []byte(`"grant_digest":"6100000000000000000000000000000000000000000000000000000000000000"`)) {
		t.Fatalf("gateway handshake did not reopen exact contract: A=%s B=%s", openA, openB)
	}

	shared.mu.Lock()
	reference := shared.referenceLocked()
	shared.failNextExecResponse = true
	shared.mu.Unlock()
	requestID1 := replication.ID128{1}
	request1 := sessionProtocolExecRequest(t, requestID1, reference, 1,
		[]byte(`DELETE FROM docs WHERE id = 1`))
	unknown := sessionProtocolRoundTrip(t, gatewayA, authority, request1, true)
	if !bytes.Contains(unknown, []byte(errSessionProtocolOutcome.Error())) ||
		bytes.Contains(unknown, []byte(`"outcome_unknown":true`)) ||
		bytes.Contains(unknown, []byte(`"ack_token"`)) {
		t.Fatalf("ambiguous first response = %s", unknown)
	}

	replayed := sessionProtocolRoundTrip(t, gatewayB, authority, request1, true)
	if !bytes.Contains(replayed, []byte(`"committed":true`)) ||
		!bytes.Contains(replayed, []byte(`"issuer_sequence":1`)) ||
		!bytes.Contains(replayed, []byte(`"terminal_revision":101`)) ||
		!bytes.Contains(replayed, []byte(`"ack_token"`)) {
		t.Fatalf("replacement gateway replay = %s", replayed)
	}
	shared.mu.Lock()
	if shared.execApplications != 1 {
		shared.mu.Unlock()
		t.Fatalf("terminal execution applications = %d, want 1", shared.execApplications)
	}
	shared.mu.Unlock()

	conflict := sessionProtocolExecRequest(t, requestID1, reference, 1,
		[]byte(`DELETE FROM docs WHERE id = 2`))
	if response := sessionProtocolRoundTrip(t, gatewayB, authority, conflict, true); !bytes.Contains(response, []byte(errSessionProtocolConflict.Error())) {
		t.Fatalf("same sequence with different program = %s", response)
	}
	requestID3 := replication.ID128{3}
	gap := sessionProtocolExecRequest(t, requestID3, reference, 3,
		[]byte(`DELETE FROM docs WHERE id = 3`))
	if response := sessionProtocolRoundTrip(t, gatewayB, authority, gap, true); !bytes.Contains(response, []byte(errSessionProtocolSequence.Error())) {
		t.Fatalf("issuer gap = %s", response)
	}
	requestID2 := replication.ID128{2}
	sequence2 := sessionProtocolExecRequest(t, requestID2, reference, 2,
		[]byte(`DELETE FROM docs WHERE id = 2`))
	if response := sessionProtocolRoundTrip(t, gatewayB, authority, sequence2, true); !bytes.Contains(response, []byte(`"issuer_sequence":2`)) ||
		!bytes.Contains(response, []byte(`"committed":true`)) {
		t.Fatalf("contiguous issuer sequence = %s", response)
	}
	forgedReference := reference
	forgedReference.GrantDigest[0] ^= 0xff
	forged := sessionProtocolExecRequest(t, replication.ID128{4}, forgedReference, 3,
		[]byte(`DELETE FROM docs WHERE id = 4`))
	if response := sessionProtocolRoundTrip(t, gatewayB, authority, forged, true); !bytes.Contains(response, []byte(errSessionProtocolGrant.Error())) {
		t.Fatalf("forged issuer grant = %s", response)
	}
	foreign := sessionProtocolExecRequest(t, replication.ID128{5}, reference, 3,
		[]byte(`DELETE FROM docs WHERE id = 5`))
	if response := sessionProtocolRoundTrip(t, gatewayB, otherAuthority, foreign, true); !bytes.Contains(response, []byte(errSessionProtocolGrant.Error())) {
		t.Fatalf("issuer grant crossed principals = %s", response)
	}

	shared.mu.Lock()
	record := *shared.records[1]
	shared.mu.Unlock()
	ack := sessionProtocolAckRequest(t, record.ack)
	if response := sessionProtocolRoundTrip(t, gatewayB, otherAuthority, ack, true); !bytes.Contains(response, []byte(errSessionProtocolAck.Error())) {
		t.Fatalf("foreign principal ACK = %s", response)
	}
	wrongToken := record.ack
	wrongToken.AckToken[0] ^= 0xff
	if response := sessionProtocolRoundTrip(t, gatewayB, authority,
		sessionProtocolAckRequest(t, wrongToken), true); !bytes.Contains(response, []byte(errSessionProtocolAck.Error())) {
		t.Fatalf("wrong token ACK = %s", response)
	}
	wrongTerminal := record.ack
	wrongTerminal.TerminalRevision++
	if response := sessionProtocolRoundTrip(t, gatewayB, authority,
		sessionProtocolAckRequest(t, wrongTerminal), true); !bytes.Contains(response, []byte(errSessionProtocolAck.Error())) {
		t.Fatalf("wrong terminal ACK = %s", response)
	}
	wrongResult := record.ack
	wrongResult.ResultDigest[0] ^= 0xff
	if response := sessionProtocolRoundTrip(t, gatewayB, authority,
		sessionProtocolAckRequest(t, wrongResult), true); !bytes.Contains(response, []byte(errSessionProtocolAck.Error())) {
		t.Fatalf("wrong result ACK = %s", response)
	}

	shared.mu.Lock()
	shared.failNextAckResponse = true
	shared.mu.Unlock()
	if response := sessionProtocolRoundTrip(t, gatewayA, authority, ack, true); !bytes.Contains(response, []byte(errSessionProtocolOutcome.Error())) {
		t.Fatalf("lost ACK response = %s", response)
	}
	retryACK := sessionProtocolRoundTrip(t, gatewayB, authority, ack, true)
	retryACKAgain := sessionProtocolRoundTrip(t, gatewayB, authority, ack, true)
	if !bytes.Equal(retryACK, retryACKAgain) ||
		!bytes.Contains(retryACK, []byte(`"ok":true,"op":"ack_exec_batch"`)) ||
		!bytes.Contains(retryACK, []byte(`"collection_rounds":1`)) {
		t.Fatalf("idempotent ACK retry: first=%s second=%s", retryACK, retryACKAgain)
	}
	shared.mu.Lock()
	defer shared.mu.Unlock()
	if shared.openApplications != 1 || shared.highwater != 2 ||
		shared.execApplications != 2 || shared.ackApplications != 1 {
		t.Fatalf("shared state: opens=%d highwater=%d executions=%d ACKs=%d",
			shared.openApplications, shared.highwater, shared.execApplications, shared.ackApplications)
	}
}

func TestDurableSessionProtocolRejectsMalformedAndUnauthorizedBeforeService(t *testing.T) {
	shared := newSessionProtocolSharedState()
	service := &sessionProtocolService{shared: shared}
	authority := serviceauthz.Authority{Node: [16]byte{0x51}, Generation: 17}
	shared.mu.Lock()
	reference := shared.referenceLocked()
	shared.mu.Unlock()
	valid := sessionProtocolExecRequest(t, replication.ID128{9}, reference, 1,
		[]byte(`DELETE FROM docs WHERE id = 9`))

	requestIDField := []byte(`"request_id":"09000000000000000000000000000000"`)
	installationField := []byte(`"installation_id":"11000000000000000000000000000000"`)
	epochField := []byte(`"issuer_epoch":1`)
	grantField := append([]byte(`"grant_digest":"`), []byte(hex.EncodeToString(reference.GrantDigest[:]))...)
	grantField = append(grantField, '"')
	malformed := [][]byte{
		bytes.Replace(bytes.Clone(valid),
			append(append(bytes.Clone(requestIDField), ','), installationField...),
			append(append(bytes.Clone(installationField), ','), requestIDField...), 1),
		bytes.Replace(bytes.Clone(valid), epochField,
			append(append(bytes.Clone(epochField), ','), epochField...), 1),
		bytes.Replace(bytes.Clone(valid), grantField,
			append(append(bytes.Clone(grantField), ','), []byte(`"principal":"spoof"`)...), 1),
		bytes.Replace(bytes.Clone(valid), grantField,
			append(append(bytes.Clone(grantField), ','), []byte(`"tenant":"spoof"`)...), 1),
		bytes.Replace(bytes.Clone(valid), []byte(`"issuer_sequence":1`), []byte(`"issuer_sequence":0`), 1),
		bytes.Replace(bytes.Clone(valid), grantField, []byte(`"grant_digest":"00"`), 1),
		[]byte(`{"op":"exec_batch","request_id":"09000000000000000000000000000000","statements":[{"sql":"DELETE FROM docs WHERE id = 9"}]}`),
		[]byte(`{"op":"exec_batch","lane_ordinal":0,"statements":[{"sql":"DELETE FROM docs WHERE id = 9"}]}`),
		[]byte(`{"op":"exec_batch","installation_id":"","issuer_epoch":0,"lane_ordinal":0,"grant_digest":"","issuer_sequence":0,"statements":[{"sql":"DELETE FROM docs WHERE id = 9"}]}`),
	}
	shared.mu.Lock()
	execCallsBefore := shared.execCalls
	shared.mu.Unlock()
	for index := range malformed {
		response := sessionProtocolRoundTrip(t, service, authority, malformed[index], true)
		if !bytes.Contains(response, []byte(errInvalidDurableExecBatch.Error())) {
			t.Fatalf("malformed case %d accepted: request=%s response=%s", index, malformed[index], response)
		}
	}
	shared.mu.Lock()
	if shared.execCalls != execCallsBefore {
		t.Fatalf("malformed durable identity reached service: before=%d after=%d", execCallsBefore, shared.execCalls)
	}
	shared.mu.Unlock()

	validIssuer := sessionProtocolIssuerOpenRequest(t, shared.open)
	invalidIssuer := bytes.Replace(bytes.Clone(validIssuer), []byte(`"lane_ordinal":7`),
		[]byte(`"lane_ordinal":7,"lane_ordinal":7`), 1)
	if response := sessionProtocolRoundTrip(t, service, authority, invalidIssuer, true); !bytes.Contains(response, []byte(errInvalidIssuerOpen.Error())) {
		t.Fatalf("duplicate issuer field response = %s", response)
	}
	invalidACK := []byte(validDurableExecBatchAckFixture[:len(validDurableExecBatchAckFixture)-1] +
		`,"principal":"spoof"}`)
	if response := sessionProtocolRoundTrip(t, service, authority, invalidACK, true); !bytes.Contains(response, []byte(errInvalidDurableExecBatchAckRequest.Error())) {
		t.Fatalf("spoofed ACK response = %s", response)
	}

	if response := sessionProtocolRoundTrip(t, service, authority,
		validIssuer, false); !bytes.Contains(response, []byte(`"error":"authorization denied"`)) {
		t.Fatalf("denied issuer response = %s", response)
	}
	if response := sessionProtocolRoundTrip(t, service, serviceauthz.Authority{},
		validIssuer, true); !bytes.Contains(response, []byte(errInvalidIssuerOpen.Error())) {
		t.Fatalf("unauthenticated issuer response = %s", response)
	}
	if response := sessionProtocolRoundTrip(t, service, authority, valid, false); !bytes.Contains(response, []byte(`"error":"authorization denied"`)) {
		t.Fatalf("denied exec response = %s", response)
	}

	assertSessionProtocolExactBound(t, validIssuer,
		maxIssuerOpenRequestBytes, func(raw []byte) error {
			var request issuerOpenWireRequest
			return decodeIssuerOpenRequest(raw, &request)
		})
	assertSessionProtocolExactBound(t, []byte(validDurableExecBatchAckFixture),
		maxDurableExecBatchAckRequestBytes, func(raw []byte) error {
			var request durableExecBatchAckWireRequest
			return decodeDurableExecBatchAckRequest(raw, &request)
		})
	assertSessionProtocolExactBound(t, valid, maxServeRequestBytes,
		validateDurableExecBatchEnvelope)

	shared.mu.Lock()
	defer shared.mu.Unlock()
	if shared.openCalls != 0 || shared.execCalls != 0 || shared.ackCalls != 0 {
		t.Fatalf("rejected input reached service: open=%d exec=%d ack=%d",
			shared.openCalls, shared.execCalls, shared.ackCalls)
	}
}

func sessionProtocolRoundTrip(
	t *testing.T,
	service durableRequestService,
	authority serviceauthz.Authority,
	request []byte,
	authorized bool,
) []byte {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	if authority.Valid() {
		var err error
		ctx, err = serviceauthz.WithAuthority(ctx, authority)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
	}
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		handleConnPolicyDurable(ctx, server, nil, nil, service,
			func(string, ...any) {}, func(serviceauthz.Capability) bool { return authorized })
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		cancel()
		_ = client.Close()
		<-done
		t.Fatal(err)
	}
	line := make([]byte, 0, len(request)+1)
	line = append(line, request...)
	line = append(line, '\n')
	if _, err := client.Write(line); err != nil {
		cancel()
		_ = client.Close()
		<-done
		t.Fatal(err)
	}
	response, err := bufio.NewReader(client).ReadBytes('\n')
	_ = client.Close()
	cancel()
	<-done
	if err != nil {
		t.Fatalf("read response for %s: %v", request, err)
	}
	if !vibejson.Valid(bytes.TrimSpace(response)) {
		t.Fatalf("invalid vibejson response: %s", response)
	}
	return response
}

func sessionProtocolExecRequest(
	t *testing.T,
	requestID replication.ID128,
	reference gateway.ReplicatedIssuerReference,
	sequence uint64,
	sql []byte,
) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(writer.BeginObject())
	must(writer.Key("op"))
	must(writer.RawUnchecked([]byte(`"exec_batch"`)))
	must(writeDurableExecBatchAckHexField(writer, "request_id", requestID[:]))
	must(writeDurableExecBatchAckHexField(writer, "installation_id", reference.Installation[:]))
	must(writeDurableExecBatchAckUintField(writer, "issuer_epoch", reference.Epoch))
	must(writeDurableExecBatchAckUintField(writer, "lane_ordinal", uint64(reference.LaneOrdinal)))
	must(writeDurableExecBatchAckHexField(writer, "grant_digest", reference.GrantDigest[:]))
	must(writeDurableExecBatchAckUintField(writer, "issuer_sequence", sequence))
	must(writer.Key("statements"))
	must(writer.BeginArray())
	must(writer.BeginObject())
	must(writer.Key("sql"))
	must(writer.String(string(sql)))
	must(writer.EndObject())
	must(writer.EndArray())
	must(writer.EndObject())
	must(writer.Flush())
	return bytes.Clone(output.Bytes())
}

func sessionProtocolIssuerOpenRequest(
	t *testing.T,
	open gateway.ReplicatedIssuerOpen,
) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(writer.BeginObject())
	must(writer.Key("op"))
	must(writer.RawUnchecked([]byte(`"issuer_open"`)))
	must(writeDurableExecBatchAckHexField(writer, "installation_id", open.Installation[:]))
	must(writeDurableExecBatchAckUintField(writer, "issuer_epoch", open.Epoch))
	must(writeDurableExecBatchAckUintField(writer, "lane_ordinal", uint64(open.LaneOrdinal)))
	must(writer.EndObject())
	must(writer.Flush())
	return bytes.Clone(output.Bytes())
}

func sessionProtocolAckRequest(
	t *testing.T,
	request durableExecBatchAckWireRequest,
) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(writer.BeginObject())
	must(writer.Key("op"))
	must(writer.RawUnchecked([]byte(`"ack_exec_batch"`)))
	must(writeDurableExecBatchAckHexField(writer, "request_id", request.Identity.RequestID[:]))
	must(writeDurableExecBatchAckHexField(writer, "request_digest", request.Identity.RequestDigest[:]))
	must(writeDurableExecBatchAckHexField(writer, "installation_id", request.Identity.Reference.Installation[:]))
	must(writeDurableExecBatchAckUintField(writer, "issuer_epoch", request.Identity.Reference.Epoch))
	must(writeDurableExecBatchAckUintField(writer, "lane_ordinal", uint64(request.Identity.Reference.LaneOrdinal)))
	must(writeDurableExecBatchAckHexField(writer, "grant_digest", request.Identity.Reference.GrantDigest[:]))
	must(writeDurableExecBatchAckUintField(writer, "issuer_sequence", request.Identity.IssuerSequence))
	must(writeDurableExecBatchAckUintField(writer, "terminal_revision", request.TerminalRevision))
	must(writeDurableExecBatchAckHexField(writer, "result_digest", request.ResultDigest[:]))
	must(writeDurableExecBatchAckHexField(writer, "ack_token", request.AckToken[:]))
	must(writer.EndObject())
	must(writer.Flush())
	return bytes.Clone(output.Bytes())
}

func assertSessionProtocolExactBound(
	t *testing.T,
	valid []byte,
	maximum int,
	decode func([]byte) error,
) {
	t.Helper()
	if len(valid) > maximum {
		t.Fatalf("valid fixture length %d exceeds bound %d", len(valid), maximum)
	}
	atBound := make([]byte, maximum)
	copy(atBound, valid)
	for index := len(valid); index < len(atBound); index++ {
		atBound[index] = ' '
	}
	if err := decode(atBound); err != nil {
		t.Fatalf("exact bound %d rejected: %v", maximum, err)
	}
	if err := decode(append(atBound, ' ')); err == nil {
		t.Fatalf("bound %d accepted %d bytes", maximum, maximum+1)
	}
}
