package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func replicatedRecoveryTestRecord(
	id distributedtxn.ID,
	role distributedtxn.ReplicatedRole,
	state uint8,
	kind distributedtxn.ReplicatedPayloadKind,
) replicatedstate.TransactionRecoveryRecord {
	return replicatedstate.TransactionRecoveryRecord{
		ID: id, Role: role, State: state, Revision: 1,
		PayloadKind: kind, PayloadCount: 1,
		CoordinatorGroup:            replication.ID128{1},
		CoordinatorShardIncarnation: replication.ID128{2},
		CoordinatorAllocation:       3, MutationDigest: distributedtxn.Digest{4},
	}
}

func replicatedRecoveryRequests(
	fence ReplicatedFence,
	authority serviceauthz.Authority,
) []*ReplicatedRequest {
	id := testTransactionID(211)
	return []*ReplicatedRequest{
		{Operation: ReplicatedTransactionRead, Authority: authority,
			Capability: serviceauthz.CapabilityTransactionRecovery, Fence: fence,
			TransactionRead: ReplicatedTransactionReadRequest{
				Kind: ReplicatedTransactionLookupCoordinator, ID: id,
				MinimumApplied: 7, MaxRows: 1,
				MaxBytes: replicatedstate.TransactionRecoverySummaryBytes +
					distributedtxn.MaxCoordinatorRecordBytes,
			}},
		{Operation: ReplicatedTransactionRead, Authority: authority,
			Capability: serviceauthz.CapabilityTransactionRecovery, Fence: fence,
			TransactionRead: ReplicatedTransactionReadRequest{
				Kind: ReplicatedTransactionLookupParticipant, ID: id,
				MinimumApplied: 8, MaxRows: 1,
				MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
			}},
		{Operation: ReplicatedTransactionRead, Authority: authority,
			Capability: serviceauthz.CapabilityTransactionRecovery, Fence: fence,
			TransactionRead: ReplicatedTransactionReadRequest{
				Kind: ReplicatedTransactionReadManifestPage, ID: id, SegmentIndex: 9,
				MinimumApplied: 9, MaxRows: 1,
				MaxBytes: replicatedstate.MaxTransactionRecoveryReadBytes,
			}},
		{Operation: ReplicatedTransactionRead, Authority: authority,
			Capability: serviceauthz.CapabilityTransactionRecovery, Fence: fence,
			TransactionRead: ReplicatedTransactionReadRequest{
				Kind: ReplicatedTransactionScanCoordinators, ID: id,
				MinimumApplied: 10, MaxRows: replicatedstate.MaxTransactionRecoveryScanRows,
				MaxBytes: replicatedstate.MaxTransactionRecoveryScanBytes,
			}},
	}
}

func TestReplicatedTransactionRecoveryRequestWireCanonicalAndFixed(t *testing.T) {
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17}
	for _, request := range replicatedRecoveryRequests(fence, authority) {
		var encoded, borrowed bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err != nil {
			t.Fatalf("encode kind %d: %v", request.TransactionRead.Kind, err)
		}
		if err := EncodeReplicatedRequestBorrowed(&borrowed, request); err != nil {
			t.Fatalf("borrow kind %d: %v", request.TransactionRead.Kind, err)
		}
		if !bytes.Equal(encoded.Bytes(), borrowed.Bytes()) ||
			encoded.Bytes()[0] != tagReplicatedTransactionRead ||
			encoded.Len()-5 != replicatedTransactionReadRequestBodyBytes {
			t.Fatalf("noncanonical fixed request kind=%d bytes=%d", request.TransactionRead.Kind, encoded.Len())
		}
		decoded, err := DecodeReplicatedRequest(bytes.NewReader(encoded.Bytes()))
		if err != nil || decoded.Operation != ReplicatedTransactionRead ||
			decoded.Authority != authority || decoded.Capability != request.Capability ||
			decoded.Fence != fence || decoded.TransactionRead != request.TransactionRead {
			t.Fatalf("decoded=%+v err=%v", decoded, err)
		}
	}
}

func TestReplicatedTransactionRecoveryRequestGolden(t *testing.T) {
	request := replicatedRecoveryRequests(testReplicatedFence(),
		serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17})[2]
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	got := sha256.Sum256(encoded.Bytes())
	want := [sha256.Size]byte{
		// This literal freezes the sole fixed-body request grammar.
		0x7d, 0xf4, 0x24, 0xa2, 0x8b, 0x31, 0xeb, 0x05,
		0x53, 0x9e, 0x91, 0xa1, 0xb3, 0xb0, 0x9a, 0xd5,
		0x57, 0x37, 0xe7, 0x81, 0x73, 0x28, 0xd3, 0x20,
		0x25, 0x1e, 0x14, 0xd0, 0xfd, 0x8d, 0x22, 0x12,
	}
	if got != want {
		t.Fatalf("recovery request golden digest=%x; update only for an intentional grammar change", got)
	}
}

func TestReplicatedTransactionRecoveryRequestRejectsMalformedBeforeDispatch(t *testing.T) {
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17}
	base := *replicatedRecoveryRequests(fence, authority)[0]
	invalid := []ReplicatedRequest{base}
	invalid[0].Capability = serviceauthz.CapabilityDataRead
	for _, mutate := range []func(*ReplicatedRequest){
		func(r *ReplicatedRequest) { r.TransactionRead.Kind = 0 },
		func(r *ReplicatedRequest) { r.TransactionRead.ID = distributedtxn.ID{} },
		func(r *ReplicatedRequest) { r.TransactionRead.MinimumApplied = 0 },
		func(r *ReplicatedRequest) { r.TransactionRead.MaxRows = 0 },
		func(r *ReplicatedRequest) { r.TransactionRead.MaxRows = 2 },
		func(r *ReplicatedRequest) { r.TransactionRead.MaxBytes++ },
		func(r *ReplicatedRequest) { r.Relation = 1 },
	} {
		candidate := base
		mutate(&candidate)
		invalid = append(invalid, candidate)
	}
	for index := range invalid {
		if err := EncodeReplicatedRequest(&bytes.Buffer{}, &invalid[index]); err == nil {
			t.Fatalf("malformed request %d encoded", index)
		}
	}

	var header [5]byte
	header[0] = tagReplicatedTransactionRead
	binary.BigEndian.PutUint32(header[1:], uint32(4+replicatedTransactionReadRequestBodyBytes+1))
	if _, err := DecodeReplicatedRequest(bytes.NewReader(header[:])); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("oversized fixed body error=%v", err)
	}
	valid := replicatedRecoveryRequests(fence, authority)[0]
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, valid); err != nil {
		t.Fatal(err)
	}
	malformed := bytes.Clone(encoded.Bytes())
	malformed[5+242] = 0
	if _, err := DecodeReplicatedRequest(bytes.NewReader(malformed)); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("invalid suboperation error=%v", err)
	}
	malformed = bytes.Clone(encoded.Bytes())
	for index := 5 + 242 + 1 + 16; index < 5+242+1+16+8; index++ {
		malformed[index] = 0
	}
	if _, err := DecodeReplicatedRequest(bytes.NewReader(malformed)); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("zero applied floor error=%v", err)
	}
}

func TestReplicatedTransactionRecoveryValueCanonicalRoundTrip(t *testing.T) {
	id := testTransactionID(212)
	coordinator, _ := buildShardManifest(t, id, 3)
	record := replicatedRecoveryTestRecord(id, distributedtxn.ReplicatedRoleCoordinator,
		uint8(distributedtxn.CoordinatorStaging), distributedtxn.ReplicatedPayloadManifestCoordinator)
	record.PayloadCount = 3
	record.Payload = coordinator
	encoded, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionLookupCoordinator, Complete: true,
		Records: []replicatedstate.TransactionRecoveryRecord{record},
	})
	if err != nil {
		t.Fatal(err)
	}
	var arena [1]replicatedstate.TransactionRecoveryRecord
	opened, err := OpenReplicatedTransactionReadValueInto(encoded, arena[:0])
	if err != nil || !opened.Complete || opened.Kind != ReplicatedTransactionLookupCoordinator ||
		len(opened.Records) != 1 || opened.Records[0].ID != id ||
		!bytes.Equal(opened.Records[0].Payload, coordinator) ||
		cap(opened.Records[0].Payload) != len(opened.Records[0].Payload) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	malformed := bytes.Clone(encoded)
	malformed[replicatedTransactionReadValueHeaderBytes+115] = 4
	if _, err := OpenReplicatedTransactionReadValueInto(malformed, arena[:0]); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("unknown recovery flags error=%v", err)
	}
}

func TestReplicatedTransactionRecoveryCancellationWitnessRoundTripAndMalformed(t *testing.T) {
	id := testTransactionID(214)
	record := replicatedRecoveryTestRecord(id, distributedtxn.ReplicatedRoleParticipant,
		uint8(distributedtxn.ParticipantReleased), distributedtxn.ReplicatedPayloadParticipantStage)
	record.PayloadCount = 0
	record.CancellationWitness = true
	record.ParticipantOrdinal = 4096
	encoded, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionLookupParticipant, Complete: true,
		Records: []replicatedstate.TransactionRecoveryRecord{record},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != replicatedTransactionReadValueHeaderBytes+
		replicatedstate.TransactionRecoverySummaryBytes {
		t.Fatalf("cancellation recovery bytes=%d", len(encoded))
	}
	var arena [1]replicatedstate.TransactionRecoveryRecord
	opened, err := OpenReplicatedTransactionReadValueInto(encoded, arena[:0])
	if err != nil || len(opened.Records) != 1 ||
		!opened.Records[0].CancellationWitness ||
		opened.Records[0].ParticipantOrdinal != 4096 ||
		opened.Records[0].PayloadCount != 0 || opened.Records[0].AffectedRowsValid ||
		opened.Records[0].AffectedRows != 0 {
		t.Fatalf("opened cancellation=%+v err=%v", opened, err)
	}
	base := replicatedTransactionReadValueHeaderBytes
	for name, mutate := range map[string]func([]byte){
		"unknown-flag":          func(raw []byte) { raw[base+115] |= 1 << 2 },
		"affected-cancellation": func(raw []byte) { raw[base+115] |= 1 },
		"wide-ordinal": func(raw []byte) {
			binary.BigEndian.PutUint64(raw[base+107:base+115], uint64(1)<<32)
		},
		"missing-witness": func(raw []byte) { raw[base+115] &^= 1 << 1 },
		"wrong-revision":  func(raw []byte) { binary.BigEndian.PutUint64(raw[base+18:base+26], 2) },
	} {
		t.Run(name, func(t *testing.T) {
			malformed := bytes.Clone(encoded)
			mutate(malformed)
			if _, err := OpenReplicatedTransactionReadValueInto(
				malformed, arena[:0],
			); !errors.Is(err, ErrReplicatedWire) {
				t.Fatalf("malformed cancellation error=%v", err)
			}
		})
	}
}

func TestReplicatedTransactionRecoveryRetiredAffectedRowsRoundTripAndMalformed(t *testing.T) {
	id := testTransactionID(215)
	record := replicatedRecoveryTestRecord(id, distributedtxn.ReplicatedRoleCoordinator,
		uint8(distributedtxn.CoordinatorRetired), distributedtxn.ReplicatedPayloadCoordinator)
	record.Revision = 3
	record.CoordinatorDecision = distributedtxn.CoordinatorCommitted
	record.AffectedRows, record.AffectedRowsValid = 4096, true
	encoded, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionLookupCoordinator, Complete: true,
		Records: []replicatedstate.TransactionRecoveryRecord{record},
	})
	if err != nil {
		t.Fatal(err)
	}
	var arena [1]replicatedstate.TransactionRecoveryRecord
	opened, err := OpenReplicatedTransactionReadValueInto(encoded, arena[:0])
	if err != nil || len(opened.Records) != 1 || !opened.Records[0].AffectedRowsValid ||
		opened.Records[0].AffectedRows != 4096 || len(opened.Records[0].Payload) != 0 {
		t.Fatalf("retired coordinator=%+v err=%v", opened, err)
	}
	base := replicatedTransactionReadValueHeaderBytes
	for name, mutate := range map[string]func([]byte){
		"active-with-rows": func(raw []byte) {
			raw[base+17] = uint8(distributedtxn.CoordinatorCommitted)
		},
		"aborted-with-rows": func(raw []byte) {
			raw[base+116] = uint8(distributedtxn.CoordinatorAborted)
		},
		"committed-without-valid": func(raw []byte) {
			raw[base+115] &^= 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			malformed := bytes.Clone(encoded)
			mutate(malformed)
			if _, err := OpenReplicatedTransactionReadValueInto(
				malformed, arena[:0],
			); !errors.Is(err, ErrReplicatedWire) {
				t.Fatalf("malformed retired coordinator error=%v", err)
			}
		})
	}
	aborted := record
	aborted.CoordinatorDecision = distributedtxn.CoordinatorAborted
	aborted.AffectedRows, aborted.AffectedRowsValid = 0, false
	if _, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionLookupCoordinator, Complete: true,
		Records: []replicatedstate.TransactionRecoveryRecord{aborted},
	}); err != nil {
		t.Fatalf("canonical aborted retirement: %v", err)
	}
}

func TestReplicatedTransactionRecoveryValueRejectsMalformedEnvelopeAndScan(t *testing.T) {
	first := replicatedRecoveryTestRecord(testTransactionID(220),
		distributedtxn.ReplicatedRoleCoordinator,
		uint8(distributedtxn.CoordinatorStaging), distributedtxn.ReplicatedPayloadCoordinator)
	second := first
	second.ID[15]++
	encoded, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionScanCoordinators, Complete: false,
		Records: []replicatedstate.TransactionRecoveryRecord{first, second},
	})
	if err != nil {
		t.Fatal(err)
	}
	var records [2]replicatedstate.TransactionRecoveryRecord
	for name, mutate := range map[string]func([]byte){
		"complete": func(raw []byte) { raw[1] = 2 },
		"count": func(raw []byte) {
			binary.BigEndian.PutUint32(raw[2:6], replicatedstate.MaxTransactionRecoveryScanRows+1)
		},
		"payload-length": func(raw []byte) { binary.BigEndian.PutUint32(raw[6:10], 1) },
		"role": func(raw []byte) {
			raw[replicatedTransactionReadValueHeaderBytes+16] = byte(distributedtxn.ReplicatedRoleParticipant)
		},
		"duplicate": func(raw []byte) {
			firstStart := replicatedTransactionReadValueHeaderBytes
			secondStart := firstStart + replicatedstate.TransactionRecoverySummaryBytes
			copy(raw[secondStart:secondStart+16], raw[firstStart:firstStart+16])
		},
	} {
		t.Run(name, func(t *testing.T) {
			malformed := bytes.Clone(encoded)
			mutate(malformed)
			if _, err := OpenReplicatedTransactionReadValueInto(malformed, records[:0]); !errors.Is(err, ErrReplicatedWire) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	withPayload := first
	withPayload.Payload = []byte{1}
	if _, err := AppendReplicatedTransactionReadValue(nil, ReplicatedTransactionReadValue{
		Kind: ReplicatedTransactionScanCoordinators, Complete: true,
		Records: []replicatedstate.TransactionRecoveryRecord{withPayload},
	}); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("scan payload error=%v", err)
	}
}

func TestReplicatedTransactionRecoveryResponseBoundIncludesEnvelope(t *testing.T) {
	request := replicatedRecoveryRequests(testReplicatedFence(),
		serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17})[2]
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil {
		t.Fatal(err)
	}
	want := replicatedReadResponseFixedBodyBytes +
		ReplicatedTransactionReadValueHeaderBytes + replicatedstate.MaxTransactionRecoveryReadBytes
	if maximum != want {
		t.Fatalf("maximum=%d want=%d", maximum, want)
	}
	var header [5]byte
	header[0] = tagReplicatedResponse
	binary.BigEndian.PutUint32(header[1:], uint32(maximum+1+4))
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(header[:]), maximum); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("oversized response header error=%v", err)
	}
}

func TestReplicatedTransactionRecoveryAuthorizationIsIndependent(t *testing.T) {
	gateway := authorizationNode(91)
	recovery := authorizationNode(92)
	reader := authorizationNode(93)
	gate := authorizationGate(t, 11,
		serviceauthz.Entry{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: recovery, Capabilities: serviceauthz.CapabilityTransactionRecovery},
		serviceauthz.Entry{Node: reader, Capabilities: serviceauthz.CapabilityDataRead},
	)
	server := &ReplicatedServer{authorization: gate}
	request := replicatedRecoveryRequests(testReplicatedFence(),
		serviceauthz.Authority{Node: recovery, Generation: 11})[0]
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("transaction recovery principal denied")
	}
	request.Authority.Node = reader
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("ordinary data reader gained transaction recovery authority")
	}
	request.Authority.Node = recovery
	if server.authorizeReplicated(authorizationNode(94), request) {
		t.Fatal("non-delegate peer forwarded transaction recovery authority")
	}
}

func TestReplicatedServerServesCanonicalTransactionRecoveryRead(t *testing.T) {
	state := testReplicatedServingState()
	id := testTransactionID(213)
	record := replicatedRecoveryTestRecord(id, distributedtxn.ReplicatedRoleCoordinator,
		uint8(distributedtxn.CoordinatorCommitted), distributedtxn.ReplicatedPayloadCoordinator)
	record.CoordinatorDecision = distributedtxn.CoordinatorCommitted
	owner := &fakeReplicatedOwner{state: state, transactionResult: raftservice.TransactionReadResult{
		Applied: 12, Complete: false,
		Records: []replicatedstate.TransactionRecoveryRecord{record},
	}}
	request := replicatedRecoveryRequests(replicatedWireState(state).Fence,
		serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17})[3]
	response := testReplicatedServer(owner).executeReplicated(context.Background(), request)
	if response.Kind != ReplicatedTransactionReadResult || response.ReadApplied != 12 ||
		!validReplicatedResponse(response) ||
		response.State.Fence != request.Fence || response.State.Applied != 12 ||
		response.State.Commit != 12 || owner.probeCalls.Load() != 1 ||
		owner.transactionRequest.Capability != serviceauthz.CapabilityTransactionRecovery ||
		owner.transactionRequest.Read.Kind != replicatedstate.TransactionRecoveryScanCoordinator ||
		owner.transactionRequest.Read.ID != request.TransactionRead.ID {
		t.Fatalf("response=%+v request=%+v", response, owner.transactionRequest)
	}
	var records [1]replicatedstate.TransactionRecoveryRecord
	opened, err := OpenReplicatedTransactionReadValueInto(response.Value, records[:0])
	if err != nil || opened.Complete || len(opened.Records) != 1 || opened.Records[0].ID != id {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
}

func TestReplicatedServerTransactionRecoveryTypedRefusals(t *testing.T) {
	state := testReplicatedServingState()
	for _, test := range []struct {
		err     error
		kind    ReplicatedResponseKind
		refusal ReplicatedRefusalCode
	}{
		{raftmodel.ErrNotLeader, ReplicatedNotLeader, ReplicatedRefusalNone},
		{raftservice.ErrServingFence, ReplicatedRefusal, ReplicatedRefusalStaleFence},
		{replicatedstate.ErrReadBehind, ReplicatedRefusal, ReplicatedRefusalReadBehind},
		{replicatedstate.ErrReadBufferBound, ReplicatedRefusal, ReplicatedRefusalReadBufferBound},
		{replicatedstate.ErrTransactionRecoveryRead, ReplicatedRefusal, ReplicatedRefusalTransactionReadMalformed},
		{raftservice.ErrPendingReadsFull, ReplicatedRefusal, ReplicatedRefusalAdmissionBound},
		{raftservice.ErrTransactionRecoveryUnauthorized, ReplicatedRefusal, ReplicatedRefusalUnauthorized},
	} {
		owner := &fakeReplicatedOwner{state: state, transactionErr: test.err}
		request := replicatedRecoveryRequests(replicatedWireState(state).Fence,
			serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17})[0]
		response := testReplicatedServer(owner).executeReplicated(context.Background(), request)
		if response.Kind != test.kind || response.Refusal != test.refusal ||
			!validReplicatedResponse(response) || owner.probeCalls.Load() != 2 {
			t.Fatalf("error=%v response=%+v", test.err, response)
		}
	}
}

func TestReplicatedServerHoldsTransactionRecoveryLeaseThroughFrameWrite(t *testing.T) {
	state := testReplicatedServingState()
	lease := &testPointReadLease{}
	called := make(chan struct{})
	id := testTransactionID(214)
	records := make([]replicatedstate.TransactionRecoveryRecord,
		replicatedstate.MaxTransactionRecoveryScanRows)
	for index := range records {
		id := id
		binary.BigEndian.PutUint16(id[14:], uint16(index+1))
		records[index] = replicatedRecoveryTestRecord(id,
			distributedtxn.ReplicatedRoleCoordinator,
			uint8(distributedtxn.CoordinatorStaging),
			distributedtxn.ReplicatedPayloadCoordinator)
	}
	owner := &fakeReplicatedOwner{state: state, transactionCalled: called,
		transactionResult: raftservice.TransactionReadResult{
			Applied: 11, Complete: false, Records: records,
		}, transactionLease: lease}
	server := testReplicatedServer(owner)
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()
	defer clientSide.Close()
	done := make(chan error, 1)
	go func() { done <- server.serveReplicatedRequest(context.Background(), serverSide) }()
	request := replicatedRecoveryRequests(replicatedWireState(state).Fence,
		serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17})[3]
	if err := EncodeReplicatedRequest(clientSide, request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("transaction recovery read was not admitted")
	}
	if lease.released.Load() {
		t.Fatal("transaction recovery reservation released before socket write")
	}
	response, err := DecodeReplicatedResponse(clientSide)
	if err != nil || response.Kind != ReplicatedTransactionReadResult {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !lease.released.Load() {
		t.Fatal("transaction recovery reservation not released after socket write")
	}
}
