package shardservice

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestFrontendContinuationNativeEnvelopeRoundTripKeepsInnerQuery(t *testing.T) {
	request := &ReplicatedRequest{
		Operation: ReplicatedQueryLeader, Authority: serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence(),
		MaxValueBytes: 4096, Query: []byte("inner-query-bytes"),
	}
	scope, ok := FrontendContinuationScopeForReplicatedRequest(request)
	if !ok || scope.Operation != serviceauthz.ServiceOperationForwardedRead || scope.Relation != ([16]byte{}) {
		t.Fatalf("query scope=%+v ok=%v", scope, ok)
	}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: [32]byte{1},
		ConnToken: serviceauthz.FrontendConnToken{2}, Scope: scope}
	if err := request.SetFrontendContinuation(envelope); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	if got, want := encoded.Len(), mustReplicatedRequestFrameBytes(t, request); got != want {
		t.Fatalf("encoded bytes=%d want=%d", got, want)
	}
	opened, err := DecodeReplicatedRequest(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if opened.Continuation == nil || *opened.Continuation != envelope || !bytes.Equal(opened.Query, request.Query) {
		t.Fatalf("opened continuation/query=%+v/%q", opened.Continuation, opened.Query)
	}
	var borrowed bytes.Buffer
	if err := EncodeReplicatedRequestBorrowed(&borrowed, request); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded.Bytes(), borrowed.Bytes()) {
		t.Fatal("borrowed encoder changed the continuation or inner query")
	}

	wrong := envelope
	wrong.Scope.Group = testReplicatedFence().Group
	wrong.Scope.Group.GroupID[0]++
	if err := request.SetFrontendContinuation(wrong); err == nil {
		t.Fatal("scope mutation was accepted by the sender adapter")
	}
}

func TestFrontendContinuationLedgerReadRejectsForgedForwarding(t *testing.T) {
	request := &ReplicatedRequest{
		Operation:  ReplicatedRequestLedgerRead,
		Authority:  serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability: serviceauthz.CapabilityRequestLedger, Fence: testReplicatedFence(),
		RequestLedgerRead: testReplicatedRequestLedgerRead(),
	}
	scope, ok := FrontendContinuationScopeForReplicatedRequest(request)
	if !ok || scope.Action != serviceauthz.FrontendActionGatewayRequestLedger || scope.Operation != serviceauthz.ServiceOperationRequestLedger || scope.FenceDigest != request.Fence.Command.RelationManifestDigest {
		t.Fatalf("ledger scope=%+v ok=%v", scope, ok)
	}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: [32]byte{3},
		ConnToken: serviceauthz.FrontendConnToken{4}, Scope: serviceauthz.FrontendContinuationScopeRecord{
			Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
			Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead,
			Group: testReplicatedFence().Group}}
	if err := request.SetFrontendContinuation(envelope); err == nil {
		t.Fatal("unsupported internal request accepted a forged forwarding scope")
	}
}

func TestFrontendContinuationProbeScopeIsGroupBound(t *testing.T) {
	request := &ReplicatedRequest{Operation: ReplicatedProbe,
		Authority:  serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      ReplicatedFence{Group: testReplicatedFence().Group, AllocationGeneration: 4}}
	scope, ok := FrontendContinuationScopeForReplicatedRequest(request)
	if !ok || scope.Action != serviceauthz.FrontendActionForwardedData ||
		scope.Operation != serviceauthz.ServiceOperationForwardedRead || scope.Group != request.Fence.Group {
		t.Fatalf("probe scope=%+v ok=%v", scope, ok)
	}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: [32]byte{5},
		ConnToken: serviceauthz.FrontendConnToken{6}, Scope: scope}
	if err := request.SetFrontendContinuation(envelope); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	if err := EncodeReplicatedRequest(&raw, request); err != nil {
		t.Fatal(err)
	}
	opened, err := DecodeReplicatedRequest(bytes.NewReader(raw.Bytes()))
	if err != nil || opened.Continuation == nil || *opened.Continuation != envelope {
		t.Fatalf("probe continuation=%+v err=%v", opened.Continuation, err)
	}
}

func mustReplicatedRequestFrameBytes(t *testing.T, request *ReplicatedRequest) int {
	t.Helper()
	bytes, err := ReplicatedRequestFrameBytes(request)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func TestCatalogScopeBindsGroupRelationAndManifest(t *testing.T) {
	request := &ReplicatedRequest{Operation: ReplicatedReadLeader, Capability: serviceauthz.CapabilityTopology, Fence: testReplicatedFence(), Relation: 1}
	scope, ok := FrontendContinuationScopeForReplicatedRequest(request)
	if !ok || scope.Action != serviceauthz.FrontendActionGatewayCatalog || scope.Operation != serviceauthz.ServiceOperationCatalogRead {
		t.Fatalf("catalog scope=%+v ok=%t", scope, ok)
	}
	changed := *request
	changed.Relation = 2
	other, ok := FrontendContinuationScopeForReplicatedRequest(&changed)
	if !ok || scope.Relation == other.Relation {
		t.Fatal("catalog read relation is not fenced")
	}
	changed = *request
	changed.Fence.Group.GroupID[0] ^= 1
	other, ok = FrontendContinuationScopeForReplicatedRequest(&changed)
	if !ok || scope.Group == other.Group {
		t.Fatal("catalog group is not fenced")
	}
	changed = *request
	changed.Fence.Command.RelationManifestDigest[0] ^= 0x80
	other, ok = FrontendContinuationScopeForReplicatedRequest(&changed)
	if !ok || scope.FenceDigest == other.FenceDigest {
		t.Fatal("catalog manifest is not fenced")
	}
	changed = *request
	changed.Fence.Command.RelationManifestDigest = [32]byte{}
	if _, ok = FrontendContinuationScopeForReplicatedRequest(&changed); ok {
		t.Fatal("missing manifest admitted")
	}
}

func TestLedgerScopeBindsGroupAndManifest(t *testing.T) {
	for _, operation := range []ReplicatedOperation{ReplicatedProbe, ReplicatedPropose, ReplicatedRequestLedgerRead} {
		request := ReplicatedRequest{Operation: operation, Capability: serviceauthz.CapabilityRequestLedger, Fence: testReplicatedFence()}
		scope, ok := FrontendContinuationScopeForReplicatedRequest(&request)
		if !ok {
			t.Fatalf("operation %v unsupported", operation)
		}
		changed := request
		changed.Fence.Group.GroupID[0] ^= 0x80
		other, ok := FrontendContinuationScopeForReplicatedRequest(&changed)
		if !ok || other.Group == scope.Group {
			t.Fatal("ledger group not fenced")
		}
		changed = request
		changed.Fence.Command.RelationManifestDigest[0] ^= 0x80
		other, ok = FrontendContinuationScopeForReplicatedRequest(&changed)
		if !ok || other.FenceDigest == scope.FenceDigest {
			t.Fatal("ledger manifest not fenced")
		}
		changed.Fence.Command.RelationManifestDigest = [32]byte{}
		if _, ok := FrontendContinuationScopeForReplicatedRequest(&changed); ok {
			t.Fatal("missing manifest admitted")
		}
	}
	request := ReplicatedRequest{Operation: ReplicatedRequestLedgerRead, Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence()}
	if _, ok := FrontendContinuationScopeForReplicatedRequest(&request); ok {
		t.Fatal("wrong ledger capability admitted")
	}
}

func TestDurableSQLInternalScopesBindGroupManifestAndCapability(t *testing.T) {
	for _, test := range []struct {
		capability serviceauthz.Capability
		operation  ReplicatedOperation
		action     serviceauthz.FrontendContinuationAction
		service    serviceauthz.ServiceOperation
	}{
		{serviceauthz.CapabilityExecutionPin, ReplicatedProbe, serviceauthz.FrontendActionGatewayExecutionPin, serviceauthz.ServiceOperationExecutionPin},
		{serviceauthz.CapabilityExecutionPin, ReplicatedPropose, serviceauthz.FrontendActionGatewayExecutionPin, serviceauthz.ServiceOperationExecutionPin},
		{serviceauthz.CapabilityExecutionPin, ReplicatedExecutionPinRead, serviceauthz.FrontendActionGatewayExecutionPin, serviceauthz.ServiceOperationExecutionPin},
		{serviceauthz.CapabilityTransactionRecovery, ReplicatedProbe, serviceauthz.FrontendActionGatewayTransactionRecovery, serviceauthz.ServiceOperationTransactionRecovery},
		{serviceauthz.CapabilityTransactionRecovery, ReplicatedPropose, serviceauthz.FrontendActionGatewayTransactionRecovery, serviceauthz.ServiceOperationTransactionRecovery},
		{serviceauthz.CapabilityTransactionRecovery, ReplicatedTransactionRead, serviceauthz.FrontendActionGatewayTransactionRecovery, serviceauthz.ServiceOperationTransactionRecovery},
	} {
		request := ReplicatedRequest{Operation: test.operation, Capability: test.capability, Fence: testReplicatedFence()}
		scope, ok := FrontendContinuationScopeForReplicatedRequest(&request)
		if !ok || scope.Action != test.action || scope.Operation != test.service {
			t.Fatalf("operation=%v capability=%v scope=%+v ok=%t", test.operation, test.capability, scope, ok)
		}
		changed := request
		changed.Fence.Group.GroupID[0] ^= 0x80
		other, ok := FrontendContinuationScopeForReplicatedRequest(&changed)
		if !ok || scope.Group == other.Group {
			t.Fatal("group not fenced")
		}
		changed = request
		changed.Fence.Command.RelationManifestDigest[0] ^= 0x80
		other, ok = FrontendContinuationScopeForReplicatedRequest(&changed)
		if !ok || scope.FenceDigest == other.FenceDigest || scope.IntentID == other.IntentID {
			t.Fatal("manifest not fenced")
		}
		changed.Fence.Command.RelationManifestDigest = [32]byte{}
		if _, ok = FrontendContinuationScopeForReplicatedRequest(&changed); ok {
			t.Fatal("missing manifest accepted")
		}
	}
	for _, operation := range []ReplicatedOperation{ReplicatedExecutionPinRead, ReplicatedTransactionRead} {
		request := ReplicatedRequest{Operation: operation, Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence()}
		if _, ok := FrontendContinuationScopeForReplicatedRequest(&request); ok {
			t.Fatal("wrong capability accepted")
		}
	}
}
