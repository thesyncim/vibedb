package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type pendingPinSessionFactory struct {
	DurableRequestExecutionPinSessionFactory
	session   *NativeSession
	principal serviceauthz.Authority
}

func (f pendingPinSessionFactory) OpenExecutionPinSession(context.Context, DurableRequestTypedExecutionContext, ReplicatedRoute) (*NativeSession, serviceauthz.Authority, func(), error) {
	return f.session, f.principal, func() {}, nil
}

type pendingPinReadClient struct {
	route   ReplicatedRoute
	record  executionpin.Record
	retried []byte
}

func (c *pendingPinReadClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: c.route.Group, AllocationGeneration: c.route.AllocationGeneration, Command: c.route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 1}, LeaderID: endpoint.Member, Applied: 10, Commit: 10, CheckpointApplied: 1}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	if request.Operation == shardservice.ReplicatedExecutionPinRead {
		value, err := shardservice.AppendReplicatedExecutionPinReadValue(nil, shardservice.ReplicatedExecutionPinReadValue{Found: true, Record: c.record})
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedExecutionPinReadResult, HasState: true, State: state, ReadApplied: 10, Value: value}, err
	}
	c.retried = bytes.Clone(request.Command)
	return nil, errors.New("pin completion still unavailable")
}

func TestDurablePinLiveReadCannotBypassPendingAcquireCompletion(t *testing.T) {
	execution := typedExecutionFixture(t)
	_, _, route := lifecycleRunnerFixture(t)
	execution, _ = bindTypedExecutionPin(t, execution, route)
	binding, err := BuildDurableRequestExecutionPinBinding(execution)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	principal := serviceauthz.Authority{Node: [16]byte{7}, Generation: 1}
	command := executionpin.Command{Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID(principal.Node), AuthorityGeneration: 1, NextController: executionpin.ID(principal.Node), NextControllerEpoch: 1, NextLeaseSpan: 100}
	applied := executionpin.Apply(executionpin.Record{}, false, command, 10, executionpin.Digest{1}, executionpin.Digest{2})
	if applied.Reason != executionpin.ReasonApplied {
		t.Fatal("invalid pin fixture")
	}
	client := &pendingPinReadClient{route: route, record: applied.Record}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{Executor: executor, Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard), Tenant: execution.Recipe.Tenant,
		ClientID: replication.ID128{3}, RetryHome: execution.Recipe.Identity.RetryHome, Resolver: BaseRelationResolver{Relation: 1}, ProposalCapability: serviceauthz.CapabilityExecutionPin})
	if err != nil {
		t.Fatal(err)
	}
	session.phase, session.epoch, session.nextSequence = nativeSessionActive, 1, 2
	nested, err := executionpin.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	outer := session.commandHeader(replication.CommandExecutionPin, 1, 2, 0)
	outer.ExecutionPin = nested
	outer.Fingerprint = nativeCommandFingerprint(outer)
	session.command, err = replication.AppendCommand(nil, outer)
	if err != nil {
		t.Fatal(err)
	}
	session.pending = true
	exact := bytes.Clone(session.command)
	authority, err := NewNativeDurableRequestExecutionPinAuthority(executor, pendingPinSessionFactory{session: session, principal: principal}, 100)
	if err != nil {
		t.Fatal(err)
	}
	_, _, lease, err := authority.AcquireOrRecover(t.Context(), execution)
	if err == nil || lease.Valid() || !session.pending || !bytes.Equal(client.retried, exact) {
		t.Fatalf("live cut bypassed pending completion: lease=%+v pending=%t retried=%t err=%v", lease, session.pending, bytes.Equal(client.retried, exact), err)
	}
}

func TestDurableRequestDefaultPinTakeoverRequiresLaterExactApply(t *testing.T) {
	execution := typedExecutionFixture(t)
	_, _, route := lifecycleRunnerFixture(t)
	_, release := bindTypedExecutionPin(t, execution, route)
	command := executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: release.Binding, PinID: release.PinID,
		AuthorityNode: executionpin.ID{7}, AuthorityGeneration: 1,
		NextController: executionpin.ID{7}, NextControllerEpoch: 1,
		NextLeaseSpan: DefaultDurableRequestExecutionPinSpan,
	}
	acquired := executionpin.Apply(executionpin.Record{}, false, command, 10, executionpin.Digest{1}, executionpin.Digest{2})
	if acquired.Reason != executionpin.ReasonApplied || acquired.Record.LeaseAppliedThrough != 11 {
		t.Fatalf("default pin is not a one-successor progress lease: %+v", acquired)
	}
	record := acquired.Record
	lease, ok := record.LeaseCertificate()
	if !ok {
		t.Fatal("missing lease")
	}
	if durableRequestPinRecoverableAtNextApply(record, 10) ||
		!durableRequestPinRecoverableAtNextApply(record, 11) ||
		durableRequestPinRecoverableAtNextApply(record, math.MaxUint64) {
		t.Fatal("wrong recovery admission boundary")
	}
	if executionpin.ValidateSideEffectFence(lease, record, 11) != nil {
		t.Fatal("old owner must remain live at the boundary observation")
	}
	if executionPinLeaseAdvanced(lease, record, 11) || !executionPinLeaseAdvanced(lease, record, 12) {
		t.Fatal("stale-pin retry did not require authenticated lease expiration")
	}
	foreignLease := lease
	foreignLease.AcquireCertificateDigest[0] ^= 1
	if executionPinLeaseAdvanced(foreignLease, record, 12) {
		t.Fatal("foreign acquisition accepted for retry")
	}
	foreign := serviceauthz.Authority{Node: [16]byte{8}, Generation: 1}
	if durableRequestPinControllerMatches(lease, foreign) {
		t.Fatal("replacement borrowed the old controller certificate")
	}
	command.Operation = executionpin.OperationRecover
	command.AuthorityNode, command.NextController = executionpin.ID(foreign.Node), executionpin.ID(foreign.Node)
	command.NextControllerEpoch = 2
	command.ExpectedController, command.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	command.ExpectedLeaseAppliedThrough, command.ExpectedLeaseRevision = record.LeaseAppliedThrough, record.LeaseRevision
	command.AcquireCertificateDigest = lease.AcquireCertificateDigest
	if early := executionpin.Apply(record, true, command, 11, executionpin.Digest{3}, executionpin.Digest{4}); early.Mutated || early.Reason != executionpin.ReasonTooEarly {
		t.Fatalf("recovery applied before expiration: %+v", early)
	}
	recovered := executionpin.Apply(record, true, command, 12, executionpin.Digest{3}, executionpin.Digest{4})
	if !executionPinLeaseAdvanced(lease, recovered.Record, 12) {
		t.Fatal("exact recovered acquisition not recognized")
	}
	if recovered.Reason != executionpin.ReasonApplied || !recovered.Mutated || recovered.Record.Controller != command.NextController ||
		executionpin.ValidateSideEffectFence(lease, recovered.Record, 12) == nil {
		t.Fatalf("later recovery did not fence the old certificate: %+v", recovered)
	}
	command.NextController, command.AuthorityNode = executionpin.ID{9}, executionpin.ID{9}
	if competing := executionpin.Apply(recovered.Record, true, command, 13, executionpin.Digest{5}, executionpin.Digest{6}); competing.Mutated || competing.Reason != executionpin.ReasonLeaseMismatch {
		t.Fatalf("competing stale recovery bypassed exact CAS: %+v", competing)
	}
	for _, mutate := range []func(*executionpin.Record){
		func(r *executionpin.Record) { r.PrepareTerminalDigest = executionpin.Digest{9} },
		func(r *executionpin.Record) { r.Status = executionpin.StatusReleased },
		func(r *executionpin.Record) { r.Status = executionpin.StatusExpired },
	} {
		blocked := record
		mutate(&blocked)
		if durableRequestPinRecoverableAtNextApply(blocked, 20) {
			t.Fatal("frozen or terminal pin became recoverable")
		}
	}
}

func TestDurableRequestWaveRefreshCannotBorrowAnotherLiveController(t *testing.T) {
	execution := typedExecutionFixture(t)
	_, _, route := lifecycleRunnerFixture(t)
	execution, _ = bindTypedExecutionPin(t, execution, route)
	lease := execution.ExecutionPinLease
	principal := serviceauthz.Authority{Node: rafttransport.NodeID(lease.Controller), Generation: 1}
	if !durableRequestPinControllerMatches(lease, principal) {
		t.Fatal("current controller rejected")
	}
	foreign := principal
	foreign.Node[0] ^= 0x80
	if durableRequestPinControllerMatches(lease, foreign) {
		t.Fatal("different gateway borrowed live controller at wave refresh")
	}
	principal.Generation = 0
	if durableRequestPinControllerMatches(lease, principal) {
		t.Fatal("invalid principal borrowed controller")
	}
	if durableRequestPinControllerMatches(executionpin.LeaseCertificate{}, foreign) {
		t.Fatal("invalid lease accepted")
	}
}
