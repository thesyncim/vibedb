package gateway

import (
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

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
