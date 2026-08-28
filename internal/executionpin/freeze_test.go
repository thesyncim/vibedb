package executionpin

import "testing"

func TestFreezeReleaseFencesTakeoverAndPreservesFixedRecord(t *testing.T) {
	acquire := testAcquire()
	original := Apply(Record{}, false, acquire, 10, testDigest(20), testDigest(21)).Record
	certificate, _ := original.AcquireCertificate()
	digest, _ := AcquireCertificateDigest(certificate)
	lease, _ := original.LeaseCertificate()
	release := Command{Operation: OperationRelease, Binding: original.Binding, PinID: original.PinID,
		AuthorityNode: acquire.AuthorityNode, AuthorityGeneration: acquire.AuthorityGeneration,
		ExpectedController: original.Controller, ExpectedControllerEpoch: original.ControllerEpoch,
		ExpectedLeaseAppliedThrough: original.LeaseAppliedThrough, ExpectedLeaseRevision: original.LeaseRevision,
		PrepareTerminalDigest: testDigest(31), AcquireCertificateDigest: digest}
	frozen, err := FreezeRelease(original, release)
	if err != nil || !frozen.Valid() || frozen.Status != StatusActive ||
		frozen.PrepareTerminalDigest != release.PrepareTerminalDigest {
		t.Fatalf("freeze=%+v err=%v", frozen, err)
	}
	want := original
	want.PrepareTerminalDigest = release.PrepareTerminalDigest
	if frozen != want || ValidateSideEffectFence(lease, frozen, original.LeaseApplied) == nil {
		t.Fatal("freeze changed lease identity or admitted side effects")
	}
	encoded, err := AppendRecord(nil, frozen)
	if err != nil || len(encoded) != RecordBytes {
		t.Fatal("freeze changed record space", err)
	}
	reopened, err := OpenRecord(encoded)
	if err != nil || reopened != frozen {
		t.Fatal("frozen state did not survive canonical reopen", err)
	}
	for _, operation := range []Operation{OperationRenew, OperationRecover, OperationExpire, OperationAcquire} {
		command := release
		command.Operation, command.PrepareTerminalDigest = operation, Digest{}
		switch operation {
		case OperationRenew:
			command.NextController, command.NextControllerEpoch, command.NextLeaseSpan = original.Controller, original.ControllerEpoch, 4
		case OperationRecover:
			command.NextController, command.NextControllerEpoch, command.NextLeaseSpan = testID(90), original.ControllerEpoch+1, 4
		case OperationExpire:
			command.AcquireCertificateDigest = Digest{}
		case OperationAcquire:
			command = acquire
		}
		if !command.Valid() {
			t.Fatalf("invalid refusal fixture %d", operation)
		}
		if got := Apply(reopened, true, command, 20, testDigest(22), testDigest(21)); got.Reason != ReasonTerminal || got.Mutated {
			t.Fatalf("frozen operation %d: %+v", operation, got)
		}
	}
	foreign := release
	foreign.PrepareTerminalDigest = testDigest(32)
	if _, err = FreezeRelease(frozen, foreign); err == nil ||
		Apply(frozen, true, foreign, 20, testDigest(22), testDigest(23)).Reason != ReasonConflict {
		t.Fatal("another terminal digest overwrote the freeze")
	}
	got := Apply(frozen, true, release, 20, testDigest(22), testDigest(23))
	if got.Reason != ReasonApplied || !got.Mutated || got.Record.Status != StatusReleased {
		t.Fatalf("exact release after expiry=%+v", got)
	}
	recover := release
	recover.Operation, recover.PrepareTerminalDigest = OperationRecover, Digest{}
	recover.NextController, recover.NextControllerEpoch, recover.NextLeaseSpan = testID(90), original.ControllerEpoch+1, 4
	advanced := Apply(original, true, recover, 20, testDigest(22), testDigest(23))
	if advanced.Reason != ReasonApplied || !advanced.Mutated {
		t.Fatal("takeover fixture failed")
	}
	if _, err = FreezeRelease(advanced.Record, release); err == nil {
		t.Fatal("recover-first accepted old release lease")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := FreezeRelease(original, release); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("freeze allocations=%g", allocations)
	}
}
