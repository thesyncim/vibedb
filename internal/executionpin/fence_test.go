package executionpin

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestLogicalFenceSurvivesRestartAndRejectsCompetingController(t *testing.T) {
	acquire := testAcquire()
	acquire.NextLeaseSpan = 2
	first := Apply(Record{}, false, acquire, 10, testDigest(80), testDigest(81))
	if first.Reason != ReasonApplied {
		t.Fatalf("acquire = %+v", first)
	}
	lease, ok := first.Record.LeaseCertificate()
	if !ok || ValidateSideEffectFence(lease, first.Record, 12) != nil {
		t.Fatal("active controller was not fenced through its exact log position")
	}

	encoded, err := AppendRecord(nil, first.Record)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRecord(encoded)
	if err != nil || ValidateSideEffectFence(lease, reopened, 12) != nil {
		t.Fatalf("restart changed logical authority: %v", err)
	}

	acquireDigest, err := AcquireCertificateDigest(first.Record.mustAcquireCertificate(t))
	if err != nil {
		t.Fatal(err)
	}
	recover := acquire
	recover.Operation = OperationRecover
	recover.ExpectedController = first.Record.Controller
	recover.ExpectedControllerEpoch = first.Record.ControllerEpoch
	recover.ExpectedLeaseAppliedThrough = first.Record.LeaseAppliedThrough
	recover.ExpectedLeaseRevision = first.Record.LeaseRevision
	recover.NextController = testID(82)
	recover.NextControllerEpoch = first.Record.ControllerEpoch + 1
	recover.NextLeaseSpan = 2
	recover.AcquireCertificateDigest = acquireDigest

	tooEarly := Apply(first.Record, true, recover, 12, testDigest(83), testDigest(84))
	if tooEarly.Reason != ReasonTooEarly || tooEarly.Mutated {
		t.Fatalf("recovery crossed a live logical fence: %+v", tooEarly)
	}
	second := Apply(first.Record, true, recover, 13, testDigest(83), testDigest(84))
	if second.Reason != ReasonApplied || !second.Mutated {
		t.Fatalf("recovery after logical fence = %+v", second)
	}
	if ValidateSideEffectFence(lease, second.Record, 13) == nil {
		t.Fatal("superseded gateway certificate authorized a side effect")
	}
	newLease, ok := second.Record.LeaseCertificate()
	if !ok || ValidateSideEffectFence(newLease, second.Record, 13) != nil {
		t.Fatal("recovery controller did not receive exact side-effect authority")
	}
}

func TestCommandGrammarRejectsWallClockAuthorityEvenWithValidChecksum(t *testing.T) {
	encoded, err := AppendCommand(nil, testAcquire())
	if err != nil {
		t.Fatal(err)
	}
	// This slot carried ObservedUnixNano before the unreleased grammar became
	// clockless. Even an hours/centuries-scale value with a recomputed checksum
	// must be rejected rather than regaining authority through clock skew.
	binary.LittleEndian.PutUint64(encoded[336:344], ^uint64(0))
	binary.LittleEndian.PutUint32(encoded[416:420], crc32.Checksum(encoded[:416], castagnoli))
	if _, err = OpenCommand(encoded); err == nil {
		t.Fatal("wall-clock scalar entered logical execution-pin authority")
	}
}

func (record Record) mustAcquireCertificate(t *testing.T) AcquireCertificate {
	t.Helper()
	certificate, ok := record.AcquireCertificate()
	if !ok {
		t.Fatal("missing acquire certificate")
	}
	return certificate
}
