package executionpin

import (
	"bytes"
	"testing"
)

var (
	testBytesSink  []byte
	testRecordSink Record
)

func testDigest(value byte) Digest {
	var digest Digest
	digest[0], digest[len(digest)-1] = value, value^0xff
	return digest
}

func testID(value byte) ID {
	var id ID
	id[0], id[len(id)-1] = value, value^0xff
	return id
}

func testBinding() Binding {
	return Binding{
		RequestKeyDigest: testDigest(1), RequestDigest: testDigest(2),
		CatalogGeneration: 3, SchemaGeneration: 4,
		SchemaManifestDigest: testDigest(5), SchemaCertificateDigest: testDigest(6),
		LogicalGroup: testID(7), LogicalRange: testID(8), MutationDigest: testDigest(9),
	}
}

func testAcquire() Command {
	binding := testBinding()
	pin, _ := DerivePinID(binding)
	return Command{
		Operation: OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: testID(41), AuthorityGeneration: 42,
		NextController: testID(10), NextControllerEpoch: 11, NextLeaseDeadline: 100,
	}
}

func TestCommandCanonicalRoundTripBoundsAndZeroAllocation(t *testing.T) {
	command := testAcquire()
	storage := make([]byte, 0, CommandBytes)
	encoded, err := AppendCommand(storage, command)
	if err != nil || len(encoded) != CommandBytes {
		t.Fatalf("AppendCommand = %d,%v", len(encoded), err)
	}
	opened, err := OpenCommand(encoded)
	if err != nil || opened != command {
		t.Fatalf("OpenCommand = %+v,%v", opened, err)
	}
	reencoded, err := AppendCommand(storage[:0], opened)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatal("command lacks one canonical encoding")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		testBytesSink, err = AppendCommand(storage[:0], command)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("AppendCommand allocations = %v", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, err = OpenCommand(encoded)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("OpenCommand allocations = %v", allocations)
	}
	for _, raw := range [][]byte{encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		if _, err = OpenCommand(raw); err == nil {
			t.Fatal("non-exact command length accepted")
		}
	}
	corrupt := bytes.Clone(encoded)
	corrupt[5] = 1
	if _, err = OpenCommand(corrupt); err == nil {
		t.Fatal("reserved command byte accepted")
	}
}

func TestPinIDBindsEveryLogicalFieldButNotControllerLease(t *testing.T) {
	binding := testBinding()
	want, err := DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*Binding){
		func(value *Binding) { value.RequestKeyDigest[0]++ },
		func(value *Binding) { value.RequestDigest[0]++ },
		func(value *Binding) { value.CatalogGeneration++ },
		func(value *Binding) { value.SchemaGeneration++ },
		func(value *Binding) { value.SchemaManifestDigest[0]++ },
		func(value *Binding) { value.SchemaCertificateDigest[0]++ },
		func(value *Binding) { value.LogicalGroup[0]++ },
		func(value *Binding) { value.LogicalRange[0]++ },
		func(value *Binding) { value.MutationDigest[0]++ },
	}
	for index, mutate := range mutations {
		changed := binding
		mutate(&changed)
		got, deriveErr := DerivePinID(changed)
		if deriveErr != nil || got == want {
			t.Fatalf("field %d did not bind PinID: %x,%v", index, got, deriveErr)
		}
	}
	command := testAcquire()
	command.NextController = testID(42)
	command.NextControllerEpoch = 43
	command.NextLeaseDeadline = 1000
	if derived, deriveErr := DerivePinID(command.Binding); deriveErr != nil || derived != want {
		t.Fatalf("controller/lease changed PinID: %x,%v", derived, deriveErr)
	}
}

func TestAcquireRenewRecoverPrepareTerminalReleaseCertificatePair(t *testing.T) {
	authority, acquireDigest := testDigest(20), testDigest(21)
	acquire := testAcquire()
	transition := Apply(Record{}, false, acquire, 10, authority, acquireDigest)
	if transition.Reason != ReasonApplied || !transition.Mutated || !transition.Record.Valid() {
		t.Fatalf("acquire = %+v", transition)
	}
	record := transition.Record
	acquireCertificate, ok := record.AcquireCertificate()
	if !ok {
		t.Fatal("missing acquisition certificate")
	}
	acquireCertificateDigest, err := AcquireCertificateDigest(acquireCertificate)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := CompletionFromRecord(OperationAcquire, record)
	if err != nil || !completion.Valid() || completion.Terminal != (TerminalCertificate{}) {
		t.Fatalf("acquire completion = %+v,%v", completion, err)
	}
	if err := ValidateAcquirePair(acquire, completion, authority); err != nil {
		t.Fatalf("acquire pair: %v", err)
	}
	if err := ValidateAcquirePair(acquire, completion, testDigest(99)); err == nil {
		t.Fatal("wrong authenticated acquire authority accepted")
	}

	renew := acquire
	renew.Operation = OperationRenew
	renew.ExpectedController, renew.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	renew.ExpectedLeaseRevision = record.LeaseRevision
	renew.NextController, renew.NextControllerEpoch = record.Controller, record.ControllerEpoch
	renew.ExpectedLeaseDeadline, renew.NextLeaseDeadline = record.LeaseDeadline, 200
	renew.AcquireCertificateDigest = acquireCertificateDigest
	transition = Apply(record, true, renew, 11, testDigest(22), testDigest(23))
	if transition.Reason != ReasonApplied || !transition.Mutated ||
		transition.Record.LeaseRevision != 2 || transition.Record.LeaseDeadline != 200 {
		t.Fatalf("renew = %+v", transition)
	}
	renewRecord := transition.Record
	record = renewRecord

	recover := acquire
	recover.Operation = OperationRecover
	recover.ExpectedController, recover.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	recover.ExpectedLeaseRevision = record.LeaseRevision
	recover.NextController, recover.NextControllerEpoch = testID(30), record.ControllerEpoch+1
	recover.ExpectedLeaseDeadline, recover.ObservedUnixNano = record.LeaseDeadline, record.LeaseDeadline
	recover.NextLeaseDeadline = 400
	recover.AcquireCertificateDigest = acquireCertificateDigest
	transition = Apply(record, true, recover, 12, testDigest(24), testDigest(25))
	if transition.Reason != ReasonApplied || !transition.Mutated ||
		transition.Record.Controller != recover.NextController || transition.Record.LeaseRevision != 3 {
		t.Fatalf("recover = %+v", transition)
	}
	recoverRecord := transition.Record
	record = recoverRecord

	release := acquire
	release.Operation = OperationRelease
	release.ExpectedController, release.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	release.ExpectedLeaseRevision = record.LeaseRevision
	release.ExpectedLeaseDeadline = record.LeaseDeadline
	release.NextController, release.NextControllerEpoch, release.NextLeaseDeadline = ID{}, 0, 0
	release.PrepareTerminalDigest = testDigest(31)
	release.AcquireCertificateDigest = acquireCertificateDigest
	transition = Apply(record, true, release, 13, testDigest(26), testDigest(27))
	if transition.Reason != ReasonApplied || !transition.Mutated ||
		transition.Record.Status != StatusReleased {
		t.Fatalf("release = %+v", transition)
	}
	record = transition.Record
	completion, err = CompletionFromRecord(OperationRelease, record)
	if err != nil || !completion.Valid() || completion.Terminal.Status != StatusReleased ||
		completion.Terminal.PrepareTerminalDigest != release.PrepareTerminalDigest {
		t.Fatalf("release completion = %+v,%v", completion, err)
	}
	if err := ValidateReleasePair(release, completion, testDigest(26)); err != nil {
		t.Fatalf("release pair: %v", err)
	}
	tamperedPair := completion
	tamperedPair.Terminal.PrepareTerminalDigest[0] ^= 1
	if err := ValidateReleasePair(release, tamperedPair, testDigest(26)); err == nil {
		t.Fatal("tampered release pair accepted")
	}
	if err := ValidateReleasePair(release, completion, testDigest(99)); err == nil {
		t.Fatal("wrong authenticated release authority accepted")
	}
	for _, historical := range []struct {
		command   Command
		authority Digest
		applied   uint64
		revision  uint64
	}{
		{renew, testDigest(22), 11, 2},
		{recover, testDigest(24), 12, 3},
	} {
		proof, proofErr := CompletionFromApplied(
			historical.command, record, historical.authority, historical.applied,
		)
		if proofErr != nil || proof.Status != StatusActive ||
			proof.Lease.Revision != historical.revision ||
			proof.Lease.Applied != historical.applied {
			t.Fatalf("historical completion = %+v,%v", proof, proofErr)
		}
	}
	if _, proofErr := CompletionFromApplied(renew, renewRecord, testDigest(22), 12); proofErr == nil {
		t.Fatal("wrong applied index reconstructed a renew proof")
	}
	encoded := make([]byte, 0, CompletionBytes)
	encoded, err = AppendCompletion(encoded, completion)
	opened, openErr := OpenCompletion(encoded)
	if err != nil || openErr != nil || opened != completion {
		t.Fatalf("completion round trip = %+v,%v,%v", opened, err, openErr)
	}
	// A new controller/session can repeat the logical release without changing
	// either applied index or transferable certificate bytes.
	retry := Apply(record, true, release, 99, testDigest(40), testDigest(27))
	retryCompletion, retryErr := CompletionFromRecord(OperationRelease, retry.Record)
	if retry.Reason != ReasonApplied || retry.Mutated || retryErr != nil ||
		retryCompletion != completion {
		t.Fatalf("release retry = %+v completion=%+v err=%v", retry, retryCompletion, retryErr)
	}
	lateAcquire := Apply(record, true, acquire, 100, authority, acquireDigest)
	if lateAcquire.Reason != ReasonConflict || lateAcquire.Mutated {
		t.Fatalf("terminal pin resurrected: %+v", lateAcquire)
	}
}

func TestExpiryBeforeAcquireCreatesAntiResurrectionTombstone(t *testing.T) {
	acquire := testAcquire()
	expire := acquire
	expire.Operation = OperationExpire
	expire.ExpectedController, expire.ExpectedControllerEpoch =
		acquire.NextController, acquire.NextControllerEpoch
	expire.ExpectedLeaseRevision = 1
	expire.ExpectedLeaseDeadline, expire.ObservedUnixNano = acquire.NextLeaseDeadline, 101
	expire.NextController, expire.NextControllerEpoch, expire.NextLeaseDeadline = ID{}, 0, 0
	transition := Apply(Record{}, false, expire, 10, testDigest(50), testDigest(51))
	if transition.Reason != ReasonApplied || !transition.Mutated ||
		transition.Record.Status != StatusExpired || transition.Record.AcquireApplied != 0 {
		t.Fatalf("planned expiry = %+v", transition)
	}
	completion, err := CompletionFromRecord(OperationExpire, transition.Record)
	if err != nil || !completion.Valid() || completion.Acquire != (AcquireCertificate{}) ||
		completion.Lease != (LeaseCertificate{}) || completion.Terminal.Status != StatusExpired {
		t.Fatalf("expiry completion = %+v,%v", completion, err)
	}
	late := Apply(transition.Record, true, acquire, 11, testDigest(52), testDigest(53))
	if late.Reason != ReasonConflict || late.Mutated {
		t.Fatalf("delayed acquire resurrected tombstone: %+v", late)
	}
}

func TestRecordCanonicalRoundTripAndCertificatesAllocationFree(t *testing.T) {
	transition := Apply(Record{}, false, testAcquire(), 10, testDigest(60), testDigest(61))
	record := transition.Record
	storage := make([]byte, 0, RecordBytes)
	encoded, err := AppendRecord(storage, record)
	opened, openErr := OpenRecord(encoded)
	if err != nil || openErr != nil || opened != record {
		t.Fatalf("record round trip = %+v,%v,%v", opened, err, openErr)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		testBytesSink, err = AppendRecord(storage[:0], record)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("AppendRecord allocations = %v", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		testRecordSink, err = OpenRecord(encoded)
		if err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("OpenRecord allocations = %v", allocations)
	}
}

func FuzzOpenCommand(f *testing.F) {
	encoded, _ := AppendCommand(nil, testAcquire())
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, raw []byte) { _, _ = OpenCommand(raw) })
}

func FuzzOpenRecord(f *testing.F) {
	record := Apply(Record{}, false, testAcquire(), 10, testDigest(70), testDigest(71)).Record
	encoded, _ := AppendRecord(nil, record)
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, raw []byte) { _, _ = OpenRecord(raw) })
}

func BenchmarkCommandCodec(b *testing.B) {
	command := testAcquire()
	storage := make([]byte, 0, CommandBytes)
	encoded, _ := AppendCommand(storage, command)
	b.ReportAllocs()
	b.SetBytes(CommandBytes)
	b.Run("append", func(b *testing.B) {
		for b.Loop() {
			testBytesSink, _ = AppendCommand(storage[:0], command)
		}
	})
	b.Run("open", func(b *testing.B) {
		for b.Loop() {
			_, _ = OpenCommand(encoded)
		}
	})
}
