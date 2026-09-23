package executionpin

import "testing"

func TestLedgerReleaseAuthorityIsBoundAndAllocationFree(t *testing.T) {
	acquire := testAcquire()
	record := Apply(Record{}, false, acquire, 10, testDigest(20), testDigest(21)).Record
	certificate, _ := record.AcquireCertificate()
	digest, _ := AcquireCertificateDigest(certificate)
	release := Command{Operation: OperationRelease, Binding: record.Binding, PinID: record.PinID,
		AuthorityNode: acquire.AuthorityNode, AuthorityGeneration: acquire.AuthorityGeneration,
		ExpectedController: record.Controller, ExpectedControllerEpoch: record.ControllerEpoch,
		ExpectedLeaseAppliedThrough: record.LeaseAppliedThrough, ExpectedLeaseRevision: record.LeaseRevision,
		PrepareTerminalDigest: testDigest(31), AcquireCertificateDigest: digest}
	encoded, err := AppendCommand(nil, release)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := ReleaseAuthorityDigest(encoded)
	if err != nil || authority == (Digest{}) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Command){
		func(c *Command) { c.AuthorityNode[0] ^= 1 },
		func(c *Command) { c.AuthorityGeneration++ },
		func(c *Command) { c.ExpectedLeaseRevision++ },
		func(c *Command) { c.PrepareTerminalDigest[0] ^= 1 },
	} {
		other := release
		mutate(&other)
		otherEncoded, err := AppendCommand(nil, other)
		if err != nil {
			t.Fatal(err)
		}
		changed, err := ReleaseAuthorityDigest(otherEncoded)
		if err != nil || changed == authority {
			t.Fatal("release authority omitted a fence", err)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := ReleaseAuthorityDigest(encoded); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("release digest allocations=%g", allocations)
	}
}
