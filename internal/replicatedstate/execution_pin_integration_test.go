package replicatedstate

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
)

func executionPinTestBinding() executionpin.Binding {
	digest := func(seed byte) executionpin.Digest {
		var value executionpin.Digest
		value[0], value[len(value)-1] = seed, seed^0xff
		return value
	}
	id := func(seed byte) executionpin.ID {
		var value executionpin.ID
		value[0], value[len(value)-1] = seed, seed^0xff
		return value
	}
	return executionpin.Binding{
		RequestKeyDigest: digest(1), RequestDigest: digest(2),
		CatalogGeneration: 3, SchemaGeneration: 4,
		SchemaManifestDigest: digest(5), SchemaCertificateDigest: digest(6),
		LogicalGroup: id(7), LogicalRange: id(8), MutationDigest: digest(9),
	}
}

func executionPinCommand(
	binding Binding,
	client replication.ID128,
	epoch, sequence uint64,
	nested executionpin.Command,
) []byte {
	raw, err := executionpin.AppendCommand(nil, nested)
	if err != nil {
		panic(err)
	}
	fingerprint := sha256.Sum256(append([]byte("execution-pin-test"), raw...))
	encoded, err := replication.AppendCommand(nil, replication.Command{
		Kind: replication.CommandExecutionPin, AuthorityClass: replication.CommandAuthorityExecutionPin,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration,
		Tenant:          []byte("pin-controller"), ClientID: client,
		ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: fingerprint, ExecutionPin: raw,
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func executionPinSessionPrototype(binding Binding, client replication.ID128) replication.Command {
	return replication.Command{
		AuthorityClass: replication.CommandAuthorityExecutionPin,
		ClusterID:      binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration,
		Tenant:          []byte("pin-controller"), ClientID: client,
	}
}

func openExecutionPinProof(t testing.TB, machine *Machine, command []byte) (
	replication.CompletionView,
	executionpin.Completion,
) {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || outer.ResultFormat != ResultFormatExecutionPin ||
		len(outer.InlineResult) != executionpin.CompletionBytes {
		t.Fatalf("outer execution-pin completion = %+v, %v", outer, err)
	}
	proof, err := executionpin.OpenCompletion(outer.InlineResult)
	if err != nil {
		t.Fatal(err)
	}
	return outer, proof
}

func TestExecutionPinAcquirePrepareTerminalReleaseRetryAndReopen(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	client := id128(0xd0)
	applySessionOpen(t, fixture.machine, 2, executionPinSessionPrototype(fixture.binding, client))
	binding := executionPinTestBinding()
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	controller := executionpin.ID(id128(0xe0))
	acquire := executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		NextController: controller, NextControllerEpoch: 1, NextLeaseDeadline: 100,
	}
	acquireBytes := executionPinCommand(fixture.binding, client, 2, 2, acquire)
	if err := fixture.machine.AdmitCommand(acquireBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), acquireBytes); err != nil {
		t.Fatal(err)
	}
	outer, acquireProof := openExecutionPinProof(t, fixture.machine, acquireBytes)
	if outer.ResultCode != ResultApplied || acquireProof.Status != executionpin.StatusActive ||
		acquireProof.Acquire.PinID != pin || acquireProof.Lease.Revision != 1 {
		t.Fatalf("acquire proof = outer=%+v proof=%+v", outer, acquireProof)
	}
	activeKey := executionPinActiveStorageKey(executionpin.Record{
		PinID: pin, Binding: binding,
	})
	if _, found, readErr := fixture.system.Collection.AppendRaw(nil, activeKey[:]); readErr != nil || !found {
		t.Fatalf("active scope index = found %v err %v", found, readErr)
	}
	active, err := fixture.machine.ScanActiveExecutionPins(
		binding.LogicalGroup, binding.LogicalRange, executionpin.PinID{}, 1,
	)
	if err != nil || len(active) != 1 || active[0].PinID != pin {
		t.Fatalf("active recovery scan = %+v, %v", active, err)
	}
	acquireDigest, err := executionpin.AcquireCertificateDigest(acquireProof.Acquire)
	if err != nil {
		t.Fatal(err)
	}
	release := executionpin.Command{
		Operation: executionpin.OperationRelease, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		ExpectedController: controller, ExpectedControllerEpoch: 1,
		ExpectedLeaseDeadline: 100, ExpectedLeaseRevision: 1,
		PrepareTerminalDigest:    executionpin.Digest(sha256.Sum256([]byte("prepared-terminal"))),
		AcquireCertificateDigest: acquireDigest,
	}
	releaseBytes := executionPinCommand(fixture.binding, client, 2, 3, release)
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), releaseBytes); err != nil {
		t.Fatal(err)
	}
	firstOuter, firstProof := openExecutionPinProof(t, fixture.machine, releaseBytes)
	if firstOuter.ResultCode != ResultApplied || firstProof.Status != executionpin.StatusReleased ||
		firstProof.Terminal.PrepareTerminalDigest != release.PrepareTerminalDigest {
		t.Fatalf("release proof = outer=%+v proof=%+v", firstOuter, firstProof)
	}
	if _, found, readErr := fixture.system.Collection.AppendRaw(nil, activeKey[:]); readErr != nil || found {
		t.Fatalf("terminal active scope index = found %v err %v", found, readErr)
	}
	active, err = fixture.machine.ScanActiveExecutionPins(
		binding.LogicalGroup, binding.LogicalRange, executionpin.PinID{}, 1,
	)
	if err != nil || len(active) != 0 {
		t.Fatalf("terminal recovery scan = %+v, %v", active, err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), releaseBytes); err != nil {
		t.Fatal(err)
	}
	retryOuter, retryProof := openExecutionPinProof(t, fixture.machine, releaseBytes)
	if retryOuter.AppliedSequence != firstOuter.AppliedSequence || retryProof != firstProof {
		t.Fatalf("retry changed certificate: first=%+v/%+v retry=%+v/%+v",
			firstOuter, firstProof, retryOuter, retryProof)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	reopenOuter, reopenProof := openExecutionPinProof(t, reopened, releaseBytes)
	if reopenOuter.AppliedSequence != firstOuter.AppliedSequence || reopenProof != firstProof ||
		reopened.state.ExecutionPinRecordCount != 1 || reopened.state.ActiveExecutionPinCount != 0 {
		t.Fatalf("reopen = state=%+v outer=%+v proof=%+v", reopened.state, reopenOuter, reopenProof)
	}
	if reopened.state.ExecutionPinResidentBytes !=
		uint64(executionPinRecordStorageKeyBytes+executionpin.RecordBytes) {
		t.Fatalf("terminal resident bytes = %d", reopened.state.ExecutionPinResidentBytes)
	}
}
