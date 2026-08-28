package routeforward

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func testDigest(seed byte) Digest {
	var digest Digest
	for index := range digest {
		digest[index] = seed + byte(index)
	}
	return digest
}

func testGroup(seed byte) raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: [16]byte{seed, 1}, ClusterIncarnation: [16]byte{seed, 2},
		TopologyRecoveryEpoch: uint64(seed) + 3,
		ShardIncarnation:      [16]byte{seed, 4}, GroupID: [16]byte{seed, 5},
	}
}

func testFence(seed uint64) raftservice.CommandFence {
	return raftservice.CommandFence{
		ReplicaSetVersion: seed + 1, ActivePolicyGeneration: seed + 2,
		ProtectionEpoch: seed + 3, OwnershipEpoch: seed + 4,
		SchemaGeneration: seed + 5, RelationManifestDigest: [32]byte{byte(seed), 9},
		RoutingVersion: seed + 6, RouteGeneration: seed + 7,
	}
}

func testAuthority(seed byte) RouteAuthority {
	return RouteAuthority{
		Group: testGroup(seed), AllocationGeneration: uint64(seed) + 11,
		Command: testFence(uint64(seed) + 20),
	}
}

func testTarget(seed byte) TargetRoute {
	return TargetRoute{
		Authority: testAuthority(seed), RouteSetDigest: testDigest(seed + 50),
	}
}

func testExactCommand(t testing.TB, old RouteAuthority) []byte {
	return testExactCommandAuthority(t, old, replication.CommandAuthorityData)
}

func testExactCommandAuthority(t testing.TB, old RouteAuthority, class replication.CommandAuthorityClass) []byte {
	t.Helper()
	command := replication.Command{
		Kind: replication.CommandMutationBatch, AuthorityClass: class,
		ClusterID:             replication.ID128(old.Group.ClusterID),
		ClusterIncarnation:    replication.ID128(old.Group.ClusterIncarnation),
		TopologyRecoveryEpoch: old.Group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration:   old.AllocationGeneration,
		ShardIncarnation:       replication.ID128(old.Group.ShardIncarnation),
		GroupID:                replication.ID128(old.Group.GroupID),
		ReplicaSetVersion:      old.Command.ReplicaSetVersion,
		ActivePolicyGeneration: old.Command.ActivePolicyGeneration,
		ProtectionEpoch:        old.Command.ProtectionEpoch, OwnershipEpoch: old.Command.OwnershipEpoch,
		SchemaGeneration: old.Command.SchemaGeneration,
		RoutingVersion:   old.Command.RoutingVersion, RouteGeneration: old.Command.RouteGeneration,
		Tenant: []byte("tenant"), ClientID: replication.ID128{1},
		ClientEpoch: 2, ClientSequence: 3, AckThrough: 1,
		Fingerprint: replication.Digest(testDigest(90)), RetryHome: replication.RetryHome{1},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"id":"k"}`),
			}},
		}},
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestMembershipStableForwardingRetainsExactSourceAuthority(t *testing.T) {
	entry, legacy := testEntry(t, TopologySplit, 2)
	exact := testExactCommandAuthority(t, entry.Old, replication.CommandAuthorityMembershipStableData)
	stable, err := BuildEntry(exact, entry.Kind, entry.Old, entry.PlanDigest, entry.Target, entry.Validity)
	if err != nil || stable.CommandDigest == entry.CommandDigest || bytes.Equal(exact, legacy) {
		t.Fatalf("stable command must have distinct, valid forwarding identity: %v", err)
	}
	// Forwarding certifies an exact old command, not a current serving fence.
	// Stable membership must not loosen the certificate's source identity.
	old := entry.Old
	old.Command.ReplicaSetVersion++
	if _, err = BuildEntry(exact, entry.Kind, old, entry.PlanDigest, entry.Target, entry.Validity); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("changed source membership accepted: %v", err)
	}
}

func testEntry(t testing.TB, kind TopologyKind, targetSeed byte) (Entry, []byte) {
	t.Helper()
	old := testAuthority(1)
	exact := testExactCommand(t, old)
	target := testTarget(targetSeed)
	entry, err := BuildEntry(exact, kind, old, testDigest(70), target, Validity{
		SourceAppliedFloor: 50, TargetAppliedFloor: 60,
		ValidFromCatalog: 20, RetainThroughCatalog: 22, ExpiresAfterCatalog: 25,
		GateEpoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return entry, exact
}

func testPublish(authority Digest, revision uint64, entry Entry) Command {
	return Command{
		Operation: OperationPublish, Authority: authority, AuthorityEpoch: 1,
		ExpectedRevision: revision, Key: EntryKey(entry), Entry: entry,
	}
}

func testActivate(authority, key Digest, revision uint64) Command {
	return Command{
		Operation: OperationActivate, Authority: authority, AuthorityEpoch: 1,
		ExpectedRevision: revision, Key: key,
	}
}

func requireOutcome(t testing.TB, outcome Outcome, reason Reason, mutated bool) {
	t.Helper()
	if outcome.Reason != reason || outcome.Mutated != mutated || outcome.Certificate == (Digest{}) {
		t.Fatalf("outcome = %+v, want reason=%d mutated=%t", outcome, reason, mutated)
	}
}

func TestPublishActivateBeforeDrainAndResolveWithoutRewriting(t *testing.T) {
	for _, topology := range []struct {
		name string
		kind TopologyKind
		seed byte
	}{
		{"split", TopologySplit, 2},
		{"move", TopologyMove, 3},
		{"replica-replacement", TopologyReplicaReplacement, 4},
	} {
		t.Run(topology.name, func(t *testing.T) {
			authority := testDigest(10)
			machine, ok := NewMachine(authority, 1, 8)
			if !ok {
				t.Fatal("machine")
			}
			entry, exact := testEntry(t, topology.kind, topology.seed)
			key := EntryKey(entry)
			publish := machine.Apply(testPublish(authority, 1, entry))
			requireOutcome(t, publish, ReasonPublished, true)
			if _, ok := machine.DrainBinding(key, 19); ok {
				t.Fatal("prepared forwarding entry authorized topology drain")
			}
			activate := machine.Apply(testActivate(authority, key, 2))
			requireOutcome(t, activate, ReasonActivated, true)
			binding, ok := machine.DrainBinding(key, 19)
			if !ok || Digest(binding) != activate.Certificate {
				t.Fatalf("active drain binding = %x, %v", binding, ok)
			}
			cut := ReadCut{
				Authority: authority, AuthorityEpoch: 1,
				AppliedRevision: machine.Status().Revision, ReadIndex: 99,
				CatalogGeneration: 20, TargetApplied: 60,
			}
			decision, reason := machine.Resolve(key, exact, cut)
			if reason != ReasonActivated || decision.Target != entry.Target ||
				decision.Certificate != activate.Certificate ||
				!bytes.Equal(decision.OriginalCommand, exact) ||
				&decision.OriginalCommand[0] != &exact[0] || cap(decision.OriginalCommand) != len(exact) {
				t.Fatalf("decision = %+v, reason=%d", decision, reason)
			}
		})
	}
}

func TestResponseLossIdempotencyAndCentralUniqueness(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, exact := testEntry(t, TopologyMove, 2)
	key := EntryKey(entry)
	publishCommand := testPublish(authority, 1, entry)
	requireOutcome(t, machine.Apply(publishCommand), ReasonPublished, true)
	requireOutcome(t, machine.Apply(publishCommand), ReasonIdempotent, false)
	activateCommand := testActivate(authority, key, 2)
	requireOutcome(t, machine.Apply(activateCommand), ReasonActivated, true)
	activeRetry := machine.Apply(activateCommand)
	requireOutcome(t, activeRetry, ReasonIdempotent, false)
	if activeRetry.State != EntryActive {
		t.Fatalf("activate response-loss retry state = %d", activeRetry.State)
	}
	cut := ReadCut{
		Authority: authority, AuthorityEpoch: 1, AppliedRevision: 3, ReadIndex: 9,
		CatalogGeneration: 20, TargetApplied: 60,
	}
	first, firstReason := machine.Resolve(key, exact, cut)
	second, secondReason := machine.Resolve(key, exact, cut)
	if firstReason != ReasonActivated || secondReason != ReasonActivated ||
		first.Target != second.Target || first.Certificate != second.Certificate ||
		&first.OriginalCommand[0] != &second.OriginalCommand[0] {
		t.Fatalf("response-loss resolve first=%+v/%d second=%+v/%d", first, firstReason, second, secondReason)
	}

	conflict := entry
	conflict.Target = testTarget(9)
	if EntryKey(conflict) != key {
		t.Fatal("target leaked into central exact-command uniqueness key")
	}
	requireOutcome(t, machine.Apply(testPublish(authority, 3, conflict)), ReasonConflict, false)
}

func TestStaleFormerLeaderAndTargetFloorFailClosed(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, exact := testEntry(t, TopologySplit, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, key, 2))
	cut := ReadCut{
		Authority: authority, AuthorityEpoch: 1,
		AppliedRevision:   machine.Status().Revision,
		CatalogGeneration: 20, TargetApplied: 60,
	}
	if _, reason := machine.Resolve(key, exact, cut); reason != ReasonStaleRead {
		t.Fatalf("former leader without ReadIndex reason = %d", reason)
	}
	cut.ReadIndex = 9
	cut.AppliedRevision = 2
	if _, reason := machine.Resolve(key, exact, cut); reason != ReasonStaleRead {
		t.Fatalf("former leader behind applied revision reason = %d", reason)
	}
	cut.AppliedRevision = machine.Status().Revision
	cut.TargetApplied = 59
	if _, reason := machine.Resolve(key, exact, cut); reason != ReasonTargetBehind {
		t.Fatalf("target below validity floor reason = %d", reason)
	}
	cut.TargetApplied = 60
	cut.CatalogGeneration = 19
	if _, reason := machine.Resolve(key, exact, cut); reason != ReasonTooEarly {
		t.Fatalf("pre-publication catalog reason = %d", reason)
	}
	cut.CatalogGeneration = 26
	if _, reason := machine.Resolve(key, exact, cut); reason != ReasonExpired {
		t.Fatalf("expired catalog reason = %d", reason)
	}
}

func TestForwardingBoundsRejectTTLAndRetainedAmplification(t *testing.T) {
	authority := testDigest(10)
	if _, ok := NewMachine(authority, 1, MaxRetainedRecords+1); ok {
		t.Fatal("accepted retained-record bound above snapshot geometry")
	}
	entry, exact := testEntry(t, TopologySplit, 2)
	invalid := entry.Validity
	invalid.ExpiresAfterCatalog = invalid.ValidFromCatalog + MaxCatalogGenerationTTL + 1
	if _, err := BuildEntry(
		exact, entry.Kind, entry.Old, entry.PlanDigest, entry.Target, invalid,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unbounded catalog TTL error = %v", err)
	}
	machine, _ := NewMachine(authority, 1, 1)
	requireOutcome(t, machine.Apply(testPublish(authority, 1, entry)), ReasonPublished, true)
	other := entry
	other.CommandDigest = testDigest(111)
	other.CommandFingerprint = testDigest(112)
	requireOutcome(t, machine.Apply(testPublish(authority, 2, other)), ReasonCapacity, false)
}

func TestEveryCommandVariantHasOneFixedCanonicalEncoding(t *testing.T) {
	authority := testDigest(10)
	entry, _ := testEntry(t, TopologyMove, 2)
	key := EntryKey(entry)
	commands := []struct {
		command Command
		bytes   int
	}{
		{testPublish(authority, 1, entry), PublishCommandBytes},
		{testActivate(authority, key, 2), ActivateCommandBytes},
		{Command{
			Operation: OperationPrune, Authority: authority, AuthorityEpoch: 1,
			ExpectedRevision: 3, Key: key,
			Clearance: Clearance{
				Key: key, CatalogGeneration: 23, RouteGateEpoch: 2, RouteGateRevision: 7,
				OldestRetryApplied: 51, AuthorityRevision: 3,
				GateCertificate: testDigest(101), RetryCertificate: testDigest(102),
			},
		}, PruneCommandBytes},
		{Command{
			Operation: OperationCompactRetired, Authority: authority, AuthorityEpoch: 1,
			NextAuthorityEpoch: 2, ExpectedRevision: 4, Key: compactKey(authority, 2),
		}, CompactCommandBytes},
	}
	for _, test := range commands {
		encoded, err := AppendCommand(make([]byte, 0, test.bytes), test.command)
		if err != nil || len(encoded) != test.bytes {
			t.Fatalf("append operation %d = %d, %v", test.command.Operation, len(encoded), err)
		}
		opened, err := OpenCommand(encoded)
		if err != nil || opened != test.command {
			t.Fatalf("open operation %d = %+v, %v", test.command.Operation, opened, err)
		}
		reencoded, err := AppendCommand(make([]byte, 0, test.bytes), opened)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("noncanonical operation %d: %v", test.command.Operation, err)
		}
	}
}

func TestBuildClearanceConsumesExactQuiescentRouteGateSettlement(t *testing.T) {
	gate, _ := routegate.NewMachine(1, 2)
	identity := routegate.Identity(testDigest(120))
	binding := routegate.Binding(testDigest(121))
	held := gate.Apply(routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: binding,
	})
	heldBytes, err := routegate.AppendOutcome(nil, held)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildClearance(
		testDigest(122), 23, 3, heldBytes,
		RetryCut{OldestApplied: 51, Certificate: testDigest(123)},
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("active-pin gate settlement error = %v", err)
	}
	gate.Apply(routegate.Command{
		Operation: routegate.OperationReleaseShared, Epoch: 1,
		Identity: identity, Binding: binding,
	})
	compacted := gate.Apply(routegate.Command{
		Operation: routegate.OperationCompactReleased, Epoch: 1,
	})
	settlement, err := routegate.AppendOutcome(nil, compacted)
	if err != nil {
		t.Fatal(err)
	}
	key := testDigest(122)
	clearance, err := BuildClearance(
		key, 23, 3, settlement,
		RetryCut{OldestApplied: 51, Certificate: testDigest(123)},
	)
	if err != nil || clearance.Key != key || clearance.RouteGateEpoch != 2 ||
		clearance.RouteGateRevision != 3 || clearance.ActivePins != 0 ||
		clearance.GateCertificate != Digest(sha256.Sum256(settlement)) {
		t.Fatalf("clearance = %+v, %v", clearance, err)
	}
}

func TestPruneRequiresPinRetryAndTTLAuthorityAndPreventsResurrection(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, _ := testEntry(t, TopologyMove, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, key, 2))
	clearance := Clearance{
		Key: key, CatalogGeneration: 23, RouteGateEpoch: 2, RouteGateRevision: 7,
		OldestRetryApplied: 51, AuthorityRevision: 3,
		GateCertificate: testDigest(101), RetryCertificate: testDigest(102),
	}
	prune := Command{
		Operation: OperationPrune, Authority: authority, AuthorityEpoch: 1,
		ExpectedRevision: 3, Key: key, Clearance: clearance,
	}
	tooEarly := prune
	tooEarly.Clearance.CatalogGeneration = 22
	requireOutcome(t, machine.Apply(tooEarly), ReasonTooEarly, false)
	pins := prune
	pins.Clearance.ActivePins = 1
	requireOutcome(t, machine.Apply(pins), ReasonPinsActive, false)
	retry := prune
	retry.Clearance.OldestRetryApplied = 50
	requireOutcome(t, machine.Apply(retry), ReasonRetryWindow, false)
	pruned := machine.Apply(prune)
	requireOutcome(t, pruned, ReasonPruned, true)
	if status := machine.Status(); status.Live != 0 || status.Tombstones != 1 {
		t.Fatalf("pruned status = %+v", status)
	}
	requireOutcome(t, machine.Apply(prune), ReasonIdempotent, false)
	requireOutcome(t, machine.Apply(testPublish(authority, 4, entry)), ReasonRetired, false)
}

func TestCompactRetiredAdvancesAuthorityBeforeReusingBoundedCapacity(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 2)
	entry, _ := testEntry(t, TopologyMove, 2)
	key := EntryKey(entry)
	delayedPublish := testPublish(authority, 1, entry)
	machine.Apply(delayedPublish)
	machine.Apply(testActivate(authority, key, 2))
	machine.Apply(Command{
		Operation: OperationPrune, Authority: authority, AuthorityEpoch: 1,
		ExpectedRevision: 3, Key: key,
		Clearance: Clearance{
			Key: key, CatalogGeneration: 23, RouteGateEpoch: 2, RouteGateRevision: 7,
			OldestRetryApplied: 51, AuthorityRevision: 3,
			GateCertificate: testDigest(101), RetryCertificate: testDigest(102),
		},
	})
	compact := Command{
		Operation: OperationCompactRetired, Authority: authority, AuthorityEpoch: 1,
		NextAuthorityEpoch: 2, ExpectedRevision: 4,
		Key: compactKey(authority, 2),
	}
	preview := machine.Preview(compact)
	requireOutcome(t, preview, ReasonCompacted, true)
	if applied := machine.Apply(compact); applied != preview {
		t.Fatalf("compact preview=%+v apply=%+v", preview, applied)
	}
	if status := machine.Status(); status != (Status{Revision: 5, AuthorityEpoch: 2}) {
		t.Fatalf("compacted status = %+v", status)
	}
	requireOutcome(t, machine.Apply(delayedPublish), ReasonStaleAuthority, false)
	fresh := testPublish(authority, 5, entry)
	fresh.AuthorityEpoch = 2
	requireOutcome(t, machine.Apply(fresh), ReasonPublished, true)
}

func TestPreviewAndFixedCodecsAreCanonicalAllocationFree(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, _ := testEntry(t, TopologyMove, 2)
	command := testPublish(authority, 1, entry)
	preview := machine.Preview(command)
	if machine.Status() != (Status{Revision: 1, AuthorityEpoch: 1}) {
		t.Fatalf("preview mutated machine = %+v", machine.Status())
	}
	if applied := machine.Apply(command); applied != preview {
		t.Fatalf("preview=%+v apply=%+v", preview, applied)
	}

	var entryStorage [EntryBytes]byte
	var commandStorage [PublishCommandBytes]byte
	var outcomeStorage [OutcomeBytes]byte
	encodedEntry, err := AppendEntry(entryStorage[:0], entry)
	if err != nil {
		t.Fatal(err)
	}
	encodedCommand, err := AppendCommand(commandStorage[:0], command)
	if err != nil {
		t.Fatal(err)
	}
	encodedOutcome, err := AppendOutcome(outcomeStorage[:0], preview)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if got := machine.Preview(command); got.Reason != ReasonIdempotent {
			panic(got.Reason)
		}
		if _, appendErr := AppendEntry(entryStorage[:0], entry); appendErr != nil {
			panic(appendErr)
		}
		if _, openErr := OpenEntry(encodedEntry); openErr != nil {
			panic(openErr)
		}
		if _, appendErr := AppendCommand(commandStorage[:0], command); appendErr != nil {
			panic(appendErr)
		}
		if _, openErr := OpenCommand(encodedCommand); openErr != nil {
			panic(openErr)
		}
		if _, appendErr := AppendOutcome(outcomeStorage[:0], preview); appendErr != nil {
			panic(appendErr)
		}
		if _, openErr := OpenOutcome(encodedOutcome); openErr != nil {
			panic(openErr)
		}
	}); !raceDetectorEnabled && allocations != 0 {
		t.Fatalf("fixed codec allocations = %v, want 0", allocations)
	}
	for offset := range encodedCommand {
		corrupt := bytes.Clone(encodedCommand)
		corrupt[offset] ^= 1
		if _, err := OpenCommand(corrupt); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted command corruption at %d: %v", offset, err)
		}
	}
}

func TestCrashReopenPreservesActiveForwardingAndCompactTombstone(t *testing.T) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, exact := testEntry(t, TopologyReplicaReplacement, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, key, 2))
	activeBytes, _ := SnapshotBytes(1, 0)
	activeStorage := make([]byte, 0, activeBytes)
	activeImage, err := AppendSnapshot(activeStorage, machine, make([]LiveRecord, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSnapshot(activeImage, 8)
	if err != nil {
		t.Fatal(err)
	}
	cut := ReadCut{
		Authority: authority, AuthorityEpoch: 1, AppliedRevision: reopened.Status().Revision,
		ReadIndex: 90, CatalogGeneration: 20, TargetApplied: 60,
	}
	if decision, reason := reopened.Resolve(key, exact, cut); reason != ReasonActivated ||
		decision.Certificate == (Digest{}) {
		t.Fatalf("reopened resolve = %+v, %d", decision, reason)
	}

	clearance := Clearance{
		Key: key, CatalogGeneration: 23, RouteGateEpoch: 2, RouteGateRevision: 7,
		OldestRetryApplied: 51, AuthorityRevision: 3,
		GateCertificate: testDigest(101), RetryCertificate: testDigest(102),
	}
	reopened.Apply(Command{
		Operation: OperationPrune, Authority: authority, AuthorityEpoch: 1,
		ExpectedRevision: 3, Key: key, Clearance: clearance,
	})
	tombBytes, _ := SnapshotBytes(0, 1)
	if activeBytes != SnapshotHeaderBytes+SnapshotLiveBytes+SnapshotChecksumBytes ||
		tombBytes != SnapshotHeaderBytes+SnapshotTombstoneBytes+SnapshotChecksumBytes ||
		tombBytes >= activeBytes/2 {
		t.Fatalf("snapshot amplification active=%d tomb=%d", activeBytes, tombBytes)
	}
	tombStorage := make([]byte, 0, tombBytes)
	tombImage, err := AppendSnapshot(tombStorage, reopened, nil, make([]TombstoneRecord, 1))
	if err != nil {
		t.Fatal(err)
	}
	retired, err := OpenSnapshot(tombImage, 8)
	if err != nil {
		t.Fatal(err)
	}
	cut.AppliedRevision = retired.Status().Revision
	if _, reason := retired.Resolve(key, exact, cut); reason != ReasonRetired {
		t.Fatalf("reopened tombstone reason = %d", reason)
	}
}

func TestForwardingGeometryBound(t *testing.T) {
	if EntryBytes != 560 || PublishCommandBytes != 668 || ActivateCommandBytes != 108 ||
		PruneCommandBytes != 300 || CompactCommandBytes != 108 || OutcomeBytes != 108 || SnapshotLiveBytes != 616 ||
		SnapshotTombstoneBytes != 88 || MaxRetainedRecords == 0 {
		t.Fatalf("unexpected geometry entry=%d publish=%d activate=%d prune=%d compact=%d outcome=%d live=%d tomb=%d max=%d",
			EntryBytes, PublishCommandBytes, ActivateCommandBytes, PruneCommandBytes,
			CompactCommandBytes, OutcomeBytes, SnapshotLiveBytes, SnapshotTombstoneBytes, MaxRetainedRecords)
	}
	if bytes, ok := SnapshotBytes(MaxRetainedRecords, 0); !ok || bytes > MaxSnapshotBytes ||
		MaxSnapshotBytes-bytes >= SnapshotLiveBytes {
		t.Fatalf("max snapshot geometry = %d, %v", bytes, ok)
	}
}

func BenchmarkResolveExactForward(b *testing.B) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, exact := testEntry(b, TopologySplit, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, key, 2))
	cut := ReadCut{
		Authority: authority, AuthorityEpoch: 1, AppliedRevision: 3,
		ReadIndex: 9, CatalogGeneration: 20, TargetApplied: 60,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(exact)))
	for b.Loop() {
		decision, reason := machine.Resolve(key, exact, cut)
		if reason != ReasonActivated || len(decision.OriginalCommand) != len(exact) {
			b.Fatal(reason)
		}
	}
}

func BenchmarkPreviewForwardingPublish(b *testing.B) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, _ := testEntry(b, TopologyMove, 2)
	command := testPublish(authority, 1, entry)
	b.ReportAllocs()
	b.SetBytes(PublishCommandBytes)
	for b.Loop() {
		if outcome := machine.Preview(command); outcome.Reason != ReasonPublished {
			b.Fatal(outcome)
		}
	}
}

func BenchmarkPreviewForwardingActivate(b *testing.B) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, _ := testEntry(b, TopologyMove, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	command := testActivate(authority, key, 2)
	b.ReportAllocs()
	b.SetBytes(ActivateCommandBytes)
	for b.Loop() {
		if outcome := machine.Preview(command); outcome.Reason != ReasonActivated {
			b.Fatal(outcome)
		}
	}
}

func BenchmarkAppendForwardingSnapshot4096(b *testing.B) {
	const records = 4096
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, records)
	base, _ := testEntry(b, TopologyMove, 2)
	for ordinal := uint64(0); ordinal < records; ordinal++ {
		entry := base
		entry.CommandDigest = Digest(sha256.Sum256([]byte{
			byte(ordinal), byte(ordinal >> 8), byte(ordinal >> 16), byte(ordinal >> 24),
		}))
		entry.CommandFingerprint = entry.CommandDigest
		command := testPublish(authority, machine.revision, entry)
		if outcome := machine.Apply(command); outcome.Reason != ReasonPublished {
			b.Fatal(outcome)
		}
	}
	total, _ := SnapshotBytes(records, 0)
	storage := make([]byte, 0, total)
	scratch := make([]LiveRecord, records)
	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ReportMetric(float64(total)/records, "B/record")
	for b.Loop() {
		if _, err := AppendSnapshot(storage[:0], machine, scratch, nil); err != nil {
			b.Fatal(err)
		}
	}
}
