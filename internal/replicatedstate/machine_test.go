package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type machineFixture struct {
	machine   *Machine
	binding   Binding
	bootstrap *pb.Snapshot
	system    CollectionTarget
	user      CollectionTarget
	log       *durable.TxnLog
	dir       string
}

var defaultUserValidationDigest = sha256.Sum256([]byte("replicatedstate/test-user-validation"))

type acceptAllMutationValidator struct{}

func (acceptAllMutationValidator) ValidatePut(_, _ []byte) MutationValidation {
	return MutationValidationAccept
}

func (acceptAllMutationValidator) ValidateDelete(_, _ []byte, _ bool) MutationValidation {
	return MutationValidationAccept
}

func newMachineFixture(t testing.TB) machineFixture {
	t.Helper()
	dir := t.TempDir()
	openCollection := func(name string, options durable.Options) CollectionTarget {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}
	system := openCollection("system", durable.Options{OpaqueValues: true})
	system = systemTargetOf(system.Collection)
	user := openCollection("user", durable.Options{})
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := testBinding()
	bootstrap := testBootstrap()
	options := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 2,
			MaxDocuments:   user.Limits.MaxDistinctMutations + 3,
			MaxBytes:       64 << 20,
		},
		MaxSessions: 128,
		RetryWindow: 8,
	}
	machine, err := Open(binding, bootstrap, system, UserCollection{Name: "docs", Target: user}, log, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return machineFixture{machine, binding, bootstrap, system, user, log, dir}
}

func targetOf(collection *durable.Collection) CollectionTarget {
	return CollectionTarget{
		Collection: collection, Validation: ValidationDeterministicMutation,
		ValidationDigest: defaultUserValidationDigest,
		Validator:        acceptAllMutationValidator{},
		Limits: CollectionLimits{
			MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
			MaxDistinctMutations: collection.MaxBatchDocuments(), MaxBatchBytes: collection.MaxBatchBytes(),
		},
	}
}

func systemTargetOf(collection *durable.Collection) CollectionTarget {
	target := targetOf(collection)
	target.Validation = ValidationOpaqueBinary
	target.ValidationDigest = [32]byte{}
	target.Validator = nil
	return target
}

func testBinding() Binding {
	return Binding{
		ClusterID: id128(1), ClusterIncarnation: id128(2), TopologyRecoveryEpoch: 3,
		Distribution: "dist", Shard: "shard", AllocationGeneration: 4,
		ShardIncarnation: id128(5), GroupID: id128(6),
		ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9,
		SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
	}
}

func id128(seed byte) replication.ID128 {
	var id replication.ID128
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testBootstrap() *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{
		Data: []byte("static-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
}

func testCommand(binding Binding, sequence uint64, mutations ...replication.Mutation) []byte {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x91})
	command, err := replication.AppendCommand(nil, replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		// Test fixtures open their initial session at Raft index 2. Callers pass
		// a zero-based user-request ordinal: the open owns sequence 1, so the
		// first mutation is sequence 2.
		ClientID: id128(77), ClientEpoch: 2, ClientSequence: sequence + 1,
		Fingerprint: fingerprint,
		Batches:     []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}},
	})
	if err != nil {
		panic(err)
	}
	return command
}

func normalMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func TestMachineApplyDedupeConflictStaleAndReopen(t *testing.T) {
	fixture := newMachineFixture(t)
	machine := fixture.machine
	if machine.Applied() != 0 {
		t.Fatalf("initial Applied = %d", machine.Applied())
	}
	if _, err := machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	applySessionOpen(t, machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":2}`)},
	)
	if err := machine.AdmitCommand(command); err != nil {
		t.Fatalf("AdmitCommand: %v", err)
	}
	publication, err := machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatalf("ApplyNormal: %v", err)
	}
	if publication.Applied != 3 {
		t.Fatalf("Applied = %d", publication.Applied)
	}
	value, found, err := fixture.user.Collection.AppendRaw(nil, []byte("k"))
	if err != nil || !found || !bytes.Equal(value, []byte(`{"n":2}`)) {
		t.Fatalf("user value = %q,%v,%v", value, found, err)
	}
	first, err := machine.LookupCompletion(command)
	if err != nil || first.AppliedSequence != 3 {
		t.Fatalf("LookupCompletion = %+v,%v", first, err)
	}
	completion, err := replication.OpenCompletion(first.Bytes)
	if err != nil || completion.ResultCode != ResultApplied {
		t.Fatalf("completion = %+v,%v", completion, err)
	}

	if _, err := machine.ApplyNormal(normalMeta(4), command); err != nil {
		t.Fatalf("exact duplicate apply: %v", err)
	}
	duplicate, err := machine.LookupCompletion(command)
	if err != nil || duplicate.AppliedSequence != 3 || !bytes.Equal(duplicate.Bytes, first.Bytes) {
		t.Fatalf("duplicate completion = %+v,%v", duplicate, err)
	}

	conflict := bytes.Clone(command)
	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	conflictingCommand := replication.Command{
		ClusterID: fixture.binding.ClusterID, ClusterIncarnation: fixture.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: fixture.binding.TopologyRecoveryEpoch,
		Distribution:          fixture.binding.Distribution, Shard: fixture.binding.Shard,
		AllocationGeneration: fixture.binding.AllocationGeneration,
		ShardIncarnation:     fixture.binding.ShardIncarnation, GroupID: fixture.binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
		ProtectionEpoch: fixture.binding.ProtectionEpoch, OwnershipEpoch: fixture.binding.OwnershipEpoch,
		SchemaGeneration: fixture.binding.SchemaGeneration, RoutingVersion: fixture.binding.RoutingVersion,
		RouteGeneration: fixture.binding.RouteGeneration, Tenant: bytes.Clone(view.Tenant),
		ClientID: view.ClientID, ClientEpoch: view.ClientEpoch, ClientSequence: view.ClientSequence,
		Fingerprint: view.Fingerprint, RetryHome: view.RetryHome,
		Batches: []replication.RelationMutationBatch{{
			Relation:  1,
			Mutations: []replication.Mutation{{Kind: replication.MutationDelete, Key: []byte("k")}},
		}},
	}
	conflict, err = replication.AppendCommand(conflict[:0], conflictingCommand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.ApplyNormal(normalMeta(5), conflict); err != nil {
		t.Fatalf("conflict apply: %v", err)
	}
	gotOriginal, err := machine.LookupCompletion(command)
	if err != nil || !bytes.Equal(gotOriginal.Bytes, first.Bytes) {
		t.Fatalf("original after conflict = %+v,%v", gotOriginal, err)
	}
	conflictingLookup, err := machine.LookupCompletion(conflict)
	if !errors.Is(err, ErrRequestConflict) || !bytes.Equal(conflictingLookup.Bytes, first.Bytes) {
		t.Fatalf("conflicting lookup = %+v,%v", conflictingLookup, err)
	}

	stale := testCommand(fixture.binding, 2,
		replication.Mutation{Kind: replication.MutationDelete, Key: []byte("k")},
	)
	staleView, _ := replication.OpenCommand(stale)
	staleCommand := replication.Command{
		ClusterID: fixture.binding.ClusterID, ClusterIncarnation: fixture.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: fixture.binding.TopologyRecoveryEpoch,
		Distribution:          fixture.binding.Distribution, Shard: fixture.binding.Shard,
		AllocationGeneration: fixture.binding.AllocationGeneration,
		ShardIncarnation:     fixture.binding.ShardIncarnation, GroupID: fixture.binding.GroupID,
		ReplicaSetVersion: 2, ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
		ProtectionEpoch: fixture.binding.ProtectionEpoch, OwnershipEpoch: fixture.binding.OwnershipEpoch,
		SchemaGeneration: fixture.binding.SchemaGeneration, RoutingVersion: fixture.binding.RoutingVersion,
		RouteGeneration: fixture.binding.RouteGeneration, Tenant: staleView.Tenant,
		ClientID: staleView.ClientID, ClientEpoch: staleView.ClientEpoch,
		ClientSequence: staleView.ClientSequence, Fingerprint: staleView.Fingerprint,
		Batches: []replication.RelationMutationBatch{{
			Relation:  1,
			Mutations: []replication.Mutation{{Kind: replication.MutationDelete, Key: []byte("k")}},
		}},
	}
	stale, err = replication.AppendCommand(nil, staleCommand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.ApplyNormal(normalMeta(6), stale); err != nil {
		t.Fatalf("stale apply: %v", err)
	}
	staleLookup, err := machine.LookupCompletion(stale)
	if err != nil {
		t.Fatal(err)
	}
	staleCompletion, err := replication.OpenCompletion(staleLookup.Bytes)
	if err != nil || staleCompletion.ResultCode != ResultStaleFence {
		t.Fatalf("stale completion = %+v,%v", staleCompletion, err)
	}
	value, found, _ = fixture.user.Collection.AppendRaw(nil, []byte("k"))
	if !found || !bytes.Equal(value, []byte(`{"n":2}`)) {
		t.Fatalf("stale command mutated value = %q,%v", value, found)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, machine.options,
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.Applied() != 6 || reopened.Published().DataChainDigest != machine.Published().DataChainDigest {
		t.Fatalf("reopened publication = %+v", reopened.Published())
	}
	snapshot, err := reopened.Snapshot("docs")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Close()
	if snapshot.Publication().Applied != 6 || snapshot.State().SessionCount != 1 ||
		snapshot.State().SessionSlotCount != 3 || snapshot.State().SessionEpochHighWater != 2 {
		t.Fatalf("snapshot state = %+v", snapshot.State())
	}
	systemRows := 0
	if err := snapshot.RangeSystem(func(_, _ []byte) error { systemRows++; return nil }); err != nil {
		t.Fatal(err)
	}
	if systemRows != 5 {
		t.Fatalf("system rows = %d, want state + session + 3 slots", systemRows)
	}
}

func TestMachineLookupCompletionIntoUsesExactCallerStorage(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	short := make([]byte, 7, replication.MaxEmptyResultCompletionEnvelopeBytes-1)
	if lookup, err := fixture.machine.LookupCompletionInto(command, short); !errors.Is(err, ErrCompletionBufferSmall) ||
		lookup.Key != ([32]byte{}) || len(lookup.Bytes) != 0 ||
		lookup.AppliedSequence != 0 || len(short) != 7 {
		t.Fatalf("short lookup = %+v, %v, len %d", lookup, err, len(short))
	}
	scratch := make([]byte, 9, replication.MaxEmptyResultCompletionEnvelopeBytes+32)
	lookup, err := fixture.machine.LookupCompletionInto(command, scratch)
	if err != nil || len(lookup.Bytes) == 0 || lookup.AppliedSequence != 3 ||
		&lookup.Bytes[0] != &scratch[:cap(scratch)][0] {
		t.Fatalf("caller-backed lookup = %+v, %v", lookup, err)
	}
	want := bytes.Clone(lookup.Bytes)
	lookup.Bytes[0] ^= 0xff
	again, err := fixture.machine.LookupCompletionInto(command, scratch)
	if err != nil || !bytes.Equal(again.Bytes, want) || &again.Bytes[0] != &scratch[:cap(scratch)][0] {
		t.Fatalf("reused lookup = %+v, %v", again, err)
	}
}

func TestMachineCompletionLookupWorkspaceIsExactReusableAndAllocationFree(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`)},
	)
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	var workspace CompletionLookupWorkspace
	scratch := make([]byte, 0, replication.MaxEmptyResultCompletionEnvelopeBytes)
	lookup := func() {
		if err := fixture.machine.BeginCompletionLookupBatch(&workspace, publication); err != nil {
			panic(err)
		}
		result, lookupErr := fixture.machine.LookupCompletionIntoWorkspace(
			&workspace, command, scratch[:0],
		)
		if lookupErr != nil || len(result.Bytes) == 0 || result.AppliedSequence != 3 ||
			&result.Bytes[0] != &scratch[:cap(scratch)][0] {
			panic("unexpected workspace completion")
		}
		if len(workspace.scratch.sessionRead) == 0 || len(workspace.scratch.slotRead) == 0 ||
			&workspace.scratch.sessionRead[0] != &workspace.sessionRead[0] ||
			&workspace.scratch.slotRead[0] != &workspace.slotRead[0] ||
			cap(workspace.scratch.decodeRead) != len(workspace.decodeRead) ||
			&workspace.scratch.decodeRead[:cap(workspace.scratch.decodeRead)][0] !=
				&workspace.decodeRead[0] {
			panic("completion lookup escaped fixed decode storage")
		}
		if err := fixture.machine.EndCompletionLookupBatch(&workspace); err != nil {
			panic(err)
		}
	}
	lookup()
	if err := fixture.machine.BeginCompletionLookupBatch(&workspace, publication); err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.BeginCompletionLookupBatch(&workspace, publication); !errors.Is(err, ErrCompletionWorkspaceBusy) {
		t.Fatalf("nested begin = %v", err)
	}
	if err := fixture.machine.EndCompletionLookupBatch(&workspace); err != nil {
		t.Fatal(err)
	}
	stale := publication
	stale.Applied--
	if err := fixture.machine.BeginCompletionLookupBatch(&workspace, stale); !errors.Is(err, ErrCompletionPublication) {
		t.Fatalf("stale publication = %v", err)
	}
	allocs := testing.AllocsPerRun(1_000, lookup)
	if allocs != 0 {
		t.Fatalf("warm exact-cut completion lookup allocations = %v, want 0", allocs)
	}
	if err := workspace.Release(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMachineCompletionLookupWorkspace(b *testing.B) {
	fixture := newMachineFixture(b)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	applySessionOpen(b, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`)},
	)
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		b.Fatal(err)
	}
	var workspace CompletionLookupWorkspace
	scratch := make([]byte, 0, replication.MaxEmptyResultCompletionEnvelopeBytes)
	if err := fixture.machine.BeginCompletionLookupBatch(&workspace, publication); err != nil {
		b.Fatal(err)
	}
	if _, err := fixture.machine.LookupCompletionIntoWorkspace(
		&workspace, command, scratch[:0],
	); err != nil {
		b.Fatal(err)
	}
	if err := fixture.machine.EndCompletionLookupBatch(&workspace); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := fixture.machine.BeginCompletionLookupBatch(&workspace, publication); err != nil {
			b.Fatal(err)
		}
		if _, err := fixture.machine.LookupCompletionIntoWorkspace(
			&workspace, command, scratch[:0],
		); err != nil {
			b.Fatal(err)
		}
		if err := fixture.machine.EndCompletionLookupBatch(&workspace); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := workspace.Release(); err != nil {
		b.Fatal(err)
	}
}

func TestMachineConfigurationAndEmptyNormalPreserveDataChainDigest(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	initial := fixture.machine.Published().DataChainDigest
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); err != nil {
		t.Fatal(err)
	}
	meta := raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryConfChange}
	conf := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	publication, err := fixture.machine.ApplyConfiguration(meta, conf)
	if err != nil {
		t.Fatal(err)
	}
	if publication.DataChainDigest != initial || publication.ReplicaSetVersion != 3 {
		t.Fatalf("configuration publication = %+v", publication)
	}
	if _, err := fixture.machine.ApplyConfiguration(meta, conf); err != nil {
		t.Fatalf("same-index exact replay: %v", err)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen after configuration: %v", err)
	}
	got := reopened.Published()
	if got.Applied != 3 || got.ReplicaSetVersion != 3 || got.ConfState.Equivalent(conf) != nil {
		t.Fatalf("reopened configuration = %+v", got)
	}
}

func TestMachinePhysicalReopenRecoversAtomicUserCompletionAndState(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"ok":true}`),
	})
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.user.Collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.log.Close(); err != nil {
		t.Fatal(err)
	}
	files := make([]*os.File, 0, 2)
	requests := make([]durable.TransactionCollectionOpen, 0, 2)
	for _, name := range []string{"system", "user"} {
		file, err := os.OpenFile(filepath.Join(fixture.dir, name+".vdb"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		requests = append(requests, durable.TransactionCollectionOpen{
			File: file, Options: durable.Options{},
		})
	}
	collections, log, err := durable.OpenCollectionsWithTransactions(
		fixture.dir, durable.TxnLogOptions{}, requests,
	)
	if err != nil {
		for _, file := range files {
			_ = file.Close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, collection := range collections {
			_ = collection.Close()
		}
		if log != nil {
			_ = log.Close()
		}
		for _, file := range files {
			_ = file.Close()
		}
	})
	system, user := targetOf(collections[0]), targetOf(collections[1])
	system = systemTargetOf(system.Collection)
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, system, UserCollection{Name: "docs", Target: user},
		log, fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Published().Applied != 3 || reopened.Published().DataChainDigest != publication.DataChainDigest {
		t.Fatalf("reopened publication = %+v, want %+v", reopened.Published(), publication)
	}
	value, found, err := user.Collection.AppendRaw(nil, []byte("k"))
	if err != nil || !found || !bytes.Equal(value, []byte(`{"ok":true}`)) {
		t.Fatalf("reopened user row = %q,%v,%v", value, found, err)
	}
	lookup, err := reopened.LookupCompletion(command)
	if err != nil || lookup.AppliedSequence != 3 {
		t.Fatalf("reopened completion = %+v,%v", lookup, err)
	}
}

func TestMachineCommittedTargetBoundBecomesDeterministicNoop(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	mutations := make([]replication.Mutation, MaxDistinctMutations+1)
	for i := range mutations {
		mutations[i] = replication.Mutation{
			Kind: replication.MutationPut, Key: []byte{byte(i + 1)}, Value: []byte(`{"n":1}`),
		}
	}
	command := testCommand(fixture.binding, 1, mutations...)
	if err := fixture.machine.AdmitCommand(command); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("AdmitCommand error = %v", err)
	}
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatalf("committed target-bound apply: %v", err)
	}
	if publication.Applied != 3 || fixture.user.Collection.Len() != 0 {
		t.Fatalf("target-bound publication=%+v rows=%d", publication, fixture.user.Collection.Len())
	}
	lookup, err := fixture.machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultTargetBound {
		t.Fatalf("target-bound completion = %+v,%v", completion, err)
	}
}

func TestMachineWrongImmutableBindingIsTerminal(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	wrong := fixture.binding
	wrong.GroupID = id128(99)
	command := testCommand(wrong, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); !errors.Is(err, ErrWrongBinding) {
		t.Fatalf("wrong-binding error = %v", err)
	}
	if fixture.machine.Applied() != 1 || fixture.user.Collection.Len() != 0 {
		t.Fatalf("wrong binding changed publication or data")
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("post-terminal apply error = %v", err)
	}
}
