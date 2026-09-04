package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

type recordingCoordinatorStager struct {
	t              testing.TB
	inline         []byte
	manifest       distributedtxn.ManifestCoordinatorRecord
	reader         *distributedtxn.ManifestReader
	targets        []distributedtxn.TransactionTargetRef
	identities     []byte
	segments       uint32
	decodedTargets uint64
}

func (s *recordingCoordinatorStager) stageInlineCoordinator(record []byte) error {
	s.inline = bytes.Clone(record)
	return nil
}

func (s *recordingCoordinatorStager) stageManifestCoordinator(record, firstSegment []byte) error {
	s.t.Helper()
	manifest, err := distributedtxn.OpenManifestCoordinator(record)
	if err != nil {
		s.t.Fatalf("open manifest coordinator: %v", err)
	}
	reader, err := distributedtxn.NewManifestReader(manifest.Manifest)
	if err != nil {
		s.t.Fatalf("new manifest reader: %v", err)
	}
	s.manifest = manifest
	s.reader = reader
	s.targets = make([]distributedtxn.TransactionTargetRef, distributedtxn.MaxManifestPageTargets)
	s.identities = make([]byte, distributedtxn.MaxManifestPageTargets*distributedtxn.MaxShardIdentityBytes*2)
	return s.stageManifestSegment(manifest.ID, 0, firstSegment)
}

func (s *recordingCoordinatorStager) stageManifestSegment(
	id distributedtxn.ID,
	index uint32,
	record []byte,
) error {
	s.t.Helper()
	if id != s.manifest.ID || index != s.segments {
		s.t.Fatalf("segment identity/index = %x/%d, want %x/%d", id, index, s.manifest.ID, s.segments)
	}
	page, err := s.reader.OpenNext(record, s.targets, s.identities)
	if err != nil {
		s.t.Fatalf("open segment %d: %v", index, err)
	}
	s.decodedTargets += uint64(len(page.Targets))
	s.segments++
	return nil
}

func testTransactionRefs(count int) []distributedtxn.TransactionTargetRef {
	refs := make([]distributedtxn.TransactionTargetRef, count)
	for i := range refs {
		refs[i] = distributedtxn.TransactionTargetRef{
			Distribution:   []byte("data"),
			Shard:          []byte(fmt.Sprintf("s%08d", i)),
			RoutingVersion: 7, AllocationGeneration: uint64(i + 1),
			OwnershipEpoch: uint64(i + 11), MutationDigest: distributedtxn.Digest{1},
			State: distributedtxn.TargetStaged,
		}
	}
	return refs
}

func testCoordinatorRecord(count int) distributedtxn.CoordinatorRecord {
	var id distributedtxn.ID
	id[0] = 9
	return distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 7, RecoveryDeadline: 3,
		Targets: testTransactionRefs(count),
	}
}

func TestStageTransactionCoordinatorPreservesInlineVTC1(t *testing.T) {
	record := testCoordinatorRecord(2)
	want, err := distributedtxn.AppendCoordinator(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	stager := &recordingCoordinatorStager{t: t}
	format, err := stageTransactionCoordinator(record, stager)
	if err != nil {
		t.Fatal(err)
	}
	if format != transactionCoordinatorInline || !bytes.Equal(stager.inline, want) || stager.reader != nil {
		t.Fatalf("format=%d inline_equal=%t segmented=%t", format, bytes.Equal(stager.inline, want), stager.reader != nil)
	}
}

func TestStageTransactionCoordinatorStreams65Targets(t *testing.T) {
	testStageTransactionCoordinatorWide(t, 65)
}

func TestStageTransactionCoordinatorStreams4097Targets(t *testing.T) {
	testStageTransactionCoordinatorWide(t, 4097)
}

func TestIndexedTransactionGroupingAdmits4097ExactTargets(t *testing.T) {
	targets := make([]transactionTarget, 0, 4097)
	byTarget := make(map[transactionTargetKey]int, 4097)
	for i := range 4097 {
		request := &shardservice.ShardRequest{
			Distribution: "data", Shard: distribution.ShardID(fmt.Sprintf("s%08d", i)),
			RoutingVersion: 7, AllocationGeneration: distribution.ShardAllocationGeneration(i + 1),
			OwnershipEpoch: distribution.OwnershipEpoch(i + 11),
		}
		var err error
		targets, err = appendTransactionStatementIndexed(
			targets, byTarget, shardCall{req: request}, shardservice.MutationStatement{SQL: "x"},
		)
		if err != nil {
			t.Fatalf("append target %d: %v", i, err)
		}
	}
	if len(targets) != 4097 || len(byTarget) != 4097 {
		t.Fatalf("participants=%d index=%d", len(targets), len(byTarget))
	}
	targets, err := appendTransactionStatementIndexed(
		targets, byTarget, targets[4096].call, shardservice.MutationStatement{SQL: "y"},
	)
	if err != nil || len(targets) != 4097 || len(targets[4096].statements) != 2 {
		t.Fatalf("exact duplicate grouping participants=%d statements=%d err=%v",
			len(targets), len(targets[4096].statements), err)
	}
}

func TestHugeSameShardPlanningRetainsOnlyAdmittedBytes(t *testing.T) {
	profile := DefaultProfiles()[ClassInteractive].withDefaults()
	profile.MaxTransactionMutations = 1_000_000
	profile.MaxTransactionBytes = 128
	request := &shardservice.ShardRequest{
		Distribution: "data", Shard: "same", RoutingVersion: 7,
		AllocationGeneration: 1, OwnershipEpoch: 11,
	}
	plan := func() ([]transactionTarget, error) {
		targets := make([]transactionTarget, 0,
			transactionTargetCapacity(nil, 1_000_000, profile))
		budget := transactionPlanBudget{profile: profile}
		for range 1_000_000 {
			var err error
			targets, err = appendTransactionStatementBudgeted(
				targets, nil, shardCall{req: request},
				shardservice.MutationStatement{SQL: "UPDATE t SET n=1"}, &budget,
			)
			if err != nil {
				return targets, err
			}
		}
		return targets, nil
	}
	targets, err := plan()
	if !errors.Is(err, ErrTransactionByteLimit) || len(targets) != 1 ||
		len(targets[0].statements) > 5 {
		t.Fatalf("participants=%d retained=%d err=%v", len(targets), len(targets[0].statements), err)
	}
	result := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			_, _ = plan()
		}
	})
	if bytes := result.AllocedBytesPerOp(); bytes > 16<<10 {
		t.Fatalf("same-shard rejected plan allocated %d bytes/op", bytes)
	}
}

func TestTransactionPlanningBudgetMatchesCanonicalBytes(t *testing.T) {
	profile := DefaultProfiles()[ClassInteractive].withDefaults()
	statements := []shardservice.MutationStatement{
		{SQL: "INSERT INTO t (k, v) VALUES (?, ?)", Params: []shardservice.Param{
			shardservice.StringParam("k"), shardservice.BoolParam(true),
		}},
		{SQL: "DELETE FROM t WHERE k = ?", Params: []shardservice.Param{
			shardservice.NumberParam("42"),
		}},
	}
	budget := transactionPlanBudget{profile: profile}
	for i := range statements {
		if err := budget.admit(&statements[i], i == 0); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := shardservice.AppendMutationBatch(nil, statements)
	if err != nil {
		t.Fatal(err)
	}
	if budget.bytes != uint64(len(encoded)) || budget.mutations != uint64(len(statements)) {
		t.Fatalf("budget bytes/mutations=%d/%d canonical=%d/%d",
			budget.bytes, budget.mutations, len(encoded), len(statements))
	}
}

func testStageTransactionCoordinatorWide(t *testing.T, count int) {
	t.Helper()
	record := testCoordinatorRecord(count)
	stager := &recordingCoordinatorStager{t: t}
	format, err := stageTransactionCoordinator(record, stager)
	if err != nil {
		t.Fatal(err)
	}
	if format != transactionCoordinatorSegmented || stager.inline != nil {
		t.Fatalf("format=%d inline=%d", format, len(stager.inline))
	}
	if err := stager.reader.Seal(); err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	if stager.manifest.Manifest.TargetCount != uint64(count) ||
		stager.decodedTargets != uint64(count) || stager.segments == 0 {
		t.Fatalf("descriptor=%+v decoded=%d segments=%d", stager.manifest.Manifest, stager.decodedTargets, stager.segments)
	}
}
