package gateway

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

type recordingCoordinatorStager struct {
	t                   testing.TB
	inline              []byte
	manifest            distributedtxn.ManifestCoordinatorRecord
	reader              *distributedtxn.ManifestReader
	participants        []distributedtxn.ParticipantRef
	identities          []byte
	segments            uint32
	decodedParticipants uint64
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
	s.participants = make([]distributedtxn.ParticipantRef, distributedtxn.MaxManifestPageParticipants)
	s.identities = make([]byte, distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
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
	page, err := s.reader.OpenNext(record, s.participants, s.identities)
	if err != nil {
		s.t.Fatalf("open segment %d: %v", index, err)
	}
	s.decodedParticipants += uint64(len(page.Participants))
	s.segments++
	return nil
}

func testTransactionRefs(count int) []distributedtxn.ParticipantRef {
	refs := make([]distributedtxn.ParticipantRef, count)
	for i := range refs {
		refs[i] = distributedtxn.ParticipantRef{
			Distribution:   []byte("data"),
			Shard:          []byte(fmt.Sprintf("s%08d", i)),
			RoutingVersion: 7, AllocationGeneration: uint64(i + 1),
			OwnershipEpoch: uint64(i + 11), MutationDigest: distributedtxn.Digest{1},
			State: distributedtxn.ParticipantStaged,
		}
	}
	return refs
}

func testCoordinatorRecord(count int) distributedtxn.CoordinatorRecord {
	var id distributedtxn.ID
	id[0] = 9
	return distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 7, RecoveryDeadline: 12345,
		Participants: testTransactionRefs(count),
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

func TestStageTransactionCoordinatorStreams65Participants(t *testing.T) {
	testStageTransactionCoordinatorWide(t, 65)
}

func TestStageTransactionCoordinatorStreams4097Participants(t *testing.T) {
	testStageTransactionCoordinatorWide(t, 4097)
}

func TestIndexedTransactionGroupingAdmits4097ExactTargets(t *testing.T) {
	participants := make([]transactionParticipant, 0, 4097)
	byTarget := make(map[transactionTargetKey]int, 4097)
	for i := range 4097 {
		request := &shardservice.ShardRequest{
			Distribution: "data", Shard: distribution.ShardID(fmt.Sprintf("s%08d", i)),
			RoutingVersion: 7, AllocationGeneration: distribution.ShardAllocationGeneration(i + 1),
			OwnershipEpoch: distribution.OwnershipEpoch(i + 11),
		}
		var err error
		participants, err = appendTransactionStatementIndexed(
			participants, byTarget, shardCall{req: request}, shardservice.MutationStatement{SQL: "x"},
		)
		if err != nil {
			t.Fatalf("append target %d: %v", i, err)
		}
	}
	if len(participants) != 4097 || len(byTarget) != 4097 {
		t.Fatalf("participants=%d index=%d", len(participants), len(byTarget))
	}
	participants, err := appendTransactionStatementIndexed(
		participants, byTarget, participants[4096].call, shardservice.MutationStatement{SQL: "y"},
	)
	if err != nil || len(participants) != 4097 || len(participants[4096].statements) != 2 {
		t.Fatalf("exact duplicate grouping participants=%d statements=%d err=%v",
			len(participants), len(participants[4096].statements), err)
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
	if stager.manifest.Manifest.ParticipantCount != uint64(count) ||
		stager.decodedParticipants != uint64(count) || stager.segments == 0 {
		t.Fatalf("descriptor=%+v decoded=%d segments=%d", stager.manifest.Manifest, stager.decodedParticipants, stager.segments)
	}
}
