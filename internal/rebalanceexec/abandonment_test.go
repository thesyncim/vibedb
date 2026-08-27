package rebalanceexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

type abandonmentCatalog struct {
	records map[[32]byte]gateway.ReplicatedOperationRecord
}

func (catalog *abandonmentCatalog) ReadOperationIDs(context.Context) ([][32]byte, error) {
	ids := make([][32]byte, 0, len(catalog.records))
	for id := range catalog.records {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
	return ids, nil
}
func (catalog *abandonmentCatalog) ReadOperation(_ context.Context, id [32]byte) (gateway.ReplicatedOperationRecord, error) {
	record, ok := catalog.records[id]
	if !ok {
		return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
	}
	return record, nil
}
func (catalog *abandonmentCatalog) PublishOperation(_ context.Context, expected uint64, next gateway.ReplicatedOperationRecord) error {
	current, ok := catalog.records[next.ID]
	if !ok || current.Revision != expected {
		return gateway.ErrReplicatedCatalogConflict
	}
	catalog.records[next.ID] = next
	return nil
}
func (catalog *abandonmentCatalog) PublishReplicaMoveAbandonment(ctx context.Context, expected uint64,
	next gateway.ReplicatedOperationRecord) error {
	return catalog.PublishOperation(ctx, expected, next)
}
func (catalog *abandonmentCatalog) DeleteOperation(_ context.Context, id [32]byte, expected uint64) error {
	record, ok := catalog.records[id]
	if !ok {
		return nil
	}
	if record.Revision != expected || record.State < gateway.ReplicatedOperationComplete {
		return gateway.ErrReplicatedCatalogConflict
	}
	delete(catalog.records, id)
	return nil
}

type abandonmentSource struct {
	requests  []snapshottransfer.SourceControlRequest
	witnesses []snapshottransfer.ArtifactAbandonmentWitness
	err       error
}

func (source *abandonmentSource) AbandonReplicaMoveSnapshot(_ context.Context,
	request snapshottransfer.SourceControlRequest, witness snapshottransfer.ArtifactAbandonmentWitness) error {
	source.requests = append(source.requests, request)
	source.witnesses = append(source.witnesses, witness)
	return source.err
}

func abandonmentFixture() (gateway.ReplicatedOperationRecord, snapshottransfer.ArtifactAbandonmentWitness) {
	id := func(seed byte) (out [16]byte) {
		for i := range out {
			out[i] = seed + byte(i)
		}
		return
	}
	descriptor := snapshottransfer.Descriptor{Group: raftmember.GroupKey{
		ClusterID: id(1), ClusterIncarnation: id(2), TopologyRecoveryEpoch: 3,
		ShardIncarnation: id(4), GroupID: id(5)}, SourceMember: 1, TargetMember: 2,
		TargetStore: id(6), TargetIncarnation: 7, SchemaGeneration: 8, ReplicaSetVersion: 9,
		SnapshotIndex: 10, SnapshotTerm: 11, Lineage: sha256.Sum256([]byte("lineage")),
		ArtifactHash: sha256.Sum256([]byte("artifact")), ArtifactBytes: 64 << 10,
		ChunkBytes: snapshottransfer.MinChunkBytes}
	witness := snapshottransfer.ArtifactAbandonmentWitness{
		Operation: [32]byte{1}, Step: [32]byte{2}, Artifact: descriptor.ArtifactHash,
		TargetStore: descriptor.TargetStore, TargetIncarnation: descriptor.TargetIncarnation,
		SchemaGeneration: descriptor.SchemaGeneration, ReplicaSetVersion: descriptor.ReplicaSetVersion,
		Owner: rafttransport.NodeID{3}, Descriptor: descriptor}
	intent := []byte("{}")
	return gateway.ReplicatedOperationRecord{ID: witness.Operation, Kind: gateway.ReplicatedOperationMove,
		State: gateway.ReplicatedOperationRunning, Revision: 4, CatalogGeneration: 12,
		Proof: sha256.Sum256(intent), IntentDigest: sha256.Sum256(intent), Intent: intent}, witness
}

func TestCatalogAbandonmentAuthorityPublishesAndReadsExactRF3Witness(t *testing.T) {
	record, witness := abandonmentFixture()
	catalog := &abandonmentCatalog{records: map[[32]byte]gateway.ReplicatedOperationRecord{record.ID: record}}
	authority := CatalogAbandonmentAuthority{Journal: catalog}
	published, err := authority.Publish(t.Context(), record.Revision, witness)
	if err != nil {
		t.Fatal(err)
	}
	if published.AuthorityRevision != 5 || published.OwnerEpoch != 12 ||
		published.LeaseRevision != 4 || published.LeaseAppliedThrough != 4 ||
		published.AbandonedAppliedThrough != 5 {
		t.Fatalf("published=%+v", published)
	}
	read, found, err := authority.ReadArtifactAbandonment(t.Context(), record.ID)
	if err != nil || !found || read != published {
		t.Fatalf("read=%+v found=%t err=%v", read, found, err)
	}
	if _, err = authority.Publish(t.Context(), record.Revision, witness); !errors.Is(err, ErrSnapshotAbandonment) {
		t.Fatalf("stale publish=%v", err)
	}
}

func TestAbandonmentSchedulerRoutesOnlyReplicatedCancelledWitness(t *testing.T) {
	record, witness := abandonmentFixture()
	catalog := &abandonmentCatalog{records: map[[32]byte]gateway.ReplicatedOperationRecord{record.ID: record}}
	authority := CatalogAbandonmentAuthority{Journal: catalog}
	source := &abandonmentSource{}
	scheduler := &AbandonmentScheduler{Directory: catalog, Authority: authority, Source: source,
		MaxRecords: 1, MaxBytes: 1 << 20}
	pass, err := scheduler.RunPass(t.Context(), AbandonmentSchedulerCursor{})
	if err != nil || pass.Deleted != 0 || len(source.requests) != 0 {
		t.Fatalf("unwitnessed pass=%+v calls=%d err=%v", pass, len(source.requests), err)
	}
	published, err := authority.Publish(t.Context(), record.Revision, witness)
	if err != nil {
		t.Fatal(err)
	}
	pass, err = scheduler.RunPass(t.Context(), AbandonmentSchedulerCursor{})
	if err != nil || pass.Witnessed != 1 || pass.Deleted != 1 || len(source.requests) != 1 {
		t.Fatalf("witnessed pass=%+v calls=%d err=%v", pass, len(source.requests), err)
	}
	request := source.requests[0]
	if source.witnesses[0] != published || request.Operation != published.Operation ||
		request.Step != published.Step || request.SourceNode != published.Owner ||
		request.Group != published.Descriptor.Group || request.TargetStore != published.TargetStore {
		t.Fatalf("route=%+v witness=%+v", request, source.witnesses[0])
	}
}

func TestAbandonmentSchedulerCrashRestartAndByteGateDoNotSkipWitness(t *testing.T) {
	record, witness := abandonmentFixture()
	catalog := &abandonmentCatalog{records: map[[32]byte]gateway.ReplicatedOperationRecord{record.ID: record}}
	authority := CatalogAbandonmentAuthority{Journal: catalog}
	if _, err := authority.Publish(t.Context(), record.Revision, witness); err != nil {
		t.Fatal(err)
	}
	source := &abandonmentSource{}
	blocked := &AbandonmentScheduler{Directory: catalog, Authority: authority, Source: source,
		MaxRecords: 1, MaxBytes: uint64(snapshottransfer.DescriptorBytes) + witness.Descriptor.ArtifactBytes - 1}
	pass, err := blocked.RunPass(t.Context(), AbandonmentSchedulerCursor{})
	if err != nil || pass.Deleted != 0 || pass.Cursor != (AbandonmentSchedulerCursor{}) || len(source.requests) != 0 {
		t.Fatalf("byte gate pass=%+v calls=%d err=%v", pass, len(source.requests), err)
	}

	source.err = errors.New("response lost after durable source delete")
	restarted := &AbandonmentScheduler{Directory: catalog, Authority: authority, Source: source,
		MaxRecords: 1, MaxBytes: 1 << 20}
	pass, err = restarted.RunPass(t.Context(), AbandonmentSchedulerCursor{})
	if !errors.Is(err, source.err) || pass.Cursor != (AbandonmentSchedulerCursor{}) {
		t.Fatalf("unknown outcome advanced cursor: pass=%+v err=%v", pass, err)
	}
	source.err = nil
	// A process restart has no trusted local cursor. Reopening from zero replays
	// the exact RF3 witness and settles the source's idempotent journal/delete.
	restarted = &AbandonmentScheduler{Directory: catalog, Authority: authority, Source: source,
		MaxRecords: 1, MaxBytes: 1 << 20}
	pass, err = restarted.RunPass(t.Context(), AbandonmentSchedulerCursor{})
	if err != nil || pass.Deleted != 1 || pass.ScheduledBytes !=
		uint64(snapshottransfer.DescriptorBytes)+witness.Descriptor.ArtifactBytes {
		t.Fatalf("restart pass=%+v err=%v", pass, err)
	}
}
