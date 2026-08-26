package snapshottransfer

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestExportPinnedSnapshotPublishesExactCertifiedArtifact(t *testing.T) {
	cut, plan := sourceExportFixture(t, sourceExportLimits())
	defer cut.Close()

	descriptor, manifest, err := ExportPinnedSnapshot(plan)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.SnapshotIndex != cut.Fence().Applied ||
		descriptor.SnapshotTerm != cut.Fence().LastTerm ||
		descriptor.Lineage != cut.Fence().LastEntryDigest ||
		descriptor.ArtifactBytes != manifest.EncodedBytes {
		t.Fatalf("descriptor=%+v manifest=%+v fence=%+v", descriptor, manifest, cut.Fence())
	}
	opened, err := plan.Repository.Manifest(descriptor)
	if err != nil || opened.Digest != manifest.Digest ||
		opened.ImageDigest != manifest.ImageDigest || opened.EncodedBytes != manifest.EncodedBytes {
		t.Fatalf("opened=%+v manifest=%+v err=%v", opened, manifest, err)
	}
	if stats := plan.Repository.Stats(); stats.Published != 1 || stats.Staged != 0 {
		t.Fatalf("repository stats=%+v", stats)
	}
}

func TestExportPinnedSnapshotResumesExactRepositoryPrefix(t *testing.T) {
	cut, sourcePlan := sourceExportFixture(t, sourceExportLimits())
	defer cut.Close()
	descriptor, manifest, err := ExportPinnedSnapshot(sourcePlan)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := sourcePlan.Repository.OpenPublished(descriptor, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(artifact)
	err = errors.Join(readErr, artifact.Close())
	if err != nil || uint64(len(payload)) != descriptor.ArtifactBytes {
		t.Fatalf("read artifact bytes=%d err=%v", len(payload), err)
	}

	target, err := OpenRepository(filepath.Join(t.TempDir(), "resume"), sourceExportLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	prefix := len(payload) / 2
	if prefix == 0 {
		t.Fatal("empty artifact")
	}
	if _, complete, appendErr := target.Append(
		descriptor, 0, payload[:prefix], sha256Bytes(payload[:prefix]),
	); appendErr != nil || complete {
		t.Fatalf("stage prefix complete=%t err=%v", complete, appendErr)
	}
	sourcePlan.Repository = target
	retriedDescriptor, retriedManifest, err := ExportPinnedSnapshot(sourcePlan)
	if err != nil || retriedDescriptor != descriptor ||
		retriedManifest.Digest != manifest.Digest {
		t.Fatalf("retry descriptor=%+v manifest=%+v err=%v", retriedDescriptor, retriedManifest, err)
	}
	if offset, complete, offsetErr := target.Offset(descriptor); offsetErr != nil ||
		!complete || offset != descriptor.ArtifactBytes {
		t.Fatalf("offset=%d complete=%t err=%v", offset, complete, offsetErr)
	}
}

func TestExportPinnedSnapshotSettlesPublishOutcomeUnknown(t *testing.T) {
	cut, plan := sourceExportFixture(t, sourceExportLimits())
	defer cut.Close()
	fault := errors.New("publish boundary")
	fired := false
	plan.Repository.fault = func(point repositoryFault) error {
		if point == faultAfterPublishRename && !fired {
			fired = true
			return fault
		}
		return nil
	}
	descriptor, _, err := ExportPinnedSnapshot(plan)
	if err != nil || !fired {
		t.Fatalf("descriptor=%+v fired=%t err=%v", descriptor, fired, err)
	}
	if _, complete, offsetErr := plan.Repository.Offset(descriptor); offsetErr != nil || !complete {
		t.Fatalf("complete=%t err=%v", complete, offsetErr)
	}
}

func TestExportPinnedSnapshotRejectsStaleCutAndRepositoryBound(t *testing.T) {
	cut, plan := sourceExportFixture(t, sourceExportLimits())
	defer cut.Close()
	plan.ExpectedFence.Applied++
	if _, _, err := ExportPinnedSnapshot(plan); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale fence err=%v", err)
	}
	plan.ExpectedFence = cut.Fence()
	bounded, err := OpenRepository(filepath.Join(t.TempDir(), "bounded"), Limits{
		MaxArtifacts: 1, MaxArtifactBytes: 512, MaxDiskBytes: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bounded.Close()
	plan.Repository = bounded
	if _, _, err = ExportPinnedSnapshot(plan); !errors.Is(err, ErrDescriptor) {
		t.Fatalf("artifact bound err=%v", err)
	}
	if stats := bounded.Stats(); stats.Artifacts != 0 || stats.DiskBytes != 0 {
		t.Fatalf("bounded repository retained state=%+v", stats)
	}
}

func sourceExportFixture(t testing.TB, limits Limits) (
	*replicatedstate.ReadSnapshot,
	SourceExportPlan,
) {
	t.Helper()
	root := t.TempDir()
	open := func(name string, options durable.Options) *durable.Collection {
		file, err := os.OpenFile(filepath.Join(root, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return collection
	}
	systemCollection := open("system.vdb", durable.Options{OpaqueValues: true})
	userCollection := open("user.vdb", durable.Options{})
	system := replicatedstate.CollectionTarget{
		Collection: systemCollection, Validation: replicatedstate.ValidationOpaqueBinary,
		Limits: replicatedstate.CollectionLimits{
			MaxKeyBytes: systemCollection.MaxKeyBytes(), MaxDocumentBytes: systemCollection.MaxDocumentBytes(),
			MaxDistinctMutations: systemCollection.MaxBatchDocuments(), MaxBatchBytes: systemCollection.MaxBatchBytes(),
		},
	}
	validation := sha256Bytes([]byte("source-export"))
	user := replicatedstate.CollectionTarget{
		Collection: userCollection, Validation: replicatedstate.ValidationDeterministicMutation,
		ValidationDigest: validation, Validator: snapshotAcceptAll{},
		Limits: replicatedstate.CollectionLimits{
			MaxKeyBytes: userCollection.MaxKeyBytes(), MaxDocumentBytes: userCollection.MaxDocumentBytes(),
			MaxDistinctMutations: userCollection.MaxBatchDocuments(), MaxBatchBytes: userCollection.MaxBatchBytes(),
		},
	}
	log, err := durable.NewTxnLog(root, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	id := func(seed byte) (out replication.ID128) {
		for index := range out {
			out[index] = seed + byte(index)
		}
		return out
	}
	binding := replicatedstate.Binding{
		ClusterID: id(1), ClusterIncarnation: id(2), TopologyRecoveryEpoch: 3,
		Distribution: "export", Shard: "all", AllocationGeneration: 4,
		ShardIncarnation: id(5), GroupID: id(6), ActivePolicyGeneration: 7,
		ProtectionEpoch: 8, OwnershipEpoch: 9, SchemaGeneration: 10,
		RoutingVersion: 11, RouteGeneration: 12,
	}
	index, term := uint64(1), uint64(1)
	conf := &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	bootstrap := &pb.Snapshot{Data: []byte("source-export-bootstrap"), Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: conf,
	}}
	machine, err := replicatedstate.Open(
		binding, bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user}, log,
		replicatedstate.Options{
			TxnLimits:   durable.TxnLimits{MaxCollections: 2, MaxDocuments: user.Limits.MaxDistinctMutations + 4, MaxBytes: 64 << 20},
			MaxSessions: 8, RetryWindow: 4,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = machine.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	cut, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	repository, err := OpenRepository(filepath.Join(t.TempDir(), "artifacts"), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	group := raftmember.GroupKey{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
	}
	return cut, SourceExportPlan{
		Repository: repository, Snapshot: cut, ExpectedFence: cut.Fence(), Group: group,
		SourceMember: 1, TargetMember: 2, TargetStore: id(20), TargetIncarnation: 21,
		ChunkBytes:        MinChunkBytes,
		ArtifactWorkspace: make([]byte, 0, MinChunkBytes),
		TransferWorkspace: make([]byte, 0, MinChunkBytes),
	}
}

func sourceExportLimits() Limits {
	return Limits{MaxArtifacts: 2, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20}
}

func sha256Bytes(src []byte) (digest [32]byte) {
	return sha256.Sum256(src)
}
