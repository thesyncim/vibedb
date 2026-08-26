package snapshottransfer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type retainedTestCut struct {
	cut *replicatedstate.ReadSnapshot
	err error
}

func (source *retainedTestCut) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	return source.cut, source.err
}

func TestRetainedSourceProviderExportsAndObservesAfterReopen(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	path := filepath.Join(root, "source-artifacts")
	node := rafttransport.NodeID{31}
	options := retainedSourceOptions(fixture, root, path, node, &retainedTestCut{cut: cut})
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	request := retainedSourceRequest(fixture, node)
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || !descriptorMatchesSourceRequest(descriptor, request) {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	if err = provider.Close(); err != nil {
		t.Fatal(err)
	}

	// The request journal may still say Running after a lost terminal publish.
	// A new process reopens repository metadata and finds the exact artifact
	// without reacquiring a state-machine cut or rescanning relation data.
	options.Cut = &retainedTestCut{err: errors.New("must not pin")}
	reopened, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	observed, found, err := reopened.ObserveSourceExport(context.Background(), request)
	if err != nil || !found || observed != descriptor {
		t.Fatalf("observed=%+v found=%t err=%v", observed, found, err)
	}
}

func TestRetainedSourceProviderRejectsWrongIdentityAndStaleMembership(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{41}
	provider, err := OpenRetainedSourceExportProvider(retainedSourceOptions(
		fixture, root, filepath.Join(root, "artifacts"), node, &retainedTestCut{cut: cut},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	wrong := request
	wrong.SourceNode[0]++
	if _, err = provider.PinSourceExport(context.Background(), wrong); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("wrong source node err=%v", err)
	}
	request.ReplicaSetVersion++
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale replica-set version err=%v", err)
	}
	// A stale cut must return its sole workspace instead of wedging later work.
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("workspace was not returned after stale cut: %v", err)
	}
}

func TestRetainedSourceProviderRepositoryPathCannotEscapeOrTraverseSymlink(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{51}
	options := retainedSourceOptions(
		fixture, root, filepath.Join(filepath.Dir(root), "escape"), node,
		&retainedTestCut{cut: cut},
	)
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("escaped repository err=%v", err)
	}
	outside := t.TempDir()
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	options.RepositoryPath = filepath.Join(link, "artifacts")
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("symlink repository err=%v", err)
	}
}

func retainedSourceOptions(
	fixture SourceExportPlan,
	root, path string,
	node rafttransport.NodeID,
	cut SourceArtifactCut,
) RetainedSourceExportOptions {
	return RetainedSourceExportOptions{
		DataRoot: root, RepositoryPath: path, Limits: sourceExportLimits(),
		ChunkBytes: MinChunkBytes, MaxConcurrent: 1,
		RuntimeIdentity: raftmember.RuntimeIdentity{
			Group: fixture.Group, Distribution: "export", Shard: "all",
			AllocationGeneration: 4, MemberID: fixture.SourceMember,
			StoreID: [16]byte{61}, NodeIncarnation: 62,
			RelationManifestDigest: fixture.ExpectedFence.RelationManifestDigest,
		},
		SourceNode: node, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		Cut: cut,
	}
}

func retainedSourceRequest(
	fixture SourceExportPlan,
	node rafttransport.NodeID,
) SourceControlRequest {
	return SourceControlRequest{
		Operation: [32]byte{1}, Step: [32]byte{2}, Group: fixture.Group,
		SourceMember: fixture.SourceMember, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		ReplicaSetVersion: fixture.ExpectedFence.ReplicaSetVersion, SourceNode: node,
	}
}
