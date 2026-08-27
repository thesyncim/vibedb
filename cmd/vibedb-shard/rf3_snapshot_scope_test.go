package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

func TestRF3SnapshotGroupPathIsExactStableAndRooted(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	if got := rf3SnapshotGroupPath(root, group, false); got != root {
		t.Fatalf("singleton path=%q want=%q", got, root)
	}
	first := rf3SnapshotGroupPath(root, group, true)
	if filepath.Dir(first) != root || len(filepath.Base(first)) != 32 || strings.Contains(filepath.Base(first), "..") {
		t.Fatalf("unsafe scoped path=%q", first)
	}
	fields := []func(*raftmember.GroupKey){
		func(g *raftmember.GroupKey) { g.ClusterID[0]++ },
		func(g *raftmember.GroupKey) { g.ClusterIncarnation[0]++ },
		func(g *raftmember.GroupKey) { g.TopologyRecoveryEpoch++ },
		func(g *raftmember.GroupKey) { g.ShardIncarnation[0]++ },
		func(g *raftmember.GroupKey) { g.GroupID[0]++ },
	}
	for index, mutate := range fields {
		changed := group
		mutate(&changed)
		if got := rf3SnapshotGroupPath(root, changed, true); got == first {
			t.Fatalf("field %d did not scope path", index)
		}
	}
}

type unopenedRF3SnapshotCut struct{}

func (unopenedRF3SnapshotCut) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	return nil, errors.New("namespace preparation must not capture an artifact")
}

func TestRF3SnapshotRepositoryNamespacesOpenIndependentProviders(t *testing.T) {
	dataRoot := t.TempDir()
	repositoryRoot := filepath.Join(dataRoot, "source-artifacts")
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	for ordinal := byte(0); ordinal < 2; ordinal++ {
		group.GroupID[0] = 5 + ordinal
		path, err := prepareRF3SnapshotRepository(dataRoot, repositoryRoot, group, true)
		if err != nil || path != rf3SnapshotGroupPath(repositoryRoot, group, true) {
			t.Fatalf("group %d path=%q err=%v", ordinal, path, err)
		}
		if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
			t.Fatalf("namespace preparation created repository state: entries=%d err=%v", len(entries), err)
		}
		for _, directory := range []string{repositoryRoot, path} {
			info, err := os.Lstat(directory)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Fatalf("namespace mode %q: %v %v", directory, info, err)
			}
		}
		if resumed, err := prepareRF3SnapshotRepository(dataRoot, repositoryRoot, group, true); err != nil || resumed != path {
			t.Fatalf("namespace restart path=%q err=%v", resumed, err)
		}
		provider, err := snapshottransfer.OpenRetainedSourceExportProvider(snapshottransfer.RetainedSourceExportOptions{
			DataRoot: dataRoot, RepositoryPath: path,
			Limits:     snapshottransfer.Limits{MaxArtifacts: 2, MaxArtifactBytes: 4096, MaxDiskBytes: 1 << 20},
			ChunkBytes: snapshottransfer.MinChunkBytes, MaxConcurrent: 1,
			RuntimeIdentity: raftmember.RuntimeIdentity{Group: group, Distribution: "data", Shard: "all",
				AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{6}, NodeIncarnation: 1, RelationManifestDigest: [32]byte{7}},
			SourceNode: rafttransport.NodeID{8}, TargetMember: 4, TargetStore: [16]byte{9}, TargetIncarnation: 1,
			Cut: unopenedRF3SnapshotCut{},
		})
		if err != nil {
			t.Fatalf("group %d actual retained provider: %v", ordinal, err)
		}
		t.Cleanup(func() {
			if err := provider.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestRF3SnapshotNamespaceRejectsUnsafePathsWithoutCreatingRepository(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	for _, kind := range []string{"outside", "root", "relative", "missing-ancestor", "root-symlink", "namespace-symlink", "child-symlink", "public-namespace", "public-child", "file"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			repository := filepath.Join(root, "artifacts")
			outside := t.TempDir()
			var err error
			switch kind {
			case "outside":
				repository = filepath.Join(outside, "artifacts")
			case "root":
				repository = root
			case "relative":
				repository = "artifacts"
			case "missing-ancestor":
				repository = filepath.Join(root, "missing", "artifacts")
			case "root-symlink":
				link := filepath.Join(root, "root-link")
				err = os.Symlink(outside, link)
				root, repository = link, filepath.Join(link, "artifacts")
			case "namespace-symlink":
				err = os.Symlink(outside, repository)
			case "public-namespace":
				err = os.Mkdir(repository, 0o755)
			case "file":
				err = os.WriteFile(repository, []byte("not a directory"), 0o600)
			case "child-symlink", "public-child":
				err = os.Mkdir(repository, 0o700)
				if err == nil {
					child := rf3SnapshotGroupPath(repository, group, true)
					if kind == "child-symlink" {
						err = os.Symlink(outside, child)
					} else {
						err = os.Mkdir(child, 0o755)
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prepareRF3SnapshotRepository(root, repository, group, true); err == nil {
				t.Fatal("unsafe namespace accepted")
			}
			if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
				t.Fatal("namespace preparation modified outside directory")
			}
		})
	}
	root := t.TempDir()
	repository := filepath.Join(root, "singleton")
	if path, err := prepareRF3SnapshotRepository(root, repository, group, false); err != nil || path != repository {
		t.Fatalf("singleton layout changed: %q %v", path, err)
	}
	if _, err := os.Stat(repository); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("singleton acquired extra namespace side effects")
	}
}
