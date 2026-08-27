package rf3testfixture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPrepareSplitRuntimeRetainsExactStaticCut(t *testing.T) {
	root := t.TempDir()
	bootstrap := InitialBootstrap([]uint64{1, 2, 3})
	if err := PrepareSplitRuntime(root, bootstrap); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "split-children", "static-bootstrap.pb")
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.Snapshot
	if err := proto.Unmarshal(first, &decoded); err != nil ||
		decoded.GetMetadata().GetIndex() != 1 || decoded.GetMetadata().GetTerm() != 1 ||
		string(decoded.GetData()) != "vibedb-rf3-split-child-bootstrap" ||
		!proto.Equal(decoded.GetMetadata().GetConfState(), bootstrap.Snapshot.GetMetadata().GetConfState()) {
		t.Fatalf("retained bootstrap differs: %v", err)
	}
	if err := PrepareSplitRuntime(root, bootstrap); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("repeated fixture preparation changed the static cut: %v", err)
	}
	for _, name := range []string{"split-runtime", "split-children"} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("private runtime directory %q: %v", name, err)
		}
	}
	if err := PrepareSplitRuntime(root, raftstore.Bootstrap{}); err == nil {
		t.Fatal("missing bootstrap accepted")
	}
}
