package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
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
