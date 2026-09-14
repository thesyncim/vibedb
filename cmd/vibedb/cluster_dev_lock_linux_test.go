package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"golang.org/x/sys/unix"
)

func TestDevColdValidationWaitsForRetainedKernelCollectionLock(t *testing.T) {
	root := t.TempDir()
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	member := devClusterMember{Member: 1, Node: "00000000000000000000000000000005",
		Store: "00000000000000000000000000000006", ServeManifest: filepath.Join(root, "serve-rf3.vibejson")}
	prepareDevTestReplica(t, member, group, devDataDistribution, devDataShard, devDataTable, devDataPrimaryKey, replication.Digest{})
	catalogPath := filepath.Join(root, "member.vdb")
	before, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	image, err := sqldriver.ValidateReplicatedSchemaCatalogImage(before)
	if err != nil {
		t.Fatal(err)
	}
	collections, err := filepath.Glob(catalogPath + ".tables/*.vjc")
	if err != nil || len(collections) == 0 {
		t.Fatalf("prepared collections=%v err=%v", collections, err)
	}
	owner, err := os.OpenFile(collections[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	// Bypass the process registry to model a kernel reference retained after
	// the crashed process has been reaped, as with registered io_uring files.
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(owner.Fd()), unix.LOCK_UN)
	if _, _, err := readDevReplicatedTableProfile(member, devDataDistribution, devDataShard,
		devDataTable, devDataPrimaryKey, group, replication.Digest{}, image); !errors.Is(err, storeio.ErrWriterLocked) {
		t.Fatalf("ordinary open bypassed nonblocking writer admission: %v", err)
	}
	type result struct {
		route devPreparedRoute
		err   error
	}
	done := make(chan result, 1)
	go func() {
		route, err := inspectDevPreparedRoute(nil, "data", devDataDistribution, devDataShard,
			devDataTable, devDataPrimaryKey, group, replication.Digest{}, true, []devClusterMember{member})
		done <- result{route, err}
	}()
	select {
	case got := <-done:
		t.Fatalf("cold validation returned before kernel lock release: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.route.digest != image.RelationManifestDigest || got.route.table.Table != devDataTable {
			t.Fatalf("cold validation after kernel release: route=%+v err=%v", got.route, got.err)
		}
	case <-time.After(devStartupWriterLockWait + time.Second):
		t.Fatal("cold validation exceeded bounded lock admission")
	}
	after, err := os.ReadFile(catalogPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("cold schema validation changed retained catalog: %v", err)
	}
}
