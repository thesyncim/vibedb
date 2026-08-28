package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
)

// This qualification provisions six target WALs. Keep the default admitted
// live suffix (128 MiB), maximum Ready record (80 MiB), and snapshot-base
// reserve (2 MiB), plus nearly 14 MiB of operational headroom per target.
// Six default 256 MiB files alone exhaust the 1536 MiB gate before their
// family manifests are counted. This is fixture configuration, not a change
// to product defaults or the physical-allocation qualification threshold.
const restoreRF3TargetWALBytes = 224 << 20

func TestRestoredRF3TargetWALGeometry(t *testing.T) {
	minimum := int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes) +
		int64(raftstore.DefaultMaxLiveBytes) + int64(raftstore.DefaultMaxRecordBytes)
	if restoreRF3TargetWALBytes < minimum || restoreRF3TargetWALBytes%4096 != 0 {
		t.Fatalf("target WAL cannot hold unchanged admitted live suffix plus one maximum Ready and base: capacity=%d minimum=%d", restoreRF3TargetWALBytes, minimum)
	}
	if raftstore.DefaultMaxLiveBytes < raftstore.MinimumReadyLiveBytes || raftstore.DefaultMaxRecordBytes < raftstore.MinimumReadyRecordBytes {
		t.Fatal("fixture does not preserve every raftmodel Ready")
	}
	// Reserve a full MiB per family for metadata in this arithmetic preflight.
	// The Linux gate still measures every real allocated inode and applies its
	// unchanged absolute and growth limits; this is not substitute evidence.
	if 6*(int64(restoreRF3TargetWALBytes)+(1<<20)) >= 1536<<20 {
		t.Fatal("six target WALs leave no bounded metadata headroom")
	}
}
