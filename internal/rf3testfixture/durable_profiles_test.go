//go:build darwin || linux

package rf3testfixture

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Preparing a WAL is insufficient: the shared process profile must retain
// election records and a useful suffix while still reserving a maximum Ready.
// Exercise the real durable WAL without requiring Linux-only SQL sidecars.
func TestDurableGatewayWALRetainsReadyHeadroomAcrossRestart(t *testing.T) {
	options := DurableGatewayWALOptions()
	const suffixBytes = 8 << 20
	if options.MaxLiveBytes != raftstore.MinimumReadyLiveBytes+suffixBytes ||
		options.MaxFileBytes != int64(raftstore.HeaderBytes+raftstore.MaxSnapshotBaseRecordBytes+
			raftstore.MinimumReadyRecordBytes)+options.MaxLiveBytes ||
		options.MaxRecordBytes != raftstore.MinimumReadyRecordBytes ||
		options.MaxEntries != raftstore.MaxReadyEntries+(16<<10) {
		t.Fatal("fixture capacity no longer reserves exactly one Ready beyond its bounded live log")
	}
	identity := raftstore.Identity{Distribution: "catalog", Shard: "controlplane",
		AllocationGeneration: 1, MemberID: 1, ClusterID: [16]byte{1},
		ClusterIncarnation: [16]byte{2}, ShardIncarnation: [16]byte{3},
		GroupID: [16]byte{4}, StoreID: [16]byte{5}}
	key := raftstore.Key{ID: "fixture-headroom", Wrapped: []byte("wrapped"), Material: [32]byte{6}}
	path := filepath.Join(t.TempDir(), "raft.wal")
	store, err := raftstore.Create(path, identity, key, InitialBootstrap([]uint64{1, 2, 3}), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	index := uint64(1)
	// Four MiB of exact retained payload across 256 separately durable Ready
	// records exercises useful workload space, not just empty-entry overhead.
	payload := bytes.Repeat([]byte{0x5a}, 16<<10)
	for boot := uint64(1); boot <= 2; boot++ {
		incarnation, err := store.BeginIncarnation()
		if err != nil || incarnation != boot {
			t.Fatalf("boot %d incarnation=%d: %v", boot, incarnation, err)
		}
		term, vote := boot+1, uint64(1)
		if err := store.ReserveReady(); err != nil {
			t.Fatalf("boot %d election headroom: %v", boot, err)
		}
		if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1,
			HardState: &pb.HardState{Term: &term, Vote: &vote, Commit: &index}, MustSync: true}); err != nil {
			t.Fatal(err)
		}
		for ready := uint64(2); ready <= 129; ready++ {
			if err := store.ReserveReady(); err != nil {
				t.Fatalf("boot %d ready %d headroom: %v", boot, ready, err)
			}
			index++
			binary.LittleEndian.PutUint64(payload[:8], index)
			entryType := pb.EntryNormal
			if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: ready,
				HardState: &pb.HardState{Term: &term, Vote: &vote, Commit: &index},
				Entries:   []*pb.Entry{{Index: &index, Term: &term, Type: &entryType, Data: payload}},
				MustSync:  true}); err != nil {
				t.Fatalf("boot %d ready %d persist: %v", boot, ready, err)
			}
		}
		if err := store.ReserveReady(); err != nil {
			t.Fatalf("boot %d final headroom: %v", boot, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = raftstore.Open(path, identity, 3, key, options)
		if err != nil {
			t.Fatal(err)
		}
		hard, _, err := store.InitialState()
		if err != nil || hard.GetCommit() != index || hard.GetTerm() != term {
			t.Fatalf("boot %d durable recovery: %v, %v", boot, hard, err)
		}
		entries, err := store.Entries(2, index+1, ^uint64(0))
		if err != nil || uint64(len(entries)) != index-1 {
			t.Fatalf("boot %d retained entries=%d: %v", boot, len(entries), err)
		}
		for offset, entry := range entries {
			binary.LittleEndian.PutUint64(payload[:8], uint64(offset)+2)
			if entry.GetIndex() != uint64(offset)+2 || entry.GetTerm() != uint64(offset/128)+2 ||
				entry.GetType() != pb.EntryNormal || !bytes.Equal(entry.Data, payload) {
				t.Fatalf("boot %d retained entry %d changed: %v", boot, offset, entry)
			}
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() != options.MaxFileBytes {
			t.Fatalf("boot %d changed fixed WAL allocation: %v", boot, err)
		}
	}
	const processWALs = 3 * DurableGatewayGroups
	reserved := int64(processWALs) * options.MaxFileBytes
	if reserved >= 2<<30 {
		t.Fatalf("fixed WAL reservation alone exceeds process data budget: %d", reserved)
	}
	t.Logf("physical profile: WALs=%d reservation=%d other_data_margin=%d retained_test_payload=%d",
		processWALs, reserved, (2<<30)-reserved, 256*len(payload))
}
