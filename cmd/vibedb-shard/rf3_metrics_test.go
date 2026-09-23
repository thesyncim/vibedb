package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	pb "go.etcd.io/raft/v3/raftpb"
)

type rf3MetricsTestLog struct{ metrics raftstore.Metrics }

func (log *rf3MetricsTestLog) Entries(uint64, uint64, uint64) ([]*pb.Entry, error) {
	return nil, nil
}
func (log *rf3MetricsTestLog) LastIndex() (uint64, error)      { return 0, nil }
func (log *rf3MetricsTestLog) Snapshot() (*pb.Snapshot, error) { return nil, nil }
func (log *rf3MetricsTestLog) Metrics() raftstore.Metrics      { return log.metrics }

func TestRF3MetricsFollowsDynamicSchemaInventory(t *testing.T) {
	first := raftmember.GroupKey{GroupID: [16]byte{1}}
	second := raftmember.GroupKey{GroupID: [16]byte{2}}
	schemas := &rf3SchemaActivator{groups: make(map[raftmember.GroupKey]*rf3SchemaGeneration)}
	provider := &rf3MetricsProvider{schemas: schemas}
	check := func(want raftstore.Metrics) {
		t.Helper()
		got := provider.StageMetrics()
		if got.WALLiveBytes != want.LiveBytes || got.WALEntries != want.Entries || got.WALSyncs != want.Syncs {
			t.Fatalf("live metrics = %+v, want WAL %+v", got, want)
		}
	}
	check(raftstore.Metrics{})
	state := &rf3SchemaGeneration{wal: &rf3MetricsTestLog{raftstore.Metrics{LiveBytes: 10, Entries: 2, Syncs: 3}}}
	schemas.mu.Lock()
	schemas.groups[first] = state
	schemas.mu.Unlock()
	check(raftstore.Metrics{LiveBytes: 10, Entries: 2, Syncs: 3})

	// Schema replacement updates the shared generation object rather than
	// rebuilding the metrics provider or its startup inventory.
	state.mu.Lock()
	state.wal = &rf3MetricsTestLog{raftstore.Metrics{LiveBytes: 20, Entries: 4, Syncs: 6}}
	state.mu.Unlock()
	check(raftstore.Metrics{LiveBytes: 20, Entries: 4, Syncs: 6})
	schemas.mu.Lock()
	schemas.groups[second] = &rf3SchemaGeneration{wal: &rf3MetricsTestLog{raftstore.Metrics{LiveBytes: 5, Entries: 1, Syncs: 2}}}
	schemas.mu.Unlock()
	check(raftstore.Metrics{LiveBytes: 25, Entries: 5, Syncs: 8})
	schemas.mu.Lock()
	delete(schemas.groups, first)
	schemas.mu.Unlock()
	check(raftstore.Metrics{LiveBytes: 5, Entries: 1, Syncs: 2})
	schemas.mu.Lock()
	delete(schemas.groups, second)
	schemas.mu.Unlock()
	if got := provider.StageMetrics(); got != (servicemetrics.StageMetricsSnapshot{}) {
		t.Fatalf("retired inventory retained metrics: %+v", got)
	}
}
