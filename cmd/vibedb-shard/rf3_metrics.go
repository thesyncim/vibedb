package main

import (
	"math"

	"github.com/thesyncim/vibedb/internal/clusterbackupservice"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type rf3MetricsProvider struct {
	owners *raftservice.ExecutionOwners
	groups []preparedRF3Group
	backup *clusterbackupservice.Service
	action *replicaaction.Service
	data   []snapshottransfer.GroupDataService
	split  *splitcontroller.ControlService
}

type coldRF3MetricsProvider struct{ groups []*preparedColdRF3Group }

func (*coldRF3MetricsProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return raftservice.ProgressMetricsSnapshot{}
}
func (provider *coldRF3MetricsProvider) StageMetrics() servicemetrics.StageMetricsSnapshot {
	var result servicemetrics.StageMetricsSnapshot
	for _, group := range provider.groups {
		metrics := group.service.Metrics()
		result.BootstrapRequests = rf3MetricsAdd(result.BootstrapRequests, metrics.Requests)
		result.BootstrapChunks = rf3MetricsAdd(result.BootstrapChunks, metrics.Chunks)
		result.BootstrapBytes = rf3MetricsAdd(result.BootstrapBytes, metrics.Bytes)
		result.BootstrapCompletions = rf3MetricsAdd(result.BootstrapCompletions, metrics.Completions)
		result.BootstrapFaults = rf3MetricsAdd(result.BootstrapFaults, metrics.Faults)
		result.BootstrapResidentBytes = rf3MetricsAdd(result.BootstrapResidentBytes, metrics.ResidentBytes)
		result.BootstrapInflight = rf3MetricsAdd(result.BootstrapInflight, metrics.Inflight)
	}
	return result
}

func (provider *rf3MetricsProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.owners.ProgressMetrics()
}

func (provider *rf3MetricsProvider) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, raftservice.ProgressMetricsSnapshot, bool) {
	return provider.owners.GroupProgressMetrics(group)
}

func (provider *rf3MetricsProvider) StageMetrics() servicemetrics.StageMetricsSnapshot {
	var result servicemetrics.StageMetricsSnapshot
	for index := range provider.groups {
		group := &provider.groups[index]
		if stats, err := group.apply.DurabilityStats(); err == nil {
			result.CheckpointApplied = rf3MetricsAdd(result.CheckpointApplied, stats.CheckpointAppliedIndex)
			result.Checkpoints = rf3MetricsAdd(result.Checkpoints, stats.Checkpoints)
			result.PhysicalCheckpoints = rf3MetricsAdd(result.PhysicalCheckpoints, stats.PhysicalCheckpoints)
			result.CheckpointBarrierSyncs = rf3MetricsAdd(result.CheckpointBarrierSyncs, stats.BarrierSyncs)
		}
		wal := group.wal.Metrics()
		result.WALLiveBytes = rf3MetricsAdd(result.WALLiveBytes, wal.LiveBytes)
		result.WALEntries = rf3MetricsAdd(result.WALEntries, wal.Entries)
		result.WALSyncs = rf3MetricsAdd(result.WALSyncs, wal.Syncs)
	}
	backup := provider.backup.Metrics()
	result.BackupRequests, result.BackupFaults = backup.Requests, backup.Faults
	result.BackupLogicalBytes, result.BackupScanBytes = backup.LogicalArtifactBytes, backup.SnapshotScanBytes
	for _, item := range provider.data {
		stats := item.Service.Stats()
		result.SnapshotTransferChunks = rf3MetricsAdd(result.SnapshotTransferChunks, stats.Chunks)
		result.SnapshotTransferBytes = rf3MetricsAdd(result.SnapshotTransferBytes, stats.Bytes)
		if stats.ResidentBytes > 0 {
			result.SnapshotResidentBytes = rf3MetricsAdd(result.SnapshotResidentBytes, uint64(stats.ResidentBytes))
		}
	}
	action := provider.action.Metrics()
	result.ReplicaActionRequests, result.ReplicaActionCompletions, result.ReplicaActionFaults =
		action.Requests, action.Completions, action.Faults
	split := provider.split.Metrics()
	result.SplitControlRequests, result.SplitControlCompletions, result.SplitControlFaults =
		split.Requests, split.Completions, split.Faults
	return result
}

func rf3MetricsAdd(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}
