package main

import (
	"math"

	"github.com/thesyncim/vibedb/internal/clusterbackupservice"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3MetricsProvider struct {
	owners *raftservice.ExecutionOwners
	groups []preparedRF3Group
	// schemas replaces the startup group slice when groups can be adopted,
	// retired, or advanced to a new schema generation while serving.
	schemas *rf3SchemaActivator
	backup  *clusterbackupservice.Service
	action  *replicaaction.Service
	data    []snapshottransfer.GroupDataService
	split   *splitcontroller.ControlService
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
	if provider.schemas != nil {
		provider.schemas.mu.RLock()
		states := make([]*rf3SchemaGeneration, 0, len(provider.schemas.groups))
		for _, state := range provider.schemas.groups {
			states = append(states, state)
		}
		provider.schemas.mu.RUnlock()
		for _, state := range states {
			if state != nil {
				state.mu.Lock()
				addRF3GroupStageMetrics(&result, state.apply, state.wal)
				state.mu.Unlock()
			}
		}
	} else {
		for index := range provider.groups {
			group := &provider.groups[index]
			addRF3GroupStageMetrics(&result, group.apply, group.recoveryLog())
		}
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

func addRF3GroupStageMetrics(result *servicemetrics.StageMetricsSnapshot, apply *sqldriver.ReplicatedApply, log rf3RecoveryLog) {
	if stats, err := apply.DurabilityStats(); err == nil {
		result.CheckpointApplied = rf3MetricsAdd(result.CheckpointApplied, stats.CheckpointAppliedIndex)
		result.Checkpoints = rf3MetricsAdd(result.Checkpoints, stats.Checkpoints)
		result.PhysicalCheckpoints = rf3MetricsAdd(result.PhysicalCheckpoints, stats.PhysicalCheckpoints)
		result.CheckpointBarrierSyncs = rf3MetricsAdd(result.CheckpointBarrierSyncs, stats.BarrierSyncs)
	}
	// Legacy group WALs expose exact counters. A node GroupView exposes only
	// conservative reservation bounds, which must not be reported as live
	// bytes or counted once per group as physical WAL synchronization work.
	if source, ok := log.(interface{ Metrics() raftstore.Metrics }); ok {
		wal := source.Metrics()
		result.WALLiveBytes = rf3MetricsAdd(result.WALLiveBytes, wal.LiveBytes)
		result.WALEntries = rf3MetricsAdd(result.WALEntries, wal.Entries)
		result.WALSyncs = rf3MetricsAdd(result.WALSyncs, wal.Syncs)
	}
}

func rf3MetricsAdd(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}
