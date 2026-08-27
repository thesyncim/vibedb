package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	vibejson "github.com/thesyncim/vibejson"
)

type gatewayDistributedMetricsContextKey struct{}

func withGatewayDistributedMetrics(ctx context.Context, metrics *gateway.DistributedMetrics) context.Context {
	if ctx == nil || metrics == nil {
		return ctx
	}
	return context.WithValue(ctx, gatewayDistributedMetricsContextKey{}, metrics)
}

func gatewayDistributedMetricsFromContext(ctx context.Context) *gateway.DistributedMetrics {
	metrics, _ := ctx.Value(gatewayDistributedMetricsContextKey{}).(*gateway.DistributedMetrics)
	return metrics
}

func newGatewayDistributedMetrics(snapshot *gateway.Snapshot, opener *gatewayShardControlOpener) (*gateway.DistributedMetrics, error) {
	if snapshot == nil || opener == nil || snapshot.ReplicatedRouteCount() <= 0 ||
		snapshot.ReplicatedRouteCount() > gateway.AbsoluteMaxDistributedMetricSamples {
		return nil, gateway.ErrDistributedMetrics
	}
	routes := make([]gateway.ReplicatedRoute, snapshot.ReplicatedRouteCount())
	for index := range routes {
		workspace := make([]gateway.ReplicatedEndpoint, gateway.ServingReplicaCount)
		route, ok := snapshot.ReplicatedRouteAt(index, workspace)
		if !ok {
			return nil, gateway.ErrDistributedMetrics
		}
		route.Replicas = append([]gateway.ReplicatedEndpoint(nil), route.Replicas...)
		routes[index] = route
	}
	return gateway.NewDistributedMetrics(opener, routes)
}

// validGatewayMetricsRequest keeps the operator endpoint deliberately narrow:
// no SQL, routing hint, identity, or result budget is interpreted beside the
// operation. Authentication and CapabilityTopology are enforced by the caller.
func validGatewayMetricsRequest(request serveRequest) bool {
	return request.Op == "metrics" && request.RequestID == "" && request.InstallationID == "" &&
		request.IssuerEpoch == 0 && request.LaneOrdinal == 0 && request.GrantDigest == "" &&
		request.IssuerSequence == 0 && request.IssuerLane == "" &&
		request.IssuerAuthenticator == "" && !request.hasSQL() && request.Class == "" &&
		request.MaxResultBytes == 0 && len(request.Params) == 0 && len(request.Statements) == 0
}

// writeGatewayMetrics emits a stable, allocation-free field walk over the
// lock-free counter snapshot. Counters are cumulative for this process
// incarnation; consumers derive rates outside the serving path.
func writeGatewayMetrics(writer *vibejson.Writer, metrics gateway.MetricsSnapshot) error {
	if err := writer.Key("metrics"); err != nil {
		return err
	}
	if err := writer.BeginObject(); err != nil {
		return err
	}
	fields := [...]struct {
		name  string
		value uint64
	}{
		{"route_single", metrics.RouteSingle},
		{"route_targeted", metrics.RouteTargeted},
		{"route_scatter", metrics.RouteScatter},
		{"route_empty", metrics.RouteEmpty},
		{"shards_fanned", metrics.ShardsFanned},
		{"rows_returned", metrics.RowsReturned},
		{"bytes_returned", metrics.BytesReturned},
		{"retries", metrics.Retries},
		{"scatter_all_shards", metrics.ScatterAllShards},
		{"scatter_unknown_route", metrics.ScatterUnknownRoute},
	}
	for _, field := range fields {
		if err := writer.Key(field.name); err != nil {
			return err
		}
		if err := writer.Uint(field.value); err != nil {
			return err
		}
	}
	return writer.EndObject()
}

func writeGatewayDistributedMetrics(writer *vibejson.Writer, metrics *gateway.DistributedMetrics) error {
	if metrics == nil {
		return nil
	}
	aggregate, err := metrics.Aggregate()
	if err != nil {
		return errors.Join(gateway.ErrDistributedMetrics, err)
	}
	if err = writer.Key("distributed_metrics"); err != nil {
		return err
	}
	if err = writer.BeginObject(); err != nil {
		return err
	}
	if err = writeGatewayMetricCut(writer, aggregate.Cut); err != nil {
		return err
	}
	if err = writeGatewayStageMetrics(writer, aggregate.Stages); err != nil {
		return err
	}
	for _, field := range [...]struct {
		name  string
		value uint64
	}{{"samples", aggregate.Samples}, {"collection_reads", aggregate.Reads}, {"collection_faults", aggregate.Faults}} {
		if err = writer.Key(field.name); err != nil {
			return err
		}
		if err = writer.Uint(field.value); err != nil {
			return err
		}
	}
	if err = writer.Key("overflow"); err != nil {
		return err
	}
	if err = writer.Bool(aggregate.Overflow); err != nil {
		return err
	}
	if err = writer.Key("members"); err != nil {
		return err
	}
	if err = writer.BeginArray(); err != nil {
		return err
	}
	for index := 0; index < metrics.Len(); index++ {
		sample, sampleErr := metrics.SnapshotAt(index)
		if sampleErr != nil {
			return sampleErr
		}
		if err = writer.BeginObject(); err != nil {
			return err
		}
		if err = writer.Key("node_aggregate"); err != nil {
			return err
		}
		if err = writer.Bool(sample.NodeAggregate); err != nil {
			return err
		}
		for _, field := range [...]struct {
			name string
			raw  []byte
		}{{"cluster_id", sample.Group.ClusterID[:]}, {"cluster_incarnation", sample.Group.ClusterIncarnation[:]},
			{"shard_incarnation", sample.Group.ShardIncarnation[:]}, {"group_id", sample.Group.GroupID[:]},
			{"node_id", sample.Node[:]}} {
			if err = writer.Key(field.name); err != nil {
				return err
			}
			if err = writeNativeHex(writer, field.raw); err != nil {
				return err
			}
		}
		for _, field := range [...]struct {
			name  string
			value uint64
		}{{"topology_recovery_epoch", sample.Group.TopologyRecoveryEpoch}, {"member_id", sample.Member},
			{"collection_reads", sample.Reads}, {"collection_faults", sample.Faults}} {
			if err = writer.Key(field.name); err != nil {
				return err
			}
			if err = writer.Uint(field.value); err != nil {
				return err
			}
		}
		if err = writeGatewayMetricCut(writer, sample.Cut); err != nil {
			return err
		}
		if err = writeGatewayStageMetrics(writer, sample.Stages); err != nil {
			return err
		}
		if err = writer.EndObject(); err != nil {
			return err
		}
	}
	if err = writer.EndArray(); err != nil {
		return err
	}
	return writer.EndObject()
}

func writeGatewayStageMetrics(writer *vibejson.Writer, stages servicemetrics.StageMetricsSnapshot) error {
	for _, field := range [...]struct {
		name  string
		value uint64
	}{
		{"checkpoint_applied", stages.CheckpointApplied}, {"checkpoints", stages.Checkpoints},
		{"physical_checkpoints", stages.PhysicalCheckpoints}, {"checkpoint_barrier_syncs", stages.CheckpointBarrierSyncs},
		{"wal_live_bytes", stages.WALLiveBytes}, {"wal_entries", stages.WALEntries}, {"wal_syncs", stages.WALSyncs},
		{"backup_requests", stages.BackupRequests}, {"backup_faults", stages.BackupFaults},
		{"backup_logical_bytes", stages.BackupLogicalBytes}, {"backup_scan_bytes", stages.BackupScanBytes},
		{"snapshot_transfer_chunks", stages.SnapshotTransferChunks}, {"snapshot_transfer_bytes", stages.SnapshotTransferBytes},
		{"snapshot_resident_bytes", stages.SnapshotResidentBytes}, {"replica_action_requests", stages.ReplicaActionRequests},
		{"replica_action_completions", stages.ReplicaActionCompletions}, {"replica_action_faults", stages.ReplicaActionFaults},
		{"split_control_requests", stages.SplitControlRequests}, {"split_control_completions", stages.SplitControlCompletions},
		{"split_control_faults", stages.SplitControlFaults},
	} {
		if err := writer.Key(field.name); err != nil {
			return err
		}
		if err := writer.Uint(field.value); err != nil {
			return err
		}
	}
	return nil
}

func writeGatewayMetricCut(writer *vibejson.Writer, cut raftservice.ProgressMetricsSnapshot) error {
	for _, field := range [...]struct {
		name  string
		value uint64
	}{{"proposal_commands", cut.ProposalCommands}, {"proposal_bytes", cut.ProposalBytes},
		{"applied_entries", cut.AppliedEntries}, {"ready_persisted", cut.ReadyPersisted},
		{"snapshots_finished", cut.SnapshotsFinished}, {"read_completions", cut.ReadCompletions},
		{"raft_faults", cut.Faults}} {
		if err := writer.Key(field.name); err != nil {
			return err
		}
		if err := writer.Uint(field.value); err != nil {
			return err
		}
	}
	return nil
}
