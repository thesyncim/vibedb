package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	ErrIndexBackfillLifecycle = errors.New("gateway: global index is not in an online-build lifecycle")
	ErrIndexBackfillTask      = errors.New("gateway: global index backfill task is stale or malformed")
)

// Keep one base check plus every PUT comfortably below the participant batch's
// statement-count envelope, leaving room for future per-page metadata.
const maxGlobalIndexBackfillPageRows = 2048

// GlobalIndexBackfillTask is one independently schedulable base-shard scan.
// After is an exclusive native-primary cursor and may be checkpointed by an
// external controller. Catalog/index/allocation identity fences delayed work.
type GlobalIndexBackfillTask struct {
	Generation         uint64
	Table              string
	Index              string
	IndexID            uint64
	Incarnation        uint64
	BaseDistribution   distribution.DistributionName
	BaseShard          distribution.ShardID
	BaseAllocation     distribution.ShardAllocationGeneration
	BaseRoutingVersion distribution.RoutingVersion
	After              []byte
}

// GlobalIndexBackfillResult is one committed scan page. A controller may
// persist Next only after this method returns successfully. Complete means the
// base-shard snapshot had no later row.
type GlobalIndexBackfillResult struct {
	Next     []byte
	Rows     int
	Complete bool
}

// PlanGlobalIndexBackfill waits for this gateway's older operations to drain,
// then returns one task per independently owned base shard. A cluster schema
// controller must obtain the same [CatalogHolder.WaitOlderDrained]
// acknowledgement from every serving gateway before dispatching any task; the
// local wait here prevents accidental unsafe use by a single-gateway caller.
func (e *Executor) PlanGlobalIndexBackfill(
	ctx context.Context,
	table, index string,
) ([]GlobalIndexBackfillTask, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e == nil || e.catalog == nil {
		return nil, ErrNoCatalog
	}
	lease := e.catalog.pinCurrent()
	if lease.snapshot == nil {
		return nil, ErrNoCatalog
	}
	defer lease.release()
	if _, err := e.catalog.WaitOlderDrained(ctx, lease.generation); err != nil {
		return nil, err
	}
	return lease.snapshot.planGlobalIndexBackfill(table, index)
}

// planGlobalIndexBackfill returns one task per independently owned base shard.
// Building and CatchingUp are writable but never readable; foreground writes
// maintain them while these tasks fill the historical rows. The catalog
// controller must first prove the Building generation is serving on every
// gateway (and older write plans have drained), then publish Ready only after
// every returned task reports Complete. This API is the bounded data plane,
// not a substitute for that cluster-wide schema-change barrier.
func (s *Snapshot) planGlobalIndexBackfill(
	table, index string,
) ([]GlobalIndexBackfillTask, error) {
	program, err := s.compileGlobalIndex(table, index, false)
	if err != nil {
		return nil, err
	}
	if program.metadata.Lifecycle != IndexBuilding &&
		program.metadata.Lifecycle != IndexCatchingUp {
		return nil, ErrIndexBackfillLifecycle
	}
	tasks := make([]GlobalIndexBackfillTask, program.baseManifest.ShardCount())
	for i := range tasks {
		shard, ok := program.baseManifest.ShardMetadataAt(i)
		if !ok {
			return nil, ErrIndexBackfillTask
		}
		tasks[i] = GlobalIndexBackfillTask{
			Generation: s.Generation(), Table: table, Index: index,
			IndexID: program.metadata.IndexID, Incarnation: program.metadata.Incarnation,
			BaseDistribution: program.baseSpec.Name, BaseShard: shard.ID,
			BaseAllocation:     shard.AllocationGeneration,
			BaseRoutingVersion: program.baseManifest.Version(),
		}
	}
	return tasks, nil
}

type backfillIndexGroup struct {
	target     distribution.Target
	address    string
	baseBits   uint8
	indexBits  uint8
	baseScopes []distributedtxn.IntentScope
	keys       [][]byte
	digests    [][sha256.Size]byte
	arena      []byte
	entries    []backfillIndexEntry
}

type backfillIndexEntry struct {
	entryStart, entryEnd int
	valueStart, valueEnd int
	scope                distributedtxn.IntentScope
}

// RunGlobalIndexBackfillTask scans and commits one bounded page. Every index
// PUT is paired with a serializable digest check on its base document. A
// concurrent UPDATE/DELETE therefore either follows the committed PUT and
// maintains it, or makes this page conflict and retry; stale entries cannot be
// resurrected by the scanner.
func (e *Executor) RunGlobalIndexBackfillTask(
	ctx context.Context,
	task GlobalIndexBackfillTask,
	maxRows, maxBytes uint64,
) (*GlobalIndexBackfillResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxRows == 0 || maxBytes == 0 {
		return nil, ErrIndexBackfillTask
	}
	lease := e.catalog.pinCurrent()
	if lease.snapshot == nil {
		return nil, ErrIndexBackfillTask
	}
	defer lease.release()
	snapshot := lease.snapshot
	if snapshot.Generation() < task.Generation {
		return nil, ErrIndexBackfillTask
	}
	program, err := snapshot.compileGlobalIndex(task.Table, task.Index, false)
	if err != nil {
		return nil, err
	}
	if (program.metadata.Lifecycle != IndexBuilding &&
		program.metadata.Lifecycle != IndexCatchingUp) ||
		program.metadata.IndexID != task.IndexID ||
		program.metadata.Incarnation != task.Incarnation ||
		program.baseSpec.Name != task.BaseDistribution ||
		program.baseManifest.Version() != task.BaseRoutingVersion {
		return nil, ErrIndexBackfillTask
	}
	baseTarget, baseAddress, err := resolveBackfillBaseTarget(snapshot, program, task)
	if err != nil {
		return nil, err
	}
	profile := e.profileFor(ClassBatch)
	requestRows := min(maxRows, profile.PerShardRows)
	requestRows = min(requestRows, uint64(maxGlobalIndexBackfillPageRows))
	requestBytes := min(maxBytes, profile.PerShardBytes)
	scanReq := &shardservice.ShardRequest{
		Distribution: task.BaseDistribution, Shard: baseTarget.Shard,
		AllocationGeneration: baseTarget.AllocationGeneration,
		RoutingVersion:       program.baseManifest.Version(), OwnershipEpoch: baseTarget.OwnershipEpoch,
		ReadPolicy: profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadOnly,
		Deadline: profile.PerShardDeadline, MaxRows: requestRows, MaxResultBytes: requestBytes,
		DocumentScan: shardservice.DocumentScanRequest{
			Relation: []byte(task.Table), After: task.After,
		},
	}
	scanCtx, cancel := context.WithTimeout(ctx, profile.PerShardDeadline)
	response, err := e.client.Do(scanCtx, baseAddress, scanReq)
	cancel()
	if err != nil {
		return nil, err
	}
	if response.Kind != shardservice.ResponseRows || len(response.Columns) != 2 ||
		!response.DocumentScan.Present {
		return nil, ErrIndexBackfillTask
	}
	groups, err := buildBackfillGroups(program, baseTarget, response.Rows)
	if err != nil {
		return nil, err
	}
	for i := range groups {
		if err := e.commitBackfillGroup(ctx, snapshot, program, baseTarget, baseAddress, &groups[i], profile); err != nil {
			return nil, err
		}
	}
	return &GlobalIndexBackfillResult{
		Next: append([]byte(nil), response.DocumentScan.Next...),
		Rows: len(response.Rows), Complete: response.DocumentScan.Complete,
	}, nil
}

func resolveBackfillBaseTarget(
	snapshot *Snapshot,
	program GlobalIndexProgram,
	task GlobalIndexBackfillTask,
) (distribution.Target, string, error) {
	for i := 0; i < program.baseManifest.ShardCount(); i++ {
		shard, ok := program.baseManifest.ShardInfo(i)
		if !ok || shard.ID != task.BaseShard {
			continue
		}
		if shard.AllocationGeneration != task.BaseAllocation || len(shard.Leaders) == 0 {
			return distribution.Target{}, "", ErrIndexBackfillTask
		}
		address, err := snapshot.Address(shard.Leaders[0])
		if err != nil {
			return distribution.Target{}, "", err
		}
		return distribution.Target{
			Shard: shard.ID, AllocationGeneration: shard.AllocationGeneration,
			ManifestOrdinal: i,
			Endpoint:        shard.Leaders[0], OwnershipEpoch: shard.Epoch, Role: distribution.RoleLeader,
		}, address, nil
	}
	return distribution.Target{}, "", ErrIndexBackfillTask
}

func buildBackfillGroups(
	program GlobalIndexProgram,
	baseTarget distribution.Target,
	rows [][]shardservice.Cell,
) ([]backfillIndexGroup, error) {
	groups := make([]backfillIndexGroup, 0, min(len(rows), 8))
	byTarget := make(map[distribution.Target]int, cap(groups))
	var workspace GlobalIndexWorkspace
	for i := range rows {
		row := rows[i]
		if len(row) != 2 || row[0].Null || row[1].Null || len(row[0].Bytes) == 0 {
			return nil, ErrIndexBackfillTask
		}
		route, err := program.RouteDocument(row[1].Bytes, &workspace)
		if err != nil {
			return nil, err
		}
		if !sameWriteTarget(route.BaseTarget, baseTarget) ||
			!bytes.Equal(route.BasePrimaryKey, row[0].Bytes) {
			return nil, ErrIndexBackfillTask
		}
		groupOrdinal, ok := byTarget[route.IndexTarget]
		if !ok {
			groupOrdinal = len(groups)
			byTarget[route.IndexTarget] = groupOrdinal
			groups = append(groups, backfillIndexGroup{
				target: route.IndexTarget, address: route.IndexAddress,
				baseBits: route.BaseBucketBits, indexBits: route.IndexBucketBits,
			})
		}
		group := &groups[groupOrdinal]
		group.keys = append(group.keys, row[0].Bytes)
		group.digests = append(group.digests, sha256.Sum256(row[1].Bytes))
		entry := backfillIndexEntry{
			entryStart: len(group.arena), scope: route.IndexScope,
		}
		group.arena = append(group.arena, route.EntryKey...)
		entry.entryEnd = len(group.arena)
		entry.valueStart = entry.entryEnd
		group.arena = append(group.arena, route.LocatorValue...)
		entry.valueEnd = len(group.arena)
		group.entries = append(group.entries, entry)
		group.baseScopes = append(group.baseScopes, route.BaseScope)
	}
	for i := range groups {
		group := &groups[i]
		group.baseScopes = coalesceIntentScopes(group.baseScopes)
		if len(group.baseScopes) > distributedtxn.MaxIntentScopes {
			group.baseBits = 0
			group.baseScopes = nil
		}
	}
	return groups, nil
}

func (e *Executor) commitBackfillGroup(
	ctx context.Context,
	snapshot *Snapshot,
	program GlobalIndexProgram,
	baseTarget distribution.Target,
	baseAddress string,
	group *backfillIndexGroup,
	profile Profile,
) error {
	baseCall := shardCall{
		target: baseTarget, address: baseAddress,
		req: &shardservice.ShardRequest{
			Distribution: program.baseSpec.Name, Shard: baseTarget.Shard,
			AllocationGeneration: baseTarget.AllocationGeneration,
			RoutingVersion:       program.baseManifest.Version(), OwnershipEpoch: baseTarget.OwnershipEpoch,
			ExecutionMode: shardservice.ExecutionReadWrite, ReadPolicy: profile.ReadPolicy,
			Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
			MaxResultBytes: profile.PerShardBytes,
			BucketBits:     group.baseBits, AccessScopes: group.baseScopes,
		},
	}
	primaryPath := []byte(program.metadata.LocatorPaths[program.primary])
	participants, err := appendTransactionStatement(nil, baseCall, shardservice.MutationStatement{
		Kind:     shardservice.MutationPrimaryCheck,
		Relation: program.metadata.Table, PrimaryPath: primaryPath,
		ExpectedKeys: group.keys, ExpectedDigests: group.digests,
	})
	if err != nil {
		return err
	}
	indexCall := shardCall{
		target: group.target, address: group.address,
		req: &shardservice.ShardRequest{
			Distribution: program.indexSpec.Name, Shard: group.target.Shard,
			AllocationGeneration: group.target.AllocationGeneration,
			RoutingVersion:       program.indexManifest.Version(), OwnershipEpoch: group.target.OwnershipEpoch,
			ExecutionMode: shardservice.ExecutionReadWrite, ReadPolicy: profile.ReadPolicy,
			Deadline: profile.PerShardDeadline, MaxRows: profile.PerShardRows,
			MaxResultBytes: profile.PerShardBytes, BucketBits: group.indexBits,
		},
	}
	for i := range group.entries {
		entry := &group.entries[i]
		indexCall.req.AccessScopes = []distributedtxn.IntentScope{entry.scope}
		participants, err = appendTransactionStatement(
			participants, indexCall, shardservice.MutationStatement{
				Kind:     shardservice.MutationGlobalIndexPut,
				Relation: program.metadata.Relation,
				IndexID:  program.metadata.IndexID, Incarnation: program.metadata.Incarnation,
				EntryKey:     group.arena[entry.entryStart:entry.entryEnd],
				Value:        group.arena[entry.valueStart:entry.valueEnd],
				LocatorCount: program.metadata.LocatorCount,
				Unique:       program.metadata.Flags&IndexUnique != 0,
			},
		)
		if err != nil {
			return err
		}
	}
	sortTransactionParticipants(participants)
	_, err = e.executeTransaction(ctx, snapshot, participants, profile)
	return err
}
