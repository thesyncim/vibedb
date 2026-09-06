package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// rf3DiagnosticSnapshot is a fixed, bounded process-local evidence record.
// It is emitted only in response to SIGUSR1 and contains detached counters;
// no request, catalog, path, or error text is retained. The histogram has the
// fixed MaxPersistGroupBatches bound and lets a trial prove real multi-group
// append waves from the process that owns the shared node log.
type rf3DiagnosticSnapshot struct {
	UTC    string `json:"utc"`
	Event  string `json:"event"`
	Serial uint64 `json:"serial"`
	PID    int    `json:"pid"`
	NodeID string `json:"node_id"`
	Groups int    `json:"groups"`

	ReadyWaves             uint64   `json:"ready_waves"`
	ReadyDurableWaves      uint64   `json:"ready_durable_waves"`
	ObservedAppendBarriers uint64   `json:"observed_append_barriers"`
	MultiGroupWaves        uint64   `json:"multi_group_waves"`
	ReadyWaveHistogram     []uint64 `json:"ready_wave_group_histogram"`
	ReadyQueueDepth        uint64   `json:"ready_queue_depth"`
	ReadyQueueCapacity     uint64   `json:"ready_queue_capacity"`
	ReadySubmissions       uint64   `json:"ready_submissions"`
	ReadyQueueWaitNs       uint64   `json:"ready_queue_wait_ns"`
	ReadyWavesAttempted    uint64   `json:"ready_waves_attempted"`
	ReadyPersistAttempts   uint64   `json:"ready_persist_attempts"`
	ReadyPersistSuccesses  uint64   `json:"ready_persist_successes"`
	ReadyPersistFailures   uint64   `json:"ready_persist_failures"`
	ReadyWavesFailed       uint64   `json:"ready_waves_failed"`
	ReadyPersistDurationNs uint64   `json:"ready_persist_duration_ns"`
	ReadyWaveDurationNs    uint64   `json:"ready_wave_duration_ns"`
	ReadyLogicalBatches    uint64   `json:"ready_logical_batches"`
	ReadySeriesSubmissions uint64   `json:"ready_series_submissions"`
	ReadySingletonSeries   uint64   `json:"ready_singleton_series_submissions"`
	ReadyMultiSeries       uint64   `json:"ready_multi_series_submissions"`
	ReadySeriesHistogram   []uint64 `json:"ready_series_histogram"`
	ReadyDurableLogical    uint64   `json:"ready_durable_logical_batches"`
	ReadyDurableSeries     uint64   `json:"ready_durable_series_submissions"`
	ReadyDurableHistogram  []uint64 `json:"ready_durable_series_histogram"`
	ActiveSubmitters       int64    `json:"active_submitters"`
	FailedWaves            uint64   `json:"failed_waves"`
	CheckpointQueue        uint64   `json:"checkpoint_queue_submissions"`
	CheckpointRejected     uint64   `json:"checkpoint_queue_rejected"`
	CheckpointQueueWaitNs  uint64   `json:"checkpoint_queue_wait_ns"`
	CheckpointServiceNs    uint64   `json:"checkpoint_service_ns"`

	NativeAvailable  bool   `json:"native_available"`
	NativeAccepted   uint64 `json:"native_accepted"`
	NativeRejected   uint64 `json:"native_rejected"`
	NativeFailed     uint64 `json:"native_failed"`
	NativeActive     uint64 `json:"native_active"`
	NativeDispatches uint64 `json:"native_semantic_dispatches"`
	NativeFrameBytes int64  `json:"native_inflight_frame_bytes"`

	GatewayAvailable       bool   `json:"gateway_available"`
	GatewayLocalCalls      uint64 `json:"gateway_local_calls"`
	GatewayRemoteCalls     uint64 `json:"gateway_remote_calls"`
	GatewaySemanticSQL     uint64 `json:"gateway_semantic_sql_calls"`
	GatewayLegacyCalls     uint64 `json:"gateway_legacy_calls"`
	GatewaySQLRequestCount uint64 `json:"gateway_sql_request_encodings"`
	GatewaySQLRequestBytes uint64 `json:"gateway_sql_request_encoded_bytes"`

	RemoteDials             uint64 `json:"remote_dials"`
	RemoteReuses            uint64 `json:"remote_reuses"`
	RemotePoisoned          uint64 `json:"remote_poisoned"`
	RemoteRejected          uint64 `json:"remote_rejected"`
	RemoteHandshakeFailures uint64 `json:"remote_handshake_failures"`
	RemoteConnections       int    `json:"remote_connections"`
	RemoteIdle              int    `json:"remote_idle"`
	RemoteWaiters           int    `json:"remote_waiters"`

	RaftProposalBatches             uint64   `json:"raft_proposal_batches"`
	RaftProposalCommands            uint64   `json:"raft_proposal_commands"`
	RaftProposalBytes               uint64   `json:"raft_proposal_bytes"`
	RaftApplyBatches                uint64   `json:"raft_apply_batches"`
	RaftAppliedEntries              uint64   `json:"raft_applied_entries"`
	RaftCommitAdvancements          uint64   `json:"raft_commit_advancements"`
	RaftCommittedEntries            uint64   `json:"raft_committed_entries"`
	RaftReadyPersisted              uint64   `json:"raft_ready_persisted"`
	RaftProposalWindowQueued        uint64   `json:"raft_proposal_window_queued"`
	RaftLateJoinUsed                uint64   `json:"raft_late_join_used"`
	RaftLateJoinMissed              uint64   `json:"raft_late_join_missed"`
	RaftLateJoinEntries             uint64   `json:"raft_late_join_entries"`
	RaftProposalQueueDepthHistogram []uint64 `json:"raft_proposal_queue_depth_histogram"`
	RaftProposalEntriesPerReady     []uint64 `json:"raft_proposal_entries_per_ready"`
	RaftProposalBytesPerReady       []uint64 `json:"raft_proposal_bytes_per_ready"`

	// Owner-side authority counters prove that the SQL read actually used the
	// fast path. They are separate from the Runtime's protocol-round counters.
	ReadIndexShared                 uint64 `json:"read_index_shared"`
	AuthorityReadHits               uint64 `json:"authority_read_hits"`
	AuthorityReadIndexFallbacks     uint64 `json:"authority_read_index_fallbacks"`
	AuthorityReadValidationRetries  uint64 `json:"authority_read_validation_retries"`
	AuthorityReadValidationFailures uint64 `json:"authority_read_validation_failures"`
	AuthorityRoundAttempts          uint64 `json:"authority_round_attempts"`

	// These counters come from Runtime authority state, rather than the owner
	// per-read Ensure offer counter. RequestsCreated counts requests appended to
	// the bounded outbound queue. GrantsAccepted includes the local self-grant
	// and excludes duplicate or replayed grants.
	ReadAuthorityRoundsStarted   uint64 `json:"read_authority_rounds_started"`
	ReadAuthorityRequestsCreated uint64 `json:"read_authority_requests_created"`
	ReadAuthorityGrantsAccepted  uint64 `json:"read_authority_grants_accepted"`

	// Resource counters sum the currently open collection generations. Schema
	// replacement or group retirement can reset them within one process, so
	// interval comparisons require an unchanged serving inventory/generation.
	ResourceStatsAvailable                bool                                  `json:"resource_stats_available"`
	ResourceStatsCoveredGroups            uint64                                `json:"resource_stats_covered_groups"`
	ResourceStatsFailures                 uint64                                `json:"resource_stats_failures"`
	PrimaryOverlayFolds                   uint64                                `json:"primary_overlay_folds"`
	PrimaryOverlayMaterializationAttempts uint64                                `json:"primary_overlay_materialization_attempts"`
	PrimaryOverlayMaterializations        uint64                                `json:"primary_overlay_materializations"`
	PrimaryOverlayMaterializationFailures uint64                                `json:"primary_overlay_materialization_failures"`
	PrimaryOverlayFoldNSCount             uint64                                `json:"primary_overlay_fold_ns_count"`
	PrimaryOverlayFoldNSSum               uint64                                `json:"primary_overlay_fold_ns_sum"`
	PrimaryOverlayFoldNSMax               uint64                                `json:"primary_overlay_fold_ns_max"`
	PrimaryOverlayFoldNSBuckets           [durable.StatsHistogramBuckets]uint64 `json:"primary_overlay_fold_ns_buckets"`
	PrimaryOverlayPressureFolds           uint64                                `json:"primary_overlay_pressure_folds"`
	PrimaryOverlaySnapshotFolds           uint64                                `json:"primary_overlay_snapshot_folds"`
	PrimaryOverlayBarrierFolds            uint64                                `json:"primary_overlay_barrier_folds"`
	PrimaryOverlayCheckpointFolds         uint64                                `json:"primary_overlay_checkpoint_folds"`
	PrimaryOverlayArenaBytes              uint64                                `json:"primary_overlay_arena_bytes"`
	PrimaryOverlayRetainedRecords         uint64                                `json:"primary_overlay_retained_records"`
	PrimaryOverlayDirtyBuckets            uint64                                `json:"primary_overlay_dirty_buckets"`
	PrimaryOverlayReservedFoldBytes       uint64                                `json:"primary_overlay_reserved_fold_bytes"`
}

type rf3DiagnosticApply interface {
	ResourceStats() (sqldriver.ReplicatedApplyResourceStats, error)
}

type rf3DiagnosticResourceTotals struct {
	available                             bool
	covered, failures                     uint64
	groups                                map[raftmember.GroupKey]rf3DiagnosticApply
	expected                              map[raftmember.GroupKey]struct{}
	overflow                              bool
	primaryOverlayFolds                   uint64
	primaryOverlayMaterializationAttempts uint64
	primaryOverlayMaterializations        uint64
	primaryOverlayMaterializationFailures uint64
	primaryOverlayFoldNS                  durable.StatsHistogram
	primaryOverlayPressureFolds           uint64
	primaryOverlaySnapshotFolds           uint64
	primaryOverlayBarrierFolds            uint64
	primaryOverlayCheckpointFolds         uint64
	primaryOverlayArenaBytes              uint64
	primaryOverlayRetainedRecords         uint64
	primaryOverlayDirtyBuckets            uint64
	primaryOverlayReservedFoldBytes       uint64
}

func addRF3DiagnosticResourceGroup(expected map[raftmember.GroupKey]struct{}, group raftmember.GroupKey) {
	if group != (raftmember.GroupKey{}) {
		expected[group] = struct{}{}
	}
}

func addRF3DiagnosticResourceProvider(
	providers map[raftmember.GroupKey]rf3DiagnosticApply,
	group raftmember.GroupKey,
	apply rf3DiagnosticApply,
) {
	if group != (raftmember.GroupKey{}) && apply != nil {
		if prior, present := providers[group]; !present || prior == nil {
			providers[group] = apply
		}
	}
}

func setRF3DiagnosticResourceProvider(
	providers map[raftmember.GroupKey]rf3DiagnosticApply,
	group raftmember.GroupKey,
	apply rf3DiagnosticApply,
) {
	if group != (raftmember.GroupKey{}) {
		providers[group] = apply
	}
}

func (totals *rf3DiagnosticResourceTotals) addUint64(target *uint64, value uint64) {
	if ^uint64(0)-*target < value {
		*target = ^uint64(0)
		totals.overflow = true
		return
	}
	*target += value
}

func (totals *rf3DiagnosticResourceTotals) failure() {
	totals.addUint64(&totals.failures, 1)
}

func (totals *rf3DiagnosticResourceTotals) add(stats durable.Stats) {
	totals.addUint64(&totals.primaryOverlayFolds, stats.PrimaryOverlayFolds)
	totals.addUint64(&totals.primaryOverlayMaterializationAttempts, stats.PrimaryOverlayMaterializationAttempts)
	totals.addUint64(&totals.primaryOverlayMaterializations, stats.PrimaryOverlayMaterializations)
	totals.addUint64(&totals.primaryOverlayMaterializationFailures, stats.PrimaryOverlayMaterializationFailures)
	totals.addUint64(&totals.primaryOverlayFoldNS.Count, stats.PrimaryOverlayFoldNS.Count)
	totals.addUint64(&totals.primaryOverlayFoldNS.Sum, stats.PrimaryOverlayFoldNS.Sum)
	if stats.PrimaryOverlayFoldNS.Max > totals.primaryOverlayFoldNS.Max {
		totals.primaryOverlayFoldNS.Max = stats.PrimaryOverlayFoldNS.Max
	}
	for index := range totals.primaryOverlayFoldNS.Buckets {
		totals.addUint64(&totals.primaryOverlayFoldNS.Buckets[index], stats.PrimaryOverlayFoldNS.Buckets[index])
	}
	totals.addUint64(&totals.primaryOverlayPressureFolds, stats.PrimaryOverlayPressureFolds)
	totals.addUint64(&totals.primaryOverlaySnapshotFolds, stats.PrimaryOverlaySnapshotFolds)
	totals.addUint64(&totals.primaryOverlayBarrierFolds, stats.PrimaryOverlayBarrierFolds)
	totals.addUint64(&totals.primaryOverlayCheckpointFolds, stats.PrimaryOverlayCheckpointFolds)
	totals.addUint64(&totals.primaryOverlayArenaBytes, stats.PrimaryOverlayArenaBytes)
	totals.addUint64(&totals.primaryOverlayRetainedRecords, stats.PrimaryOverlayRetainedRecords)
	totals.addUint64(&totals.primaryOverlayDirtyBuckets, stats.PrimaryOverlayDirtyBuckets)
	totals.addUint64(&totals.primaryOverlayReservedFoldBytes, stats.PrimaryOverlayReservedFoldBytes)
}

func aggregateRF3DiagnosticResources(
	expected map[raftmember.GroupKey]struct{},
	providers map[raftmember.GroupKey]rf3DiagnosticApply,
	inventoryUnavailable bool,
) rf3DiagnosticResourceTotals {
	totals := rf3DiagnosticResourceTotals{expected: expected, groups: providers}
	if inventoryUnavailable {
		totals.failure()
	}
	for group := range expected {
		apply, present := providers[group]
		if !present || apply == nil {
			totals.failure()
			continue
		}
		resources, err := apply.ResourceStats()
		if err != nil || resources.RelationCount == 0 ||
			resources.RelationCount > uint16(len(resources.Relations)) {
			totals.failure()
			continue
		}
		totals.addUint64(&totals.covered, 1)
		totals.add(resources.System)
		totals.add(resources.Capture)
		for relation := uint16(0); relation < resources.RelationCount; relation++ {
			totals.add(resources.Relations[relation])
		}
	}
	if totals.overflow {
		totals.failure()
	}
	totals.available = len(expected) != 0 && totals.covered == uint64(len(expected)) &&
		totals.failures == 0 && !totals.overflow
	return totals
}

type rf3DiagnosticInventorySnapshot struct {
	nativeGroups map[raftmember.GroupKey]struct{}
	providers    map[raftmember.GroupKey]rf3DiagnosticApply
	usable       bool
}

func snapshotRF3DiagnosticInventory(inventory *rf3AdoptedGroupInventory) rf3DiagnosticInventorySnapshot {
	if inventory == nil {
		return rf3DiagnosticInventorySnapshot{usable: true}
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	snapshot := rf3DiagnosticInventorySnapshot{
		nativeGroups: make(map[raftmember.GroupKey]struct{}),
		providers:    make(map[raftmember.GroupKey]rf3DiagnosticApply),
		usable:       inventory.root != nil && !inventory.failed,
	}
	if native := inventory.nativeChildren.Load(); native != nil {
		for group := range *native {
			if group != (raftmember.GroupKey{}) {
				snapshot.nativeGroups[group] = struct{}{}
			}
		}
	}
	if snapshot.usable {
		for group, runtime := range inventory.runtimes {
			if runtime.apply != nil {
				addRF3DiagnosticResourceProvider(snapshot.providers, group, runtime.apply)
			}
		}
	}
	return snapshot
}

func snapshotRF3DiagnosticSchemaProviders(schemas *rf3SchemaActivator) map[raftmember.GroupKey]rf3DiagnosticApply {
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply)
	if schemas == nil {
		return providers
	}
	type schemaState struct {
		group raftmember.GroupKey
		state *rf3SchemaGeneration
	}
	schemas.mu.RLock()
	states := make([]schemaState, 0, len(schemas.groups))
	for group, state := range schemas.groups {
		states = append(states, schemaState{group: group, state: state})
	}
	schemas.mu.RUnlock()
	for _, entry := range states {
		var apply rf3DiagnosticApply
		if entry.state != nil {
			entry.state.mu.Lock()
			apply = entry.state.apply
			entry.state.mu.Unlock()
		}
		// A mapped generation with no current apply must mask a prepared
		// predecessor rather than accidentally reporting that closed handle.
		setRF3DiagnosticResourceProvider(providers, entry.group, apply)
	}
	return providers
}

func collectRF3DiagnosticResources(
	manifest rf3Manifest,
	prepared []preparedRF3Group,
	inventory *rf3AdoptedGroupInventory,
	schemas *rf3SchemaActivator,
) rf3DiagnosticResourceTotals {
	bundles := manifest.groupBundles()
	expected := make(map[raftmember.GroupKey]struct{}, len(bundles)+len(prepared))
	providers := make(map[raftmember.GroupKey]rf3DiagnosticApply, len(bundles)+len(prepared))
	for _, bundle := range bundles {
		addRF3DiagnosticResourceGroup(expected, bundle.Route.Group)
	}
	for index := range prepared {
		group := groupFromBinding(prepared[index].base.Binding)
		if group == (raftmember.GroupKey{}) {
			group = prepared[index].manifest.Route.Group
		}
		if prepared[index].apply != nil {
			addRF3DiagnosticResourceProvider(providers, group, prepared[index].apply)
		}
	}
	inventorySnapshot := snapshotRF3DiagnosticInventory(inventory)
	for group := range inventorySnapshot.nativeGroups {
		addRF3DiagnosticResourceGroup(expected, group)
	}
	for group, apply := range inventorySnapshot.providers {
		addRF3DiagnosticResourceProvider(providers, group, apply)
	}
	for group, apply := range snapshotRF3DiagnosticSchemaProviders(schemas) {
		setRF3DiagnosticResourceProvider(providers, group, apply)
	}
	return aggregateRF3DiagnosticResources(expected, providers, inventory != nil && !inventorySnapshot.usable)
}

func applyRF3DiagnosticProgress(snapshot *rf3DiagnosticSnapshot, metrics raftservice.ProgressMetricsSnapshot) {
	if snapshot == nil {
		return
	}
	snapshot.RaftProposalBatches = metrics.ProposalBatches
	snapshot.RaftProposalCommands = metrics.ProposalCommands
	snapshot.RaftProposalBytes = metrics.ProposalBytes
	snapshot.RaftApplyBatches = metrics.ApplyBatches
	snapshot.RaftAppliedEntries = metrics.AppliedEntries
	snapshot.RaftCommitAdvancements = metrics.CommitAdvancements
	snapshot.RaftCommittedEntries = metrics.CommittedEntries
	snapshot.RaftReadyPersisted = metrics.ReadyPersisted
	snapshot.RaftProposalWindowQueued = metrics.ProposalWindowQueued
	snapshot.RaftLateJoinUsed = metrics.LateJoinUsed
	snapshot.RaftLateJoinMissed = metrics.LateJoinMissed
	snapshot.RaftLateJoinEntries = metrics.LateJoinEntries
	copy(snapshot.RaftProposalQueueDepthHistogram, metrics.ProposalQueueDepthHistogram[:])
	copy(snapshot.RaftProposalEntriesPerReady, metrics.ProposalEntriesPerReady[:])
	copy(snapshot.RaftProposalBytesPerReady, metrics.ProposalBytesPerReady[:])
	snapshot.ReadIndexShared = metrics.ReadIndexShared
	snapshot.AuthorityReadHits = metrics.AuthorityReadHits
	snapshot.AuthorityReadIndexFallbacks = metrics.AuthorityReadIndexFallbacks
	snapshot.AuthorityReadValidationRetries = metrics.AuthorityReadValidationRetries
	snapshot.AuthorityReadValidationFailures = metrics.AuthorityReadValidationFailures
	snapshot.AuthorityRoundAttempts = metrics.AuthorityRoundAttempts
}

func applyRF3DiagnosticSequencer(snapshot *rf3DiagnosticSnapshot, stats raftstore.NodeSubmissionSequencerStats) {
	if snapshot == nil {
		return
	}
	snapshot.ReadyWaves = stats.ReadyWavesSucceeded
	snapshot.ReadyDurableWaves = stats.ReadyDurableWaves
	snapshot.ObservedAppendBarriers = stats.ObservedAppendBarriers
	snapshot.MultiGroupWaves = stats.MultiGroupWaves
	copy(snapshot.ReadyWaveHistogram, stats.ReadyWaveGroupHistogram[:])
	snapshot.ReadyQueueDepth = stats.QueueDepth
	snapshot.ReadyQueueCapacity = stats.QueueCapacity
	snapshot.ReadySubmissions = stats.ReadySubmissions
	snapshot.ReadyQueueWaitNs = stats.ReadyQueueWaitNanos
	snapshot.ReadyWavesAttempted = stats.ReadyWavesAttempted
	snapshot.ReadyPersistAttempts = stats.ReadyPersistAttempts
	snapshot.ReadyPersistSuccesses = stats.ReadyPersistSuccesses
	snapshot.ReadyPersistFailures = stats.ReadyPersistFailures
	snapshot.ReadyWavesFailed = stats.ReadyWavesFailed
	snapshot.ReadyPersistDurationNs = stats.ReadyPersistDurationNanos
	snapshot.ReadyWaveDurationNs = stats.ReadyWaveDurationNanos
	snapshot.ReadyLogicalBatches = stats.ReadyLogicalBatches
	snapshot.ReadySeriesSubmissions = stats.ReadySeriesSubmissions
	snapshot.ReadySingletonSeries = stats.ReadySingletonSeriesSubmissions
	snapshot.ReadyMultiSeries = stats.ReadyMultiSeriesSubmissions
	copy(snapshot.ReadySeriesHistogram, stats.ReadySeriesHistogram[:])
	snapshot.ReadyDurableLogical = stats.ReadyDurableLogicalBatches
	snapshot.ReadyDurableSeries = stats.ReadyDurableSeriesSubmissions
	copy(snapshot.ReadyDurableHistogram, stats.ReadyDurableSeriesHistogram[:])
	snapshot.ActiveSubmitters = stats.ActiveSubmitters
	snapshot.FailedWaves = stats.FailedWaves
	snapshot.CheckpointQueue = stats.CheckpointQueueSubmissions
	snapshot.CheckpointRejected = stats.CheckpointQueueRejected
	snapshot.CheckpointQueueWaitNs = stats.CheckpointQueueWaitNanos
	snapshot.CheckpointServiceNs = stats.CheckpointServiceNanos
}

// emitRF3DiagnosticSnapshot writes one machine-readable line with a stable
// prefix and atomically replaces the bounded node-root latest snapshot.
// Benchmark harnesses can send SIGUSR1 at trial boundaries and parse the
// serial and counters without opening an unauthenticated metrics endpoint.
// The caller invokes this synchronously from the lifecycle select, before any
// owner or transport is drained.
func emitRF3DiagnosticSnapshot(
	manifest rf3Manifest,
	profile *rafttransport.PeerTLS,
	nodeOwner *rf3NodeOwner,
	server *shardservice.ReplicatedServer,
	embedded *rf3EmbeddedGateway,
	serial *atomic.Uint64,
	inventory *rf3AdoptedGroupInventory,
) {
	emitRF3DiagnosticSnapshotWithResources(manifest, profile, nodeOwner, server, embedded, serial, inventory, nil, nil, nil)
}

func emitRF3DiagnosticSnapshotWithResources(
	manifest rf3Manifest,
	profile *rafttransport.PeerTLS,
	nodeOwner *rf3NodeOwner,
	server *shardservice.ReplicatedServer,
	embedded *rf3EmbeddedGateway,
	serial *atomic.Uint64,
	inventory *rf3AdoptedGroupInventory,
	prepared []preparedRF3Group,
	schemas *rf3SchemaActivator,
	progressMetrics *raftservice.ProgressMetrics,
	authorityRoundMetrics ...func() raftmember.ReadAuthorityRoundMetrics,
) {
	snapshot := rf3DiagnosticSnapshot{
		UTC: time.Now().UTC().Format(time.RFC3339Nano), Event: "snapshot", PID: os.Getpid(),
		Groups:                          len(manifest.groupBundles()),
		ReadyWaveHistogram:              make([]uint64, raftstore.MaxPersistGroupBatches+1),
		ReadySeriesHistogram:            make([]uint64, raftstore.MaxReadySeries+1),
		ReadyDurableHistogram:           make([]uint64, raftstore.MaxReadySeries+1),
		RaftProposalQueueDepthHistogram: make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalEntriesPerReady:     make([]uint64, raftservice.ProposalEntryHistogramBuckets),
		RaftProposalBytesPerReady:       make([]uint64, raftservice.ProposalBytesHistogramBuckets),
	}
	resources := collectRF3DiagnosticResources(manifest, prepared, inventory, schemas)
	// Production manifests always carry nonzero group identities. Preserve the
	// old declared count for in-memory legacy fixtures that intentionally omit
	// those identities; such a record remains unavailable below.
	snapshot.Groups = len(resources.expected)
	if snapshot.Groups == 0 {
		snapshot.Groups = len(manifest.groupBundles())
	}
	snapshot.ResourceStatsAvailable = resources.available
	snapshot.ResourceStatsCoveredGroups = resources.covered
	snapshot.ResourceStatsFailures = resources.failures
	snapshot.PrimaryOverlayFolds = resources.primaryOverlayFolds
	snapshot.PrimaryOverlayMaterializationAttempts = resources.primaryOverlayMaterializationAttempts
	snapshot.PrimaryOverlayMaterializations = resources.primaryOverlayMaterializations
	snapshot.PrimaryOverlayMaterializationFailures = resources.primaryOverlayMaterializationFailures
	snapshot.PrimaryOverlayFoldNSCount = resources.primaryOverlayFoldNS.Count
	snapshot.PrimaryOverlayFoldNSSum = resources.primaryOverlayFoldNS.Sum
	snapshot.PrimaryOverlayFoldNSMax = resources.primaryOverlayFoldNS.Max
	snapshot.PrimaryOverlayFoldNSBuckets = resources.primaryOverlayFoldNS.Buckets
	snapshot.PrimaryOverlayPressureFolds = resources.primaryOverlayPressureFolds
	snapshot.PrimaryOverlaySnapshotFolds = resources.primaryOverlaySnapshotFolds
	snapshot.PrimaryOverlayBarrierFolds = resources.primaryOverlayBarrierFolds
	snapshot.PrimaryOverlayCheckpointFolds = resources.primaryOverlayCheckpointFolds
	snapshot.PrimaryOverlayArenaBytes = resources.primaryOverlayArenaBytes
	snapshot.PrimaryOverlayRetainedRecords = resources.primaryOverlayRetainedRecords
	snapshot.PrimaryOverlayDirtyBuckets = resources.primaryOverlayDirtyBuckets
	snapshot.PrimaryOverlayReservedFoldBytes = resources.primaryOverlayReservedFoldBytes
	if serial != nil {
		snapshot.Serial = serial.Add(1)
	}
	if profile != nil {
		node := profile.LocalIdentity().Node
		snapshot.NodeID = fmt.Sprintf("%x", node[:])
	}
	if nodeOwner != nil && nodeOwner.sequencer != nil {
		applyRF3DiagnosticSequencer(&snapshot, nodeOwner.sequencer.Stats())
	}
	if progressMetrics != nil {
		applyRF3DiagnosticProgress(&snapshot, progressMetrics.Snapshot())
	}
	if len(authorityRoundMetrics) != 0 && authorityRoundMetrics[0] != nil {
		metrics := authorityRoundMetrics[0]()
		snapshot.ReadAuthorityRoundsStarted = metrics.RoundsStarted
		snapshot.ReadAuthorityRequestsCreated = metrics.RequestsCreated
		snapshot.ReadAuthorityGrantsAccepted = metrics.GrantsAccepted
	}
	if server != nil {
		stats := server.Stats()
		snapshot.NativeAvailable = true
		snapshot.NativeAccepted = stats.Accepted
		snapshot.NativeRejected = stats.Rejected
		snapshot.NativeFailed = stats.Failed
		snapshot.NativeActive = stats.Active
		snapshot.NativeDispatches = stats.SemanticDispatch
		snapshot.NativeFrameBytes = stats.InFlightFrameBytes
	}
	if embedded != nil {
		snapshot.GatewayAvailable = embedded.client != nil
		if embedded.client != nil {
			stats := embedded.client.Stats()
			snapshot.GatewayLocalCalls = stats.LocalCalls
			snapshot.GatewayRemoteCalls = stats.RemoteCalls
			snapshot.GatewaySemanticSQL = stats.SemanticSQLCalls
			snapshot.GatewayLegacyCalls = stats.LegacyCalls
			snapshot.GatewaySQLRequestCount = stats.SQLRequestEncodings
			snapshot.GatewaySQLRequestBytes = stats.SQLRequestEncodedBytes
		}
		if embedded.remote != nil {
			stats := embedded.remote.Stats()
			snapshot.RemoteDials = stats.Dials
			snapshot.RemoteReuses = stats.Reuses
			snapshot.RemotePoisoned = stats.Poisoned
			snapshot.RemoteRejected = stats.Rejected
			snapshot.RemoteHandshakeFailures = stats.HandshakeFailures
			snapshot.RemoteConnections = stats.Connections
			snapshot.RemoteIdle = stats.Idle
			snapshot.RemoteWaiters = stats.Waiters
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "VIBEDB_RF3_DIAGNOSTIC %s\n", raw)
	if manifest.NodeLog != nil {
		// Replace through a same-directory temporary so readers never observe a
		// partial record. Sync before returning so the caller can wait for the
		// serial to appear on disk before entering a timed interval. Only one
		// fixed-size latest record is retained.
		directory := filepath.Dir(manifest.NodeLog.Path)
		if file, openErr := os.CreateTemp(directory, ".rf3-diagnostics-"); openErr == nil {
			temporary := file.Name()
			_, writeErr := file.Write(raw)
			syncErr := file.Sync()
			closeErr := file.Close()
			renameErr := error(nil)
			if writeErr == nil && syncErr == nil && closeErr == nil {
				renameErr = os.Rename(temporary, filepath.Join(directory, "rf3-diagnostics.json"))
			}
			if renameErr != nil || writeErr != nil || syncErr != nil || closeErr != nil {
				_ = os.Remove(temporary)
			}
		}
	}
}
