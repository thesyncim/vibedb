package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

type rf3DiagnosticAuthorityGroupIdentity struct {
	ClusterID             string `json:"cluster_id"`
	ClusterIncarnation    string `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
	ShardIncarnation      string `json:"shard_incarnation"`
	GroupID               string `json:"group_id"`
}

type rf3DiagnosticAuthorityRuntimeIdentity struct {
	Group                  rf3DiagnosticAuthorityGroupIdentity `json:"group"`
	Distribution           string                              `json:"distribution"`
	Shard                  string                              `json:"shard"`
	AllocationGeneration   uint64                              `json:"allocation_generation"`
	MemberID               uint64                              `json:"member_id"`
	StoreID                string                              `json:"store_id"`
	NodeIncarnation        uint64                              `json:"node_incarnation"`
	RelationManifestDigest string                              `json:"relation_manifest_digest"`
}

type rf3DiagnosticAuthorityConfig struct {
	AppliedVersion uint64 `json:"applied_version"`
	Digest         string `json:"digest"`
	Joint          bool   `json:"joint"`
	Pending        bool   `json:"pending"`
}

type rf3DiagnosticAuthorityRequest struct {
	Group             rf3DiagnosticAuthorityGroupIdentity `json:"group"`
	Term              uint64                              `json:"term"`
	Holder            uint64                              `json:"holder"`
	HolderIncarnation uint64                              `json:"holder_incarnation"`
	Config            rf3DiagnosticAuthorityConfig        `json:"config"`
	PolicyVersion     uint32                              `json:"policy_version"`
	PolicyDigest      string                              `json:"policy_digest"`
	Nonce             uint64                              `json:"nonce"`
	StartAtNs         int64                               `json:"start_at_ns"`
}

type rf3DiagnosticAuthorityClock struct {
	SampleNs    int64 `json:"sample_ns"`
	Initialized bool  `json:"initialized"`
	Faulted     bool  `json:"faulted"`
}

type rf3DiagnosticAuthorityPromise struct {
	HasRecord            bool                          `json:"has_record"`
	Request              rf3DiagnosticAuthorityRequest `json:"request"`
	GrantedAtNs          int64                         `json:"granted_at_ns"`
	PromiseUntilNs       int64                         `json:"promise_until_ns"`
	ActiveKnown          bool                          `json:"active_known"`
	Active               bool                          `json:"active"`
	QuarantineConfigured bool                          `json:"quarantine_configured"`
	QuarantineAtNs       int64                         `json:"quarantine_at_ns"`
	QuarantineUntilNs    int64                         `json:"quarantine_until_ns"`
	QuarantineKnown      bool                          `json:"quarantine_known"`
	QuarantineActive     bool                          `json:"quarantine_active"`
	QuarantineError      bool                          `json:"quarantine_error"`
}

type rf3DiagnosticAuthorityObservation struct {
	Group                rf3DiagnosticAuthorityGroupIdentity `json:"group"`
	Term                 uint64                              `json:"term"`
	Leader               uint64                              `json:"leader"`
	LeaderIncarnation    uint64                              `json:"leader_incarnation"`
	Config               rf3DiagnosticAuthorityConfig        `json:"config"`
	CurrentTermCommitted bool                                `json:"current_term_committed"`
	Stable               bool                                `json:"stable"`
}

type rf3DiagnosticAuthorityHolder struct {
	Available      bool                          `json:"available"`
	Request        rf3DiagnosticAuthorityRequest `json:"request"`
	ExpiresAtNs    int64                         `json:"expires_at_ns"`
	AcceptedVoters []uint64                      `json:"accepted_voter_ids,omitempty"`
}

type rf3DiagnosticAuthorityGateInput struct {
	Input                   string                          `json:"input"`
	BlockedByQuarantine     uint64                          `json:"blocked_by_quarantine"`
	BlockedByLivePromise    uint64                          `json:"blocked_by_live_promise"`
	BlockedByClockFault     uint64                          `json:"blocked_by_clock_fault"`
	Allowed                 uint64                          `json:"allowed"`
	AllowedWhileLivePromise uint64                          `json:"allowed_while_live_promise"`
	QuarantineBlockedTime   rf3DiagnosticAuthorityBlockTime `json:"quarantine_blocked_time"`
	LivePromiseBlockedTime  rf3DiagnosticAuthorityBlockTime `json:"live_promise_blocked_time"`
	ClockFaultBlockedTime   rf3DiagnosticAuthorityBlockTime `json:"clock_fault_blocked_time"`
}

type rf3DiagnosticAuthorityBlockTime struct {
	FirstNs   int64 `json:"first_ns"`
	LastNs    int64 `json:"last_ns"`
	Available bool  `json:"available"`
}

type rf3DiagnosticAuthorityGate struct {
	Inputs                              []rf3DiagnosticAuthorityGateInput `json:"inputs"`
	FirstQuarantineExpiredObservationNs int64                             `json:"first_quarantine_expired_observation_ns"`
	QuarantineExpiredAvailable          bool                              `json:"quarantine_expired_available"`
}

type rf3DiagnosticAuthorityGroup struct {
	RuntimeIdentity      rf3DiagnosticAuthorityRuntimeIdentity `json:"runtime_identity"`
	Status               string                                `json:"status"`
	PolicyVersion        uint32                                `json:"policy_version"`
	PolicyDigest         string                                `json:"policy_digest"`
	Clock                rf3DiagnosticAuthorityClock           `json:"clock"`
	Observation          rf3DiagnosticAuthorityObservation     `json:"observation"`
	ObservationAvailable bool                                  `json:"observation_available"`
	ObservationError     string                                `json:"observation_error"`
	Promise              rf3DiagnosticAuthorityPromise         `json:"promise"`
	Holder               rf3DiagnosticAuthorityHolder          `json:"holder"`
	Gate                 rf3DiagnosticAuthorityGate            `json:"gate"`
}

// rf3DiagnosticResourceCollection is one detached durable collection cut. The
// aggregate counters below remain the compatibility summary; this bounded list
// keeps collection boundaries visible when a terminal memory cut needs to
// distinguish live durable resources from process heap memory.
type rf3DiagnosticResourceCollection struct {
	Group           rf3DiagnosticAuthorityGroupIdentity `json:"group"`
	Collection      string                              `json:"collection"`
	RelationOrdinal int                                 `json:"relation_ordinal"`

	// ResidentBytes is the durable store's logical live-resource accounting; it
	// is not an operating-system resident-memory measurement.
	ResidentBytes                uint64 `json:"resident_bytes"`
	CommitCapacityBytes          uint64 `json:"commit_capacity_bytes"`
	SnapshotCapacity             uint64 `json:"snapshot_capacity"`
	ActiveSnapshots              uint64 `json:"active_snapshots"`
	OldestSnapshotGeneration     uint64 `json:"oldest_snapshot_generation"`
	OldestSnapshotAgeGenerations uint64 `json:"oldest_snapshot_age_generations"`
	FreeScratchCapacityBytes     uint64 `json:"free_scratch_capacity_bytes"`
	FreeScratchExternalBytes     uint64 `json:"free_scratch_external_bytes"`
	FreeScratchLiveBytes         uint64 `json:"free_scratch_live_bytes"`
}

type rf3DiagnosticRuntimeMemStats struct {
	HeapAlloc    uint64 `json:"heap_alloc"`
	HeapInuse    uint64 `json:"heap_inuse"`
	HeapIdle     uint64 `json:"heap_idle"`
	HeapReleased uint64 `json:"heap_released"`
	Sys          uint64 `json:"sys"`
}

// rf3AuthorityDiagnostics groups the bounded authority cuts passed into one
// SIGUSR1 snapshot. Keeping the two detached cuts together avoids positional
// call-site mistakes as the surrounding resource inputs evolve.
type rf3AuthorityDiagnostics struct {
	RoundMetrics func() raftmember.ReadAuthorityRoundMetrics
	Evidence     func() []raftmember.ReadAuthorityEvidence
}

func rf3DiagnosticAuthorityGroupIdentityJSON(identity raftauthority.GroupIdentity) rf3DiagnosticAuthorityGroupIdentity {
	return rf3DiagnosticAuthorityGroupIdentity{
		ClusterID:             hex.EncodeToString(identity.ClusterID[:]),
		ClusterIncarnation:    hex.EncodeToString(identity.ClusterIncarnation[:]),
		TopologyRecoveryEpoch: identity.TopologyRecoveryEpoch,
		ShardIncarnation:      hex.EncodeToString(identity.ShardIncarnation[:]),
		GroupID:               hex.EncodeToString(identity.GroupID[:]),
	}
}

func rf3DiagnosticAuthorityRuntimeIdentityJSON(identity raftmember.RuntimeIdentity) rf3DiagnosticAuthorityRuntimeIdentity {
	return rf3DiagnosticAuthorityRuntimeIdentity{
		Group:                  rf3DiagnosticAuthorityGroupIdentityJSON(authorityGroupIdentity(identity.Group)),
		Distribution:           identity.Distribution,
		Shard:                  identity.Shard,
		AllocationGeneration:   identity.AllocationGeneration,
		MemberID:               identity.MemberID,
		StoreID:                hex.EncodeToString(identity.StoreID[:]),
		NodeIncarnation:        identity.NodeIncarnation,
		RelationManifestDigest: hex.EncodeToString(identity.RelationManifestDigest[:]),
	}
}

func authorityGroupIdentity(group raftmember.GroupKey) raftauthority.GroupIdentity {
	return raftauthority.GroupIdentity{
		ClusterID:             group.ClusterID,
		ClusterIncarnation:    group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		ShardIncarnation:      group.ShardIncarnation,
		GroupID:               group.GroupID,
	}
}

func rf3DiagnosticAuthorityConfigJSON(config raftauthority.ConfigIdentity) rf3DiagnosticAuthorityConfig {
	return rf3DiagnosticAuthorityConfig{
		AppliedVersion: config.AppliedVersion,
		Digest:         hex.EncodeToString(config.Digest[:]),
		Joint:          config.Joint,
		Pending:        config.Pending,
	}
}

func rf3DiagnosticAuthorityRequestJSON(request raftauthority.AuthorityRequest) rf3DiagnosticAuthorityRequest {
	return rf3DiagnosticAuthorityRequest{
		Group:             rf3DiagnosticAuthorityGroupIdentityJSON(request.Group),
		Term:              request.Term,
		Holder:            request.Holder,
		HolderIncarnation: request.HolderIncarnation,
		Config:            rf3DiagnosticAuthorityConfigJSON(request.Config),
		PolicyVersion:     request.PolicyVersion,
		PolicyDigest:      hex.EncodeToString(request.PolicyDigest[:]),
		Nonce:             request.Nonce,
		StartAtNs:         int64(request.StartAt),
	}
}

func rf3DiagnosticAuthorityObservationJSON(observation raftauthority.AuthorityObservation) rf3DiagnosticAuthorityObservation {
	return rf3DiagnosticAuthorityObservation{
		Group:                rf3DiagnosticAuthorityGroupIdentityJSON(observation.Group),
		Term:                 observation.Term,
		Leader:               observation.Leader,
		LeaderIncarnation:    observation.LeaderIncarnation,
		Config:               rf3DiagnosticAuthorityConfigJSON(observation.Config),
		CurrentTermCommitted: observation.CurrentTermCommitted,
		Stable:               observation.Stable,
	}
}

func rf3DiagnosticAuthorityBlockTimeJSON(value raftmember.ReadAuthorityGateBlockTime) rf3DiagnosticAuthorityBlockTime {
	return rf3DiagnosticAuthorityBlockTime{
		FirstNs: int64(value.First), LastNs: int64(value.Last), Available: value.Available,
	}
}

func rf3DiagnosticAuthorityStatus(status raftmember.ReadAuthorityEvidenceStatus) string {
	switch status {
	case raftmember.ReadAuthorityEvidenceConfigured:
		return "configured"
	case raftmember.ReadAuthorityEvidenceDisabled:
		return "disabled"
	default:
		return "unavailable"
	}
}

func rf3DiagnosticAuthorityObservationError(err raftmember.ReadAuthorityEvidenceError) string {
	switch err {
	case raftmember.ReadAuthorityEvidenceErrorLeaderIncarnation:
		return "leader_incarnation"
	case raftmember.ReadAuthorityEvidenceErrorConfiguration:
		return "configuration"
	case raftmember.ReadAuthorityEvidenceErrorClockFault:
		return "clock_fault"
	case raftmember.ReadAuthorityEvidenceErrorUnavailable:
		return "unavailable"
	default:
		return ""
	}
}

func rf3DiagnosticAuthorityGateInputName(index int) string {
	switch index {
	case raftmember.ReadAuthorityGateMessage:
		return "message"
	case raftmember.ReadAuthorityGateTick:
		return "tick"
	case raftmember.ReadAuthorityGateCampaign:
		return "campaign"
	case raftmember.ReadAuthorityGateTransfer:
		return "transfer"
	default:
		return "unknown"
	}
}

func rf3DiagnosticAuthorityGateJSON(metrics raftmember.ReadAuthorityGateMetrics) rf3DiagnosticAuthorityGate {
	inputs := make([]rf3DiagnosticAuthorityGateInput, raftmember.ReadAuthorityGateInputCount)
	for index := range inputs {
		blocked := metrics.BlockedByReason[index]
		times := metrics.BlockedTime[index]
		inputs[index] = rf3DiagnosticAuthorityGateInput{
			Input:                   rf3DiagnosticAuthorityGateInputName(index),
			BlockedByQuarantine:     blocked[raftmember.ReadAuthorityGateQuarantine],
			BlockedByLivePromise:    blocked[raftmember.ReadAuthorityGateLivePromise],
			BlockedByClockFault:     blocked[raftmember.ReadAuthorityGateClockFault],
			Allowed:                 metrics.Allowed[index],
			AllowedWhileLivePromise: metrics.AllowedWhileLivePromise[index],
			QuarantineBlockedTime:   rf3DiagnosticAuthorityBlockTimeJSON(times[raftmember.ReadAuthorityGateQuarantine]),
			LivePromiseBlockedTime:  rf3DiagnosticAuthorityBlockTimeJSON(times[raftmember.ReadAuthorityGateLivePromise]),
			ClockFaultBlockedTime:   rf3DiagnosticAuthorityBlockTimeJSON(times[raftmember.ReadAuthorityGateClockFault]),
		}
	}
	return rf3DiagnosticAuthorityGate{
		Inputs:                              inputs,
		FirstQuarantineExpiredObservationNs: int64(metrics.FirstQuarantineExpiredAt),
		QuarantineExpiredAvailable:          metrics.QuarantineExpiredAvailable,
	}
}

func rf3DiagnosticAuthorityEvidenceCovers(
	evidence []raftmember.ReadAuthorityEvidence,
	expected map[raftmember.GroupKey]struct{},
) bool {
	if len(expected) == 0 || len(evidence) != len(expected) {
		return false
	}
	seen := make(map[raftmember.GroupKey]struct{}, len(evidence))
	for _, group := range evidence {
		key := group.Identity.Group
		if _, ok := expected[key]; !ok {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return len(seen) == len(expected)
}

func rf3DiagnosticAuthorityExpectedKeys(keys []raftmember.GroupKey) map[raftmember.GroupKey]struct{} {
	if len(keys) == 0 {
		return nil
	}
	expected := make(map[raftmember.GroupKey]struct{}, len(keys))
	for _, key := range keys {
		expected[key] = struct{}{}
	}
	if len(expected) != len(keys) {
		return nil
	}
	return expected
}

func rf3DiagnosticAuthorityGroupEvidence(evidence raftmember.ReadAuthorityEvidence) rf3DiagnosticAuthorityGroup {
	return rf3DiagnosticAuthorityGroup{
		RuntimeIdentity: rf3DiagnosticAuthorityRuntimeIdentityJSON(evidence.Identity),
		Status:          rf3DiagnosticAuthorityStatus(evidence.Status),
		PolicyVersion:   evidence.PolicyVersion,
		PolicyDigest:    hex.EncodeToString(evidence.PolicyDigest[:]),
		Clock: rf3DiagnosticAuthorityClock{
			SampleNs:    int64(evidence.Clock.Sample),
			Initialized: evidence.Clock.Initialized,
			Faulted:     evidence.Clock.Faulted,
		},
		Observation:          rf3DiagnosticAuthorityObservationJSON(evidence.Observation),
		ObservationAvailable: evidence.ObservationAvailable,
		ObservationError:     rf3DiagnosticAuthorityObservationError(evidence.ObservationError),
		Promise: rf3DiagnosticAuthorityPromise{
			HasRecord:            evidence.Promise.HasRecord,
			Request:              rf3DiagnosticAuthorityRequestJSON(evidence.Promise.Request),
			GrantedAtNs:          int64(evidence.Promise.GrantedAt),
			PromiseUntilNs:       int64(evidence.Promise.PromiseUntil),
			ActiveKnown:          evidence.PromiseKnown,
			Active:               evidence.PromiseActive,
			QuarantineConfigured: evidence.Promise.QuarantineConfigured,
			QuarantineAtNs:       int64(evidence.Promise.QuarantineAt),
			QuarantineUntilNs:    int64(evidence.Promise.QuarantineUntil),
			QuarantineKnown:      evidence.QuarantineKnown,
			QuarantineActive:     evidence.QuarantineActive,
			QuarantineError:      evidence.Promise.QuarantineError,
		},
		Holder: rf3DiagnosticAuthorityHolder{
			Available:      evidence.Holder.Available,
			Request:        rf3DiagnosticAuthorityRequestJSON(evidence.Holder.Request),
			ExpiresAtNs:    int64(evidence.Holder.ExpiresAt),
			AcceptedVoters: append([]uint64(nil), evidence.Holder.AcceptedVoters...),
		},
		Gate: rf3DiagnosticAuthorityGateJSON(evidence.Gate),
	}
}

// rf3DiagnosticSnapshot is a fixed, bounded process-local evidence record.
// It is emitted only in response to SIGUSR1 and contains detached counters plus
// the current bounded authority metadata; no history, catalog, path, or error
// text is retained. The histogram has the fixed MaxPersistGroupBatches bound
// and lets a trial prove real multi-group append waves from the process that
// owns the shared node log.
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
	ReadAuthorityRoundsStarted     uint64                        `json:"read_authority_rounds_started"`
	ReadAuthorityRequestsCreated   uint64                        `json:"read_authority_requests_created"`
	ReadAuthorityGrantsAccepted    uint64                        `json:"read_authority_grants_accepted"`
	ReadAuthorityEvidenceAvailable bool                          `json:"read_authority_evidence_available"`
	ReadAuthorityEvidence          []rf3DiagnosticAuthorityGroup `json:"read_authority_evidence,omitempty"`

	// Resource counters sum the currently open collection generations. Schema
	// replacement or group retirement can reset them within one process, so
	// interval comparisons require an unchanged serving inventory/generation.
	ResourceStatsAvailable                bool                                  `json:"resource_stats_available"`
	ResourceStatsCoveredGroups            uint64                                `json:"resource_stats_covered_groups"`
	ResourceStatsFailures                 uint64                                `json:"resource_stats_failures"`
	ResourceCollectionsAvailable          bool                                  `json:"resource_collections_available"`
	ResourceCollectionsTruncated          bool                                  `json:"resource_collections_truncated"`
	ResourceCollections                   []rf3DiagnosticResourceCollection     `json:"resource_collections,omitempty"`
	AutomaticCheckpoints                  uint64                                `json:"automatic_checkpoints"`
	RetirementPressureCheckpoints         uint64                                `json:"retirement_pressure_checkpoints"`
	DirtyBytes                            uint64                                `json:"dirty_bytes"`
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

	RuntimeMemStatsAvailable bool                         `json:"runtime_memstats_available"`
	RuntimeMemStats          rf3DiagnosticRuntimeMemStats `json:"runtime_memstats"`
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
	collections                           []rf3DiagnosticResourceCollection
	collectionsTruncated                  bool
	automaticCheckpoints                  uint64
	retirementPressureCheckpoints         uint64
	dirtyBytes                            uint64
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

const rf3DiagnosticMaxResourceCollections = 64

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

func (totals *rf3DiagnosticResourceTotals) addCollection(
	group raftmember.GroupKey,
	kind string,
	relationOrdinal int,
	stats durable.Stats,
) {
	if len(totals.collections) < rf3DiagnosticMaxResourceCollections {
		totals.collections = append(totals.collections, rf3DiagnosticResourceCollection{
			Group:                        rf3DiagnosticAuthorityGroupIdentityJSON(authorityGroupIdentity(group)),
			Collection:                   kind,
			RelationOrdinal:              relationOrdinal,
			ResidentBytes:                stats.ResidentBytes,
			CommitCapacityBytes:          stats.CommitCapacityBytes,
			SnapshotCapacity:             stats.SnapshotCapacity,
			ActiveSnapshots:              stats.ActiveSnapshots,
			OldestSnapshotGeneration:     stats.OldestSnapshotGeneration,
			OldestSnapshotAgeGenerations: stats.OldestSnapshotAgeGenerations,
			FreeScratchCapacityBytes:     stats.FreeScratchCapacityBytes,
			FreeScratchExternalBytes:     stats.FreeScratchExternalBytes,
			FreeScratchLiveBytes:         stats.FreeScratchLiveBytes,
		})
	} else {
		totals.collectionsTruncated = true
	}
	totals.add(stats)
}

func (totals *rf3DiagnosticResourceTotals) failure() {
	totals.addUint64(&totals.failures, 1)
}

func (totals *rf3DiagnosticResourceTotals) add(stats durable.Stats) {
	totals.addUint64(&totals.automaticCheckpoints, stats.AutomaticCheckpoints)
	totals.addUint64(&totals.retirementPressureCheckpoints, stats.RetirementPressureCheckpoints)
	totals.addUint64(&totals.dirtyBytes, stats.DirtyBytes)
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
	groups := make([]raftmember.GroupKey, 0, len(expected))
	for group := range expected {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(left, right int) bool {
		return rf3DiagnosticGroupLess(groups[left], groups[right])
	})
	for _, group := range groups {
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
		totals.addCollection(group, "system", -1, resources.System)
		totals.addCollection(group, "capture", -1, resources.Capture)
		for relation := uint16(0); relation < resources.RelationCount; relation++ {
			totals.addCollection(group, "relation", int(relation), resources.Relations[relation])
		}
	}
	if totals.overflow {
		totals.failure()
	}
	totals.available = len(expected) != 0 && totals.covered == uint64(len(expected)) &&
		totals.failures == 0 && !totals.overflow
	return totals
}

func rf3DiagnosticGroupLess(left, right raftmember.GroupKey) bool {
	if compared := bytes.Compare(left.ClusterID[:], right.ClusterID[:]); compared != 0 {
		return compared < 0
	}
	if compared := bytes.Compare(left.ClusterIncarnation[:], right.ClusterIncarnation[:]); compared != 0 {
		return compared < 0
	}
	if left.TopologyRecoveryEpoch != right.TopologyRecoveryEpoch {
		return left.TopologyRecoveryEpoch < right.TopologyRecoveryEpoch
	}
	if compared := bytes.Compare(left.ShardIncarnation[:], right.ShardIncarnation[:]); compared != 0 {
		return compared < 0
	}
	return bytes.Compare(left.GroupID[:], right.GroupID[:]) < 0
}

func applyRF3DiagnosticResourceTotals(snapshot *rf3DiagnosticSnapshot, resources rf3DiagnosticResourceTotals) {
	if snapshot == nil {
		return
	}
	snapshot.ResourceStatsAvailable = resources.available
	snapshot.ResourceStatsCoveredGroups = resources.covered
	snapshot.ResourceStatsFailures = resources.failures
	snapshot.ResourceCollectionsAvailable = resources.available && !resources.collectionsTruncated && len(resources.collections) != 0
	snapshot.ResourceCollectionsTruncated = resources.collectionsTruncated
	snapshot.ResourceCollections = resources.collections
	snapshot.AutomaticCheckpoints = resources.automaticCheckpoints
	snapshot.RetirementPressureCheckpoints = resources.retirementPressureCheckpoints
	snapshot.DirtyBytes = resources.dirtyBytes
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
	emitRF3DiagnosticSnapshotWithResources(manifest, profile, nodeOwner, server, embedded, serial, inventory, nil, nil, nil, rf3AuthorityDiagnostics{})
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
	authorityDiagnostics rf3AuthorityDiagnostics,
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
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	snapshot.RuntimeMemStatsAvailable = true
	snapshot.RuntimeMemStats = rf3DiagnosticRuntimeMemStats{
		HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse,
		HeapIdle: memory.HeapIdle, HeapReleased: memory.HeapReleased, Sys: memory.Sys,
	}
	resources := collectRF3DiagnosticResources(manifest, prepared, inventory, schemas)
	// Production manifests always carry nonzero group identities. Preserve the
	// old declared count for in-memory legacy fixtures that intentionally omit
	// those identities; such a record remains unavailable below.
	snapshot.Groups = len(resources.expected)
	if snapshot.Groups == 0 {
		snapshot.Groups = len(manifest.groupBundles())
	}
	applyRF3DiagnosticResourceTotals(&snapshot, resources)
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
	if authorityDiagnostics.RoundMetrics != nil {
		metrics := authorityDiagnostics.RoundMetrics()
		snapshot.ReadAuthorityRoundsStarted = metrics.RoundsStarted
		snapshot.ReadAuthorityRequestsCreated = metrics.RequestsCreated
		snapshot.ReadAuthorityGrantsAccepted = metrics.GrantsAccepted
	}
	if authorityDiagnostics.Evidence != nil {
		evidence := authorityDiagnostics.Evidence()
		snapshot.ReadAuthorityEvidence = make([]rf3DiagnosticAuthorityGroup, 0, len(evidence))
		for _, group := range evidence {
			snapshot.ReadAuthorityEvidence = append(snapshot.ReadAuthorityEvidence, rf3DiagnosticAuthorityGroupEvidence(group))
		}
		// An omitted optional runtime must remain an incomplete cut rather than
		// being mistaken for a disabled or authority-free group.
		snapshot.ReadAuthorityEvidenceAvailable = rf3DiagnosticAuthorityEvidenceCovers(evidence, resources.expected)
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
	writeRF3DiagnosticRecord(manifest, raw)
}

func emitRF3AuthorityStartupEvidence(
	manifest rf3Manifest,
	serial *atomic.Uint64,
	evidence []raftmember.ReadAuthorityEvidence,
	expected []raftmember.GroupKey,
) {
	if len(evidence) == 0 {
		return
	}
	record := struct {
		UTC                    string                        `json:"utc"`
		Event                  string                        `json:"event"`
		Serial                 uint64                        `json:"serial"`
		PID                    int                           `json:"pid"`
		Groups                 int                           `json:"groups"`
		ReadAuthorityAvailable bool                          `json:"read_authority_evidence_available"`
		ReadAuthority          []rf3DiagnosticAuthorityGroup `json:"read_authority_startup_evidence"`
	}{
		UTC: time.Now().UTC().Format(time.RFC3339Nano), Event: "read_authority_startup",
		PID: os.Getpid(), Groups: len(evidence),
		ReadAuthorityAvailable: rf3DiagnosticAuthorityEvidenceCovers(evidence, rf3DiagnosticAuthorityExpectedKeys(expected)),
		ReadAuthority:          make([]rf3DiagnosticAuthorityGroup, 0, len(evidence)),
	}
	if serial != nil {
		record.Serial = serial.Add(1)
	}
	for _, group := range evidence {
		record.ReadAuthority = append(record.ReadAuthority, rf3DiagnosticAuthorityGroupEvidence(group))
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return
	}
	writeRF3DiagnosticRecord(manifest, raw)
}

func writeRF3DiagnosticRecord(manifest rf3Manifest, raw []byte) {
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
