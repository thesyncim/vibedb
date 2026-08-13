// Package mixedtelemetry defines the versioned diagnostic record exchanged by
// cmd/mixed and cmd/mixedsuite. The latency table remains deliberately stable;
// internal counters travel on stderr and mixedsuite retains them as separate
// long-form rows.
package mixedtelemetry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

const (
	// Schema is bumped whenever Record changes incompatibly.
	Schema = 1
	// Prefix lets a parent distinguish the record from ordinary diagnostics.
	Prefix = "mixed-telemetry-json\t"
)

// Record contains measurements for the timed phase. Counter fields are
// after-before deltas. The two maximum fields are process high-waters sampled
// immediately before and after the phase: high-waters cannot truthfully be
// subtracted, so both samples are retained explicitly.
type Record struct {
	Schema    int    `json:"schema"`
	Engine    string `json:"engine"`
	Clients   int    `json:"clients"`
	Available bool   `json:"durable_stats_available"`

	RuntimeTotalAllocBytes uint64 `json:"runtime_total_alloc_bytes"`
	RuntimeMallocs         uint64 `json:"runtime_mallocs"`

	ScalarPatchAttempts            uint64 `json:"scalar_patch_attempts,omitempty"`
	ScalarPatchAccepts             uint64 `json:"scalar_patch_accepts,omitempty"`
	CompactPatchAttempts           uint64 `json:"compact_patch_attempts,omitempty"`
	CompactPatchAccepts            uint64 `json:"compact_patch_accepts,omitempty"`
	OverlayFolds                   uint64 `json:"overlay_folds,omitempty"`
	OverlayFoldAttempts            uint64 `json:"overlay_fold_attempts,omitempty"`
	OverlayMaterializations        uint64 `json:"overlay_materializations,omitempty"`
	OverlayMaterializationFailures uint64 `json:"overlay_materialization_failures,omitempty"`
	OverlayPressureFolds           uint64 `json:"overlay_pressure_folds,omitempty"`
	OverlaySnapshotFolds           uint64 `json:"overlay_snapshot_folds,omitempty"`
	OverlayBarrierFolds            uint64 `json:"overlay_barrier_folds,omitempty"`
	OverlayCheckpointFolds         uint64 `json:"overlay_checkpoint_folds,omitempty"`
	ConcurrentReplaces             uint64 `json:"concurrent_replaces,omitempty"`
	ConcurrentFallbacks            uint64 `json:"concurrent_fallbacks,omitempty"`
	PublishGroups                  uint64 `json:"publish_groups,omitempty"`
	PublishGroupMaxBefore          uint64 `json:"publish_group_max_before,omitempty"`
	PublishGroupMax                uint64 `json:"publish_group_max,omitempty"`
	AutomaticCheckpoints           uint64 `json:"automatic_checkpoints,omitempty"`
	OverlayArenaBytesBefore        uint64 `json:"overlay_arena_bytes_before,omitempty"`
	OverlayArenaBytes              uint64 `json:"overlay_arena_bytes,omitempty"`
	OverlayRetainedRecordsBefore   uint64 `json:"overlay_retained_records_before,omitempty"`
	OverlayRetainedRecords         uint64 `json:"overlay_retained_records,omitempty"`
	OverlayDirtyBucketsBefore      uint64 `json:"overlay_dirty_buckets_before,omitempty"`
	OverlayDirtyBuckets            uint64 `json:"overlay_dirty_buckets,omitempty"`
	OverlayReservedFoldBytesBefore uint64 `json:"overlay_reserved_fold_bytes_before,omitempty"`
	OverlayReservedFoldBytes       uint64 `json:"overlay_reserved_fold_bytes,omitempty"`
	OverlayDirtyBucketLimit        uint64 `json:"overlay_dirty_bucket_limit,omitempty"`
	OverlayDirtyByteLimit          uint64 `json:"overlay_dirty_byte_limit,omitempty"`

	JournalAcks             uint64 `json:"journal_acks,omitempty"`
	ChainAcks               uint64 `json:"chain_acks,omitempty"`
	JournalSyncs            uint64 `json:"journal_syncs,omitempty"`
	JournalGroupMaxBefore   uint64 `json:"journal_group_max_before,omitempty"`
	JournalGroupMax         uint64 `json:"journal_group_max,omitempty"`
	JournalStrictSyncs      uint64 `json:"journal_strict_syncs,omitempty"`
	JournalStrictRecords    uint64 `json:"journal_strict_records,omitempty"`
	JournalStrictMutations  uint64 `json:"journal_strict_mutations,omitempty"`
	JournalStrictBytes      uint64 `json:"journal_strict_bytes,omitempty"`
	JournalDeltaCheckpoints uint64 `json:"journal_delta_checkpoints,omitempty"`
	JournalDeltaRecords     uint64 `json:"journal_delta_records,omitempty"`
	JournalDeltaBytes       uint64 `json:"journal_delta_bytes,omitempty"`
	JournalDeltaFallbacks   uint64 `json:"journal_delta_fallbacks,omitempty"`
	DeviceBytes             uint64 `json:"device_bytes,omitempty"`

	LeafSplits    uint64 `json:"leaf_splits,omitempty"`
	EmptyReclaims uint64 `json:"empty_reclaims,omitempty"`

	Histograms map[string]Histogram `json:"histograms,omitempty"`
}

// Histogram is the measured-phase delta of a cumulative durable histogram.
// MaxBefore and Max are explicit process high-water samples; Count, Sum, and
// Buckets are after-before deltas.
type Histogram struct {
	Count     uint64   `json:"count"`
	Sum       uint64   `json:"sum"`
	MaxBefore uint64   `json:"max_before"`
	Max       uint64   `json:"max"`
	Buckets   []uint64 `json:"buckets"`
}

// Metric is the stable long-form representation mixedsuite writes and
// summarizes. Scope separates process-global runtime costs from VibeDB-only
// durable counters.
type Metric struct {
	Scope string
	Name  string
	Value uint64
}

// Metrics returns metrics in a deterministic order. Runtime metrics exist for
// every engine; durable metrics are omitted when the engine has no Stats API.
func (r Record) Metrics() []Metric {
	metrics := []Metric{
		{Scope: "runtime", Name: "total-alloc-bytes", Value: r.RuntimeTotalAllocBytes},
		{Scope: "runtime", Name: "mallocs", Value: r.RuntimeMallocs},
	}
	if !r.Available {
		return metrics
	}
	metrics = append(metrics,
		Metric{Scope: "vibedb", Name: "scalar-patch-attempts", Value: r.ScalarPatchAttempts},
		Metric{Scope: "vibedb", Name: "scalar-patch-accepts", Value: r.ScalarPatchAccepts},
		Metric{Scope: "vibedb", Name: "compact-patch-attempts", Value: r.CompactPatchAttempts},
		Metric{Scope: "vibedb", Name: "compact-patch-accepts", Value: r.CompactPatchAccepts},
		Metric{Scope: "vibedb", Name: "overlay-folds", Value: r.OverlayFolds},
		Metric{Scope: "vibedb", Name: "overlay-fold-attempts", Value: r.OverlayFoldAttempts},
		Metric{Scope: "vibedb", Name: "overlay-materializations", Value: r.OverlayMaterializations},
		Metric{Scope: "vibedb", Name: "overlay-materialization-failures", Value: r.OverlayMaterializationFailures},
		Metric{Scope: "vibedb", Name: "overlay-pressure-folds", Value: r.OverlayPressureFolds},
		Metric{Scope: "vibedb", Name: "overlay-snapshot-folds", Value: r.OverlaySnapshotFolds},
		Metric{Scope: "vibedb", Name: "overlay-barrier-folds", Value: r.OverlayBarrierFolds},
		Metric{Scope: "vibedb", Name: "overlay-checkpoint-folds", Value: r.OverlayCheckpointFolds},
		Metric{Scope: "vibedb", Name: "concurrent-replaces", Value: r.ConcurrentReplaces},
		Metric{Scope: "vibedb", Name: "concurrent-fallbacks", Value: r.ConcurrentFallbacks},
		Metric{Scope: "vibedb", Name: "publish-groups", Value: r.PublishGroups},
		Metric{Scope: "vibedb-high-water", Name: "publish-group-max-before", Value: r.PublishGroupMaxBefore},
		Metric{Scope: "vibedb-high-water", Name: "publish-group-max", Value: r.PublishGroupMax},
		Metric{Scope: "vibedb", Name: "automatic-checkpoints", Value: r.AutomaticCheckpoints},
		Metric{Scope: "vibedb-gauge", Name: "overlay-arena-bytes-before", Value: r.OverlayArenaBytesBefore},
		Metric{Scope: "vibedb-gauge", Name: "overlay-arena-bytes", Value: r.OverlayArenaBytes},
		Metric{Scope: "vibedb-gauge", Name: "overlay-retained-records-before", Value: r.OverlayRetainedRecordsBefore},
		Metric{Scope: "vibedb-gauge", Name: "overlay-retained-records", Value: r.OverlayRetainedRecords},
		Metric{Scope: "vibedb-gauge", Name: "overlay-dirty-buckets-before", Value: r.OverlayDirtyBucketsBefore},
		Metric{Scope: "vibedb-gauge", Name: "overlay-dirty-buckets", Value: r.OverlayDirtyBuckets},
		Metric{Scope: "vibedb-gauge", Name: "overlay-reserved-fold-bytes-before", Value: r.OverlayReservedFoldBytesBefore},
		Metric{Scope: "vibedb-gauge", Name: "overlay-reserved-fold-bytes", Value: r.OverlayReservedFoldBytes},
		Metric{Scope: "vibedb-gauge", Name: "overlay-dirty-bucket-limit", Value: r.OverlayDirtyBucketLimit},
		Metric{Scope: "vibedb-gauge", Name: "overlay-dirty-byte-limit", Value: r.OverlayDirtyByteLimit},
		Metric{Scope: "vibedb", Name: "journal-acks", Value: r.JournalAcks},
		Metric{Scope: "vibedb", Name: "chain-acks", Value: r.ChainAcks},
		Metric{Scope: "vibedb", Name: "journal-syncs", Value: r.JournalSyncs},
		Metric{Scope: "vibedb-high-water", Name: "journal-group-max-before", Value: r.JournalGroupMaxBefore},
		Metric{Scope: "vibedb-high-water", Name: "journal-group-max", Value: r.JournalGroupMax},
		Metric{Scope: "vibedb", Name: "journal-strict-syncs", Value: r.JournalStrictSyncs},
		Metric{Scope: "vibedb", Name: "journal-strict-records", Value: r.JournalStrictRecords},
		Metric{Scope: "vibedb", Name: "journal-strict-mutations", Value: r.JournalStrictMutations},
		Metric{Scope: "vibedb", Name: "journal-strict-bytes", Value: r.JournalStrictBytes},
		Metric{Scope: "vibedb", Name: "journal-delta-checkpoints", Value: r.JournalDeltaCheckpoints},
		Metric{Scope: "vibedb", Name: "journal-delta-records", Value: r.JournalDeltaRecords},
		Metric{Scope: "vibedb", Name: "journal-delta-bytes", Value: r.JournalDeltaBytes},
		Metric{Scope: "vibedb", Name: "journal-delta-fallbacks", Value: r.JournalDeltaFallbacks},
		Metric{Scope: "vibedb", Name: "device-bytes", Value: r.DeviceBytes},
		Metric{Scope: "vibedb", Name: "leaf-splits", Value: r.LeafSplits},
		Metric{Scope: "vibedb", Name: "empty-reclaims", Value: r.EmptyReclaims},
	)
	histogramNames := make([]string, 0, len(r.Histograms))
	for name := range r.Histograms {
		histogramNames = append(histogramNames, name)
	}
	sort.Strings(histogramNames)
	for _, name := range histogramNames {
		histogram := r.Histograms[name]
		metrics = append(metrics,
			Metric{Scope: "vibedb-histogram", Name: name + "-count", Value: histogram.Count},
			Metric{Scope: "vibedb-histogram", Name: name + "-sum", Value: histogram.Sum},
			Metric{Scope: "vibedb-high-water", Name: name + "-max-before", Value: histogram.MaxBefore},
			Metric{Scope: "vibedb-high-water", Name: name + "-max", Value: histogram.Max},
		)
		for bucket, value := range histogram.Buckets {
			metrics = append(metrics, Metric{
				Scope: "vibedb-histogram",
				Name:  fmt.Sprintf("%s-bucket-%02d", name, bucket),
				Value: value,
			})
		}
	}
	return metrics
}

// Write emits exactly one single-line record suitable for stderr transport.
func Write(w io.Writer, record Record) error {
	record.Schema = Schema
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s%s\n", Prefix, data)
	return err
}

// Parse extracts exactly one record while allowing unrelated human-readable
// diagnostic lines before or after it.
func Parse(src []byte) (Record, error) {
	scanner := bufio.NewScanner(bytes.NewReader(src))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var (
		record Record
		found  bool
	)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, Prefix) {
			continue
		}
		if found {
			return Record{}, errors.New("mixed child emitted multiple telemetry records")
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, Prefix)), &record); err != nil {
			return Record{}, fmt.Errorf("decode mixed telemetry: %w", err)
		}
		found = true
	}
	if err := scanner.Err(); err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, errors.New("mixed child omitted telemetry record")
	}
	if record.Schema != Schema {
		return Record{}, fmt.Errorf(
			"mixed telemetry schema = %d, want %d", record.Schema, Schema,
		)
	}
	return record, nil
}
