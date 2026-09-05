package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const diagnosticLimit = 1 << 20

type diagnosticTarget struct {
	NodeID     string `json:"node_id"`
	PID        int    `json:"pid"`
	Path       string `json:"path"`
	Executable string `json:"executable"`
}

type diagnosticRecord struct {
	NodeID   string          `json:"node_id"`
	PID      int             `json:"pid"`
	Serial   uint64          `json:"serial"`
	File     string          `json:"file"`
	SHA256   string          `json:"sha256"`
	Snapshot json.RawMessage `json:"snapshot"`
}

type diagnosticDelta struct {
	NodeID                      string            `json:"node_id"`
	Counters                    map[string]uint64 `json:"counters"`
	Histogram                   []uint64          `json:"ready_wave_group_histogram"`
	ProposalQueueDepthHistogram []uint64          `json:"raft_proposal_queue_depth_histogram"`
	ProposalEntriesPerReady     []uint64          `json:"raft_proposal_entries_per_ready"`
	ProposalBytesPerReady       []uint64          `json:"raft_proposal_bytes_per_ready"`
}

type diagnosticBracket struct {
	Before                  []diagnosticRecord `json:"before"`
	After                   []diagnosticRecord `json:"after"`
	Deltas                  []diagnosticDelta  `json:"deltas"`
	BeforeCompletedOffsetNS int64              `json:"before_completed_offset_ns"`
	AfterStartedOffsetNS    int64              `json:"after_started_offset_ns"`
}

type diagnosticControl struct {
	targets   []diagnosticTarget
	directory string
	check     func(diagnosticTarget) error
	signal    func(diagnosticTarget) error
}

var diagnosticCounters = []string{
	"ready_waves", "ready_durable_waves", "observed_append_barriers", "multi_group_waves", "failed_waves",
	"ready_submissions", "ready_queue_wait_ns", "ready_waves_attempted", "ready_persist_attempts",
	"ready_persist_successes", "ready_persist_failures", "ready_waves_failed", "ready_persist_duration_ns", "ready_wave_duration_ns",
	"ready_logical_batches", "ready_series_submissions", "ready_singleton_series_submissions", "ready_multi_series_submissions",
	"ready_durable_logical_batches", "ready_durable_series_submissions",
	"checkpoint_queue_submissions", "checkpoint_queue_rejected", "checkpoint_queue_wait_ns", "checkpoint_service_ns",
	"native_accepted", "native_rejected", "native_failed", "native_semantic_dispatches",
	"gateway_local_calls", "gateway_remote_calls", "gateway_semantic_sql_calls", "gateway_legacy_calls",
	"gateway_sql_request_encodings", "gateway_sql_request_encoded_bytes", "remote_dials", "remote_reuses",
	"remote_poisoned", "remote_rejected", "remote_handshake_failures",
	"raft_proposal_batches", "raft_proposal_commands", "raft_proposal_bytes", "raft_apply_batches",
	"raft_applied_entries", "raft_commit_advancements", "raft_committed_entries", "raft_ready_persisted",
	"raft_proposal_window_queued", "raft_late_join_used", "raft_late_join_missed", "raft_late_join_entries",
}

func readBounded(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > diagnosticLimit {
		return nil, fmt.Errorf("diagnostic file is not a bounded regular file")
	}
	data := make([]byte, info.Size())
	if _, err := file.ReadAt(data, 0); err != nil {
		return nil, err
	}
	return data, nil
}

func loadDiagnosticControl(path, directory string) (*diagnosticControl, error) {
	raw, err := readBounded(path)
	if err != nil {
		return nil, err
	}
	var targets []diagnosticTarget
	if err := json.Unmarshal(raw, &targets); err != nil {
		return nil, err
	}
	if len(targets) != 3 && len(targets) != 6 {
		return nil, fmt.Errorf("diagnostics require exactly three or six ready candidate nodes")
	}
	nodes, pids, paths := map[string]bool{}, map[int]bool{}, map[string]bool{}
	for _, target := range targets {
		node, err := hex.DecodeString(target.NodeID)
		if err != nil || len(node) != 16 || strings.ToLower(target.NodeID) != target.NodeID || target.PID <= 1 ||
			target.Executable != "/bench/candidate-vibedb-shard" || !filepath.IsAbs(target.Path) ||
			filepath.Base(target.Path) != "rf3-diagnostics.json" || nodes[target.NodeID] || pids[target.PID] || paths[target.Path] {
			return nil, fmt.Errorf("invalid or duplicate candidate diagnostic binding")
		}
		nodes[target.NodeID], pids[target.PID], paths[target.Path] = true, true, true
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	return &diagnosticControl{targets: targets, directory: directory,
		check: func(target diagnosticTarget) error {
			executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", target.PID))
			if err != nil || executable != target.Executable {
				return fmt.Errorf("candidate diagnostic PID %d no longer has the expected executable", target.PID)
			}
			return nil
		},
		signal: func(target diagnosticTarget) error {
			signal := rf3DiagnosticSignal()
			if signal == nil {
				return fmt.Errorf("diagnostic signalling is unavailable on this platform")
			}
			process, err := os.FindProcess(target.PID)
			if err != nil {
				return err
			}
			return process.Signal(signal)
		}}, nil
}

func decodeDiagnostic(raw []byte) (diagnosticRecord, error) {
	var header struct {
		NodeID string `json:"node_id"`
		PID    int    `json:"pid"`
		Serial uint64 `json:"serial"`
		UTC    string `json:"utc"`
		Event  string `json:"event"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return diagnosticRecord{}, err
	}
	if _, err := time.Parse(time.RFC3339Nano, header.UTC); err != nil || header.Event != "snapshot" || header.Serial == 0 || header.PID <= 1 {
		return diagnosticRecord{}, fmt.Errorf("invalid diagnostic snapshot header")
	}
	return diagnosticRecord{NodeID: header.NodeID, PID: header.PID, Serial: header.Serial, Snapshot: raw}, nil
}

func (control *diagnosticControl) capture(ctx context.Context, workload string, clients, repetition int, phase string) ([]diagnosticRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	prior := make([]uint64, len(control.targets))
	for index, target := range control.targets {
		if err := control.check(target); err != nil {
			return nil, err
		}
		raw, err := readBounded(target.Path)
		if err == nil {
			previous, err := decodeDiagnostic(raw)
			if err != nil {
				return nil, err
			}
			if previous.PID == target.PID && previous.NodeID == target.NodeID {
				prior[index] = previous.Serial
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	// Check every binding before signalling any node. Never send this signal
	// to a legacy parent process, which has no registered SIGUSR1 handler.
	for _, target := range control.targets {
		if err := control.check(target); err != nil {
			return nil, err
		}
		if err := control.signal(target); err != nil {
			return nil, err
		}
	}
	records := make([]diagnosticRecord, 0, len(control.targets))
	for index, target := range control.targets {
		for {
			raw, err := readBounded(target.Path)
			if err == nil {
				record, err := decodeDiagnostic(raw)
				if err != nil {
					return records, err
				}
				if record.PID == target.PID && record.NodeID == target.NodeID && record.Serial > prior[index] {
					if err := control.check(target); err != nil {
						return records, err
					}
					name := fmt.Sprintf("%s-c%d-r%d-node%d-%s.json", workload, clients, repetition, index, phase)
					file, err := os.OpenFile(filepath.Join(control.directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
					if err != nil {
						return records, err
					}
					_, writeErr := file.Write(raw)
					syncErr := file.Sync()
					closeErr := file.Close()
					if writeErr != nil || syncErr != nil || closeErr != nil {
						return records, fmt.Errorf("archive diagnostic: write=%v sync=%v close=%v", writeErr, syncErr, closeErr)
					}
					digest := sha256.Sum256(raw)
					record.SHA256 = hex.EncodeToString(digest[:])
					record.File = filepath.Join("diagnostics", name)
					records = append(records, record)
					break
				}
			} else if !os.IsNotExist(err) {
				return records, err
			}
			select {
			case <-ctx.Done():
				return records, fmt.Errorf("diagnostic acknowledgement for node %s PID %d: %w", target.NodeID, target.PID, ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	return records, nil
}

func diagnosticDeltas(before, after []diagnosticRecord) ([]diagnosticDelta, error) {
	if len(before) != len(after) {
		return nil, fmt.Errorf("incomplete diagnostic bracket")
	}
	deltas := make([]diagnosticDelta, 0, len(before))
	for index, first := range before {
		last := after[index]
		if first.NodeID != last.NodeID || first.PID != last.PID || first.Serial >= last.Serial {
			return nil, fmt.Errorf("diagnostic process identity or serial changed")
		}
		var a, b map[string]json.RawMessage
		if err := json.Unmarshal(first.Snapshot, &a); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(last.Snapshot, &b); err != nil {
			return nil, err
		}
		delta := diagnosticDelta{NodeID: first.NodeID, Counters: map[string]uint64{}}
		for _, key := range diagnosticCounters {
			var low, high uint64
			if json.Unmarshal(a[key], &low) != nil || json.Unmarshal(b[key], &high) != nil || high < low {
				return nil, fmt.Errorf("diagnostic counter %s is missing or decreased", key)
			}
			delta.Counters[key] = high - low
		}
		decodeHistogram := func(key string) ([]uint64, error) {
			var low, high []uint64
			if json.Unmarshal(a[key], &low) != nil || json.Unmarshal(b[key], &high) != nil ||
				len(low) != len(high) || len(low) < 2 {
				return nil, fmt.Errorf("invalid diagnostic histogram %s", key)
			}
			delta := make([]uint64, len(low))
			for index := range low {
				if high[index] < low[index] {
					return nil, fmt.Errorf("diagnostic histogram decreased")
				}
				delta[index] = high[index] - low[index]
			}
			return delta, nil
		}
		var err error
		if delta.Histogram, err = decodeHistogram("ready_wave_group_histogram"); err != nil {
			return nil, err
		}
		if delta.ProposalQueueDepthHistogram, err = decodeHistogram("raft_proposal_queue_depth_histogram"); err != nil {
			return nil, err
		}
		if delta.ProposalEntriesPerReady, err = decodeHistogram("raft_proposal_entries_per_ready"); err != nil {
			return nil, err
		}
		if delta.ProposalBytesPerReady, err = decodeHistogram("raft_proposal_bytes_per_ready"); err != nil {
			return nil, err
		}
		deltas = append(deltas, delta)
	}
	return deltas, nil
}
