package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestBoundedWriterRejectsRecordPastLimit(t *testing.T) {
	var output bytes.Buffer
	writer := &boundedWriter{writer: &output, left: 3}
	if _, err := writer.Write([]byte("four")); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("oversized record error = %v", err)
	}
	if output.Len() != 0 || writer.left != 3 {
		t.Fatalf("oversized record changed output: bytes=%q left=%d", output.Bytes(), writer.left)
	}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatalf("bounded record: %v", err)
	}
	if output.String() != "ok" || writer.left != 1 {
		t.Fatalf("bounded record output=%q left=%d", output.String(), writer.left)
	}
}

func TestNodeIDFromGroupUsesLocalMemberRoute(t *testing.T) {
	var domain rafttransport.TrustDomain
	domain.ClusterID[0], domain.ClusterIncarnation[0] = 1, 2
	group := manifestGroup{}
	group.Route.ClusterID = hex16(domain.ClusterID)
	group.Route.ClusterIncarnation = hex16(domain.ClusterIncarnation)
	group.Route.ShardIncarnation = hex16([16]byte{3})
	group.Route.GroupID = hex16([16]byte{4})
	group.Route.TopologyRecoveryEpoch = 1
	group.Route.MemberID = 2
	group.Route.Distribution, group.Route.Shard = "data", "all"
	group.Members = []manifestMember{{MemberID: 1, NodeID: hex16([16]byte{5})}, {MemberID: 2, NodeID: hex16([16]byte{6})}, {MemberID: 3, NodeID: hex16([16]byte{7})}}
	got, err := nodeIDFromGroup(group, domain)
	if err != nil {
		t.Fatal(err)
	}
	if got != (rafttransport.NodeID{6}) {
		t.Fatalf("local node = %x, want 06", got)
	}
}

func TestGroupFromManifestRejectsWrongTrustDomain(t *testing.T) {
	group := manifestGroup{}
	group.Route.ClusterID = hex16([16]byte{1})
	group.Route.ClusterIncarnation = hex16([16]byte{2})
	group.Route.ShardIncarnation = hex16([16]byte{3})
	group.Route.GroupID = hex16([16]byte{4})
	group.Route.TopologyRecoveryEpoch = 1
	group.Route.MemberID = 1
	group.Route.Distribution, group.Route.Shard = "data", "all"
	group.Members = []manifestMember{{MemberID: 1, NodeID: hex16([16]byte{5})}, {MemberID: 2, NodeID: hex16([16]byte{6})}, {MemberID: 3, NodeID: hex16([16]byte{7})}}
	if _, err := groupFromManifest(group, rafttransport.TrustDomain{ClusterID: [16]byte{9}, ClusterIncarnation: [16]byte{2}}); err == nil {
		t.Fatal("group accepted a mismatched trust domain")
	}
}

func TestPreflightTrackerRetainsPostReadyTransientFailures(t *testing.T) {
	tracker := preflightTracker{}
	if err := tracker.observe(1, true, ""); err != nil {
		t.Fatalf("initial ready cut: %v", err)
	}
	if err := tracker.observe(2, false, "valid status and metrics cuts 14/21"); err != nil {
		t.Fatalf("paused-node cut aborted after readiness: %v", err)
	}
	if err := tracker.observe(3, false, "valid status and metrics cuts 14/21"); err != nil {
		t.Fatalf("repeated paused-node cut aborted after readiness: %v", err)
	}
	if !tracker.satisfied {
		t.Fatal("preflight readiness was not latched")
	}
	if err := tracker.observe(4, true, ""); err != nil {
		t.Fatalf("recovered cut: %v", err)
	}
}

func TestPreflightTrackerRejectsMissingInitialCuts(t *testing.T) {
	tracker := preflightTracker{}
	for sequence := uint64(1); sequence < preflightCycles; sequence++ {
		if err := tracker.observe(sequence, false, "valid status and metrics cuts 0/21"); err != nil {
			t.Fatalf("early incomplete cut %d: %v", sequence, err)
		}
	}
	err := tracker.observe(preflightCycles, false, "valid status and metrics cuts 0/21")
	if err == nil || !strings.Contains(err.Error(), "preflight incomplete") {
		t.Fatalf("missing initial cuts error = %v", err)
	}
}

func TestLatchTrackerCapturesFirstCompletePostCONTycle(t *testing.T) {
	directory := t.TempDir()
	requestPath := filepath.Join(directory, "latch-request.json")
	outputPath := filepath.Join(directory, "post-cont-cut.json")
	requested := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	request := []byte(`{"event":"post-cont","requested_utc":"` + requested + `","node_id":"00112233445566778899aabbccddeeff","pid":42}`)
	if err := os.WriteFile(requestPath, request, 0o600); err != nil {
		t.Fatal(err)
	}
	tracker := &latchTracker{requestPath: requestPath, outputPath: outputPath, maxBytes: 8 << 20}
	if err := tracker.arm(); err != nil {
		t.Fatalf("arm: %v", err)
	}
	requestedAt, err := time.Parse(time.RFC3339Nano, requested)
	if err != nil {
		t.Fatal(err)
	}
	before := cycle{Sequence: 1, UTC: requestedAt.Add(-time.Millisecond).Format(time.RFC3339Nano), PreflightReady: true}
	if err := tracker.annotate(&before); err != nil {
		t.Fatalf("pre-request cycle: %v", err)
	}
	if before.Latch != nil {
		t.Fatal("pre-request cycle was labeled as post-CONT")
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-request artifact stat error = %v", err)
	}
	preArm := cycle{Sequence: 2, UTC: tracker.armedAt.Add(-time.Millisecond).Format(time.RFC3339Nano), PreflightReady: true}
	if err := tracker.annotate(&preArm); err != nil {
		t.Fatalf("pre-arm cycle: %v", err)
	}
	if preArm.Latch != nil {
		t.Fatal("cycle captured before latch arm was labeled as post-CONT")
	}
	incomplete := cycle{Sequence: 3, UTC: tracker.armedAt.Add(time.Millisecond).Format(time.RFC3339Nano)}
	if err := tracker.annotate(&incomplete); err != nil {
		t.Fatalf("incomplete cycle: %v", err)
	}
	if incomplete.Latch == nil || incomplete.Latch.Complete {
		t.Fatalf("incomplete latch = %+v", incomplete.Latch)
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete artifact stat error = %v", err)
	}
	complete := cycle{Sequence: 4, UTC: tracker.armedAt.Add(2 * time.Millisecond).Format(time.RFC3339Nano), PreflightReady: true}
	if err := tracker.annotate(&complete); err != nil {
		t.Fatalf("complete cycle: %v", err)
	}
	if complete.Latch == nil || !complete.Latch.Complete || complete.Latch.Sequence != complete.Sequence {
		t.Fatalf("complete latch = %+v", complete.Latch)
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var artifact latchArtifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Schema != "vibedb.rf3-diagnostic-latch/1" || artifact.Sequence != 4 ||
		artifact.NodeID != "00112233445566778899aabbccddeeff" || artifact.PID != 42 ||
		artifact.Cycle.Latch == nil || !artifact.Cycle.Latch.Complete {
		t.Fatalf("artifact = %+v", artifact)
	}
	later := cycle{Sequence: 5, UTC: tracker.armedAt.Add(3 * time.Millisecond).Format(time.RFC3339Nano), PreflightReady: true}
	if err := tracker.annotate(&later); err != nil {
		t.Fatalf("later cycle: %v", err)
	}
	var retained latchArtifact
	if err := json.Unmarshal(mustReadFile(t, outputPath), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Sequence != 4 {
		t.Fatalf("latch artifact was overwritten: sequence=%d", retained.Sequence)
	}
}

func TestReadNodeDiagnosticMapsOwnerAuthorityCounters(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "rf3-diagnostics.json")
	nodeID := "00112233445566778899aabbccddeeff"
	raw := []byte(`{"event":"snapshot","utc":"2026-09-05T18:00:00Z","serial":7,"pid":42,"node_id":"` + nodeID + `","raft_applied_entries":11,"raft_ready_persisted":12,"raft_commit_advancements":13,"raft_committed_entries":14,"authority_read_hits":15,"authority_read_index_fallbacks":16,"authority_read_validation_retries":17,"authority_read_validation_failures":18,"authority_round_attempts":19,"read_authority_rounds_started":20,"read_authority_requests_created":21,"read_authority_grants_accepted":22}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readNodeDiagnostic(path, nodeID)
	if err != nil {
		t.Fatalf("read node diagnostic: %v", err)
	}
	if got.Source != "rf3-diagnostics-file" || !got.AuthorityAvailable || got.PID != 42 || got.Serial != 7 ||
		got.Metrics == nil || uint64PointerValue(got.Metrics.AuthorityReadHits) != 15 ||
		uint64PointerValue(got.Metrics.AuthorityReadValidationFailures) != 18 ||
		uint64PointerValue(got.Metrics.ReadAuthorityRequestsCreated) != 21 ||
		uint64PointerValue(got.Metrics.ReadAuthorityGrantsAccepted) != 22 {
		t.Fatalf("node diagnostic = %+v", got)
	}
}

func TestReadNodeDiagnosticRejectsMissingAuthorityCounter(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "rf3-diagnostics.json")
	nodeID := "00112233445566778899aabbccddeeff"
	raw := []byte(`{"event":"snapshot","utc":"2026-09-05T18:00:00Z","serial":7,"pid":42,"node_id":"` + nodeID + `","raft_applied_entries":11}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNodeDiagnostic(path, nodeID); err == nil || !strings.Contains(err.Error(), "authority counter") {
		t.Fatalf("missing authority counter error = %v", err)
	}
}

func TestNodeMetricsOmitsUnavailableAuthorityCounters(t *testing.T) {
	value := nodeMetricsSnapshot{
		NodeID: "00112233445566778899aabbccddeeff", Scope: "node_process",
		Source: "servicemetrics", AuthorityAvailable: false,
		Metrics: &nodeMetrics{AppliedEntries: 11},
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "authority_read_hits") ||
		strings.Contains(string(raw), "read_authority_rounds_started") {
		t.Fatalf("unavailable authority counters were serialized as zero: %s", raw)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func uint64PointerValue(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func hex16(value [16]byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, 32)
	for index, part := range value {
		result[index*2] = digits[part>>4]
		result[index*2+1] = digits[part&15]
	}
	return string(result)
}
