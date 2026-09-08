//go:build linux

package gatewayruntime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/shardservice"
)

const (
	durableRF3MultiRelationDiagnosticMarker  = "VIBEDB_RF3_DIAGNOSTIC "
	durableRF3MultiRelationDiagnosticWait    = 750 * time.Millisecond
	durableRF3MultiRelationDiagnosticProbe   = 250 * time.Millisecond
	durableRF3MultiRelationDiagnosticTailMax = 64 << 10
)

// durableRF3MultiRelationTrace records only the request currently in flight.
// It is deliberately test-side state: it gives a failed external process run a
// stage and original deadline without changing the request wire or timeout.
type durableRF3MultiRelationTrace struct {
	ctx     context.Context
	stage   string
	request string
	started time.Time
}

func (trace *durableRF3MultiRelationTrace) set(ctx context.Context, stage, request string) {
	trace.ctx, trace.stage, trace.request, trace.started = ctx, stage, request, time.Now()
}

func (trace *durableRF3MultiRelationTrace) contextRemaining() time.Duration {
	if trace == nil || trace.ctx == nil {
		return 0
	}
	deadline, ok := trace.ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(deadline)
}

func (trace *durableRF3MultiRelationTrace) log(
	t testing.TB, fixture *durableRF3ExternalFixture,
) {
	t.Helper()
	if trace == nil {
		return
	}
	stageElapsed := time.Duration(0)
	if !trace.started.IsZero() {
		stageElapsed = time.Since(trace.started)
	}
	t.Logf("RF3_DIAG unexpected_outcome stage=%q request=%q fixture_context_remaining=%s stage_elapsed=%s targets=%s",
		trace.stage, trace.request, trace.contextRemaining(), stageElapsed,
		durableRF3MultiRelationTargets(fixture))
}

func durableRF3GroupDiagnosticIdentity(group raftmember.GroupKey) string {
	return fmt.Sprintf("cluster_id=%x cluster_incarnation=%x topology_recovery_epoch=%d shard_incarnation=%x group_id=%x",
		group.ClusterID, group.ClusterIncarnation, group.TopologyRecoveryEpoch,
		group.ShardIncarnation, group.GroupID)
}

func durableRF3MultiRelationTargets(fixture *durableRF3ExternalFixture) string {
	if fixture == nil {
		return "unavailable"
	}
	var result strings.Builder
	for group := 0; group < durableRF3ExternalGroups; group++ {
		if result.Len() != 0 {
			result.WriteString(";")
		}
		route := fixture.routes[group]
		fmt.Fprintf(&result, "role=%s %s", durableRF3ExternalRoleNames[group],
			durableRF3GroupDiagnosticIdentity(route.Group))
		for member := 0; member < durableRF3ExternalVoters; member++ {
			fmt.Fprintf(&result, " member%d_native=%s", member+1, route.Replicas[member].NativeEndpoint)
		}
	}
	return result.String()
}

func (fixture *durableRF3ExternalFixture) logRouteLeaderCut(
	t testing.TB,
	group int,
	cut string,
	states [durableRF3ExternalVoters]shardservice.ReplicatedMemberState,
	errors [durableRF3ExternalVoters]error,
	excluded int,
) {
	if fixture == nil || !fixture.diagnosticCuts {
		return
	}
	for member := 0; member < durableRF3ExternalVoters; member++ {
		if member == excluded {
			continue
		}
		fixture.logRouteLeaderObservation(t, group, member, cut, states[member], errors[member])
	}
}

func (fixture *durableRF3ExternalFixture) logRouteLeaderObservation(
	t testing.TB,
	group, member int, cut string,
	state shardservice.ReplicatedMemberState,
	probeErr error,
) {
	if fixture == nil || !fixture.diagnosticCuts {
		return
	}
	native := "unavailable"
	groupKey := raftmember.GroupKey{}
	if group >= 0 && group < durableRF3ExternalGroups {
		route := fixture.routes[group]
		groupKey = route.Group
		if member >= 0 && member < len(route.Replicas) {
			native = route.Replicas[member].NativeEndpoint
		}
	}
	probeError := errorsTruncate(probeErr, durableRF3ExternalRetryDiagnosticBytes)
	t.Logf("RF3_DIAG cut=%s role=%s group={%s} member=%d native_endpoint=%s term=%d leader=%d commit=%d applied=%d checkpoint_applied=%d state_member=%d state_node_incarnation=%d probe_error=%q",
		cut, durableRF3ExternalRoleNames[group], durableRF3GroupDiagnosticIdentity(groupKey),
		member+1, native, state.Fence.Term, state.LeaderID, state.Commit, state.Applied,
		state.CheckpointApplied, state.Fence.MemberID, state.Fence.NodeIncarnation, probeError)
}

func errorsTruncate(err error, maximum int) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if maximum > 0 && len(text) > maximum {
		return text[:maximum] + "..."
	}
	return text
}

func (fixture *durableRF3ExternalFixture) captureDiagnosticSnapshots(
	t testing.TB, cut string,
) {
	t.Helper()
	if fixture == nil || !fixture.diagnosticCuts {
		return
	}
	for member, process := range fixture.shards {
		before := strings.Count(process.Diagnostics(), durableRF3MultiRelationDiagnosticMarker)
		pid := process.PID()
		if pid == 0 {
			t.Logf("RF3_DIAG cut=%s member=%d diagnostic_snapshot=process-not-running", cut, member+1)
			continue
		}
		child, err := os.FindProcess(pid)
		if err != nil {
			t.Logf("RF3_DIAG cut=%s member=%d diagnostic_snapshot=find-process-error=%q", cut, member+1, err)
			continue
		}
		if err := child.Signal(syscall.SIGUSR1); err != nil {
			t.Logf("RF3_DIAG cut=%s member=%d pid=%d diagnostic_snapshot=signal-error=%q", cut, member+1, pid, err)
			continue
		}
		deadline := time.Now().Add(durableRF3MultiRelationDiagnosticWait)
		var diagnostics string
		record := "unavailable"
		observed := false
		for time.Now().Before(deadline) {
			diagnostics = process.Diagnostics()
			if strings.Count(diagnostics, durableRF3MultiRelationDiagnosticMarker) > before {
				if complete, ok := durableRF3CompleteDiagnosticRecord(diagnostics); ok {
					record, observed = complete, true
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		if diagnostics == "" {
			diagnostics = process.Diagnostics()
		}
		t.Logf("RF3_DIAG cut=%s member=%d pid=%d diagnostic_snapshot_observed=%t diagnostic_snapshot=%s",
			cut, member+1, pid, observed, record)
	}
}

func durableRF3CompleteDiagnosticRecord(diagnostics string) (string, bool) {
	index := strings.LastIndex(diagnostics, durableRF3MultiRelationDiagnosticMarker)
	if index < 0 {
		return "", false
	}
	record := diagnostics[index:]
	if end := strings.IndexByte(record, '\n'); end >= 0 {
		record = record[:end]
	} else {
		return "", false
	}
	if len(record) > durableRF3MultiRelationDiagnosticTailMax {
		record = record[:durableRF3MultiRelationDiagnosticTailMax] + "...[diagnostic record truncated]"
	}
	return strings.TrimSpace(record), true
}

func (fixture *durableRF3ExternalFixture) preserveCurrentBootDiagnostics(
	t testing.TB, member int, cut string,
) {
	t.Helper()
	if fixture == nil || member < 0 || member >= len(fixture.shards) {
		return
	}
	process := fixture.shards[member]
	pid := process.PID()
	diagnostics := process.Diagnostics()
	truncated := false
	if len(diagnostics) > durableRF3MultiRelationDiagnosticTailMax {
		truncated = true
		diagnostics = diagnostics[len(diagnostics)-durableRF3MultiRelationDiagnosticTailMax:]
	}
	t.Logf("RF3_DIAG cut=%s member=%d pid=%d retained_log_truncated=%t retained_log_tail=\n%s",
		cut, member+1, pid, truncated, diagnostics)
}

func (fixture *durableRF3ExternalFixture) captureUnexpectedMultiRelationFailure(
	t testing.TB, trace *durableRF3MultiRelationTrace,
) {
	t.Helper()
	if fixture == nil || !fixture.diagnosticCuts {
		return
	}
	trace.log(t, fixture)
	fixture.captureDiagnosticSnapshots(t, "unexpected-outcome")
	for member := range fixture.shards {
		fixture.preserveCurrentBootDiagnostics(t, member, "unexpected-outcome")
	}
	fixture.captureDiagnosticProbeCut(t, "unexpected-outcome", durableRF3MultiRelationDiagnosticProbe)
}

func (fixture *durableRF3ExternalFixture) captureDiagnosticProbeCut(
	t testing.TB, cut string, timeout time.Duration,
) {
	t.Helper()
	if fixture == nil || !fixture.diagnosticCuts || fixture.probeClient == nil {
		return
	}
	for group := 0; group < durableRF3ExternalGroups; group++ {
		for member := 0; member < durableRF3ExternalVoters; member++ {
			state, err := fixture.probeMemberWithin(group, member, false, timeout)
			fixture.logRouteLeaderObservation(t, group, member, cut, state, err)
		}
	}
}
