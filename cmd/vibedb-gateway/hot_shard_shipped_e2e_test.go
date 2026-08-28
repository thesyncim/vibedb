package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

const (
	gatewayHotShardProcessMode = "VIBEDB_GATEWAY_HOT_SHARD_PROCESS_MODE"
	gatewayHotShardProcessLog  = "VIBEDB_GATEWAY_HOT_SHARD_PROCESS_LOG"
	gatewayHotShardCrashExit   = 86
	gatewayHotShardPartitions  = 64
)

// TestGatewayHotShardExternalProcessCrashPartition runs the shipped command
// composition on both sides of real process exits. The partition process
// retries one replicated pressure cut under sustained submission failure; the
// crash process exits after recording the operation but before returning an
// outcome; a fresh process must submit the same operation ID successfully.
func TestGatewayHotShardExternalProcessCrashPartition(t *testing.T) {
	if os.Getenv(gatewayHotShardProcessMode) != "" {
		return
	}
	journal := t.TempDir() + "/operations"
	run := func(mode string, wantExit int) {
		t.Helper()
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(executable,
			"-test.run=^TestGatewayHotShardOperationProcessHelper$", "-test.count=1")
		command.Env = append(os.Environ(), gatewayHotShardProcessMode+"="+mode,
			gatewayHotShardProcessLog+"="+journal)
		output, err := command.CombinedOutput()
		if wantExit == 0 && err != nil {
			t.Fatalf("%s process: %v\n%s", mode, err, output)
		}
		if wantExit != 0 {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != wantExit {
				t.Fatalf("%s process exit=%v want=%d\n%s", mode, err, wantExit, output)
			}
		}
	}
	run("partition", 0)
	run("crash", gatewayHotShardCrashExit)
	run("recover", 0)

	raw, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != gatewayHotShardPartitions+2 {
		t.Fatalf("operation attempts=%d want=%d", len(lines), gatewayHotShardPartitions+2)
	}
	var operation [32]byte
	counts := map[string]int{}
	for index, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("operation line %d = %q", index, line)
		}
		decoded, err := hex.DecodeString(fields[1])
		if err != nil || len(decoded) != len(operation) {
			t.Fatalf("operation line %d id=%q err=%v", index, fields[1], err)
		}
		if index == 0 {
			copy(operation[:], decoded)
		} else if string(operation[:]) != string(decoded) {
			t.Fatalf("operation changed across process fault: first=%x attempt=%x", operation, decoded)
		}
		counts[fields[0]]++
	}
	if operation == ([32]byte{}) || counts["partition"] != gatewayHotShardPartitions ||
		counts["crash"] != 1 || counts["recover"] != 1 {
		t.Fatalf("operation=%x process attempts=%v", operation, counts)
	}
}

func TestGatewayHotShardOperationProcessHelper(t *testing.T) {
	mode := os.Getenv(gatewayHotShardProcessMode)
	if mode == "" {
		return
	}
	path := os.Getenv(gatewayHotShardProcessLog)
	if path == "" || mode != "partition" && mode != "crash" && mode != "recover" {
		t.Fatal("invalid process helper configuration")
	}
	catalog, record, observations := gatewayHotShardMoveFixture(t)
	controller, err := hotshard.New(hotshard.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	submitter := gatewayHotShardProcessSubmitter{path: path, mode: mode}
	runtime := &gatewayHotShardRuntime{authority: &gatewayHotShardTestAuthority{record: record},
		controller: controller, operationsBound: true,
		operations: gatewayHotShardOperationAuthorities{
			moves:   gatewayHotReplicaMoveFactory{observations: observations, grants: observations},
			moveRun: submitter,
		}}
	attempts := 1
	if mode == "partition" {
		attempts = gatewayHotShardPartitions
	}
	for attempt := 0; attempt < attempts; attempt++ {
		pass, err := runtime.runPressurePass(context.Background(), catalog)
		if mode == "partition" {
			if err == nil || pass.Admission.MoveCount != 1 {
				t.Fatalf("partition attempt %d pass=%+v err=%v", attempt, pass, err)
			}
			continue
		}
		if err != nil || pass.Admission.MoveCount != 1 {
			t.Fatalf("%s pass=%+v err=%v", mode, pass, err)
		}
	}
}

type gatewayHotShardProcessSubmitter struct {
	path string
	mode string
}

func (submitter gatewayHotShardProcessSubmitter) Submit(
	_ context.Context, plan *rebalance.Plan,
) (rebalance.Action, error) {
	if plan == nil {
		return rebalance.Action{}, errors.New("nil replica-move plan")
	}
	file, err := os.OpenFile(submitter.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return rebalance.Action{}, err
	}
	_, writeErr := fmt.Fprintf(file, "%s %x\n", submitter.mode, plan.OperationID())
	syncErr := file.Sync()
	closeErr := file.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return rebalance.Action{}, err
	}
	if submitter.mode == "crash" {
		os.Exit(gatewayHotShardCrashExit)
	}
	if submitter.mode == "partition" {
		return rebalance.Action{}, errors.New("injected operation transport partition")
	}
	return rebalance.Action{Kind: rebalance.ActionAddLearner}, nil
}

func TestGatewayHotShardForegroundP99Overhead(t *testing.T) {
	runtime, observation := gatewayHotShardForegroundFixture(t)
	for range 1_024 {
		runtime.ObservePressure(observation)
	}
	if allocations := testing.AllocsPerRun(2_000, func() {
		runtime.ObservePressure(observation)
	}); allocations != 0 {
		t.Fatalf("foreground pressure allocations/op=%f", allocations)
	}

	const samples, batch = 256, 256
	latencies := make([]time.Duration, samples)
	for sample := range latencies {
		started := time.Now()
		for range batch {
			runtime.ObservePressure(observation)
		}
		latencies[sample] = time.Since(started) / batch
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p99 := latencies[(len(latencies)*99+99)/100-1]
	const maximumP99 = 25 * time.Microsecond
	if p99 > maximumP99 {
		t.Fatalf("foreground hot-shard p99=%s exceeds gate=%s", p99, maximumP99)
	}
	t.Logf("foreground hot-shard p99=%s allocations/op=0", p99)
}

func BenchmarkGatewayHotShardForegroundPressure(b *testing.B) {
	runtime, observation := gatewayHotShardForegroundFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runtime.ObservePressure(observation)
	}
}

func gatewayHotShardForegroundFixture(t testing.TB) (*gatewayHotShardRuntime, gateway.PressureObservation) {
	t.Helper()
	catalog, record, _ := gatewayHotShardMoveFixture(t)
	view, err := hotshard.OpenView(record.Payload)
	if err != nil || len(view.Reports) != 1 {
		t.Fatalf("pressure view reports=%d err=%v", len(view.Reports), err)
	}
	capacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 1_000_000
	}
	collector, err := hotshard.NewCollector(catalog, 2, autosplit.DefaultRecorderLanes,
		hotshard.StaticCapacityProvider{Capacity: capacity, MigrationBytes: 1},
		autosplit.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	runtime := &gatewayHotShardRuntime{}
	runtime.collector.Store(collector)
	return runtime, gateway.PressureObservation{Source: view.Reports[0].Recommendation.Source,
		AccessScopes: []distributedtxn.IntentScope{{Start: 7, End: 8}}, Write: true}
}
