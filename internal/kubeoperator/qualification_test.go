package kubeoperator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKindQualificationIsolatesDiscoveryAndCleanup(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "kubernetes", "qualify-kind.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	create := strings.Index(script, "kind create cluster --name")
	discover := strings.Index(script, "kubectl apply --dry-run=client")
	if create < 0 || discover <= create {
		t.Fatal("kubectl discovery must run after creating the API server")
	}
	if !strings.Contains(script, `export KUBECONFIG="${work_dir}/kubeconfig"`) {
		t.Fatal("qualification must not use or modify the ambient kubeconfig")
	}
	check := strings.Index(script, "if kind get clusters")
	claim := strings.Index(script, "cluster_created=true")
	if check < 0 || claim <= check || claim >= create || !strings.Contains(script, `if [[ "${cluster_created}" == true ]]; then`+"\n"+`    kind delete cluster --name`) {
		t.Fatal("cleanup must own the new cluster, never a preexisting one")
	}
	if !strings.Contains(script, `local exit_status=$?`) || !strings.Contains(script, `return "${exit_status}"`) ||
		!strings.Contains(script, "timeout --kill-after=1s 20s kubectl --request-timeout=5s logs") || !strings.Contains(script, "--tail=100 --limit-bytes=65536") {
		t.Fatal("failed qualification must retain bounded diagnostics and its failure status")
	}
}

func TestKindFailureEvidenceRetainsEveryPreviousShard(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "kubernetes", "qualify-kind.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	start, end := strings.Index(script, "collect_failure_evidence() {"), strings.Index(script, "\ncleanup() {")
	if start < 0 || end <= start {
		t.Fatal("missing bounded failure collector")
	}
	collector := script[start:end]
	for _, required := range []string{"timeout --kill-after=1s 10s", "timeout --kill-after=1s 3s kubectl --request-timeout=2s logs", "--previous --tail=100 --limit-bytes=65536", "lastState.terminated.reason", "lastState.terminated.exitCode", "lastState.terminated.signal"} {
		if !strings.Contains(collector, required) {
			t.Fatalf("failure collector lost %q", required)
		}
	}
	// Run only the failure collector with fake commands: no cluster, ambient
	// kubeconfig, deployment, or cleanup side effects. An absent previous log
	// and an overlong previous log must not hide any of the other eight pods.
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", "-c", `set -euo pipefail
evidence_dir=$1
namespace=vibedb-test
timeout() { shift 2; "$@"; }
kubectl() {
  printf '%s\n' "$*" >> "${evidence_dir}/calls.txt"
  if [[ "$*" == *--previous* ]]; then
    if [[ "$*" == *vibedb-catalog-0* ]]; then
      printf 'no previous instance\n' >&2
      return 1
    fi
    if [[ "$*" == *vibedb-ledger-1* ]]; then
      printf '%070000d' 0
      return
    fi
    printf 'previous process log\n'
  else
    printf 'bounded current evidence\n'
  fi
}
`+collector+"\ncollect_failure_evidence\n", "kind-evidence-test", root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("failure collector: %v %s", err, output)
	}
	calls, err := os.ReadFile(filepath.Join(root, "calls.txt"))
	if err != nil || strings.Count(string(calls), "--previous") != 9 {
		t.Fatalf("must visit all nine previous shard instances: %v %s", err, calls)
	}
	previous, err := os.ReadFile(filepath.Join(root, "failed-previous-shards.txt"))
	if err != nil || len(previous) > 9*(65536+64) || strings.Count(string(previous), " previous shard ===") != 9 ||
		!strings.Contains(string(previous), "vibedb-data-2 previous shard") || !strings.Contains(string(previous), "no previous instance") {
		t.Fatalf("bounded complete previous evidence: bytes=%d err=%v", len(previous), err)
	}
	if _, err := os.Stat(filepath.Join(root, "failed-terminations.txt")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, `if [[ "${exit_status}" -ne 0 && "${cluster_created}" == true ]]; then`+"\n    collect_failure_evidence") {
		t.Fatal("failure collector must not run on successful qualification")
	}
}
