package kubeoperator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		!strings.Contains(script, "timeout 20s kubectl --request-timeout=5s logs") || !strings.Contains(script, "--tail=100 --limit-bytes=65536") {
		t.Fatal("failed qualification must retain bounded diagnostics and its failure status")
	}
}
