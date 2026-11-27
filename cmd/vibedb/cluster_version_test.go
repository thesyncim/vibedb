package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestClusterVersionCLI(t *testing.T) {
	// 1. JSON output of local version window.
	var out, errBuf bytes.Buffer
	if code := runClusterVersionTo([]string{"--json"}, &out, &errBuf); code != 0 {
		t.Fatalf("runClusterVersionTo(--json) = %d, stderr=%q", code, errBuf.String())
	}
	var report clusterVersionReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("unmarshal report: %v (raw=%q)", err, out.String())
	}
	if report.Current != "1.1" || report.MinSupported != "1.0" || len(report.Features) == 0 {
		t.Fatalf("unexpected local report: %+v", report)
	}

	// 2. Compatible mixed-version rolling upgrade check (1.0 peer + 1.0..1.1 peer, active 1.0).
	out.Reset()
	errBuf.Reset()
	if code := runClusterVersionTo([]string{"--json", "--check-peers", "1.0,1.0..1.1", "--active", "1.0"}, &out, &errBuf); code != 0 {
		t.Fatalf("compatible check = %d, stdout=%q stderr=%q", code, out.String(), errBuf.String())
	}
	if !strings.Contains(out.String(), `"compatible": true`) || !strings.Contains(out.String(), `"negotiated_max": "1.0"`) {
		t.Fatalf("unexpected compatible JSON: %s", out.String())
	}

	// 3. Incompatible peer (2.0) fails closed with exit code 1.
	out.Reset()
	errBuf.Reset()
	if code := runClusterVersionTo([]string{"--check-peers", "2.0"}, &out, &errBuf); code != 1 {
		t.Fatalf("incompatible check = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "compatible:    false") {
		t.Fatalf("expected failure diagnostic on stderr, got %q", errBuf.String())
	}
}
