//go:build darwin || linux

package rf3testfixture

import (
	"strings"
	"testing"
)

func TestProcessDiagnosticPreservesReadinessAndFinalFailure(t *testing.T) {
	var diagnostic ProcessDiagnostic
	prefix := "ready\n"
	diagnostic.Write([]byte(prefix))
	diagnostic.Write([]byte(strings.Repeat("x", MaxProcessDiagnosticBytes)))
	for range 100 {
		diagnostic.Write([]byte(strings.Repeat("y", 1000)))
	}
	diagnostic.Write([]byte("final failure\n"))
	got := diagnostic.String()
	if !strings.HasPrefix(got, prefix) || !strings.HasSuffix(got, "final failure\n") || !strings.Contains(got, "[diagnostic output truncated]") {
		t.Fatal("bounded diagnostics lost startup or failure output")
	}
	if len(got) > MaxProcessDiagnosticBytes+32 {
		t.Fatalf("diagnostic budget exceeded: %d", len(got))
	}
}

func TestProcessDiagnosticSmallWritesRemainExact(t *testing.T) {
	var diagnostic ProcessDiagnostic
	text := strings.Repeat("ab", MaxProcessDiagnosticBytes/2)
	for offset := 0; offset < len(text); offset += 997 {
		diagnostic.Write([]byte(text[offset:min(len(text), offset+997)]))
	}
	if got := diagnostic.String(); got != text {
		t.Fatal("untruncated diagnostic changed bytes")
	}
}
