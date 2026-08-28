package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestQualificationUsesShippedRF3BatchRead(t *testing.T) {
	var request struct {
		Op             string             `json:"op"`
		Class          string             `json:"class"`
		MaxResultBytes uint32             `json:"max_result_bytes"`
		Statements     []qualifyStatement `json:"statements"`
	}
	raw := qualificationQuery()
	if err := vibejson.Unmarshal(raw, &request); err != nil || request.Op != "read_batch" ||
		request.Class != "interactive" || request.MaxResultBytes != qualificationMaxResponseBytes ||
		len(request.Statements) != 1 || request.Statements[0].SQL != "SELECT * FROM documents WHERE id = ?" ||
		len(request.Statements[0].Params) != 1 || request.Statements[0].Params[0] != (qualifyParam{Kind: "string", Text: "kind-proof"}) {
		t.Fatalf("qualification left the native RF3 exact-point path: %s (%v)", raw, err)
	}
}

func qualificationBatchResponseFixture() []byte {
	return []byte(`{"ok":true,"found":[true],"documents":[{"id":"kind-proof"}],"observations":[{"cluster_id":"01000000000000000000000000000000","cluster_incarnation":"02000000000000000000000000000000","topology_recovery_epoch":1,"shard_incarnation":"03000000000000000000000000000000","group_id":"04000000000000000000000000000000","route_id":"0500000000000000000000000000000000000000000000000000000000000000","applied":7}]}`)
}

func TestQualificationBatchResponseRequiresExactRowAndReadIndexObservation(t *testing.T) {
	valid := qualificationBatchResponseFixture()
	if !qualificationRowVisible(valid) {
		t.Fatal("valid native RF3 result rejected")
	}
	withRetries := bytes.Replace(valid, []byte(`"applied":7`), []byte(`"applied":7,"retries":2`), 1)
	if !qualificationRowVisible(withRetries) {
		t.Fatal("valid retry observation rejected")
	}
	if err := qualificationResponseError("read_batch", []byte(`{"ok":false,"code":"unauthorized","grant_digest":"private-grant"}`)); !strings.Contains(err.Error(), "unauthorized") || strings.Contains(err.Error(), "private-grant") {
		t.Fatalf("native refusal diagnostic lost or leaked data: %v", err)
	}
	for _, test := range []struct{ name, before, after string }{
		{"false-ok", `"ok":true`, `"ok":false`},
		{"not-found", `"found":[true]`, `"found":[false]`},
		{"missing-found", `"found":[true],`, ``},
		{"duplicate-found", `"found":[true]`, `"found":[true,true]`},
		{"wrong-row", `"kind-proof"`, `"other"`},
		{"extra-column", `{"id":"kind-proof"}`, `{"id":"kind-proof","extra":1}`},
		{"extra-row", `[{"id":"kind-proof"}]`, `[{"id":"kind-proof"},{"id":"kind-proof"}]`},
		{"zero-applied", `"applied":7`, `"applied":0`},
		{"missing-applied", `,"applied":7`, ``},
		{"negative-applied", `"applied":7`, `"applied":-1`},
		{"zero-epoch", `"topology_recovery_epoch":1`, `"topology_recovery_epoch":0`},
		{"malformed-route", `0500000000000000000000000000000000000000000000000000000000000000`, `garbage`},
		{"zero-route", `0500000000000000000000000000000000000000000000000000000000000000`, strings.Repeat("0", 64)},
		{"zero-group", `04000000000000000000000000000000`, strings.Repeat("0", 32)},
		{"missing-group", `"group_id":"04000000000000000000000000000000",`, ``},
		{"negative-retries", `"applied":7`, `"applied":7,"retries":-1`},
		{"global-claim", `"applied":7`, `"applied":7,"global_timestamp":7`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !bytes.Contains(valid, []byte(test.before)) {
				t.Fatal("inactive test mutation")
			}
			if qualificationRowVisible(bytes.Replace(valid, []byte(test.before), []byte(test.after), 1)) {
				t.Fatal("invalid RF3 result accepted")
			}
		})
	}
	for _, raw := range [][]byte{nil, []byte(`{"ok":true,"rows":[["kind-proof"]]}`), []byte(`{"ok":false,"code":"unauthorized"}`), valid[:len(valid)-1]} {
		if qualificationRowVisible(raw) {
			t.Fatal("non-native or malformed result accepted")
		}
	}
	for _, observations := range []string{"[]", "null", "[{},{}]"} {
		raw := []byte(`{"ok":true,"found":[true],"documents":[{"id":"kind-proof"}],"observations":` + observations + `}`)
		if qualificationRowVisible(raw) {
			t.Fatal("missing or surplus group observation accepted")
		}
	}
}
