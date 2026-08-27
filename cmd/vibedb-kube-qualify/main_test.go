package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestQualificationFailureReportsBoundedStageWithoutAuthorityPayload(t *testing.T) {
	err := qualificationResponseError("issuer_open", []byte(`{"error":"shard unavailable","grant_digest":"private-grant","rows":[["private-row"]]}`))
	if !strings.Contains(err.Error(), "issuer_open") || !strings.Contains(err.Error(), "shard unavailable") ||
		strings.Contains(err.Error(), "private-") {
		t.Fatalf("unsafe or missing failure detail: %v", err)
	}
	raw, marshalErr := vibejson.Marshal(&issuerOpenResponse{Error: strings.Repeat("x", 4096)})
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if err := qualificationResponseError("query", raw); len(err.Error()) > 350 {
		t.Fatalf("unbounded failure message: %d", len(err.Error()))
	}
	if err := qualificationResponseError("exec_batch", []byte(`{"grant_digest":"private-grant"}`)); !strings.Contains(err.Error(), "unexpected response") || strings.Contains(err.Error(), "private-grant") {
		t.Fatalf("unsafe unrecognized response: %v", err)
	}
}

func TestQualificationWireContractsUseVibeJSON(t *testing.T) {
	var grant issuerOpenResponse
	if err := vibejson.Unmarshal([]byte(`{"ok":true,"op":"issuer_open","installation_id":"81000000000000000000000000000000","issuer_epoch":1,"lane_ordinal":0,"grant_digest":"0300000000000000000000000000000000000000000000000000000000000000"}`), &grant); err != nil || !grant.OK {
		t.Fatalf("real issuer response: %+v %v", grant, err)
	}
	query := qualificationQuery()
	if !vibejson.Valid(query) || !bytes.Contains(query, []byte(`"kind-proof"`)) ||
		qualificationRowVisible([]byte(`{"ok":true,"rows":[["other"]]}`)) ||
		!qualificationRowVisible([]byte(`{"ok":true,"rows":[["kind-proof"]]}`)) ||
		qualificationRowVisible([]byte(`{"ok":true,"rows":[["kind-proof"],["kind-proof"]]}`)) {
		t.Fatalf("query/response contract query=%s", query)
	}
	if !committedResponse([]byte(`{"committed":true}`)) ||
		committedResponse([]byte(`{"committed":false}`)) ||
		committedResponse([]byte(`{"ok":true}`)) {
		t.Fatal("commit response contract")
	}
}

func TestQualificationOptionsAreBounded(t *testing.T) {
	base := []string{"-certificate=c", "-key=k", "-roots=r",
		"-gateway-node=11000000000000000000000000000000", "-state=" + filepath.Join(t.TempDir(), "state")}
	if _, err := parseClientOptions("verify", base); err != nil {
		t.Fatal(err)
	}
	for _, arguments := range [][]string{
		append(append([]string(nil), base...), "-samples=4097"),
		append(append([]string(nil), base...), "-max-p99=6s", "-max-latency=5s"),
		{"-certificate=c", "-key=k", "-roots=r", "-gateway-node=11", "-state=/tmp/state"},
		{"-certificate=c", "-key=k", "-roots=r", "-gateway-node=11000000000000000000000000000000", "-state=relative"},
	} {
		if _, err := parseClientOptions("verify", arguments); err == nil {
			t.Fatalf("accepted options=%q", arguments)
		}
	}
}

func TestQualificationDNSNamespaceIsCanonical(t *testing.T) {
	for _, valid := range []string{"vibedb-test", "a", "a1"} {
		if !validDNSLabel(valid) {
			t.Fatalf("rejected DNS label %q", valid)
		}
	}
	for _, invalid := range []string{"", "VibeDB", "-vibedb", "vibedb-", "vibedb_test"} {
		if validDNSLabel(invalid) {
			t.Fatalf("accepted DNS label %q", invalid)
		}
	}
}

func TestWriteExclusiveDoesNotReplaceRequestIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request.vibejson")
	if err := writeExclusive(path, []byte(`{"one":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(path, []byte(`{"two":2}`)); err == nil {
		t.Fatal("request identity was replaced")
	}
}

func TestParseRSSIsExactAndBounded(t *testing.T) {
	if got, ok := parseRSS([]byte("Name:\ttest\nVmRSS:\t1234 kB\n")); !ok || got != 1234*1024 {
		t.Fatalf("rss=%d ok=%v", got, ok)
	}
	for _, invalid := range [][]byte{
		[]byte("VmRSS: 1 MB\n"), []byte("VmRSS: nope kB\n"), []byte("VmSize: 1 kB\n"),
	} {
		if _, ok := parseRSS(invalid); ok {
			t.Fatalf("accepted rss=%q", invalid)
		}
	}
}
