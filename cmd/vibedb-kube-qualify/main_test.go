package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestQualificationWireContractsUseVibeJSON(t *testing.T) {
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
