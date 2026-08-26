package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

const validDurableExecBatchAckFixture = `{"op":"ack_exec_batch","request_id":"0102030405060708090a0b0c0d0e0f10","request_digest":"1111111111111111111111111111111111111111111111111111111111111111","installation_id":"21212121212121212121212121212121","issuer_epoch":7,"lane_ordinal":3,"grant_digest":"2222222222222222222222222222222222222222222222222222222222222222","issuer_sequence":9,"terminal_revision":11,"result_digest":"3131313131313131313131313131313131313131313131313131313131313131","ack_token":"4141414141414141414141414141414141414141414141414141414141414141"}`

func TestDurableExecBatchAckWireRequestRoundTrip(t *testing.T) {
	var request durableExecBatchAckWireRequest
	if err := decodeDurableExecBatchAckRequest([]byte(validDurableExecBatchAckFixture), &request); err != nil {
		t.Fatal(err)
	}
	if request.Identity.RequestID[0] != 1 || request.Identity.RequestID[15] != 16 ||
		request.Identity.RequestDigest[0] != 0x11 || request.Identity.Reference.Epoch != 7 ||
		request.Identity.Reference.Installation[0] != 0x21 || request.Identity.Reference.LaneOrdinal != 3 ||
		request.Identity.Reference.GrantDigest[0] != 0x22 || request.Identity.IssuerSequence != 9 ||
		request.TerminalRevision != 11 || request.ResultDigest[0] != 0x31 ||
		request.AckToken[0] != 0x41 {
		t.Fatalf("decoded request = %+v", request)
	}

	response := durableExecBatchAckWireResponse{
		durableExecBatchAckWireRequest: request,
		Applied:                        17, CollectionRounds: 2,
	}
	var output bytes.Buffer
	if err := writeDurableExecBatchAckResponse(vibejson.NewWriter(&output), &response); err != nil {
		t.Fatal(err)
	}
	want := `{"ok":true,"op":"ack_exec_batch","request_id":"0102030405060708090a0b0c0d0e0f10","request_digest":"1111111111111111111111111111111111111111111111111111111111111111","installation_id":"21212121212121212121212121212121","issuer_epoch":7,"lane_ordinal":3,"grant_digest":"2222222222222222222222222222222222222222222222222222222222222222","issuer_sequence":9,"terminal_revision":11,"result_digest":"3131313131313131313131313131313131313131313131313131313131313131","ack_token":"4141414141414141414141414141414141414141414141414141414141414141","applied":17,"collection_rounds":2}` + "\n"
	if output.String() != want {
		t.Fatalf("response = %q, want %q", output.String(), want)
	}
	if !vibejson.Valid(bytes.TrimSpace(output.Bytes())) {
		t.Fatalf("response is not valid vibejson: %q", output.Bytes())
	}
}

func TestDurableExecBatchAckWireRejectsNonCanonicalOrIncompleteIdentity(t *testing.T) {
	tests := map[string]string{
		"unknown operation": strings.Replace(validDurableExecBatchAckFixture, "ack_exec_batch", "exec_batch", 1),
		"uppercase token":   strings.Replace(validDurableExecBatchAckFixture, strings.Repeat("41", 32), strings.Repeat("4A", 32), 1),
		"short token":       strings.Replace(validDurableExecBatchAckFixture, strings.Repeat("41", 32), strings.Repeat("41", 31), 1),
		"zero token":        strings.Replace(validDurableExecBatchAckFixture, strings.Repeat("41", 32), strings.Repeat("00", 32), 1),
		"zero epoch":        strings.Replace(validDurableExecBatchAckFixture, `"issuer_epoch":7`, `"issuer_epoch":0`, 1),
		"zero sequence":     strings.Replace(validDurableExecBatchAckFixture, `"issuer_sequence":9`, `"issuer_sequence":0`, 1),
		"zero terminal":     strings.Replace(validDurableExecBatchAckFixture, `"terminal_revision":11`, `"terminal_revision":0`, 1),
		"trailing field":    strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"sql":"delete from x"}`,
		"statements":        strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"statements":[]}`,
		"params":            strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"params":[]}`,
		"class":             strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"class":"batch"}`,
		"max result":        strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"max_result_bytes":1}`,
		"tenant spoof":      strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"tenant":"other"}`,
		"principal spoof":   strings.TrimSuffix(validDurableExecBatchAckFixture, "}") + `,"principal":"other"}`,
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			var request durableExecBatchAckWireRequest
			if err := decodeDurableExecBatchAckRequest([]byte(source), &request); !errors.Is(err, errInvalidDurableExecBatchAckRequest) {
				t.Fatalf("error = %v, want %v", err, errInvalidDurableExecBatchAckRequest)
			}
		})
	}
}

func TestDurableExecBatchAckWireRejectsReorderedDuplicateAndOversizedFields(t *testing.T) {
	tests := map[string]string{
		"reordered": strings.Replace(validDurableExecBatchAckFixture,
			`"request_id":"0102030405060708090a0b0c0d0e0f10","request_digest":"1111111111111111111111111111111111111111111111111111111111111111"`,
			`"request_digest":"1111111111111111111111111111111111111111111111111111111111111111","request_id":"0102030405060708090a0b0c0d0e0f10"`, 1),
		"duplicate": strings.Replace(validDurableExecBatchAckFixture,
			`"request_id":"0102030405060708090a0b0c0d0e0f10"`,
			`"request_id":"0102030405060708090a0b0c0d0e0f10","request_id":"0102030405060708090a0b0c0d0e0f10"`, 1),
		"missing": strings.Replace(validDurableExecBatchAckFixture,
			`,"terminal_revision":11`, "", 1),
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			var request durableExecBatchAckWireRequest
			if err := decodeDurableExecBatchAckRequest([]byte(source), &request); !errors.Is(err, errInvalidDurableExecBatchAckRequest) {
				t.Fatalf("error = %v, want %v", err, errInvalidDurableExecBatchAckRequest)
			}
		})
	}
	oversized := append([]byte(validDurableExecBatchAckFixture), bytes.Repeat([]byte(" "), maxDurableExecBatchAckRequestBytes)...)
	var request durableExecBatchAckWireRequest
	if err := decodeDurableExecBatchAckRequest(oversized, &request); !errors.Is(err, errInvalidDurableExecBatchAckRequest) {
		t.Fatalf("oversized error = %v", err)
	}
}

func TestDurableExecBatchAckCandidateRequiresExactFirstOperation(t *testing.T) {
	for _, source := range []string{
		validDurableExecBatchAckFixture,
		" \n\t" + validDurableExecBatchAckFixture,
		`{"op":"ack_exec_batch"}`,
		`{ "op" : "ack_exec_batch" }`,
	} {
		if !durableExecBatchAckRequestCandidate([]byte(source)) {
			t.Fatalf("candidate rejected %q", source)
		}
	}
	for _, source := range []string{
		`{"request_id":"01","op":"ack_exec_batch"}`,
		`{"op":"ack_exec_batch_suffix"}`,
		`{"op":"exec_batch"}`,
	} {
		if durableExecBatchAckRequestCandidate([]byte(source)) {
			t.Fatalf("non-candidate accepted %q", source)
		}
	}
}

func TestDurableExecBatchAckResponseRejectsInvalidStateBeforeWriting(t *testing.T) {
	var output bytes.Buffer
	if err := writeDurableExecBatchAckResponse(vibejson.NewWriter(&output), nil); !errors.Is(err, errInvalidDurableExecBatchAckResponse) || output.Len() != 0 {
		t.Fatalf("nil response output=%q err=%v", output.Bytes(), err)
	}
	var request durableExecBatchAckWireRequest
	if err := decodeDurableExecBatchAckRequest([]byte(validDurableExecBatchAckFixture), &request); err != nil {
		t.Fatal(err)
	}
	response := durableExecBatchAckWireResponse{durableExecBatchAckWireRequest: request}
	if err := writeDurableExecBatchAckResponse(vibejson.NewWriter(&output), &response); !errors.Is(err, errInvalidDurableExecBatchAckResponse) || output.Len() != 0 {
		t.Fatalf("zero-applied response output=%q err=%v", output.Bytes(), err)
	}
}
