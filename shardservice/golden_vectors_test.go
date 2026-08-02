package shardservice

// This file loads testdata/shard_wire_v1_vectors.txt — the frozen shard-service
// wire codec v1 golden vectors — and asserts every one byte-for-byte against the
// live codec, in both directions. See that file's header for the grammar and for
// why it must never be edited to make a failing assertion pass: a mismatch means
// the codec drifted from a frozen wire format or the fixture was authored
// inconsistently, either of which is a stop condition, not something this test
// (or the fixture) should be adjusted to hide.
//
// To re-author the fixture after an intentional, reviewed format change, run
//
//	SHARDSERVICE_EMIT_GOLDEN=1 go test ./shardservice/ -run TestEmitGoldenVectors -v
//
// and replace the file with the emitted lines. The emitter never writes the
// file itself.

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// goldenRequest and goldenResponse are the ordered, named fixtures the vectors
// pin. Their order is the file's order.
type goldenRequest struct {
	name string
	req  *ShardRequest
}

type goldenResponse struct {
	name string
	resp *ShardResponse
}

func goldenRequests() []goldenRequest {
	return []goldenRequest{
		{
			name: "select_no_params",
			req: &ShardRequest{
				SQL:            "SELECT id FROM messages",
				Distribution:   "tenant_data",
				Shard:          "-80",
				RoutingVersion: 3,
				OwnershipEpoch: 7,
				ReadPolicy:     ReadStrong,
				Deadline:       5 * time.Second,
				MaxResultBytes: 1 << 20,
				MaxRows:        1000,
			},
		},
		{
			name: "insert_all_param_kinds",
			req: &ShardRequest{
				SQL: "INSERT INTO messages VALUES ($1,$2,$3,$4,$5)",
				Params: []Param{
					NullParam(),
					BoolParam(true),
					NumberParam("50e-1"),
					StringParam("hello\x00world"),
					DocumentParam(`{"a":[1,2,3]}`),
				},
				Distribution:   "tenant_data",
				Shard:          "80-",
				RoutingVersion: 42,
				OwnershipEpoch: 9,
			},
		},
		{
			name: "empty_and_zero_fields",
			req: &ShardRequest{
				SQL:    "",
				Params: []Param{StringParam(""), NumberParam("0"), BoolParam(false)},
			},
		},
	}
}

func goldenResponses() []goldenResponse {
	return []goldenResponse{
		{
			name: "rows_two_columns",
			resp: RowsResponse(
				[]Column{{Name: "id", TypeOID: 20}, {Name: "doc", TypeOID: 114}},
				[][]Cell{
					{{Bytes: []byte("1")}, {Bytes: []byte(`{"x":1}`)}},
					{{Bytes: []byte("2")}, {Null: true}},
				},
			),
		},
		{
			name: "rows_empty",
			resp: RowsResponse([]Column{{Name: "n", TypeOID: 20}}, nil),
		},
		{
			name: "completion",
			resp: CompletionResponse(42),
		},
		{
			name: "error_not_owner",
			resp: NewErrorResponse(ErrorNotOwner, "not owner: this shard serves -80"),
		},
		{
			name: "error_stale_epoch",
			resp: NewErrorResponse(ErrorOwnershipEpoch, "ownership epoch mismatch: request 6, configured 7"),
		},
	}
}

// loadGoldenVectors parses the fixture file into request/response hex maps and
// preserves the record order for the completeness cross-check.
func loadGoldenVectors(t *testing.T) (reqHex, respHex map[string]string, order []string) {
	t.Helper()
	f, err := os.Open("testdata/shard_wire_v1_vectors.txt")
	if err != nil {
		t.Fatalf("open golden vectors: %v", err)
	}
	defer f.Close()

	reqHex = map[string]string{}
	respHex = map[string]string{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			t.Fatalf("line %d: want `KIND name hex`, got %q", lineNo, line)
		}
		switch fields[0] {
		case "REQUEST":
			reqHex[fields[1]] = fields[2]
			order = append(order, "REQUEST/"+fields[1])
		case "RESPONSE":
			respHex[fields[1]] = fields[2]
			order = append(order, "RESPONSE/"+fields[1])
		default:
			t.Fatalf("line %d: unknown record kind %q", lineNo, fields[0])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan golden vectors: %v", err)
	}
	if len(reqHex)+len(respHex) == 0 {
		t.Fatal("no golden vectors parsed; fixture path or grammar changed?")
	}
	return reqHex, respHex, order
}

// TestShardWireV1GoldenVectors asserts every fixture encodes to its recorded
// bytes and every recorded byte string decodes and re-encodes to itself.
func TestShardWireV1GoldenVectors(t *testing.T) {
	reqHex, respHex, order := loadGoldenVectors(t)

	for _, gr := range goldenRequests() {
		want, ok := reqHex[gr.name]
		if !ok {
			t.Fatalf("REQUEST %q has no recorded vector", gr.name)
		}
		var buf bytes.Buffer
		if err := EncodeRequest(&buf, gr.req); err != nil {
			t.Fatalf("REQUEST %q: encode: %v", gr.name, err)
		}
		got := hex.EncodeToString(buf.Bytes())
		if got != want {
			t.Fatalf("REQUEST %q:\n  got  %s\n  want %s", gr.name, got, want)
		}
		// Decode the recorded bytes and re-encode: the wire is stable in both
		// directions.
		raw, err := hex.DecodeString(want)
		if err != nil {
			t.Fatalf("REQUEST %q: fixture hex does not decode: %v", gr.name, err)
		}
		decoded, err := DecodeRequest(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("REQUEST %q: decode recorded bytes: %v", gr.name, err)
		}
		var reBuf bytes.Buffer
		if err := EncodeRequest(&reBuf, decoded); err != nil {
			t.Fatalf("REQUEST %q: re-encode: %v", gr.name, err)
		}
		if !bytes.Equal(reBuf.Bytes(), raw) {
			t.Fatalf("REQUEST %q: re-encode differs from recorded bytes", gr.name)
		}
		delete(reqHex, gr.name)
	}
	if len(reqHex) != 0 {
		t.Fatalf("recorded REQUEST vectors with no fixture: %v", keys(reqHex))
	}

	for _, gr := range goldenResponses() {
		want, ok := respHex[gr.name]
		if !ok {
			t.Fatalf("RESPONSE %q has no recorded vector", gr.name)
		}
		var buf bytes.Buffer
		if err := EncodeResponse(&buf, gr.resp); err != nil {
			t.Fatalf("RESPONSE %q: encode: %v", gr.name, err)
		}
		got := hex.EncodeToString(buf.Bytes())
		if got != want {
			t.Fatalf("RESPONSE %q:\n  got  %s\n  want %s", gr.name, got, want)
		}
		raw, err := hex.DecodeString(want)
		if err != nil {
			t.Fatalf("RESPONSE %q: fixture hex does not decode: %v", gr.name, err)
		}
		decoded, err := DecodeResponse(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("RESPONSE %q: decode recorded bytes: %v", gr.name, err)
		}
		var reBuf bytes.Buffer
		if err := EncodeResponse(&reBuf, decoded); err != nil {
			t.Fatalf("RESPONSE %q: re-encode: %v", gr.name, err)
		}
		if !bytes.Equal(reBuf.Bytes(), raw) {
			t.Fatalf("RESPONSE %q: re-encode differs from recorded bytes", gr.name)
		}
		delete(respHex, gr.name)
	}
	if len(respHex) != 0 {
		t.Fatalf("recorded RESPONSE vectors with no fixture: %v", keys(respHex))
	}

	t.Logf("checked %d golden records", len(order))
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestEmitGoldenVectors prints the fixture file the golden test asserts against.
// It is skipped unless SHARDSERVICE_EMIT_GOLDEN=1 and never writes the file, so
// re-authoring is always a deliberate human step, not a self-healing test.
func TestEmitGoldenVectors(t *testing.T) {
	if os.Getenv("SHARDSERVICE_EMIT_GOLDEN") != "1" {
		t.Skip("set SHARDSERVICE_EMIT_GOLDEN=1 to emit the golden vector fixture")
	}
	var sb strings.Builder
	for _, gr := range goldenRequests() {
		var buf bytes.Buffer
		if err := EncodeRequest(&buf, gr.req); err != nil {
			t.Fatalf("encode %q: %v", gr.name, err)
		}
		fmt.Fprintf(&sb, "REQUEST %s %s\n", gr.name, hex.EncodeToString(buf.Bytes()))
	}
	for _, gr := range goldenResponses() {
		var buf bytes.Buffer
		if err := EncodeResponse(&buf, gr.resp); err != nil {
			t.Fatalf("encode %q: %v", gr.name, err)
		}
		fmt.Fprintf(&sb, "RESPONSE %s %s\n", gr.name, hex.EncodeToString(buf.Bytes()))
	}
	t.Logf("golden vector body:\n%s", sb.String())
}
