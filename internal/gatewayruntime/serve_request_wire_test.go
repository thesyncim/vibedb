package gatewayruntime

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

func TestServeRequestWireDecode(t *testing.T) {
	var request serveRequest
	var scratch serveRequestDecodeScratch
	raw := []byte(`{"op":"query","sql":"SELECT 1","class":"batch","params":[{"kind":"string","text":"x"}]}`)
	if err := decodeServeRequest(raw, &request, &scratch); err != nil {
		t.Fatal(err)
	}
	if request.Op != "query" || request.sqlText() != "SELECT 1" || request.Class != "batch" ||
		len(request.Params) != 1 || request.Params[0].textValue() != "x" {
		t.Fatalf("decoded request = %#v", request)
	}
}

func TestServeRequestWireBorrowsCleanSQLAndOwnsEscapedSQL(t *testing.T) {
	var request serveRequest
	var scratch serveRequestDecodeScratch
	clean := []byte(`{"op":"query","sql":"SELECT 1"}`)
	if err := decodeServeRequest(clean, &request, &scratch); err != nil {
		t.Fatal(err)
	}
	index := bytes.Index(clean, []byte("SELECT"))
	clean[index] = 's'
	if request.sqlText() != "sELECT 1" {
		t.Fatalf("clean SQL did not alias input: %q", request.sqlText())
	}
	escaped := []byte(`{"op":"query","sql":"SELECT\u00201"}`)
	if err := decodeServeRequest(escaped, &request, &scratch); err != nil {
		t.Fatal(err)
	}
	escaped[bytes.Index(escaped, []byte(`\u0020`))+2] = 'f'
	if request.sqlText() != "SELECT 1" {
		t.Fatalf("escaped SQL did not use caller-owned arena: %q", request.sqlText())
	}
}

func TestServeRequestWireSteadyStateAllocations(t *testing.T) {
	raw := []byte(`{"op":"read_batch","class":"batch","max_result_bytes":1048576,"statements":[{"sql":"SELECT * FROM a WHERE id = ?","params":[{"kind":"string","text":"a"}]},{"sql":"SELECT * FROM b WHERE id = ?","params":[{"kind":"number","text":"7"}]}]}`)
	var request serveRequest
	var scratch serveRequestDecodeScratch
	if err := decodeServeRequest(raw, &request, &scratch); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := decodeServeRequest(raw, &request, &scratch); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("steady decode allocations = %.2f, want 0", allocations)
	}
}

func TestServeRequestWireBoundsAndMalformedInputs(t *testing.T) {
	var request serveRequest
	var scratch serveRequestDecodeScratch
	oversized := append([]byte(`{"sql":"`), bytes.Repeat([]byte{'x'}, maxServeRequestBytes)...)
	oversized = append(oversized, '"', '}')
	if err := decodeServeRequest(oversized, &request, &scratch); !errors.Is(err, errInvalidServeRequest) {
		t.Fatalf("oversized error = %v", err)
	}

	for _, raw := range []string{
		``, `null`, `[]`, `{`, `{"op":1}`, `{"sql":false}`,
		`{"params":{}}`, `{"params":[null]}`,
		`{"params":[{"kind":"string","text":7}]}`,
		`{"statements":{}}`, `{"statements":[null]}`,
		`{"statements":[{"sql":7}]}`,
	} {
		if err := decodeServeRequest([]byte(raw), &request, &scratch); err == nil {
			t.Fatalf("accepted malformed request %q", raw)
		}
	}

	var many strings.Builder
	many.WriteString(`{"op":"exec_batch","statements":[`)
	for index := 0; index <= shardservice.MaxMutationStatements; index++ {
		if index != 0 {
			many.WriteByte(',')
		}
		many.WriteString(`{"sql":"x"}`)
	}
	many.WriteString(`]}`)
	if many.Len() >= maxServeRequestBytes {
		t.Fatalf("statement-bound fixture unexpectedly exceeds frame: %d", many.Len())
	}
	if err := decodeServeRequest([]byte(many.String()), &request, &scratch); err != nil {
		t.Fatalf("per-participant statement limit leaked into distributed envelope: %v", err)
	}
	if len(request.Statements) != shardservice.MaxMutationStatements+1 {
		t.Fatalf("decoded %d distributed statements", len(request.Statements))
	}

	many.Reset()
	many.WriteString(`{"statements":[`)
	for index := 0; index <= maxServeDecodeMetadataBytes/serveStatementMetadataBytes; index++ {
		if index != 0 {
			many.WriteByte(',')
		}
		many.WriteString(`{}`)
	}
	many.WriteString(`]}`)
	if many.Len() >= maxServeRequestBytes {
		t.Fatalf("metadata-bound fixture exceeds frame: %d", many.Len())
	}
	if err := decodeServeRequest([]byte(many.String()), &request, &scratch); err == nil {
		t.Fatal("accepted decode metadata exceeding request-memory budget")
	}
	if scratch.metadataBytes > maxServeDecodeMetadataBytes {
		t.Fatal("decoder exceeded metadata budget before rejecting request")
	}
}

func TestServeRequestWireDuplicateKindUsesLastValue(t *testing.T) {
	for _, fields := range []string{
		`"kind":"string","kind":"bogus"`,
		`"kind":"number","kind":""`,
	} {
		var request serveRequest
		var scratch serveRequestDecodeScratch
		raw := []byte(`{"params":[{` + fields + `,"text":"7"}]}`)
		if err := decodeServeRequest(raw, &request, &scratch); err != nil {
			t.Fatal(err)
		}
		if request.Params[0].wireKind != 0 {
			t.Fatal("duplicate kind retained stale enum")
		}
		if _, err := buildParams(request.Params); err == nil {
			t.Fatal("unknown final kind accepted")
		}
	}
	var request serveRequest
	var scratch serveRequestDecodeScratch
	if err := decodeServeRequest([]byte(`{"params":[{"kind":"bogus","kind":"string","text":"ok"}]}`), &request, &scratch); err != nil {
		t.Fatal(err)
	}
	if _, err := buildParams(request.Params); err != nil {
		t.Fatalf("recognized final kind rejected: %v", err)
	}
}

func TestDurableExecBatchWireSinglePassIdentityAndStatements(t *testing.T) {
	raw := []byte(`{"op":"exec_batch","request_id":"01000000000000000000000000000000","installation_id":"02000000000000000000000000000000","issuer_epoch":7,"lane_ordinal":2,"grant_digest":"0303030303030303030303030303030303030303030303030303030303030303","issuer_sequence":9,"class":"batch","statements":[{"sql":"DELETE FROM docs WHERE id = ?","params":[{"kind":"string","text":"a"}]}]}`)
	var request serveRequest
	var scratch serveRequestDecodeScratch
	if err := decodeDurableExecBatchRequest(raw, &request, &scratch); err != nil {
		t.Fatal(err)
	}
	identity, ok := structuredExecBatchIdentity(&request)
	if !ok || identity != request.wireIdentity || len(request.Statements) != 1 ||
		request.Statements[0].sqlText() != "DELETE FROM docs WHERE id = ?" ||
		len(request.Statements[0].Params) != 1 {
		t.Fatalf("single-pass decode lost identity/statements: %#v", request)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := decodeDurableExecBatchRequest(raw, &request, &scratch); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("steady structured decode allocations = %.2f, want 0", allocations)
	}
	// The public strict path must admit a distributed batch larger than one
	// participant's mutation limit; participant routing enforces that limit
	// after the envelope has been decoded.
	start := bytes.Index(raw, []byte(`"statements":[`)) + len(`"statements":[`)
	large := append([]byte(nil), raw[:start]...)
	for index := 0; index <= shardservice.MaxMutationStatements; index++ {
		if index != 0 {
			large = append(large, ',')
		}
		large = append(large, `{"sql":"DELETE FROM docs WHERE id = 1"}`...)
	}
	large = append(large, ']', '}')
	if err := decodeDurableExecBatchRequest(large, &request, &scratch); err != nil {
		t.Fatalf("strict distributed batch rejected by participant ceiling: %v", err)
	}
	if len(request.Statements) != shardservice.MaxMutationStatements+1 {
		t.Fatal("lost distributed statements")
	}
}

func BenchmarkServeRequestWireDecode(b *testing.B) {
	raw := []byte(`{"op":"query","sql":"SELECT * FROM messages WHERE tenant = ? AND id = ?","class":"interactive","params":[{"kind":"string","text":"tenant-a"},{"kind":"string","text":"key-a"}]}`)
	var request serveRequest
	var scratch serveRequestDecodeScratch
	if err := decodeServeRequest(raw, &request, &scratch); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := decodeServeRequest(raw, &request, &scratch); err != nil {
			b.Fatal(err)
		}
	}
}
