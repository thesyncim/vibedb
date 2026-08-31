package shardservice

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

func TestSQLDiagnosticEnvelopeRoundTripIsLossless(t *testing.T) {
	want := SQLDiagnostic{
		Code: "22P02", Message: "invalid café ☕", Hint: "use vérité",
		Position: 7, HasPosition: true,
	}
	response, ok := newSQLDiagnosticResponse(want)
	if !ok {
		t.Fatal("valid SQL diagnostic was not encoded")
	}
	first := encodeResponse(t, response)
	decoded, err := DecodeResponse(bytes.NewReader(first))
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	got, ok := decoded.SQLDiagnostic()
	if !ok || got != want {
		t.Fatalf("SQLDiagnostic = (%+v, %v), want (%+v, true)", got, ok, want)
	}
	if second := encodeResponse(t, decoded); !bytes.Equal(second, first) {
		t.Fatalf("diagnostic encoding is not deterministic:\nfirst  %x\nsecond %x", first, second)
	}

	zero, ok := newSQLDiagnosticResponse(SQLDiagnostic{
		Code: "42601", Message: "at beginning", HasPosition: true,
	})
	if !ok {
		t.Fatal("present byte position zero was rejected")
	}
	if got, ok := zero.SQLDiagnostic(); !ok || !got.HasPosition || got.Position != 0 {
		t.Fatalf("zero SQL position = (%+v, %v), want present zero", got, ok)
	}

	const golden = "VDBSQL:ATIyMDEyAQAAAAkAEGRpdmlzaW9uIGJ5IHplcm8AAA"
	got, ok = NewErrorResponse(ErrorMalformedRequest, golden).SQLDiagnostic()
	if !ok || got.Code != "22012" || got.Message != "division by zero" ||
		!got.HasPosition || got.Position != 9 {
		t.Fatalf("golden SQL diagnostic = (%+v, %v)", got, ok)
	}
}

func TestSQLDiagnosticEnvelopeBoundsAndUTF8(t *testing.T) {
	maximal := strings.Repeat("界", 1365) + "x"
	if len(maximal) != maxSQLDiagnosticFieldBytes {
		t.Fatalf("test field length = %d", len(maximal))
	}
	response, ok := newSQLDiagnosticResponse(SQLDiagnostic{
		Code: "22003", Message: maximal, Hint: maximal,
	})
	if !ok {
		t.Fatal("maximal UTF-8 diagnostic was rejected")
	}
	got, ok := response.SQLDiagnostic()
	if !ok || got.Message != maximal || got.Hint != maximal {
		t.Fatal("maximal diagnostic was not lossless")
	}

	invalid := []SQLDiagnostic{
		{Code: "22003", Message: strings.Repeat("x", maxSQLDiagnosticFieldBytes+1)},
		{Code: "22003", Message: "ok", Hint: strings.Repeat("x", maxSQLDiagnosticFieldBytes+1)},
		{Code: "2200x", Message: "bad state"},
		{Code: "22003", Message: string([]byte{0xff})},
		{Code: "22003", Message: "bad absent position", Position: 1},
		{Code: "22003"},
	}
	for _, diagnostic := range invalid {
		if _, ok := newSQLDiagnosticResponse(diagnostic); ok {
			t.Fatalf("invalid diagnostic encoded: %+v", diagnostic)
		}
	}
}

func TestSQLDiagnosticReservedEnvelopeFailsClosed(t *testing.T) {
	invalid := []string{
		sqlDiagnosticEnvelopePrefix,
		sqlDiagnosticEnvelopePrefix + "!not-base64!",
		sqlDiagnosticEnvelopePrefix + base64.RawStdEncoding.EncodeToString(
			[]byte{sqlDiagnosticEnvelopeVersion + 1, '2', '2', '0', '0', '3', 0, 0, 1, 'x', 0, 0},
		),
		sqlDiagnosticEnvelopePrefix + strings.Repeat("A", maxSQLDiagnosticEncodedBytes+1),
	}
	for _, message := range invalid {
		response := NewErrorResponse(ErrorMalformedRequest, message)
		if _, ok := response.SQLDiagnostic(); ok {
			t.Fatalf("malformed reserved message decoded: %q", message)
		}
		encoded := encodeResponse(t, response)
		decoded, err := DecodeResponse(bytes.NewReader(encoded))
		if err != nil || decoded.ErrorMessage != message {
			t.Fatalf("legacy reserved-looking message round trip = (%+v, %v), want %q", decoded, err, message)
		}
		if _, ok := decoded.SQLDiagnostic(); ok {
			t.Fatalf("malformed network text decoded as SQL diagnostic: %q", message)
		}
	}

	valid, ok := newSQLDiagnosticResponse(SQLDiagnostic{Code: "22012", Message: "division by zero"})
	if !ok {
		t.Fatal("valid diagnostic did not encode")
	}
	valid.ErrorKind = ErrorResourceLimit
	decoded, err := DecodeResponse(bytes.NewReader(encodeResponse(t, valid)))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded.SQLDiagnostic(); ok {
		t.Fatal("envelope on a non-malformed error kind became a SQL diagnostic")
	}
}

func TestLegacyResponsesKeepExactWireAndLayout(t *testing.T) {
	legacy := NewErrorResponse(ErrorMalformedRequest, "plain legacy error")
	got := encodeResponse(t, legacy)
	var body encbuf
	body.u8(wireVersion)
	body.u8(uint8(ResponseError))
	body.u8(uint8(ErrorMalformedRequest))
	body.str("plain legacy error")
	body.u8(0)
	want := rawFrame(tagResponse, body.b)
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy response bytes changed:\n got %x\nwant %x", got, want)
	}
	decoded, err := DecodeResponse(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if _, ok := decoded.SQLDiagnostic(); ok || decoded.ErrorMessage != legacy.ErrorMessage {
		t.Fatalf("plain malformed request became a SQL diagnostic: %+v", decoded)
	}

	// Keep this shape independent of ShardResponse so an added transport field
	// makes the layout gate fail on every architecture, including 386.
	type legacyShardResponseLayout struct {
		Kind            ResponseKind
		Columns         []Column
		Rows            [][]Cell
		RowBatch        RowBatchReply
		RowsAffected    int64
		ErrorKind       ErrorKind
		ErrorMessage    string
		HasReadPosition bool
		ReadPosition    Position
		DocumentScan    DocumentScanReply
		Transaction     TransactionReply
		Exchange        ExchangeReply
	}
	if got, want := unsafe.Sizeof(ShardResponse{}), unsafe.Sizeof(legacyShardResponseLayout{}); got != want {
		t.Fatalf("ShardResponse size = %d, want legacy %d", got, want)
	}
	actual, expected := reflect.TypeOf(ShardResponse{}), reflect.TypeOf(legacyShardResponseLayout{})
	for i := 0; i < expected.NumField(); i++ {
		wantField := expected.Field(i)
		gotField, ok := actual.FieldByName(wantField.Name)
		if !ok || gotField.Offset != wantField.Offset || gotField.Type != wantField.Type {
			t.Fatalf("ShardResponse field %s layout = (%v, %d, %v), want (%v, %d, %v)",
				wantField.Name, ok, gotField.Offset, gotField.Type, true, wantField.Offset, wantField.Type)
		}
	}

	completion := CompletionResponse(1)
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := EncodeResponse(io.Discard, completion); err != nil {
			panic(err)
		}
	}); allocations != 1 {
		t.Fatalf("ordinary success EncodeResponse allocations = %v, want legacy 1", allocations)
	}
}

func TestClassifyErrorCarriesKnownSQLDiagnosticsOnly(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     string
		position int
	}{
		{"division", &query.ScalarDivisionByZeroError{Pos: 9}, "22012", 9},
		{"range", &query.ScalarNumericRangeError{Pos: 8, Operation: "power", Requested: 2, Limit: 1}, "22003", 8},
		{"invalid_text", &query.ScalarInvalidTextError{Pos: 7, Target: "BOOLEAN"}, "22P02", 7},
		{"schema", &store.SchemaViolationError{Path: "/n", Expected: store.SchemaNumber}, "23514", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := classifyError(test.err)
			diagnostic, ok := response.SQLDiagnostic()
			if !ok || diagnostic.Code != test.code || diagnostic.Message != test.err.Error() {
				t.Fatalf("classifyError = %+v / %+v, want %s / %q", response, diagnostic, test.code, test.err)
			}
			if test.name != "schema" && (!diagnostic.HasPosition || diagnostic.Position != test.position) {
				t.Fatalf("position = (%d, %v), want (%d, true)", diagnostic.Position, diagnostic.HasPosition, test.position)
			}
		})
	}

	unknown := errors.New("private implementation failure")
	response := classifyError(unknown)
	if response.ErrorKind != ErrorMalformedRequest || response.ErrorMessage != unknown.Error() {
		t.Fatalf("unknown classification = %+v", response)
	}
	if _, ok := response.SQLDiagnostic(); ok {
		t.Fatal("unknown error acquired a SQL diagnostic")
	}
	resource := classifyError(query.ErrResultBudget)
	if resource.ErrorKind != ErrorResourceLimit || resource.ErrorMessage != query.ErrResultBudget.Error() {
		t.Fatalf("resource classification changed: %+v", resource)
	}
}
