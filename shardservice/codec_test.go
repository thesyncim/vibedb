package shardservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
)

// encodeRequest / encodeResponse are one-shot helpers that fail the test rather
// than thread an error through every case.
func encodeRequest(t *testing.T, req *ShardRequest) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeRequest(&buf, req); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	return buf.Bytes()
}

func encodeResponse(t *testing.T, resp *ShardResponse) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeResponse(&buf, resp); err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	return buf.Bytes()
}

// TestRequestRoundTrip encodes a request, decodes it, and re-encodes the decoded
// value: equal bytes both times prove the codec is deterministic and lossless.
func TestRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *ShardRequest
	}{
		{
			name: "empty",
			req:  &ShardRequest{},
		},
		{
			name: "select_no_params",
			req: &ShardRequest{
				SQL:            "SELECT 1",
				Distribution:   "tenant_data",
				Shard:          "-80",
				RoutingVersion: 7,
				OwnershipEpoch: 3,
				ReadPolicy:     ReadStrong,
				Deadline:       5 * time.Second,
				MaxResultBytes: 1 << 20,
				MaxRows:        1000,
			},
		},
		{
			name: "all_param_kinds",
			req: &ShardRequest{
				SQL:           "INSERT INTO messages VALUES ($1,$2,$3,$4,$5)",
				ExecutionMode: ExecutionReadWrite,
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
			name: "empty_strings",
			req: &ShardRequest{
				SQL:    "",
				Params: []Param{StringParam(""), NumberParam("0"), BoolParam(false)},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b1 := encodeRequest(t, tc.req)
			got, err := DecodeRequest(bytes.NewReader(b1))
			if err != nil {
				t.Fatalf("DecodeRequest: %v", err)
			}
			b2 := encodeRequest(t, got)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("re-encode not stable:\n first %x\nsecond %x", b1, b2)
			}
			if got.SQL != tc.req.SQL {
				t.Errorf("SQL = %q, want %q", got.SQL, tc.req.SQL)
			}
			if len(got.Params) != len(tc.req.Params) {
				t.Fatalf("params = %d, want %d", len(got.Params), len(tc.req.Params))
			}
			for i := range got.Params {
				if !reflect.DeepEqual(got.Params[i], tc.req.Params[i]) {
					t.Errorf("param %d = %+v, want %+v", i, got.Params[i], tc.req.Params[i])
				}
			}
			if got.Deadline != tc.req.Deadline {
				t.Errorf("Deadline = %v, want %v", got.Deadline, tc.req.Deadline)
			}
			if got.ExecutionMode != tc.req.ExecutionMode {
				t.Errorf("ExecutionMode = %v, want %v", got.ExecutionMode, tc.req.ExecutionMode)
			}
		})
	}
}

// TestResponseRoundTrip does the same for every response shape.
func TestResponseRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		resp *ShardResponse
	}{
		{
			name: "rows",
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
			name: "completion_zero",
			resp: CompletionResponse(0),
		},
		{
			name: "error",
			resp: NewErrorResponse(ErrorNotOwner, "not owner: shard -80"),
		},
		{
			name: "read_only",
			resp: NewErrorResponse(ErrorReadOnly, "mutation refused"),
		},
		{
			name: "commit_outcome_unknown",
			resp: NewErrorResponse(ErrorCommitOutcomeUnknown, "completion unknown"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b1 := encodeResponse(t, tc.resp)
			got, err := DecodeResponse(bytes.NewReader(b1))
			if err != nil {
				t.Fatalf("DecodeResponse: %v", err)
			}
			b2 := encodeResponse(t, got)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("re-encode not stable:\n first %x\nsecond %x", b1, b2)
			}
			if got.Kind != tc.resp.Kind {
				t.Errorf("Kind = %v, want %v", got.Kind, tc.resp.Kind)
			}
		})
	}
}

// TestEncodeDeterministic proves the same value encodes to identical bytes
// across repeated calls, which the golden vectors depend on.
func TestEncodeDeterministic(t *testing.T) {
	req := &ShardRequest{
		SQL:            "SELECT $1",
		Params:         []Param{NumberParam("5"), StringParam("k")},
		Distribution:   "d",
		Shard:          "s",
		RoutingVersion: 1,
		OwnershipEpoch: 2,
		MaxRows:        10,
	}
	first := encodeRequest(t, req)
	for i := 0; i < 8; i++ {
		if got := encodeRequest(t, req); !bytes.Equal(got, first) {
			t.Fatalf("encode %d differs: %x vs %x", i, got, first)
		}
	}
}

// rawFrame assembles a tag + self-covering length + body frame, the shape both
// decoders read, so a test can hand a decoder a body it built byte by byte.
func rawFrame(tag byte, body []byte) []byte {
	f := make([]byte, 5+len(body))
	f[0] = tag
	binary.BigEndian.PutUint32(f[1:5], uint32(len(body)+4))
	copy(f[5:], body)
	return f
}

// TestDecodeRequestMalformed proves every malformed or oversized request frame
// is rejected with a typed error rather than a panic or a large allocation.
func TestDecodeRequestMalformed(t *testing.T) {
	// A minimal valid request body for mutation: version, three empty strings,
	// two u64 (routing, epoch), policy and execution-mode bytes, three u64
	// (deadline, maxbytes, maxrows), a zero param count, and an absent minimum
	// position marker.
	valid := func() []byte {
		var e encbuf
		e.u8(wireVersion1)
		e.str("SELECT 1")
		e.str("d")
		e.str("s")
		e.u64(0)
		e.u64(0)
		e.u8(uint8(ReadStrong))
		e.u8(uint8(ExecutionReadOnly))
		e.u64(0)
		e.u64(0)
		e.u64(0)
		e.u32(0)
		e.u8(0)
		return e.b
	}

	tests := []struct {
		name  string
		frame []byte
		want  error
	}{
		{
			name:  "short_header",
			frame: []byte{tagRequest, 0, 0},
			want:  io.ErrUnexpectedEOF,
		},
		{
			name:  "wrong_tag",
			frame: rawFrame(tagResponse, valid()),
			want:  errBadTag,
		},
		{
			name:  "bad_length_too_small",
			frame: []byte{tagRequest, 0, 0, 0, 3},
			want:  errBadLength,
		},
		{
			name:  "oversized_length",
			frame: []byte{tagRequest, 0x7f, 0xff, 0xff, 0xff},
			want:  errFrameTooLarge,
		},
		{
			name:  "bad_version",
			frame: rawFrame(tagRequest, []byte{0x02}),
			want:  errBadVersion,
		},
		{
			name: "bad_read_policy",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u8(0x7f) // policy byte out of range
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "bad_execution_mode",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(0x7f) // execution mode out of range
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "impossible_param_count",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(1000000) // claims a million params in a tiny body
				return rawFrame(tagRequest, e.b)
			}(),
			want: errImpossibleCount,
		},
		{
			name: "bad_param_kind",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(1)
				e.u8(0xff) // invalid ParamKind
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "trailing_bytes",
			frame: func() []byte {
				b := append(valid(), 0xaa)
				return rawFrame(tagRequest, b)
			}(),
			want: errTrailing,
		},
		{
			name: "negative_deadline",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(1 << 63) // high bit set: not a valid non-negative duration
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errNegativeDuration,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRequest(bytes.NewReader(tc.frame))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestDecodeResponseMalformed proves the response decoder is bounded the same
// way, including the zero-column/nonzero-row amplification guard.
func TestDecodeResponseMalformed(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
		want  error
	}{
		{
			name:  "wrong_tag",
			frame: rawFrame(tagRequest, []byte{wireVersion1, uint8(ResponseCompletion), 0, 0, 0, 0, 0, 0, 0, 0}),
			want:  errBadTag,
		},
		{
			name:  "bad_version",
			frame: rawFrame(tagResponse, []byte{0x09}),
			want:  errBadVersion,
		},
		{
			name:  "bad_response_kind",
			frame: rawFrame(tagResponse, []byte{wireVersion1, 0xff}),
			want:  errBadEnum,
		},
		{
			name: "rows_without_columns",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.u8(uint8(ResponseRows))
				e.u32(0)       // zero columns
				e.u32(1000000) // but a million rows
				return rawFrame(tagResponse, e.b)
			}(),
			want: errRowArity,
		},
		{
			name: "impossible_row_count",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.u8(uint8(ResponseRows))
				e.u32(1) // one column
				e.str("c")
				e.u32(0)       // its OID
				e.u32(1000000) // a million rows in a tiny body
				return rawFrame(tagResponse, e.b)
			}(),
			want: errImpossibleCount,
		},
		{
			name: "bad_error_kind",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion1)
				e.u8(uint8(ResponseError))
				e.u8(0xff) // invalid ErrorKind
				e.str("boom")
				return rawFrame(tagResponse, e.b)
			}(),
			want: errBadEnum,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeResponse(bytes.NewReader(tc.frame))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestEncodeRejectsInvalid proves the encoders fail closed on values the wire
// cannot represent, rather than emitting a frame no decoder would accept.
func TestEncodeRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		enc  func() error
		want error
	}{
		{
			name: "negative_deadline",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Deadline: -1})
			},
			want: errNegativeDuration,
		},
		{
			name: "bad_read_policy",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{ReadPolicy: ReadPolicy(9)})
			},
			want: errBadEnum,
		},
		{
			name: "bad_execution_mode",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{ExecutionMode: ExecutionMode(9)})
			},
			want: errBadEnum,
		},
		{
			name: "invalid_param_kind",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Params: []Param{{Kind: ParamInvalid}}})
			},
			want: errBadEnum,
		},
		{
			name: "row_arity_mismatch",
			enc: func() error {
				return EncodeResponse(io.Discard, &ShardResponse{
					Kind:    ResponseRows,
					Columns: []Column{{Name: "a"}, {Name: "b"}},
					Rows:    [][]Cell{{{Bytes: []byte("x")}}}, // one cell, two columns
				})
			},
			want: errRowArity,
		},
		{
			name: "invalid_response_kind",
			enc: func() error {
				return EncodeResponse(io.Discard, &ShardResponse{Kind: ResponseInvalid})
			},
			want: errBadEnum,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.enc(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestParamRuntimeValue proves each wire parameter materializes into a
// standard-library value sql/driver's runtime already binds.
func TestParamRuntimeValue(t *testing.T) {
	if (Param{Kind: ParamInvalid}).Valid() {
		t.Error("invalid parameter kind reported valid")
	}
	if !StringParam("k").Valid() {
		t.Error("string parameter reported invalid")
	}
	if got := NullParam().RuntimeValue(); got != nil {
		t.Errorf("null = %v, want nil", got)
	}
	if got := BoolParam(true).RuntimeValue(); got != true {
		t.Errorf("bool = %v, want true", got)
	}
	if got, ok := NumberParam("5e0").RuntimeValue().(interface{ String() string }); !ok || got.String() != "5e0" {
		t.Errorf("number = %v, want json.Number 5e0", NumberParam("5e0").RuntimeValue())
	}
	if got := StringParam("k").RuntimeValue(); got != "k" {
		t.Errorf("string = %v, want k", got)
	}
	if got := DocumentParam(`{"a":1}`).RuntimeValue(); got != `{"a":1}` {
		t.Errorf("document = %v, want JSON text", got)
	}
}

// TestReuseDistributionTypes is a compile-time-style check that the request
// carries the distribution package's own typed IDs, not private copies.
func TestReuseDistributionTypes(t *testing.T) {
	req := &ShardRequest{
		Distribution:   distribution.DistributionName("d"),
		Shard:          distribution.ShardID("s"),
		RoutingVersion: distribution.RoutingVersion(1),
		OwnershipEpoch: distribution.OwnershipEpoch(1),
	}
	if req.Distribution != "d" {
		t.Fatal("distribution type mismatch")
	}
}
