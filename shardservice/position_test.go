package shardservice

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func testPosition(distributionName, shard string, index uint64) Position {
	return Position{
		Distribution: distribution.DistributionName(distributionName),
		Shard:        distribution.ShardID(shard),
		LogID:        [16]byte{0x91, 0x22, 0x73, 0x44, 0x35, 0x66, 0x17, 0x88, 0x49, 0xaa, 0x5b, 0xcc, 0x7d, 0xee, 0x8f, 0x10},
		Index:        index,
	}
}

func TestPositionValidation(t *testing.T) {
	valid := testPosition("tenant_data", "-80", 7)
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid position: %v", err)
	}
	if !valid.SameSource(testPosition("tenant_data", "-80", 9)) ||
		!valid.SameLog(testPosition("tenant_data", "-80", 9)) {
		t.Fatal("same log identity did not compare equal")
	}
	differentLog := valid
	differentLog.LogID[0]++
	if !valid.SameSource(differentLog) || valid.SameLog(differentLog) {
		t.Fatal("log lineage was not fenced independently from source identity")
	}

	tests := []struct {
		name   string
		mutate func(*Position)
	}{
		{"empty_distribution", func(p *Position) { p.Distribution = "" }},
		{"long_distribution", func(p *Position) {
			p.Distribution = distribution.DistributionName(strings.Repeat("d", MaxPositionIdentityBytes+1))
		}},
		{"invalid_distribution_utf8", func(p *Position) { p.Distribution = distribution.DistributionName("\xff") }},
		{"empty_shard", func(p *Position) { p.Shard = "" }},
		{"long_shard", func(p *Position) {
			p.Shard = distribution.ShardID(strings.Repeat("s", MaxPositionIdentityBytes+1))
		}},
		{"invalid_shard_utf8", func(p *Position) { p.Shard = distribution.ShardID("\xff") }},
		{"zero_log", func(p *Position) { p.LogID = [16]byte{} }},
		{"zero_index", func(p *Position) { p.Index = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := valid
			tc.mutate(&p)
			if err := p.Validate(); !errors.Is(err, ErrInvalidPosition) {
				t.Fatalf("Validate = %v, want ErrInvalidPosition", err)
			}
		})
	}
}

func TestPositionWireRoundTrip(t *testing.T) {
	p := testPosition("tenant_data", "-80", 42)
	req := &ShardRequest{SQL: "SELECT 1", HasMinPosition: true, MinPosition: p}
	var request bytes.Buffer
	if err := EncodeRequest(&request, req); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	encodedRequest := append([]byte(nil), request.Bytes()...)
	absentRequest := encodeRequest(t, &ShardRequest{SQL: "SELECT 1"})
	const positionWireBytes = 1 + 1 + len("tenant_data") + 1 + len("-80") + 16 + 8
	if got := len(encodedRequest) - len(absentRequest) + 1; got != positionWireBytes {
		t.Fatalf("encoded position bytes = %d, want %d", got, positionWireBytes)
	}
	gotReq, err := DecodeRequest(bytes.NewReader(encodedRequest))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if !gotReq.HasMinPosition || !reflect.DeepEqual(gotReq.MinPosition, p) {
		t.Fatalf("minimum position = %+v, want %+v", gotReq.MinPosition, p)
	}

	resp := RowsResponse([]Column{{Name: "n"}}, nil)
	resp.HasReadPosition = true
	resp.ReadPosition = p
	var response bytes.Buffer
	if err := EncodeResponse(&response, resp); err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	gotResp, err := DecodeResponse(&response)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if !gotResp.HasReadPosition || !reflect.DeepEqual(gotResp.ReadPosition, p) {
		t.Fatalf("read position = %+v, want %+v", gotResp.ReadPosition, p)
	}
}

func TestPositionWireStrictValidation(t *testing.T) {
	p := testPosition("d", "s", 1)

	bad := p
	bad.Index = 0
	if err := EncodeRequest(io.Discard, &ShardRequest{HasMinPosition: true, MinPosition: bad}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatalf("invalid request position encode = %v, want ErrInvalidPosition", err)
	}
	if err := EncodeRequest(io.Discard, &ShardRequest{MinPosition: p}); !errors.Is(err, errNonCanonicalPosition) {
		t.Fatalf("absent request with payload = %v, want errNonCanonicalPosition", err)
	}
	if err := EncodeResponse(io.Discard, &ShardResponse{Kind: ResponseCompletion, HasReadPosition: true, ReadPosition: p}); !errors.Is(err, errUnexpectedReadPosition) {
		t.Fatalf("completion read position encode = %v, want errUnexpectedReadPosition", err)
	}
	if err := EncodeResponse(io.Discard, &ShardResponse{Kind: ResponseRows, ReadPosition: p}); !errors.Is(err, errNonCanonicalPosition) {
		t.Fatalf("absent response with payload = %v, want errNonCanonicalPosition", err)
	}
	for _, kind := range []ResponseKind{ResponseCompletion, ResponseError} {
		var body encbuf
		body.u8(wireVersion)
		body.u8(uint8(kind))
		if kind == ResponseCompletion {
			body.u64(0)
		} else {
			body.u8(uint8(ErrorMalformedRequest))
			body.str("refused")
		}
		if err := body.position(true, p); err != nil {
			t.Fatalf("build %s frame: %v", kind, err)
		}
		if _, err := DecodeResponse(bytes.NewReader(rawFrame(tagResponse, body.b))); !errors.Is(err, errUnexpectedReadPosition) {
			t.Fatalf("decoded %s read position = %v, want errUnexpectedReadPosition", kind, err)
		}
	}

	request := encodeRequest(t, &ShardRequest{})
	request[len(request)-1] = 2
	if _, err := DecodeRequest(bytes.NewReader(request)); !errors.Is(err, errBadPresence) {
		t.Fatalf("request presence marker = %v, want errBadPresence", err)
	}

	response := encodeResponse(t, RowsResponse(nil, nil))
	response[len(response)-1] = 2
	if _, err := DecodeResponse(bytes.NewReader(response)); !errors.Is(err, errBadPresence) {
		t.Fatalf("response presence marker = %v, want errBadPresence", err)
	}

	withPosition := encodeRequest(t, &ShardRequest{HasMinPosition: true, MinPosition: p})
	for i := len(withPosition) - 8; i < len(withPosition); i++ {
		withPosition[i] = 0
	}
	if _, err := DecodeRequest(bytes.NewReader(withPosition)); !errors.Is(err, ErrInvalidPosition) {
		t.Fatalf("zero decoded index = %v, want ErrInvalidPosition", err)
	}

	var body encbuf
	body.u8(wireVersion)
	body.str("")
	body.str("")
	body.str("")
	body.u64(0)
	body.u64(0)
	body.u64(0)
	body.u8(uint8(ReadStrong))
	body.u8(uint8(ExecutionReadOnly))
	body.u64(0)
	body.u64(0)
	body.u64(0)
	body.u32(0)
	body.u8(1)
	body.u8(MaxPositionIdentityBytes)
	body.b = append(body.b, "short"...)
	if _, err := DecodeRequest(bytes.NewReader(rawFrame(tagRequest, body.b))); !errors.Is(err, errTruncated) {
		t.Fatalf("truncated bounded identity = %v, want errTruncated", err)
	}
}
