package gatewayruntime

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

func TestWriteNativeDataReadResponses(t *testing.T) {
	position := replication.Digest{1, 2, 3}
	tests := []struct {
		name     string
		response nativeDataWireResponse
		want     string
	}{
		{
			name: "found",
			response: nativeDataWireResponse{
				OK: true, Position: position, Applied: 42, Found: true,
				Document: []byte(`{"id":"a","n":1}`), Retries: 2,
			},
			want: `{"ok":true,"route_id":"0102030000000000000000000000000000000000000000000000000000000000","applied":42,"found":true,"document":{"id":"a","n":1},"retries":2}` + "\n",
		},
		{
			name: "missing",
			response: nativeDataWireResponse{
				OK: true, Position: position, Applied: 43,
			},
			want: `{"ok":true,"route_id":"0102030000000000000000000000000000000000000000000000000000000000","applied":43,"found":false}` + "\n",
		},
		{
			name: "typed error",
			response: nativeDataWireResponse{
				Code: nativeDataResponseReadBehind, Retryable: true, Retries: 3,
			},
			want: `{"ok":false,"code":"read_behind","retryable":true,"retries":3}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch nativeDataResponseScratch
			var destination bytes.Buffer
			writer := vibejson.NewWriter(&destination)
			if err := writeNativeDataResponse(writer, &test.response); err != nil {
				t.Fatal(err)
			}
			if got := destination.String(); got != test.want {
				t.Fatalf("response = %q, want %q", got, test.want)
			}
			destination.Reset()
			if err := writeNativeDataResponseDirect(&destination, &test.response, &scratch); err != nil {
				t.Fatal(err)
			}
			if got := destination.String(); got != test.want {
				t.Fatalf("direct response = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWriteNativeDataResponseRejectsInvalidState(t *testing.T) {
	position := replication.Digest{1}
	tests := []nativeDataWireResponse{
		{},
		{OK: true, Applied: 1},
		{OK: true, Position: position},
		{Code: 255},
		{Code: nativeDataResponseUnavailable, Found: true},
		{Code: nativeDataResponseUnavailable, Found: true, Document: []byte(`{}`)},
		{Code: nativeDataResponseUnavailable, Document: []byte(`{}`)},
		{Code: nativeDataResponseUnavailable, HasRequest: true},
	}
	for _, response := range tests {
		var scratch nativeDataResponseScratch
		writer := vibejson.NewWriter(&bytes.Buffer{})
		if err := writeNativeDataResponse(writer, &response); !errors.Is(err, errInvalidNativeDataResponse) {
			t.Fatalf("response %+v error = %v", response, err)
		}
		if err := writeNativeDataResponseDirect(io.Discard, &response, &scratch); !errors.Is(err, errInvalidNativeDataResponse) {
			t.Fatalf("direct response %+v error = %v", response, err)
		}
	}
}

func TestWriteNativeDataResponseWarmAllocationFree(t *testing.T) {
	writer := vibejson.NewWriter(io.Discard)
	var scratch nativeDataResponseScratch
	response := nativeDataWireResponse{
		OK: true, Position: replication.Digest{1}, Applied: 42, Found: true,
		Document: []byte(`{"id":"a","n":1}`), Retries: 1,
	}
	if err := writeNativeDataResponse(writer, &response); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := writeNativeDataResponse(writer, &response); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("warm response allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := writeNativeDataResponseDirect(io.Discard, &response, &scratch); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("direct response allocations = %v, want 0", allocations)
	}
}

func TestWriteNativeDataConnResponseTimesOutBlockedClient(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	response := nativeDataWireResponse{
		OK: true, Position: replication.Digest{1}, Applied: 42, Found: true,
		Document: []byte(`{"id":"a"}`),
	}
	var scratch nativeDataResponseScratch
	started := time.Now()
	err := writeNativeDataConnResponse(server, &response, &scratch, 20*time.Millisecond)
	if err == nil {
		t.Fatal("blocked response unexpectedly succeeded")
	}
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("blocked response error = %T %v", err, err)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond || elapsed > time.Second {
		t.Fatalf("blocked response elapsed = %v", elapsed)
	}
}

func BenchmarkWriteNativeDataResponse(b *testing.B) {
	writer := vibejson.NewWriter(io.Discard)
	response := nativeDataWireResponse{
		OK: true, Position: replication.Digest{1}, Applied: 42, Found: true,
		Document: []byte(`{"id":"a","n":1}`), Retries: 1,
	}
	if err := writeNativeDataResponse(writer, &response); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := writeNativeDataResponse(writer, &response); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteNativeDataResponseDirect(b *testing.B) {
	response := nativeDataWireResponse{
		OK: true, Position: replication.Digest{1}, Applied: 42, Found: true,
		Document: []byte(`{"id":"a","n":1}`), Retries: 1,
	}
	var scratch nativeDataResponseScratch
	b.ReportAllocs()
	for b.Loop() {
		if err := writeNativeDataResponseDirect(io.Discard, &response, &scratch); err != nil {
			b.Fatal(err)
		}
	}
}
