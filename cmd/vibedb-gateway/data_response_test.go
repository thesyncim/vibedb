package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

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
			var destination bytes.Buffer
			writer := vibejson.NewWriter(&destination)
			if err := writeNativeDataResponse(writer, &test.response); err != nil {
				t.Fatal(err)
			}
			if got := destination.String(); got != test.want {
				t.Fatalf("response = %q, want %q", got, test.want)
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
		{Code: nativeDataResponseUnavailable, Document: []byte(`{}`)},
		{Code: nativeDataResponseUnavailable, HasRequest: true},
	}
	for _, response := range tests {
		writer := vibejson.NewWriter(&bytes.Buffer{})
		if err := writeNativeDataResponse(writer, &response); !errors.Is(err, errInvalidNativeDataResponse) {
			t.Fatalf("response %+v error = %v", response, err)
		}
	}
}

func TestWriteNativeDataResponseWarmAllocationFree(t *testing.T) {
	writer := vibejson.NewWriter(io.Discard)
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
