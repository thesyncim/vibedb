package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	vibejson "github.com/thesyncim/vibejson"
)

const testNativeRequestID = "00112233445566778899aabbccddeeff"
const testNativeRouteID = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func testNativeEncodedKey() string {
	return base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3, 4})
}

func TestDecodeNativeDataRequestExactShapes(t *testing.T) {
	key := testNativeEncodedKey()
	tests := []struct {
		name  string
		src   string
		want  nativeDataOperation
		check func(*testing.T, *nativeDataWireRequest, []byte)
	}{
		{
			name: "linearizable get",
			src:  `{"op":"get","table":"docs","key":"` + key + `","consistency":"linearizable"}`,
			want: nativeDataOperationGet,
			check: func(t *testing.T, request *nativeDataWireRequest, _ []byte) {
				if request.Consistency != nativeDataLinearizable || request.Applied != 0 ||
					request.RouteID != ([32]byte{}) {
					t.Fatalf("linearizable request = %+v", request)
				}
			},
		},
		{
			name: "applied get",
			src: `{"op":"get","table":"docs","key":"` + key +
				`","consistency":"at_least_applied","route_id":"` + testNativeRouteID + `","applied":42}`,
			want: nativeDataOperationGet,
			check: func(t *testing.T, request *nativeDataWireRequest, _ []byte) {
				if request.Consistency != nativeDataAtLeastApplied || request.Applied != 42 ||
					request.RouteID == ([32]byte{}) {
					t.Fatalf("applied request = %+v", request)
				}
			},
		},
		{
			name: "put",
			src: `{"op":"put","table":"docs","key":"` + key +
				`","document":{"id":"a","n":1},"request_id":"` + testNativeRequestID + `"}`,
			want: nativeDataOperationPut,
			check: func(t *testing.T, request *nativeDataWireRequest, src []byte) {
				if !bytes.Equal(request.Document, []byte(`{"id":"a","n":1}`)) ||
					request.RequestID == ([16]byte{}) || !aliasesNativeRequest(src, request.Document) {
					t.Fatalf("put request document=%s id=%x", request.Document, request.RequestID)
				}
			},
		},
		{
			name: "delete",
			src: `{"op":"delete","table":"docs","key":"` + key +
				`","request_id":"` + testNativeRequestID + `"}`,
			want: nativeDataOperationDelete,
			check: func(t *testing.T, request *nativeDataWireRequest, _ []byte) {
				if len(request.Document) != 0 || request.RequestID == ([16]byte{}) {
					t.Fatalf("delete request = %+v", request)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src := []byte(test.src)
			var request nativeDataWireRequest
			if err := decodeNativeDataRequest(src, &request); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if request.Operation != test.want || !bytes.Equal(request.Table, []byte("docs")) ||
				!bytes.Equal(request.OrderedKey(), []byte{1, 2, 3, 4}) ||
				!aliasesNativeRequest(src, request.Table) {
				t.Fatalf("request = %+v key=%x", request, request.OrderedKey())
			}
			test.check(t, &request, src)
		})
	}
}

func aliasesNativeRequest(source, value []byte) bool {
	if len(source) == 0 || len(value) == 0 {
		return false
	}
	index := bytes.Index(source, value)
	return index >= 0 && &value[0] == &source[index]
}

func TestDecodeNativeDataRequestRejectsNoncanonicalInput(t *testing.T) {
	key := testNativeEncodedKey()
	valid := `{"op":"get","table":"docs","key":"` + key + `","consistency":"linearizable"}`
	tests := []string{
		``, `null`, `{}`,
		`{"table":"docs","op":"get","key":"` + key + `","consistency":"linearizable"}`,
		`{"o\u0070":"get","table":"docs","key":"` + key + `","consistency":"linearizable"}`,
		`{"op":"get","table":"do\u0063s","key":"` + key + `","consistency":"linearizable"}`,
		`{"op":"get","table":"docs","key":"AQIDBA==","consistency":"linearizable"}`,
		`{"op":"get","table":"docs","key":"` + key + `","consistency":"at_least_applied","route_id":"` + testNativeRouteID + `","applied":0}`,
		`{"op":"get","table":"docs","key":"` + key + `","consistency":"linearizable","applied":1}`,
		`{"op":"put","table":"docs","key":"` + key + `","document":[],"request_id":"` + testNativeRequestID + `"}`,
		`{"op":"put","table":"docs","key":"` + key + `","request_id":"` + testNativeRequestID + `","document":{}}`,
		`{"op":"delete","table":"docs","key":"` + key + `","request_id":"00112233445566778899AABBCCDDEEFF"}`,
		`{"op":"delete","table":"docs","key":"` + key + `","request_id":"00000000000000000000000000000000"}`,
		valid + ` ` + valid,
	}
	for _, source := range tests {
		var request nativeDataWireRequest
		if err := decodeNativeDataRequest([]byte(source), &request); !errors.Is(err, errInvalidNativeDataRequest) {
			t.Errorf("decode %q = %v, want invalid", source, err)
		}
	}
}

func TestDecodeNativeDataRequestCanonicalEscapedTable(t *testing.T) {
	source := []byte(`{"op":"get","table":"do\"cs","key":"AQIDBA","consistency":"linearizable"}`)
	var request nativeDataWireRequest
	if err := decodeNativeDataRequest(source, &request); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(request.Table, []byte(`do"cs`)) || aliasesNativeRequest(source, request.Table) {
		t.Fatalf("decoded table = %q aliases=%v", request.Table, aliasesNativeRequest(source, request.Table))
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := decodeNativeDataRequest(source, &request); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("escaped table allocations = %v, want 0", allocations)
	}
}

func TestNativeCanonicalTableStringMatchesVibeJSON(t *testing.T) {
	names := []string{
		`slash\\quote"`, "\b\f\n\r\t\x01", `<>&`, "café-世界",
		"line paragraph ", strings.Repeat("<", 120),
	}
	for _, name := range names {
		encoded, err := vibejson.Marshal(&name)
		if err != nil {
			t.Fatalf("marshal %q: %v", name, err)
		}
		var encodedStore [collectionname.MaxNameBytes*6 + 2]byte
		if got := appendNativeCanonicalString(encodedStore[:0], []byte(name)); !bytes.Equal(got, encoded) {
			t.Fatalf("canonical %q = %q, want %q", name, got, encoded)
		}
		source := make([]byte, 0, len(encoded)+64)
		source = append(source, `{"op":"get","table":`...)
		source = append(source, encoded...)
		source = append(source, `,"key":"AQIDBA","consistency":"linearizable"}`...)
		var request nativeDataWireRequest
		if err := decodeNativeDataRequest(source, &request); err != nil {
			t.Fatalf("decode %q: %v", source, err)
		}
		if !bytes.Equal(request.Table, []byte(name)) {
			t.Fatalf("table = %q, want %q", request.Table, name)
		}
	}

	for _, source := range []string{
		`{"op":"get","table":"a\/b","key":"AQIDBA","consistency":"linearizable"}`,
		`{"op":"get","table":"\u003C","key":"AQIDBA","consistency":"linearizable"}`,
		`{"op":"get","table":"\u000a","key":"AQIDBA","consistency":"linearizable"}`,
	} {
		var request nativeDataWireRequest
		if err := decodeNativeDataRequest([]byte(source), &request); !errors.Is(err, errInvalidNativeDataRequest) {
			t.Fatalf("alternate spelling %q = %v", source, err)
		}
	}
}

func TestDecodeNativeDataRequestWarmAllocationFree(t *testing.T) {
	source := []byte(`{"op":"put","table":"docs","key":"AQIDBA","document":{"id":"a","n":1},"request_id":"` + testNativeRequestID + `"}`)
	var request nativeDataWireRequest
	if err := decodeNativeDataRequest(source, &request); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := decodeNativeDataRequest(source, &request); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warm decode allocations = %v, want 0", allocations)
	}
}

func BenchmarkDecodeNativeDataRequest(b *testing.B) {
	source := []byte(`{"op":"put","table":"docs","key":"AQIDBA","document":{"id":"a","n":1},"request_id":"` + testNativeRequestID + `"}`)
	var request nativeDataWireRequest
	if err := decodeNativeDataRequest(source, &request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for b.Loop() {
		if err := decodeNativeDataRequest(source, &request); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzDecodeNativeDataRequest(f *testing.F) {
	f.Add([]byte(`{"op":"get","table":"docs","key":"AQIDBA","consistency":"linearizable"}`))
	f.Add([]byte(`{"op":"delete","table":"docs","key":"AQIDBA","request_id":"` + testNativeRequestID + `"}`))
	f.Fuzz(func(t *testing.T, source []byte) {
		if len(source) > 1<<20 {
			t.Skip()
		}
		var request nativeDataWireRequest
		if decodeNativeDataRequest(source, &request) == nil {
			if request.Operation == 0 || len(request.Table) == 0 || len(request.OrderedKey()) == 0 {
				t.Fatalf("accepted incomplete request: %+v", request)
			}
		}
	})
}
