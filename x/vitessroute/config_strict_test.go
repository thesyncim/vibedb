package vitessroute

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestLoadKeyspaceStrictJSONEnvelope(t *testing.T) {
	valid := `{"sharded":true,"vindexes":{},"tables":{}}`
	if _, err := LoadKeyspace([]byte(valid)); err != nil {
		t.Fatalf("valid VSchema: %v", err)
	}

	deep := strings.Repeat("[", maxVSchemaJSONDepth+1) + "0" + strings.Repeat("]", maxVSchemaJSONDepth+1)
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{name: "duplicate exact", data: []byte(`{"sharded":true,"sharded":true,"vindexes":{},"tables":{}}`), want: "duplicate object member"},
		{name: "duplicate escaped spelling", data: []byte(`{"sharded":true,"\u0073harded":true,"vindexes":{},"tables":{}}`), want: "duplicate object member"},
		{name: "duplicate nested escaped key", data: []byte(`{"sharded":true,"vindexes":{"v":{"type":"xxhash"},"\u0076":{"type":"xxhash"}},"tables":{}}`), want: "duplicate object member"},
		{name: "unknown root", data: []byte(`{"sharded":true,"vindexes":{},"tables":{},"routing_rules":{}}`), want: "unknown field"},
		{name: "case variant unknown", data: []byte(`{"Sharded":true,"vindexes":{},"tables":{}}`), want: "unknown field"},
		{name: "unknown nested", data: []byte(`{"sharded":true,"vindexes":{"v":{"type":"xxhash","owner":"t"}},"tables":{}}`), want: "unknown field"},
		{name: "trailing value", data: []byte(valid + `{}`), want: "after top-level value"},
		{name: "excessive depth", data: []byte(deep), want: "maximum nesting depth"},
		{name: "invalid UTF-8", data: append([]byte(`{"sharded":true,"vindexes":{},"tables":{"`), 0xff), want: "invalid UTF-8"},
		{name: "raw NUL", data: append([]byte(valid), 0), want: "NUL byte"},
		{name: "escaped NUL", data: []byte(`{"sharded":true,"vindexes":{"bad\u0000":{"type":"xxhash"}},"tables":{}}`), want: "NUL in decoded object member"},
		{name: "numeric param", data: []byte(`{"sharded":true,"vindexes":{"v":{"type":"multicol","params":{"column_count":1}}},"tables":{}}`), want: "cannot decode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadKeyspace(test.data)
			if err == nil || !errors.Is(err, ErrUnsupportedVSchema) {
				t.Fatalf("LoadKeyspace() error = %v, want typed rejection", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadKeyspace() error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadKeyspaceRejectsOversizedJSONBeforeParsing(t *testing.T) {
	data := bytes.Repeat([]byte{' '}, maxVSchemaJSONBytes+1)
	_, err := LoadKeyspace(data)
	if err == nil || !errors.Is(err, ErrUnsupportedVSchema) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("LoadKeyspace() error = %v, want typed size rejection", err)
	}
}

func BenchmarkLoadKeyspaceStrictJSON(b *testing.B) {
	data := []byte(`{"sharded":true,"vindexes":{"xx":{"type":"xxhash"}},"tables":{"messages":{"column_vindexes":[{"column":"tenant_id","name":"xx"}]}}}`)
	b.Run("vibejson-canonical", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keyspace, err := LoadKeyspace(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkKeyspaceSink = keyspace
		}
	})
	b.Run("encoding-json-former", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			var raw rawKeyspace
			if err := decoder.Decode(&raw); err != nil {
				b.Fatal(err)
			}
			if decoder.More() {
				b.Fatal("trailing data")
			}
			keyspace, err := buildKeyspace(raw)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkKeyspaceSink = keyspace
		}
	})
}

var benchmarkKeyspaceSink *Keyspace
