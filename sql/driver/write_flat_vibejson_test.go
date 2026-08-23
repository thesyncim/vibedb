package driver

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestFlatInsertVibeJSONMatchesLegacyScalarBytes(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (value) VALUES (?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	boolValue := true
	intValue := int64(-17)
	floatValue := 1.25
	stringValue := "pointer <>& "
	numberValue := query.Number("9007199254740993")
	for _, value := range []any{
		nil, false,
		int(-1), int8(-8), int16(-16), int32(-32), int64(-64),
		uint(1), uint8(8), uint16(16), uint32(32), uint64(math.MaxUint64),
		float32(0.1), float64(-123.5),
		"text <>&  ", []byte("borrowed <bytes>"),
		query.Number("1.2300e+9"), stdjson.Number("-0.125E-2"),
		&boolValue, &intValue, &floatValue, &stringValue, &numberValue,
	} {
		got, err := encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, []any{value}, 1<<20,
		)
		if err != nil {
			t.Fatalf("encode %T: %v", value, err)
		}
		wantValue, err := legacyFlatInsertValue(row.Values[0], []any{value})
		if err != nil {
			t.Fatalf("legacy value %T: %v", value, err)
		}
		want, err := stdjson.Marshal(map[string]any{"value": wantValue})
		if err != nil {
			t.Fatalf("legacy marshal %T: %v", value, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("encode %T = %s, want %s", value, got, want)
		}
	}
}

func TestFlatInsertVibeJSONRawValueUsesExactNumberContract(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (value) VALUES (?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	rawNumber := vibejson.RawValue{Src: []byte("9007199254740993")}
	got, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, []any{rawNumber}, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"value":9007199254740993}`
	if string(got) != want {
		t.Fatalf("raw-number binding = %s, want %s", got, want)
	}
	// The removed map encoder treated RawValue as an ordinary exported Go
	// struct and therefore encoded Src as base64. RawValue is accepted only
	// after NumberBytes validation, so preserving its exact numeric spelling is
	// the deliberate scalar-binding contract rather than legacy byte parity.
	legacy, err := stdjson.Marshal(map[string]any{"value": rawNumber})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, legacy) {
		t.Fatalf("raw-number binding retained the legacy struct/base64 encoding: %s", got)
	}
}

func TestFlatInsertVibeJSONFloatSizeAndAdmission(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (value) VALUES (?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	boundary := -math.Nextafter(1e-6, math.Inf(1))
	pointerBoundary := boundary
	values := []any{
		float32(0.1), float32(math.SmallestNonzeroFloat32),
		0.0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64,
		math.Nextafter(1e-6, 0), 1e-6, math.Nextafter(1e-6, math.Inf(1)),
		-math.Nextafter(1e-6, 0), -1e-6, boundary,
		math.Nextafter(1e21, 0), 1e21, math.Nextafter(1e21, math.Inf(1)),
		-math.Nextafter(1e21, 0), -1e21, -math.Nextafter(1e21, math.Inf(1)),
		&pointerBoundary,
	}
	for _, value := range values {
		normalized := value
		switch value := value.(type) {
		case float32:
			normalized = float64(value)
		case *float64:
			normalized = *value
		}
		wantScalar, err := stdjson.Marshal(normalized)
		if err != nil {
			t.Fatalf("marshal %T(%v): %v", value, normalized, err)
		}
		scalarBytes, err := flatScalarEncodedCapacity(value, math.MaxUint64)
		if err != nil || scalarBytes != uint64(len(wantScalar)) {
			t.Fatalf("capacity %T(%v) = (%d, %v), want %d for %s",
				value, normalized, scalarBytes, err, len(wantScalar), wantScalar)
		}
		got, err := encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, []any{value}, 1<<20,
		)
		if err != nil {
			t.Fatalf("encode %T(%v): %v", value, normalized, err)
		}
		if _, err := encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, []any{value}, len(got),
		); err != nil {
			t.Fatalf("exact admission %T(%v), bytes=%d: %v", value, normalized, len(got), err)
		}
		if _, err := encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, []any{value}, len(got)-1,
		); !errors.Is(err, durable.ErrDocumentTooLarge) {
			t.Fatalf("undersized admission %T(%v) = %v, want ErrDocumentTooLarge",
				value, normalized, err)
		}
	}
	args := []any{boundary}
	if _, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, args, 1<<20,
	); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		flatInsertBenchmarkSink, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, args, 1<<20,
		)
		if err != nil {
			panic(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("boundary float flat INSERT allocated %.2f times, want one owned output", allocs)
	}
}

func TestFlatJSONStringEncodedBytesMatchesEncoder(t *testing.T) {
	for _, value := range []string{
		"",
		"plain ASCII",
		"quotes \\\" and slash \\\\",
		"controls \b\f\n\r\t\x00\x01\x1f",
		"HTML <>&",
		"line separators   and  ",
		"Unicode café 世界 🚀",
		strings.Repeat("<>&\\\"\n  ", 64),
	} {
		want, err := stdjson.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := flatJSONStringEncodedBytes(value, math.MaxUint64)
		if err != nil || got != uint64(len(want)) {
			t.Fatalf("encoded bytes for %q = (%d, %v), want %d", value, got, err, len(want))
		}
		if got != 0 {
			if _, err := flatJSONStringEncodedBytes(value, got-1); err == nil {
				t.Fatalf("encoded bytes for %q fit below exact size %d", value, got)
			}
		}
	}
	if _, err := flatJSONStringEncodedBytes(string([]byte{0xff}), math.MaxUint64); err == nil {
		t.Fatal("invalid UTF-8 string received an encoded size")
	}
}

func TestFlatInsertVibeJSONBindingErrorsPrecedeSizeErrors(t *testing.T) {
	statement, err := query.PrepareDML(
		`INSERT INTO docs ("` + strings.Repeat("key", 64) + `") VALUES (?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	if _, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, nil, 1,
	); err == nil || errors.Is(err, durable.ErrDocumentTooLarge) ||
		!strings.Contains(err.Error(), "was not bound") {
		t.Fatalf("oversized key with unbound operand = %v, want binding error", err)
	}
	invalid := append(bytes.Repeat([]byte{'x'}, 2048), 0xff)
	if _, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, []any{invalid}, 1024,
	); err == nil || errors.Is(err, durable.ErrDocumentTooLarge) ||
		!strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("oversized invalid UTF-8 operand = %v, want UTF-8 binding error", err)
	}
}

func TestFlatInsertVibeJSONCompilesSortedFields(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (z, a, middle) VALUES (?, ?, ?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	got, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, &insert.Rows[0],
		[]any{"last", 1, true}, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"a":1,"middle":true,"z":"last"}`
	if string(got) != want {
		t.Fatalf("encoded duplicate fields = %s, want %s", got, want)
	}
	ordinals := statement.InsertFlatFieldOrdinals()
	if len(ordinals) != 3 || ordinals[0] != 1 || ordinals[1] != 2 || ordinals[2] != 0 ||
		cap(ordinals) != len(ordinals) {
		t.Fatalf("compiled field ordinals = %v len/cap=%d/%d", ordinals, len(ordinals), cap(ordinals))
	}
}

func TestFlatInsertVibeJSONCompilesEscapedKeyBytes(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (\"<>&\", \"line  separator\") VALUES (?, ?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	var wantKeyBytes uint64
	for _, column := range insert.Columns {
		encoded, err := stdjson.Marshal(column.Segments[0].Key)
		if err != nil {
			t.Fatal(err)
		}
		wantKeyBytes += uint64(len(encoded))
	}
	if got := statement.InsertFlatKeyJSONBytes(); got != wantKeyBytes {
		t.Fatalf("compiled escaped-key bytes = %d, want %d", got, wantKeyBytes)
	}
	args := []any{"html", "line"}
	got, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, &insert.Rows[0], args, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	want, err := stdjson.Marshal(map[string]any{
		"<>&":             "html",
		"line  separator": "line",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("escaped-key encode = %s, want %s", got, want)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		flatInsertBenchmarkSink, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, &insert.Rows[0], args, 1<<20,
		)
		if err != nil {
			panic(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("escaped-key flat INSERT allocated %.2f times, want one owned output", allocs)
	}
}

func TestFlatInsertVibeJSONRejectsInvalidNumbersAndOwnsOutput(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (value) VALUES (?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	for _, value := range []any{
		query.Number("01"), stdjson.Number("1."),
		vibejson.RawValue{Src: []byte("--1")},
	} {
		if _, err := encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, []any{value}, 1<<20,
		); err == nil {
			t.Fatalf("invalid %T number encoded", value)
		}
	}
	raw := []byte("borrowed")
	encoded, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, []any{raw}, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	if string(encoded) != `{"value":"borrowed"}` {
		t.Fatalf("encoded output retained caller bytes: %s", encoded)
	}
	rawNumber := vibejson.RawValue{Src: []byte("9007199254740993")}
	encoded, err = encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, []any{rawNumber}, 1<<20,
	)
	if err != nil || string(encoded) != `{"value":9007199254740993}` {
		t.Fatalf("raw numeric value = %s, %v", encoded, err)
	}
}

func TestFlatInsertVibeJSONWarmAllocation(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (id, label, score, active) VALUES (?, ?, ?, ?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	args := []any{"id", "allocation-free planning", query.Number("1.25"), true}
	if _, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, args, 1<<20,
	); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		flatInsertBenchmarkSink, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, args, 1<<20,
		)
		if err != nil {
			panic(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("flat INSERT encoding allocated %.2f times, want only the owned output", allocs)
	}
}

func TestFlatInsertVibeJSONEscapeHeavyWarmAllocation(t *testing.T) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (value) VALUES (?)",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	value := strings.Repeat("<>&\\\"\b\f\n\r\t\x00\x1f   café 世界", 64)
	args := []any{value}
	want, err := stdjson.Marshal(map[string]any{"value": value})
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, args, 1<<20,
	)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("escape-heavy encode = (%s, %v), want %s", got, err, want)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		flatInsertBenchmarkSink, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, args, 1<<20,
		)
		if err != nil {
			panic(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("escape-heavy flat INSERT allocated %.2f times, want only the owned output", allocs)
	}
}

func BenchmarkFlatInsertVibeJSON(b *testing.B) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (id, label, score, active) VALUES (?, ?, ?, ?)",
	)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	args := []any{"id", strings.Repeat("x", 128), query.Number("1.25"), true}
	encoded, err := encodeFlatInsertDocument(
		statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
		insert, row, args, 1<<20,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		flatInsertBenchmarkSink, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, args, 1<<20,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlatInsertStdJSONLegacy(b *testing.B) {
	statement, err := query.PrepareDML(
		"INSERT INTO docs (id, label, score, active) VALUES (?, ?, ?, ?)",
	)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	insert := statement.Tree().Insert
	row := &insert.Rows[0]
	args := []any{"id", strings.Repeat("x", 128), query.Number("1.25"), true}
	values := make(map[string]any, len(insert.Columns))
	for index, column := range insert.Columns {
		values[column.Segments[0].Key], err = legacyFlatInsertValue(row.Values[index], args)
		if err != nil {
			b.Fatal(err)
		}
	}
	encoded, err := stdjson.Marshal(values)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		values := make(map[string]any, len(insert.Columns))
		for index, column := range insert.Columns {
			values[column.Segments[0].Key], err = legacyFlatInsertValue(row.Values[index], args)
			if err != nil {
				b.Fatal(err)
			}
		}
		flatInsertBenchmarkSink, err = stdjson.Marshal(values)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func legacyFlatInsertValue(operand sqlast.Operand, args []any) (any, error) {
	value, err := operandValue(operand, args)
	if err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case query.Number:
		return stdjson.Number(value), nil
	case *query.Number:
		return stdjson.Number(*value), nil
	case []byte:
		return string(value), nil
	case float32:
		return float64(value), nil
	case *bool:
		return *value, nil
	case *int64:
		return *value, nil
	case *float64:
		return *value, nil
	case *string:
		return *value, nil
	default:
		return value, nil
	}
}

var flatInsertBenchmarkSink []byte
