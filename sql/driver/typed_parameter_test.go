package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

func TestPreparedTypedValuesParameterMetadata(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	prepared := runtimePrepare(t, session,
		`VALUES (BOOL 't', TEXT 'x'), (?, ?)`)
	defer prepared.Close()

	if prepared.NumParams() != 2 ||
		prepared.ParamKind(0) != ParamScalar ||
		prepared.ParamKind(1) != ParamScalar ||
		prepared.ParamType(0) != ParamTypeBool ||
		prepared.ParamType(1) != ParamTypeText {
		t.Fatalf("typed parameter metadata = count %d, kinds %s/%s, types %s/%s",
			prepared.NumParams(), prepared.ParamKind(0), prepared.ParamKind(1),
			prepared.ParamType(0), prepared.ParamType(1))
	}
	if prepared.ParamType(-1) != ParamTypeInvalid ||
		prepared.ParamType(2) != ParamTypeInvalid {
		t.Fatalf("out-of-range parameter types = %s/%s, want invalid",
			prepared.ParamType(-1), prepared.ParamType(2))
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if prepared.ParamType(0) != ParamTypeBool ||
			prepared.ParamType(1) != ParamTypeText {
			panic("unstable parameter metadata")
		}
	}); allocs != 0 {
		t.Fatalf("typed parameter lookup allocated %.2f times, want zero", allocs)
	}
}

func TestPreparedStandaloneUnknownParameterResolvesText(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	prepared := runtimePrepare(t, session, `VALUES (?)`)
	defer prepared.Close()
	if got := prepared.ParamType(0); got != ParamTypeText {
		t.Fatalf("standalone unknown parameter type = %s, want text", got)
	}
}

func TestPreparedSetCommonTypeParameterMetadata(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	prepared := runtimePrepare(t, session,
		`SELECT ? UNION ALL SELECT BOOL 't'`)
	defer prepared.Close()
	if got := prepared.ParamType(0); got != ParamTypeBool {
		t.Fatalf("set common-type parameter = %s, want boolean", got)
	}
}

func TestPreparedMutationFilterParameterMetadata(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	for _, source := range []string{
		`CREATE TABLE users (id STRING PRIMARY KEY, enabled BOOL)`,
		`CREATE TABLE other (id STRING PRIMARY KEY)`,
	} {
		statement := runtimePrepare(t, session, source)
		if _, err := statement.Exec(context.Background(), nil); err != nil {
			statement.Close()
			t.Fatal(err)
		}
		statement.Close()
	}

	prepared := runtimePrepare(t, session, `DELETE FROM users WHERE id IN `+
		`(SELECT TEXT 'x' UNION ALL SELECT ? FROM other)`)
	defer prepared.Close()
	if got := prepared.ParamType(0); got != ParamTypeText {
		t.Fatalf("mutation filter parameter type = %s, want text", got)
	}
}

func TestPrepareWithParameterTypesReachesDMLSelectChildren(t *testing.T) {
	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()

	for _, source := range []string{
		`CREATE TABLE typed_dml_source (id STRING PRIMARY KEY, flag BOOL)`,
		`CREATE TABLE typed_dml_target (id STRING PRIMARY KEY, flag BOOL)`,
	} {
		statement := runtimePrepare(t, session, source)
		if _, err := statement.Exec(context.Background(), nil); err != nil {
			statement.Close()
			t.Fatal(err)
		}
		statement.Close()
	}
	for _, parameterType := range []ParamType{
		ParamTypeBool,
		ParamTypeText,
		ParamTypeVarchar,
		ParamTypeName,
		ParamTypeBPChar,
		ParamTypeOther,
	} {
		prepared, err := session.PrepareWithParameterTypes(
			context.Background(), `DELETE FROM typed_dml_source WHERE id = ?`,
			[]ParamType{parameterType},
		)
		if err != nil {
			t.Fatalf("DML parameter type %s: %v", parameterType, err)
		}
		if got := prepared.ParamType(0); got != parameterType {
			prepared.Close()
			t.Fatalf("DML parameter type %s forwarded as %s", parameterType, got)
		}
		prepared.Close()
	}

	for _, source := range []string{
		`DELETE FROM typed_dml_source WHERE flag IN (SELECT ? UNION ALL SELECT ?)`,
		`UPDATE typed_dml_source SET flag = true WHERE flag IN ` +
			`(SELECT ? UNION ALL SELECT ?)`,
		`INSERT INTO typed_dml_target SELECT * FROM typed_dml_source WHERE flag IN ` +
			`(SELECT ? UNION ALL SELECT ?)`,
	} {
		prepared, err := session.PrepareWithParameterTypes(
			context.Background(), source,
			[]ParamType{ParamTypeBool, ParamTypeUnspecified},
		)
		if err != nil {
			t.Fatalf("PrepareWithParameterTypes(%q): %v", source, err)
		}
		if prepared.ParamType(0) != ParamTypeBool ||
			prepared.ParamType(1) != ParamTypeBool {
			prepared.Close()
			t.Fatalf("declared DML parameter types for %q = %s/%s, want boolean/boolean",
				source, prepared.ParamType(0), prepared.ParamType(1))
		}
		if prepared.ParamTypePosition(0) != strings.Index(source, "?") ||
			prepared.ParamTypePosition(1) != strings.LastIndex(source, "?") {
			prepared.Close()
			t.Fatalf("declared DML parameter positions for %q = %d/%d, want %d/%d",
				source, prepared.ParamTypePosition(0), prepared.ParamTypePosition(1),
				strings.Index(source, "?"), strings.LastIndex(source, "?"))
		}
		prepared.Close()
	}

	source := `DELETE FROM typed_dml_source WHERE flag IN ` +
		`(SELECT ? UNION ALL SELECT ?)`
	_, err := session.PrepareWithParameterTypes(
		context.Background(), source,
		[]ParamType{ParamTypeBool, ParamTypeText},
	)
	var positioned interface{ Position() int }
	position := -1
	if errors.As(err, &positioned) {
		position = positioned.Position()
	}
	if position != strings.LastIndex(source, "?") {
		t.Fatalf("declared DML mismatch = %T %v, position %d, want %d",
			err, err, position, strings.LastIndex(source, "?"))
	}
}

func TestDMLDocumentParameterTypesStayOutsideScalarAnalysis(t *testing.T) {
	documentKinds := []ParamKind{ParamDocument}
	for _, parameterType := range []ParamType{ParamTypeOther, ParamTypeText} {
		parameterTypes := []ParamType{parameterType}
		var resolved []query.ParameterType
		var err error
		if allocations := testing.AllocsPerRun(1000, func() {
			resolved, err = queryParameterTypes(parameterTypes, documentKinds)
		}); allocations != 0 {
			t.Fatalf("document-only %s type conversion allocated %.2f times, want zero",
				parameterType, allocations)
		}
		if err != nil || resolved != nil {
			t.Fatalf("document-only %s type conversion = %v, %v; want nil, nil",
				parameterType, resolved, err)
		}
	}

	database, session := openRuntimeSession(t)
	defer database.Close()
	defer session.Close()
	create := runtimePrepare(t, session,
		`CREATE TABLE typed_document_dml (id STRING PRIMARY KEY)`)
	if _, err := create.Exec(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	create.Close()
	for _, parameterType := range []ParamType{ParamTypeOther, ParamTypeText} {
		prepared, err := session.PrepareWithParameterTypes(
			context.Background(),
			`UPDATE typed_document_dml SET "$doc" = ? WHERE id = ?`,
			[]ParamType{parameterType, ParamTypeText},
		)
		if err != nil {
			t.Fatalf("document type %s reached scalar analysis: %v", parameterType, err)
		}
		if prepared.ParamKind(0) != ParamDocument ||
			prepared.ParamType(0) != ParamTypeUnspecified ||
			prepared.ParamType(1) != ParamTypeText {
			prepared.Close()
			t.Fatalf("document/scalar metadata for %s = kinds %s/%s, types %s/%s",
				parameterType, prepared.ParamKind(0), prepared.ParamKind(1),
				prepared.ParamType(0), prepared.ParamType(1))
		}
		prepared.Close()
	}
}
