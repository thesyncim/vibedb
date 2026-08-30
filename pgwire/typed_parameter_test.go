package pgwire

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestTypedValuesParameterDescriptionUsesInferredOIDs(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("typed_values",
		`VALUES (BOOL 't', TEXT 'x'), ($1, $2)`))
	c.send(msgDescribe, describeMsg(targetStatement, "typed_values"))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	if has(messages, msgErrorResponse) {
		t.Fatalf("typed Parse/Describe failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	got := decodeParameterDescription(t,
		find(t, messages, msgParameterDesc).body)
	if len(got) != 2 || got[0] != oidBool || got[1] != oidText {
		t.Fatalf("typed ParameterDescription = %v, want [%d %d]",
			got, oidBool, oidText)
	}
}

func TestSetCommonTypeParameterDescriptionAndExecution(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		bound  string
	}{
		{
			name:   "cross leaf",
			source: `SELECT $1 UNION ALL SELECT BOOL 't'`,
			bound:  "off",
		},
		{
			name: "grouped propagation",
			source: `(SELECT true UNION SELECT $1 ORDER BY 1) ` +
				`UNION ALL SELECT BOOL 'f'`,
			bound: "tr",
		},
		{
			name:   "derived propagation",
			source: `SELECT * FROM (SELECT BOOL 't' UNION ALL SELECT $1) AS q`,
			bound:  "off",
		},
		{
			name: "CTE propagation",
			source: `WITH q AS (SELECT BOOL 't' UNION ALL SELECT $1) ` +
				`SELECT * FROM q`,
			bound: "off",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("typed_set", test.source))
			c.send(msgDescribe, describeMsg(targetStatement, "typed_set"))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)
			if has(messages, msgErrorResponse) {
				t.Fatalf("typed set Parse/Describe failed: %s",
					formatError(find(t, messages, msgErrorResponse).body))
			}
			got := decodeParameterDescription(t,
				find(t, messages, msgParameterDesc).body)
			if len(got) != 1 || got[0] != oidBool {
				t.Fatalf("typed set ParameterDescription = %v, want [%d]",
					got, oidBool)
			}

			executed := extendedSQL(c, test.source,
				[][]byte{[]byte(test.bound)})
			if has(executed, msgErrorResponse) {
				t.Fatalf("typed set Execute failed: %s",
					formatError(find(t, executed, msgErrorResponse).body))
			}
			rows := rowsOf(t, executed)
			if len(rows) < 2 {
				t.Fatalf("typed set rows = %q, want at least two", rows)
			}
			for _, row := range rows {
				if len(row) != 1 || (string(row[0]) != "t" && string(row[0]) != "f") {
					t.Fatalf("typed set published non-boolean row %q", row)
				}
			}
		})
	}
}

func TestMutationFilterParameterDescriptionUsesInferredOID(t *testing.T) {
	c := connect(t)
	created := c.query(`CREATE TABLE other (id STRING PRIMARY KEY)`)
	if has(created, msgErrorResponse) {
		t.Fatalf("create other: %s",
			formatError(find(t, created, msgErrorResponse).body))
	}
	c.send(msgParse, parseMsg("typed_delete", `DELETE FROM users WHERE id IN `+
		`(SELECT TEXT 'x' UNION ALL SELECT $1 FROM other)`))
	c.send(msgDescribe, describeMsg(targetStatement, "typed_delete"))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	if has(messages, msgErrorResponse) {
		t.Fatalf("typed mutation Parse/Describe failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	got := decodeParameterDescription(t,
		find(t, messages, msgParameterDesc).body)
	if len(got) != 1 || got[0] != oidText {
		t.Fatalf("mutation ParameterDescription = %v, want [%d]", got, oidText)
	}
}

func TestRepeatedWireParameterCannotHaveConflictingInferredTypes(t *testing.T) {
	c := connect(t)
	source := `VALUES (BOOL 't', TEXT 'x'), ($1, $1)`
	c.send(msgParse, parseMsg("conflicting", source))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateAmbiguousParameter)
	wantPosition := strings.LastIndex(source, "$1") + 1
	if fields['P'] != fmt.Sprint(wantPosition) || fields['D'] != "boolean versus text" {
		t.Fatalf("repeated parameter error fields = %v, want position %d and PostgreSQL detail",
			fields, wantPosition)
	}
}

func TestDeclaredParameterTypeParticipatesInSetResolution(t *testing.T) {
	for _, test := range []struct {
		name       string
		oids       []int32
		want       []int32
		resultOID  int32
		resultSize int16
	}{
		{
			name: "boolean and unknown", oids: []int32{oidBool, oidUnknown},
			want: []int32{oidBool, oidBool}, resultOID: oidBool, resultSize: 1,
		},
		{
			name: "varchar and unknown", oids: []int32{oidVarchar, oidUnknown},
			want: []int32{oidVarchar, oidVarchar}, resultOID: oidVarchar, resultSize: -1,
		},
		{
			name: "name and unknown", oids: []int32{oidName, oidUnknown},
			want: []int32{oidName, oidName}, resultOID: oidName, resultSize: 64,
		},
		{
			name: "bpchar and unknown", oids: []int32{oidBPChar, oidUnknown},
			want: []int32{oidBPChar, oidBPChar}, resultOID: oidBPChar, resultSize: -1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("declared_set",
				`SELECT $1 UNION ALL SELECT $2`, test.oids...))
			c.send(msgDescribe, describeMsg(targetStatement, "declared_set"))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)
			if has(messages, msgErrorResponse) {
				t.Fatalf("declared set Parse failed: %s",
					formatError(find(t, messages, msgErrorResponse).body))
			}
			got := decodeParameterDescription(t,
				find(t, messages, msgParameterDesc).body)
			if !slices.Equal(got, test.want) {
				t.Fatalf("declared set ParameterDescription = %v, want %v", got, test.want)
			}
			description := decodeRowDescription(t,
				find(t, messages, msgRowDescription).body)
			if len(description) != 1 || description[0].oid != test.resultOID ||
				description[0].size != test.resultSize {
				t.Fatalf("declared set RowDescription = %+v, want OID %d size %d",
					description, test.resultOID, test.resultSize)
			}
		})
	}
}

func TestDeclaredStringCandidateBindPreservesPostgreSQLDomainSemantics(t *testing.T) {
	const source = `SELECT $1 UNION ALL SELECT $2`
	for _, test := range []struct {
		name      string
		oid       int32
		left      string
		right     string
		wantLeft  string
		wantRight string
	}{
		{
			name: "varchar preserves blanks", oid: oidVarchar,
			left: "head   ", right: "tail   ",
			wantLeft: "head   ", wantRight: "tail   ",
		},
		{
			name: "name clips inferred unknown", oid: oidName,
			left: "head", right: strings.Repeat("n", 64),
			wantLeft: "head", wantRight: strings.Repeat("n", 63),
		},
		{
			name: "bpchar preserves blanks", oid: oidBPChar,
			left: "head   ", right: "tail   ",
			wantLeft: "head   ", wantRight: "tail   ",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("declared_string", source, test.oid, oidUnknown))
			c.send(msgBind, bindMsg("", "declared_string", nil,
				[][]byte{[]byte(test.left), []byte(test.right)}, nil))
			c.send(msgExecute, executeMsg("", 0))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)
			if has(messages, msgErrorResponse) {
				t.Fatalf("declared string Execute failed: %s",
					formatError(find(t, messages, msgErrorResponse).body))
			}
			rows := rowsOf(t, messages)
			if len(rows) != 2 || len(rows[0]) != 1 || len(rows[1]) != 1 ||
				string(rows[0][0]) != test.wantLeft ||
				string(rows[1][0]) != test.wantRight {
				t.Fatalf("declared string rows = %q, want %q/%q",
					rows, test.wantLeft, test.wantRight)
			}
		})
	}
}

func TestDeclaredUnsupportedOIDFailsTypedCaseAsPositionedUndefinedOperator(t *testing.T) {
	const source = `SELECT CASE BOOL 't' WHEN $1 THEN BOOL 't' ELSE BOOL 'f' END`
	c := connect(t)
	c.send(msgParse, parseMsg("unsupported_case", source, oidInt4))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateUndefinedFunction)
	wantPosition := strings.Index(source, "$1") + 1
	if fields['P'] != fmt.Sprint(wantPosition) {
		t.Fatalf("undefined operator position = %q, want %d; fields %v",
			fields['P'], wantPosition, fields)
	}
}

func TestDeclaredParameterTypesReachDMLSelectChildren(t *testing.T) {
	c := connect(t)
	for _, source := range []string{
		`CREATE TABLE typed_dml_source (id STRING PRIMARY KEY, flag BOOL)`,
		`CREATE TABLE typed_dml_target (id STRING PRIMARY KEY, flag BOOL)`,
	} {
		messages := c.query(source)
		if has(messages, msgErrorResponse) {
			t.Fatalf("DML type fixture %q: %s", source,
				formatError(find(t, messages, msgErrorResponse).body))
		}
	}

	for index, source := range []string{
		`DELETE FROM typed_dml_source WHERE flag IN ` +
			`(SELECT $1 UNION ALL SELECT $2)`,
		`UPDATE typed_dml_source SET flag = true WHERE flag IN ` +
			`(SELECT $1 UNION ALL SELECT $2)`,
		`INSERT INTO typed_dml_target SELECT * FROM typed_dml_source WHERE flag IN ` +
			`(SELECT $1 UNION ALL SELECT $2)`,
	} {
		name := fmt.Sprintf("declared_dml_%d", index)
		c.send(msgParse, parseMsg(name, source, oidBool, oidUnknown))
		c.send(msgDescribe, describeMsg(targetStatement, name))
		c.send(msgSync, nil)
		messages := c.until(msgReadyForQuery)
		if has(messages, msgErrorResponse) {
			t.Fatalf("declared DML Parse %q failed: %s", source,
				formatError(find(t, messages, msgErrorResponse).body))
		}
		got := decodeParameterDescription(t,
			find(t, messages, msgParameterDesc).body)
		want := []int32{oidBool, oidBool}
		if !slices.Equal(got, want) {
			t.Fatalf("declared DML ParameterDescription for %q = %v, want %v",
				source, got, want)
		}
	}
}

func TestDeclaredDMLSetTypeMismatchHasPostgreSQLPosition(t *testing.T) {
	c := connect(t)
	source := `DELETE FROM users WHERE flag IN ` +
		`(SELECT $1 UNION ALL SELECT $2)`
	c.send(msgParse, parseMsg("declared_dml_conflict", source, oidBool, oidText))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateDatatypeMismatch)
	want := strings.LastIndex(source, "$2") + 1
	if fields['P'] != fmt.Sprint(want) {
		t.Fatalf("declared DML mismatch position = %q, want %d; fields %v",
			fields['P'], want, fields)
	}
}

func TestDeclaredDocumentOIDStaysOutsideDMLScalarAnalysis(t *testing.T) {
	c := connect(t)
	const source = `UPDATE users SET "$doc" = $1 WHERE _pgwire_key = $2`
	for index, documentOID := range []int32{oidJSON, oidJSONB, oidBytea, oidText} {
		name := fmt.Sprintf("declared_document_%d", index)
		c.send(msgParse, parseMsg(name, source, documentOID, oidText))
		c.send(msgDescribe, describeMsg(targetStatement, name))
		c.send(msgSync, nil)
		messages := c.until(msgReadyForQuery)
		if has(messages, msgErrorResponse) {
			t.Fatalf("declared document OID %d reached scalar analysis: %s",
				documentOID, formatError(find(t, messages, msgErrorResponse).body))
		}
		got := decodeParameterDescription(t,
			find(t, messages, msgParameterDesc).body)
		want := []int32{documentOID, oidText}
		if !slices.Equal(got, want) {
			t.Fatalf("declared document OID %d description = %v, want %v",
				documentOID, got, want)
		}
	}
}

func TestDeclaredSetTypeMismatchHasPostgreSQLPosition(t *testing.T) {
	c := connect(t)
	source := `SELECT $1 UNION ALL SELECT $2`
	c.send(msgParse, parseMsg("declared_conflict", source, oidBool, oidText))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateDatatypeMismatch)
	want := strings.LastIndex(source, "$2") + 1
	if fields['P'] != fmt.Sprint(want) {
		t.Fatalf("declared mismatch position = %q, want %d; fields %v",
			fields['P'], want, fields)
	}
}

func TestTypedParameterRejectsIncompatibleDeclaredOID(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("wrong_bool",
		`VALUES (BOOL 't'), ($1)`, oidText))
	c.send(msgSync, nil)
	expectError(t, c.until(msgReadyForQuery), sqlstateDatatypeMismatch)
}

func TestTypedParameterPreservesCompatibleDeclaredOID(t *testing.T) {
	c := connect(t)
	c.send(msgParse, parseMsg("varchar_text",
		`VALUES (TEXT 'x'), ($1)`, oidVarchar))
	c.send(msgDescribe, describeMsg(targetStatement, "varchar_text"))
	c.send(msgSync, nil)
	messages := c.until(msgReadyForQuery)
	if has(messages, msgErrorResponse) {
		t.Fatalf("compatible typed Parse failed: %s",
			formatError(find(t, messages, msgErrorResponse).body))
	}
	got := decodeParameterDescription(t,
		find(t, messages, msgParameterDesc).body)
	if len(got) != 1 || got[0] != oidVarchar {
		t.Fatalf("compatible ParameterDescription = %v, want [%d]", got, oidVarchar)
	}

	c.send(msgParse, parseMsg("unknown_bool",
		`VALUES (BOOL 't'), ($1)`, oidUnknown))
	c.send(msgDescribe, describeMsg(targetStatement, "unknown_bool"))
	c.send(msgSync, nil)
	messages = c.until(msgReadyForQuery)
	got = decodeParameterDescription(t,
		find(t, messages, msgParameterDesc).body)
	if len(got) != 1 || got[0] != oidBool {
		t.Fatalf("resolved UNKNOWN ParameterDescription = %v, want [%d]", got, oidBool)
	}
}

func TestDeclaredStringCategoryCoercionMatchesPostgreSQL(t *testing.T) {
	for _, test := range []struct {
		name string
		oid  int32
		raw  string
		want string
	}{
		{
			name: "name truncation", oid: oidName,
			raw: strings.Repeat("n", 70), want: strings.Repeat("n", 63),
		},
		{name: "bpchar blank trimming", oid: oidBPChar, raw: "tail   ", want: "tail"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("typed_text",
				`VALUES (TEXT 'base'), ($1)`, test.oid))
			c.send(msgBind, bindMsg("", "typed_text", nil,
				[][]byte{[]byte(test.raw)}, nil))
			c.send(msgExecute, executeMsg("", 0))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)
			if has(messages, msgErrorResponse) {
				t.Fatalf("typed string bind failed: %s",
					formatError(find(t, messages, msgErrorResponse).body))
			}
			rows := rowsOf(t, messages)
			if len(rows) != 2 || string(rows[1][0]) != test.want {
				t.Fatalf("typed string rows = %q, want second value %q", rows, test.want)
			}
		})
	}
}

func TestTypedTextParametersRejectNULBeforeTypeInput(t *testing.T) {
	for _, source := range []string{
		`VALUES (BOOL 't'), ($1)`,
		`VALUES (TEXT 'x'), ($1)`,
	} {
		c := connect(t)
		expectError(t, extendedSQL(c, source,
			[][]byte{{'t', 0}}), sqlstateCharacterNotInRepertoire)
	}
}

func TestInferredBooleanParameterUsesPostgreSQLInputGrammar(t *testing.T) {
	c := connect(t)
	messages := extendedSQL(c,
		`VALUES (BOOL 'f'), ($1)`, [][]byte{[]byte("  Tr  ")})
	rows := rowsOf(t, messages)
	if len(rows) != 2 || string(rows[0][0]) != "f" || string(rows[1][0]) != "t" {
		t.Fatalf("typed boolean rows = %q", rows)
	}
	expectError(t, extendedSQL(c,
		`VALUES (BOOL 't'), ($1)`, [][]byte{[]byte("o")}),
		sqlstateInvalidTextRepresentation)
}

func TestTypedBooleanBindAllocatesZeroAfterWarmup(t *testing.T) {
	stmt := &prepared{
		wireParams: 1,
		paramKinds: []sqldriver.ParamKind{sqldriver.ParamScalar},
		paramTypes: []sqldriver.ParamType{sqldriver.ParamTypeBool},
	}
	slot := boundValueSlot{}
	var decodeStore []byte
	raw := []byte(" FaLs ")
	for range 3 {
		if _, err := bindParameter(raw, formatText, stmt.parameterOID(0),
			stmt.paramKind(0), stmt.paramType(0), &slot, &decodeStore); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		value, err := bindParameter(raw, formatText, stmt.parameterOID(0),
			stmt.paramKind(0), stmt.paramType(0), &slot, &decodeStore)
		if err != nil || value != &slot.boolean || slot.boolean {
			panic("unstable typed boolean bind")
		}
	}); allocs != 0 {
		t.Fatalf("typed boolean Bind allocated %.2f times, want zero", allocs)
	}
}

func TestDeclaredOccurrenceTypesAbsentPathAllocatesZero(t *testing.T) {
	order := []int{1, 1}
	for _, declared := range [][]int32{nil, {oidUnknown}} {
		if allocations := testing.AllocsPerRun(1000, func() {
			if got := declaredOccurrenceTypes(declared, order); got != nil {
				panic("unresolved declaration produced type metadata")
			}
		}); allocations != 0 {
			t.Fatalf("absent declared-type path allocated %.2f times, want zero",
				allocations)
		}
	}
}

func TestInferredBooleanBinaryLengthSQLStateMatchesPostgreSQL(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  []byte
		code string
	}{
		{name: "under length", raw: []byte{}, code: sqlstateProtocolViolation},
		{name: "over length", raw: []byte{0, 1}, code: sqlstateInvalidBinaryRepresentation},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("", `VALUES (BOOL 't'), ($1)`))
			c.send(msgBind, bindMsg("", "", []int16{formatBinary}, [][]byte{test.raw}, nil))
			c.send(msgSync, nil)
			expectError(t, c.until(msgReadyForQuery), test.code)
		})
	}
}
