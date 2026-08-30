package pgwire

import (
	"strings"
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestDeclaredNameBindMatchesPostgreSQLTextAndBinaryBoundaries(t *testing.T) {
	const source = `VALUES (TEXT 'base'), ($1)`
	tests := []struct {
		name   string
		format int16
		raw    string
		want   string
		code   string
		detail string
	}{
		{
			name: "text 63 bytes",
			raw:  strings.Repeat("n", 63),
			want: strings.Repeat("n", 63),
		},
		{
			name: "text 64 bytes clips",
			raw:  strings.Repeat("n", 64),
			want: strings.Repeat("n", 63),
		},
		{
			name: "text clipping preserves UTF-8",
			raw:  strings.Repeat("n", 62) + "é",
			want: strings.Repeat("n", 62),
		},
		{
			name:   "binary 63 bytes",
			format: formatBinary,
			raw:    strings.Repeat("n", 63),
			want:   strings.Repeat("n", 63),
		},
		{
			name:   "binary 64 bytes rejects",
			format: formatBinary,
			raw:    strings.Repeat("n", 64),
			code:   sqlstateNameTooLong,
			detail: "Identifier must be less than 64 characters.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := connect(t)
			c.send(msgParse, parseMsg("typed_name", source, oidName))
			c.send(msgBind, bindMsg("", "typed_name", []int16{test.format},
				[][]byte{[]byte(test.raw)}, nil))
			c.send(msgExecute, executeMsg("", 0))
			c.send(msgSync, nil)
			messages := c.until(msgReadyForQuery)

			if test.code != "" {
				fields := expectError(t, messages, test.code)
				if fields['D'] != test.detail {
					t.Fatalf("binary name error detail = %q, want %q; fields %v",
						fields['D'], test.detail, fields)
				}
				return
			}
			if has(messages, msgErrorResponse) {
				t.Fatalf("declared name Bind failed: %s",
					formatError(find(t, messages, msgErrorResponse).body))
			}
			rows := rowsOf(t, messages)
			if len(rows) != 2 || len(rows[1]) != 1 || string(rows[1][0]) != test.want {
				t.Fatalf("declared name rows = %q, want second value %q", rows, test.want)
			}
		})
	}
}

func TestInferredNameRejectsOverlengthBinaryUnknown(t *testing.T) {
	const source = `SELECT $1 UNION ALL SELECT $2`
	c := connect(t)
	c.send(msgParse, parseMsg("inferred_name", source, oidName, oidUnknown))
	c.send(msgBind, bindMsg("", "inferred_name", []int16{formatText, formatBinary},
		[][]byte{[]byte("head"), []byte(strings.Repeat("n", 64))}, nil))
	c.send(msgSync, nil)
	fields := expectError(t, c.until(msgReadyForQuery), sqlstateNameTooLong)
	const wantDetail = "Identifier must be less than 64 characters."
	if fields['D'] != wantDetail {
		t.Fatalf("inferred binary name detail = %q, want %q; fields %v",
			fields['D'], wantDetail, fields)
	}
}

func TestDeclaredStringInputSemanticsDoNotRequireOptionalBackendMetadata(t *testing.T) {
	stmt := &prepared{
		wireParams: 1,
		paramOIDs:  []int32{oidName},
		paramKinds: []sqldriver.ParamKind{sqldriver.ParamScalar},
	}
	slot := boundValueSlot{}
	var decodeStore []byte
	value, err := bindParameter(
		[]byte(strings.Repeat("n", 64)), formatText, stmt.parameterOID(0),
		stmt.paramKind(0), stmt.paramType(0), &slot, &decodeStore,
	)
	if err != nil || value != &slot.text ||
		slot.text != strings.Repeat("n", 63) {
		t.Fatalf("metadata-free text name = %q/%v, want 63-byte clipped name",
			slot.text, err)
	}
	_, err = bindParameter(
		[]byte(strings.Repeat("n", 64)), formatBinary, stmt.parameterOID(0),
		stmt.paramKind(0), stmt.paramType(0), &slot, &decodeStore,
	)
	pg, ok := err.(*pgError)
	if !ok || pg.code != sqlstateNameTooLong ||
		pg.detail != "Identifier must be less than 64 characters." {
		t.Fatalf("metadata-free binary name error = %T %v, want 42622 with Detail",
			err, err)
	}

	stmt.paramOIDs[0] = oidBPChar
	value, err = bindParameter(
		[]byte("tail   "), formatText, stmt.parameterOID(0),
		stmt.paramKind(0), stmt.paramType(0), &slot, &decodeStore,
	)
	if err != nil || value != &slot.text || slot.text != "tail   " {
		t.Fatalf("metadata-free bpchar = %q/%v, want trailing blanks preserved",
			slot.text, err)
	}
}
