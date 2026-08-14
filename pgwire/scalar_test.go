package pgwire

import (
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestScalarOrderByAmbiguousOutputSQLState(t *testing.T) {
	const source = `SELECT id + 1 AS score, age + 1 AS score FROM users ORDER BY score`
	_, err := sqlast.Parse(source)
	if err == nil {
		t.Fatal("ambiguous ORDER BY output parsed")
	}
	pg := asPGErrorIn(err, source)
	if pg.code != sqlstateAmbiguousColumn ||
		pg.position != strings.LastIndex(source, "score")+1 {
		t.Fatalf("ambiguous scalar ORDER BY => code=%s position=%d: %v", pg.code, pg.position, err)
	}
}

func TestScalarSQLStateMappingAndUTF8Position(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{&query.ScalarTypeError{Pos: 3, Operation: "addition", Left: query.TypeString, Right: query.TypeNumber}, sqlstateDatatypeMismatch},
		{&query.ScalarDivisionByZeroError{Pos: 3}, sqlstateDivisionByZero},
		{&query.ScalarAggregateBudgetError{
			Pos: 3, Operation: "multiplication",
			Err: &query.AggregateBudgetError{Limit: 8, Used: 7, Requested: 2},
		}, sqlstateProgramLimitExceeded},
		{&query.ScalarNumericRangeError{Pos: 3, Operation: "division", Requested: 9, Limit: 8}, sqlstateNumericValueOutOfRange},
	}
	for _, test := range tests {
		got := asPGErrorIn(test.err, "éé+")
		if got.code != test.code || got.position != 3 {
			t.Fatalf("%T => code=%s position=%d, want %s/3", test.err, got.code, got.position, test.code)
		}
	}
}

func TestScalarSimpleExtendedRowsDescriptionsErrorsAndRecovery(t *testing.T) {
	c := connect(t)

	messages := c.query(`SELECT id + 1 AS next, name || '!' AS label FROM users WHERE age * 2 >= 60 ORDER BY id`)
	description := decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 2 || description[0].name != "next" ||
		description[1].name != "label" || description[0].oid != oidJSON ||
		description[1].oid != oidText {
		t.Fatalf("scalar RowDescription = %+v", description)
	}
	rows := rowsOf(t, messages)
	if len(rows) != 3 || string(rows[0][0]) != "2" || string(rows[0][1]) != `amy!` ||
		string(rows[1][0]) != "6" || string(rows[1][1]) != `dee!` ||
		string(rows[2][0]) != "7" || rows[2][1] != nil {
		t.Fatalf("simple scalar rows = %q", rows)
	}

	messages = extendedSQL(c, `SELECT id * $1 + $2 AS value FROM users WHERE id = 1`,
		[][]byte{[]byte("3"), []byte("2")})
	description = decodeRowDescription(t, find(t, messages, msgRowDescription).body)
	if len(description) != 1 || description[0].name != "value" || description[0].oid != oidJSON {
		t.Fatalf("extended scalar RowDescription = %+v", description)
	}
	rows = rowsOf(t, messages)
	if len(rows) != 1 || string(rows[0][0]) != "5" {
		t.Fatalf("extended scalar rows = %q", rows)
	}

	fields := expectError(t, c.query(`SELECT age || 'x' FROM users`), sqlstateDatatypeMismatch)
	if fields['P'] != "12" {
		t.Fatalf("type-error position = %q, want 12", fields['P'])
	}
	fields = expectError(t, c.query(`SELECT id / 0 FROM users`), sqlstateDivisionByZero)
	if fields['P'] != "11" {
		t.Fatalf("division position = %q, want 11", fields['P'])
	}

	messages = c.query(`SELECT id + 1 FROM users WHERE id = 1`)
	if has(messages, msgErrorResponse) || len(rowsOf(t, messages)) != 1 {
		t.Fatalf("session did not recover after scalar errors: %s", tags(messages))
	}
}

func TestScalarComputedOutputAliasOrdersBeforeLimit(t *testing.T) {
	c := connect(t)
	messages := c.query(`
		SELECT id, age + 1 AS score
		FROM users ORDER BY score DESC, id DESC LIMIT 3`)
	if has(messages, msgErrorResponse) {
		t.Fatalf("computed alias ORDER BY failed: %s", tags(messages))
	}
	rows := rowsOf(t, messages)
	if len(rows) != 3 || string(rows[0][0]) != "5" || string(rows[0][1]) != "4.6e1" ||
		string(rows[1][0]) != "6" || string(rows[1][1]) != "3.1e1" ||
		string(rows[2][0]) != "1" || string(rows[2][1]) != "3.1e1" {
		t.Fatalf("computed alias ordered rows = %q", rows)
	}
}
