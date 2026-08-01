package pgwire

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
)

const scalarCardinalityFailure = `SELECT id FROM users WHERE id = (SELECT id FROM users)`

func TestCardinalityViolationErrorClassification(t *testing.T) {
	err := fmt.Errorf("execute scalar expression: %w", &query.CardinalityViolationError{})
	pg := asPGError(err)
	if pg.code != sqlstateCardinalityViolation {
		t.Fatalf("SQLSTATE = %q, want %q", pg.code, sqlstateCardinalityViolation)
	}
	if pg.message != err.Error() {
		t.Fatalf("message = %q, want %q", pg.message, err.Error())
	}
}

func TestCardinalityViolationProtocolRecovery(t *testing.T) {
	t.Run("simple query", func(t *testing.T) {
		c := connect(t)
		fields := expectError(t, c.query(scalarCardinalityFailure),
			sqlstateCardinalityViolation)
		if fields['S'] != "ERROR" ||
			!strings.Contains(fields['M'], "more than one row") {
			t.Fatalf("cardinality ErrorResponse = severity %q message %q",
				fields['S'], fields['M'])
		}

		msgs := c.query(`SELECT id FROM users WHERE id = 1`)
		assertCardinalityRecovery(t, msgs)
	})

	t.Run("extended query", func(t *testing.T) {
		c := connect(t)
		c.send(msgParse, parseMsg("cardinality", scalarCardinalityFailure))
		c.send(msgBind, bindMsg("cardinality-portal", "cardinality", nil, nil, nil))
		c.send(msgExecute, executeMsg("cardinality-portal", 0))
		c.send(msgSync, nil)
		msgs := c.until(msgReadyForQuery)
		fields := expectError(t, msgs, sqlstateCardinalityViolation)
		if fields['S'] != "ERROR" ||
			!strings.Contains(fields['M'], "more than one row") {
			t.Fatalf("cardinality ErrorResponse = severity %q message %q",
				fields['S'], fields['M'])
		}
		if has(msgs, msgCommandComplete) || has(msgs, msgDataRow) {
			t.Fatalf("failed extended execution emitted result completion: %s", tags(msgs))
		}

		c.send(msgParse, parseMsg("recovery", `SELECT id FROM users WHERE id = 1`))
		c.send(msgBind, bindMsg("recovery-portal", "recovery", nil, nil, nil))
		c.send(msgExecute, executeMsg("recovery-portal", 0))
		c.send(msgSync, nil)
		assertCardinalityRecovery(t, c.until(msgReadyForQuery))
	})
}

func assertCardinalityRecovery(t *testing.T, msgs []backendMessage) {
	t.Helper()
	if has(msgs, msgErrorResponse) {
		t.Fatalf("connection did not recover after cardinality violation: %s",
			formatError(find(t, msgs, msgErrorResponse).body))
	}
	if got := commandTagOf(t, msgs); got != "SELECT 1" {
		t.Fatalf("recovery CommandComplete = %q, want SELECT 1", got)
	}
}
