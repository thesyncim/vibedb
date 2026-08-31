package pgwire

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

type transportedSQLDiagnostic struct {
	state       string
	message     string
	hint        string
	position    int
	hasPosition bool
}

func (e *transportedSQLDiagnostic) Error() string            { return e.message }
func (e *transportedSQLDiagnostic) SQLState() string         { return e.state }
func (e *transportedSQLDiagnostic) SQLHint() string          { return e.hint }
func (e *transportedSQLDiagnostic) SQLPosition() (int, bool) { return e.position, e.hasPosition }

func TestTransportedSQLDiagnosticPreservesFieldsAndConvertsPosition(t *testing.T) {
	const source = `SELECT 'é' / 0`
	bytePosition := strings.Index(source, "/")
	err := &transportedSQLDiagnostic{
		state: "22012", message: "division by zero", hint: "use a nonzero divisor",
		position: bytePosition, hasPosition: true,
	}
	got := asPGErrorIn(err, source)
	wantPosition := utf8.RuneCountInString(source[:bytePosition]) + 1
	if got.code != err.state || got.message != err.message || got.hint != err.hint || got.position != wantPosition {
		t.Fatalf("transported diagnostic = %+v, want code=%s message=%q hint=%q position=%d",
			got, err.state, err.message, err.hint, wantPosition)
	}
	if !errors.Is(got, err) {
		t.Fatal("transported diagnostic lost its typed cause")
	}
}

func TestTransportedSQLDiagnosticFailsClosedWithoutValidState(t *testing.T) {
	tests := []error{
		errors.New("ordinary remote failure"),
		&transportedSQLDiagnostic{state: "2200x", message: "invalid lowercase state"},
		&transportedSQLDiagnostic{state: "2200", message: "short state"},
	}
	for _, err := range tests {
		if got := asPGErrorIn(err, "SELECT 1"); got.code != sqlstateInternalError || got.position != 0 {
			t.Fatalf("%T %q => code=%s position=%d, want XX000/no position", err, err, got.code, got.position)
		}
	}

	withoutPosition := &transportedSQLDiagnostic{state: "22P02", message: "invalid input"}
	if got := asPGErrorIn(withoutPosition, "é"); got.code != "22P02" || got.position != 0 {
		t.Fatalf("absent transported position => code=%s position=%d", got.code, got.position)
	}
}
