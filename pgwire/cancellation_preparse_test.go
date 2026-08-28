package pgwire

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func cancelOnCheck(n int) (func() error, *int) {
	checks := 0
	return func() error {
		checks++
		if checks == n {
			return query.ErrCanceled
		}
		return nil
	}, &checks
}

func TestProtocolPreparseCancellationIsBoundedAndExact(t *testing.T) {
	large := strings.Repeat("x", 1<<20)
	shape := catalogShape{
		segments: []string{"catalog-prefix", "catalog-suffix"},
		capture:  captureOID,
	}
	tests := []struct {
		name string
		run  func(func() error) error
	}{
		{
			name: "statement iterator quoted literal",
			run: func(check func() error) error {
				iter := statementIterator{src: "SELECT '" + large + "'", check: check}
				_, _, err := iter.next()
				return err
			},
		},
		{
			name: "UTF-8 admission",
			run: func(check func() error) error {
				_, err := validUTF8Cancelable(large, check)
				return err
			},
		},
		{
			name: "classification long token",
			run: func(check func() error) error {
				_, _, err := classifyCancelable(large, check)
				return err
			},
		},
		{
			name: "transaction comment",
			run: func(check func() error) error {
				_, err := parseTransactionCommandCancelable(
					"BEGIN /*"+large+"*/", kindBegin, check,
				)
				return err
			},
		},
		{
			name: "SET quoted value",
			run: func(check func() error) error {
				_, err := parseSetCancelable(
					"SET application_name = '"+large+"'", check,
				)
				return err
			},
		},
		{
			name: "fixed SELECT quoted value",
			run: func(check func() error) error {
				_, _, err := (shimFunctions{}).parseFixedSelectCancelable(
					"SELECT '"+large+"'", check,
				)
				return err
			},
		},
		{
			name: "catalog shape recognition",
			run: func(check func() error) error {
				_, _, err := matchCatalogShapeCancelable(
					"catalog-prefix"+large+"catalog-suffix", &shape, check,
				)
				return err
			},
		},
		{
			name: "extended Parse ownership copy",
			run: func(check func() error) error {
				_, _, err := ownPreparedTextCancelable("large", large, check)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			check, checks := cancelOnCheck(4)
			if err := tc.run(check); err != query.ErrCanceled {
				t.Fatalf("cancellation error = %v, want exact %v", err, query.ErrCanceled)
			}
			if *checks != 4 {
				t.Fatalf("cancellation checks = %d, want 4", *checks)
			}
		})
	}
}

func TestBoundProtocolCancellationCallbackDoesNotAllocate(t *testing.T) {
	var s session
	s.bindCancellationCheck()
	if s.cancelCheck == nil {
		t.Fatal("session cancellation callback was not bound")
	}
	const text = "SELECT 1"
	run := func() {
		iter := statementIterator{src: text, check: s.cancelCheck}
		got, ok, err := iter.next()
		if err != nil || !ok || got != text {
			panic("statement iteration failed")
		}
		kind, _, err := classifyCancelable(got, s.cancelCheck)
		if err != nil || kind != kindSelect {
			panic("statement classification failed")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(1000, run); allocs != 0 {
		t.Fatalf("bound cancellation callback allocated %.2f times per parse, want zero", allocs)
	}
}

func newPreparseTestSession(t *testing.T) (*session, *bytes.Buffer) {
	t.Helper()
	database, err := sqldriver.Open(filepath.Join(t.TempDir(), "preparse.vdb"))
	if err != nil {
		t.Fatalf("open SQL catalog: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL catalog: %v", err)
		}
	})
	runtime, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("open SQL session: %v", err)
	}
	output := new(bytes.Buffer)
	s := &session{
		database:   "app",
		user:       "tester",
		params:     make(map[string]string, len(parameters)),
		statements: map[string]*prepared{},
		portals:    map[string]*portal{},
		sql:        &embeddedSession{runtime},
		w:          newWriter(output, 16<<10),
	}
	for name, parameter := range parameters {
		s.params[name] = parameter.initial
	}
	if err := runtime.SetCancelFlag(&s.queryCancel); err != nil {
		t.Fatalf("install cancellation flag: %v", err)
	}
	s.bindCancellationCheck()
	t.Cleanup(s.release)
	return s, output
}

func TestCancellationReportedAfterSuccessfulBeginRollsBackState(t *testing.T) {
	s, _ := newPreparseTestSession(t)
	stmt, err := s.prepare("", "BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.release()
	if err := s.sql.Begin(context.Background(), stmt.txOptions); err != nil {
		t.Fatal(err)
	}
	s.queryCancel.Cancel()
	if !s.takeCancel() {
		t.Fatal("successful BEGIN cancellation was not pending at the command boundary")
	}
	err = s.cancelSuccessfulBegin(stmt)
	if !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("canceled successful BEGIN = %v, want query.ErrCanceled", err)
	}
	if state := s.sql.State(); state != sqldriver.SessionIdle {
		t.Fatalf("canceled successful BEGIN left runtime state %s, want idle", state)
	}
	if err := s.sql.Begin(context.Background(), stmt.txOptions); err != nil {
		t.Fatalf("BEGIN after canceled successful BEGIN: %v", err)
	}
	if err := s.sql.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMaxSimpleQueryPreparseCancellationExecutesNothingAndReusesSession(t *testing.T) {
	s, output := newPreparseTestSession(t)
	prefix := "SET application_name = 'must-not-stick'; /*"
	suffix := "*/ SET application_name = 'also-must-not-stick'"
	padding := maxMessageBody - 1 - len(prefix) - len(suffix)
	if padding <= 0 {
		t.Fatal("maximal simple Query fixture has no padding")
	}
	text := prefix + strings.Repeat("x", padding) + suffix
	check, checks := cancelOnCheck(8)
	s.cancelCheck = check
	if err := s.simpleQuery(text); err != nil {
		t.Fatalf("canceled simple Query transport error: %v", err)
	}
	if *checks != 8 {
		t.Fatalf("simple Query cancellation checks = %d, want 8", *checks)
	}
	if !bytes.Contains(output.Bytes(), []byte(sqlstateQueryCanceled)) {
		t.Fatalf("canceled simple Query did not emit SQLSTATE %s", sqlstateQueryCanceled)
	}
	if got := s.params["application_name"]; got != "" {
		t.Fatalf("preflight cancellation executed a leading SET: %q", got)
	}

	output.Reset()
	s.cancelCheck = nil
	s.bindCancellationCheck()
	if err := s.simpleQuery("SET application_name = 'after'"); err != nil {
		t.Fatalf("simple Query after cancellation: %v", err)
	}
	if got := s.params["application_name"]; got != "after" {
		t.Fatalf("session after cancellation retained application_name %q, want after", got)
	}
	if bytes.Contains(output.Bytes(), []byte(sqlstateQueryCanceled)) {
		t.Fatal("cancellation poisoned the next simple Query")
	}
}

func TestMaxExtendedParseCancellationPublishesNothingAndReusesSession(t *testing.T) {
	s, _ := newPreparseTestSession(t)
	s.msg = frontendMessage{name: "stable", query: "SELECT 1"}
	if err := s.handleCancelableParse(); err != nil {
		t.Fatalf("prepare stable statement: %v", err)
	}
	stable := s.statements["stable"]
	if stable == nil {
		t.Fatal("stable statement was not published")
	}

	prefix := "SELECT '"
	suffix := "'"
	padding := maxPreparedInputBytes - s.statementBytes - len("canceled") -
		len(prefix) - len(suffix)
	if padding <= 0 {
		t.Fatal("maximal extended Parse fixture has no padding")
	}
	s.msg = frontendMessage{
		name:  "canceled",
		query: prefix + strings.Repeat("x", padding) + suffix,
	}
	check, checks := cancelOnCheck(8)
	s.cancelCheck = check
	err := s.handleCancelableParse()
	var pg *pgError
	if !errors.As(err, &pg) || pg.code != sqlstateQueryCanceled {
		t.Fatalf("extended Parse cancellation = %v, want SQLSTATE %s", err, sqlstateQueryCanceled)
	}
	if *checks != 8 {
		t.Fatalf("extended Parse cancellation checks = %d, want 8", *checks)
	}
	if s.statements["canceled"] != nil {
		t.Fatal("canceled extended Parse published a statement")
	}
	if s.statements["stable"] != stable {
		t.Fatal("canceled extended Parse disturbed an existing statement")
	}

	s.cancelCheck = nil
	s.bindCancellationCheck()
	s.msg = frontendMessage{name: "after", query: "SELECT 1"}
	if err := s.handleCancelableParse(); err != nil {
		t.Fatalf("extended Parse after cancellation: %v", err)
	}
	if s.statements["after"] == nil {
		t.Fatal("session did not publish a statement after cancellation")
	}
}
