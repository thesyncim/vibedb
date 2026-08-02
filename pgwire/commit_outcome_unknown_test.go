package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

type cyclicProtocolError struct{}

func (e *cyclicProtocolError) Error() string { return "cyclic protocol error" }
func (e *cyclicProtocolError) Unwrap() error { return e }

func TestCommitOutcomeUnknownMappingPreservesTypedCauseChain(t *testing.T) {
	cause := fmt.Errorf("journal sync: %w: %w",
		durable.ErrCommitOutcomeUnknown, syscall.EIO)
	mapped := asPGError(cause)
	if mapped.code != sqlstateStatementCompletionUnknown {
		t.Fatalf("SQLSTATE = %q, want %q",
			mapped.code, sqlstateStatementCompletionUnknown)
	}
	if mapped.severity != "ERROR" {
		t.Fatalf("severity = %q, want ERROR", mapped.severity)
	}
	if !errors.Is(mapped, durable.ErrCommitOutcomeUnknown) {
		t.Fatal("mapped error lost ErrCommitOutcomeUnknown identity")
	}
	if !errors.Is(mapped, syscall.EIO) {
		t.Fatal("mapped error lost the journal sync root cause")
	}
	priority := asPGError(errors.Join(cause, durable.ErrDocumentTooLarge))
	if priority.code != sqlstateStatementCompletionUnknown ||
		!errors.Is(priority, durable.ErrDocumentTooLarge) {
		t.Fatalf("unknown-outcome priority or sibling cause lost: %#v", priority)
	}
	canceled := asPGError(errors.Join(queryCanceled(), cause))
	if canceled.code != sqlstateStatementCompletionUnknown ||
		!errors.Is(canceled, query.ErrCanceled) ||
		!errors.Is(canceled, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("cancellation masked an unknown completion: %#v", canceled)
	}

	// A prior protocol classification remains authoritative for an ordinary
	// wrapper, and mapping its outer join must preserve every sibling without
	// pointing the clone back into itself.
	specific := newError(sqlstateUniqueViolation, "specific failure").
		withCause(syscall.EIO)
	if remapped := asPGError(specific); remapped != specific {
		t.Fatal("asPGError replaced an existing, more-specific pgError")
	}
	if specific.code != sqlstateUniqueViolation ||
		!errors.Is(specific, syscall.EIO) {
		t.Fatalf("specific classification or causes changed: %#v", specific)
	}
	joined := errors.Join(specific, syscall.ENOSPC)
	joinedMapped := asPGError(joined)
	if joinedMapped.code != sqlstateUniqueViolation ||
		!errors.Is(joinedMapped, syscall.EIO) ||
		!errors.Is(joinedMapped, syscall.ENOSPC) {
		t.Fatalf("joined specific classification or causes changed: %#v",
			joinedMapped)
	}
	if joinedMapped == specific || !errorTreeAcyclic(joinedMapped) {
		t.Fatal("joined protocol classification was not copied into an acyclic cause tree")
	}

	// A direct pgError remains authoritative and retains identity. Once an
	// outer wrapper combines that classification with an unknown completion,
	// 40003 controls whether a retry is safe.
	preclassified := newError(sqlstateUniqueViolation, "premature classification").
		withCause(cause)
	if got := asPGError(preclassified); got != preclassified {
		t.Fatalf("direct pgError identity changed: got %p, want %p", got, preclassified)
	}
	if got := asPGError(fmt.Errorf("commit boundary: %w", preclassified)); got.code != sqlstateStatementCompletionUnknown ||
		!errors.Is(got, durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(got, syscall.EIO) {
		t.Fatalf("wrapped prior SQLSTATE masked unknown completion: %#v", got)
	}
}

func TestCommitOutcomeUnknownClassificationRejectsCyclicCauseGraphs(t *testing.T) {
	cycle := &cyclicProtocolError{}
	if errorTreeAcyclic(cycle) {
		t.Fatal("self-unwrapping error was accepted as acyclic")
	}
	mapped := asPGError(cycle)
	if mapped.code != sqlstateInternalError || mapped.cause != nil {
		t.Fatalf("cyclic classification = %#v, want detached XX000", mapped)
	}
	positioned := asPGErrorIn(cycle, "SELECT 1")
	if positioned.code != sqlstateInternalError || positioned.position != 0 ||
		positioned.cause != nil {
		t.Fatalf("positioned cyclic classification = %#v, want detached XX000", positioned)
	}
	if protocol, ok := classifiedProtocolError(cycle); !ok ||
		protocol.code != sqlstateInternalError || protocol.cause != nil {
		t.Fatalf("cyclic protocol boundary = (%#v, %t), want detached XX000", protocol, ok)
	}

	// Reusing one child in two errors.Join branches is a DAG, not a cycle.
	shared := fmt.Errorf("shared: %w", syscall.EIO)
	if tree := errors.Join(shared, shared); !errorTreeAcyclic(tree) {
		t.Fatal("shared errors.Join subtree was mistaken for a cycle")
	}

	// withCause itself refuses to publish a direct cycle, keeping every pgError
	// safe for a later errors.Is call even before protocol classification.
	direct := newError(sqlstateInternalError, "direct")
	direct.withCause(direct)
	if direct.cause != nil || !errorTreeAcyclic(direct) {
		t.Fatalf("withCause retained a direct cycle: %#v", direct)
	}
}

func TestCommitOutcomeUnknownExtendedRecoveryWaitsForSyncAndReusesSession(t *testing.T) {
	s, output := newPreparseTestSession(t)
	cause := fmt.Errorf("journal sync: %w: %w",
		durable.ErrCommitOutcomeUnknown, syscall.EIO)

	if err := s.rejectExtended(errors.Join(queryCanceled(), cause)); err != nil {
		t.Fatalf("reject unknown outcome: %v", err)
	}
	if !s.failed {
		t.Fatal("unknown execution outcome did not enter discard-until-Sync state")
	}

	// Parse is valid but must be ignored while failed. If it were executed, the
	// named statement would survive Sync and make the protocol state observable.
	s.msg = frontendMessage{name: "must_be_discarded", query: "SELECT 1"}
	if err := s.extended(msgParse); err != nil {
		t.Fatalf("discard Parse before Sync: %v", err)
	}
	if _, exists := s.statements["must_be_discarded"]; exists {
		t.Fatal("extended error state executed a Parse before Sync")
	}
	if err := s.finishExtendedBatch(); err != nil {
		t.Fatalf("Sync after unknown outcome: %v", err)
	}
	if s.failed {
		t.Fatal("Sync did not clear extended error state")
	}

	messages := bufferedBackendMessages(t, output.Bytes())
	fields := expectError(t, messages, sqlstateStatementCompletionUnknown)
	if fields['S'] != "ERROR" || fields['V'] != "ERROR" {
		t.Fatalf("40003 severity fields = S:%q V:%q, want ERROR/ERROR",
			fields['S'], fields['V'])
	}
	assertReadyStatus(t, messages, statusIdle)
	if len(messages) != 2 || messages[0].tag != msgErrorResponse ||
		messages[1].tag != msgReadyForQuery {
		t.Fatalf("failed batch response sequence = %s, want [ErrorResponse ReadyForQuery]",
			tags(messages))
	}

	output.Reset()
	if err := s.simpleQuery("SELECT 1"); err != nil {
		t.Fatalf("simple Query after Sync: %v", err)
	}
	reused := bufferedBackendMessages(t, output.Bytes())
	if has(reused, msgErrorResponse) {
		t.Fatalf("40003 poisoned the next statement: %s", tags(reused))
	}
	if got := commandTagOf(t, reused); got != "SELECT 1" {
		t.Fatalf("next statement tag = %q, want SELECT 1", got)
	}
	assertReadyStatus(t, reused, statusIdle)
}

func TestCommitOutcomeUnknownCompletionBoundaryLeavesSimpleSessionReusable(t *testing.T) {
	s, output := newPreparseTestSession(t)
	cause := fmt.Errorf("journal sync: %w: %w",
		durable.ErrCommitOutcomeUnknown, syscall.EIO)

	// This is the exact reporting helper used after simple Query commit and
	// while Sync finalizes an implicit extended transaction. The typed runtime
	// has already returned to idle before this boundary.
	s.reportTransactionCompletionError(cause)
	s.w.readyForQuery(s.transactionStatus())
	if err := s.flush(); err != nil {
		t.Fatalf("flush completion error: %v", err)
	}
	messages := bufferedBackendMessages(t, output.Bytes())
	expectError(t, messages, sqlstateStatementCompletionUnknown)
	assertReadyStatus(t, messages, statusIdle)
	if len(messages) != 2 || messages[0].tag != msgErrorResponse ||
		messages[1].tag != msgReadyForQuery {
		t.Fatalf("completion response sequence = %s, want [ErrorResponse ReadyForQuery]",
			tags(messages))
	}

	output.Reset()
	if err := s.simpleQuery("SELECT 1"); err != nil {
		t.Fatalf("simple Query after completion error: %v", err)
	}
	reused := bufferedBackendMessages(t, output.Bytes())
	if has(reused, msgErrorResponse) {
		t.Fatalf("completion-time 40003 poisoned the next Query: %s", tags(reused))
	}
	if got := commandTagOf(t, reused); got != "SELECT 1" {
		t.Fatalf("next statement tag = %q, want SELECT 1", got)
	}
	assertReadyStatus(t, reused, statusIdle)
}

func bufferedBackendMessages(t *testing.T, raw []byte) []backendMessage {
	t.Helper()
	var messages []backendMessage
	for len(raw) != 0 {
		if len(raw) < 5 {
			t.Fatalf("truncated backend message header: %d bytes", len(raw))
		}
		length := int(binary.BigEndian.Uint32(raw[1:5]))
		if length < 4 || length > len(raw)-1 {
			t.Fatalf("invalid backend message length %d with %d bytes remaining",
				length, len(raw))
		}
		end := 1 + length
		messages = append(messages, backendMessage{
			tag:  raw[0],
			body: bytes.Clone(raw[5:end]),
		})
		raw = raw[end:]
	}
	return messages
}
