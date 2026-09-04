package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/pginput"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The extended query protocol: Parse, Bind, Describe, Execute, Close, Flush,
// Sync.
//
// This is what real client libraries use. The SQL database maps Parse, Bind,
// and Execute onto the shared typed SQL runtime, including DDL, DML, SELECT,
// transaction state, and scalar-versus-document parameter roles. Named
// statements retain one parsed plan instead of reparsing at Execute.
//
// # The error state
//
// The protocol's rule is that after an error the backend discards every message
// up to and including the next Sync, and answers that Sync with ReadyForQuery.
// Failing to implement it is not a cosmetic bug: a client that pipelines
// Parse/Bind/Execute/Sync in one write expects a failed Bind to suppress the
// Execute, and a server that ran the Execute anyway would execute a portal that
// was never successfully bound. The flag is [session.failed] and it is cleared
// only by Sync.
//
// # Parameter types
//
// A statement's placeholders are schemaless by default: ParameterDescription
// reports type 0, "unspecified", unless Parse declared a type or SQL analysis
// selected one. Typed VALUES and set-operation contexts infer bool/text exactly
// and therefore advertise those OIDs even when Parse omitted them. A compatible
// client-declared OID is preserved; an incompatible declaration is rejected at
// Parse, before a portal can bind bytes under the wrong domain.
//
// Only a still-unspecified scalar pushes the question onto Bind; its rule is
// stated on [bindValue]. A parameter that spells a JSON scalar is that scalar,
// and anything else is a string. Inferred and declared types bypass that
// compatibility inference and use their exact textual/binary input grammar.

// extended handles one extended-protocol message.
func (s *session) extended(tag byte) error {
	// Sync is the resynchronization point and is processed even in the error
	// state; every other message is discarded there.
	if tag == msgSync {
		return s.finishExtendedBatch()
	}
	if s.failed {
		return nil
	}
	if tag == msgFlush {
		return s.flush()
	}

	var err error
	switch tag {
	case msgParse:
		err = s.handleCancelableParse()
	case msgBind:
		err = s.handleCancelableBind()
	case msgDescribe:
		err = s.handleDescribe()
	case msgExecute:
		err = s.handleExecute()
	case msgClose:
		err = s.handleClose()
	}
	if err == nil {
		return nil
	}
	return s.rejectExtended(err)
}

// rejectExtended publishes one non-transport extended-protocol failure and
// enters PostgreSQL's discard-until-Sync state. Keeping this transition in one
// function makes execution failures, including SQLSTATE 40003 unknown commit
// outcomes, follow exactly the same recovery boundary as Parse and Bind
// failures. Sync remains the sole operation that clears failed.
func (s *session) rejectExtended(err error) error {
	pg, ok := classifiedProtocolError(err)
	if !ok {
		return err
	}
	s.markTransactionFailed()
	s.failed = true
	s.w.errorResponse(pg)
	// The error is pushed now rather than left for the next Sync. A client is
	// entitled to send Flush after any extended-protocol message and wait for
	// the result, and a Flush that arrives in the error state is discarded along
	// with everything else up to Sync — so an unflushed error would leave that
	// client blocked on a message this server had already written and not sent.
	if err := s.flush(); err != nil {
		return err
	}
	if pg.severity == "FATAL" {
		return pg
	}
	return nil
}

func (s *session) handleCancelableParse() error {
	s.beginCancelable()
	defer s.endCancelable()
	return s.handleParse()
}

func (s *session) handleCancelableBind() error {
	s.beginCancelable()
	defer s.endCancelable()
	return s.handleBind()
}

func (s *session) finishExtendedBatch() error {
	s.beginCancelable()
	defer s.endCancelable()

	if s.implicitExtended {
		// A non-holdable portal cannot survive the end of its implicit
		// transaction. Close every live runtime cursor before commit/rollback
		// so snapshot ownership is balanced even when Execute suspended it.
		s.closeRuntimePortals()
		if !s.failed && s.sql.State() == sqldriver.SessionInTransaction &&
			s.takeCancel() {
			s.markTransactionFailed()
			s.w.errorResponse(queryCanceled())
		}
		var err error
		switch s.sql.State() {
		case sqldriver.SessionFailedTransaction:
			err = s.sql.Rollback(context.Background())
		case sqldriver.SessionInTransaction:
			err = s.sql.Commit(context.Background())
		}
		if err != nil {
			s.reportTransactionCompletionError(err)
		}
	} else if s.extendedDDL {
		// DDL publishes outside the runtime transaction overlay, but its
		// protocol portal is still non-holdable and ends at Sync.
		s.closeRuntimePortals()
	}
	// A cancel racing with transaction finalization, or arriving while no
	// Execute was active between protocol messages, belongs to this completed
	// batch and must not poison the next one.
	_ = s.takeCancel()
	s.failed = false
	s.implicitExtended = false
	s.extendedDDL = false
	s.extendedSQL = false
	s.extendedSessionChange = false
	s.w.readyForQuery(s.transactionStatus())
	return s.flush()
}

func (s *session) handleParse() error {
	m := &s.msg
	old := s.statements[m.name]
	if m.name != "" {
		// The unnamed statement is replaceable by design; a named one is not,
		// and silently replacing it would strand any portal built from it on a
		// plan the client believes it still has.
		if _, exists := s.statements[m.name]; exists {
			return newError(sqlstateDuplicateStatement,
				fmt.Sprintf("prepared statement %q already exists", m.name))
		}
		if len(s.statements) >= maxStatements {
			return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
				"this connection already holds %d prepared statements", maxStatements))
		}
	}
	if old != nil && s.reuseUnnamedParse(old, m) {
		s.w.parseComplete()
		return nil
	}
	charge := preparedInputCharge(m.name, m.query, len(m.paramOIDs))
	retained := s.statementBytes + charge
	if old != nil {
		retained -= old.retainedBytes
	}
	if charge > maxPreparedInputBytes || retained > maxPreparedInputBytes {
		return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"prepared statements on one connection may retain at most %d bytes of "+
				"names, SQL, and parameter type metadata", maxPreparedInputBytes))
	}
	ownedName, ownedQuery, err := ownPreparedTextCancelable(
		m.name, m.query, s.cancelCheck,
	)
	if err != nil {
		return asPGErrorIn(err, m.query)
	}
	stmt, err := s.prepare(ownedName, ownedQuery, m.paramOIDs)
	if err != nil {
		return err
	}
	if len(m.paramOIDs) > stmt.wireParams {
		stmt.release()
		return newError(sqlstateProtocolViolation, fmt.Sprintf(
			"parse message declares %d parameter types, but the statement has %d wire parameters",
			len(m.paramOIDs), stmt.wireParams))
	}
	derivedCharge := preparedDerivedCharge(stmt)
	if derivedCharge > maxPreparedInputBytes-charge ||
		retained > maxPreparedInputBytes-derivedCharge {
		stmt.release()
		return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"prepared statements on one connection may retain at most %d bytes of "+
				"input, numbered-parameter mappings, and wire-parameter metadata",
			maxPreparedInputBytes))
	}
	charge += derivedCharge
	retained += derivedCharge
	stmt.retainedBytes = charge
	if len(m.paramOIDs) != 0 {
		// An exact-size allocation keeps the charge exact instead of depending
		// on append's implementation-selected spare capacity.
		stmt.paramOIDs = make([]int32, len(m.paramOIDs))
		copy(stmt.paramOIDs, m.paramOIDs)
	}
	if err := validateDeclaredParamOIDs(stmt); err != nil {
		stmt.release()
		return err
	}
	if old != nil {
		// Replacing the unnamed statement. Every portal built from it is
		// invalidated, because its plan is about to be released.
		s.closePortalsOf(old)
		old.release()
		s.statementBindBytes -= old.bindBytes
	}
	s.statements[ownedName] = stmt
	s.statementBytes = retained
	s.w.parseComplete()
	return nil
}

// reuseUnnamedParse keeps the still-live runtime when a client repeats the
// exact unnamed Parse contract. The old statement is available here because
// replacement is transactional: handleParse does not close it until a new
// prepare succeeds. Reusing it avoids protocol classification, parameter
// rewriting, semantic compilation, and RowDescription construction.
func (s *session) reuseUnnamedParse(old *prepared, m *frontendMessage) bool {
	if old == nil || m == nil || m.name != "" || old.name != "" ||
		old.runtime == nil || old.sql != m.query ||
		!slices.Equal(old.paramOIDs, m.paramOIDs) {
		return false
	}
	if s.cancelCheck != nil && s.cancelCheck() != nil {
		return false
	}
	reuser, ok := old.runtime.(BackendStatementParseReuser)
	if !ok || !reuser.ReusableForParse() {
		return false
	}
	// Parse replacement destroys every portal derived from the prior unnamed
	// statement even though its immutable plan can stay live.
	s.closePortalsOf(old)
	s.statementBindBytes -= old.bindBytes
	old.bindBytes = 0
	return true
}

const (
	// Every prepared plan owns parser/compiler objects and the first chunks of
	// several pointer-stable arenas even when its SQL is terse.
	preparedPlanFixedBytes = 8 << 10
	// SQL structure cannot grow without source tokens. This deliberately loose
	// multiplier covers AST nodes, lowered plan nodes, path registries, output
	// metadata, arena geometric slack, and worst-case JSON literal escaping.
	// It is an admission bound, not a claim about a Go struct's exact size.
	preparedPlanByteMultiplier = 128
)

// preparedDerivedCharge conservatively accounts client-shaped storage created
// by parsing/lowering and by adapting PostgreSQL placeholders to the shared SQL
// runtime.
//
// Numbered parameters retain an occurrence-to-wire mapping. Their rewritten
// SQL also remains reachable through the prepared AST/plan, alongside the
// original SQL retained for diagnostics. Both are selected by client input and
// therefore belong under the aggregate prepared-input bound rather than being
// waved away as implementation metadata. Capacity, not length, is charged for
// slices because that is what the garbage collector keeps reachable.
func preparedDerivedCharge(stmt *prepared) int {
	if stmt == nil {
		return 0
	}
	charge := preparedPlanFixedBytes
	charge = preparedChargeMul(charge, len(stmt.sql), preparedPlanByteMultiplier)
	charge = preparedChargeMul(charge, cap(stmt.paramKinds), 8)
	charge = preparedChargeMul(charge, cap(stmt.paramTypes), 8)
	charge = preparedChargeMul(charge, cap(stmt.paramTypePositions), 8)
	charge = preparedChargeMul(charge, cap(stmt.paramPositions), 8)
	charge = preparedChargeMul(charge, cap(stmt.paramOrder), 8)
	if stmt.paramOrder != nil {
		// rewriteNumberedParameters preserves byte length.
		charge = preparedChargeAdd(charge, len(stmt.sql))
	}
	return charge
}

func preparedChargeMul(total, n, multiplier int) int {
	if total > maxPreparedInputBytes ||
		n > (maxPreparedInputBytes-total)/multiplier {
		return maxPreparedInputBytes + 1
	}
	return total + n*multiplier
}

func preparedChargeAdd(total, n int) int {
	if total > maxPreparedInputBytes || n > maxPreparedInputBytes-total {
		return maxPreparedInputBytes + 1
	}
	return total + n
}

// preparedInputCharge is the exact byte count of client-selected storage a
// prepared statement retains. The parser/compiler's derived representation is
// bounded separately; this charge closes the easy aggregate-retention path
// through a tiny SQL string carrying tens of thousands of copied OIDs.
func preparedInputCharge(name, query string, oidCount int) int {
	total := len(name) + len(query)
	if oidCount > (maxPreparedInputBytes-total)/4 {
		return maxPreparedInputBytes + 1
	}
	return total + 4*oidCount
}

func (s *session) handleBind() error {
	if err := s.cancellationError(); err != nil {
		return asPGError(err)
	}
	m := &s.msg
	stmt, ok := s.statements[m.name]
	if !ok {
		return newError(sqlstateInvalidStatementName,
			fmt.Sprintf("prepared statement %q does not exist", m.name))
	}
	if m.portal != "" {
		if _, exists := s.portals[m.portal]; exists {
			return newError(sqlstateDuplicateCursor,
				fmt.Sprintf("portal %q already exists", m.portal))
		}
		if len(s.portals) >= maxPortals {
			return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
				"this connection already holds %d portals", maxPortals))
		}
	}
	want := stmt.wireParams
	if len(m.params) != want {
		return newError(sqlstateProtocolViolation, fmt.Sprintf(
			"bind message supplies %d parameters, but the statement requires %d",
			len(m.params), want))
	}
	// Both format-code arrays follow the protocol's 0-or-1-or-one-per-item rule,
	// and both are checked. Letting a wrong parameter count through is not
	// laxity: formatFor answers text for an index past the array, so a client
	// that sent two codes for three binary parameters would have its third
	// parameter decoded as text and would get wrong rows with no error at all.
	if err := checkFormats("parameter", m.paramFormats, len(m.params)); err != nil {
		return err
	}
	if err := checkFormats("result", m.resultFormats, len(stmt.cols)); err != nil {
		return err
	}

	// The existing portal of this name is destroyed before the new one is bound,
	// which is what PostgreSQL does and what makes a failed Bind safe. Reusing
	// the old portal's argument storage — the point of which is that re-binding
	// the unnamed portal, the hot path, allocates nothing — writes over the old
	// portal's own arguments in place, so leaving it reachable after a bind that
	// failed halfway would leave it holding a mixture of the old values and the
	// new ones. That produced wrong rows rather than an error.
	var p *portal
	if old, ok := s.portals[m.portal]; ok {
		delete(s.portals, m.portal)
		s.portalBytes -= old.retainedBytes
		old.resetForBind(m.portal, stmt)
		p = old
	} else {
		p = &portal{name: m.portal, stmt: stmt}
	}
	p.retainedBytes = portalCharge(p, m, stmt)
	if p.retainedBytes > maxPortalBytes-s.portalBytes {
		return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"bound portals on one connection may retain at most %d bytes",
			maxPortalBytes))
	}
	if err := s.bindInto(p, m, stmt); err != nil {
		return asPGError(err)
	}
	bindBytes := max(stmt.bindBytes, boundLiteralCharge(p.literalCharges, stmt))
	if bindBytes-stmt.bindBytes > maxPreparedBindBytes-s.statementBindBytes {
		return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"bound literals retained by prepared statements on one connection may use "+
				"at most %d bytes", maxPreparedBindBytes))
	}
	// Publication is the commit point. Every conversion and both aggregate
	// budgets have succeeded, so an Execute can no longer observe a
	// half-bound portal and a failed Bind retains no new session object.
	// Message names borrow the reader body. A published named portal outlives
	// that body, so take ownership only at the successful Bind commit point.
	if err := s.cancellationError(); err != nil {
		return asPGError(err)
	}
	p.name = strings.Clone(p.name)
	s.portals[p.name] = p
	s.portalBytes += p.retainedBytes
	s.statementBindBytes += bindBytes - stmt.bindBytes
	stmt.bindBytes = bindBytes
	s.w.bindComplete()
	return nil
}

// ownPreparedText copies a Parse message's name and SQL into one allocation.
// Both returned strings view that one immutable backing block. The prepared
// object and its map key retain them together, so separate allocations would
// buy no lifetime advantage.
func ownPreparedText(name, sql string) (string, string) {
	ownedName, ownedSQL, _ := ownPreparedTextCancelable(name, sql, nil)
	return ownedName, ownedSQL
}

func ownPreparedTextCancelable(
	name string,
	sql string,
	check func() error,
) (string, string, error) {
	if check != nil {
		if err := check(); err != nil {
			return "", "", err
		}
	}
	if len(name)+len(sql) == 0 {
		return "", "", nil
	}
	buf := make([]byte, len(name)+len(sql))
	copyAt := func(at int, src string) error {
		for len(src) != 0 {
			chunk := min(len(src), protocolCancelByteInterval)
			copy(buf[at:at+chunk], src[:chunk])
			at += chunk
			src = src[chunk:]
			if len(src) != 0 && check != nil {
				if err := check(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := copyAt(0, name); err != nil {
		return "", "", err
	}
	if err := copyAt(len(name), sql); err != nil {
		return "", "", err
	}
	text := byteview.String(buf)
	return text[:len(name)], text[len(name):], nil
}

// bindInto converts and accounts one Bind without publishing it. handleBind
// publishes only after the separate compiler-retention budget also succeeds.
func (s *session) bindInto(p *portal, m *frontendMessage, stmt *prepared) error {
	p.formats = append(p.formats[:0], m.resultFormats...)
	var err error
	if p.args, p.wireArgs, p.valueSlots, p.argStore, p.decodeStore, err = bindArgs(
		p.args, p.wireArgs, p.valueSlots, p.argStore, p.decodeStore, m, stmt,
		s.cancelCheck,
	); err != nil {
		return err
	}
	p.literalCharges = fillStatementLiteralCharges(
		p.literalCharges, p.wireArgs, stmt,
	)
	// append may round a slice's capacity above the exact count charged before
	// binding. Account the capacity that will really remain reachable, and fail
	// closed if the allocator's growth crossed the bound.
	p.retainedBytes = portalCapacityCharge(p)
	if p.retainedBytes > maxPortalBytes-s.portalBytes {
		return newError(sqlstateProgramLimitExceeded, fmt.Sprintf(
			"bound portals on one connection may retain at most %d bytes",
			maxPortalBytes))
	}
	return nil
}

// portalCharge conservatively accounts everything a bound portal retains.
//
// Each wire byte is copied once. Each placeholder additionally occupies one
// interface slot in args, each wire parameter one in wireArgs and one
// 64-byte-conservative stable scalar slot, and the per-wire literal charge
// cache one machine word. These deliberately conservative sizes work on both
// 32- and 64-bit targets without importing unsafe merely to reproduce one
// toolchain's current layouts.
func portalCharge(p *portal, m *frontendMessage, stmt *prepared) int {
	nameBytes := max(len(m.portal), len(p.name))
	formatBytes := 2 * max(len(m.resultFormats), cap(p.formats))
	storeBytes := cap(p.argStore)
	rawBytes := 0
	for _, raw := range m.params {
		rawBytes += len(raw)
	}
	storeBytes = max(storeBytes, rawBytes)
	decodeBytes := max(cap(p.decodeStore), jsonDecodeReserve(m, stmt))
	wireArgs := max(len(m.params), cap(p.wireArgs))
	valueSlots := max(len(m.params), cap(p.valueSlots))
	args := len(m.params)
	if stmt.paramOrder != nil {
		args = len(stmt.paramOrder)
	}
	args = max(args, cap(p.args))
	charges := max(len(m.params), cap(p.literalCharges))
	return nameBytes + formatBytes + storeBytes + decodeBytes + 16*wireArgs +
		64*valueSlots + 16*args + 8*charges
}

func portalCapacityCharge(p *portal) int {
	return len(p.name) + cap(p.argStore) + cap(p.decodeStore) +
		16*cap(p.wireArgs) + 64*cap(p.valueSlots) + 16*cap(p.args) +
		8*cap(p.literalCharges) + 2*cap(p.formats)
}

// fillLiteralCharges computes one compiler high-water upper bound per decoded
// wire value. Repeated numbered parameters then reuse the cached integer rather
// than rescanning a large string once per $1 occurrence.
func fillLiteralCharges(dst []int, values []any) []int {
	return fillStatementLiteralCharges(dst, values, nil)
}

func fillStatementLiteralCharges(
	dst []int,
	values []any,
	stmt *prepared,
) []int {
	dst = dst[:0]
	for i, value := range values {
		if stmt != nil && stmt.paramKind(i) == sqldriver.ParamDocument {
			// A whole document is validated and written directly. It never
			// enters the predicate compiler's retained literal arenas; its
			// bytes are already charged exactly by the portal store.
			dst = append(dst, 0)
			continue
		}
		dst = append(dst, compilerLiteralCharge(value))
	}
	return dst
}

// compilerLiteralCharge conservatively bounds all storage a single bound
// occurrence can make the reusable compiler retain.
//
// A string can be interned and also rendered as a JSON equality needle.
// encodedJSONStringLen is exact; four times the decoded length covers its text
// arena reservation and geometric chunk slack, and eight times the encoded
// length covers both the needle arena and reusable formatting scratch with the
// same slack. An exact Number can occupy three digit runs, so twelve times its
// spelling covers them. The fixed kibibyte covers pointer boxes, predicate and
// tape entries, primitive numeric renderings, and arena first-chunk slack.
//
// The estimate intentionally describes the worst comparison shape because the
// public Statement does not expose which parameter occurs under equality. It
// may reject an unusually huge value early, but it never admits a value on the
// assumption that a cheaper operator will remain cheaper in a future lowering.
func compilerLiteralCharge(value any) int {
	const fixed = 1 << 10
	total := fixed
	switch value := value.(type) {
	case string:
		total = bindChargeMul(total, len(value), 4)
		total = bindChargeMul(total, encodedJSONStringLen(value), 8)
	case *string:
		total = bindChargeMul(total, len(*value), 4)
		total = bindChargeMul(total, encodedJSONStringLen(*value), 8)
	case query.Number:
		total = bindChargeMul(total, len(value), 12)
	case *query.Number:
		total = bindChargeMul(total, len(*value), 12)
	}
	return total
}

// encodedJSONStringLen is the exact number of bytes appendJSONString in the
// query compiler emits, saturating once the bind budget has already been
// exceeded. Bind has validated UTF-8 before this point; JSON escaping is
// byte-oriented for ASCII control characters and preserves every other byte.
func encodedJSONStringLen(value string) int {
	total := 2 // surrounding quotes
	for i := 0; i < len(value); i++ {
		n := 1
		switch value[i] {
		case '"', '\\', '\n', '\r', '\t':
			n = 2
		default:
			if value[i] < 0x20 {
				n = 6
			}
		}
		total = bindChargeAdd(total, n)
		if total > maxPreparedBindBytes {
			return total
		}
	}
	return total
}

func bindChargeMul(total, n, multiplier int) int {
	if total > maxPreparedBindBytes ||
		n > (maxPreparedBindBytes-total)/multiplier {
		return maxPreparedBindBytes + 1
	}
	return total + n*multiplier
}

func bindChargeAdd(total, n int) int {
	if total > maxPreparedBindBytes || n > maxPreparedBindBytes-total {
		return maxPreparedBindBytes + 1
	}
	return total + n
}

// boundLiteralCharge sums cached per-wire bounds in placeholder-occurrence
// order. A repeated $1 therefore stores and scans its bytes once in the portal
// but is charged once per compiler occurrence, exactly where the compiler's
// reusable arenas expand.
func boundLiteralCharge(charges []int, stmt *prepared) int {
	if stmt.paramOrder == nil {
		total := 0
		for _, charge := range charges {
			total = bindChargeAdd(total, charge)
			if total > maxPreparedBindBytes {
				break
			}
		}
		return total
	}
	total := 0
	for _, wire := range stmt.paramOrder {
		total = bindChargeAdd(total, charges[wire-1])
		if total > maxPreparedBindBytes {
			break
		}
	}
	return total
}

// checkFormats enforces the protocol's rule for a format-code array: none, one
// that applies to every item, or exactly one per item.
func checkFormats(what string, codes []int16, items int) error {
	switch len(codes) {
	case 0, 1:
		return nil
	case items:
		return nil
	}
	return newError(sqlstateProtocolViolation, fmt.Sprintf(
		"bind message supplies %d %s format codes for %d %ss",
		len(codes), what, items, what))
}

func (s *session) handleDescribe() error {
	m := &s.msg
	if m.target == targetStatement {
		stmt, ok := s.statements[m.name]
		if !ok {
			return newError(sqlstateInvalidStatementName,
				fmt.Sprintf("prepared statement %q does not exist", m.name))
		}
		if err := checkRowDescription(stmt.cols); err != nil {
			return err
		}
		s.w.parameterDesc(stmt)
		return s.describeRows(stmt, nil)
	}
	p, ok := s.portals[m.portal]
	if !ok {
		return newError(sqlstateInvalidCursorName,
			fmt.Sprintf("portal %q does not exist", m.portal))
	}
	return s.describeRows(p.stmt, p.formats)
}

// describeRows writes the RowDescription, or NoData for a statement that
// returns no rows.
func (s *session) describeRows(stmt *prepared, formats []int16) error {
	if len(stmt.cols) == 0 {
		s.w.noData()
		return nil
	}
	return s.w.rowDescription(stmt.cols, formats)
}

func (s *session) handleExecute() error {
	s.beginCancelable()
	defer s.endCancelable()

	m := &s.msg
	p, ok := s.portals[m.portal]
	if !ok {
		return newError(sqlstateInvalidCursorName,
			fmt.Sprintf("portal %q does not exist", m.portal))
	}
	if err := s.beforeExtendedExecute(p.stmt); err != nil {
		return err
	}
	return s.execute(p, m.maxRows)
}

func (s *session) beforeExtendedExecute(stmt *prepared) error {
	if s.sql == nil || stmt == nil {
		return nil
	}
	if stmt.kind == kindSet || stmt.kind == kindReset || stmt.kind == kindDiscard {
		if s.extendedSQL {
			return newError(sqlstateFeatureNotSupported,
				"SET, RESET, and DISCARD require their own Sync-delimited batch").
				withHint("send Sync before changing session parameters")
		}
		s.extendedSessionChange = true
		return nil
	}
	if isTransactionCommandKind(stmt.kind) {
		if s.extendedDDL || s.extendedSessionChange {
			return newError(sqlstateFeatureNotSupported,
				"transaction control cannot share a batch with DDL or session-parameter changes")
		}
		s.extendedSQL = true
		return nil
	}
	if stmt.runtime == nil {
		return nil
	}
	if s.extendedSessionChange {
		return newError(sqlstateFeatureNotSupported,
			"SET, RESET, and DISCARD require their own Sync-delimited batch").
			withHint("send Sync after the session command before executing stored-row SQL")
	}
	kind := stmt.runtime.Kind()
	atomicWrite := autocommitWrites(s.sql) && kind != sqlast.KindSelect
	if atomicWrite && s.sql.State() != sqldriver.SessionIdle {
		return newError(sqlstateFeatureNotSupported,
			"distributed writes require auto-commit mode")
	}
	ddl := runtimeKindIsDDL(kind) || atomicWrite
	if ddl {
		if s.extendedSQL {
			return newError(sqlstateFeatureNotSupported,
				"DDL must be the only catalog command between PostgreSQL Sync points").
				withHint("send Sync after the preceding command and execute DDL in its own batch")
		}
		s.extendedDDL = true
		s.extendedSQL = true
		return nil
	}
	if s.extendedDDL {
		return newError(sqlstateFeatureNotSupported,
			"DDL must be the only catalog command between PostgreSQL Sync points").
			withHint("send Sync after DDL before executing another catalog command")
	}
	if s.sql.State() == sqldriver.SessionIdle {
		if err := s.sql.Begin(context.Background(), sqldriver.TxOptions{}); err != nil {
			return asPGErrorIn(err, stmt.sql)
		}
		s.transactionIsolation = sqldriver.IsolationDefault
		s.implicitExtended = true
	}
	s.extendedSQL = true
	return nil
}

func (s *session) handleClose() error {
	m := &s.msg
	if m.target == targetStatement {
		if stmt, ok := s.statements[m.name]; ok {
			s.closePortalsOf(stmt)
			stmt.release()
			delete(s.statements, m.name)
			s.statementBytes -= stmt.retainedBytes
			s.statementBindBytes -= stmt.bindBytes
		}
	} else if p, ok := s.portals[m.portal]; ok {
		p.release()
		delete(s.portals, m.portal)
		s.portalBytes -= p.retainedBytes
	}
	// Close of something that does not exist is not an error; the protocol says
	// so explicitly, and a client closing a statement it is not sure it created
	// is an ordinary shape.
	s.w.closeComplete()
	return nil
}

// closePortalsOf drops every portal built from stmt. Closing a statement
// destroys its plan, and a portal that outlived the plan it was bound to would
// be a dangling pointer with a wire protocol in front of it.
func (s *session) closePortalsOf(stmt *prepared) {
	for name, p := range s.portals {
		if p.stmt == stmt {
			p.release()
			delete(s.portals, name)
			s.portalBytes -= p.retainedBytes
		}
	}
}

// bindArgs converts a Bind message's parameters into the engine's argument
// vector, copying every wire value it keeps exactly once.
//
// The copy is not optional. The parameter values view the reader's message
// buffer, which the next frontend message overwrites, and a portal outlives
// arbitrarily many messages. Copying into one contiguous store rather than
// per-value keeps a bound portal at a fixed number of allocations regardless
// of its parameter count. wireArgs is the crucial intermediate representation:
// repeated $n placeholders append another interface header to args, not another
// copy of the parameter bytes.
func bindArgs(
	args, wireArgs []any,
	valueSlots []boundValueSlot,
	store []byte,
	decodeStore []byte,
	m *frontendMessage,
	stmt *prepared,
	check func() error,
) ([]any, []any, []boundValueSlot, []byte, []byte, error) {
	if check != nil {
		if err := check(); err != nil {
			return args, wireArgs, valueSlots, store, decodeStore, err
		}
	}
	// Interface slices are scanned to capacity by the garbage collector.
	// Clear stale entries before shortening them so a bind with fewer
	// parameters cannot pin an older value-slot backing array.
	clear(args[:cap(args)])
	clear(wireArgs[:cap(wireArgs)])
	clear(valueSlots[:cap(valueSlots)])
	args = args[:0]
	wireArgs = wireArgs[:0]
	if cap(valueSlots) < len(m.params) {
		valueSlots = make([]boundValueSlot, len(m.params))
	} else {
		valueSlots = valueSlots[:len(m.params)]
	}
	store = store[:0]
	decodeStore = decodeStore[:0]
	// Reserve the whole store up front so the views taken below cannot be
	// invalidated by a later append growing the backing array. Summing wire
	// values, rather than placeholder occurrences, is what prevents a repeated
	// $1 from amplifying one bounded Bind message into an unbounded allocation.
	total := 0
	for i, raw := range m.params {
		if check != nil && i&255 == 0 {
			if err := check(); err != nil {
				return args, wireArgs, valueSlots, store, decodeStore, err
			}
		}
		total += len(raw)
	}
	if cap(store) < total {
		store = make([]byte, 0, total)
	}
	// A decoded JSON string is never larger than its wire spelling. Reserve one
	// contiguous destination before taking any string views, so a later append
	// cannot move the backing array out from under an earlier argument.
	decoded := jsonDecodeReserve(m, stmt)
	if cap(decodeStore) < decoded {
		decodeStore = make([]byte, 0, decoded)
	}
	for wire, raw := range m.params {
		if check != nil && wire&255 == 0 {
			if err := check(); err != nil {
				return args, wireArgs, valueSlots, store, decodeStore, err
			}
		}
		if raw == nil {
			if stmt.paramKind(wire) == sqldriver.ParamDocument {
				return args, wireArgs, valueSlots, store, decodeStore,
					stmt.documentBindError(wire,
						newError(sqlstateInvalidParameterValue,
							"value cannot be SQL NULL; bind the JSON literal null explicitly"))
			}
			wireArgs = append(wireArgs, nil)
			continue
		}
		start := len(store)
		if check == nil {
			store = append(store, raw...)
		} else {
			for len(raw) > 0 {
				if err := check(); err != nil {
					return args, wireArgs, valueSlots, store, decodeStore, err
				}
				chunk := min(len(raw), 4<<10)
				store = append(store, raw[:chunk]...)
				raw = raw[chunk:]
			}
		}
		value, err := bindParameter(store[start:len(store):len(store)],
			formatFor(m.paramFormats, wire), stmt.parameterOID(wire),
			stmt.paramKind(wire), stmt.paramType(wire),
			&valueSlots[wire], &decodeStore)
		if err != nil {
			return args, wireArgs, valueSlots, store, decodeStore,
				stmt.documentBindError(wire, err)
		}
		if check != nil {
			if err := check(); err != nil {
				return args, wireArgs, valueSlots, store, decodeStore, err
			}
		}
		wireArgs = append(wireArgs, value)
	}
	if stmt.paramOrder == nil {
		args = append(args, wireArgs...)
		return args, wireArgs, valueSlots, store, decodeStore, nil
	}
	for index, wire := range stmt.paramOrder {
		if check != nil && index&255 == 0 {
			if err := check(); err != nil {
				return args, wireArgs, valueSlots, store, decodeStore, err
			}
		}
		args = append(args, wireArgs[wire-1])
	}
	return args, wireArgs, valueSlots, store, decodeStore, nil
}

// jsonDecodeReserve is an allocation-free upper bound for storage needed to
// decode declared JSON string parameters. It deliberately reserves for every
// declared JSON value, not only values that currently begin with a quote: the
// preflight portal charge must be independent of validation and must reject
// oversized input before conversion allocates.
func jsonDecodeReserve(m *frontendMessage, stmt *prepared) int {
	total := 0
	for i, raw := range m.params {
		if stmt.paramKind(i) == sqldriver.ParamDocument {
			continue
		}
		switch stmt.parameterOID(i) {
		case oidJSON, oidJSONB:
			total += len(raw)
		}
	}
	return total
}

func oidAt(oids []int32, i int) int32 {
	if i < 0 || i >= len(oids) {
		return 0
	}
	return oids[i]
}

func (p *prepared) paramKind(i int) sqldriver.ParamKind {
	if p == nil || i < 0 || i >= len(p.paramKinds) {
		return sqldriver.ParamScalar
	}
	return p.paramKinds[i]
}

func (p *prepared) paramType(i int) sqldriver.ParamType {
	if p == nil || i < 0 || i >= len(p.paramTypes) {
		return sqldriver.ParamTypeUnspecified
	}
	return p.paramTypes[i]
}

// documentBindError adds protocol-facing identity and source attribution only
// to a document parameter conversion failure. It never includes the bound
// bytes; hostile or secret input therefore cannot be reflected through an
// ErrorResponse. Scalar conversion and cancellation errors pass through
// unchanged.
func (p *prepared) documentBindError(wire int, err error) error {
	if err == nil || p == nil || p.paramKind(wire) != sqldriver.ParamDocument {
		return err
	}
	var pg *pgError
	if !errors.As(err, &pg) || pg == nil {
		return err
	}
	pg.message = fmt.Sprintf("document parameter $%d: %s", wire+1, pg.message)
	if pg.position == 0 && wire >= 0 && wire < len(p.paramPositions) &&
		p.paramPositions[wire] != 0 {
		pg.position = charPosition(p.sql, p.paramPositions[wire]-1)
	}
	return pg
}

func (p *prepared) parameterOID(i int) int32 {
	declared := oidAt(p.paramOIDs, i)
	if inferred := inferredParameterOID(p.paramType(i)); inferred != 0 {
		// UNKNOWN is PostgreSQL's unresolved string domain. An analyzed typed
		// context resolves it exactly like an omitted OID, so advertise the
		// selected domain rather than preserving UNKNOWN.
		if declared == 0 || declared == oidUnknown {
			return inferred
		}
		return declared
	}
	if declared != 0 {
		return declared
	}
	if p.paramKind(i) == sqldriver.ParamDocument {
		// Advertising json removes an otherwise unavoidable ambiguity for
		// clients such as pgx: they can choose the correct text/binary encoder
		// without guessing from a Go []byte or string.
		return oidJSON
	}
	return 0
}

func inferredParameterOID(paramType sqldriver.ParamType) int32 {
	switch paramType {
	case sqldriver.ParamTypeBool:
		return oidBool
	case sqldriver.ParamTypeText:
		return oidText
	case sqldriver.ParamTypeVarchar:
		return oidVarchar
	case sqldriver.ParamTypeName:
		return oidName
	case sqldriver.ParamTypeBPChar:
		return oidBPChar
	default:
		return 0
	}
}

func validateDeclaredParamOIDs(p *prepared) error {
	if p == nil || len(p.paramOIDs) == 0 || len(p.paramTypes) == 0 {
		return nil
	}
	for wire, declared := range p.paramOIDs {
		inferred := p.paramType(wire)
		if inferred == sqldriver.ParamTypeUnspecified || declared == 0 ||
			declared == oidUnknown || declaredParamTypeCompatible(inferred, declared) {
			continue
		}
		err := newError(sqlstateDatatypeMismatch, fmt.Sprintf(
			"parameter $%d has declared type OID %d but is inferred as %s",
			wire+1, declared, inferred))
		if wire < len(p.paramTypePositions) && p.paramTypePositions[wire] != 0 {
			err.position = charPosition(p.sql, p.paramTypePositions[wire]-1)
		}
		return err
	}
	return nil
}

func declaredParamTypeCompatible(paramType sqldriver.ParamType, oid int32) bool {
	switch paramType {
	case sqldriver.ParamTypeBool:
		return oid == oidBool
	case sqldriver.ParamTypeText,
		sqldriver.ParamTypeVarchar,
		sqldriver.ParamTypeName,
		sqldriver.ParamTypeBPChar:
		// PostgreSQL's string-category parameters have implicit coercions to
		// one another during common-type selection; numeric, boolean, JSON, and
		// bytea parameters do not.
		return isStringParameterOID(oid)
	case sqldriver.ParamTypeOther:
		return oid != 0 && oid != oidUnknown && oid != oidBool &&
			!isStringParameterOID(oid)
	default:
		return false
	}
}

func isStringParameterOID(oid int32) bool {
	return oid == oidText || oid == oidVarchar || oid == oidName || oid == oidBPChar
}

func isStringParameterType(paramType sqldriver.ParamType) bool {
	return paramType == sqldriver.ParamTypeText ||
		paramType == sqldriver.ParamTypeVarchar ||
		paramType == sqldriver.ParamTypeName ||
		paramType == sqldriver.ParamTypeBPChar
}

// PostgreSQL OIDs a client may declare for a parameter. Only the ones whose
// binary encoding is unambiguous and whose meaning maps onto a JSON scalar are
// listed; a parameter declared as anything else is refused rather than guessed
// at.
const (
	oidBytea   = 17
	oidInt2    = 21
	oidInt4    = 23
	oidFloat4  = 700
	oidFloat8  = 701
	oidVarchar = 1043
	oidName    = 19
	oidBPChar  = 1042
	oidUnknown = 705
)

func bindParameter(
	raw []byte,
	format int16,
	oid int32,
	role sqldriver.ParamKind,
	inferred sqldriver.ParamType,
	slot *boundValueSlot,
	decodeStore *[]byte,
) (any, error) {
	if role == sqldriver.ParamDocument {
		return bindJSONDocument(raw, format, oid, slot)
	}
	stringTarget := inferred
	if stringTarget == sqldriver.ParamTypeUnspecified &&
		isStringParameterOID(oid) {
		// An external backend may implement neither optional type-metadata
		// interface. The declared wire domain still owns its input semantics;
		// only analysis-time coercion is unavailable in that fallback.
		stringTarget = declaredParamType(oid)
	}
	if isStringParameterType(stringTarget) {
		if format != formatBinary {
			if err := validateTextParameter(raw); err != nil {
				return nil, err
			}
		}
		var err error
		raw, err = coerceDeclaredStringParameter(raw, format, oid, stringTarget)
		if err != nil {
			return nil, err
		}
		slot.text = byteview.String(raw)
		return &slot.text, nil
	}
	return bindValue(raw, format, oid, slot, decodeStore)
}

func validateTextParameter(raw []byte) error {
	if !utf8.Valid(raw) || containsNUL(raw) {
		return newError(sqlstateCharacterNotInRepertoire,
			"a text-format parameter is not valid PostgreSQL UTF-8 text")
	}
	return nil
}

func containsNUL(raw []byte) bool {
	for _, b := range raw {
		if b == 0 {
			return true
		}
	}
	return false
}

// coerceDeclaredStringParameter applies PostgreSQL's source input semantics
// and then its implicit coercion to the analyzed string-category target.
// Every returned slice aliases portal-owned input and the scans allocate
// nothing.
func coerceDeclaredStringParameter(
	raw []byte,
	format int16,
	oid int32,
	target sqldriver.ParamType,
) ([]byte, error) {
	if format == formatBinary && (!utf8.Valid(raw) || containsNUL(raw)) {
		return nil, newError(sqlstateCharacterNotInRepertoire,
			"a binary string parameter is not valid PostgreSQL UTF-8 text")
	}
	switch oid {
	case oidName:
		if len(raw) > postgresNameDataBytes {
			// PostgreSQL's text and binary input functions deliberately differ
			// here. namein clips an overlength textual identifier, while namerecv
			// rejects the same bytes instead of silently changing a binary value.
			if format == formatBinary {
				return nil, newError(sqlstateNameTooLong, "identifier too long").
					withDetail("Identifier must be less than 64 characters.")
			}
			raw = clipPostgresName(raw)
		}
	case oidBPChar:
		if target != sqldriver.ParamTypeBPChar {
			raw = trimBPCharSpaces(raw)
		}
	}
	// A cast into name clips regardless of the source parameter's wire format:
	// binary belongs to the source receive function, not to the target cast.
	if target == sqldriver.ParamTypeName && oid != oidName &&
		len(raw) > postgresNameDataBytes {
		raw = clipPostgresName(raw)
	}
	return raw, nil
}

const postgresNameDataBytes = 63

func clipPostgresName(raw []byte) []byte {
	end := postgresNameDataBytes
	for end > 0 && raw[end]&0xc0 == 0x80 {
		end--
	}
	return raw[:end:end]
}

func trimBPCharSpaces(raw []byte) []byte {
	end := len(raw)
	for end > 0 && raw[end-1] == ' ' {
		end--
	}
	return raw[:end:end]
}

// bindJSONDocument validates one complete JSON value while preserving its
// exact bytes. Objects and arrays are intentionally admitted here even though
// scalar predicate parameters reject them.
func bindJSONDocument(
	raw []byte,
	format int16,
	oid int32,
	slot *boundValueSlot,
) (any, error) {
	document := raw
	if format == formatBinary {
		switch oid {
		case oidJSON, oidText, oidVarchar, oidName, oidBPChar, oidUnknown, oidBytea:
			// These binary encodings are their bytes unchanged.
		case oidJSONB:
			if len(document) == 0 || document[0] != 1 {
				return nil, newError(sqlstateInvalidParameterValue,
					"a binary jsonb document must begin with version byte 1")
			}
			document = document[1:]
		default:
			return nil, newError(sqlstateFeatureNotSupported, fmt.Sprintf(
				"this server cannot decode a JSON document parameter of binary type OID %d",
				oid)).
				withHint("bind the document as json, jsonb, text, or bytea")
		}
	} else {
		switch oid {
		case 0, oidUnknown, oidJSON, oidJSONB, oidText, oidVarchar, oidName, oidBPChar, oidBytea:
		default:
			return nil, newError(sqlstateDatatypeMismatch, fmt.Sprintf(
				"a whole-document parameter cannot have scalar type OID %d", oid)).
				withHint("bind the document as json, jsonb, text, or bytea")
		}
	}
	if !utf8.Valid(document) || containsNUL(document) {
		return nil, newError(sqlstateCharacterNotInRepertoire,
			"value is not valid UTF-8")
	}
	if err := vibejson.Validate(document); err != nil {
		return nil, newError(sqlstateInvalidParameterValue,
			"value is not one valid JSON value")
	}
	slot.text = byteview.String(document)
	return &slot.text, nil
}

// bindValue converts one bound parameter into a value the engine can compare
// against.
//
// # Text format, no declared type
//
// This is the case almost every client produces, because ParameterDescription
// reports every parameter as unspecified and a client with no type to encode
// for sends the value's text. The rule is: text that spells a JSON scalar is
// that scalar, and anything else is a string. So 21 binds as the exact decimal
// 21, true binds as a boolean, and Ada Lovelace binds as a string.
//
// The rule is inferred and therefore has an edge, which is worth stating
// plainly rather than burying: a client binding the *string* "21" against a
// path holding strings gets a number and matches nothing, because the text on
// the wire is identical to the number's. There are three ways out and this
// package takes all three. A client may declare the parameter's OID in Parse,
// which is honoured below and is what pgx does when it knows the Go type. A
// client may bind in binary with a declared OID, which is unambiguous. Or the
// statement may be written with a literal, which this dialect spells in JSON
// and never has to guess about.
//
// The alternative rule — every untyped text parameter is a string — was
// rejected because it makes the overwhelmingly common case wrong: WHERE age >=
// $1 with 21 would compare a number against a string and, under this engine's
// cross-type total order, match nothing at all, silently.
//
// Numbers keep their exact decimal spelling as a [query.Number] rather than
// being parsed into a float64, for the same reason the result encoding does:
// this engine compares by exact decimal value, and 9007199254740993 must not
// become 9007199254740992 on the way in any more than on the way out.
func bindValue(
	raw []byte,
	format int16,
	oid int32,
	slot *boundValueSlot,
	decodeStore *[]byte,
) (any, error) {
	if format == formatBinary {
		return bindBinary(raw, oid, slot, decodeStore)
	}
	if !utf8.Valid(raw) || containsNUL(raw) {
		return nil, newError(sqlstateCharacterNotInRepertoire,
			"a text-format parameter is not valid UTF-8")
	}
	// raw already lives in the portal's retained argStore. Aliasing it avoids
	// making a second per-parameter copy merely to change the slice header into
	// a string header.
	text := byteview.String(raw)
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidFloat4, oidFloat8, oidNumeric:
		if !isJSONNumber(text) {
			// Do not echo the value. Besides leaking application data into an
			// ErrorResponse, quoting a hostile 16 MiB parameter can expand it
			// severalfold and leave that capacity pinned in the session writer.
			return nil, newError(sqlstateInvalidParameterValue,
				"parameter declared as a numeric type does not spell a JSON number")
		}
		slot.number = query.Number(text)
		return &slot.number, nil
	case oidBool:
		value, ok := pginput.Boolean(text)
		if !ok {
			return nil, newError(sqlstateInvalidTextRepresentation,
				"parameter declared boolean does not spell a boolean")
		}
		slot.boolean = value
		return &slot.boolean, nil
	case oidText, oidVarchar, oidName, oidBPChar:
		slot.text = text
		return &slot.text, nil
	case oidJSON, oidJSONB:
		return bindJSONScalar(raw, slot, decodeStore)
	case 0, oidUnknown:
		// Only an unspecified value uses the compatibility inference below.
		// A declared JSON value has JSON's quoted-string and validation
		// semantics and was handled above.
	default:
		return nil, newError(sqlstateFeatureNotSupported, fmt.Sprintf(
			"this server has no conversion for a parameter of type OID %d", oid)).
			withHint("bind the parameter without declaring its type, and it will be read " +
				"as a JSON scalar")
	}
	switch text {
	case "true":
		slot.boolean = true
		return &slot.boolean, nil
	case "false":
		slot.boolean = false
		return &slot.boolean, nil
	case "null":
		return nil, nil
	}
	if isJSONNumber(text) {
		slot.number = query.Number(text)
		return &slot.number, nil
	}
	slot.text = text
	return &slot.text, nil
}

// bindJSONScalar decodes one declared json/jsonb value without building a tree.
// Strings are unescaped directly into portal-owned capacity, numbers keep their
// exact spelling, and containers are refused explicitly because query
// predicates accept scalar literals only.
func bindJSONScalar(raw []byte, slot *boundValueSlot, decodeStore *[]byte) (any, error) {
	raw = trimJSONSpace(raw)
	if len(raw) == 0 {
		return nil, newError(sqlstateInvalidParameterValue,
			"a parameter declared as json/jsonb is empty")
	}
	switch raw[0] {
	case '"':
		start := len(*decodeStore)
		decoded, ok, err := (vibejson.RawValue{Src: raw}).AppendText(*decodeStore)
		if err != nil {
			return nil, newError(sqlstateInvalidParameterValue,
				fmt.Sprintf("parameter declared as json/jsonb is invalid JSON: %v", err))
		}
		if !ok {
			return nil, newError(sqlstateInvalidParameterValue,
				"parameter declared as json/jsonb is not a JSON string")
		}
		*decodeStore = decoded
		slot.text = byteview.String(decoded[start:len(decoded):len(decoded)])
		return &slot.text, nil
	case 't':
		if len(raw) == 4 && string(raw) == "true" {
			slot.boolean = true
			return &slot.boolean, nil
		}
	case 'f':
		if len(raw) == 5 && string(raw) == "false" {
			slot.boolean = false
			return &slot.boolean, nil
		}
	case 'n':
		if len(raw) == 4 && string(raw) == "null" {
			return nil, nil
		}
	case '[', '{':
		if err := vibejson.Validate(raw); err != nil {
			return nil, newError(sqlstateInvalidParameterValue,
				fmt.Sprintf("parameter declared as json/jsonb is invalid JSON: %v", err))
		}
		return nil, newError(sqlstateFeatureNotSupported,
			"JSON array and object parameters are not supported in scalar predicates").
			withHint("bind a JSON string, number, boolean, or null")
	default:
		text := byteview.String(raw)
		if isJSONNumber(text) {
			slot.number = query.Number(text)
			return &slot.number, nil
		}
	}
	return nil, newError(sqlstateInvalidParameterValue,
		"parameter declared as json/jsonb is not one valid JSON value")
}

func trimJSONSpace(raw []byte) []byte {
	start, end := 0, len(raw)
	for start < end && isJSONSpace(raw[start]) {
		start++
	}
	for end > start && isJSONSpace(raw[end-1]) {
		end--
	}
	return raw[start:end]
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// bindBinary decodes a parameter sent in binary format.
//
// A binary parameter with no declared type is refused, and the refusal is the
// correct answer rather than a limitation: binary encodings are per-type and a
// server that does not know the type cannot decode the bytes. Guessing would
// produce a value the client never sent, which is the failure mode this package
// avoids everywhere else.
func bindBinary(
	raw []byte,
	oid int32,
	slot *boundValueSlot,
	decodeStore *[]byte,
) (any, error) {
	switch oid {
	case oidBool:
		if len(raw) != 1 {
			return nil, badBinary("bool", len(raw), 1)
		}
		slot.boolean = raw[0] != 0
		return &slot.boolean, nil
	case oidInt2:
		if len(raw) != 2 {
			return nil, badBinary("int2", len(raw), 2)
		}
		slot.integer = int64(int16(binary.BigEndian.Uint16(raw)))
		return &slot.integer, nil
	case oidInt4:
		if len(raw) != 4 {
			return nil, badBinary("int4", len(raw), 4)
		}
		slot.integer = int64(int32(binary.BigEndian.Uint32(raw)))
		return &slot.integer, nil
	case oidInt8:
		if len(raw) != 8 {
			return nil, badBinary("int8", len(raw), 8)
		}
		slot.integer = int64(binary.BigEndian.Uint64(raw))
		return &slot.integer, nil
	case oidFloat4:
		if len(raw) != 4 {
			return nil, badBinary("float4", len(raw), 4)
		}
		value := float64(math.Float32frombits(binary.BigEndian.Uint32(raw)))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, nonFiniteBinary("float4")
		}
		slot.floating = value
		return &slot.floating, nil
	case oidFloat8:
		if len(raw) != 8 {
			return nil, badBinary("float8", len(raw), 8)
		}
		value := math.Float64frombits(binary.BigEndian.Uint64(raw))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, nonFiniteBinary("float8")
		}
		slot.floating = value
		return &slot.floating, nil
	case oidText, oidVarchar, oidName, oidBPChar, oidUnknown:
		// These types' binary encoding is their text bytes unchanged.
		if !utf8.Valid(raw) || containsNUL(raw) {
			return nil, newError(sqlstateCharacterNotInRepertoire,
				"a binary text parameter is not valid UTF-8")
		}
		slot.text = byteview.String(raw)
		return &slot.text, nil
	case oidJSON:
		// json's binary encoding is its text bytes unchanged.
		return bindJSONScalar(raw, slot, decodeStore)
	case oidJSONB:
		// PostgreSQL's jsonb binary format is one version byte followed by the
		// textual JSON representation. Version 1 is the only defined version.
		if len(raw) == 0 || raw[0] != 1 {
			return nil, newError(sqlstateInvalidParameterValue,
				"a binary jsonb parameter must begin with version byte 1")
		}
		return bindJSONScalar(raw[1:], slot, decodeStore)
	}
	return nil, newError(sqlstateFeatureNotSupported, fmt.Sprintf(
		"this server cannot decode a binary parameter of type OID %d", oid)).
		withHint("send the parameter in text format, or declare a type this server decodes: " +
			"bool, int2, int4, int8, float4, float8, text, varchar, json, or jsonb")
}

func badBinary(name string, size, expected int) error {
	code := sqlstateInvalidBinaryRepresentation
	if size < expected {
		code = sqlstateProtocolViolation
	}
	return newError(code, fmt.Sprintf(
		"a binary %s parameter is the wrong length: %d bytes", name, size))
}

func nonFiniteBinary(name string) error {
	return newError(sqlstateInvalidParameterValue, fmt.Sprintf(
		"a binary %s parameter is non-finite; JSON numbers must be finite", name))
}

// isJSONNumber reports whether text is a JSON number.
//
// It is JSON's grammar rather than SQL's looser one on purpose, and matches the
// rule this dialect's own literals follow: "007", "1.", "+1", and "0x10" are
// not numbers here, so a parameter spelling one of them binds as a string
// rather than as a number the engine would have to invent a reading for.
func isJSONNumber(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '-' {
		i++
	}
	digits := func() int {
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		return i - start
	}
	n := digits()
	if n == 0 || (n > 1 && text[i-n] == '0') {
		return false
	}
	if i < len(text) && text[i] == '.' {
		i++
		if digits() == 0 {
			return false
		}
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		if digits() == 0 {
			return false
		}
	}
	return i == len(text)
}

// commandTag renders the CommandComplete tag for a completed SELECT.
func commandTag(rows int) string { return "SELECT " + strconv.Itoa(rows) }
