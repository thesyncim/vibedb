package pgwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/thesyncim/vibedb/query"
)

// The extended query protocol: Parse, Bind, Describe, Execute, Close, Flush,
// Sync.
//
// This is what real client libraries use, and it maps onto this engine's
// prepared statements almost exactly: Parse is [query.PrepareStatement], Bind
// is the argument vector, Execute is RunInto, and the plan cache a
// [query.Statement] already keeps is the reason a named statement is worth
// naming.
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
// A statement's placeholders have no declared type here and cannot have one:
// this dialect's '?' compares against whatever the path holds, and the path is
// schemaless. ParameterDescription therefore reports type 0, "unspecified", for
// every parameter, which is a legal answer and the only true one.
//
// That pushes the question onto Bind, and the rule is stated on [bindValue]. It
// is the one place in this package where a value's meaning is inferred rather
// than carried, and the inference is the same one the dialect's own literal
// grammar makes: a parameter that spells a JSON scalar is that scalar, and
// anything else is a string. A client that needs the distinction removed can
// declare the parameter's OID in Parse, which this server honours.

// extended handles one extended-protocol message.
func (s *session) extended(tag byte) error {
	// Sync is the resynchronization point and is processed even in the error
	// state; every other message is discarded there.
	if tag == msgSync {
		s.failed = false
		s.w.readyForQuery(statusIdle)
		return s.flush()
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
		err = s.handleParse()
	case msgBind:
		err = s.handleBind()
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
	var pg *pgError
	if !errors.As(err, &pg) {
		return err
	}
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

func (s *session) handleParse() error {
	m := &s.msg
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
	stmt, err := s.prepare(m.name, m.query)
	if err != nil {
		return err
	}
	stmt.paramOIDs = append(stmt.paramOIDs[:0], m.paramOIDs...)
	if old, ok := s.statements[m.name]; ok {
		// Replacing the unnamed statement. Every portal built from it is
		// invalidated, because its plan is about to be released.
		s.closePortalsOf(old)
		old.release()
	}
	s.statements[m.name] = stmt
	s.w.parseComplete()
	return nil
}

func (s *session) handleBind() error {
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
	if old, ok := s.portals[m.portal]; ok {
		delete(s.portals, m.portal)
		old.release()
		p := &portal{name: m.portal, stmt: stmt,
			args: old.args, argStore: old.argStore, formats: old.formats}
		return s.bindInto(p, m, stmt)
	}
	return s.bindInto(&portal{name: m.portal, stmt: stmt}, m, stmt)
}

// bindInto completes a Bind, publishing the portal only once every value has
// been converted.
func (s *session) bindInto(p *portal, m *frontendMessage, stmt *prepared) error {
	p.formats = append(p.formats[:0], m.resultFormats...)
	var err error
	if p.args, p.argStore, err = bindArgs(p.args, p.argStore, m, stmt); err != nil {
		return err
	}
	s.portals[m.portal] = p
	s.w.bindComplete()
	return nil
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
		s.w.parameterDesc(stmt.wireParams)
		s.describeRows(stmt, nil)
		return nil
	}
	p, ok := s.portals[m.portal]
	if !ok {
		return newError(sqlstateInvalidCursorName,
			fmt.Sprintf("portal %q does not exist", m.portal))
	}
	s.describeRows(p.stmt, p.formats)
	return nil
}

// describeRows writes the RowDescription, or NoData for a statement that
// returns no rows.
func (s *session) describeRows(stmt *prepared, formats []int16) {
	if len(stmt.cols) == 0 {
		s.w.noData()
		return
	}
	s.w.rowDescription(stmt.cols, formats)
}

func (s *session) handleExecute() error {
	m := &s.msg
	p, ok := s.portals[m.portal]
	if !ok {
		return newError(sqlstateInvalidCursorName,
			fmt.Sprintf("portal %q does not exist", m.portal))
	}
	return s.execute(p, m.maxRows)
}

func (s *session) handleClose() error {
	m := &s.msg
	if m.target == targetStatement {
		if stmt, ok := s.statements[m.name]; ok {
			s.closePortalsOf(stmt)
			stmt.release()
			delete(s.statements, m.name)
		}
	} else if p, ok := s.portals[m.portal]; ok {
		p.release()
		delete(s.portals, m.portal)
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
		}
	}
}

// bindArgs converts a Bind message's parameters into the engine's argument
// vector, copying every byte it keeps.
//
// The copy is not optional. The parameter values view the reader's message
// buffer, which the next frontend message overwrites, and a portal outlives
// arbitrarily many messages. Copying into one contiguous store rather than
// per-value keeps a bound portal at two allocations regardless of its parameter
// count.
func bindArgs(args []any, store []byte, m *frontendMessage, stmt *prepared) ([]any, []byte, error) {
	args = args[:0]
	store = store[:0]
	// The engine binds by placeholder position, and the wire binds by parameter
	// number, so a rewritten statement reads its values through paramOrder. A
	// repeated $n produces two placeholders reading one wire value, which is
	// why the loop is over placeholders and not over the message's parameters.
	count := len(m.params)
	if stmt.paramOrder != nil {
		count = len(stmt.paramOrder)
	}
	// Reserve the whole store up front so the views taken below cannot be
	// invalidated by a later append growing the backing array.
	total := 0
	for i := range count {
		total += len(m.params[wireIndex(stmt, i)])
	}
	if cap(store) < total {
		store = make([]byte, 0, total)
	}
	for i := range count {
		wire := wireIndex(stmt, i)
		raw := m.params[wire]
		if raw == nil {
			args = append(args, nil)
			continue
		}
		start := len(store)
		store = append(store, raw...)
		value, err := bindValue(store[start:len(store):len(store)],
			formatFor(m.paramFormats, wire), oidAt(stmt.paramOIDs, wire))
		if err != nil {
			return args, store, err
		}
		args = append(args, value)
	}
	return args, store, nil
}

// wireIndex answers which of the message's parameters the i-th placeholder
// reads.
func wireIndex(stmt *prepared, i int) int {
	if stmt.paramOrder == nil {
		return i
	}
	return stmt.paramOrder[i] - 1
}

func oidAt(oids []int32, i int) int32 {
	if i < 0 || i >= len(oids) {
		return 0
	}
	return oids[i]
}

// PostgreSQL OIDs a client may declare for a parameter. Only the ones whose
// binary encoding is unambiguous and whose meaning maps onto a JSON scalar are
// listed; a parameter declared as anything else is refused rather than guessed
// at.
const (
	oidInt2    = 21
	oidInt4    = 23
	oidFloat4  = 700
	oidFloat8  = 701
	oidVarchar = 1043
	oidName    = 19
	oidBPChar  = 1042
	oidUnknown = 705
)

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
func bindValue(raw []byte, format int16, oid int32) (any, error) {
	if format == formatBinary {
		return bindBinary(raw, oid)
	}
	text := string(raw)
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidFloat4, oidFloat8, oidNumeric:
		if !isJSONNumber(text) {
			return nil, newError(sqlstateInvalidParameterValue, fmt.Sprintf(
				"parameter declared as a numeric type does not spell a number: %q", text))
		}
		return query.Number(text), nil
	case oidBool:
		switch text {
		case "t", "true", "TRUE", "on", "1", "y", "yes":
			return true, nil
		case "f", "false", "FALSE", "off", "0", "n", "no":
			return false, nil
		}
		return nil, newError(sqlstateInvalidParameterValue,
			fmt.Sprintf("parameter declared boolean does not spell a boolean: %q", text))
	case oidText, oidVarchar, oidName, oidBPChar:
		return text, nil
	case 0, oidUnknown, oidJSON, oidJSONB:
		// Unspecified, or a JSON type, which is the same inference: the text is
		// read as a JSON scalar if it is one.
	default:
		return nil, newError(sqlstateFeatureNotSupported, fmt.Sprintf(
			"this server has no conversion for a parameter of type OID %d", oid)).
			withHint("bind the parameter without declaring its type, and it will be read " +
				"as a JSON scalar")
	}
	switch text {
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "null":
		return nil, nil
	}
	if isJSONNumber(text) {
		return query.Number(text), nil
	}
	return text, nil
}

// bindBinary decodes a parameter sent in binary format.
//
// A binary parameter with no declared type is refused, and the refusal is the
// correct answer rather than a limitation: binary encodings are per-type and a
// server that does not know the type cannot decode the bytes. Guessing would
// produce a value the client never sent, which is the failure mode this package
// avoids everywhere else.
func bindBinary(raw []byte, oid int32) (any, error) {
	switch oid {
	case oidBool:
		if len(raw) != 1 {
			return nil, badBinary("bool", len(raw))
		}
		return raw[0] != 0, nil
	case oidInt2:
		if len(raw) != 2 {
			return nil, badBinary("int2", len(raw))
		}
		return int64(int16(binary.BigEndian.Uint16(raw))), nil
	case oidInt4:
		if len(raw) != 4 {
			return nil, badBinary("int4", len(raw))
		}
		return int64(int32(binary.BigEndian.Uint32(raw))), nil
	case oidInt8:
		if len(raw) != 8 {
			return nil, badBinary("int8", len(raw))
		}
		return int64(binary.BigEndian.Uint64(raw)), nil
	case oidFloat4:
		if len(raw) != 4 {
			return nil, badBinary("float4", len(raw))
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), nil
	case oidFloat8:
		if len(raw) != 8 {
			return nil, badBinary("float8", len(raw))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(raw)), nil
	case oidText, oidVarchar, oidName, oidBPChar, oidUnknown:
		// These types' binary encoding is their text bytes unchanged.
		return string(raw), nil
	case oidJSON:
		// json's binary encoding is its text bytes unchanged, so it takes the
		// same inference an untyped text parameter takes.
		return bindValue(raw, formatText, oidJSON)
	}
	return nil, newError(sqlstateFeatureNotSupported, fmt.Sprintf(
		"this server cannot decode a binary parameter of type OID %d", oid)).
		withHint("send the parameter in text format, or declare a type this server decodes: " +
			"bool, int2, int4, int8, float4, float8, text, varchar, or json")
}

func badBinary(name string, size int) error {
	return newError(sqlstateInvalidParameterValue, fmt.Sprintf(
		"a binary %s parameter is the wrong length: %d bytes", name, size))
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
