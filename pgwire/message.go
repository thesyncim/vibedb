package pgwire

import "errors"

// Decoding a frontend message body into its fields.
//
// The decoder fills a caller-owned [frontendMessage] rather than returning a
// new one, so a session that handles a million Binds reuses one set of slices
// instead of allocating a parameter vector per message. Every slice it produces
// is a view into the reader's body buffer and is therefore valid only until the
// next message is read; a handler that needs a value to outlive the message —
// a portal's bound parameters, above all — copies it, and says so where it
// does.

// errUnknownMessage reports a frontend tag that is not in the protocol at all.
var errUnknownMessage = errors.New("pgwire: unrecognized frontend message type")

// errUnimplementedSubprotocol reports a message that is part of the protocol
// and belongs to a subprotocol this server does not implement. It is separate
// from errUnknownMessage because the two deserve different answers: an
// unrecognized tag is a protocol violation, and a recognized one from a
// subprotocol we never invited the client into is a missing feature.
var errUnimplementedSubprotocol = errors.New(
	"pgwire: this server implements neither the copy subprotocol nor the function-call " +
		"subprotocol")

// errBadTarget reports a Describe or Close naming neither a statement nor a
// portal.
var errBadTarget = errors.New(
	"pgwire: Describe and Close must name a prepared statement ('S') or a portal ('P')")

// errBadFormat reports a format code that is neither text nor binary.
var errBadFormat = errors.New("pgwire: format code must be 0 (text) or 1 (binary)")

// errBadRowLimit reports a negative Execute row limit.
var errBadRowLimit = errors.New("pgwire: Execute row limit must not be negative")

// A frontendMessage is one decoded client message. Which fields are meaningful
// depends on tag; the rest keep whatever the previous decode left, which is
// harmless because every handler reads only the fields its own tag defines.
type frontendMessage struct {
	tag byte

	// name is the prepared-statement name for Parse, Describe 'S' and Close
	// 'S', and the statement a Bind draws from.
	name string
	// portal is the portal name for Bind, Execute, Describe 'P' and Close 'P'.
	portal string
	// query is the statement text of Query and Parse.
	query string
	// target is the 'S' or 'P' of Describe and Close.
	target byte
	// maxRows is Execute's row limit; zero means "all rows".
	maxRows int32

	// paramOIDs are the parameter type hints of Parse. This server ignores
	// their values (see extended.go) but decodes them because a message it did
	// not fully decode is a message it cannot bound.
	paramOIDs []int32
	// paramFormats and resultFormats are Bind's format-code arrays, already
	// validated to be text or binary.
	paramFormats  []int16
	resultFormats []int16
	// params are Bind's parameter values, nil for a SQL NULL. They view the
	// reader's buffer.
	params [][]byte

	// payload is the uninterpreted body of an authentication response, which
	// the SASL exchange reads and nothing else does.
	payload []byte
}

// reset drops every view the previous message left behind.
//
// It clears to capacity rather than truncating to zero, and that distinction is
// the whole point. A parameter slice viewing a body larger than
// [retainedBuffer] points into storage the reader deliberately did not keep, and
// truncating with params[:0] leaves those pointers live past len — where the
// garbage collector still finds them, because it scans a slice to its capacity.
// The observable effect is that one rejected multi-megabyte Bind pins its whole
// body for as long as the session lives, which is exactly what the reader's
// retained-buffer cap exists to prevent.
func (m *frontendMessage) reset() {
	clear(m.params[:cap(m.params)])
	m.params = m.params[:0]
	m.payload = nil
	m.query = ""
}

// decodeFrontend fills m from one message body.
//
// It validates shape and nothing else: that the fields are present, that the
// counts are possible, that a format code is one of the two that exist, and
// that the body was consumed exactly. Whether the named portal exists, whether
// the parameter count matches the statement, and whether the SQL is anything
// this engine can run are all decisions for layers that know about sessions.
func decodeFrontend(m *frontendMessage, tag byte, body []byte) error {
	m.reset()
	m.tag = tag
	f := fields{b: body}
	switch tag {
	case msgQuery:
		m.query = f.cstring()

	case msgParse:
		m.name = f.name()
		m.query = f.cstring()
		n := f.count(4)
		m.paramOIDs = m.paramOIDs[:0]
		for range n {
			m.paramOIDs = append(m.paramOIDs, f.int32())
		}

	case msgBind:
		m.portal = f.name()
		m.name = f.name()
		var err error
		if m.paramFormats, err = decodeFormats(&f, m.paramFormats); err != nil {
			return err
		}
		// A parameter is an int32 length followed by that many bytes, so four
		// bytes is the smallest one can be and count's bound is exact.
		n := f.count(4)
		m.params = m.params[:0]
		for range n {
			size := f.int32()
			if size == -1 {
				// -1 is the protocol's SQL NULL. It is the only negative length
				// with a meaning, and every other one is a malformed message.
				m.params = append(m.params, nil)
				continue
			}
			m.params = append(m.params, f.slice(int(size)))
		}
		if m.resultFormats, err = decodeFormats(&f, m.resultFormats); err != nil {
			return err
		}

	case msgDescribe, msgClose:
		m.target = f.uint8()
		name := f.name()
		if m.target == targetPortal {
			m.portal = name
		} else {
			m.name = name
		}
		if !f.bad() && m.target != targetStatement && m.target != targetPortal {
			return errBadTarget
		}

	case msgExecute:
		m.portal = f.name()
		m.maxRows = f.int32()
		if !f.bad() && m.maxRows < 0 {
			return errBadRowLimit
		}

	case msgSync, msgFlush, msgTerminate:
		// All three are bodiless; end below rejects a body that is not.

	case msgPasswordOrSASL:
		m.payload = f.slice(len(f.b))

	case msgFunctionCall, msgCopyData, msgCopyDone, msgCopyFail:
		// These are protocol features this server does not implement. They are
		// named rather than swept into the default case so the session can
		// answer 0A000 — a missing feature — instead of 08P01, which would tell
		// the client its message was malformed when it was merely unwelcome.
		return errUnimplementedSubprotocol

	default:
		return errUnknownMessage
	}
	return f.end()
}

// decodeFormats reads one of Bind's two format-code arrays into dst's storage.
//
// A format code is validated here rather than where it is used because an
// invalid one is a malformed message, not a request this server is declining:
// there are exactly two format codes in the protocol and a third value has no
// meaning to reject on policy grounds.
func decodeFormats(f *fields, dst []int16) ([]int16, error) {
	n := f.count(2)
	dst = dst[:0]
	for range n {
		code := f.int16()
		if f.bad() {
			break
		}
		if code != formatText && code != formatBinary {
			return dst, errBadFormat
		}
		dst = append(dst, int16(code))
	}
	return dst, nil
}

// formatFor answers the format code for the index'th item under the protocol's
// three-case rule: no codes means every item is text, one code applies to every
// item, and otherwise there is one code per item.
//
// The rule is stated once, here, because getting it wrong for result columns
// and right for parameters (or the reverse) produces a server that works with
// one client and silently mis-encodes for another.
func formatFor(codes []int16, index int) int16 {
	switch len(codes) {
	case 0:
		return formatText
	case 1:
		return codes[0]
	default:
		if index < 0 || index >= len(codes) {
			return formatText
		}
		return codes[index]
	}
}
