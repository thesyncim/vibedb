package pgwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibejson/x/byteview"
)

// The frontend message reader, and the field cursor every message body is
// decoded through.
//
// # Security posture
//
// This file is the only part of the package that reads bytes an unauthenticated
// remote peer chose, and it is written on the assumption that every one of them
// is hostile. Three properties are maintained, and each is maintained
// structurally rather than by remembering to check:
//
// First, no allocation is ever sized by a number the peer sent without that
// number having first been bounded against a constant. A frontend message
// declares its own length in a signed 32-bit field, which means the wire can
// carry a length of two gigabytes, a negative length, or a length smaller than
// the four bytes the field itself occupies. [reader.next] rejects all three
// before it reaches a make, and the accepted range is a compile-time constant
// ([maxMessageBody], or [maxStartupPacket] for the packet that arrives before
// authentication). The read buffer a session retains between messages is capped
// separately at [retainedBuffer], so a single oversized statement cannot leave
// an idle connection holding megabytes for the rest of its life.
//
// Second, no declared count is trusted against the bytes actually present.
// Bind declares a parameter count and then a length-prefixed value for each,
// and a message claiming 32767 parameters in a nine-byte body is a perfectly
// legal encoding of a lie. [fields.count] refuses a count whose elements cannot
// fit in what remains of the body, which turns that lie into an immediate
// protocol error instead of a large speculative slice. The same rule applies
// to every length-prefixed value: [fields.slice] compares against the remaining
// body, never against the message length.
//
// Third, a malformed body can never panic. The decoder is a cursor over a byte
// slice ([fields]) whose accessors bounds-check, return a zero value, and latch
// a failure flag; once latched it stays latched, so a caller that decodes six
// fields and checks once at the end is as safe as one that checks after each.
// No accessor slices without checking, no accessor can produce a negative
// length, and the cursor never holds an index that could go out of range. That
// is what makes [FuzzFrontendMessage] a meaningful test rather than a
// formality: arbitrary bytes must decode to a message or an error, and there is
// no third outcome available.
//
// What this file deliberately does not do is interpret. It produces a decoded
// message with its fields separated and nothing else checked — an unknown tag,
// a nonsensical format code, a Describe of neither 'S' nor 'P' are all reported
// as decode failures, but a valid Bind naming a portal that does not exist is
// not this layer's problem. Keeping policy out of the parser is what lets the
// parser be audited for memory safety in isolation.

// errMessageTooLarge is returned when a peer declares a message body larger
// than this server will read. It is a protocol violation rather than a resource
// error because the only way to produce one is to send a length no real client
// sends.
var errMessageTooLarge = errors.New("pgwire: frontend message is larger than this server accepts")

// errBadLength reports a message length field that cannot describe a message:
// negative, or smaller than the four bytes the field itself occupies.
var errBadLength = errors.New("pgwire: frontend message length field is not a valid length")

// errTruncated reports a message body that ended in the middle of a field.
var errTruncated = errors.New("pgwire: frontend message body ended inside a field")

// errUnterminated reports a string field with no terminating NUL byte.
var errUnterminated = errors.New("pgwire: frontend message string field is not NUL-terminated")

// errTrailing reports bytes left over after a message decoded successfully,
// which means the sender and this decoder disagree about the message's shape.
var errTrailing = errors.New("pgwire: frontend message has trailing bytes after its last field")

// errNameTooLong reports a prepared-statement or portal name past
// [maxIdentifier].
var errNameTooLong = errors.New("pgwire: prepared statement or portal name is too long")

// errImpossibleCount reports an element count larger than the remaining body
// could hold. It is separated from plain truncation because it is the signature
// of a deliberately malformed message rather than a short read.
var errImpossibleCount = errors.New(
	"pgwire: frontend message declares more elements than its body can hold")

// A reader decodes framed frontend messages from one connection.
//
// It is not safe for concurrent use and is not meant to be: one connection is
// read by exactly one goroutine, which is also what makes the returned body
// slice's lifetime rule ("valid until the next call") a rule anyone can hold
// to.
type reader struct {
	br *bufio.Reader
	// buf is the retained body buffer, never larger than retainedBuffer.
	buf []byte
	// hdr holds the five header bytes of a tagged message.
	hdr [5]byte
}

func newReader(r io.Reader, bufSize int) *reader {
	return &reader{br: bufio.NewReaderSize(r, bufSize)}
}

// body returns storage of exactly size bytes to read a message into.
//
// The split at retainedBuffer is the whole point: an ordinary message reuses
// one buffer forever and allocates nothing, and an unusually large one gets
// storage this reader does not keep. Without the split, a session that once
// received a ten-megabyte statement would hold ten megabytes until it closed,
// and a peer could open a thousand connections and do that deliberately.
func (r *reader) body(size int) []byte {
	if size > retainedBuffer {
		return make([]byte, size)
	}
	if cap(r.buf) < size {
		r.buf = make([]byte, retainedBuffer)
	}
	return r.buf[:size]
}

// next reads one tagged frontend message and returns its tag and body. The
// body is valid until the next call on r.
//
// The length is validated against a constant before it is used for anything at
// all. That ordering is the invariant: there is no path from a peer-supplied
// integer to an allocation that does not pass through the bounds check above
// it.
func (r *reader) next(max int) (byte, []byte, error) {
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		return 0, nil, err
	}
	length := int32(binary.BigEndian.Uint32(r.hdr[1:]))
	if length < 4 {
		return 0, nil, errBadLength
	}
	if int64(length)-4 > int64(max) {
		return 0, nil, errMessageTooLarge
	}
	size := int(length) - 4
	if size == 0 {
		return r.hdr[0], nil, nil
	}
	body := r.body(size)
	if _, err := io.ReadFull(r.br, body); err != nil {
		return 0, nil, err
	}
	return r.hdr[0], body, nil
}

// startup reads the untagged startup packet, which carries a length and then a
// version or request code in place of a tag.
//
// It is bounded far more tightly than a tagged message because it arrives from
// a peer that has not authenticated and may never; [maxStartupPacket] is
// PostgreSQL's own ceiling for the same reason.
func (r *reader) startup() (int32, []byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r.br, head[:]); err != nil {
		return 0, nil, err
	}
	length := int32(binary.BigEndian.Uint32(head[:]))
	if length < 8 {
		return 0, nil, errBadLength
	}
	if int64(length) > int64(maxStartupPacket) {
		return 0, nil, errMessageTooLarge
	}
	body := r.body(int(length) - 4)
	if _, err := io.ReadFull(r.br, body); err != nil {
		return 0, nil, err
	}
	code := int32(binary.BigEndian.Uint32(body[:4]))
	return code, body[4:], nil
}

// A fields cursor decodes one message body.
//
// Every accessor is total: given any remaining bytes it either consumes exactly
// what it needs and returns a value, or consumes nothing, latches a failure,
// and returns a zero. Because the failure is sticky, a decoder may read a whole
// message and test once at the end; a latched cursor keeps returning zeros
// rather than reading past anything. The first failure's reason is kept so the
// error the client sees names what was actually wrong rather than reporting
// every malformed body as "truncated".
type fields struct {
	b   []byte
	why error
}

// fail latches the cursor. Clearing b as well means every later accessor takes
// its short path without a second flag test, and makes an accidental read after
// failure impossible rather than merely wrong. Only the first reason is kept,
// because every later one is a consequence of it.
func (f *fields) fail(why error) {
	if f.why == nil {
		f.why = why
	}
	f.b = nil
}

func (f *fields) bad() bool { return f.why != nil }

func (f *fields) uint8() byte {
	if len(f.b) < 1 {
		f.fail(errTruncated)
		return 0
	}
	v := f.b[0]
	f.b = f.b[1:]
	return v
}

// int16 decodes a signed 16-bit field. It is signed on the wire and callers
// must treat a negative value as invalid rather than converting it, which is
// why this returns int and not a count.
func (f *fields) int16() int {
	if len(f.b) < 2 {
		f.fail(errTruncated)
		return 0
	}
	v := int16(binary.BigEndian.Uint16(f.b))
	f.b = f.b[2:]
	return int(v)
}

func (f *fields) int32() int32 {
	if len(f.b) < 4 {
		f.fail(errTruncated)
		return 0
	}
	v := int32(binary.BigEndian.Uint32(f.b))
	f.b = f.b[4:]
	return v
}

// cstring decodes a NUL-terminated string and copies it for callers such as
// startup negotiation that retain fields past the next read. Ordinary frontend
// dispatch uses cstringView and copies only at the specific ownership boundary.
func (f *fields) cstring() string {
	i := bytes.IndexByte(f.b, 0)
	if i < 0 {
		f.fail(errUnterminated)
		return ""
	}
	s := string(f.b[:i])
	f.b = f.b[i+1:]
	return s
}

// cstringView is cstring's message-lifetime form. Frontend dispatch handles a
// decoded message before the reader can reuse its body, so transient names and
// query text can borrow these bytes and avoid one allocation per message.
// Handlers that retain a value must copy it at their ownership boundary.
func (f *fields) cstringView() string {
	i := bytes.IndexByte(f.b, 0)
	if i < 0 {
		f.fail(errUnterminated)
		return ""
	}
	s := byteview.String(f.b[:i])
	f.b = f.b[i+1:]
	return s
}

func (f *fields) nameView() string {
	s := f.cstringView()
	if len(s) > maxIdentifier {
		f.fail(errNameTooLong)
		return ""
	}
	return s
}

// slice consumes n bytes and returns a view into the body. The view shares the
// reader's buffer and is only valid until the next message, which is exactly
// the lifetime a decoded message has.
func (f *fields) slice(n int) []byte {
	if n < 0 || n > len(f.b) {
		f.fail(errTruncated)
		return nil
	}
	v := f.b[:n]
	f.b = f.b[n:]
	return v
}

// count decodes an element count and refuses one the remaining body cannot
// possibly hold.
//
// This is the check that makes a hostile Bind cheap. A count is an int16 and
// each element it introduces occupies at least elem bytes, so a body with
// fewer than count*elem bytes left is describing elements that are not there.
// Refusing here means the decoder never allocates a slice for elements it has
// not yet seen the bytes for, which is the difference between a protocol error
// and a peer choosing this server's allocation sizes.
func (f *fields) count(elem int) int {
	n := f.int16()
	if f.bad() {
		return 0
	}
	if n < 0 || n > maxParameters || n*elem > len(f.b) {
		f.fail(errImpossibleCount)
		return 0
	}
	return n
}

// end reports whether the body was consumed exactly.
//
// Trailing bytes are an error rather than something to ignore. A body longer
// than its fields means the peer and this decoder disagree about the message,
// and continuing on the assumption that the parts we understood were the parts
// that mattered is how a protocol implementation ends up executing something
// the sender did not write.
func (f *fields) end() error {
	if f.why != nil {
		return f.why
	}
	if len(f.b) != 0 {
		return errTrailing
	}
	return nil
}
