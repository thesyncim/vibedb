package pgwire

import (
	"bufio"
	"encoding/binary"
	"io"
)

// The backend message writer.
//
// Messages are built into one retained scratch buffer and copied into a
// bufio.Writer when they are complete, rather than written field by field. The
// reason is the length prefix: a message's int32 length is the second thing on
// the wire and the last thing known, so a streaming writer would have to either
// buffer anyway or seek, and seeking is not available on a socket. Building
// into a buffer whose capacity survives between messages makes a DataRow free
// of allocation once a connection has warmed up, which matters because a
// DataRow is written once per row and everything else is written once per
// query.
//
// The first write error is latched and every later call becomes a no-op. A
// backend message stream has no recovery: if a DataRow could not be written
// there is no way to tell the client that, since telling it would require
// writing. Latching means the session discovers the failure at its next flush
// and closes the connection, and no intermediate code has to thread an error
// through a dozen calls that can only ever fail for the same reason.

// A writer serializes backend messages onto one connection.
type writer struct {
	bw  *bufio.Writer
	buf []byte
	err error
}

func newWriter(w io.Writer, bufSize int) *writer {
	return &writer{bw: bufio.NewWriterSize(w, bufSize)}
}

// begin starts a message with the given tag, reserving the four bytes its
// length will be patched into.
func (w *writer) begin(tag byte) {
	w.buf = append(w.buf[:0], tag, 0, 0, 0, 0)
}

// end patches the length and hands the message to the buffered writer.
//
// The length covers itself but not the tag, which is the protocol's own
// convention and the single most commonly mis-transcribed detail in it.
func (w *writer) end() {
	binary.BigEndian.PutUint32(w.buf[1:5], uint32(len(w.buf)-1))
	w.write(w.buf)
}

func (w *writer) write(b []byte) {
	if w.err != nil {
		return
	}
	_, w.err = w.bw.Write(b)
}

func (w *writer) byte(b byte) { w.buf = append(w.buf, b) }

func (w *writer) int16(v int) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(int16(v)))
}

func (w *writer) int32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

// str appends a NUL-terminated string.
func (w *writer) str(s string) {
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}

func (w *writer) raw(b []byte)       { w.buf = append(w.buf, b...) }
func (w *writer) rawString(s string) { w.buf = append(w.buf, s...) }

// flush pushes buffered messages onto the connection and reports the first
// error seen since the last flush.
func (w *writer) flush() error {
	if w.err == nil {
		w.err = w.bw.Flush()
	}
	return w.err
}

// The individual backend messages. Each is a single function so the wire shape
// of every message this server can emit is readable in one place, and so a
// change to one cannot accidentally alter another.

func (w *writer) authenticationOK() {
	w.begin(msgAuthentication)
	w.int32(authOK)
	w.end()
}

func (w *writer) authenticationSASL(mechanisms ...string) {
	w.begin(msgAuthentication)
	w.int32(authSASL)
	for _, m := range mechanisms {
		w.str(m)
	}
	w.byte(0) // the mechanism list is itself NUL-terminated
	w.end()
}

func (w *writer) authenticationSASLContinue(data []byte) {
	w.begin(msgAuthentication)
	w.int32(authSASLContinue)
	w.raw(data)
	w.end()
}

func (w *writer) authenticationSASLFinal(data []byte) {
	w.begin(msgAuthentication)
	w.int32(authSASLFinal)
	w.raw(data)
	w.end()
}

func (w *writer) parameterStatus(name, value string) {
	w.begin(msgParameterStatus)
	w.str(name)
	w.str(value)
	w.end()
}

func (w *writer) backendKeyData(pid, key int32) {
	w.begin(msgBackendKeyData)
	w.int32(pid)
	w.int32(key)
	w.end()
}

func (w *writer) readyForQuery(status byte) {
	w.begin(msgReadyForQuery)
	w.byte(status)
	w.end()
}

func (w *writer) commandComplete(tag string) {
	w.begin(msgCommandComplete)
	w.str(tag)
	w.end()
}

func (w *writer) emptyQueryResponse() {
	w.begin(msgEmptyQuery)
	w.end()
}

func (w *writer) noData() {
	w.begin(msgNoData)
	w.end()
}

func (w *writer) parseComplete()   { w.begin(msgParseComplete); w.end() }
func (w *writer) bindComplete()    { w.begin(msgBindComplete); w.end() }
func (w *writer) closeComplete()   { w.begin(msgCloseComplete); w.end() }
func (w *writer) portalSuspended() { w.begin(msgPortalSuspended); w.end() }
func (w *writer) parameterDesc(n int) {
	w.begin(msgParameterDesc)
	w.int16(n)
	for range n {
		// Every placeholder in this dialect accepts the same value domain, so
		// there is no per-parameter type to declare. Zero is the protocol's
		// "unspecified", which is what a client that asked is entitled to.
		w.int32(0)
	}
	w.end()
}
