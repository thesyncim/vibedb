package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// The highest-value test in this package: arbitrary bytes through the frontend
// message decoder must never panic, never hang, and never allocate more than
// the declared bounds allow.
//
// The target drives the whole untrusted path, not just the field decoder: the
// same [reader] a session uses frames the input, so a corpus entry exercises
// the length validation, the buffer sizing, and the per-message decode in the
// order and with the state a real connection would. Feeding [decodeFrontend]
// alone would leave the framing — where every peer-chosen length lives —
// untested.
//
// Three properties are asserted, and the third is the one that needs a
// mechanism rather than an eye. A panic fails by definition. A hang is bounded
// because the input is finite and every read either consumes bytes or returns
// an error, so a target that returned at all did not loop. And unbounded
// allocation is caught structurally rather than by measuring the heap: after
// each input, the reader's retained buffer must still be within its cap and
// every slice the decoder produced must be no longer than the input that
// produced it. Those two capacities are exactly where a trusted length would
// show up, and unlike a heap measurement they are deterministic — runtime
// totals are process-wide and the fuzzer runs sixteen workers at once, so a
// heap delta would measure the other fifteen.

func FuzzFrontendMessage(f *testing.F) {
	// Seeds: one well-formed message of each shape the decoder has a branch
	// for, then the malformed shapes that have historically broken hand-written
	// protocol decoders.
	seed := func(tag byte, body []byte) {
		f.Add(frame(tag, body))
	}
	seed(msgQuery, append([]byte("SELECT 1"), 0))
	seed(msgParse, parseMsg("s", "SELECT a FROM b WHERE c = ?", 23))
	seed(msgBind, bindMsg("p", "s", []int16{0}, [][]byte{[]byte("1"), nil}, []int16{1}))
	seed(msgDescribe, describeMsg(targetStatement, "s"))
	seed(msgExecute, executeMsg("p", 10))
	seed(msgClose, closeMsg(targetPortal, "p"))
	seed(msgSync, nil)
	seed(msgFlush, nil)
	seed(msgTerminate, nil)
	seed(msgPasswordOrSASL, []byte("SCRAM-SHA-256\x00\x00\x00\x00\x05n,,r="))

	// A length that claims almost four gigabytes.
	f.Add([]byte{msgQuery, 0xff, 0xff, 0xff, 0xff})
	// A length smaller than the length field itself.
	f.Add([]byte{msgQuery, 0x00, 0x00, 0x00, 0x02})
	// A negative length.
	f.Add([]byte{msgBind, 0x80, 0x00, 0x00, 0x00})
	// A Bind claiming 32767 parameters in nine bytes.
	f.Add(frame(msgBind, []byte{0, 0, 0, 0, 0x7f, 0xff}))
	// A parameter whose declared length exceeds the body.
	f.Add(frame(msgBind, []byte{0, 0, 0, 0, 0, 1, 0x7f, 0xff, 0xff, 0xff}))
	// A string field with no terminator.
	f.Add(frame(msgQuery, []byte("SELECT 1")))
	// An unknown tag.
	f.Add(frame(0x00, []byte{1, 2, 3}))
	// Two messages back to back, the second truncated.
	f.Add(append(frame(msgSync, nil), msgQuery, 0, 0, 0, 0x40))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := newReader(bytes.NewReader(data), 64)
		var m frontendMessage
		for {
			tag, body, err := r.next(maxMessageBody)
			if err != nil {
				if !acceptableReadError(err) {
					t.Fatalf("unexpected framing error %v for %q", err, data)
				}
				break
			}
			if err := decodeFrontend(&m, tag, body); err != nil {
				// A decode failure is a legitimate outcome and is where most
				// inputs land. What matters is that it is an error and not a
				// panic, and that the loop can continue: the framing was valid,
				// so the stream is still positioned at a message boundary.
				continue
			}
			// A successfully decoded message must be self-consistent, because
			// the handlers rely on it: the parameter and format arrays are the
			// lengths the message declared, and the counts are non-negative by
			// construction.
			checkDecoded(t, &m)
		}
		if cap(r.buf) > retainedBuffer {
			t.Fatalf("the reader retained a %d-byte buffer, past the %d-byte cap",
				cap(r.buf), retainedBuffer)
		}
		// Every array the decoder builds is introduced by a count the peer
		// chose, and every element of every one of them occupies at least two
		// bytes of the body. So a capacity past the input's own length means a
		// count was believed instead of checked.
		for name, size := range map[string]int{
			"parameters":             cap(m.params),
			"parameter format codes": cap(m.paramFormats),
			"result format codes":    cap(m.resultFormats),
			"parameter type OIDs":    cap(m.paramOIDs),
		} {
			if size > len(data) {
				t.Fatalf("the decoder reserved %d %s from a %d-byte input",
					size, name, len(data))
			}
		}
	})
}

// checkDecoded asserts the invariants a handler is entitled to assume.
func checkDecoded(t *testing.T, m *frontendMessage) {
	t.Helper()
	if len(m.name) > maxIdentifier || len(m.portal) > maxIdentifier {
		t.Fatalf("a decoded message carries an over-long name: %d/%d bytes",
			len(m.name), len(m.portal))
	}
	if m.maxRows < 0 {
		t.Fatalf("a decoded Execute carries a negative row limit %d", m.maxRows)
	}
	for _, f := range m.paramFormats {
		if f != formatText && f != formatBinary {
			t.Fatalf("a decoded Bind carries parameter format code %d", f)
		}
	}
	for _, f := range m.resultFormats {
		if f != formatText && f != formatBinary {
			t.Fatalf("a decoded Bind carries result format code %d", f)
		}
	}
	if m.tag == msgDescribe || m.tag == msgClose {
		if m.target != targetStatement && m.target != targetPortal {
			t.Fatalf("a decoded Describe/Close targets %q", string(rune(m.target)))
		}
	}
}

// acceptableReadError is the closed set of failures framing may report. Any
// other error means a code path produced something the session loop would not
// know how to classify.
func acceptableReadError(err error) bool {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case errors.Is(err, errBadLength), errors.Is(err, errMessageTooLarge):
		return true
	}
	return false
}

// frame wraps a body in the tag and length prefix the protocol uses.
func frame(tag byte, body []byte) []byte {
	out := make([]byte, 0, len(body)+5)
	out = append(out, tag)
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)+4))
	return append(out, body...)
}

// FuzzStartupPacket covers the one message that arrives before anything is
// known about the peer, and which is therefore bounded far more tightly than
// the rest. Its parameter list is a run of NUL-terminated strings with no
// count, so the loop that reads it is the one place a missing terminator could
// turn into an unbounded read.
func FuzzStartupPacket(f *testing.F) {
	startup := func(params ...string) []byte {
		body := binary.BigEndian.AppendUint32(nil, protocolVersion30)
		for _, p := range params {
			body = append(body, p...)
			body = append(body, 0)
		}
		body = append(body, 0)
		return append(binary.BigEndian.AppendUint32(nil, uint32(len(body)+4)), body...)
	}
	f.Add(startup("user", "alice", "database", "app"))
	f.Add(startup())
	f.Add(startup("user"))
	// SSL, GSS, and cancel request codes.
	for _, code := range []uint32{codeSSLRequest, codeGSSENCRequest, codeCancelRequest} {
		packet := binary.BigEndian.AppendUint32(nil, 8)
		f.Add(binary.BigEndian.AppendUint32(packet, code))
	}
	// A length past the startup ceiling, and one below the minimum.
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff})
	f.Add([]byte{0x00, 0x00, 0x00, 0x04})
	// A parameter list with no terminating NUL.
	f.Add(append(binary.BigEndian.AppendUint32(nil, 16),
		binary.BigEndian.AppendUint32(nil, protocolVersion30)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := newReader(bytes.NewReader(data), 64)
		code, body, err := r.startup()
		if err != nil {
			if !acceptableReadError(err) {
				t.Fatalf("unexpected startup framing error %v", err)
			}
			return
		}
		if code == protocolVersion30 {
			// Walk the parameter list the way negotiate does. It must terminate
			// on any input.
			fs := fields{b: body}
			for range maxStartupPacket {
				name := fs.cstring()
				if fs.bad() || name == "" {
					return
				}
				fs.cstring()
				if fs.bad() {
					return
				}
			}
			t.Fatal("the startup parameter walk did not terminate")
		}
	})
}

// FuzzSCRAMClientMessages keeps the authentication grammar on the same hostile
// input footing as the wire decoder. Both messages are peer-controlled text;
// success must imply the structural invariants authenticate relies on, while
// every other byte sequence must return an error rather than panic.
func FuzzSCRAMClientMessages(f *testing.F) {
	binding := "biws" // base64("n,,")
	f.Add("n,,n=,r=NONCE",
		"c="+binding+",r=NONCE,z=optional,p=AAAA")
	f.Add("n,a=administrator,n=,r=NONCE",
		"c="+binding+",r=NONCE,p=AAAA")
	f.Add("n,,n=,r=NONCE,r=OTHER",
		"c="+binding+",r=NONCE,p=AAAA,p=AAAA")
	f.Add("n,,m=required,n=,r=NONCE",
		"c="+binding+",r=NONCE,m=required,p=AAAA")

	f.Fuzz(func(t *testing.T, firstMessage, finalMessage string) {
		first, err := parseClientFirst(firstMessage)
		if err == nil {
			if first.nonce == "" || first.bare == "" || first.gs2Header == "" {
				t.Fatalf("successful client-first parse has empty required state: %+v", first)
			}
			if !validSCRAMNonce(first.nonce) {
				t.Fatalf("successful client-first parse has invalid nonce %q", first.nonce)
			}
		}

		proof, withoutProof, err := parseClientFinal(
			finalMessage, "NONCE", "n,,")
		if err == nil && (len(proof) == 0 || withoutProof == "") {
			t.Fatalf("successful client-final parse has empty proof/message: %d %q",
				len(proof), withoutProof)
		}
	})
}
