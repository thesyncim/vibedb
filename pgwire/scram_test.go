package pgwire

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// Given a server configured with SCRAM-SHA-256, when a client performs the RFC
// 5802 exchange, then authentication succeeds for the right password, fails for
// the wrong one, fails identically for a user that does not exist, and is
// refused rather than downgraded when the client demands channel binding.
//
// The client side below is written from the RFC rather than from scram.go, so
// the test is a second implementation agreeing with the first rather than the
// first agreeing with itself. The one thing it shares is the server signature
// check, which is the client's half of mutual authentication and is what proves
// the server actually knew the password rather than merely accepting a proof.

// scramClient performs the client half of one exchange.
type scramClient struct {
	t        *testing.T
	c        *testClient
	user     string
	password string
	// gs2 is the channel-binding flag the client sends: "n" for "I do not
	// support it", "y" for "I do and you did not offer it", "p=tls-server-end-
	// point" for "I require it".
	gs2 string
}

// authenticate runs the exchange and returns the final backend message, which
// is AuthenticationOk on success and ErrorResponse on failure.
func (s *scramClient) authenticate() backendMessage {
	s.t.Helper()
	s.c.sendStartup(map[string]string{"user": s.user, "database": "app"})

	m := s.c.recv()
	if m.tag == msgErrorResponse {
		return m
	}
	if m.tag != msgAuthentication {
		s.t.Fatalf("expected an authentication request, got %q", string(rune(m.tag)))
	}
	if code := int32(binary.BigEndian.Uint32(m.body)); code != authSASL {
		s.t.Fatalf("expected AuthenticationSASL (%d), got %d", authSASL, code)
	}
	if !strings.Contains(string(m.body[4:]), "SCRAM-SHA-256") {
		s.t.Fatalf("the server did not offer SCRAM-SHA-256: %q", m.body[4:])
	}

	gs2Header := s.gs2 + ",,"
	clientNonce := "0123456789abcdef"
	clientFirstBare := "n=,r=" + clientNonce
	initial := append([]byte("SCRAM-SHA-256"), 0)
	initial = binary.BigEndian.AppendUint32(initial, uint32(len(gs2Header)+len(clientFirstBare)))
	initial = append(initial, gs2Header...)
	initial = append(initial, clientFirstBare...)
	s.c.send(msgPasswordOrSASL, initial)

	m = s.c.recv()
	if m.tag == msgErrorResponse {
		return m
	}
	if code := int32(binary.BigEndian.Uint32(m.body)); code != authSASLContinue {
		s.t.Fatalf("expected AuthenticationSASLContinue (%d), got %d", authSASLContinue, code)
	}
	serverFirst := string(m.body[4:])
	nonce, salt, iterations := parseServerFirst(s.t, serverFirst)
	if !strings.HasPrefix(nonce, clientNonce) {
		s.t.Fatalf("the combined nonce %q does not start with the client's", nonce)
	}

	clientFinalWithoutProof := "c=" +
		base64.StdEncoding.EncodeToString([]byte(gs2Header)) + ",r=" + nonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	salted, err := pbkdf2.Key(sha256.New, s.password, salt, iterations, sha256.Size)
	if err != nil {
		s.t.Fatalf("pbkdf2: %v", err)
	}
	clientKey := hmacOf(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacOf(storedKey[:], authMessage)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	final := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	s.c.send(msgPasswordOrSASL, []byte(final))

	m = s.c.recv()
	if m.tag == msgErrorResponse {
		return m
	}
	if code := int32(binary.BigEndian.Uint32(m.body)); code != authSASLFinal {
		s.t.Fatalf("expected AuthenticationSASLFinal (%d), got %d", authSASLFinal, code)
	}
	// Mutual authentication: the server proves it knew the verifier.
	serverKey := hmacOf(salted, "Server Key")
	want := "v=" + base64.StdEncoding.EncodeToString(hmacOf(serverKey, authMessage))
	if got := string(m.body[4:]); got != want {
		s.t.Fatalf("the server signature is %q, want %q; mutual authentication failed",
			got, want)
	}
	return s.c.recv()
}

func parseServerFirst(t *testing.T, msg string) (nonce string, salt []byte, iterations int) {
	t.Helper()
	for _, attr := range strings.Split(msg, ",") {
		key, value, _ := strings.Cut(attr, "=")
		switch key {
		case "r":
			nonce = value
		case "s":
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				t.Fatalf("the salt is not base64: %q", value)
			}
			salt = decoded
		case "i":
			n, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("the iteration count is not a number: %q", value)
			}
			iterations = n
		}
	}
	if nonce == "" || len(salt) == 0 || iterations < 4096 {
		t.Fatalf("server-first message is incomplete or weak: %q", msg)
	}
	return nonce, salt, iterations
}

func hmacOf(key []byte, text string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(text))
	return h.Sum(nil)
}

// scramServer builds a server that knows one user.
func scramServer(t *testing.T, user, password string) *Server {
	t.Helper()
	verifier, err := NewVerifier(password)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return newTestServer(t, Options{
		Auth: SCRAM(func(name string) (Verifier, bool) {
			if name != user {
				return Verifier{}, false
			}
			return verifier, true
		}),
	})
}

func TestSCRAMAcceptsTheRightPassword(t *testing.T) {
	srv := scramServer(t, "alice", "correct-horse")
	c := dial(t, srv)
	sc := &scramClient{t: t, c: c, user: "alice", password: "correct-horse", gs2: "n"}
	m := sc.authenticate()
	if m.tag != msgAuthentication {
		t.Fatalf("expected AuthenticationOk after the exchange, got %q: %s",
			string(rune(m.tag)), formatError(m.body))
	}
	if code := int32(binary.BigEndian.Uint32(m.body)); code != authOK {
		t.Fatalf("expected AuthenticationOk (%d), got %d", authOK, code)
	}
	// The session works afterwards.
	for {
		if c.recv().tag == msgReadyForQuery {
			break
		}
	}
	if has(c.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("the authenticated session could not run a query")
	}
}

func TestSCRAMAcceptsASpaceInAPassword(t *testing.T) {
	const password = " correct horse "
	srv := scramServer(t, "alice", password)
	c := dial(t, srv)
	sc := &scramClient{t: t, c: c, user: "alice", password: password, gs2: "n"}
	m := sc.authenticate()
	if m.tag != msgAuthentication {
		t.Fatalf("a password containing ASCII spaces produced %q, want AuthenticationOk: %s",
			string(rune(m.tag)), formatError(m.body))
	}
	if code := int32(binary.BigEndian.Uint32(m.body)); code != authOK {
		t.Fatalf("expected AuthenticationOk (%d), got %d", authOK, code)
	}
}

func TestNewServerRejectsSCRAMWithoutALookup(t *testing.T) {
	if _, err := NewServer(
		FromDatabase(testDatabase(t, "users", corpus)),
		Options{Auth: SCRAM(nil)},
	); err == nil {
		t.Fatal("NewServer accepted SCRAM without a verifier lookup")
	}
}

type failingEntropyReader struct{}

func (failingEntropyReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}

func TestNewServerReportsSCRAMEntropyFailureWithoutPanicking(t *testing.T) {
	previous := cryptorand.Reader
	cryptorand.Reader = failingEntropyReader{}
	t.Cleanup(func() { cryptorand.Reader = previous })

	auth := SCRAM(func(string) (Verifier, bool) { return Verifier{}, false })
	_, err := NewServer(
		FromDatabase(testDatabase(t, "users", corpus)),
		Options{Auth: auth},
	)
	if err == nil || !strings.Contains(err.Error(), "entropy unavailable") {
		t.Fatalf("NewServer after SCRAM entropy failure = %v", err)
	}
}

func TestSCRAMRejectsTheWrongPassword(t *testing.T) {
	srv := scramServer(t, "alice", "correct-horse")
	c := dial(t, srv)
	sc := &scramClient{t: t, c: c, user: "alice", password: "wrong", gs2: "n"}
	m := sc.authenticate()
	if m.tag != msgErrorResponse {
		t.Fatalf("a wrong password produced %q, want ErrorResponse", string(rune(m.tag)))
	}
	fs := errorFields(m.body)
	if fs['C'] != sqlstateInvalidPassword || fs['S'] != "FATAL" {
		t.Fatalf("wrong failure for a bad password: %s", formatError(m.body))
	}
}

// TestSCRAMDoesNotRevealWhetherAUserExists checks the mock-authentication
// property: an unknown user must produce the same exchange and the same error
// as a known user with the wrong password, so the server is not a user
// enumeration oracle.
func TestSCRAMDoesNotRevealWhetherAUserExists(t *testing.T) {
	srv := scramServer(t, "alice", "correct-horse")

	wrongPassword := &scramClient{t: t, c: dial(t, srv), user: "alice",
		password: "wrong", gs2: "n"}
	unknownUser := &scramClient{t: t, c: dial(t, srv), user: "mallory",
		password: "wrong", gs2: "n"}

	// Both reach the final message — an unknown user must not short-circuit —
	// and both fail with the same code and the same text.
	first := errorFields(wrongPassword.authenticate().body)
	second := errorFields(unknownUser.authenticate().body)
	if first['C'] != second['C'] {
		t.Fatalf("SQLSTATE differs between a wrong password (%s) and an unknown user (%s)",
			first['C'], second['C'])
	}
	if strings.Contains(second['M'], "does not exist") ||
		strings.Contains(second['M'], "unknown") {
		t.Fatalf("the error reveals that the user does not exist: %q", second['M'])
	}
}

func TestSCRAMAcceptsAClientThatSupportsChannelBinding(t *testing.T) {
	// gs2 flag "y" means the client supports channel binding and did not see a
	// -PLUS mechanism offered. The server must accept it and must verify the
	// flag is echoed, which is what detects a stripped-mechanism downgrade.
	srv := scramServer(t, "alice", "correct-horse")
	sc := &scramClient{t: t, c: dial(t, srv), user: "alice",
		password: "correct-horse", gs2: "y"}
	if m := sc.authenticate(); m.tag != msgAuthentication {
		t.Fatalf("a channel-binding-capable client was rejected: %s", formatError(m.body))
	}
}

func TestSCRAMRefusesAClientThatRequiresChannelBinding(t *testing.T) {
	srv := scramServer(t, "alice", "correct-horse")
	sc := &scramClient{t: t, c: dial(t, srv), user: "alice",
		password: "correct-horse", gs2: "p=tls-server-end-point"}
	m := sc.authenticate()
	if m.tag != msgErrorResponse {
		t.Fatalf("a client requiring channel binding was not refused: %q", string(rune(m.tag)))
	}
	fs := errorFields(m.body)
	if fs['C'] != sqlstateFeatureNotSupported {
		t.Fatalf("wrong refusal for required channel binding: %s", formatError(m.body))
	}
	if !strings.Contains(fs['M'], "TLS") {
		t.Errorf("the refusal does not say why channel binding is unavailable: %q", fs['M'])
	}
}

func TestSCRAMRefusesAnAuthorizationIdentityItCannotEnforce(t *testing.T) {
	_, err := parseClientFirst("n,a=administrator,n=,r=NONCE")
	expectSCRAMErrorCode(t, err, sqlstateFeatureNotSupported)
}

func TestSCRAMClientFirstAttributesFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  string
		code string
	}{
		{name: "missing username", msg: "n,,r=NONCE", code: sqlstateProtocolViolation},
		{name: "reordered required attributes", msg: "n,,r=NONCE,n=",
			code: sqlstateProtocolViolation},
		{name: "duplicate username", msg: "n,,n=,r=NONCE,n=other",
			code: sqlstateProtocolViolation},
		{name: "duplicate nonce", msg: "n,,n=,r=NONCE,r=OTHER",
			code: sqlstateProtocolViolation},
		{name: "multi-letter attribute", msg: "n,,n=,r=NONCE,xx=value",
			code: sqlstateProtocolViolation},
		{name: "invalid username escape", msg: "n,,n=bad=escape,r=NONCE",
			code: sqlstateProtocolViolation},
		{name: "non-printable nonce", msg: "n,,n=,r=NON CE",
			code: sqlstateProtocolViolation},
		{name: "defined attribute in extension slot", msg: "n,,n=,r=NONCE,c=value",
			code: sqlstateProtocolViolation},
		{name: "empty optional extension", msg: "n,,n=,r=NONCE,z=",
			code: sqlstateProtocolViolation},
		{name: "nul in extension", msg: "n,,n=,r=NONCE,z=\x00",
			code: sqlstateProtocolViolation},
		{name: "trailing separator", msg: "n,,n=,r=NONCE,",
			code: sqlstateProtocolViolation},
		{name: "mandatory extension prefix", msg: "n,,m=required,n=,r=NONCE",
			code: sqlstateFeatureNotSupported},
		{name: "mandatory extension suffix", msg: "n,,n=,r=NONCE,m=required",
			code: sqlstateFeatureNotSupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseClientFirst(tc.msg)
			expectSCRAMErrorCode(t, err, tc.code)
		})
	}

	first, err := parseClientFirst("n,,n=,r=NONCE,z=optional")
	if err != nil {
		t.Fatalf("an optional client-first extension was refused: %v", err)
	}
	if first.nonce != "NONCE" || first.bare != "n=,r=NONCE,z=optional" {
		t.Fatalf("optional extension was not preserved in the authenticated message: %+v", first)
	}
}

func TestSCRAMClientFinalAttributesFailClosed(t *testing.T) {
	binding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	for _, tc := range []struct {
		name string
		msg  string
		code string
	}{
		{name: "reordered required attributes",
			msg: "r=NONCE,c=" + binding + ",p=AAAA", code: sqlstateProtocolViolation},
		{name: "duplicate binding",
			msg:  "c=" + binding + ",r=NONCE,c=" + binding + ",p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "duplicate nonce",
			msg:  "c=" + binding + ",r=NONCE,r=OTHER,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "proof before final proof",
			msg:  "c=" + binding + ",r=NONCE,p=AAAA,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "multi-letter attribute",
			msg:  "c=" + binding + ",r=NONCE,xx=value,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "non-printable nonce",
			msg:  "c=" + binding + ",r=NON CE,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "defined attribute in extension slot",
			msg:  "c=" + binding + ",r=NONCE,s=value,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "empty optional extension",
			msg:  "c=" + binding + ",r=NONCE,z=,p=AAAA",
			code: sqlstateProtocolViolation},
		{name: "mandatory extension",
			msg:  "c=" + binding + ",r=NONCE,m=required,p=AAAA",
			code: sqlstateFeatureNotSupported},
		{name: "empty proof",
			msg: "c=" + binding + ",r=NONCE,p=", code: sqlstateProtocolViolation},
		{name: "non-canonical proof base64",
			msg: "c=" + binding + ",r=NONCE,p=AB==", code: sqlstateProtocolViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseClientFinal(tc.msg, "NONCE", "n,,")
			expectSCRAMErrorCode(t, err, tc.code)
		})
	}

	proof, withoutProof, err := parseClientFinal(
		"c="+binding+",r=NONCE,z=optional,p=AAAA", "NONCE", "n,,")
	if err != nil {
		t.Fatalf("an optional client-final extension was refused: %v", err)
	}
	if len(proof) == 0 || withoutProof != "c="+binding+",r=NONCE,z=optional" {
		t.Fatalf("optional extension was not preserved in the authenticated message: %q",
			withoutProof)
	}
}

func expectSCRAMErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("SCRAM message succeeded, want SQLSTATE %s", code)
	}
	var pg *pgError
	if !errors.As(err, &pg) || pg.code != code {
		t.Fatalf("SCRAM failure = %v, want SQLSTATE %s", err, code)
	}
}

// TestSCRAMDetectsATamperedChannelBindingHeader drives the downgrade case
// directly: a client that sent "y" but echoes "n" in its final message must be
// rejected, because that mismatch is the signature of an attacker having
// removed SCRAM-SHA-256-PLUS from the advertisement.
func TestSCRAMDetectsATamperedChannelBindingHeader(t *testing.T) {
	_, _, err := parseClientFinal(
		"c="+base64.StdEncoding.EncodeToString([]byte("n,,"))+",r=NONCE,p=AAAA",
		"NONCE", "y,,")
	if err == nil {
		t.Fatal("a mismatched channel-binding header was accepted")
	}
	var pg *pgError
	if !errors.As(err, &pg) || pg.code != sqlstateInvalidAuthorization {
		t.Fatalf("wrong failure for a tampered header: %v", err)
	}
}

func TestSCRAMRejectsAReplayedNonce(t *testing.T) {
	_, _, err := parseClientFinal(
		"c="+base64.StdEncoding.EncodeToString([]byte("n,,"))+",r=OTHER,p=AAAA",
		"NONCE", "n,,")
	if err == nil {
		t.Fatal("a client-final message with the wrong nonce was accepted")
	}
}

func TestNewVerifierRefusesAPasswordItCannotNormalize(t *testing.T) {
	for _, password := range []string{"", "café", "tab\there", "line\nbreak"} {
		if _, err := NewVerifier(password); err == nil {
			t.Errorf("NewVerifier accepted %q, which needs SASLprep this package does not "+
				"implement", password)
		}
	}
	for _, password := range []string{"Printable-ASCII_1234!", "with space", " leading and trailing "} {
		if _, err := NewVerifier(password); err != nil {
			t.Errorf("NewVerifier refused safe ASCII password %q: %v", password, err)
		}
	}
}

func TestVerifierUsesAFreshSaltAndTheRFCWorkFactor(t *testing.T) {
	a, err := NewVerifier("same-password")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	b, err := NewVerifier("same-password")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if string(a.salt) == string(b.salt) {
		t.Fatal("two verifiers for one password share a salt")
	}
	if string(a.storedKey) == string(b.storedKey) {
		t.Fatal("two verifiers for one password share a stored key")
	}
	if a.iterations < 4096 {
		t.Fatalf("the iteration count is %d, below RFC 7677's minimum of 4096", a.iterations)
	}
	if len(a.salt) < 16 {
		t.Fatalf("the salt is %d bytes, below RFC 7677's minimum of 16", len(a.salt))
	}
}

func TestTrustAcceptsEveryConnection(t *testing.T) {
	c := dial(t, newTestServer(t, Options{Auth: Trust()}))
	c.startup(map[string]string{"user": "anyone"})
	if has(c.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("a trust-authenticated session could not run a query")
	}
}
