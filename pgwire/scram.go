package pgwire

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Authentication: trust, and SCRAM-SHA-256 implemented from RFC 5802 and RFC
// 7677 against the standard library's primitives.
//
// SCRAM is here rather than md5 because md5 authentication is a
// challenge-response over an unsalted MD5 of the password and the username, and
// every part of that sentence is a reason not to ship it in something written
// in 2026. SCRAM-SHA-256 is what PostgreSQL 10 and later default to, it is what
// every current client library implements, and it needs nothing this module
// does not already have: crypto/hmac, crypto/sha256, crypto/subtle,
// crypto/rand, and — since Go 1.24 — crypto/pbkdf2, all standard library.
//
// # What is and is not implemented
//
// SCRAM-SHA-256 is implemented; SCRAM-SHA-256-PLUS is not, because channel
// binding binds the authentication to a TLS channel and this server does not
// speak TLS. That is not a gap to paper over: a client configured to *require*
// channel binding is told so, with a clear message, rather than being quietly
// downgraded. A client that merely supports it sends the gs2 flag "y", meaning
// "I support channel binding and you did not advertise it", and this server
// verifies that flag is echoed intact in the client's final message — which is
// the entire point of the flag, since an attacker stripping the -PLUS mechanism
// from the advertisement would otherwise go undetected.
//
// SASLprep (RFC 4013) is not implemented, and passwords that would need it are
// refused at [NewVerifier] rather than mis-normalized at authentication time. A
// wrong normalization is not a security hole but it is an interoperability trap
// of the worst kind — the password works from one client and fails from another
// — and implementing stringprep's tables correctly is a larger and more
// error-prone job than the rest of this file put together. An ASCII password
// with no control characters is its own SASLprep output, so the restriction
// costs nothing for the passwords anyone actually configures programmatically.
//
// # Not leaking which users exist
//
// An unknown user is authenticated against a verifier derived from a per-server
// secret and the username, so the exchange proceeds normally, takes the same
// work, and fails at the proof. Without that, a server that skipped straight to
// an error on an unknown user would be a user-enumeration oracle answering in
// microseconds. PostgreSQL does the same thing and calls it mock
// authentication.

// defaultIterations is the PBKDF2 work factor for a new verifier. RFC 7677
// specifies at least 4096 and PostgreSQL's default is the same; it is a server
// cost per authentication as well as an attacker cost per guess, which is why
// it is not simply set as high as it will go.
const defaultIterations = 4096

// saltLength is the verifier salt size in bytes. RFC 7677 requires at least 16.
const saltLength = 16

// nonceLength is the server nonce size in bytes before base64. RFC 5802 does
// not fix a length; this is the same order as PostgreSQL's.
const nonceLength = 18

// An Authenticator decides whether a connection may proceed. The set is closed
// — construct one with [Trust] or [SCRAM] — because a mechanism is a wire
// protocol, not a policy hook: adding one means adding message exchanges, and
// there is no way for code outside this package to do that.
type Authenticator interface {
	// validate rejects an authenticator that cannot run safely. The interface
	// is closed, so requiring this method makes every future mechanism state
	// its construction invariant before NewServer can admit it.
	validate() error
	// authenticate runs the mechanism's exchange for user. Returning nil means
	// the connection may proceed; the caller writes AuthenticationOk.
	authenticate(c authConn, user string) error
}

// An authConn is the part of a session a mechanism is allowed to touch: write
// backend messages, push them, and read the client's next authentication reply.
type authConn interface {
	out() *writer
	push() error
	// nextAuthReply reads the next frontend message, requiring it to be the
	// 'p' password/SASL message, and returns its uninterpreted body.
	nextAuthReply() ([]byte, error)
}

// Trust accepts every connection without asking for a credential.
//
// It is the right choice for a server bound to a loopback address or a unix
// socket inside a trust boundary, and the wrong choice for anything else. It is
// named rather than implied by a nil Authenticator so that a configuration that
// authenticates nobody is a thing someone wrote down.
func Trust() Authenticator { return trustAuth{} }

type trustAuth struct{}

func (trustAuth) validate() error                     { return nil }
func (trustAuth) authenticate(authConn, string) error { return nil }

// A Verifier is one user's stored SCRAM-SHA-256 credentials: a salt, an
// iteration count, and the two derived keys.
//
// It is deliberately not the password. A server holding verifiers cannot
// authenticate *as* its users to another SCRAM server, which is the property
// SCRAM's design exists to provide, and storing them is strictly better than
// storing what they were derived from. The zero Verifier is not usable.
type Verifier struct {
	salt       []byte
	iterations int
	storedKey  []byte
	serverKey  []byte
}

func (v Verifier) valid() bool {
	return len(v.salt) >= saltLength &&
		v.iterations >= defaultIterations &&
		len(v.storedKey) == sha256.Size &&
		len(v.serverKey) == sha256.Size
}

// NewVerifier derives a verifier from a cleartext password with a fresh random
// salt.
//
// It refuses a password that is not printable ASCII, because RFC 5802 requires
// the password to be SASLprep-normalized and this package does not implement
// SASLprep; see the file comment. The refusal is here, at configuration time,
// rather than at authentication time, so the failure is discovered by whoever
// can fix it.
func NewVerifier(password string) (Verifier, error) {
	if err := checkSASLprepSafe(password); err != nil {
		return Verifier{}, err
	}
	salt := make([]byte, saltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return Verifier{}, err
	}
	return deriveVerifier(password, salt, defaultIterations)
}

func deriveVerifier(password string, salt []byte, iterations int) (Verifier, error) {
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return Verifier{}, err
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	return Verifier{
		salt:       salt,
		iterations: iterations,
		storedKey:  stored[:],
		serverKey:  mac(salted, "Server Key"),
	}, nil
}

// checkSASLprepSafe reports whether password is its own SASLprep output.
//
// SASLprep maps some space characters, normalizes to NFKC, and prohibits
// several ranges. Every one of those transformations is the identity on
// printable ASCII, so that is the set this package accepts as safe without
// implementing the rest.
func checkSASLprepSafe(password string) error {
	if password == "" {
		return errors.New("pgwire: a SCRAM password may not be empty")
	}
	for i := range len(password) {
		if c := password[i]; c < 0x20 || c > 0x7e {
			return fmt.Errorf(
				"pgwire: this server's SCRAM implementation accepts only printable ASCII "+
					"passwords, because it does not implement SASLprep (RFC 4013) and will not "+
					"guess at a normalization; byte %d is %#x", i, c)
		}
	}
	return nil
}

// SCRAM authenticates with SCRAM-SHA-256, looking each user's verifier up with
// lookup. An unknown user is reported by returning false, and produces a
// mechanically identical exchange that fails at the proof.
func SCRAM(lookup func(user string) (Verifier, bool)) Authenticator {
	a := &scramAuth{lookup: lookup}
	if lookup == nil {
		// Keep construction side-effect free for an invalid configuration.
		// NewServer reports the actionable lookup error from validate.
		return a
	}
	secret := make([]byte, 32)
	// A failure here means the system entropy source is unavailable, which is
	// not a condition this package can proceed through. Preserve it for
	// NewServer to report rather than panicking in a configuration constructor;
	// a predictable fallback would be exactly the user-enumeration oracle the
	// mock exists to prevent.
	if _, err := io.ReadFull(rand.Reader, secret); err != nil {
		a.initErr = err
		return a
	}
	a.mockSecret = secret
	return a
}

type scramAuth struct {
	lookup     func(string) (Verifier, bool)
	mockSecret []byte
	initErr    error
}

func (a *scramAuth) validate() error {
	if a == nil || a.lookup == nil {
		return errors.New("pgwire: SCRAM requires a non-nil verifier lookup")
	}
	if a.initErr != nil {
		return fmt.Errorf("pgwire: cannot initialize SCRAM mock authentication: %w",
			a.initErr)
	}
	if len(a.mockSecret) != 32 {
		return errors.New("pgwire: SCRAM mock-authentication secret is not initialized")
	}
	return nil
}

// mockVerifier fabricates a stable verifier for an unknown user, so that the
// exchange an attacker sees is indistinguishable from one for a real user with
// the wrong password. It is stable per server and per username because a salt
// that changed between attempts would itself be the tell.
func (a *scramAuth) mockVerifier(user string) (Verifier, error) {
	salt := mac(a.mockSecret, "salt:"+user)[:saltLength]
	return deriveVerifier(
		string(mac(a.mockSecret, "password:"+user)), salt, defaultIterations,
	)
}

func (a *scramAuth) authenticate(c authConn, user string) error {
	if err := a.validate(); err != nil {
		return fatal(sqlstateInternalError, err.Error())
	}
	// Derive the per-user mock on every attempt, including a known user. An
	// unknown-only PBKDF2 would make the delay before AuthenticationSASL a
	// direct user-enumeration oracle: known users would use their precomputed
	// verifier while unknown users paid 4096 rounds here.
	mock, err := a.mockVerifier(user)
	if err != nil {
		return fatal(sqlstateInternalError,
			"pgwire: cannot derive a SCRAM mock verifier: "+err.Error())
	}
	verifier, known := a.lookup(user)
	if !known || !verifier.valid() {
		verifier = mock
		known = false
	}

	c.out().authenticationSASL("SCRAM-SHA-256")
	if err := c.push(); err != nil {
		return err
	}
	body, err := c.nextAuthReply()
	if err != nil {
		return err
	}
	first, err := parseSASLInitial(body)
	if err != nil {
		return err
	}

	serverNonce, err := newNonce()
	if err != nil {
		return err
	}
	combined := first.nonce + serverNonce
	serverFirst := "r=" + combined +
		",s=" + base64.StdEncoding.EncodeToString(verifier.salt) +
		",i=" + strconv.Itoa(verifier.iterations)
	c.out().authenticationSASLContinue([]byte(serverFirst))
	if err := c.push(); err != nil {
		return err
	}

	final, err := c.nextAuthReply()
	if err != nil {
		return err
	}
	proof, withoutProof, err := parseClientFinal(string(final), combined, first.gs2Header)
	if err != nil {
		return err
	}

	authMessage := first.bare + "," + serverFirst + "," + withoutProof
	clientSignature := mac(verifier.storedKey, authMessage)
	if len(proof) != len(clientSignature) {
		return errAuthFailed(user)
	}
	clientKey := make([]byte, len(proof))
	for i := range proof {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	candidate := sha256.Sum256(clientKey)
	if subtle.ConstantTimeCompare(candidate[:], verifier.storedKey) != 1 || !known {
		return errAuthFailed(user)
	}

	serverSignature := mac(verifier.serverKey, authMessage)
	c.out().authenticationSASLFinal(
		[]byte("v=" + base64.StdEncoding.EncodeToString(serverSignature)))
	return nil
}

func errAuthFailed(user string) error {
	return fatal(sqlstateInvalidPassword,
		fmt.Sprintf("password authentication failed for user %q", user))
}

// mac returns HMAC-SHA-256 of text under key.
func mac(key []byte, text string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(text))
	return h.Sum(nil)
}

func newNonce() (string, error) {
	b := make([]byte, nonceLength)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// clientFirst is the decoded SASLInitialResponse.
type clientFirst struct {
	// gs2Header is the channel-binding prefix, which the client must echo
	// base64-encoded in its final message.
	gs2Header string
	// bare is the message after the gs2 header, which is the first third of the
	// AuthMessage the proof is computed over.
	bare  string
	nonce string
}

// parseSASLInitial decodes a SASLInitialResponse: a mechanism name, a length,
// and the mechanism's own first message.
func parseSASLInitial(body []byte) (clientFirst, error) {
	f := fields{b: body}
	mechanism := f.cstring()
	size := f.int32()
	data := f.slice(int(size))
	if err := f.end(); err != nil {
		return clientFirst{}, protocolError(err)
	}
	if mechanism != "SCRAM-SHA-256" {
		if mechanism == "SCRAM-SHA-256-PLUS" {
			return clientFirst{}, fatal(sqlstateFeatureNotSupported,
				"SCRAM-SHA-256-PLUS is not available: this server does not speak TLS, so there "+
					"is no channel to bind to")
		}
		return clientFirst{}, fatal(sqlstateInvalidAuthorization,
			fmt.Sprintf("unsupported SASL mechanism %q; this server offers SCRAM-SHA-256",
				mechanism))
	}
	return parseClientFirst(string(data))
}

// parseClientFirst splits "gs2-header,client-first-bare".
//
// The gs2 header is one of "n,,", "y,,", "p=<name>,,", each optionally carrying
// an authzid in the second field. Only the first two are acceptable here: "p="
// demands channel binding, which this server has not advertised and cannot
// provide, and answering it with anything but a refusal would be pretending to
// bind a channel that does not exist.
func parseClientFirst(msg string) (clientFirst, error) {
	flag, rest, ok := strings.Cut(msg, ",")
	if !ok {
		return clientFirst{}, malformedSCRAM()
	}
	authzid, bare, ok := strings.Cut(rest, ",")
	if !ok {
		return clientFirst{}, malformedSCRAM()
	}
	switch {
	case flag == "n" || flag == "y":
	case strings.HasPrefix(flag, "p="):
		return clientFirst{}, fatal(sqlstateFeatureNotSupported,
			"the client requires SCRAM channel binding, which this server cannot provide "+
				"because it does not speak TLS")
	default:
		return clientFirst{}, malformedSCRAM()
	}
	if authzid != "" {
		if !strings.HasPrefix(authzid, "a=") || !validSCRAMName(authzid[2:], false) {
			return clientFirst{}, malformedSCRAM()
		}
		// Authentication identities come from the startup packet. Silently
		// accepting a distinct GS2 authorization identity would tell the client
		// that an authorization decision happened when this server has no such
		// policy layer.
		return clientFirst{}, fatal(sqlstateFeatureNotSupported,
			"SCRAM authorization identities are not supported")
	}
	first := clientFirst{gs2Header: flag + "," + authzid + ",", bare: bare}

	attrs := scramAttributeScanner{text: bare}
	name, username, err := attrs.next()
	if err != nil {
		return clientFirst{}, scramAttributeError(name, err)
	}
	if name == 'm' {
		return clientFirst{}, unsupportedSCRAMMandatoryExtension()
	}
	// PostgreSQL clients commonly leave n= empty because the protocol startup
	// packet already carries the authentication identity. A non-empty spelling
	// must nevertheless obey SCRAM's =2C/=3D name escaping.
	if name != 'n' || !validSCRAMName(username, true) {
		return clientFirst{}, malformedSCRAM()
	}
	name, nonce, err := attrs.next()
	if err != nil {
		return clientFirst{}, scramAttributeError(name, err)
	}
	if name == 'm' {
		return clientFirst{}, unsupportedSCRAMMandatoryExtension()
	}
	if name != 'r' || !validSCRAMNonce(nonce) {
		return clientFirst{}, malformedSCRAM()
	}
	first.nonce = nonce
	for {
		var value string
		name, value, err = attrs.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return clientFirst{}, scramAttributeError(name, err)
		}
		if name == 'm' {
			return clientFirst{}, unsupportedSCRAMMandatoryExtension()
		}
		if isDefinedSCRAMAttribute(name) || value == "" {
			return clientFirst{}, malformedSCRAM()
		}
	}
	return first, nil
}

// parseClientFinal decodes the client's final message and checks the two things
// it is the client's job to prove it did not lose: the combined nonce, and the
// gs2 header it started with.
//
// Verifying the echoed header is what makes the "y" flag mean anything. A
// downgrade attacker who removed SCRAM-SHA-256-PLUS from the mechanism list
// would leave the client sending "y" while the server believed it had never
// offered channel binding, and comparing the two is how the mismatch surfaces.
func parseClientFinal(msg, expectedNonce, gs2Header string) (proof []byte, withoutProof string, err error) {
	cut := strings.LastIndex(msg, ",p=")
	if cut < 0 || cut+3 == len(msg) {
		return nil, "", malformedSCRAM()
	}
	withoutProof = msg[:cut]

	attrs := scramAttributeScanner{text: withoutProof}
	name, binding, scanErr := attrs.next()
	if scanErr != nil {
		return nil, "", scramAttributeError(name, scanErr)
	}
	if name == 'm' {
		return nil, "", unsupportedSCRAMMandatoryExtension()
	}
	if name != 'c' || binding == "" {
		return nil, "", malformedSCRAM()
	}
	wantBinding := base64.StdEncoding.EncodeToString([]byte(gs2Header))
	if binding != wantBinding {
		return nil, "", fatal(sqlstateInvalidAuthorization,
			"the SCRAM channel-binding header the client echoed does not match the one "+
				"it sent, which is what a stripped-mechanism downgrade looks like")
	}

	name, nonce, scanErr := attrs.next()
	if scanErr != nil {
		return nil, "", scramAttributeError(name, scanErr)
	}
	if name == 'm' {
		return nil, "", unsupportedSCRAMMandatoryExtension()
	}
	if name != 'r' || !validSCRAMNonce(nonce) {
		return nil, "", malformedSCRAM()
	}
	// Compared in constant time because it is a value derived from a secret
	// exchange, and because there is no reason not to.
	if subtle.ConstantTimeCompare([]byte(nonce), []byte(expectedNonce)) != 1 {
		return nil, "", fatal(sqlstateInvalidAuthorization,
			"the SCRAM nonce the client returned is not the one this server issued")
	}

	for {
		var value string
		name, value, scanErr = attrs.next()
		if errors.Is(scanErr, io.EOF) {
			break
		}
		if scanErr != nil {
			return nil, "", scramAttributeError(name, scanErr)
		}
		switch name {
		case 'm':
			return nil, "", unsupportedSCRAMMandatoryExtension()
		case 'p':
			// LastIndex above locates the real final proof, so seeing another
			// proof here means the client sent it outside its designated slot.
			return nil, "", malformedSCRAM()
		default:
			// Every attribute defined by RFC 5802 has one designated position;
			// only as-yet unassigned names are optional extensions.
			if isDefinedSCRAMAttribute(name) || value == "" {
				return nil, "", malformedSCRAM()
			}
		}
	}

	proof, decodeErr := base64.StdEncoding.Strict().DecodeString(msg[cut+3:])
	if decodeErr != nil || len(proof) == 0 {
		return nil, "", malformedSCRAM()
	}
	return proof, withoutProof, nil
}

// scramAttributeScanner walks an RFC 5802 attribute list without materializing
// a []string. Attribute names are one ASCII letter, and a name may appear at
// most once in a message. The caller enforces message-specific order: n,r for
// client-first and c,r for client-final, with optional extensions afterwards.
type scramAttributeScanner struct {
	text string
	pos  int
	seen [256]bool
}

func (s *scramAttributeScanner) next() (name byte, value string, err error) {
	if s.pos == len(s.text) {
		return 0, "", io.EOF
	}
	start := s.pos
	end := len(s.text)
	if comma := strings.IndexByte(s.text[start:], ','); comma >= 0 {
		end = start + comma
		s.pos = end + 1
		if s.pos == len(s.text) {
			return 0, "", malformedSCRAM()
		}
	} else {
		s.pos = len(s.text)
	}
	attr := s.text[start:end]
	if len(attr) < 2 || attr[1] != '=' || !isSCRAMAttributeName(attr[0]) ||
		!utf8.ValidString(attr) || strings.IndexByte(attr[2:], 0) >= 0 {
		return 0, "", malformedSCRAM()
	}
	name = attr[0]
	if s.seen[name] {
		return name, "", malformedSCRAM()
	}
	s.seen[name] = true
	return name, attr[2:], nil
}

func isSCRAMAttributeName(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isDefinedSCRAMAttribute(c byte) bool {
	switch c {
	case 'a', 'c', 'e', 'i', 'm', 'n', 'p', 'r', 's', 'v':
		return true
	}
	return false
}

func validSCRAMNonce(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		if value[i] < 0x21 || value[i] > 0x7e || value[i] == ',' {
			return false
		}
	}
	return true
}

func validSCRAMName(value string, emptyOK bool) bool {
	if value == "" {
		return emptyOK
	}
	for i := 0; i < len(value); i++ {
		if value[i] != '=' {
			continue
		}
		if i+2 >= len(value) ||
			value[i+1:i+3] != "2C" && value[i+1:i+3] != "3D" {
			return false
		}
		i += 2
	}
	return true
}

func scramAttributeError(name byte, err error) error {
	if name == 'm' {
		return unsupportedSCRAMMandatoryExtension()
	}
	return err
}

func unsupportedSCRAMMandatoryExtension() error {
	return fatal(sqlstateFeatureNotSupported,
		"SCRAM mandatory extensions are not supported")
}

func malformedSCRAM() error {
	return fatal(sqlstateProtocolViolation, "malformed SCRAM message")
}

func protocolError(err error) error {
	return fatal(sqlstateProtocolViolation, err.Error())
}
