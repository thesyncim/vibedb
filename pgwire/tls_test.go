package pgwire

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestTLSNegotiationCarriesACompleteSession(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	srv := newTestServer(t, Options{TLSConfig: serverTLS})
	c := dialTLS(t, srv, clientTLS)
	c.startup(map[string]string{"user": "tester", "database": "app"})

	state := c.conn.(*tls.Conn).ConnectionState()
	if !state.HandshakeComplete || state.Version < tls.VersionTLS12 {
		t.Fatalf("TLS state = %+v, want a completed TLS 1.2+ handshake", state)
	}
	if tag := commandTagOf(t, c.query(`SELECT 1 AS one`)); tag != "SELECT 1" {
		t.Fatalf("query over TLS returned command tag %q, want SELECT 1", tag)
	}
}

func TestTLSAndSCRAMAuthenticateTogether(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	verifier, err := NewVerifier("correct-horse")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	srv := newTestServer(t, Options{
		TLSConfig:  serverTLS,
		RequireTLS: true,
		Auth: SCRAM(func(user string) (Verifier, bool) {
			return verifier, user == "alice"
		}),
	})
	c := dialTLS(t, srv, clientTLS)
	scram := &scramClient{
		t: t, c: c, user: "alice", password: "correct-horse", gs2: "y",
	}
	m := scram.authenticate()
	if m.tag != msgAuthentication ||
		int32(binary.BigEndian.Uint32(m.body)) != authOK {
		t.Fatalf("TLS+SCRAM authentication returned %q: %s",
			string(rune(m.tag)), formatError(m.body))
	}
	for c.recv().tag != msgReadyForQuery {
	}
	if has(c.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("TLS+SCRAM session could not execute a query")
	}
}

func TestTLSIsOptionalUnlessExplicitlyRequired(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)

	t.Run("configured TLS keeps an intentional plaintext path", func(t *testing.T) {
		c := dial(t, newTestServer(t, Options{TLSConfig: serverTLS}))
		c.startup(map[string]string{"user": "tester"})
	})

	t.Run("required TLS rejects plaintext and accepts TLS", func(t *testing.T) {
		srv := newTestServer(t, Options{TLSConfig: serverTLS, RequireTLS: true})
		plain := dial(t, srv)
		plain.sendStartup(map[string]string{"user": "tester"})
		m := plain.recv()
		if fs := errorFields(m.body); m.tag != msgErrorResponse ||
			fs['C'] != sqlstateInvalidAuthorization || fs['S'] != "FATAL" {
			t.Fatalf("plaintext startup refusal = %q %s, want fatal 28000",
				string(rune(m.tag)), formatError(m.body))
		}

		encrypted := dialTLS(t, srv, clientTLS)
		encrypted.startup(map[string]string{"user": "tester"})
	})
}

func TestTLSConfigIsValidatedAndCloned(t *testing.T) {
	database := testDatabase(t, "users", corpus)
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{name: "required without config", opts: Options{Auth: Trust(), RequireTLS: true}},
		{name: "config without certificate", opts: Options{
			Auth: Trust(), TLSConfig: &tls.Config{},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if server, err := NewServer(database, tc.opts); err == nil || server != nil {
				t.Fatalf("NewServer = (%v, %v), want (nil, error)", server, err)
			}
		})
	}

	serverTLS, clientTLS := testTLSConfigs(t)
	srv := newTestServer(t, Options{TLSConfig: serverTLS})
	serverTLS.Certificates = nil
	c := dialTLS(t, srv, clientTLS)
	c.startup(map[string]string{"user": "tester"})
}

func TestTLSHandshakeIsBoundedByReadTimeout(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	reported := make(chan error, 1)
	srv := newTestServer(t, Options{
		TLSConfig:   serverTLS,
		ReadTimeout: 25 * time.Millisecond,
		OnError: func(err error) {
			reported <- err
		},
	})
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeConn(server)
	}()
	t.Cleanup(func() { _ = client.Close() })

	writeSSLRequest(t, client)
	var response [1]byte
	if _, err := io.ReadFull(client, response[:]); err != nil {
		t.Fatalf("read SSLRequest response: %v", err)
	}
	if response[0] != 'S' {
		t.Fatalf("SSLRequest response = %q, want S", response[0])
	}

	select {
	case err := <-reported:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("stalled TLS handshake reported %v, want a timeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stalled TLS handshake did not reach ReadTimeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session did not exit after its TLS handshake timed out")
	}
}

func TestTLSRequestCannotPipelinePlaintextAcrossTheUpgrade(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	c := dial(t, newTestServer(t, Options{TLSConfig: serverTLS}))
	packet := sslRequestPacket()
	packet = append(packet, 0x16) // first byte of a TLS handshake record
	c.sendRaw(packet)
	expectStartupProtocolViolation(t, c)
}

func TestEncryptionNegotiationCannotRestartInsideTLS(t *testing.T) {
	for _, code := range []int32{codeSSLRequest, codeGSSENCRequest} {
		name := "SSL"
		if code == codeGSSENCRequest {
			name = "GSS"
		}
		t.Run(name, func(t *testing.T) {
			serverTLS, clientTLS := testTLSConfigs(t)
			c := dialTLS(t, newTestServer(t, Options{TLSConfig: serverTLS}), clientTLS)
			sendEncryptionRequest(c, code, nil)
			expectStartupProtocolViolation(t, c)
		})
	}
}

func TestRequireTLSAppliesToCancelRequests(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	srv := newTestServer(t, Options{TLSConfig: serverTLS, RequireTLS: true})
	target := dialTLS(t, srv, clientTLS)
	target.startup(map[string]string{"user": "tester"})
	packet := cancelRequestPacket(target.pid, target.secret)

	plaintext := dial(t, srv)
	if err := plaintext.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set plaintext cancel read deadline: %v", err)
	}
	plaintext.sendRaw(packet)
	plaintext.drainWrites()
	if _, err := plaintext.br.ReadByte(); err == nil {
		t.Fatal("plaintext CancelRequest received a reply instead of a silent rejection")
	}

	encrypted := dialTLS(t, srv, clientTLS)
	if err := encrypted.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set encrypted cancel read deadline: %v", err)
	}
	encrypted.sendRaw(packet)
	encrypted.drainWrites()
	if _, err := encrypted.br.ReadByte(); err == nil {
		t.Fatal("encrypted CancelRequest received a reply")
	}
	// The target was idle, so PostgreSQL cancellation semantics ignore the
	// valid key instead of arming the next statement.
	if has(target.query(`SELECT 1`), msgErrorResponse) {
		t.Fatal("an idle TLS CancelRequest poisoned the target's next statement")
	}
}

func dialTLS(t *testing.T, srv *Server, config *tls.Config) *testClient {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeConn(server)
	}()

	writeSSLRequest(t, client)
	var response [1]byte
	if _, err := io.ReadFull(client, response[:]); err != nil {
		t.Fatalf("read SSLRequest response: %v", err)
	}
	if response[0] != 'S' {
		t.Fatalf("SSLRequest response = %q, want S", response[0])
	}
	tlsConn := tls.Client(client, config)
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	_ = tlsConn.SetDeadline(time.Time{})
	c := newTestClient(t, tlsConn)
	t.Cleanup(func() {
		close(c.outbox)
		_ = tlsConn.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("TLS session did not exit after its client closed")
		}
	})
	return c
}

func writeSSLRequest(t *testing.T, conn net.Conn) {
	t.Helper()
	packet := sslRequestPacket()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set SSLRequest write deadline: %v", err)
	}
	if _, err := conn.Write(packet); err != nil {
		t.Fatalf("write SSLRequest: %v", err)
	}
}

func sslRequestPacket() []byte {
	packet := make([]byte, 8)
	binary.BigEndian.PutUint32(packet, 8)
	binary.BigEndian.PutUint32(packet[4:], codeSSLRequest)
	return packet
}

func cancelRequestPacket(pid, secret int32) []byte {
	packet := binary.BigEndian.AppendUint32(nil, 16)
	packet = binary.BigEndian.AppendUint32(packet, codeCancelRequest)
	packet = binary.BigEndian.AppendUint32(packet, uint32(pid))
	return binary.BigEndian.AppendUint32(packet, uint32(secret))
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate TLS key: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create TLS certificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse TLS certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	}, &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
}
