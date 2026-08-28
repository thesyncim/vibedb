//go:build darwin || linux

package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

const (
	authenticatedShardProcessEnable  = "VIBEDB_AUTH_TRANSPORT_PROCESS_E2E"
	authenticatedShardProcessHelper  = "VIBEDB_AUTH_TRANSPORT_PROCESS_HELPER"
	authenticatedShardProcessCert    = "VIBEDB_AUTH_TRANSPORT_CERT"
	authenticatedShardProcessKey     = "VIBEDB_AUTH_TRANSPORT_KEY"
	authenticatedShardProcessNext    = "VIBEDB_AUTH_TRANSPORT_NEXT_CERT"
	authenticatedShardProcessNextKey = "VIBEDB_AUTH_TRANSPORT_NEXT_KEY"
	authenticatedShardProcessRoots   = "VIBEDB_AUTH_TRANSPORT_ROOTS"
	authenticatedShardProcessOID     = "VIBEDB_AUTH_TRANSPORT_OID"
	authenticatedShardProcessGateway = "VIBEDB_AUTH_TRANSPORT_GATEWAY"
	authenticatedShardProcessUser    = "VIBEDB_AUTH_TRANSPORT_USER"
)

const authenticatedShardProcessOperations = 256

func TestAuthenticatedGatewayShardProcessPartitionRotationAndDeputyFaults(t *testing.T) {
	if os.Getenv(authenticatedShardProcessHelper) != "" {
		return
	}
	if os.Getenv(authenticatedShardProcessEnable) != "1" {
		t.Skip("set VIBEDB_AUTH_TRANSPORT_PROCESS_E2E=1 for the external authenticated transport gate")
	}
	root := t.TempDir()
	domain := gatewayPeerIdentity(73, 1).TrustDomain
	shard := gatewayPeerIdentity(73, 10).Node
	gatewayNode := gatewayPeerIdentity(73, 30).Node
	rogueGateway := gatewayPeerIdentity(73, 50).Node
	user := gatewayPeerIdentity(73, 70).Node
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	credentials, roots, err := writeShardProcessCredentials(root, oid, domain,
		[]rafttransport.NodeID{shard, gatewayNode, rogueGateway, user, shard})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	listenerFile, err := listener.File()
	if err != nil {
		t.Fatal(err)
	}
	var childError bytes.Buffer
	command := exec.Command(os.Args[0], "-test.run=^TestAuthenticatedGatewayShardProcessHelper$")
	command.ExtraFiles = []*os.File{listenerFile}
	command.Stderr = &childError
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Env = append(os.Environ(),
		authenticatedShardProcessHelper+"=1",
		authenticatedShardProcessCert+"="+credentials[0].Certificate,
		authenticatedShardProcessKey+"="+credentials[0].Key,
		authenticatedShardProcessNext+"="+credentials[4].Certificate,
		authenticatedShardProcessNextKey+"="+credentials[4].Key,
		authenticatedShardProcessRoots+"="+roots,
		authenticatedShardProcessOID+"="+oid.String(),
		authenticatedShardProcessGateway+"="+fmt.Sprintf("%x", gatewayNode),
		authenticatedShardProcessUser+"="+fmt.Sprintf("%x", user),
	)
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = listenerFile.Close()
	_ = listener.Close()
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	ready := make(chan error, 1)
	stdoutReader := bufio.NewReader(stdout)
	var childOutput bytes.Buffer
	go func() {
		line, readErr := stdoutReader.ReadString('\n')
		if readErr == nil && line != "ready\n" {
			readErr = fmt.Errorf("unexpected helper readiness %q", line)
		}
		ready <- readErr
		if readErr == nil {
			_, _ = io.Copy(&childOutput, stdoutReader)
		}
	}()
	select {
	case err = <-ready:
		if err != nil {
			t.Fatalf("helper readiness: %v: %s", err, childError.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("helper readiness timed out: %s", childError.String())
	}

	link := newAuthenticatedShardProcessLink(t, listener.Addr().String())
	defer link.close()
	deadline := servicetls.FixedDeadline(5 * time.Second)
	gatewayProfile := loadShardProcessProfile(t, credentials[1], roots, oid.String())
	client := newShardQualificationClient(t, gatewayProfile, link.address(), shard, deadline)
	defer client.Close()
	ctx := context.Background()
	connection, err := client.Dial(ctx, link.address())
	if err != nil {
		t.Fatal(err)
	}
	request := ownedReq("SELECT 1")
	request.Authority = serviceauthz.Authority{Node: user, Generation: 1}
	runAuthenticatedShardProcessRequests(t, ctx, connection, request, authenticatedShardProcessOperations)

	request.Authority.Node = rogueGateway
	response, roundErr := RoundTrip(ctx, connection, request)
	if !errors.Is(roundErr, ErrUnauthorized) || response != nil {
		t.Fatalf("trusted gateway confused-deputy response=%+v err=%v", response, roundErr)
	}
	request.Authority.Node = user

	link.partition()
	if _, roundErr = RoundTrip(ctx, connection, request); roundErr == nil {
		t.Fatal("directional partition retained established gateway-to-shard stream")
	}
	_ = connection.Close()
	if partitioned, dialErr := client.Dial(ctx, link.address()); dialErr == nil {
		_ = partitioned.Close()
		t.Fatal("directional partition admitted a replacement stream")
	}
	link.heal()
	connection, err = client.Dial(ctx, link.address())
	if err != nil {
		t.Fatal(err)
	}
	runAuthenticatedShardProcessRequests(t, ctx, connection, request, authenticatedShardProcessOperations)

	if err = command.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	rotationDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(rotationDeadline) {
		if _, roundErr = RoundTrip(ctx, connection, request); roundErr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if roundErr == nil {
		t.Fatal("certificate rotation did not revoke the old process stream")
	}
	_ = connection.Close()
	connection, err = client.Dial(ctx, link.address())
	if err != nil {
		t.Fatal(err)
	}
	runAuthenticatedShardProcessRequests(t, ctx, connection, request, authenticatedShardProcessOperations)
	_ = connection.Close()

	rogueProfile := loadShardProcessProfile(t, credentials[2], roots, oid.String())
	rogueClient := newShardQualificationClient(t, rogueProfile, link.address(), shard, deadline)
	rogueConnection, rogueErr := rogueClient.Dial(ctx, link.address())
	if rogueErr == nil {
		_ = rogueConnection.SetDeadline(time.Now().Add(time.Second))
		_, rogueErr = RoundTrip(ctx, rogueConnection, request)
		_ = rogueConnection.Close()
	}
	_ = rogueClient.Close()
	if rogueErr == nil {
		t.Fatal("rogue gateway process identity reached the shard protocol")
	}

	if err = command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err = command.Wait(); err != nil {
		t.Fatalf("helper exit: %v: stdout=%s stderr=%s", err, childOutput.String(), childError.String())
	}
}

func TestAuthenticatedGatewayShardProcessHelper(t *testing.T) {
	if os.Getenv(authenticatedShardProcessHelper) == "" {
		return
	}
	listener, err := net.FileListener(os.NewFile(3, "authenticated-shard-listener"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	profile := loadShardProcessProfile(t, shardProcessCredential{
		Certificate: os.Getenv(authenticatedShardProcessCert), Key: os.Getenv(authenticatedShardProcessKey),
	}, os.Getenv(authenticatedShardProcessRoots), os.Getenv(authenticatedShardProcessOID))
	nextProfile := loadShardProcessProfile(t, shardProcessCredential{
		Certificate: os.Getenv(authenticatedShardProcessNext), Key: os.Getenv(authenticatedShardProcessNextKey),
	}, os.Getenv(authenticatedShardProcessRoots), os.Getenv(authenticatedShardProcessOID))
	gatewayNode, err := servicetls.ParseNodeID(os.Getenv(authenticatedShardProcessGateway))
	if err != nil {
		t.Fatal(err)
	}
	user, err := servicetls.ParseNodeID(os.Getenv(authenticatedShardProcessUser))
	if err != nil {
		t.Fatal(err)
	}
	policy := newShardQualificationPolicy(t, 1, gatewayNode, user)
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := servicetls.NewNodeAuthorizer([]rafttransport.NodeID{gatewayNode})
	if err != nil {
		t.Fatal(err)
	}
	server, err := servicetls.NewServer(profile, rafttransport.TrafficShardSQL, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	shard := newShardServer(t)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()
	rotate := make(chan os.Signal, 1)
	signal.Notify(rotate, syscall.SIGHUP)
	defer signal.Stop(rotate)
	go func() {
		select {
		case <-rotate:
			if rotateErr := server.Rotate(nextProfile, authorizer); rotateErr != nil {
				t.Errorf("rotate TLS: %v", rotateErr)
				cancel()
			}
		case <-ctx.Done():
		}
	}()
	if _, err = fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	err = server.Serve(ctx, listener, servicetls.Limits{
		MaxConnections: 16, MaxHandshakes: 4,
		HandshakeDeadline: servicetls.FixedDeadline(5 * time.Second),
	}, func(_ context.Context, connection rafttransport.PeerConnection) {
		shard.ServeAuthorizedConn(connection, gate, nil)
	})
	if ctx.Err() == nil {
		t.Fatal(err)
	}
}

func loadShardProcessProfile(t testing.TB, credential shardProcessCredential,
	roots, oid string) *rafttransport.PeerTLS {
	t.Helper()
	profile, err := servicetls.LoadProfile(credential.Certificate, credential.Key, roots, oid, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

type shardProcessCredential struct{ Certificate, Key string }

func writeShardProcessCredentials(root string, oid asn1.ObjectIdentifier,
	domain rafttransport.TrustDomain, nodes []rafttransport.NodeID) ([]shardProcessCredential, string, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true,
		BasicConstraintsValid: true, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, "", err
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", err
	}
	roots := filepath.Join(root, "roots.pem")
	if err = os.WriteFile(roots, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return nil, "", err
	}
	credentials := make([]shardProcessCredential, len(nodes))
	for index, node := range nodes {
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			return nil, "", keyErr
		}
		extension, extensionErr := rafttransport.PeerIdentityExtension(oid,
			rafttransport.PeerIdentity{TrustDomain: domain, Node: node})
		if extensionErr != nil {
			return nil, "", extensionErr
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(index + 2)),
			Subject: pkix.Name{CommonName: "ignored"}, NotBefore: now.Add(-time.Hour),
			NotAfter: now.Add(2 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			ExtraExtensions: []pkix.Extension{extension}}
		leafDER, leafErr := x509.CreateCertificate(rand.Reader, leaf, caCertificate, &key.PublicKey, caKey)
		if leafErr != nil {
			return nil, "", leafErr
		}
		certificate := filepath.Join(root, fmt.Sprintf("credential-%d.pem", index))
		certificatePEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...)
		if writeErr := os.WriteFile(certificate, certificatePEM, 0o600); writeErr != nil {
			return nil, "", writeErr
		}
		keyDER, marshalErr := x509.MarshalECPrivateKey(key)
		if marshalErr != nil {
			return nil, "", marshalErr
		}
		keyPath := filepath.Join(root, fmt.Sprintf("credential-%d-key.pem", index))
		if writeErr := os.WriteFile(keyPath,
			pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); writeErr != nil {
			return nil, "", writeErr
		}
		credentials[index] = shardProcessCredential{Certificate: certificate, Key: keyPath}
	}
	return credentials, roots, nil
}

func runAuthenticatedShardProcessRequests(t testing.TB, ctx context.Context, connection net.Conn,
	request *shardservice.ShardRequest, operations int) {
	t.Helper()
	for operation := 0; operation < operations; operation++ {
		response, err := RoundTrip(ctx, connection, request)
		if err != nil || response.Kind != shardservice.ResponseRows {
			t.Fatalf("operation=%d response=%+v err=%v", operation, response, err)
		}
	}
}

type authenticatedShardProcessLink struct {
	listener *net.TCPListener
	target   string
	mu       sync.Mutex
	enabled  bool
	active   map[net.Conn]net.Conn
	wg       sync.WaitGroup
}

func newAuthenticatedShardProcessLink(t testing.TB, target string) *authenticatedShardProcessLink {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	link := &authenticatedShardProcessLink{listener: listener, target: target,
		enabled: true, active: make(map[net.Conn]net.Conn)}
	link.wg.Add(1)
	go link.accept()
	return link
}

func (link *authenticatedShardProcessLink) address() string { return link.listener.Addr().String() }

func (link *authenticatedShardProcessLink) accept() {
	defer link.wg.Done()
	for {
		front, err := link.listener.Accept()
		if err != nil {
			return
		}
		link.mu.Lock()
		enabled := link.enabled
		link.mu.Unlock()
		if !enabled {
			_ = front.Close()
			continue
		}
		back, err := net.DialTimeout("tcp", link.target, 5*time.Second)
		if err != nil {
			_ = front.Close()
			continue
		}
		link.mu.Lock()
		if !link.enabled {
			link.mu.Unlock()
			_ = front.Close()
			_ = back.Close()
			continue
		}
		link.active[front] = back
		link.mu.Unlock()
		link.wg.Add(1)
		go link.relay(front, back)
	}
}

func (link *authenticatedShardProcessLink) relay(front, back net.Conn) {
	defer link.wg.Done()
	var directions sync.WaitGroup
	directions.Add(2)
	go func() { defer directions.Done(); _, _ = io.Copy(front, back); _ = front.Close() }()
	go func() { defer directions.Done(); _, _ = io.Copy(back, front); _ = back.Close() }()
	directions.Wait()
	link.mu.Lock()
	delete(link.active, front)
	link.mu.Unlock()
}

func (link *authenticatedShardProcessLink) partition() {
	link.mu.Lock()
	link.enabled = false
	for front, back := range link.active {
		_ = front.Close()
		_ = back.Close()
		delete(link.active, front)
	}
	link.mu.Unlock()
}

func (link *authenticatedShardProcessLink) heal() {
	link.mu.Lock()
	link.enabled = true
	link.mu.Unlock()
}

func (link *authenticatedShardProcessLink) close() {
	_ = link.listener.Close()
	link.partition()
	link.wg.Wait()
}
