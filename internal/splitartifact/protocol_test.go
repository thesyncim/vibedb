package splitartifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testArtifact struct{ *bytes.Reader }

func (testArtifact) Close() error { return nil }

type testSource struct {
	identity Identity
	payload  []byte
	opens    atomic.Int64
}

func (source *testSource) OpenSplitArtifact(_ context.Context, identity Identity) (Artifact, error) {
	if identity != source.identity {
		return nil, ErrUnauthorized
	}
	source.opens.Add(1)
	return testArtifact{bytes.NewReader(source.payload)}, nil
}

type testConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

func (connection *testConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (*testConnection) PeerKeyDigest() [32]byte { return [32]byte{} }
func (*testConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficSnapshot
}

type retryOpener struct {
	service        *Service
	source, target rafttransport.PeerIdentity
	failFirst      bool
	opens          int
}

func (opener *retryOpener) OpenSnapshot(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	if node != opener.source.Node {
		return nil, rafttransport.ErrNodeNotFound
	}
	opener.opens++
	client, server := net.Pipe()
	go func() { _ = opener.service.Serve(ctx, &testConnection{Conn: server, identity: opener.target}) }()
	connection := net.Conn(client)
	if opener.failFirst && opener.opens == 1 {
		connection = &failReadConnection{Conn: client, remaining: ResponseBytes + 73}
	}
	return &testConnection{Conn: connection, identity: opener.source}, nil
}

type failReadConnection struct {
	net.Conn
	remaining int
}

func (connection *failReadConnection) Read(dst []byte) (int, error) {
	if connection.remaining == 0 {
		_ = connection.Conn.Close()
		return 0, io.ErrUnexpectedEOF
	}
	if len(dst) > connection.remaining {
		dst = dst[:connection.remaining]
	}
	n, err := connection.Conn.Read(dst)
	connection.remaining -= n
	return n, err
}

func TestCanonicalRequestRejectsTailAndReservedBytes(t *testing.T) {
	identity := Identity{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, Child: 3,
		ArtifactDigest: [32]byte{4}, ArtifactBytes: 999}
	value := request{Identity: identity, Offset: 17, ChunkBytes: MinChunkBytes}
	raw, err := appendRequest(nil, value)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openRequest(raw)
	if err != nil || opened != value {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	for _, index := range []int{8 + 65, 132} {
		forged := bytes.Clone(raw)
		forged[index] = 1
		if _, err = openRequest(forged); !errors.Is(err, ErrProtocol) {
			t.Fatalf("reserved byte %d accepted: %v", index, err)
		}
	}
	if _, err = openRequest(append(raw, 0)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
}

func TestStreamReconnectsAtUnverifiedChunkAndBoundsMemory(t *testing.T) {
	payload := bytes.Repeat([]byte("split-artifact-network"), 701)
	identity := Identity{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, Child: 1,
		ArtifactDigest: [32]byte{3}, ArtifactBytes: uint64(len(payload))}
	source := &testSource{identity: identity, payload: payload}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	target := rafttransport.PeerIdentity{Node: rafttransport.NodeID{22}}
	service, err := NewService(ServiceOptions{Source: source,
		Authorize:    func(peer rafttransport.PeerIdentity, got Identity) bool { return peer == target && got == identity },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 2,
		MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	sourcePeer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{11}}
	opener := &retryOpener{service: service, source: sourcePeer, target: target, failFirst: true}
	stream, err := OpenStream(context.Background(), StreamOptions{Opener: opener,
		SourceNode: sourcePeer.Node, Identity: identity, ReadDeadline: deadline, WriteDeadline: deadline,
		ChunkBytes: MinChunkBytes, MaxReconnects: 2, Workspace: make([]byte, MinChunkBytes)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil || closeErr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("bytes=%d err=%v close=%v", len(got), err, closeErr)
	}
	if opener.opens != 2 || source.opens.Load() != 2 {
		t.Fatalf("opens client=%d source=%d, want retry exactly once", opener.opens, source.opens.Load())
	}
}

func TestServiceRejectsUnauthorizedBeforeOpeningArtifact(t *testing.T) {
	identity := Identity{Operation: [32]byte{1}, PlanDigest: [32]byte{2},
		ArtifactDigest: [32]byte{3}, ArtifactBytes: 9}
	source := &testSource{identity: identity, payload: []byte("123456789")}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewService(ServiceOptions{Source: source,
		Authorize:    func(rafttransport.PeerIdentity, Identity) bool { return false },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- service.Serve(context.Background(), &testConnection{Conn: server}) }()
	raw, _ := appendRequest(nil, request{Identity: identity, ChunkBytes: MinChunkBytes})
	if _, err = client.Write(raw); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err = <-done; !errors.Is(err, ErrUnauthorized) || source.opens.Load() != 0 {
		t.Fatalf("err=%v source opens=%d", err, source.opens.Load())
	}
}
