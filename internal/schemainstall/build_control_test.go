package schemainstall

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type buildTestFunc func(context.Context, BuildRequest, string) (sqldriver.ReplicatedSchemaDDLTarget, error)

func (f buildTestFunc) BuildSchema(ctx context.Context, r BuildRequest, sql string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	return f(ctx, r, sql)
}

type shadowBuildTestFunc func(context.Context, BuildRequest, string) (bool, error)

func (f shadowBuildTestFunc) BuildSchema(context.Context, BuildRequest, string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	return sqldriver.ReplicatedSchemaDDLTarget{}, ErrInvalid
}

func (f shadowBuildTestFunc) BuildSchemaShadow(ctx context.Context, request BuildRequest, sql string) (bool, error) {
	return f(ctx, request, sql)
}

type buildTestOpener func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (f buildTestOpener) OpenShardControl(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	return f(ctx, node)
}

func buildFixture() (BuildRequest, string, rafttransport.PeerIdentity) {
	r, _, _, _ := schemaFixture(71)
	const sql = "DROP INDEX IF EXISTS absent"
	request := BuildRequest{Operation: r.Operation, Group: r.Group, AllocationGeneration: r.AllocationGeneration,
		FromSchemaGeneration: r.FromSchemaGeneration, FromRelationManifestDigest: r.FromRelationManifestDigest,
		SourceApplied: 7, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	peer := rafttransport.PeerIdentity{Node: [16]byte{9}, TrustDomain: rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}}
	return request, sql, peer
}

func TestSchemaBuildCodecRejectsTruncationAndBounds(t *testing.T) {
	r, _, _ := buildFixture()
	raw, err := AppendBuildRequest(nil, r)
	if err != nil || len(raw) != buildRequestBytes {
		t.Fatalf("encode: %d %v", len(raw), err)
	}
	decoded, err := ReadBuildRequest(bytes.NewReader(raw))
	if err != nil || decoded != r {
		t.Fatalf("round trip: %+v %v", decoded, err)
	}
	for n := range len(raw) {
		if _, err := ReadBuildRequest(bytes.NewReader(raw[:n])); err == nil {
			t.Fatalf("accepted prefix=%d", n)
		}
	}
	for _, n := range []uint64{0, sqldriver.ReplicatedChildSchemaMaxBytes + 1, ^uint64(0)} {
		bad := bytes.Clone(raw)
		binary.LittleEndian.PutUint64(bad[168:176], n)
		if _, err := ReadBuildRequest(bytes.NewReader(bad)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("unbounded SQL %d: %v", n, err)
		}
	}
	digest, _ := BuildRequestDigest(r)
	r.SourceApplied++
	changed, _ := BuildRequestDigest(r)
	if changed == digest {
		t.Fatal("source cut not bound to identity")
	}
	var header [buildResponseBytes]byte
	copy(header[:8], buildResponseMagic[:])
	copy(header[16:48], changed[:])
	binary.LittleEndian.PutUint64(header[48:56], maxBuildReceiptBytes+1)
	if _, err := readBuildResponse(bytes.NewReader(header[:]), r); !errors.Is(err, ErrBound) {
		t.Fatalf("unbounded receipt: %v", err)
	}
	copy(header[16:48], digest[:])
	if _, err := readBuildResponse(bytes.NewReader(header[:]), r); !errors.Is(err, ErrInvalid) {
		t.Fatalf("substituted request: %v", err)
	}
}

func TestSchemaBuildAuthenticatedClientAndAdmission(t *testing.T) {
	for _, mode := range []string{"success", "denied", "wrong-domain", "wrong-node", "capacity", "lost-receipt"} {
		t.Run(mode, func(t *testing.T) {
			r, sql, peer := buildFixture()
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			calls := 0
			var closeServer func() error
			s, err := NewBuildControlService(BuildControlOptions{
				Builder: buildTestFunc(func(_ context.Context, got BuildRequest, text string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
					calls++
					if got != r || text != sql {
						return sqldriver.ReplicatedSchemaDDLTarget{}, ErrConflict
					}
					if mode == "lost-receipt" {
						_ = closeServer()
					}
					return sqldriver.ReplicatedSchemaDDLTarget{NoOp: true}, nil
				}),
				Authorize:    func(rafttransport.PeerIdentity, BuildRequest) bool { return mode != "denied" },
				ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, BuildTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "capacity" {
				s.admitted <- struct{}{}
			}
			done := make(chan error, 1)
			client, err := NewClient(ClientOptions{ReadDeadline: deadline, WriteDeadline: deadline,
				Opener: buildTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
					local, remote := net.Pipe()
					closeServer = remote.Close
					serverPeer, clientPeer := peer, peer
					if mode == "wrong-domain" {
						serverPeer.TrustDomain.ClusterID[0]++
					}
					if mode == "wrong-node" {
						clientPeer.Node[0]++
					}
					go func() { done <- s.Serve(t.Context(), &schemaPeerConnection{Conn: remote, identity: serverPeer}) }()
					return &schemaPeerConnection{Conn: local, identity: clientPeer}, nil
				})})
			if err != nil {
				t.Fatal(err)
			}
			target, err := client.Build(t.Context(), peer.Node, r, sql)
			serveErr := <-done // also synchronizes the builder's writes to calls.
			switch mode {
			case "success":
				if err != nil || serveErr != nil || !target.NoOp || calls != 1 {
					t.Fatalf("build: %+v %v / %v calls=%d", target, err, serveErr, calls)
				}
			case "lost-receipt":
				if !errors.Is(err, ErrOutcomeUnknown) || calls != 1 {
					t.Fatalf("lost receipt: %v calls=%d", err, calls)
				}
			case "capacity":
				if !errors.Is(err, ErrBound) || calls != 0 {
					t.Fatalf("capacity: %v calls=%d", err, calls)
				}
			default:
				if !errors.Is(err, rafttransport.ErrUnauthorized) || calls != 0 {
					t.Fatalf("authority: %v calls=%d", err, calls)
				}
			}
		})
	}
}

func TestSchemaShadowBuildAuthenticatedClient(t *testing.T) {
	request, sql, peer := buildFixture()
	request.Shadow, request.SourceApplied = true, 0
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewBuildControlService(BuildControlOptions{
		Builder: shadowBuildTestFunc(func(_ context.Context, got BuildRequest, text string) (bool, error) {
			if got != request || text != sql {
				return false, ErrConflict
			}
			return true, nil
		}),
		Authorize: func(identity rafttransport.PeerIdentity, got BuildRequest) bool {
			return identity == peer && got == request
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, BuildTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	client, err := NewClient(ClientOptions{ReadDeadline: deadline, WriteDeadline: deadline,
		Opener: buildTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			local, remote := net.Pipe()
			go func() { done <- service.Serve(t.Context(), &schemaPeerConnection{Conn: remote, identity: peer}) }()
			return &schemaPeerConnection{Conn: local, identity: peer}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	noOp, err := client.BuildShadow(t.Context(), peer.Node, request, sql)
	if serveErr := <-done; err != nil || serveErr != nil || !noOp {
		t.Fatalf("shadow no-op=%t err=%v serve=%v", noOp, err, serveErr)
	}
}

func TestSchemaBuildSQLDigestAndExecutionDeadline(t *testing.T) {
	for _, badSQL := range []bool{true, false} {
		r, sql, peer := buildFixture()
		deadline := func() time.Time { return time.Now().Add(time.Second) }
		calls := 0
		s, err := NewBuildControlService(BuildControlOptions{
			Builder: buildTestFunc(func(ctx context.Context, _ BuildRequest, _ string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
				calls++
				<-ctx.Done()
				return sqldriver.ReplicatedSchemaDDLTarget{}, ctx.Err()
			}), Authorize: func(rafttransport.PeerIdentity, BuildRequest) bool { return true },
			ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, BuildTimeout: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- s.Serve(t.Context(), &schemaPeerConnection{Conn: server, identity: peer}) }()
		raw, _ := AppendBuildRequest(nil, r)
		if _, err := client.Write(raw); err != nil {
			t.Fatal(err)
		}
		if n, err := readBuildResponseHeader(client, r); err != nil || n != 0 {
			t.Fatalf("admission: %d %v", n, err)
		}
		payload := []byte(sql)
		if badSQL {
			payload[0]++
		}
		if _, err := client.Write(payload); err != nil {
			t.Fatal(err)
		}
		_, responseErr := readBuildResponse(client, r)
		_ = client.Close()
		serveErr := <-done
		if badSQL {
			if !errors.Is(responseErr, ErrInvalid) || calls != 0 {
				t.Fatalf("SQL substitution: %v calls=%d", responseErr, calls)
			}
		} else if responseErr == nil || !errors.Is(serveErr, context.DeadlineExceeded) || calls != 1 {
			t.Fatalf("unbounded execution: %v / %v calls=%d", responseErr, serveErr, calls)
		}
	}
}
