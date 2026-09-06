package nodecontrol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
	"net"
	"testing"
	"time"
)

func TestPreparationSourceFrameRejectsUnboundedAndNoncanonicalHeaders(t *testing.T) {
	var wire bytes.Buffer
	nonce := [16]byte{1}
	if err := writePreparationSourceFrame(&wire, preparationSourceRequestMagic, nonce, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	got, n, err := readPreparationSourceFrame(bytes.NewReader(wire.Bytes()), preparationSourceRequestMagic, 7)
	if err != nil || n != nonce || string(got) != "payload" {
		t.Fatalf("roundtrip %q %x %v", got, n, err)
	}
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"oversize", func(b []byte) { binary.BigEndian.PutUint32(b[24:28], ^uint32(0)) }},
		{"zero", func(b []byte) { clear(b[24:28]) }},
		{"reserved", func(b []byte) { b[31] = 1 }},
		{"nonce", func(b []byte) { clear(b[8:24]) }},
		{"version", func(b []byte) { b[7]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := append([]byte(nil), wire.Bytes()[:32]...)
			test.mutate(raw)
			if _, _, err := readPreparationSourceFrame(bytes.NewReader(raw), preparationSourceRequestMagic, 7); !errors.Is(err, ErrBound) {
				t.Fatalf("invalid header reached body read: %v", err)
			}
		})
	}
}
func TestPreparationSourceServiceAuthorizesBeforeExport(t *testing.T) {
	intent := testIntent([]byte("payload"), gateway.EnrollmentReserved)
	domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
	for _, test := range []struct {
		name      string
		authorize bool
		domain    rafttransport.TrustDomain
		rawSuffix string
	}{
		{"authorized", true, domain, ""}, {"denied", false, domain, ""}, {"foreign cluster", true, rafttransport.TrustDomain{}, ""}, {"noncanonical", true, domain, " "},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			service, err := NewPreparationSourceService(func(_ context.Context, got gateway.GroupEnrollmentIntent, _ [3]PreparationMember) ([]byte, error) {
				calls++
				if got.Digest() != intent.Digest() {
					t.Error("intent changed")
				}
				return []byte("certified payload"), nil
			}, intent.Source.Node, func(rafttransport.PeerIdentity, gateway.GroupEnrollmentIntent) bool { return test.authorize }, deadline, deadline)
			if err != nil {
				t.Fatal(err)
			}
			left, right := net.Pipe()
			defer left.Close()
			_ = left.SetDeadline(deadline())
			done := make(chan error, 1)
			go func() {
				done <- service.Serve(t.Context(), &nodeInfoTestConn{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: test.domain, Node: rafttransport.NodeID{9}}, class: rafttransport.TrafficShardControl})
			}()
			raw, err := vibejson.Marshal(&PreparationSourceRequest{Intent: intent})
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, []byte(test.rawSuffix)...)
			if err = writePreparationSourceFrame(left, preparationSourceRequestMagic, [16]byte{1}, raw); err != nil {
				t.Fatal(err)
			}
			payload, nonce, replyErr := readPreparationSourceFrame(left, preparationSourceReplyMagic, MaxPayloadBytes)
			serveErr := <-done
			if test.name == "authorized" {
				if serveErr != nil || replyErr != nil || calls != 1 || string(payload) != "certified payload" || nonce != ([16]byte{1}) {
					t.Fatalf("export %q calls=%d errors=%v/%v", payload, calls, serveErr, replyErr)
				}
			} else if serveErr == nil || replyErr == nil || calls != 0 {
				t.Fatalf("unauthorized export calls=%d errors=%v/%v", calls, serveErr, replyErr)
			}
		})
	}
}

type preparationSourceOpenerFunc func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (f preparationSourceOpenerFunc) OpenShardControl(c context.Context, n rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	return f(c, n)
}
func TestPreparationSourceClientRejectsReplySubstitution(t *testing.T) {
	intent := testIntent([]byte("payload"), gateway.EnrollmentReserved)
	domain := rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}
	spec := PreparationSpec{Kind: PreparationSpecKind, Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard, AllocationGeneration: intent.AllocationGeneration, ReplicaOrdinal: intent.ReplicaOrdinal, SourceCommand: intent.ExpectedCommand, LogicalSchemaDigest: intent.ExpectedCommand.RelationManifestDigest, InitialVoters: [3]PreparationMember{{MemberID: 1, Node: intent.Source.Node, PeerAddress: string(intent.Source.Endpoint)}, {MemberID: 2, Node: rafttransport.NodeID{7}, PeerAddress: "peer2"}, {MemberID: 3, Node: rafttransport.NodeID{8}, PeerAddress: "peer3"}}, Target: PreparationMember{MemberID: intent.Target.Member, Node: intent.Target.Node, PeerAddress: string(intent.Target.Endpoint), NativeAddress: string(intent.Target.NativeEndpoint), ControlAddress: string(intent.Target.ControlEndpoint)}, TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID, Table: "orders", CreateTable: "CREATE TABLE orders (id TEXT PRIMARY KEY)", Apply: PreparationApplyProfile{MaxSessions: 16, RetryWindow: 8, MaxCollections: 1, MaxDocuments: 1, MaxBytes: 1024, ShardKey: "id"}, Log: PreparationLogProfile{MaxFileBytes: 4096, MaxRecordBytes: 1024, MaxRecords: 4, MaxEntries: 4, MaxLiveBytes: 4096}}
	payload, err := AppendPreparationSpec(nil, spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"valid", "nonce", "payload", "peer", "domain"} {
		t.Run(name, func(t *testing.T) {
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			done := make(chan struct{})
			opener := preparationSourceOpenerFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
				left, right := net.Pipe()
				identity := rafttransport.PeerIdentity{TrustDomain: domain, Node: node}
				if name == "peer" {
					identity.Node[0]++
				}
				if name == "domain" {
					identity.TrustDomain.ClusterID[0]++
				}
				go func() {
					defer close(done)
					defer right.Close()
					_ = right.SetDeadline(deadline())
					_, nonce, err := readPreparationSourceFrame(right, preparationSourceRequestMagic, maxPreparationSourceRequestBytes)
					if err != nil {
						return
					}
					body := payload
					if name == "nonce" {
						nonce[0] ^= 1
					}
					if name == "payload" {
						body = []byte("garbage")
					}
					_ = writePreparationSourceFrame(right, preparationSourceReplyMagic, nonce, body)
				}()
				return &nodeInfoTestConn{Conn: left, identity: identity, class: rafttransport.TrafficShardControl}, nil
			})
			client, err := NewPreparationSourceClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			got, err := client.Read(t.Context(), intent, spec.InitialVoters)
			<-done
			if name == "valid" {
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("valid reply: %v", err)
				}
			} else if err == nil {
				t.Fatal("substituted reply accepted")
			}
		})
	}
}
