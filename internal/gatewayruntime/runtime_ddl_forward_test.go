package gatewayruntime

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type runtimeDDLFixture struct {
	owner       *Runtime
	participant *Runtime
	actor       serviceauthz.Authority
	profiles    []*rafttransport.PeerTLS
	policy      *serviceauthz.Policy
	served      chan error
}

func runtimeForwardDDLFixture(t *testing.T, run func(context.Context, serviceauthz.Authority, string) error) runtimeDDLFixture {
	t.Helper()
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{21}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{22}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{23}, Capabilities: serviceauthz.CapabilitySchema},
		{Node: rafttransport.NodeID{24}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{25}, Capabilities: serviceauthz.CapabilityDataRead},
	})
	owner := runtimeParticipantForTest(t, profiles[0], policy)
	owner.config.ControlParticipantOnly = false
	owner.clientTLS, _ = gateway.NewAuthorizedClientTLS(profiles[0], policy)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = owner.listener.Close()
	owner.listener = listener
	roster := []gatewayControlEndpoint{
		{Member: gateway.ClusterCatalogDrainMember{Node: profiles[0].LocalIdentity().Node, Incarnation: 1}},
		{Member: gateway.ClusterCatalogDrainMember{Node: profiles[1].LocalIdentity().Node, Incarnation: 1}},
	}
	owner.ddlForwardOwner = &gatewayDDLForwardOwner{capability: owner.clientTLS,
		roster: map[rafttransport.NodeID]struct{}{roster[0].Member.Node: {}, roster[1].Member.Node: {}}, run: run, admission: make(chan struct{}, 16)}
	participant := runtimeParticipantForTest(t, profiles[1], policy)
	participant.clientTLS, err = gateway.NewAuthorizedClientTLS(profiles[1], policy)
	if err != nil {
		t.Fatal(err)
	}
	participant.replicaControlManifest = &gatewayReplicaControlManifest{Gateways: roster}
	participant.config.PGListenAddress = "127.0.0.1:0"
	participant.config.DDLOwnerAddress, participant.config.DDLOwnerNode = listener.Addr().String(), profiles[0].LocalIdentity().Node
	if err := participant.openDDL(); err != nil {
		t.Fatal(err)
	}
	if participant.pgDDL == nil || participant.ddlForwardTLS == nil || participant.schemaDDL != nil || participant.ddlForwardOwner != nil {
		t.Fatal("participant did not retain exclusive owner forwarding")
	}
	served := make(chan error, 1)
	go func() { served <- owner.Serve(context.Background()) }()
	awaitRuntimeSignal(t, owner.Ready(), "authenticated DDL owner")
	return runtimeDDLFixture{owner: owner, participant: participant,
		actor: serviceauthz.Authority{Node: profiles[2].LocalIdentity().Node, Generation: 1}, profiles: profiles, policy: policy, served: served}
}

func TestRuntimeParticipantDDLForwardsAuthenticatedActorOnce(t *testing.T) {
	var calls atomic.Int32
	var failure atomic.Int32
	dispatched := make(chan string, 8)
	fixture := runtimeForwardDDLFixture(t, func(ctx context.Context, actor serviceauthz.Authority, text string) error {
		calls.Add(1)
		dispatched <- text
		fromContext, ok := serviceauthz.FromContext(ctx)
		if !ok || fromContext != actor || actor.Node != (rafttransport.NodeID{23}) || actor.Generation != 1 {
			t.Errorf("forwarded actor changed: %+v context=%+v", actor, fromContext)
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > gatewayDDLForwardTimeout {
			t.Error("DDL owner lost bounded deadline")
		}
		if failure.Load() == 1 {
			return sqldriver.ErrTableExists
		}
		if failure.Load() == 2 {
			return errors.Join(durable.ErrCommitOutcomeUnknown, sqldriver.ErrTableExists, context.Canceled)
		}
		return nil
	})
	for _, text := range []string{"CREATE TABLE things (id STRING PRIMARY KEY)", "ALTER TABLE things ADD COLUMN value INTEGER", "DROP TABLE things"} {
		if err := fixture.participant.pgDDL(t.Context(), fixture.actor, text); err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if got := <-dispatched; got != text {
			t.Fatalf("forwarded SQL changed: %q", got)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("DDL was dropped or repeated: %d", calls.Load())
	}
	failure.Store(1)
	err := fixture.participant.pgDDL(t.Context(), fixture.actor, "CREATE TABLE things (id STRING PRIMARY KEY)")
	var diagnostic interface{ SQLState() string }
	if !errors.As(err, &diagnostic) || diagnostic.SQLState() != "42P07" || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("lost deterministic SQLSTATE: %v", err)
	}
	failure.Store(2)
	err = fixture.participant.pgDDL(t.Context(), fixture.actor, "DROP TABLE things")
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.As(err, &diagnostic) || diagnostic.SQLState() != "40003" {
		t.Fatalf("owner unknown completion lost to deterministic sibling: %v", err)
	}
	for _, text := range []string{"SELECT 1", "DROP TABLE things; DROP TABLE other", "CREATE UNIQUE INDEX x ON things (id)", strings.Repeat("x", sqldriver.ReplicatedChildSchemaMaxBytes+1)} {
		if err := fixture.participant.pgDDL(t.Context(), fixture.actor, text); err == nil {
			t.Fatalf("unbounded/non-DDL accepted: %.50s", text)
		}
	}
	if calls.Load() != 5 {
		t.Fatal("refused grammar reached schema owner")
	}
	if err := fixture.participant.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.participant.ddlForwardTLS.Dial(t.Context(), fixture.owner.listener.Addr().String()); err == nil {
		t.Fatal("Close retained forwarding client")
	}
}

func TestRuntimeDDLOwnerChecksPeerActorGenerationAndEnvelope(t *testing.T) {
	var calls atomic.Int32
	fixture := runtimeForwardDDLFixture(t, func(context.Context, serviceauthz.Authority, string) error { calls.Add(1); return nil })
	base := gatewayDDLForwardRequest{Op: "ddl_forward", Actor: hex.EncodeToString(fixture.actor.Node[:]), Generation: 1,
		Deadline: time.Now().Add(time.Minute).UnixNano(), SQL: "DROP TABLE things"}
	for _, test := range []struct {
		name  string
		peer  int
		edit  func(*gatewayDDLForwardRequest)
		raw   func([]byte) []byte
		state string
	}{
		{name: "outside-roster", peer: 3, state: "42501"},
		{name: "no-delegation", peer: 2, state: "42501"},
		{name: "actor-denied", peer: 1, edit: func(r *gatewayDDLForwardRequest) {
			node := fixture.profiles[4].LocalIdentity().Node
			r.Actor = hex.EncodeToString(node[:])
		}, state: "42501"},
		{name: "stale-generation", peer: 1, edit: func(r *gatewayDDLForwardRequest) { r.Generation++ }, state: "42501"},
		{name: "expired", peer: 1, edit: func(r *gatewayDDLForwardRequest) { r.Deadline = 1 }, state: "57014"},
		{name: "noncanonical", peer: 1, raw: func(raw []byte) []byte { return append(raw, ' ') }, state: "08P01"},
		{name: "ddl-only", peer: 1, edit: func(r *gatewayDDLForwardRequest) { r.SQL = "SELECT 1" }, state: "0A000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := base
			if test.edit != nil {
				test.edit(&request)
			}
			raw, err := vibejson.Marshal(&request)
			if err != nil {
				t.Fatal(err)
			}
			if test.raw != nil {
				raw = test.raw(raw)
			}
			response := runtimeSendRawDDL(t, fixture, test.peer, raw)
			if response.Status != "error" || response.State != test.state {
				t.Fatalf("response=%+v", response)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("unauthorized request executed DDL")
	}
}

func runtimeSendRawDDL(t *testing.T, fixture runtimeDDLFixture, profileIndex int, raw []byte) gatewayDDLForwardResponse {
	t.Helper()
	address := fixture.owner.listener.Addr().String()
	client, err := servicetls.NewClient(servicetls.ClientOptions{TLS: fixture.profiles[profileIndex], Class: rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: fixture.profiles[0].LocalIdentity().Node}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: servicetls.FixedDeadline(time.Second), MaxConnections: 1, MaxHandshakes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	connection, err := client.Dial(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := connection.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(connection)
	if !scanner.Scan() {
		t.Fatalf("read DDL response: %v", scanner.Err())
	}
	var response gatewayDDLForwardResponse
	if err := vibejson.Unmarshal(scanner.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func TestRuntimeDDLForwardCancellationJoinsOwnerAndNeverRetries(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var calls atomic.Int32
	fixture := runtimeForwardDDLFixture(t, func(ctx context.Context, _ serviceauthz.Authority, _ string) error {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return errors.Join(durable.ErrCommitOutcomeUnknown, ctx.Err())
	})
	ctx, cancel := context.WithCancel(t.Context())
	forwarded := make(chan error, 1)
	go func() { forwarded <- fixture.participant.pgDDL(ctx, fixture.actor, "DROP TABLE things") }()
	awaitRuntimeSignal(t, entered, "schema owner execution")
	cancel()
	if err := awaitRuntimeError(t, forwarded, "canceled forwarding"); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("lost unknown completion: %v", err)
	}
	awaitRuntimeSignal(t, canceled, "owner peer cancellation")
	closed := make(chan error, 1)
	go func() { closed <- fixture.owner.Close() }()
	assertRuntimeWaiting(t, closed, "owner schema execution")
	close(release)
	if err := awaitRuntimeError(t, closed, "DDL owner Close"); err != nil {
		t.Fatal(err)
	}
	if err := awaitRuntimeError(t, fixture.served, "DDL owner Serve"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("unknown DDL was retried %d times", calls.Load())
	}
}

func TestRuntimeDDLForwardLostReplyRemainsUnknown(t *testing.T) {
	fixture := runtimeForwardDDLFixture(t, func(context.Context, serviceauthz.Authority, string) error { return nil })
	var dials atomic.Int32
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		dials.Add(1)
		connection, err := fixture.participant.ddlForwardTLS.Dial(ctx, address)
		if err != nil {
			return nil, err
		}
		return runtimeLostDDLReply{connection}, nil
	}
	err := forwardGatewayDDL(t.Context(), fixture.participant.clientTLS, dial,
		fixture.owner.listener.Addr().String(), fixture.actor, "DROP TABLE things")
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || dials.Load() != 1 {
		t.Fatalf("lost reply=%v dials=%d", err, dials.Load())
	}
}

type runtimeLostDDLReply struct{ net.Conn }

func (runtimeLostDDLReply) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
