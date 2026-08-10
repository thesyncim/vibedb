package gateway

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// testOwnership is the static identity the in-process shard server adopts; every
// owned request must carry these coordinates to be admitted.
func testOwnership() shardservice.Ownership {
	return shardservice.Ownership{Distribution: "tenant_data", Shard: "-80", AllocationGeneration: 1, Epoch: 7, RoutingVersion: 3}
}

// ownedReq builds a request that admits against testOwnership.
func ownedReq(sql string, params ...shardservice.Param) *shardservice.ShardRequest {
	own := testOwnership()
	return &shardservice.ShardRequest{
		SQL:                  sql,
		Params:               params,
		Distribution:         own.Distribution,
		Shard:                own.Shard,
		AllocationGeneration: own.AllocationGeneration,
		RoutingVersion:       own.RoutingVersion,
		OwnershipEpoch:       own.Epoch,
		ExecutionMode:        shardservice.ExecutionReadWrite,
	}
}

// newShardServer builds a real in-process shard server over a fresh durable
// catalog. The gateway client speaks to it over net.Pipe, exercising the real
// wire codec end to end without binding a port.
func newShardServer(t *testing.T) *shardservice.Server {
	t.Helper()
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), "shard.vdb"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srv, err := shardservice.NewServer(db, testOwnership(), shardservice.Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// pipeClient returns a client whose dial function serves each round-trip on a
// fresh in-process connection to srv.
func pipeClient(srv *shardservice.Server) *Client {
	return NewClient(func(_ context.Context, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go srv.ServeConn(server)
		return client, nil
	})
}

// TestClientRoundTrip drives DDL, DML, and a SELECT through the client against a
// real shard server, proving one synchronous round-trip per call returns the
// completion and row frames intact.
func TestClientRoundTrip(t *testing.T) {
	c := pipeClient(newShardServer(t))
	ctx := context.Background()

	ddl, err := c.Do(ctx, "shard-a", ownedReq(
		`CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL, n INTEGER)`))
	if err != nil {
		t.Fatalf("DDL: %v", err)
	}
	if ddl.Kind != shardservice.ResponseCompletion {
		t.Fatalf("DDL kind = %s, want Completion", ddl.Kind)
	}

	ins, err := c.Do(ctx, "shard-a", ownedReq(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		shardservice.StringParam("keep"), shardservice.StringParam("Ada"), shardservice.NumberParam("1")))
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if ins.Kind != shardservice.ResponseCompletion || ins.RowsAffected != 1 {
		t.Fatalf("INSERT = %+v, want completion affecting 1 row", ins)
	}

	sel, err := c.Do(ctx, "shard-a", ownedReq(`SELECT id FROM docs`))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if sel.Kind != shardservice.ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("SELECT = %+v, want one row", sel)
	}
	if sel.HasReadPosition || !sel.ReadPosition.IsZero() {
		t.Fatalf("strong read claimed unimplemented applied position %+v", sel.ReadPosition)
	}
	if got := string(sel.Rows[0][0].Bytes); got != `"keep"` {
		t.Fatalf("selected id = %s, want \"keep\"", got)
	}
}

// TestClientErrorMapping proves each admission refusal returns a typed
// *ShardError whose Unwrap matches the expected sentinel — the distribution
// ownership sentinels for these routing-fence kinds.
func TestClientErrorMapping(t *testing.T) {
	c := pipeClient(newShardServer(t))
	ctx := context.Background()

	cases := []struct {
		name     string
		mutate   func(*shardservice.ShardRequest)
		kind     shardservice.ErrorKind
		sentinel error
	}{
		{"wrong_distribution", func(r *shardservice.ShardRequest) { r.Distribution = "other" }, shardservice.ErrorNotOwner, distribution.ErrNotShardOwner},
		{"wrong_shard", func(r *shardservice.ShardRequest) { r.Shard = "80-" }, shardservice.ErrorNotOwner, distribution.ErrNotShardOwner},
		{"stale_allocation", func(r *shardservice.ShardRequest) { r.AllocationGeneration++ }, shardservice.ErrorShardAllocation, distribution.ErrShardAllocation},
		{"stale_routing_version", func(r *shardservice.ShardRequest) { r.RoutingVersion = 99 }, shardservice.ErrorRoutingVersion, distribution.ErrRoutingVersion},
		{"stale_epoch", func(r *shardservice.ShardRequest) { r.OwnershipEpoch = 99 }, shardservice.ErrorOwnershipEpoch, distribution.ErrOwnershipEpoch},
		{"session_position_unsupported", func(r *shardservice.ShardRequest) { r.ReadPolicy = shardservice.ReadSession }, shardservice.ErrorPositionUnsupported, ErrPositionUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := ownedReq(`SELECT id FROM docs`)
			tc.mutate(req)
			_, err := c.Do(ctx, "shard-a", req)
			var se *ShardError
			if !errors.As(err, &se) {
				t.Fatalf("err = %v, want *ShardError", err)
			}
			if se.Kind != tc.kind {
				t.Fatalf("error kind = %s, want %s", se.Kind, tc.kind)
			}
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.sentinel)
			}
		})
	}
}

// TestClientExtendedErrorMapping covers safety refusals that are difficult to
// inject through an ordinary healthy shard execution.
func TestClientExtendedErrorMapping(t *testing.T) {
	tests := []struct {
		kind     shardservice.ErrorKind
		sentinel error
	}{
		{shardservice.ErrorReadOnly, ErrWriteNotSupported},
		{shardservice.ErrorUnsupportedReadPolicy, ErrReadPolicyUnsupported},
		{shardservice.ErrorCommitOutcomeUnknown, ErrCommitOutcomeUnknown},
		{shardservice.ErrorPositionUnsupported, ErrPositionUnsupported},
		{shardservice.ErrorPositionIdentity, ErrPositionIdentity},
		{shardservice.ErrorPositionNotReached, ErrPositionNotReached},
	}
	for _, tc := range tests {
		err := shardError(shardservice.NewErrorResponse(tc.kind, tc.kind.String()))
		if !errors.Is(err, tc.sentinel) {
			t.Fatalf("kind %s maps to %v, want errors.Is %v", tc.kind, err, tc.sentinel)
		}
		if isStaleErr(err) {
			t.Fatalf("kind %s was classified as retryable stale topology", tc.kind)
		}
	}
}

// TestRoundTripValidatesReadPositionProof drives logically malicious but
// wire-valid success responses through the client. A missing, unrelated, or
// behind proof fails closed with no response value and is never classified as
// retryable stale topology.
func TestRoundTripValidatesReadPositionProof(t *testing.T) {
	minimum := sessionPosition("tenant_data", "-80", 7, 40)
	baseReq := func() *shardservice.ShardRequest {
		return &shardservice.ShardRequest{
			SQL:            "SELECT 1",
			HasMinPosition: true,
			MinPosition:    minimum,
			Distribution:   minimum.Distribution,
			Shard:          minimum.Shard,
			ReadPolicy:     shardservice.ReadSession,
			ExecutionMode:  shardservice.ExecutionReadOnly,
			RoutingVersion: 1,
			OwnershipEpoch: 1,
		}
	}
	rowsAt := func(p Position) *shardservice.ShardResponse {
		resp := shardservice.RowsResponse(nil, nil)
		resp.HasReadPosition = true
		resp.ReadPosition = p
		return resp
	}

	tests := []struct {
		name     string
		resp     *shardservice.ShardResponse
		wantKind shardservice.ErrorKind
		wantIs   error
	}{
		{
			name:     "missing",
			resp:     shardservice.RowsResponse(nil, nil),
			wantKind: shardservice.ErrorPositionUnsupported,
			wantIs:   ErrPositionUnsupported,
		},
		{
			name: "different_log",
			resp: func() *shardservice.ShardResponse {
				p := minimum
				p.LogID[0]++
				p.Index = 100
				return rowsAt(p)
			}(),
			wantKind: shardservice.ErrorPositionIdentity,
			wantIs:   ErrPositionIdentity,
		},
		{
			name: "behind",
			resp: func() *shardservice.ShardResponse {
				p := minimum
				p.Index--
				return rowsAt(p)
			}(),
			wantKind: shardservice.ErrorPositionNotReached,
			wantIs:   ErrPositionNotReached,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := roundTripScriptedResponse(t, baseReq(), tc.resp)
			if got != nil {
				t.Fatalf("response leaked on failed proof: %+v", got)
			}
			var se *ShardError
			if !errors.As(err, &se) || se.Kind != tc.wantKind {
				t.Fatalf("err = %v, want *ShardError kind %s", err, tc.wantKind)
			}
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if isStaleErr(err) {
				t.Fatalf("position proof failure %v classified as retryable stale topology", err)
			}
		})
	}

	ahead := minimum
	ahead.Index++
	for _, valid := range []Position{minimum, ahead} {
		got, err := roundTripScriptedResponse(t, baseReq(), rowsAt(valid))
		if err != nil || got == nil || !got.HasReadPosition || got.ReadPosition != valid {
			t.Fatalf("valid proof = (%+v, %v), want accepted position %+v", got, err, valid)
		}
	}

	withoutMinimum := baseReq()
	withoutMinimum.HasMinPosition = false
	withoutMinimum.MinPosition = Position{}
	got, err := roundTripScriptedResponse(t, withoutMinimum, rowsAt(ahead))
	if err != nil || got == nil || !got.HasReadPosition {
		t.Fatalf("unsolicited valid read position = (%+v, %v), want accepted", got, err)
	}
}

func TestClientRejectsMismatchedMinimumBeforeDial(t *testing.T) {
	minimum := sessionPosition("tenant_data", "-80", 7, 40)
	req := &shardservice.ShardRequest{
		SQL:            "SELECT 1",
		HasMinPosition: true,
		MinPosition:    minimum,
		Distribution:   minimum.Distribution,
		Shard:          "80-",
		ReadPolicy:     shardservice.ReadSession,
		ExecutionMode:  shardservice.ExecutionReadOnly,
		RoutingVersion: 1,
		OwnershipEpoch: 1,
	}
	dials := 0
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected dial")
	})
	resp, err := client.Do(context.Background(), "unused", req)
	if resp != nil {
		t.Fatalf("response = %+v, want nil", resp)
	}
	if !errors.Is(err, ErrPositionIdentity) {
		t.Fatalf("err = %v, want errors.Is ErrPositionIdentity", err)
	}
	var shardErr *ShardError
	if !errors.As(err, &shardErr) || shardErr.Kind != shardservice.ErrorPositionIdentity {
		t.Fatalf("err = %v, want typed PositionIdentity", err)
	}
	if dials != 0 {
		t.Fatalf("dials = %d, want zero", dials)
	}
}

func roundTripScriptedResponse(
	t *testing.T,
	req *shardservice.ShardRequest,
	resp *shardservice.ShardResponse,
) (*shardservice.ShardResponse, error) {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()
	served := make(chan error, 1)
	go func() {
		defer server.Close()
		if _, err := shardservice.DecodeRequest(server); err != nil {
			served <- err
			return
		}
		served <- shardservice.EncodeResponse(server, resp)
	}()
	got, err := RoundTrip(context.Background(), client, req)
	if serveErr := <-served; serveErr != nil {
		t.Fatalf("scripted shard: %v", serveErr)
	}
	return got, err
}

// TestRoundTripHonorsContext proves a deadline or cancellation unblocks the
// blocking round-trip and surfaces as ctx.Err(). The peer never reads, so the
// encode blocks until the tripped deadline fails it — deterministically.
func TestRoundTripHonorsContext(t *testing.T) {
	cases := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			want: context.Canceled,
		},
		{
			name: "deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			want: context.DeadlineExceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			ctx, cancel := tc.ctx()
			defer cancel()
			_, err := RoundTrip(ctx, client, ownedReq(`SELECT id FROM docs`))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.want)
			}
		})
	}
}
