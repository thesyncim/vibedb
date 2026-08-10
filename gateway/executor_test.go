package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
)

// shardNode is one in-process shard server plus the ownership and address a
// gateway reaches it by.
type shardNode struct {
	address string
	own     shardservice.Ownership
	server  *shardservice.Server
}

// seedStmt is one setup statement run against a shard before the read.
type seedStmt struct {
	sql    string
	params []shardservice.Param
}

// newShardNode builds a shard server over a fresh durable catalog for own,
// reachable at address.
func newShardNode(t *testing.T, address string, own shardservice.Ownership) shardNode {
	t.Helper()
	db := initializeTestShardStore(
		t, filepath.Join(t.TempDir(), address+".vdb"), own,
	)
	srv, err := shardservice.NewServer(db, own, shardservice.Options{})
	if err != nil {
		t.Fatalf("NewServer %s: %v", address, err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return shardNode{address: address, own: own, server: srv}
}

// dialer returns a DialFunc that pipes each round-trip to the addressed server.
func dialer(nodes map[string]shardNode) DialFunc {
	return func(_ context.Context, address string) (net.Conn, error) {
		node, ok := nodes[address]
		if !ok {
			return nil, fmt.Errorf("no shard at %q", address)
		}
		client, server := net.Pipe()
		go node.server.ServeConn(server)
		return client, nil
	}
}

// seed runs the setup statements against one node over the wire, admitting
// against its ownership coordinates.
func seed(t *testing.T, c *Client, node shardNode, stmts ...seedStmt) {
	t.Helper()
	for _, s := range stmts {
		req := &shardservice.ShardRequest{
			SQL: s.sql, Params: s.params,
			Distribution:         node.own.Distribution,
			Shard:                node.own.Shard,
			AllocationGeneration: node.own.AllocationGeneration,
			RoutingVersion:       node.own.RoutingVersion,
			OwnershipEpoch:       node.own.Epoch,
			ExecutionMode:        shardservice.ExecutionReadWrite,
		}
		if _, err := c.Do(context.Background(), node.address, req); err != nil {
			t.Fatalf("seed %q on %s: %v", s.sql, node.address, err)
		}
	}
}

// twoShardSnapshot builds a snapshot whose two-shard tenant_data manifest routes
// endpoints ep-a/ep-b to the two node addresses at the given generation and
// manifest routing version.
func twoShardSnapshot(t *testing.T, gen uint64, version distribution.RoutingVersion) *Snapshot {
	t.Helper()
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{testManifest(t, version)},
	}
	snap, err := NewSnapshot(config, map[distribution.EndpointID]string{"ep-a": "shard-a", "ep-b": "shard-b"}, gen)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	return snap
}

// newTwoShardCluster wires two shard nodes (owning -80 and 80- at routing
// version) plus a client dialing them, and seeds the messages table on each.
func newTwoShardCluster(t *testing.T, version distribution.RoutingVersion) (*Client, map[string]shardNode) {
	t.Helper()
	nodes := map[string]shardNode{
		"shard-a": newShardNode(t, "shard-a", shardservice.Ownership{Distribution: "tenant_data", Shard: "-80", AllocationGeneration: 1, Epoch: 7, RoutingVersion: version}),
		"shard-b": newShardNode(t, "shard-b", shardservice.Ownership{Distribution: "tenant_data", Shard: "80-", AllocationGeneration: 2, Epoch: 9, RoutingVersion: version}),
	}
	c := NewClient(dialer(nodes))
	ddl := seedStmt{sql: `CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`}
	seed(t, c, nodes["shard-a"], ddl,
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, params: []shardservice.Param{shardservice.StringParam("a1"), shardservice.NumberParam("1")}},
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, params: []shardservice.Param{shardservice.StringParam("a3"), shardservice.NumberParam("3")}},
	)
	seed(t, c, nodes["shard-b"], ddl,
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, params: []shardservice.Param{shardservice.StringParam("b2"), shardservice.NumberParam("2")}},
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`, params: []shardservice.Param{shardservice.StringParam("b4"), shardservice.NumberParam("4")}},
	)
	return c, nodes
}

// scatterQuery is a bounded scatter read of the ordered n column across shards.
func scatterQuery() Query {
	return Query{
		SQL:   `SELECT n FROM messages ORDER BY n`,
		Class: ClassBatch,
	}
}

// TestExecutorScatterMerge drives a bounded scatter read across two shards and
// proves the gateway fans out, merges the already-ordered shard rows into one
// globally ordered result, and records the route and fan-out metrics.
func TestExecutorScatterMerge(t *testing.T) {
	c, _ := newTwoShardCluster(t, 3)
	e := NewExecutor(c, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})

	res, err := e.Query(context.Background(), scatterQuery())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.RouteKind != distribution.RouteScatter {
		t.Fatalf("route kind = %v, want Scatter", res.RouteKind)
	}
	if res.ShardsFanned != 2 {
		t.Fatalf("shards fanned = %d, want 2", res.ShardsFanned)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("merged rows = %v, want [1 2 3 4]", got)
	}

	m := e.Metrics()
	if m.RouteScatter != 1 || m.ShardsFanned != 2 || m.RowsReturned != 4 {
		t.Fatalf("metrics = %+v, want one scatter fanning 2 shards returning 4 rows", m)
	}
}

// TestExecutorSingleShardPassthrough proves a single-shard route streams the
// shard's own ordered result through unchanged, without a merge pass.
func TestExecutorSingleShardPassthrough(t *testing.T) {
	own := shardservice.Ownership{Distribution: "tenant_data", Shard: "all", AllocationGeneration: 1, Epoch: 11, RoutingVersion: 4}
	nodes := map[string]shardNode{"solo": newShardNode(t, "solo", own)}
	c := NewClient(dialer(nodes))
	seed(t, c, nodes["solo"],
		seedStmt{sql: `CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`},
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES ('x', 5)`},
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES ('y', 1)`},
		seedStmt{sql: `INSERT INTO messages (tenant_id, n) VALUES ('z', 3)`},
	)

	man, err := distribution.NewManifest("tenant_data", 4, []distribution.Shard{{
		ID:                   "all",
		AllocationGeneration: 1,
		Range:                distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true}},
		Leaders:              []distribution.EndpointID{"ep-solo"},
		Epoch:                11,
	}})
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{man},
	}
	snap, err := NewSnapshot(config, map[distribution.EndpointID]string{"ep-solo": "solo"}, 1)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	e := NewExecutor(c, NewCatalogHolder(snap), Options{})

	res, err := e.Query(context.Background(), Query{
		SQL:   `SELECT n FROM messages WHERE tenant_id = 'x' ORDER BY n`,
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.RouteKind != distribution.RouteSingle || res.ShardsFanned != 1 {
		t.Fatalf("route = %v fanning %d shards, want Single fanning 1", res.RouteKind, res.ShardsFanned)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, []int64{5}) {
		t.Fatalf("rows = %v, want [5]", got)
	}
}

// TestExecutorStaleGenerationRetry proves a shard's stale routing-version
// refusal triggers exactly one retry, and only against a strictly newer
// refreshed generation, which then succeeds.
func TestExecutorStaleGenerationRetry(t *testing.T) {
	// The live shards serve routing version 5; the pinned generation 1 routes at
	// version 3, so the first attempt is refused as stale.
	c, _ := newTwoShardCluster(t, 5)
	holder := NewCatalogHolder(twoShardSnapshot(t, 1, 3))

	refreshed := twoShardSnapshot(t, 2, 5)
	var refreshes int
	e := NewExecutor(c, holder, Options{
		MaxRetries: 2,
		Refresh: func(_ context.Context, staleGen uint64) (*Snapshot, error) {
			refreshes++
			if staleGen != 1 {
				t.Errorf("refresh staleGen = %d, want 1", staleGen)
			}
			return refreshed, nil
		},
	})

	res, err := e.Query(context.Background(), scatterQuery())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.Retries != 1 {
		t.Fatalf("retries = %d, want 1", res.Retries)
	}
	if res.Generation != 2 {
		t.Fatalf("served generation = %d, want 2", res.Generation)
	}
	if refreshes != 1 {
		t.Fatalf("refresh count = %d, want 1", refreshes)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("rows = %v, want [1 2 3 4]", got)
	}
	if m := e.Metrics(); m.Retries != 1 {
		t.Fatalf("metric retries = %d, want 1", m.Retries)
	}
}

// TestExecutorStaleWithoutNewerGenerationFails proves a stale refusal with no
// newer generation available fails closed rather than routing against stale
// metadata or mixing generations.
func TestExecutorStaleWithoutNewerGenerationFails(t *testing.T) {
	c, _ := newTwoShardCluster(t, 5) // shards serve version 5
	holder := NewCatalogHolder(twoShardSnapshot(t, 1, 3))
	e := NewExecutor(c, holder, Options{MaxRetries: 2}) // no Refresh: holder never advances

	_, err := e.Query(context.Background(), scatterQuery())
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("err = %v, want errors.Is ErrStaleGeneration", err)
	}
}

// TestExecutorFailClosedOnShardError proves a shard's non-retryable error fails
// the whole operation rather than returning a partial result.
func TestExecutorFailClosedOnShardError(t *testing.T) {
	c, _ := newTwoShardCluster(t, 3)
	e := NewExecutor(c, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})

	q := scatterQuery()
	q.SQL = `SELECT n FROM does_not_exist ORDER BY n`
	res, err := e.Query(context.Background(), q)
	if err == nil {
		t.Fatalf("Query returned a result %+v, want a fail-closed error", res)
	}
	if isStaleErr(err) {
		t.Fatalf("err = %v, want a non-retryable planner error", err)
	}
	if !errors.Is(err, ErrTableNotPlaced) {
		t.Fatalf("err = %v, want errors.Is ErrTableNotPlaced", err)
	}
}

// TestExecutorAggregateRowCap proves the gateway enforces its own aggregate row
// cap across shards, independent of each shard's per-request cap.
func TestExecutorAggregateRowCap(t *testing.T) {
	c, _ := newTwoShardCluster(t, 3)
	profiles := DefaultProfiles()
	batch := profiles[ClassBatch]
	batch.MaxAggregateRows = 1 // each shard alone returns two rows
	profiles[ClassBatch] = batch
	e := NewExecutor(c, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{Profiles: profiles})

	_, err := e.Query(context.Background(), scatterQuery())
	if !errors.Is(err, ErrResultLimit) {
		t.Fatalf("err = %v, want errors.Is ErrResultLimit", err)
	}
}

// TestExecutorNoCatalog proves an executor with no published generation fails
// closed.
func TestExecutorNoCatalog(t *testing.T) {
	e := NewExecutor(NewClient(nil), NewCatalogHolder(nil), Options{})
	_, err := e.Query(context.Background(), scatterQuery())
	if !errors.Is(err, ErrNoCatalog) {
		t.Fatalf("err = %v, want errors.Is ErrNoCatalog", err)
	}
}

// TestExecutorCanceledBeforePreflight proves a canceled request does not parse,
// pin a catalog, or dial a shard.
func TestExecutorCanceledBeforePreflight(t *testing.T) {
	dials := 0
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected dial")
	})
	e := NewExecutor(client, NewCatalogHolder(nil), Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.Query(ctx, Query{SQL: `SELECT 1`})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if dials != 0 {
		t.Fatalf("dials = %d, want zero", dials)
	}
}

func TestExecutorRejectsInvalidParameterBeforeDispatch(t *testing.T) {
	dials := 0
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected dial")
	})
	e := NewExecutor(client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})
	_, err := e.Query(context.Background(), Query{
		SQL:    `SELECT n FROM messages WHERE tenant_id = ?`,
		Params: []shardservice.Param{{Kind: shardservice.ParamInvalid}},
	})
	if !errors.Is(err, ErrPlanParameters) {
		t.Fatalf("err = %v, want ErrPlanParameters", err)
	}
	if dials != 0 {
		t.Fatalf("dials = %d, want zero", dials)
	}
}

// equalInts reports whether two int64 slices are element-wise equal.
func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
