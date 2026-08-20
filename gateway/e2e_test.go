package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// End-to-end coverage of the bounded distributed read path: a real multi-shard
// cluster of in-process shard services, each owning a disjoint keyspace range of
// one manifest and backed by its own embedded store, reached over the real wire
// codec through a counting dialer. The tests prove the fan-out shapes (single,
// targeted subset, scatter), the cross-shard ordered merge and global limit,
// per-shard cap propagation, deadline-driven cancellation of outstanding shards,
// stale-epoch refresh-then-retry against a strictly newer generation, targeted-
// only admission of an unbounded scatter, and single-generation pinning.

// e2eShard is one shard of the cluster: its ownership coordinates, keyspace
// range, synthetic address, backing server, and the n values seeded into it.
type e2eShard struct {
	id                   distribution.ShardID
	allocationGeneration distribution.ShardAllocationGeneration
	endpoint             distribution.EndpointID
	epoch                distribution.OwnershipEpoch
	kr                   distribution.KeyRange
	address              string
	server               *shardservice.Server
	keys                 []string
	ns                   []int64
}

// e2eCluster is a running multi-shard cluster plus the counting dialer and
// client a gateway reaches it by.
type e2eCluster struct {
	dist    distribution.DistributionName
	version distribution.RoutingVersion
	shards  []e2eShard
	man     *distribution.Manifest
	mapper  *distribution.NativeMapper
	dialer  *e2eDialer
	client  *Client
}

// countedConn wraps a shard connection to count how many times it was closed, so
// a test can prove an outstanding shard's connection was released on cancellation.
type countedConn struct {
	net.Conn
	rec     *e2eDialer
	address string
	once    sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.rec.mu.Lock()
		c.rec.closes[c.address]++
		c.rec.mu.Unlock()
	})
	return c.Conn.Close()
}

// e2eDialer serves each dial on a fresh in-process pipe to the addressed server
// and records per-address dial and close counts. An address marked as a
// blackhole is never served, so its writes block until the caller's context
// trips the deadline — modeling an unresponsive shard.
type e2eDialer struct {
	mu      sync.Mutex
	dials   map[string]int
	closes  map[string]int
	servers map[string]*shardservice.Server
	black   map[string]bool
}

func newE2EDialer(servers map[string]*shardservice.Server) *e2eDialer {
	return &e2eDialer{
		dials:   map[string]int{},
		closes:  map[string]int{},
		servers: servers,
		black:   map[string]bool{},
	}
}

func (d *e2eDialer) dial(_ context.Context, address string) (net.Conn, error) {
	d.mu.Lock()
	d.dials[address]++
	black := d.black[address]
	srv := d.servers[address]
	d.mu.Unlock()
	if !black && srv == nil {
		return nil, fmt.Errorf("no shard at %q", address)
	}
	client, server := net.Pipe()
	if black {
		// The peer end is intentionally abandoned; the first write blocks with
		// no reader until the client's context trips the pipe's deadline.
		_ = server
		return &countedConn{Conn: client, rec: d, address: address}, nil
	}
	go srv.ServeConn(server)
	return &countedConn{Conn: client, rec: d, address: address}, nil
}

func (d *e2eDialer) blackhole(addresses ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, a := range addresses {
		d.black[a] = true
	}
}

func (d *e2eDialer) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.dials)
	clear(d.closes)
}

func (d *e2eDialer) dialCount(address string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials[address]
}

func (d *e2eDialer) closeCount(address string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closes[address]
}

func (d *e2eDialer) totalDials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, v := range d.dials {
		n += v
	}
	return n
}

// e2eRange builds a shard key range from a start byte and either an open upper
// bound or an exclusive end byte.
func e2eRange(start byte, max bool, end byte) distribution.KeyRange {
	r := distribution.KeyRange{Start: point(start)}
	if max {
		r.End = distribution.KeyspaceEnd{Max: true}
	} else {
		r.End = distribution.KeyspaceEnd{Point: point(end)}
	}
	return r
}

// plainDial serves each dial on a fresh pipe without counting; the cluster seeds
// its shards through it so seeding never pollutes the counting dialer.
func plainDial(servers map[string]*shardservice.Server) DialFunc {
	return func(_ context.Context, address string) (net.Conn, error) {
		srv, ok := servers[address]
		if !ok {
			return nil, fmt.Errorf("no shard at %q", address)
		}
		client, server := net.Pipe()
		go srv.ServeConn(server)
		return client, nil
	}
}

// newE2ECluster starts a four-shard cluster splitting the keyspace at 0x40, 0x80,
// and 0xC0, each shard a real in-process server over a fresh store, and seeds two
// distinct n values into each shard's messages table.
func newE2ECluster(t *testing.T) *e2eCluster {
	t.Helper()
	const dist = distribution.DistributionName("tenant_data")
	const version = distribution.RoutingVersion(7)

	specs := []struct {
		id       distribution.ShardID
		endpoint distribution.EndpointID
		epoch    distribution.OwnershipEpoch
		kr       distribution.KeyRange
		ns       []int64
	}{
		{"s0", "ep-0", 10, e2eRange(0x00, false, 0x40), []int64{1, 2}},
		{"s1", "ep-1", 11, e2eRange(0x40, false, 0x80), []int64{11, 12}},
		{"s2", "ep-2", 12, e2eRange(0x80, false, 0xC0), []int64{21, 22}},
		{"s3", "ep-3", 13, e2eRange(0xC0, true, 0x00), []int64{31, 32}},
	}

	servers := make(map[string]*shardservice.Server, len(specs))
	shards := make([]e2eShard, len(specs))
	manShards := make([]distribution.Shard, len(specs))
	for i, spec := range specs {
		allocationGeneration := distribution.ShardAllocationGeneration(i + 1)
		ownership := shardservice.Ownership{
			Distribution: dist, Shard: spec.id, AllocationGeneration: allocationGeneration,
			Epoch: spec.epoch, RoutingVersion: version,
		}
		db := initializeTestShardStore(
			t, filepath.Join(t.TempDir(), string(spec.id)+".vdb"), ownership,
		)
		srv, err := shardservice.NewServer(db, ownership, shardservice.Options{})
		if err != nil {
			t.Fatalf("NewServer %s: %v", spec.id, err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		address := "shard-" + string(spec.id)
		servers[address] = srv
		shards[i] = e2eShard{
			id: spec.id, allocationGeneration: allocationGeneration,
			endpoint: spec.endpoint, epoch: spec.epoch,
			kr: spec.kr, address: address, server: srv, ns: spec.ns,
		}
		manShards[i] = distribution.Shard{
			ID: spec.id, AllocationGeneration: allocationGeneration,
			Range: spec.kr, Leaders: []distribution.EndpointID{spec.endpoint}, Epoch: spec.epoch,
		}
	}

	man, err := distribution.NewManifest(dist, version, manShards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}

	dialer := newE2EDialer(servers)
	c := &e2eCluster{
		dist:    dist,
		version: version,
		shards:  shards,
		man:     man,
		mapper:  distribution.NewNativeMapper(1),
		dialer:  dialer,
		// Fan-out tests count physical dials as their dispatch oracle. Keep that
		// transport diagnostic independent from the default client's reuse policy;
		// client_pool_test.go covers pooling itself.
		client: NewClientWithOptions(dialer.dial, ClientOptions{
			DisableConnectionReuse: true,
		}),
	}

	// Seed each shard through an uncounted dialer.
	seedClient := NewClient(plainDial(servers))
	for i := range c.shards {
		c.shards[i].keys = c.keysForShard(t, c.shards[i].id, len(c.shards[i].ns))
		sh := c.shards[i]
		c.seed(t, seedClient, sh, `CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`, nil)
		for j, n := range sh.ns {
			c.seed(t, seedClient, sh,
				`INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
				[]shardservice.Param{
					shardservice.StringParam(sh.keys[j]),
					shardservice.NumberParam(fmt.Sprintf("%d", n)),
				})
		}
	}
	return c
}

// seed runs one setup statement against a shard, admitting against its ownership.
func (c *e2eCluster) seed(t *testing.T, client *Client, sh e2eShard, sql string, params []shardservice.Param) {
	t.Helper()
	req := &shardservice.ShardRequest{
		SQL: sql, Params: params,
		Distribution: c.dist, Shard: sh.id, AllocationGeneration: sh.allocationGeneration,
		RoutingVersion: c.version, OwnershipEpoch: sh.epoch,
		ExecutionMode: shardservice.ExecutionReadWrite,
	}
	if _, err := client.Do(context.Background(), sh.address, req); err != nil {
		t.Fatalf("seed %q on %s: %v", sql, sh.id, err)
	}
}

// snapshot builds a snapshot generation with the cluster's live epochs.
func (c *e2eCluster) snapshot(t *testing.T, gen uint64) *Snapshot {
	return c.buildSnapshot(t, gen, nil)
}

// buildSnapshot builds a snapshot generation, applying any per-shard epoch
// override so a test can pin a generation whose ownership epoch is stale.
func (c *e2eCluster) buildSnapshot(t *testing.T, gen uint64, epochs map[distribution.ShardID]distribution.OwnershipEpoch) *Snapshot {
	t.Helper()
	manShards := make([]distribution.Shard, len(c.shards))
	endpoints := make(map[distribution.EndpointID]string, len(c.shards))
	for i, s := range c.shards {
		epoch := s.epoch
		if o, ok := epochs[s.id]; ok {
			epoch = o
		}
		manShards[i] = distribution.Shard{
			ID: s.id, AllocationGeneration: s.allocationGeneration,
			Range: s.kr, Leaders: []distribution.EndpointID{s.endpoint}, Epoch: epoch,
		}
		endpoints[s.endpoint] = s.address
	}
	man, err := distribution.NewManifest(c.dist, c.version, manShards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: c.dist, Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: c.dist, Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{man},
	}
	snap, err := NewSnapshot(config, endpoints, gen)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	return snap
}

// keysForShard finds count distinct string values owned by want. The fixture
// seeds rows under these values, keeping SQL-derived routing and physical data
// ownership consistent.
func (c *e2eCluster) keysForShard(t *testing.T, want distribution.ShardID, count int) []string {
	t.Helper()
	out := make([]string, 0, count)
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("k%d", i)
		s := distribution.NewString(key)
		p, err := c.mapper.PointFor([]distribution.Scalar{s})
		if err != nil {
			t.Fatalf("PointFor: %v", err)
		}
		if id, ok := c.man.ResolvePoint(p); ok && id == want {
			out = append(out, key)
			if len(out) == count {
				return out
			}
		}
	}
	t.Fatalf("found %d keys for shard %s, want %d", len(out), want, count)
	return nil
}

// freshKeysForShard finds count distinct keys owned by want that the fixture
// did not seed, so a write test can insert under them and prove the statement
// routed to the owning shard instead of colliding with a seeded row.
func (c *e2eCluster) freshKeysForShard(t *testing.T, want distribution.ShardID, count int) []string {
	t.Helper()
	taken := map[string]bool{}
	for _, s := range c.shards {
		for _, k := range s.keys {
			taken[k] = true
		}
	}
	out := make([]string, 0, count)
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("k%d", i)
		if taken[key] {
			continue
		}
		s := distribution.NewString(key)
		p, err := c.mapper.PointFor([]distribution.Scalar{s})
		if err != nil {
			t.Fatalf("PointFor: %v", err)
		}
		if id, ok := c.man.ResolvePoint(p); ok && id == want {
			out = append(out, key)
			if len(out) == count {
				return out
			}
		}
	}
	t.Fatalf("found %d fresh keys for shard %s, want %d", len(out), want, count)
	return nil
}

// verifyInserted reads back one written key through the read path and proves
// the write committed on its owning shard under the exact key and value.
func (c *e2eCluster) verifyInserted(t *testing.T, key string, n int64) {
	t.Helper()
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	c.dialer.reset()
	res, err := e.Query(context.Background(), Query{
		SQL:    `SELECT n FROM messages WHERE tenant_id = ?`,
		Params: stringParams(key),
		Class:  ClassInteractive,
	})
	if err != nil {
		t.Fatalf("verification read: %v", err)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, []int64{n}) {
		t.Fatalf("written row = %v, want [%d]", got, n)
	}
}

// verifyDeleted reads back a deleted key and proves the row is gone.
func (c *e2eCluster) verifyDeleted(t *testing.T, key string) {
	t.Helper()
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	c.dialer.reset()
	res, err := e.Query(context.Background(), Query{
		SQL:    `SELECT n FROM messages WHERE tenant_id = ?`,
		Params: stringParams(key),
		Class:  ClassInteractive,
	})
	if err != nil {
		t.Fatalf("verification read: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("deleted key still holds rows: %+v", res.Rows)
	}
}

// assertDialedOnly asserts every want address was dialed exactly once and no
// other shard was contacted.
func (c *e2eCluster) assertDialedOnly(t *testing.T, want ...string) {
	c.assertDialedOnlyN(t, 1, want...)
}

func (c *e2eCluster) assertDialedOnlyN(t *testing.T, count int, want ...string) {
	t.Helper()
	set := map[string]bool{}
	for _, a := range want {
		set[a] = true
	}
	for _, s := range c.shards {
		got := c.dialer.dialCount(s.address)
		switch {
		case set[s.address] && got != count:
			t.Fatalf("shard %s dialed %d times, want %d", s.id, got, count)
		case !set[s.address] && got != 0:
			t.Fatalf("shard %s dialed %d times, want 0", s.id, got)
		}
	}
}

// sortedNs returns the n values of the given shards in ascending order — the
// expected merged output of a route that fans to exactly those shards.
func sortedNs(shards ...e2eShard) []int64 {
	var out []int64
	for _, s := range shards {
		out = append(out, s.ns...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// selectOrdered is the ordered read every fan-out test runs on each shard.
const selectOrdered = `SELECT n FROM messages ORDER BY n`

// TestE2EFanoutShapes proves the three physical fan-out shapes over the real
// wire: a single-shard route contacts exactly one shard, a targeted route
// contacts exactly the routed subset, and a scatter contacts every shard — each
// merging the contacted shards' already-ordered rows into one globally ordered
// result.
func TestE2EFanoutShapes(t *testing.T) {
	c := newE2ECluster(t)
	holder := NewCatalogHolder(c.snapshot(t, 1))

	tests := []struct {
		name       string
		sql        string
		params     []shardservice.Param
		class      OperationClass
		wantKind   distribution.RouteKind
		wantShards int
		wantDialed []string
		wantRows   []int64
	}{
		{
			name:       "single_shard",
			sql:        `SELECT n FROM messages WHERE tenant_id IN (?, ?) ORDER BY n`,
			params:     stringParams(c.shards[2].keys...),
			class:      ClassInteractive,
			wantKind:   distribution.RouteSingle,
			wantShards: 1,
			wantDialed: []string{c.shards[2].address},
			wantRows:   sortedNs(c.shards[2]),
		},
		{
			name:       "targeted_subset",
			sql:        `SELECT n FROM messages WHERE tenant_id IN (?, ?, ?, ?) ORDER BY n`,
			params:     stringParams(append(append([]string{}, c.shards[0].keys...), c.shards[2].keys...)...),
			class:      ClassInteractive,
			wantKind:   distribution.RouteTargeted,
			wantShards: 2,
			wantDialed: []string{c.shards[0].address, c.shards[2].address},
			wantRows:   sortedNs(c.shards[0], c.shards[2]),
		},
		{
			name:       "scatter_all",
			sql:        selectOrdered,
			class:      ClassBatch,
			wantKind:   distribution.RouteScatter,
			wantShards: 4,
			wantDialed: []string{c.shards[0].address, c.shards[1].address, c.shards[2].address, c.shards[3].address},
			wantRows:   sortedNs(c.shards...),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c.dialer.reset()
			e := NewExecutor(c.client, holder, Options{})
			res, err := e.Query(context.Background(), Query{
				SQL:    tc.sql,
				Params: tc.params,
				Class:  tc.class,
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if res.RouteKind != tc.wantKind {
				t.Fatalf("route kind = %v, want %v", res.RouteKind, tc.wantKind)
			}
			if res.ShardsFanned != tc.wantShards {
				t.Fatalf("shards fanned = %d, want %d", res.ShardsFanned, tc.wantShards)
			}
			if got := decodeInts(t, res.Rows); !equalInts(got, tc.wantRows) {
				t.Fatalf("merged rows = %v, want %v", got, tc.wantRows)
			}
			if tc.wantShards == 1 {
				c.assertDialedOnly(t, tc.wantDialed...)
			} else {
				c.assertDialedOnlyN(t, 3, tc.wantDialed...)
			}
		})
	}
}

func stringParams(values ...string) []shardservice.Param {
	params := make([]shardservice.Param, len(values))
	for i, value := range values {
		params[i] = shardservice.StringParam(value)
	}
	return params
}

// TestE2EDistributedWritesRejectedBeforeDispatch proves the distributed read
// API cannot partially commit or replay a mutation: every mutating statement
// kind, including INSERT RETURNING, is refused before route planning or
// dispatch. A final read proves shard data stayed unchanged.
func TestE2EDistributedWritesRejectedBeforeDispatch(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})

	tests := []struct {
		name  string
		sql   string
		class OperationClass
	}{
		{
			name:  "single_insert",
			sql:   `INSERT INTO messages (tenant_id, n) VALUES ('blocked', 99)`,
			class: ClassInteractive,
		},
		{
			name:  "targeted_update",
			sql:   `UPDATE messages SET "$doc" = '{"tenant_id":"blocked","n":99}'`,
			class: ClassInteractive,
		},
		{
			name:  "scatter_delete",
			sql:   `DELETE FROM messages`,
			class: ClassBatch,
		},
		{
			name:  "scatter_ddl",
			sql:   `DROP TABLE messages`,
			class: ClassBatch,
		},
		{
			name:  "targeted_insert_returning",
			sql:   `INSERT INTO messages (tenant_id, n) VALUES ('blocked', 99) RETURNING tenant_id`,
			class: ClassInteractive,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c.dialer.reset()
			_, err := e.Query(context.Background(), Query{
				SQL:   tc.sql,
				Class: tc.class,
			})
			if !errors.Is(err, ErrWriteNotSupported) {
				t.Fatalf("Query err = %v, want ErrWriteNotSupported", err)
			}
			var typed *WriteNotSupportedError
			if !errors.As(err, &typed) {
				t.Fatalf("Query err = %T, want *WriteNotSupportedError", err)
			}
			if got := c.dialer.totalDials(); got != 0 {
				t.Fatalf("mutation dialed %d shards, want zero", got)
			}
		})
	}

	c.dialer.reset()
	_, err := e.Query(context.Background(), Query{
		SQL:   `SELCT broken`,
		Class: ClassBatch,
	})
	var parseErr *sqlast.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("malformed SQL err = %T %v, want *sql.ParseError", err, err)
	}
	if got := c.dialer.totalDials(); got != 0 {
		t.Fatalf("malformed SQL dialed %d shards, want zero", got)
	}

	c.dialer.reset()
	res, err := e.Query(context.Background(), Query{
		SQL:   selectOrdered,
		Class: ClassBatch,
	})
	if err != nil {
		t.Fatalf("verification read: %v", err)
	}
	if got, want := decodeInts(t, res.Rows), sortedNs(c.shards...); !equalInts(got, want) {
		t.Fatalf("rows after refused mutations = %v, want %v", got, want)
	}
}

// TestE2EDistributedWriteRoutesToOwningShard proves every single-shard write
// form dispatches to exactly the shard owning its rows and commits as one
// local statement: a flat multi-row INSERT, a whole-document INSERT with
// RETURNING, a whole-document UPDATE whose replacement keeps the shard key, a
// targeted DELETE, and a contradictory-predicate DELETE that routes to an
// empty set and contacts no shard. Each write is read back through the read
// path to prove it landed on its owning shard.
func TestE2EDistributedWriteRoutesToOwningShard(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	sh := c.shards[1]

	// Flat multi-row INSERT: every row routes to the same shard.
	flat := c.freshKeysForShard(t, sh.id, 3)
	c.dialer.reset()
	res, err := e.Exec(context.Background(), Query{
		SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?), (?, ?)`,
		Params: []shardservice.Param{
			shardservice.StringParam(flat[0]), shardservice.NumberParam("100"),
			shardservice.StringParam(flat[1]), shardservice.NumberParam("101"),
		},
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec insert: %v", err)
	}
	if res.Kind != shardservice.ResponseCompletion || res.RowsAffected != 2 {
		t.Fatalf("insert = %+v, want completion affecting 2 rows", res)
	}
	if res.RouteKind != distribution.RouteSingle || res.ShardsFanned != 1 {
		t.Fatalf("route = %v fanned %d, want single shard", res.RouteKind, res.ShardsFanned)
	}
	c.assertDialedOnly(t, sh.address)
	c.verifyInserted(t, flat[0], 100)
	c.verifyInserted(t, flat[1], 101)

	// Whole-document INSERT with RETURNING: the row routes by its document's
	// shard key and the returned row is the one published.
	docKey := flat[2]
	c.dialer.reset()
	res, err = e.Exec(context.Background(), Query{
		SQL:    `INSERT INTO messages VALUES (?) RETURNING n`,
		Params: []shardservice.Param{shardservice.DocumentParam(fmt.Sprintf(`{"tenant_id":%q,"n":200}`, docKey))},
		Class:  ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec document insert: %v", err)
	}
	if res.Kind != shardservice.ResponseRows {
		t.Fatalf("insert returning kind = %s, want Rows", res.Kind)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, []int64{200}) {
		t.Fatalf("returned row = %v, want [200]", got)
	}
	c.assertDialedOnly(t, sh.address)
	c.verifyInserted(t, docKey, 200)

	// Whole-document UPDATE: the replacement keeps the shard key, so the row
	// stays in place and the statement targets exactly its shard.
	c.dialer.reset()
	res, err = e.Exec(context.Background(), Query{
		SQL: `UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
		Params: []shardservice.Param{
			shardservice.DocumentParam(fmt.Sprintf(`{"tenant_id":%q,"n":300}`, flat[0])),
			shardservice.StringParam(flat[0]),
		},
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec update: %v", err)
	}
	if res.Kind != shardservice.ResponseCompletion || res.RowsAffected != 1 {
		t.Fatalf("update = %+v, want completion affecting 1 row", res)
	}
	c.assertDialedOnly(t, sh.address)
	c.verifyInserted(t, flat[0], 300)

	// Targeted DELETE: the predicate resolves to the row's shard and the row
	// is gone afterwards.
	c.dialer.reset()
	res, err = e.Exec(context.Background(), Query{
		SQL:    `DELETE FROM messages WHERE tenant_id = ?`,
		Params: stringParams(flat[0]),
		Class:  ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec delete: %v", err)
	}
	if res.Kind != shardservice.ResponseCompletion || res.RowsAffected != 1 {
		t.Fatalf("delete = %+v, want completion affecting 1 row", res)
	}
	c.assertDialedOnly(t, sh.address)
	c.verifyDeleted(t, flat[0])

	// A contradictory shard-key predicate resolves to an empty route: a
	// successful local no-op that contacts no shard.
	c.dialer.reset()
	res, err = e.Exec(context.Background(), Query{
		SQL:   `DELETE FROM messages WHERE tenant_id = 'a' AND tenant_id = 'b'`,
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec empty-route delete: %v", err)
	}
	if res.RouteKind != distribution.RouteEmpty || res.RowsAffected != 0 {
		t.Fatalf("empty route = %v affected %d, want empty and zero", res.RouteKind, res.RowsAffected)
	}
	if got := c.dialer.totalDials(); got != 0 {
		t.Fatalf("empty route dialed %d shards, want zero", got)
	}
}

func TestE2EAtomicBatchAcrossShards(t *testing.T) {
	c := newE2ECluster(t)
	executor := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	keys0 := c.freshKeysForShard(t, c.shards[0].id, 2)
	key2 := c.freshKeysForShard(t, c.shards[2].id, 1)[0]

	result, err := executor.ExecBatch(context.Background(), []Query{
		{
			SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(keys0[0]), shardservice.NumberParam("101"),
			},
			Class: ClassInteractive,
		},
		{
			SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(key2), shardservice.NumberParam("202"),
			},
			Class: ClassInteractive,
		},
	})
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if result.Kind != shardservice.ResponseCompletion || result.RowsAffected != 2 ||
		result.RouteKind != distribution.RouteTargeted || result.ShardsFanned != 2 {
		t.Fatalf("batch result = %+v", result)
	}
	c.verifyInserted(t, keys0[0], 101)
	c.verifyInserted(t, key2, 202)

	// The second participant fails preparation on a duplicate primary key. The
	// first participant's successful dry-run must be rolled back and every
	// participant barrier released before the batch returns.
	_, err = executor.ExecBatch(context.Background(), []Query{
		{
			SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(keys0[1]), shardservice.NumberParam("303"),
			},
			Class: ClassInteractive,
		},
		{
			SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(c.shards[3].keys[0]), shardservice.NumberParam("404"),
			},
			Class: ClassInteractive,
		},
	})
	if err == nil {
		t.Fatal("batch with a duplicate participant mutation succeeded")
	}
	c.verifyDeleted(t, keys0[1])
}

func TestE2EGlobalUniqueIndexCommitsWithBaseInsert(t *testing.T) {
	c := newE2ECluster(t)
	const (
		indexDistribution = distribution.DistributionName("message_n_index")
		indexVersion      = distribution.RoutingVersion(19)
	)
	type indexShard struct {
		id         distribution.ShardID
		allocation distribution.ShardAllocationGeneration
		epoch      distribution.OwnershipEpoch
		endpoint   distribution.EndpointID
		address    string
		kr         distribution.KeyRange
	}
	indexShards := []indexShard{
		{id: "i0", allocation: 101, epoch: 31, endpoint: "ep-i0", address: "index-i0", kr: e2eRange(0, false, 0x80)},
		{id: "i1", allocation: 102, epoch: 32, endpoint: "ep-i1", address: "index-i1", kr: e2eRange(0x80, true, 0)},
	}
	manifestShards := make([]distribution.Shard, len(indexShards))
	for i := range indexShards {
		shard := &indexShards[i]
		ownership := shardservice.Ownership{
			Distribution: indexDistribution, Shard: shard.id,
			AllocationGeneration: shard.allocation, Epoch: shard.epoch,
			RoutingVersion: indexVersion,
		}
		database := initializeTestShardStore(
			t, filepath.Join(t.TempDir(), string(shard.id)+".vdb"), ownership,
		)
		server, err := shardservice.NewServer(database, ownership, shardservice.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Close() })
		c.dialer.mu.Lock()
		c.dialer.servers[shard.address] = server
		c.dialer.mu.Unlock()
		manifestShards[i] = distribution.Shard{
			ID: shard.id, AllocationGeneration: shard.allocation,
			Range: shard.kr, Leaders: []distribution.EndpointID{shard.endpoint},
			Epoch: shard.epoch,
		}
	}
	indexManifest, err := distribution.NewManifest(
		indexDistribution, indexVersion, manifestShards,
	)
	if err != nil {
		t.Fatal(err)
	}
	seedClient := NewClient(plainDial(c.dialer.servers))
	defer seedClient.Close()
	for i := range indexShards {
		shard := &indexShards[i]
		_, err := seedClient.Do(context.Background(), shard.address, &shardservice.ShardRequest{
			SQL:          `CREATE TABLE messages_by_n (PRIMARY KEY (id))`,
			Distribution: indexDistribution, Shard: shard.id,
			AllocationGeneration: shard.allocation,
			RoutingVersion:       indexVersion, OwnershipEpoch: shard.epoch,
			ExecutionMode: shardservice.ExecutionReadWrite,
		})
		if err != nil {
			t.Fatalf("create index relation on %s: %v", shard.id, err)
		}
	}

	endpoints := make(map[distribution.EndpointID]string, len(c.shards)+len(indexShards))
	for i := range c.shards {
		endpoints[c.shards[i].endpoint] = c.shards[i].address
	}
	for i := range indexShards {
		endpoints[indexShards[i].endpoint] = indexShards[i].address
	}
	snapshot, err := NewSnapshotWithIndexes(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{
			{Name: c.dist, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			{Name: indexDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
		},
		Placements: []distribution.TablePlacement{
			{Table: "messages", Distribution: c.dist, Columns: []string{"/tenant_id"}},
			{Table: "messages_by_n", Distribution: indexDistribution, Columns: []string{"/n"}},
		},
		Manifests: []*distribution.Manifest{c.man, indexManifest},
	}, endpoints, 2, []IndexDescriptor{{
		IndexID: 91, Incarnation: 1, Table: "messages", Name: "by_n",
		Relation: "messages_by_n", Paths: []string{"/n"},
		LocatorPaths: []string{"/tenant_id"},
		Flags:        IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(c.client, NewCatalogHolder(snapshot), Options{})
	baseKey := c.freshKeysForShard(t, c.shards[1].id, 1)[0]
	result, err := executor.Exec(context.Background(), Query{
		SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		Params: []shardservice.Param{
			shardservice.StringParam(baseKey), shardservice.NumberParam("777"),
		},
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("indexed insert: %v", err)
	}
	if result.RowsAffected != 1 || result.ShardsFanned != 2 ||
		result.RouteKind != distribution.RouteTargeted {
		t.Fatalf("indexed insert result = %+v", result)
	}
	c.verifyInserted(t, baseKey, 777)

	foundLocator := false
	for i := range indexShards {
		shard := &indexShards[i]
		response, queryErr := seedClient.Do(context.Background(), shard.address, &shardservice.ShardRequest{
			SQL:          `SELECT * FROM messages_by_n`,
			Distribution: indexDistribution, Shard: shard.id,
			AllocationGeneration: shard.allocation,
			RoutingVersion:       indexVersion, OwnershipEpoch: shard.epoch,
			ExecutionMode: shardservice.ExecutionReadOnly,
		})
		if queryErr != nil {
			t.Fatalf("read index relation %s: %v", shard.id, queryErr)
		}
		for _, row := range response.Rows {
			if len(row) == 1 && string(row[0].Bytes) == `["`+baseKey+`"]` {
				foundLocator = true
			}
		}
	}
	if !foundLocator {
		t.Fatal("committed global index locator was not found")
	}
	indexedRead, err := executor.Query(context.Background(), Query{
		SQL: `SELECT tenant_id, n FROM messages WHERE n = ?`,
		Params: []shardservice.Param{
			shardservice.NumberParam("777"),
		},
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("global indexed read: %v", err)
	}
	if indexedRead.RouteKind != distribution.RouteTargeted ||
		indexedRead.ShardsFanned != 2 || len(indexedRead.Rows) != 1 ||
		len(indexedRead.Rows[0]) != 2 ||
		string(indexedRead.Rows[0][0].Bytes) != `"`+baseKey+`"` ||
		string(indexedRead.Rows[0][1].Bytes) != "777" ||
		indexedRead.PlanFingerprint == "" {
		t.Fatalf("global indexed read result = %+v", indexedRead)
	}
	missingRead, err := executor.Query(context.Background(), Query{
		SQL: `SELECT tenant_id, n FROM messages WHERE n = ?`,
		Params: []shardservice.Param{
			shardservice.NumberParam("778"),
		},
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatalf("missing global indexed read: %v", err)
	}
	if missingRead.RouteKind != distribution.RouteSingle ||
		missingRead.ShardsFanned != 1 || len(missingRead.Rows) != 0 {
		t.Fatalf("missing global indexed read result = %+v", missingRead)
	}

	conflictingBase := c.freshKeysForShard(t, c.shards[3].id, 1)[0]
	_, err = executor.Exec(context.Background(), Query{
		SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		Params: []shardservice.Param{
			shardservice.StringParam(conflictingBase), shardservice.NumberParam("777"),
		},
		Class: ClassInteractive,
	})
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("duplicate global claim err = %v", err)
	}
	c.verifyDeleted(t, conflictingBase)
}

func TestE2EAtomicBatchSpansTablesAndShards(t *testing.T) {
	c := newE2ECluster(t)
	seedClient := NewClient(plainDial(c.dialer.servers))
	defer seedClient.Close()
	for i := range c.shards {
		c.seed(t, seedClient, c.shards[i],
			`CREATE TABLE audit (tenant_id STRING PRIMARY KEY, event STRING NOT NULL)`, nil)
	}
	endpoints := make(map[distribution.EndpointID]string, len(c.shards))
	for i := range c.shards {
		endpoints[c.shards[i].endpoint] = c.shards[i].address
	}
	snapshot, err := NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name: c.dist, Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		}},
		Placements: []distribution.TablePlacement{
			{Table: "messages", Distribution: c.dist, Columns: []string{"/tenant_id"}},
			{Table: "audit", Distribution: c.dist, Columns: []string{"/tenant_id"}},
		},
		Manifests: []*distribution.Manifest{c.man},
	}, endpoints, 2)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	executor := NewExecutor(c.client, NewCatalogHolder(snapshot), Options{})
	messageKey := c.shards[1].keys[0]
	auditKey := c.freshKeysForShard(t, c.shards[3].id, 1)[0]
	result, err := executor.ExecBatch(context.Background(), []Query{
		{
			SQL: `UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(fmt.Sprintf(`{"tenant_id":%q,"n":111}`, messageKey)),
				shardservice.StringParam(messageKey),
			},
			Class: ClassInteractive,
		},
		{
			SQL: `INSERT INTO audit (tenant_id, event) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(messageKey), shardservice.StringParam("updated"),
			},
			Class: ClassInteractive,
		},
		{
			SQL: `INSERT INTO audit (tenant_id, event) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(auditKey), shardservice.StringParam("remote"),
			},
			Class: ClassInteractive,
		},
	})
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if result.RowsAffected != 3 || result.ShardsFanned != 2 ||
		result.RouteKind != distribution.RouteTargeted {
		t.Fatalf("batch result = %+v", result)
	}
	message, err := executor.Query(context.Background(), Query{
		SQL:    `SELECT n FROM messages WHERE tenant_id = ?`,
		Params: []shardservice.Param{shardservice.StringParam(messageKey)},
		Class:  ClassInteractive,
	})
	if err != nil || !equalInts(decodeInts(t, message.Rows), []int64{111}) {
		t.Fatalf("updated message = %+v, %v", message, err)
	}
	for key, want := range map[string]string{messageKey: `"updated"`, auditKey: `"remote"`} {
		audit, err := executor.Query(context.Background(), Query{
			SQL:    `SELECT event FROM audit WHERE tenant_id = ?`,
			Params: []shardservice.Param{shardservice.StringParam(key)},
			Class:  ClassInteractive,
		})
		if err != nil || len(audit.Rows) != 1 || len(audit.Rows[0]) != 1 ||
			string(audit.Rows[0][0].Bytes) != want {
			t.Fatalf("audit %q = %+v, %v; want %s", key, audit, err, want)
		}
	}
}

// TestE2EWriteRefusalsBeforeDispatch proves every write shape that cannot be
// proven single-shard is refused before any network I/O: a cross-shard INSERT
// batch, a scatter UPDATE or DELETE over a non-shard-key predicate, an
// unbounded DELETE, an UPDATE whose replacement moves the row's shard key, a
// SELECT through Exec, and DDL through Exec.
func TestE2EWriteRefusalsBeforeDispatch(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	a, b := c.shards[0], c.shards[2]

	tests := []struct {
		name   string
		sql    string
		params []shardservice.Param
		want   error
	}{
		{
			name: "cross_shard_insert",
			sql:  `INSERT INTO messages (tenant_id, n) VALUES (?, ?), (?, ?)`,
			params: []shardservice.Param{
				shardservice.StringParam(c.freshKeysForShard(t, a.id, 1)[0]), shardservice.NumberParam("1"),
				shardservice.StringParam(c.freshKeysForShard(t, b.id, 1)[0]), shardservice.NumberParam("2"),
			},
			want: ErrWriteCrossShard,
		},
		{
			name:   "scatter_update",
			sql:    `UPDATE messages SET "$doc" = ? WHERE n = ?`,
			params: []shardservice.Param{shardservice.DocumentParam(`{"tenant_id":"x","n":5}`), shardservice.NumberParam("5")},
			want:   ErrWriteScatter,
		},
		{
			name:   "scatter_delete",
			sql:    `DELETE FROM messages WHERE n = ?`,
			params: []shardservice.Param{shardservice.NumberParam("5")},
			want:   ErrWriteScatter,
		},
		{
			name: "unbounded_delete",
			sql:  `DELETE FROM messages`,
			want: ErrDistributedWriteUnsupported,
		},
		{
			name: "shard_key_move",
			sql:  `UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
			params: []shardservice.Param{
				shardservice.DocumentParam(fmt.Sprintf(`{"tenant_id":%q,"n":7}`, c.freshKeysForShard(t, b.id, 1)[0])),
				shardservice.StringParam(a.keys[0]),
			},
			want: ErrWriteShardKeyMove,
		},
		{
			name:   "select_via_exec",
			sql:    `SELECT n FROM messages WHERE tenant_id = ?`,
			params: stringParams(a.keys[0]),
			want:   ErrExecRequiresMutation,
		},
		{
			name: "ddl_via_exec",
			sql:  `DROP TABLE messages`,
			want: ErrWriteNotSupported,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c.dialer.reset()
			_, err := e.Exec(context.Background(), Query{
				SQL: tc.sql, Params: tc.params, Class: ClassInteractive,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Exec err = %v, want errors.Is %v", err, tc.want)
			}
			if got := c.dialer.totalDials(); got != 0 {
				t.Fatalf("refusal dialed %d shards, want zero", got)
			}
		})
	}

	// A refused write never lands: the pre-refusal rows are unchanged.
	c.verifyInserted(t, a.keys[0], 1)
}

// TestE2EWriteStaleEpochRetry proves a write whose pinned generation carries a
// stale ownership epoch is retried exactly once, against a strictly newer
// refreshed generation, mirroring the read path: the shard refuses the stale
// epoch and the refreshed generation commits the statement.
func TestE2EWriteStaleEpochRetry(t *testing.T) {
	c := newE2ECluster(t)
	stale := c.buildSnapshot(t, 1, map[distribution.ShardID]distribution.OwnershipEpoch{c.shards[1].id: 99})
	fresh := c.snapshot(t, 2)
	holder := NewCatalogHolder(stale)

	var refreshes int
	e := NewExecutor(c.client, holder, Options{
		MaxRetries: 2,
		Refresh: func(_ context.Context, staleGen uint64) (*Snapshot, error) {
			refreshes++
			if staleGen != 1 {
				t.Errorf("refresh staleGen = %d, want 1", staleGen)
			}
			return fresh, nil
		},
	})

	sh := c.shards[1]
	docKey := c.freshKeysForShard(t, sh.id, 1)[0]
	c.dialer.reset()
	res, err := e.Exec(context.Background(), Query{
		SQL:    `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		Params: []shardservice.Param{shardservice.StringParam(docKey), shardservice.NumberParam("400")},
		Class:  ClassInteractive,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.Retries != 1 {
		t.Fatalf("retries = %d, want 1", res.Retries)
	}
	if res.Generation != 2 {
		t.Fatalf("served generation = %d, want 2 (retried against the refreshed generation)", res.Generation)
	}
	if refreshes != 1 {
		t.Fatalf("refresh count = %d, want 1", refreshes)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("rows affected = %d, want 1", res.RowsAffected)
	}
	if got := c.dialer.totalDials(); got != 2 {
		t.Fatalf("total dials = %d, want 2 (one refused stale, one committed fresh)", got)
	}
	c.verifyInserted(t, docKey, 400)
}

// TestE2EGlobalLimitTrim proves a scatter read trims the globally merged result
// to the statement's global LIMIT.
func TestE2EGlobalLimitTrim(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})

	res, err := e.Query(context.Background(), Query{
		SQL:   selectOrdered + ` LIMIT 3`,
		Class: ClassBatch,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := sortedNs(c.shards...)[:3]
	if got := decodeInts(t, res.Rows); !equalInts(got, want) {
		t.Fatalf("trimmed rows = %v, want %v", got, want)
	}
}

// TestE2EPerShardMaxRowsPropagated proves the profile's per-shard row cap is
// pushed down to each shard: with a cap below a shard's row count, the shard
// itself refuses with a resource limit far under its own default, which surfaces
// as the gateway result-limit sentinel.
func TestE2EPerShardMaxRowsPropagated(t *testing.T) {
	c := newE2ECluster(t)
	profiles := DefaultProfiles()
	batch := profiles[ClassBatch]
	batch.PerShardRows = 1 // each shard holds two rows
	profiles[ClassBatch] = batch
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{Profiles: profiles})

	_, err := e.Query(context.Background(), Query{
		SQL:   `SELECT n FROM messages`,
		Class: ClassBatch,
	})
	if !errors.Is(err, ErrResultLimit) {
		t.Fatalf("err = %v, want errors.Is ErrResultLimit", err)
	}
}

// TestE2EScatterAggregateFinalization proves shard-local aggregate states are
// finalized once at the gateway instead of being mistaken for result rows.
func TestE2EScatterAggregateFinalization(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})

	result, err := e.Query(context.Background(), Query{
		SQL:   `SELECT COUNT(*) FROM messages`,
		Class: ClassBatch,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := decodeInts(t, result.Rows); !equalInts(got, []int64{8}) {
		t.Fatalf("global COUNT = %v, want [8]", got)
	}
	if got := c.dialer.totalDials(); got != 12 {
		t.Fatalf("total dials = %d, want acquire/query/release per shard", got)
	}
	if result.PlanFingerprint == "" || result.Planning.Memo.Groups != 2 ||
		result.Planning.PhysicalAlternatives != 2 {
		t.Fatalf("planning diagnostics = %+v fingerprint=%q", result.Planning, result.PlanFingerprint)
	}
}

func TestE2EEmptyRouteAggregateIdentity(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	result, err := e.Query(context.Background(), Query{
		SQL: `SELECT COUNT(*), SUM(n), MIN(n), MAX(n) FROM messages ` +
			`WHERE tenant_id = 'a' AND tenant_id = 'b'`,
		Class: ClassInteractive,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 4 ||
		string(result.Rows[0][0].Bytes) != "0" {
		t.Fatalf("empty aggregate row = %+v", result.Rows)
	}
	for i := 1; i < 4; i++ {
		if !result.Rows[0][i].Null {
			t.Fatalf("empty aggregate column %d = %+v, want NULL", i, result.Rows[0][i])
		}
	}
	if got := c.dialer.totalDials(); got != 0 {
		t.Fatalf("empty aggregate opened %d shard connections", got)
	}
}

// TestE2EDeadlineCancelsOutstanding proves the global deadline is honored and
// cancels every outstanding shard: with all shards unresponsive, the operation
// fails with the deadline error rather than hanging, and every fanned shard's
// connection is released.
func TestE2EDeadlineCancelsOutstanding(t *testing.T) {
	c := newE2ECluster(t)
	for _, s := range c.shards {
		c.dialer.blackhole(s.address)
	}

	profiles := DefaultProfiles()
	batch := profiles[ClassBatch]
	batch.GlobalDeadline = 200 * time.Millisecond
	batch.PerShardDeadline = 10 * time.Second
	batch.MaxConcurrency = 4
	profiles[ClassBatch] = batch
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{Profiles: profiles})

	done := make(chan error, 1)
	go func() {
		_, err := e.Query(context.Background(), Query{
			SQL:   selectOrdered,
			Class: ClassBatch,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want errors.Is context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not honor the deadline; outstanding shards were not cancelled")
	}

	if got := c.dialer.totalDials(); got != 4 {
		t.Fatalf("total dials = %d, want 4 (every shard fanned)", got)
	}
	for _, s := range c.shards {
		if c.dialer.closeCount(s.address) == 0 {
			t.Fatalf("shard %s connection was never released after cancellation", s.id)
		}
	}
}

// TestE2EStaleEpochRefreshRetry proves a stale ownership epoch from one shard
// triggers exactly one retry, only against a strictly newer refreshed
// generation, and never mixes generations within an attempt.
func TestE2EStaleEpochRefreshRetry(t *testing.T) {
	c := newE2ECluster(t)
	// Generation 1 pins a stale epoch for s2 (live epoch is 12), so a scatter's
	// s2 leg is refused; generation 2 carries the correct epochs.
	stale := c.buildSnapshot(t, 1, map[distribution.ShardID]distribution.OwnershipEpoch{"s2": 99})
	fresh := c.snapshot(t, 2)
	holder := NewCatalogHolder(stale)

	var refreshes int
	e := NewExecutor(c.client, holder, Options{
		MaxRetries: 2,
		Refresh: func(_ context.Context, staleGen uint64) (*Snapshot, error) {
			refreshes++
			if staleGen != 1 {
				t.Errorf("refresh staleGen = %d, want 1", staleGen)
			}
			return fresh, nil
		},
	})

	res, err := e.Query(context.Background(), Query{
		SQL:   selectOrdered,
		Class: ClassBatch,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.Retries != 1 {
		t.Fatalf("retries = %d, want 1", res.Retries)
	}
	if res.Generation != 2 {
		t.Fatalf("served generation = %d, want 2 (retried against the refreshed generation)", res.Generation)
	}
	if refreshes != 1 {
		t.Fatalf("refresh count = %d, want 1", refreshes)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, sortedNs(c.shards...)) {
		t.Fatalf("merged rows = %v, want %v", got, sortedNs(c.shards...))
	}
}

// TestE2ETargetedOnlyRejectsScatter proves the interactive profile's targeted-
// only admission refuses an unbounded scatter with the typed routing sentinel,
// before any shard is contacted.
func TestE2ETargetedOnlyRejectsScatter(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})

	_, err := e.Query(context.Background(), Query{
		SQL:   selectOrdered,
		Class: ClassInteractive, // targeted-only
	})
	if !errors.Is(err, distribution.ErrScatterRejected) {
		t.Fatalf("err = %v, want errors.Is ErrScatterRejected", err)
	}
	if got := c.dialer.totalDials(); got != 0 {
		t.Fatalf("total dials = %d, want 0 (a rejected scatter contacts no shard)", got)
	}
}

// TestE2EPinsSingleGeneration proves one operation pins exactly one catalog
// generation: a successful read never consults a newer generation, so its
// refresh source is never invoked and its served generation is the pinned one.
func TestE2EPinsSingleGeneration(t *testing.T) {
	c := newE2ECluster(t)
	holder := NewCatalogHolder(c.snapshot(t, 1))
	e := NewExecutor(c.client, holder, Options{
		Refresh: func(context.Context, uint64) (*Snapshot, error) {
			t.Errorf("refresh consulted a newer generation on a successful operation")
			return nil, errors.New("must not refresh")
		},
	})

	// Publish a newer generation before the read; pinning must ignore it because
	// the operation reads the current generation once and holds it throughout.
	holder.Publish(c.snapshot(t, 2))

	res, err := e.Query(context.Background(), Query{
		SQL:   selectOrdered,
		Class: ClassBatch,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.Retries != 0 {
		t.Fatalf("retries = %d, want 0 (a healthy read pins one generation)", res.Retries)
	}
	if res.Generation != 2 {
		t.Fatalf("served generation = %d, want 2 (the generation current at pin time)", res.Generation)
	}
	if got := decodeInts(t, res.Rows); !equalInts(got, sortedNs(c.shards...)) {
		t.Fatalf("merged rows = %v, want %v", got, sortedNs(c.shards...))
	}
}
