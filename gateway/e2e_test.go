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
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
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
	id       distribution.ShardID
	endpoint distribution.EndpointID
	epoch    distribution.OwnershipEpoch
	kr       distribution.KeyRange
	address  string
	server   *shardservice.Server
	ns       []int64
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
		db, err := sqldriver.Open(filepath.Join(t.TempDir(), string(spec.id)+".vdb"))
		if err != nil {
			t.Fatalf("Open %s: %v", spec.id, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		srv, err := shardservice.NewServer(db, shardservice.Ownership{
			Distribution: dist, Shard: spec.id, Epoch: spec.epoch, RoutingVersion: version,
		}, shardservice.Options{})
		if err != nil {
			t.Fatalf("NewServer %s: %v", spec.id, err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		address := "shard-" + string(spec.id)
		servers[address] = srv
		shards[i] = e2eShard{
			id: spec.id, endpoint: spec.endpoint, epoch: spec.epoch,
			kr: spec.kr, address: address, server: srv, ns: spec.ns,
		}
		manShards[i] = distribution.Shard{
			ID: spec.id, Range: spec.kr, Leaders: []distribution.EndpointID{spec.endpoint}, Epoch: spec.epoch,
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
		client:  NewClient(dialer.dial),
	}

	// Seed each shard through an uncounted dialer.
	seedClient := NewClient(plainDial(servers))
	for _, sh := range shards {
		c.seed(t, seedClient, sh, `CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`, nil)
		for j, n := range sh.ns {
			c.seed(t, seedClient, sh,
				`INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
				[]shardservice.Param{
					shardservice.StringParam(fmt.Sprintf("%s-r%d", sh.id, j)),
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
		Distribution: c.dist, Shard: sh.id, RoutingVersion: c.version, OwnershipEpoch: sh.epoch,
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
			ID: s.id, Range: s.kr, Leaders: []distribution.EndpointID{s.endpoint}, Epoch: epoch,
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

// keyForShard finds a string shard-key value whose native-mapper point resolves
// to the named shard, so a test can bind a route to a specific shard.
func (c *e2eCluster) keyForShard(t *testing.T, want distribution.ShardID) distribution.Scalar {
	t.Helper()
	for i := 0; i < 100000; i++ {
		s := distribution.NewString(fmt.Sprintf("k%d", i))
		p, err := c.mapper.PointFor([]distribution.Scalar{s})
		if err != nil {
			t.Fatalf("PointFor: %v", err)
		}
		if id, ok := c.man.ResolvePoint(p); ok && id == want {
			return s
		}
	}
	t.Fatalf("no key resolves to shard %s", want)
	return distribution.Scalar{}
}

// assertDialedOnly asserts every want address was dialed exactly once and no
// other shard was contacted.
func (c *e2eCluster) assertDialedOnly(t *testing.T, want ...string) {
	t.Helper()
	set := map[string]bool{}
	for _, a := range want {
		set[a] = true
	}
	for _, s := range c.shards {
		got := c.dialer.dialCount(s.address)
		switch {
		case set[s.address] && got != 1:
			t.Fatalf("shard %s dialed %d times, want 1", s.id, got)
		case !set[s.address] && got != 0:
			t.Fatalf("shard %s dialed %d times, want 0", s.id, got)
		}
	}
}

// mustFinite builds a finite domain over the values, failing the test on error.
func mustFinite(t *testing.T, values ...distribution.Scalar) distribution.ValueDomain {
	t.Helper()
	d, err := distribution.FiniteDomain(values...)
	if err != nil {
		t.Fatalf("FiniteDomain: %v", err)
	}
	return d
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

	k0 := c.keyForShard(t, "s0")
	k2 := c.keyForShard(t, "s2")

	tests := []struct {
		name       string
		cons       distribution.BoundConstraints
		class      OperationClass
		wantKind   distribution.RouteKind
		wantShards int
		wantDialed []string
		wantRows   []int64
	}{
		{
			name:       "single_shard",
			cons:       distribution.BoundConstraints{mustFinite(t, k2)},
			class:      ClassInteractive,
			wantKind:   distribution.RouteSingle,
			wantShards: 1,
			wantDialed: []string{c.shards[2].address},
			wantRows:   sortedNs(c.shards[2]),
		},
		{
			name:       "targeted_subset",
			cons:       distribution.BoundConstraints{mustFinite(t, k0, k2)},
			class:      ClassInteractive,
			wantKind:   distribution.RouteTargeted,
			wantShards: 2,
			wantDialed: []string{c.shards[0].address, c.shards[2].address},
			wantRows:   sortedNs(c.shards[0], c.shards[2]),
		},
		{
			name:       "scatter_all",
			cons:       distribution.BoundConstraints{distribution.UnknownDomain()},
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
				Distribution: c.dist,
				Constraints:  tc.cons,
				SQL:          selectOrdered,
				Class:        tc.class,
				Order:        []OrderKey{{Column: 0}},
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
			c.assertDialedOnly(t, tc.wantDialed...)
		})
	}
}

// TestE2EDistributedWritesRejectedBeforeDispatch proves the distributed read
// API cannot partially commit or replay a mutation: every mutating statement
// kind, including INSERT RETURNING, is refused before route dispatch across
// single, targeted, and scatter shapes. A final read proves shard data stayed
// unchanged.
func TestE2EDistributedWritesRejectedBeforeDispatch(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})
	k0 := c.keyForShard(t, "s0")
	k2 := c.keyForShard(t, "s2")

	tests := []struct {
		name  string
		sql   string
		cons  distribution.BoundConstraints
		class OperationClass
	}{
		{
			name:  "single_insert",
			sql:   `INSERT INTO messages (tenant_id, n) VALUES ('blocked', 99)`,
			cons:  distribution.BoundConstraints{mustFinite(t, k2)},
			class: ClassInteractive,
		},
		{
			name:  "targeted_update",
			sql:   `UPDATE messages SET "$doc" = '{"tenant_id":"blocked","n":99}'`,
			cons:  distribution.BoundConstraints{mustFinite(t, k0, k2)},
			class: ClassInteractive,
		},
		{
			name:  "scatter_delete",
			sql:   `DELETE FROM messages`,
			cons:  distribution.BoundConstraints{distribution.UnknownDomain()},
			class: ClassBatch,
		},
		{
			name:  "scatter_ddl",
			sql:   `DROP TABLE messages`,
			cons:  distribution.BoundConstraints{distribution.UnknownDomain()},
			class: ClassBatch,
		},
		{
			name:  "targeted_insert_returning",
			sql:   `INSERT INTO messages (tenant_id, n) VALUES ('blocked', 99) RETURNING tenant_id`,
			cons:  distribution.BoundConstraints{mustFinite(t, k0, k2)},
			class: ClassInteractive,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c.dialer.reset()
			_, err := e.Query(context.Background(), Query{
				Distribution: c.dist,
				Constraints:  tc.cons,
				SQL:          tc.sql,
				Class:        tc.class,
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          `SELCT broken`,
		Class:        ClassBatch,
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          selectOrdered,
		Class:        ClassBatch,
		Order:        []OrderKey{{Column: 0}},
	})
	if err != nil {
		t.Fatalf("verification read: %v", err)
	}
	if got, want := decodeInts(t, res.Rows), sortedNs(c.shards...); !equalInts(got, want) {
		t.Fatalf("rows after refused mutations = %v, want %v", got, want)
	}
}

// TestE2EGlobalLimitTrim proves a scatter read trims the globally merged result
// to the statement's global LIMIT.
func TestE2EGlobalLimitTrim(t *testing.T) {
	c := newE2ECluster(t)
	e := NewExecutor(c.client, NewCatalogHolder(c.snapshot(t, 1)), Options{})

	res, err := e.Query(context.Background(), Query{
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          selectOrdered,
		Class:        ClassBatch,
		Order:        []OrderKey{{Column: 0}},
		Limit:        3,
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          `SELECT n FROM messages`,
		Class:        ClassBatch,
	})
	if !errors.Is(err, ErrResultLimit) {
		t.Fatalf("err = %v, want errors.Is ErrResultLimit", err)
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
			Distribution: c.dist,
			Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
			SQL:          selectOrdered,
			Class:        ClassBatch,
			Order:        []OrderKey{{Column: 0}},
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          selectOrdered,
		Class:        ClassBatch,
		Order:        []OrderKey{{Column: 0}},
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          selectOrdered,
		Class:        ClassInteractive, // targeted-only
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
		Distribution: c.dist,
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          selectOrdered,
		Class:        ClassBatch,
		Order:        []OrderKey{{Column: 0}},
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
