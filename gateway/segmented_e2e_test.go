package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// TestSegmentedExecBatchAcross65RealShardServers is the public regression gate
// for the old inline-manifest boundary. Every mutation is SQL-planned and
// routed by Executor, then crosses the real shard wire and durable participant
// journal before it is applied. Sixty-five distinct fenced shard targets force
// the VTM1/VTCM lane; the number is not itself an admission limit.
func TestSegmentedExecBatchAcross65RealShardServers(t *testing.T) {
	const shardCount = distributedtxn.MaxInlineParticipants + 1
	cluster := newSegmentedE2ECluster(t, shardCount)
	queries := make([]Query, shardCount)
	for i := range queries {
		queries[i] = Query{
			SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(cluster.keys[i]),
				shardservice.NumberParam(fmt.Sprintf("%d", i+1)),
			},
			Class: ClassAdmin,
		}
	}

	executor := NewExecutor(cluster.client, NewCatalogHolder(cluster.snapshot), Options{})
	result, err := executor.ExecBatch(t.Context(), queries)
	if err != nil {
		t.Fatalf("ExecBatch across %d targets: %v", shardCount, err)
	}
	if result.RouteKind != distribution.RouteTargeted ||
		result.ShardsFanned != shardCount || result.RowsAffected != shardCount {
		t.Fatalf("result = %+v, want targeted %d-shard commit", result, shardCount)
	}
	if got := cluster.wire.totalCalls(shardservice.TransactionStageManifestCoordinator); got != 1 {
		t.Fatalf("segmented coordinator begins = %d, want 1", got)
	}
	if got := cluster.wire.totalCalls(shardservice.TransactionStageCoordinator); got != 0 {
		t.Fatalf("legacy inline coordinator begins = %d, want 0", got)
	}

	// Read every participant back through its exact fenced shard endpoint. This
	// proves the reported fan-out is not merely a coordinator cardinality claim.
	for index := range shardCount {
		request := segmentedOwnedRequest(cluster.nodes[index].own)
		request.SQL = `SELECT n FROM messages WHERE tenant_id = ?`
		request.Params = []shardservice.Param{shardservice.StringParam(cluster.keys[index])}
		request.ExecutionMode = shardservice.ExecutionReadOnly
		response, readErr := cluster.client.Do(t.Context(), cluster.nodes[index].address, request)
		if readErr != nil {
			t.Fatalf("verify shard %d: %v", index, readErr)
		}
		if got := decodeInts(t, response.Rows); !equalInts(got, []int64{int64(index + 1)}) {
			t.Fatalf("shard %d rows = %v, want [%d]", index, got, index+1)
		}
	}

	// Losing the atomic VTCM+page-zero response and the resolving lookup must
	// return a durable identity, never a generic socket error. Discovery must not
	// speculate by issuing an abort before it has learned the durable state.
	for i := range queries {
		queries[i].SQL = `DELETE FROM messages WHERE tenant_id = ?`
		queries[i].Params = []shardservice.Param{shardservice.StringParam(cluster.keys[i])}
	}
	faults := &segmentedFaultDialer{serve: func(address string, conn net.Conn) {
		cluster.servers[address].server.ServeConn(conn)
	}}
	faults.dropNext(cluster.nodes[0].address, shardservice.TransactionStageManifestCoordinator)
	faults.dropNext(cluster.nodes[0].address, shardservice.TransactionLookupCoordinator)
	faultClient := NewClientWithOptions(faults.dial, ClientOptions{DisableConnectionReuse: true})
	faultExecutor := NewExecutor(faultClient, NewCatalogHolder(cluster.snapshot), Options{})
	_, err = faultExecutor.ExecBatch(t.Context(), queries)
	var unknown *TransactionOutcomeUnknownError
	if !errors.As(err, &unknown) || unknown.ID.IsZero() {
		t.Fatalf("lost begin/lookup = %T %v calls=%v, want identified outcome unknown", err, err, faults.callSnapshot())
	}
	if got := faults.totalCalls(shardservice.TransactionAbortCoordinator); got != 0 {
		t.Fatalf("abort calls before durable discovery = %d, want zero", got)
	}
	faults.assertClean(t)

	profile := DefaultProfiles()[ClassAdmin].withDefaults()
	template := segmentedOwnedRequest(cluster.nodes[0].own)
	lookup := transactionRequest(
		template, profile, shardservice.TransactionLookupCoordinator, unknown.ID, 0, nil,
	)
	observed, lookupErr := executor.transactionRoundTrip(
		t.Context(), cluster.nodes[0].address, lookup, profile,
	)
	if lookupErr != nil {
		t.Fatalf("resolve identified outcome: %v", lookupErr)
	}
	manifestRecord, openErr := distributedtxn.OpenManifestCoordinator(observed.Transaction.Record)
	if openErr != nil || observed.Transaction.RecordKind != shardservice.TransactionRecordManifestCoordinator ||
		manifestRecord.Manifest.ParticipantCount != shardCount {
		t.Fatalf("resolved coordinator = %+v open=%v", observed.Transaction, openErr)
	}
	pageRequest := transactionRequest(
		template, profile, shardservice.TransactionReadManifestSegment, unknown.ID, 0, nil,
	)
	pageRequest.ExecutionMode = shardservice.ExecutionReadOnly
	pageRequest.Transaction.SegmentIndex = 0
	page, pageErr := executor.transactionRoundTrip(
		t.Context(), cluster.nodes[0].address, pageRequest, profile,
	)
	if pageErr != nil || page.Transaction.RecordKind != shardservice.TransactionRecordManifestSegment ||
		page.Transaction.SegmentIndex != 0 || len(page.Transaction.Record) == 0 {
		t.Fatalf("resolved page zero = %+v err=%v", page.Transaction, pageErr)
	}
	if observed.Transaction.CoordinatorState != distributedtxn.CoordinatorStaging {
		t.Fatalf("resolved coordinator state = %v, want staging", observed.Transaction.CoordinatorState)
	}
	abort := transactionRequest(
		template, profile, shardservice.TransactionAbortCoordinator,
		unknown.ID, observed.Transaction.Revision, nil,
	)
	aborted, abortErr := executor.transactionRoundTrip(
		t.Context(), cluster.nodes[0].address, abort, profile,
	)
	if abortErr != nil || aborted.Transaction.CoordinatorState != distributedtxn.CoordinatorAborted {
		t.Fatalf("resolve staging coordinator by abort = %+v err=%v", aborted, abortErr)
	}
}

type segmentedE2ECluster struct {
	nodes    []shardNode
	keys     []string
	client   *Client
	snapshot *Snapshot
	servers  map[string]shardNode
	wire     *segmentedFaultDialer
}

func newSegmentedE2ECluster(t *testing.T, count int) segmentedE2ECluster {
	t.Helper()
	const (
		dist    = distribution.DistributionName("tenant_data")
		version = distribution.RoutingVersion(19)
	)
	nodes := make([]shardNode, count)
	shards := make([]distribution.Shard, count)
	endpoints := make(map[distribution.EndpointID]string, count)
	nodeIndex := make(map[string]int, count)
	for i := range count {
		id := distribution.ShardID(fmt.Sprintf("s%03d", i))
		endpoint := distribution.EndpointID(fmt.Sprintf("ep%03d", i))
		address := fmt.Sprintf("shard%03d", i)
		allocation := distribution.ShardAllocationGeneration(i + 1)
		epoch := distribution.OwnershipEpoch(i + 101)
		keyRange := distribution.KeyRange{Start: point(byte(i))}
		if i == count-1 {
			keyRange.End.Max = true
		} else {
			keyRange.End.Point = point(byte(i + 1))
		}
		own := shardservice.Ownership{
			Distribution: dist, Shard: id, AllocationGeneration: allocation,
			Epoch: epoch, RoutingVersion: version,
		}
		nodes[i] = newShardNode(t, address, own)
		shards[i] = distribution.Shard{
			ID: id, AllocationGeneration: allocation, Range: keyRange,
			Leaders: []distribution.EndpointID{endpoint}, Epoch: epoch,
		}
		endpoints[endpoint] = address
		nodeIndex[address] = i
	}
	manifest, err := distribution.NewManifest(dist, version, shards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: dist, Arity: 1, MapperVersion: 1}},
		Placements: []distribution.TablePlacement{{
			Table: "messages", Distribution: dist, Columns: []string{"/tenant_id"},
		}},
		Manifests: []*distribution.Manifest{manifest},
	}
	snapshot, err := NewSnapshot(config, endpoints, 31)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	serverByAddress := make(map[string]shardNode, count)
	for i := range nodes {
		serverByAddress[nodes[i].address] = nodes[i]
	}
	wire := &segmentedFaultDialer{serve: func(address string, conn net.Conn) {
		serverByAddress[address].server.ServeConn(conn)
	}}
	client := NewClientWithOptions(wire.dial, ClientOptions{
		DisableConnectionReuse: true,
	})
	for i := range nodes {
		seed(t, client, nodes[i], seedStmt{
			sql: `CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		})
	}

	keys := make([]string, count)
	remaining := count
	mapper := distribution.NewNativeMapper(1)
	for candidate := 0; candidate < 1_000_000 && remaining != 0; candidate++ {
		key := fmt.Sprintf("wide-%d", candidate)
		mapped, mapErr := mapper.PointFor([]distribution.Scalar{distribution.NewString(key)})
		if mapErr != nil {
			t.Fatalf("PointFor: %v", mapErr)
		}
		id, ok := manifest.ResolvePoint(mapped)
		if !ok {
			t.Fatalf("manifest did not resolve mapped key %q", key)
		}
		index := nodeIndex[endpoints[shardsByID(t, shards, id).Leaders[0]]]
		if keys[index] == "" {
			keys[index] = key
			remaining--
		}
	}
	if remaining != 0 {
		t.Fatalf("failed to find routed keys for %d of %d shards", remaining, count)
	}
	return segmentedE2ECluster{
		nodes: nodes, keys: keys, client: client, snapshot: snapshot,
		servers: serverByAddress, wire: wire,
	}
}

func shardsByID(t *testing.T, shards []distribution.Shard, id distribution.ShardID) distribution.Shard {
	t.Helper()
	for i := range shards {
		if shards[i].ID == id {
			return shards[i]
		}
	}
	t.Fatalf("missing shard %s", id)
	return distribution.Shard{}
}

// TestSegmentedCoordinatorResponseLossAndRestartBoundaries drives the gateway's
// real coordinator stager through an actual shard server. It proves the atomic
// begin frame leaves both VTCM and page zero discoverable when its response is
// lost, and that a later page whose response is lost can be retried byte-for-
// byte after a process-style close/reopen boundary.
func TestSegmentedCoordinatorResponseLossAndRestartBoundaries(t *testing.T) {
	own := shardservice.Ownership{
		Distribution: "tenant_data", Shard: "s00000000",
		AllocationGeneration: 1, Epoch: 11, RoutingVersion: 7,
	}
	harness := newRestartableSegmentedShard(t, own)
	dialer := &segmentedFaultDialer{serve: func(_ string, conn net.Conn) { harness.serve(conn) }}
	client := NewClientWithOptions(dialer.dial, ClientOptions{DisableConnectionReuse: true})
	executor := NewExecutor(client, NewCatalogHolder(nil), Options{})
	profile := DefaultProfiles()[ClassAdmin].withDefaults()
	template := segmentedOwnedRequest(own)
	participant := transactionParticipant{call: shardCall{address: "coordinator", req: template}}
	stager := gatewayCoordinatorStager{
		executor: executor, ctx: t.Context(), coordinator: &participant, profile: profile,
	}

	record, pages, id := segmentedCoordinatorFixture(t, own, 2200)
	if len(pages) < 2 {
		t.Fatalf("manifest pages = %d, want at least two", len(pages))
	}
	dialer.dropNext("coordinator", shardservice.TransactionStageManifestCoordinator)
	if err := stager.stageManifestCoordinator(record, pages[0]); err == nil {
		t.Fatal("atomic coordinator begin unexpectedly received its response")
	}
	dialer.assertClean(t)

	lookup := transactionRequest(
		template, profile, shardservice.TransactionLookupCoordinator, id, 0, nil,
	)
	observed, err := executor.transactionRoundTrip(t.Context(), "coordinator", lookup, profile)
	if err != nil {
		t.Fatalf("lookup after lost begin response: %v", err)
	}
	if observed.Transaction.RecordKind != shardservice.TransactionRecordManifestCoordinator ||
		!bytes.Equal(observed.Transaction.Record, record) {
		t.Fatalf("lookup after lost begin = %+v", observed.Transaction)
	}
	assertSegmentedPage(t, executor, template, profile, id, 0, pages[0])

	// Exact begin replay converges before and after reopening the shard store.
	if err := stager.stageManifestCoordinator(record, pages[0]); err != nil {
		t.Fatalf("retry atomic begin: %v", err)
	}
	harness.restart(t)
	if err := stager.stageManifestCoordinator(record, pages[0]); err != nil {
		t.Fatalf("retry atomic begin after reopen: %v", err)
	}

	dialer.dropNext("coordinator", shardservice.TransactionStageManifestSegment)
	if err := stager.stageManifestSegment(id, 1, pages[1]); err == nil {
		t.Fatal("later manifest page unexpectedly received its response")
	}
	dialer.assertClean(t)
	harness.restart(t)
	if err := stager.stageManifestSegment(id, 1, pages[1]); err != nil {
		t.Fatalf("retry page one after reopen: %v", err)
	}
	assertSegmentedPage(t, executor, template, profile, id, 1, pages[1])

	for index := 2; index < len(pages); index++ {
		if err := stager.stageManifestSegment(id, uint32(index), pages[index]); err != nil {
			t.Fatalf("stage page %d: %v", index, err)
		}
	}
	commit := transactionRequest(
		template, profile, shardservice.TransactionCommitCoordinator, id, 1, nil,
	)
	committed, err := executor.transactionRoundTrip(t.Context(), "coordinator", commit, profile)
	if err != nil {
		t.Fatalf("commit complete manifest: %v", err)
	}
	if committed.Transaction.CoordinatorState != distributedtxn.CoordinatorCommitted {
		t.Fatalf("commit state = %v, want committed", committed.Transaction.CoordinatorState)
	}
}

func segmentedCoordinatorFixture(
	t *testing.T,
	own shardservice.Ownership,
	count int,
) ([]byte, [][]byte, distributedtxn.ID) {
	t.Helper()
	id := recoveryTestID(211)
	refs := testTransactionRefs(count)
	for i := range refs {
		refs[i].Distribution = []byte(own.Distribution)
	}
	refs[0].Shard = []byte(own.Shard)
	refs[0].RoutingVersion = uint64(own.RoutingVersion)
	refs[0].AllocationGeneration = uint64(own.AllocationGeneration)
	refs[0].OwnershipEpoch = uint64(own.Epoch)
	pages := make([][]byte, 0, 4)
	descriptor, err := buildTransactionManifest(
		refs, make([]byte, distributedtxn.ManifestSegmentBytes),
		func(segment distributedtxn.ManifestSegment) error {
			pages = append(pages, bytes.Clone(segment.Raw))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	record, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 31, Manifest: descriptor,
	})
	if err != nil {
		t.Fatalf("append manifest coordinator: %v", err)
	}
	return record, pages, id
}

func assertSegmentedPage(
	t *testing.T,
	executor *Executor,
	template *shardservice.ShardRequest,
	profile Profile,
	id distributedtxn.ID,
	index uint32,
	want []byte,
) {
	t.Helper()
	request := transactionRequest(
		template, profile, shardservice.TransactionReadManifestSegment, id, 0, nil,
	)
	request.ExecutionMode = shardservice.ExecutionReadOnly
	request.Transaction.SegmentIndex = index
	response, err := executor.transactionRoundTrip(t.Context(), "coordinator", request, profile)
	if err != nil {
		t.Fatalf("read manifest page %d: %v", index, err)
	}
	if response.Transaction.RecordKind != shardservice.TransactionRecordManifestSegment ||
		response.Transaction.SegmentIndex != index || !bytes.Equal(response.Transaction.Record, want) {
		t.Fatalf("manifest page %d = %+v", index, response.Transaction)
	}
}

func segmentedOwnedRequest(own shardservice.Ownership) *shardservice.ShardRequest {
	return &shardservice.ShardRequest{
		Distribution: own.Distribution, Shard: own.Shard,
		AllocationGeneration: own.AllocationGeneration,
		RoutingVersion:       own.RoutingVersion, OwnershipEpoch: own.Epoch,
		ExecutionMode: shardservice.ExecutionReadWrite,
	}
}

type restartableSegmentedShard struct {
	mu   sync.RWMutex
	path string
	own  shardservice.Ownership
	db   *sqldriver.Database
	srv  *shardservice.Server
}

func newRestartableSegmentedShard(
	t *testing.T,
	own shardservice.Ownership,
) *restartableSegmentedShard {
	t.Helper()
	h := &restartableSegmentedShard{path: filepath.Join(t.TempDir(), "segmented.vdb"), own: own}
	db, err := sqldriver.InitializeShardStore(h.path, sqldriver.ShardStoreBinding{
		Distribution: own.Distribution, Shard: own.Shard,
		AllocationGeneration: own.AllocationGeneration,
	})
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	h.db = db
	h.openServer(t)
	t.Cleanup(func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.srv != nil {
			_ = h.srv.Close()
		}
		if h.db != nil {
			_ = h.db.Close()
		}
	})
	return h
}

func (h *restartableSegmentedShard) openServer(t *testing.T) {
	t.Helper()
	srv, err := shardservice.NewServer(h.db, h.own, shardservice.Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h.srv = srv
}

func (h *restartableSegmentedShard) restart(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.srv.Close(); err != nil {
		t.Fatalf("close shard server: %v", err)
	}
	if err := h.db.Close(); err != nil {
		t.Fatalf("close shard database: %v", err)
	}
	db, err := sqldriver.OpenShardStore(h.path, sqldriver.ShardStoreBinding{
		Distribution: h.own.Distribution, Shard: h.own.Shard,
		AllocationGeneration: h.own.AllocationGeneration,
	})
	if err != nil {
		t.Fatalf("OpenShardStore: %v", err)
	}
	h.db = db
	h.openServer(t)
}

func (h *restartableSegmentedShard) serve(conn net.Conn) {
	h.mu.RLock()
	srv := h.srv
	h.mu.RUnlock()
	srv.ServeConn(conn)
}

type segmentedFaultDialer struct {
	mu    sync.Mutex
	serve func(address string, conn net.Conn)
	drops map[segmentedFaultKey]int
	calls map[segmentedFaultKey]int
	err   error
}

type segmentedFaultKey struct {
	address   string
	operation shardservice.TransactionOperation
}

func (d *segmentedFaultDialer) dropNext(
	address string,
	operation shardservice.TransactionOperation,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.drops == nil {
		d.drops = make(map[segmentedFaultKey]int)
	}
	d.drops[segmentedFaultKey{address: address, operation: operation}]++
}

func (d *segmentedFaultDialer) dial(_ context.Context, address string) (net.Conn, error) {
	client, proxy := net.Pipe()
	go d.proxy(address, proxy)
	return client, nil
}

func (d *segmentedFaultDialer) proxy(address string, front net.Conn) {
	defer front.Close()
	request, err := shardservice.DecodeRequest(front)
	if err != nil {
		d.recordError(err)
		return
	}
	backend, server := net.Pipe()
	go d.serve(address, server)
	if err = shardservice.EncodeRequest(backend, request); err == nil {
		var response *shardservice.ShardResponse
		response, err = shardservice.DecodeResponse(backend)
		if err == nil && !d.consumeDrop(address, request.Transaction.Operation) {
			err = shardservice.EncodeResponse(front, response)
		}
	}
	_ = backend.Close()
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		d.recordError(err)
	}
}

func (d *segmentedFaultDialer) consumeDrop(
	address string,
	operation shardservice.TransactionOperation,
) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.calls == nil {
		d.calls = make(map[segmentedFaultKey]int)
	}
	key := segmentedFaultKey{address: address, operation: operation}
	d.calls[key]++
	if d.drops[key] == 0 {
		return false
	}
	d.drops[key]--
	return true
}

func (d *segmentedFaultDialer) callSnapshot() map[segmentedFaultKey]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	copy := make(map[segmentedFaultKey]int, len(d.calls))
	for key, count := range d.calls {
		copy[key] = count
	}
	return copy
}

func (d *segmentedFaultDialer) totalCalls(operation shardservice.TransactionOperation) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := 0
	for key, count := range d.calls {
		if key.operation == operation {
			total += count
		}
	}
	return total
}

func (d *segmentedFaultDialer) recordError(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err == nil {
		d.err = err
	}
}

func (d *segmentedFaultDialer) assertClean(t *testing.T) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		t.Fatalf("fault proxy: %v", d.err)
	}
}
