package shardservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

// testOwner is the static identity every server test configures its shard with.
func testOwner() Ownership {
	return Ownership{
		Distribution:         "tenant_data",
		Shard:                "-80",
		AllocationGeneration: 1,
		Epoch:                7,
		RoutingVersion:       3,
	}
}

// ownedRequest returns a request that admits against testOwner, carrying sql.
func ownedRequest(sql string, params ...Param) *ShardRequest {
	own := testOwner()
	return &ShardRequest{
		SQL:                  sql,
		Params:               params,
		Distribution:         own.Distribution,
		Shard:                own.Shard,
		AllocationGeneration: own.AllocationGeneration,
		RoutingVersion:       own.RoutingVersion,
		OwnershipEpoch:       own.Epoch,
		ExecutionMode:        ExecutionReadWrite,
	}
}

// openDB opens a fresh durable catalog under path.
func openDB(t *testing.T, path string) *sqldriver.Database {
	t.Helper()
	owner := testOwner()
	binding := sqldriver.ShardStoreBinding{
		Distribution: owner.Distribution, Shard: owner.Shard,
		AllocationGeneration: owner.AllocationGeneration,
	}
	_, statErr := os.Stat(path)
	var db *sqldriver.Database
	var err error
	if os.IsNotExist(statErr) {
		db, err = sqldriver.InitializeShardStore(path, binding)
	} else if statErr != nil {
		t.Fatalf("stat shard store: %v", statErr)
	} else {
		db, err = sqldriver.OpenShardStore(path, binding)
	}
	if err != nil {
		t.Fatalf("open shard store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newServer builds a server over a fresh database and testOwner. It returns the
// server and its database so a test can restart against the same path.
func newServer(t *testing.T, opts Options) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shard.vdb")
	db := openDB(t, path)
	srv, err := NewServer(db, testOwner(), opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, path
}

// dial serves one in-process connection and returns the client half.
func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go srv.ServeConn(server)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type gatedCloseConn struct {
	net.Conn
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
	err     error
}

type gatedWriteConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *gatedWriteConn) Write(payload []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Write(payload)
}

func (c *gatedCloseConn) Close() error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
		c.err = c.Conn.Close()
	})
	return c.err
}

// roundTrip sends one request and reads one response over conn.
func roundTrip(t *testing.T, conn net.Conn, req *ShardRequest) *ShardResponse {
	t.Helper()
	if err := EncodeRequest(conn, req); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	resp, err := DecodeResponse(conn)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	return resp
}

// exec sends a statement expected to complete, failing on any error frame.
func exec(t *testing.T, conn net.Conn, req *ShardRequest) *ShardResponse {
	t.Helper()
	resp := roundTrip(t, conn, req)
	if resp.Kind == ResponseError {
		t.Fatalf("statement %q returned error frame: %s: %s",
			req.SQL, resp.ErrorKind, resp.ErrorMessage)
	}
	return resp
}

func ownedExchangeRequest(operation ExchangeOperation, key exchange.Key) *ShardRequest {
	req := ownedRequest("")
	req.Exchange = ExchangeRequest{Operation: operation, Key: key}
	if operation == ExchangePull {
		req.ExecutionMode = ExecutionReadOnly
	}
	return req
}

func TestExchangeWireLifecycleRedeliveryAndOwnershipFence(t *testing.T) {
	srv, _ := newServer(t, Options{
		MaxExchangeMailboxes: 2, MaxExchangeBufferBytes: 128,
	})
	conn := dial(t, srv)
	key := testExchangeKey(81)

	open := ownedExchangeRequest(ExchangeOpen, key)
	open.Exchange.Producers = 1
	open.Exchange.QueuedBatches = 2
	open.Exchange.ProducerBatches = 2
	open.Exchange.BufferedRows = 4
	open.Exchange.BufferedBytes = 64
	open.Exchange.TotalRows = 8
	open.Exchange.TotalBytes = 128
	opened := exec(t, conn, open)
	if opened.Exchange.Operation != ExchangeOpen {
		t.Fatalf("Open = %+v", opened)
	}
	// A transport retry recomputes its local absolute deadline but returns the
	// already admitted mailbox without changing its original lifetime.
	open.Deadline = time.Second
	if got := exec(t, conn, open); got.Exchange.Operation != ExchangeOpen {
		t.Fatalf("Open retry = %+v", got)
	}

	push := ownedExchangeRequest(ExchangePush, key)
	push.Exchange.Batch = exchange.Batch{Rows: 1, Data: []byte("first")}
	if got := exec(t, conn, push); got.Exchange.Operation != ExchangePush {
		t.Fatalf("Push = %+v", got)
	}
	if got := exec(t, conn, push); got.Exchange.Operation != ExchangePush {
		t.Fatalf("Push retry = %+v", got)
	}
	push.Exchange.Batch = exchange.Batch{
		Sequence: 1, Rows: 1, Data: []byte("second"), Final: true,
	}
	if got := exec(t, conn, push); got.Exchange.Operation != ExchangePush {
		t.Fatalf("second Push = %+v", got)
	}

	pull := ownedExchangeRequest(ExchangePull, key)
	first := exec(t, conn, pull)
	if first.Exchange.Operation != ExchangePull || first.Exchange.EOF ||
		first.Exchange.Batch.Sequence != 0 || string(first.Exchange.Batch.Data) != "first" {
		t.Fatalf("first Pull = %+v", first)
	}
	repeated := exec(t, conn, pull)
	if repeated.Exchange.Batch.Sequence != first.Exchange.Batch.Sequence ||
		string(repeated.Exchange.Batch.Data) != "first" {
		t.Fatalf("unacked Pull = %+v, want redelivery", repeated)
	}
	pull.Exchange.HasAck = true
	pull.Exchange.AckProducer = first.Exchange.Batch.Producer
	pull.Exchange.AckSequence = first.Exchange.Batch.Sequence
	second := exec(t, conn, pull)
	if second.Exchange.Batch.Sequence != 1 || string(second.Exchange.Batch.Data) != "second" ||
		!second.Exchange.Batch.Final {
		t.Fatalf("acked Pull = %+v", second)
	}
	pull.Exchange.AckSequence = second.Exchange.Batch.Sequence
	done := exec(t, conn, pull)
	if !done.Exchange.EOF {
		t.Fatalf("drained Pull = %+v, want EOF", done)
	}
	if retry := exec(t, conn, pull); !retry.Exchange.EOF {
		t.Fatalf("EOF ack retry = %+v, want EOF", retry)
	}

	conflict := *open
	conflict.Exchange.TotalRows++
	if got := roundTrip(t, conn, &conflict); got.Kind != ResponseError || got.ErrorKind != ErrorExchangeConflict {
		t.Fatalf("conflicting Open = %+v", got)
	}

	staleKey := testExchangeKey(91)
	stale := ownedExchangeRequest(ExchangeOpen, staleKey)
	stale.Exchange.Producers = 1
	stale.Exchange.QueuedBatches = 1
	stale.Exchange.ProducerBatches = 1
	stale.Exchange.BufferedRows = 1
	stale.Exchange.BufferedBytes = 1
	stale.Exchange.TotalRows = 1
	stale.Exchange.TotalBytes = 1
	stale.OwnershipEpoch++
	if got := roundTrip(t, conn, stale); got.Kind != ResponseError || got.ErrorKind != ErrorOwnershipEpoch {
		t.Fatalf("stale Open = %+v", got)
	}
	probe := ownedExchangeRequest(ExchangePush, staleKey)
	probe.Exchange.Batch = exchange.Batch{Final: true}
	if got := roundTrip(t, conn, probe); got.Kind != ResponseError || got.ErrorKind != ErrorExchangeNotFound {
		t.Fatalf("stale Open changed registry: %+v", got)
	}

	cancel := ownedExchangeRequest(ExchangeCancel, key)
	if got := exec(t, conn, cancel); got.Exchange.Operation != ExchangeCancel {
		t.Fatalf("Cancel = %+v", got)
	}
	if got := exec(t, conn, cancel); got.Exchange.Operation != ExchangeCancel {
		t.Fatalf("Cancel retry = %+v", got)
	}
	if got := roundTrip(t, conn, push); got.Kind != ResponseError || got.ErrorKind != ErrorExchangeNotFound {
		t.Fatalf("post-Cancel Push = %+v", got)
	}
}

func TestDirectRepartitionPushesShardCursorToDestinationWorkers(t *testing.T) {
	destination, _ := newServer(t, Options{})
	source, _ := newServer(t, Options{
		ExchangeDial: func(_ context.Context, address []byte) (net.Conn, error) {
			if string(address) != "destination" {
				return nil, fmt.Errorf("unexpected exchange address %q", address)
			}
			client, server := net.Pipe()
			go destination.ServeConn(server)
			return client, nil
		},
	})
	destinationConn := dial(t, destination)
	sourceConn := dial(t, source)

	exec(t, sourceConn, ownedRequest(`CREATE TABLE docs (
		id STRING PRIMARY KEY, tenant STRING NOT NULL, n INTEGER NOT NULL)`))
	for _, row := range [][3]string{
		{"1", "alpha", "1"}, {"2", "alpha", "2"}, {"3", "beta", "3"},
		{"4", "gamma", "4"}, {"5", "gamma", "5"},
	} {
		exec(t, sourceConn, ownedRequest(
			`INSERT INTO docs (id, tenant, n) VALUES (?, ?, ?)`,
			StringParam(row[0]), StringParam(row[1]), NumberParam(row[2]),
		))
	}

	const partitions = 4
	key := testExchangeKey(111)
	key.Stage, key.Attempt = 6, 2
	for partition := uint32(0); partition < partitions; partition++ {
		open := ownedExchangeRequest(ExchangeOpen, key)
		open.Exchange.Key.Partition = partition
		open.Exchange.Producers = 1
		open.Exchange.QueuedBatches = 8
		open.Exchange.ProducerBatches = 8
		open.Exchange.BufferedRows = 64
		open.Exchange.BufferedBytes = 64 << 10
		open.Exchange.TotalRows = 128
		open.Exchange.TotalBytes = 128 << 10
		exec(t, destinationConn, open)
	}

	owner := testOwner()
	targets := make([]RepartitionTarget, partitions)
	for partition := range targets {
		targets[partition] = RepartitionTarget{
			Address: []byte("destination"), Distribution: owner.Distribution, Shard: owner.Shard,
			AllocationGeneration: owner.AllocationGeneration,
			RoutingVersion:       owner.RoutingVersion, OwnershipEpoch: owner.Epoch,
		}
	}
	repartition := ownedRequest(`SELECT tenant, COUNT(*) FROM docs GROUP BY tenant`)
	repartition.ExecutionMode = ExecutionReadOnly
	repartition.PartialAggregate = true
	repartition.MaxRows = 64
	repartition.MaxResultBytes = 1 << 20
	repartition.Repartition = RepartitionRequest{
		Operation: key.Operation, Stage: key.Stage, Attempt: key.Attempt,
		KeyColumns: []uint16{0}, Targets: targets,
		BlockRows: 2, BlockBytes: 256, MaxMemory: partitions * 256,
	}
	response := exec(t, sourceConn, repartition)
	if response.Kind != ResponseCompletion || response.RowsAffected != 3 {
		t.Fatalf("repartition response = %+v, want three produced groups", response)
	}

	partitioner := &repartitionPartitioner{columns: []uint16{0}, partitions: partitions}
	groups := make(map[string]string)
	for partition := uint32(0); partition < partitions; partition++ {
		pull := ownedExchangeRequest(ExchangePull, key)
		pull.Exchange.Key.Partition = partition
		for {
			batch := exec(t, destinationConn, pull).Exchange
			if batch.EOF {
				break
			}
			pull.Exchange.HasAck = true
			pull.Exchange.AckProducer = batch.Batch.Producer
			pull.Exchange.AckSequence = batch.Batch.Sequence
			if batch.Batch.Rows == 0 {
				continue
			}
			block, err := exchange.OpenBlock(batch.Batch.Data)
			if err != nil || block.Rows() != batch.Batch.Rows || block.Columns() != 2 {
				t.Fatalf("partition %d block = %dx%d, %v", partition, block.Rows(), block.Columns(), err)
			}
			row := make([]exchange.Cell, 2)
			for block.NextInto(row) {
				actual, err := partitioner.Partition(row)
				if err != nil || actual != partition {
					t.Fatalf("partition %d row mapped to %d, %v", partition, actual, err)
				}
				groups[string(row[0].Bytes)] = string(row[1].Bytes)
			}
		}
	}
	for group, count := range map[string]string{`"alpha"`: "2", `"beta"`: "1", `"gamma"`: "2"} {
		if groups[group] != count {
			t.Fatalf("group %s count = %s, want %s (all %v)", group, groups[group], count, groups)
		}
	}
}

func TestExchangeReducerCombinesPartitionLocallyAndRetriesAfterCompletion(t *testing.T) {
	server, _ := newServer(t, Options{})
	conn := dial(t, server)
	inputKey := testExchangeKey(121)
	inputKey.Stage, inputKey.Partition, inputKey.Attempt = 4, 2, 3
	outputKey := inputKey
	outputKey.Stage++

	openBox := func(key exchange.Key, producers uint16) {
		req := ownedExchangeRequest(ExchangeOpen, key)
		req.Exchange.Producers = producers
		req.Exchange.QueuedBatches = 8
		req.Exchange.ProducerBatches = 8
		req.Exchange.BufferedRows = 64
		req.Exchange.BufferedBytes = 64 << 10
		req.Exchange.TotalRows = 128
		req.Exchange.TotalBytes = 128 << 10
		exec(t, conn, req)
	}
	openBox(inputKey, 2)
	openBox(outputKey, 1)

	pushRows := func(producer uint16, rows [][]exchange.Cell) {
		var block exchange.BlockBuilder
		if err := block.Reset(nil, 5, 16, 4<<10); err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			if err := block.AppendRow(rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		data, count := block.Bytes()
		req := ownedExchangeRequest(ExchangePush, inputKey)
		req.Exchange.Batch = exchange.Batch{
			Producer: producer, Rows: count, Data: data, Final: true,
		}
		exec(t, conn, req)
	}
	pushRows(0, [][]exchange.Cell{
		{{Bytes: []byte(`1`)}, {Bytes: []byte(`2`)}, {Bytes: []byte(`5`)}, {Bytes: []byte(`2`)}, {Bytes: []byte(`3`)}},
		{{Bytes: []byte(`2`)}, {Bytes: []byte(`1`)}, {Bytes: []byte(`4`)}, {Bytes: []byte(`4`)}, {Bytes: []byte(`4`)}},
	})
	pushRows(1, [][]exchange.Cell{
		{{Bytes: []byte(`1.0`)}, {Bytes: []byte(`3`)}, {Bytes: []byte(`7`)}, {Bytes: []byte(`1`)}, {Bytes: []byte(`10`)}},
	})

	reduce := ownedExchangeRequest(ExchangeReduce, inputKey)
	reduce.Exchange.Output = outputKey
	reduce.Exchange.Kinds = []distributedagg.Kind{
		distributedagg.None, distributedagg.Count, distributedagg.Sum,
		distributedagg.Min, distributedagg.Max,
	}
	reduce.Exchange.GroupKeys = []uint16{0}
	reduce.Exchange.MaxStateBytes = 1 << 20
	reduce.Exchange.BlockRows = 16
	reduce.Exchange.BlockBytes = 4 << 10
	if got := exec(t, conn, reduce); got.Exchange.Operation != ExchangeReduce {
		t.Fatalf("Reduce = %+v", got)
	}
	// The input is drained, so this proves retry success comes from the output
	// terminal marker rather than recomputing or duplicating rows.
	if got := exec(t, conn, reduce); got.Exchange.Operation != ExchangeReduce {
		t.Fatalf("Reduce retry = %+v", got)
	}

	pull := ownedExchangeRequest(ExchangePull, outputKey)
	response := exec(t, conn, pull)
	if response.Exchange.EOF || !response.Exchange.Batch.Final {
		t.Fatalf("output = %+v", response.Exchange)
	}
	block, err := exchange.OpenBlock(response.Exchange.Batch.Data)
	if err != nil || block.Rows() != 2 || block.Columns() != 5 {
		t.Fatalf("output block = %dx%d, %v", block.Rows(), block.Columns(), err)
	}
	got := make(map[string][4]string)
	row := make([]exchange.Cell, 5)
	for block.NextInto(row) {
		got[string(row[0].Bytes)] = [4]string{
			string(row[1].Bytes), string(row[2].Bytes),
			string(row[3].Bytes), string(row[4].Bytes),
		}
	}
	if got["1"] != [4]string{"5", "12", "1", "10"} ||
		got["2"] != [4]string{"1", "4", "4", "4"} {
		t.Fatalf("reduced groups = %v", got)
	}
}

func TestExchangeReducerLimitIsTypedResourceLimit(t *testing.T) {
	response := reducerError(distributedagg.ErrLimit)
	if response.Kind != ResponseError || response.ErrorKind != ErrorResourceLimit {
		t.Fatalf("reducer limit = %+v, want ResourceLimit error frame", response)
	}
	response = reducerError(distributedagg.ErrAggregate)
	if response.Kind != ResponseError || response.ErrorKind == ErrorResourceLimit {
		t.Fatalf("malformed reducer fragment = %+v, want non-limit error frame", response)
	}
}

func TestRepartitionDefaultDialIsLoopbackOnly(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9000", "[::1]:9000", "localhost:9000"} {
		if !loopbackExchangeAddress([]byte(address)) {
			t.Fatalf("loopback address %q was rejected", address)
		}
	}
	for _, address := range []string{"10.0.0.1:9000", "example.com:9000", "destination", "127.0.0.1"} {
		if loopbackExchangeAddress([]byte(address)) {
			t.Fatalf("non-loopback/invalid address %q was admitted", address)
		}
	}
}

// cellText returns the JSON bytes of one non-null cell as a string.
func cellText(t *testing.T, resp *ShardResponse, row, col int) string {
	t.Helper()
	if resp.Kind != ResponseRows {
		t.Fatalf("response kind = %s, want Rows", resp.Kind)
	}
	if row >= len(resp.Rows) || col >= len(resp.Rows[row]) {
		t.Fatalf("cell (%d,%d) out of range for %dx%d result",
			row, col, len(resp.Rows), len(resp.Columns))
	}
	c := resp.Rows[row][col]
	if c.Null {
		t.Fatalf("cell (%d,%d) is null", row, col)
	}
	return string(c.Bytes)
}

const ddlDocs = `CREATE TABLE docs (id STRING PRIMARY KEY, name STRING NOT NULL, n INTEGER)`

func TestPartialAggregateFragmentRemovesFinalOrderAndLimit(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	for _, row := range []struct {
		id string
		n  string
	}{{"a", "1"}, {"b", "1"}, {"c", "2"}} {
		exec(t, conn, ownedRequest(
			`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
			StringParam(row.id), StringParam(row.id), NumberParam(row.n),
		))
	}

	const statement = `SELECT n, COUNT(*) FROM docs GROUP BY n ORDER BY n LIMIT ? OFFSET 1`
	final := ownedRequest(statement, NumberParam("1"))
	final.ExecutionMode = ExecutionReadOnly
	if got := exec(t, conn, final); len(got.Rows) != 1 {
		t.Fatalf("final shard rows = %d, want 1", len(got.Rows))
	}

	partial := ownedRequest(statement, NumberParam("1"))
	partial.ExecutionMode = ExecutionReadOnly
	partial.PartialAggregate = true
	got := exec(t, conn, partial)
	if len(got.Rows) != 2 {
		t.Fatalf("partial shard rows = %d, want every 2 groups", len(got.Rows))
	}
	counts := map[string]string{}
	for i := range got.Rows {
		counts[cellText(t, got, i, 0)] = cellText(t, got, i, 1)
	}
	if counts["1"] != "2" || counts["2"] != "1" {
		t.Fatalf("partial groups = %v, want map[1:2 2:1]", counts)
	}
}

func TestPartialAggregateFragmentRetainsLocalDistinct(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	for _, row := range []struct {
		id string
		n  string
	}{{"a", "1"}, {"b", "1"}, {"c", "2"}} {
		exec(t, conn, ownedRequest(
			`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
			StringParam(row.id), StringParam(row.id), NumberParam(row.n),
		))
	}

	req := ownedRequest(`SELECT DISTINCT n FROM docs ORDER BY n LIMIT 1`)
	req.ExecutionMode = ExecutionReadOnly
	req.PartialAggregate = true
	got := exec(t, conn, req)
	if len(got.Rows) != 2 || cellText(t, got, 0, 0) != "1" || cellText(t, got, 1, 0) != "2" {
		t.Fatalf("partial DISTINCT rows = %+v, want [1 2]", got.Rows)
	}
}

func TestTransactionStageAndLookupAreDurableAndIdempotent(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	owner := testOwner()
	id := testTransactionID(11)
	record, err := distributedtxn.AppendTarget(nil, distributedtxn.TargetRecord{
		ID: id, State: distributedtxn.TargetStaged, Revision: 1,
		RoutingVersion:       uint64(owner.RoutingVersion),
		AllocationGeneration: uint64(owner.AllocationGeneration),
		OwnershipEpoch:       uint64(owner.Epoch), CoordinatorDistribution: []byte(owner.Distribution), CoordinatorShard: []byte(owner.Shard),
		CoordinatorAllocation:     uint64(owner.AllocationGeneration),
		CoordinatorRoutingVersion: uint64(owner.RoutingVersion), CoordinatorOwnershipEpoch: uint64(owner.Epoch),
		MutationDigest: testTransactionDigest(55), Mutation: []byte("compact-mutation"),
	})
	if err != nil {
		t.Fatalf("AppendParticipant: %v", err)
	}
	stage := ownedRequest("")
	stage.Transaction = TransactionRequest{Operation: TransactionStageTarget, Record: record}
	first := exec(t, conn, stage)
	if first.Transaction.Role != TransactionRoleTarget ||
		first.Transaction.ID != id || first.Transaction.Revision != 1 ||
		first.Transaction.TargetState != distributedtxn.TargetStaged {
		t.Fatalf("stage response = %+v", first)
	}
	// A lost stage response is retried byte-for-byte and resolves to the same
	// durable state without another journal entry.
	second := exec(t, conn, stage)
	if !second.Transaction.Equal(first.Transaction) {
		t.Fatalf("duplicate stage = %+v, want %+v", second.Transaction, first.Transaction)
	}
	lookup := ownedRequest("")
	lookup.Transaction = TransactionRequest{Operation: TransactionLookupTarget, ID: id}
	observed := exec(t, conn, lookup)
	if !observed.Transaction.Equal(first.Transaction) {
		t.Fatalf("lookup = %+v, want %+v", observed.Transaction, first.Transaction)
	}
}

func TestTransactionStageSurvivesShardRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.vdb")
	owner := testOwner()
	id := testTransactionID(21)
	record, err := distributedtxn.AppendTarget(nil, distributedtxn.TargetRecord{
		ID: id, State: distributedtxn.TargetStaged, Revision: 1,
		RoutingVersion:       uint64(owner.RoutingVersion),
		AllocationGeneration: uint64(owner.AllocationGeneration),
		OwnershipEpoch:       uint64(owner.Epoch), CoordinatorDistribution: []byte(owner.Distribution), CoordinatorShard: []byte(owner.Shard),
		CoordinatorAllocation:     uint64(owner.AllocationGeneration),
		CoordinatorRoutingVersion: uint64(owner.RoutingVersion), CoordinatorOwnershipEpoch: uint64(owner.Epoch),
		MutationDigest: testTransactionDigest(77), Mutation: []byte("restart-mutation"),
	})
	if err != nil {
		t.Fatalf("AppendParticipant: %v", err)
	}

	db := openDB(t, path)
	srv, err := NewServer(db, owner, Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	conn := dial(t, srv)
	stage := ownedRequest("")
	stage.Transaction = TransactionRequest{Operation: TransactionStageTarget, Record: record}
	want := exec(t, conn, stage).Transaction
	_ = conn.Close()
	if err := srv.Close(); err != nil {
		t.Fatalf("first server Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first database Close: %v", err)
	}

	db = openDB(t, path)
	srv, err = NewServer(db, owner, Options{})
	if err != nil {
		t.Fatalf("restart NewServer: %v", err)
	}
	defer srv.Close()
	conn = dial(t, srv)
	lookup := ownedRequest("")
	lookup.Transaction = TransactionRequest{Operation: TransactionLookupTarget, ID: id}
	if got := exec(t, conn, lookup).Transaction; !got.Equal(want) {
		t.Fatalf("recovered transaction = %+v, want %+v", got, want)
	}
}

func TestTransactionApplyPublishesMutationAndTargetStateTogether(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	owner := testOwner()
	id := testTransactionID(31)
	mutation, err := AppendMutationBatch(nil, []MutationStatement{{
		SQL:    `INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		Params: []Param{StringParam("atomic"), StringParam("published"), NumberParam("7")},
	}})
	if err != nil {
		t.Fatalf("AppendMutationBatch: %v", err)
	}
	digest := distributedtxn.TargetDigest(0, nil, mutation)
	record, err := distributedtxn.AppendTarget(nil, distributedtxn.TargetRecord{
		ID: id, State: distributedtxn.TargetStaged, Revision: 1,
		RoutingVersion:       uint64(owner.RoutingVersion),
		AllocationGeneration: uint64(owner.AllocationGeneration),
		OwnershipEpoch:       uint64(owner.Epoch), CoordinatorDistribution: []byte(owner.Distribution), CoordinatorShard: []byte(owner.Shard),
		CoordinatorAllocation:     uint64(owner.AllocationGeneration),
		CoordinatorRoutingVersion: uint64(owner.RoutingVersion), CoordinatorOwnershipEpoch: uint64(owner.Epoch),
		MutationDigest: digest, Mutation: mutation,
	})
	if err != nil {
		t.Fatalf("AppendParticipant: %v", err)
	}
	stage := ownedRequest("")
	stage.Transaction = TransactionRequest{Operation: TransactionStageTarget, Record: record}
	exec(t, conn, stage)
	prepare := ownedRequest("")
	prepare.Transaction = TransactionRequest{
		Operation: TransactionPrepareTarget, ID: id, Revision: 1,
	}
	prepared := exec(t, conn, prepare)
	if prepared.Transaction.TargetState != distributedtxn.TargetPrepared ||
		prepared.Transaction.Revision != 2 {
		t.Fatalf("prepare = %+v", prepared)
	}
	apply := ownedRequest("")
	apply.Transaction = TransactionRequest{
		Operation: TransactionApplyTarget, ID: id, Revision: 2,
	}
	applied := exec(t, conn, apply)
	if applied.RowsAffected != 1 ||
		applied.Transaction.TargetState != distributedtxn.TargetApplied ||
		applied.Transaction.Revision != 3 {
		t.Fatalf("apply = %+v", applied)
	}
	// A response-lost retry resolves the retained SQL-atomic outcome and cannot
	// execute the INSERT a second time.
	retried := exec(t, conn, apply)
	if retried.RowsAffected != 1 || !retried.Transaction.Equal(applied.Transaction) {
		t.Fatalf("retried apply = %+v, want %+v", retried, applied)
	}
	blockedConn := dial(t, srv)
	blockedResult := make(chan *ShardResponse, 1)
	blockedErr := make(chan error, 1)
	go func() {
		query := ownedRequest(`SELECT name, n FROM docs WHERE id = ?`, StringParam("atomic"))
		if err := EncodeRequest(blockedConn, query); err != nil {
			blockedErr <- err
			return
		}
		response, err := DecodeResponse(blockedConn)
		if err != nil {
			blockedErr <- err
			return
		}
		blockedResult <- response
	}()
	select {
	case result := <-blockedResult:
		t.Fatalf("read crossed an applied-but-unreleased participant barrier: %+v", result)
	case err := <-blockedErr:
		t.Fatalf("blocked read failed early: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	release := ownedRequest("")
	release.Transaction = TransactionRequest{
		Operation: TransactionReleaseTarget, ID: id, Revision: 3,
	}
	released := exec(t, conn, release)
	if released.Transaction.TargetState != distributedtxn.TargetReleased ||
		released.Transaction.Revision != 4 {
		t.Fatalf("release = %+v", released)
	}
	lookup := ownedRequest("")
	lookup.Transaction = TransactionRequest{Operation: TransactionLookupTarget, ID: id}
	if retained := exec(t, conn, lookup); retained.RowsAffected != 1 ||
		!retained.Transaction.Equal(released.Transaction) {
		t.Fatalf("released lookup = %+v, want transaction %+v", retained, released.Transaction)
	}
	if afterRelease := exec(t, conn, apply); afterRelease.RowsAffected != 1 ||
		!afterRelease.Transaction.Equal(released.Transaction) {
		t.Fatalf("apply retry after release = %+v, want retained release", afterRelease)
	}
	var query *ShardResponse
	select {
	case query = <-blockedResult:
	case err := <-blockedErr:
		t.Fatalf("released read: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("released participant did not wake blocked read")
	}
	if query.Kind != ResponseRows || len(query.Rows) != 1 ||
		cellText(t, query, 0, 0) != `"published"` || cellText(t, query, 0, 1) != "7" {
		t.Fatalf("published row = %+v", query)
	}
}

func TestScopedTargetBlocksOnlyIntersectingTraffic(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("visible"), StringParam("row"), NumberParam("1"),
	))
	owner := testOwner()
	id := testTransactionID(91)
	mutation, err := AppendMutationBatch(nil, []MutationStatement{{
		SQL: `DELETE FROM docs WHERE id = ?`, Params: []Param{StringParam("other")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	scopes := []distributedtxn.IntentScope{{Start: 10, End: 11}}
	record, err := distributedtxn.AppendTarget(nil, distributedtxn.TargetRecord{
		ID: id, State: distributedtxn.TargetStaged, Revision: 1,
		RoutingVersion: uint64(owner.RoutingVersion), AllocationGeneration: uint64(owner.AllocationGeneration),
		OwnershipEpoch:          uint64(owner.Epoch),
		CoordinatorDistribution: []byte(owner.Distribution), CoordinatorShard: []byte(owner.Shard),
		CoordinatorAllocation:     uint64(owner.AllocationGeneration),
		CoordinatorRoutingVersion: uint64(owner.RoutingVersion), CoordinatorOwnershipEpoch: uint64(owner.Epoch),
		BucketBits: 8, IntentScopes: scopes,
		MutationDigest: distributedtxn.TargetDigest(8, scopes, mutation), Mutation: mutation,
	})
	if err != nil {
		t.Fatal(err)
	}
	stage := ownedRequest("")
	stage.Transaction = TransactionRequest{Operation: TransactionStageTarget, Record: record}
	exec(t, conn, stage)
	disjointFenceID := testTransactionID(92)
	disjointFence := ownedRequest("")
	disjointFence.BucketBits = 8
	disjointFence.AccessScopes = []distributedtxn.IntentScope{{Start: 20, End: 21}}
	disjointFence.Transaction = TransactionRequest{
		Operation: TransactionAcquireReadFence, ID: disjointFenceID, Revision: 1,
	}
	exec(t, conn, disjointFence)
	disjointFence.AccessScopes = nil
	disjointFence.BucketBits = 0
	disjointFence.Transaction.Operation = TransactionReleaseReadFence
	exec(t, conn, disjointFence)
	overlappingFence := ownedRequest("")
	overlappingFence.BucketBits = 8
	overlappingFence.AccessScopes = scopes
	overlappingFence.Transaction = TransactionRequest{
		Operation: TransactionAcquireReadFence, ID: testTransactionID(93), Revision: 1,
	}
	if response := roundTrip(t, conn, overlappingFence); response.Kind != ResponseError ||
		response.ErrorKind != ErrorReadFenceBusy {
		t.Fatalf("fence over participant = %+v, want read-fence busy", response)
	}

	disjoint := ownedRequest(`SELECT n FROM docs WHERE id = ?`, StringParam("visible"))
	disjoint.BucketBits = 8
	disjoint.AccessScopes = []distributedtxn.IntentScope{{Start: 20, End: 21}}
	if response := exec(t, conn, disjoint); response.Kind != ResponseRows || len(response.Rows) != 1 {
		t.Fatalf("disjoint read = %+v", response)
	}

	blockedConn := dial(t, srv)
	blocked := make(chan *ShardResponse, 1)
	blockedErr := make(chan error, 1)
	go func() {
		request := ownedRequest(`SELECT n FROM docs WHERE id = ?`, StringParam("visible"))
		request.BucketBits = 8
		request.AccessScopes = []distributedtxn.IntentScope{{Start: 10, End: 11}}
		if err := EncodeRequest(blockedConn, request); err != nil {
			blockedErr <- err
			return
		}
		response, err := DecodeResponse(blockedConn)
		if err != nil {
			blockedErr <- err
			return
		}
		blocked <- response
	}()
	select {
	case response := <-blocked:
		t.Fatalf("intersecting read crossed intent: %+v", response)
	case err := <-blockedErr:
		t.Fatalf("intersecting read failed early: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	abort := ownedRequest("")
	abort.Transaction = TransactionRequest{
		Operation: TransactionAbortTarget, ID: id, Revision: 1,
	}
	exec(t, conn, abort)
	select {
	case response := <-blocked:
		if response.Kind != ResponseRows || len(response.Rows) != 1 {
			t.Fatalf("released read = %+v", response)
		}
	case err := <-blockedErr:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("intersecting read did not resume after abort")
	}
}

func TestWriteAdmissionReleasedBeforeTerminalResponse(t *testing.T) {
	srv, _ := newServer(t, Options{})
	setup := dial(t, srv)
	exec(t, setup, ownedRequest(ddlDocs))

	client, server := net.Pipe()
	gated := &gatedWriteConn{
		Conn: server, entered: make(chan struct{}), release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(gated.release) }) }
	t.Cleanup(func() {
		releaseWrite()
		_ = client.Close()
	})
	go srv.ServeConn(gated)
	request := ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("response-boundary"), StringParam("visible"), NumberParam("1"),
	)
	if err := EncodeRequest(client, request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gated.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write response did not reach the transport boundary")
	}

	fenceID := testTransactionID(122)
	if err := srv.readFences.acquire(fenceID, time.Second, 0, nil); err != nil {
		t.Fatalf("terminal response retained write admission: %v", err)
	}
	defer srv.readFences.release(fenceID)
	releaseWrite()
	response, err := DecodeResponse(client)
	if err != nil {
		t.Fatal(err)
	}
	if response.Kind != ResponseCompletion || response.RowsAffected != 1 {
		t.Fatalf("write response = %+v", response)
	}
}

func TestCoherentReadFenceBlocksOnlyIntersectingWrites(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("visible"), StringParam("row"), NumberParam("1"),
	))
	id := testTransactionID(121)
	scope := []distributedtxn.IntentScope{{Start: 10, End: 11}}
	acquire := ownedRequest("")
	acquire.BucketBits = 8
	acquire.AccessScopes = scope
	acquire.Deadline = time.Second
	acquire.Transaction = TransactionRequest{
		Operation: TransactionAcquireReadFence, ID: id, Revision: 1,
	}
	exec(t, conn, acquire)

	read := ownedRequest(`SELECT n FROM docs WHERE id = ?`, StringParam("visible"))
	read.ExecutionMode = ExecutionReadOnly
	read.BucketBits = 8
	read.AccessScopes = scope
	read.ReadFenceID = id
	if response := exec(t, conn, read); response.Kind != ResponseRows || len(response.Rows) != 1 {
		t.Fatalf("fenced read = %+v", response)
	}
	widened := ownedRequest(`SELECT n FROM docs`)
	widened.ExecutionMode = ExecutionReadOnly
	widened.ReadFenceID = id
	if response := roundTrip(t, conn, widened); response.Kind != ResponseError ||
		response.ErrorKind != ErrorTransactionConflict {
		t.Fatalf("widened fenced read = %+v, want transaction conflict", response)
	}

	disjoint := ownedRequest(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
		DocumentParam(`{"id":"visible","name":"row","n":2}`), StringParam("visible"))
	disjoint.BucketBits = 8
	disjoint.AccessScopes = []distributedtxn.IntentScope{{Start: 20, End: 21}}
	if response := exec(t, conn, disjoint); response.RowsAffected != 1 {
		t.Fatalf("disjoint write = %+v", response)
	}

	blockedConn := dial(t, srv)
	blocked := make(chan *ShardResponse, 1)
	blockedErr := make(chan error, 1)
	go func() {
		request := ownedRequest(`UPDATE docs SET "$doc" = ? WHERE id = ?`,
			DocumentParam(`{"id":"visible","name":"row","n":3}`), StringParam("visible"))
		request.BucketBits = 8
		request.AccessScopes = scope
		if err := EncodeRequest(blockedConn, request); err != nil {
			blockedErr <- err
			return
		}
		response, err := DecodeResponse(blockedConn)
		if err != nil {
			blockedErr <- err
			return
		}
		blocked <- response
	}()
	select {
	case response := <-blocked:
		t.Fatalf("intersecting write crossed fence: %+v", response)
	case err := <-blockedErr:
		t.Fatalf("intersecting write failed early: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	release := ownedRequest("")
	release.Transaction = TransactionRequest{
		Operation: TransactionReleaseReadFence, ID: id, Revision: 1,
	}
	exec(t, conn, release)
	select {
	case response := <-blocked:
		if response.Kind != ResponseCompletion || response.RowsAffected != 1 {
			t.Fatalf("released write = %+v", response)
		}
	case err := <-blockedErr:
		t.Fatalf("released write failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("released fence did not wake intersecting write")
	}
}

// TestServerExecuteLifecycle drives the full request lifecycle over one
// connection: DDL, DML, and a parameterized SELECT that streams back rows.
func TestServerExecuteLifecycle(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	create := exec(t, conn, ownedRequest(ddlDocs))
	if create.Kind != ResponseCompletion {
		t.Fatalf("CREATE TABLE kind = %s, want Completion", create.Kind)
	}

	insert := exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	if insert.Kind != ResponseCompletion || insert.RowsAffected != 1 {
		t.Fatalf("INSERT = %+v, want completion affecting 1 row", insert)
	}
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("b"), StringParam("Grace"), NumberParam("9")))

	sel := exec(t, conn, ownedRequest(
		`SELECT id, name, n FROM docs WHERE n >= ? ORDER BY id`,
		NumberParam("7")))
	if sel.Kind != ResponseRows {
		t.Fatalf("SELECT kind = %s, want Rows", sel.Kind)
	}
	wantCols := []string{"id", "name", "n"}
	if len(sel.Columns) != len(wantCols) {
		t.Fatalf("columns = %d, want %d", len(sel.Columns), len(wantCols))
	}
	for i, name := range wantCols {
		if sel.Columns[i].Name != name {
			t.Fatalf("column %d = %q, want %q", i, sel.Columns[i].Name, name)
		}
		if sel.Columns[i].TypeOID != pgOIDJSON {
			t.Fatalf("column %d OID = %d, want %d", i, sel.Columns[i].TypeOID, pgOIDJSON)
		}
	}
	if len(sel.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(sel.Rows))
	}
	if got := cellText(t, sel, 0, 0); got != `"a"` {
		t.Fatalf("row 0 id = %s, want \"a\"", got)
	}
	if got := cellText(t, sel, 1, 2); got != `9` {
		t.Fatalf("row 1 n = %s, want 9", got)
	}
}

func TestServerGlobalIndexLookupUsesRawBoundedLane(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(`CREATE TABLE messages_by_email (PRIMARY KEY (id))`))

	ctx := context.Background()
	session, err := srv.db.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	key := []byte{1, 5, 'a', '@', 'b'}
	locator := []byte(`["tenant-7",7]`)
	if err := session.Begin(ctx, sqldriver.TxOptions{Isolation: sqldriver.IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ApplyGlobalIndexMutation(
		ctx, "messages_by_email", 17, 3, key, locator, 2, true, false,
	); err != nil {
		t.Fatal(err)
	}
	if err := session.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	lookup := ownedRequest("")
	lookup.ExecutionMode = ExecutionReadOnly
	lookup.GlobalIndexLookup = GlobalIndexLookupRequest{
		Relation: []byte("messages_by_email"), IndexID: 17, Incarnation: 3,
		KeyTuples: [][]byte{key}, LocatorCount: 2, Unique: true,
	}
	response := exec(t, conn, lookup)
	if len(response.Columns) != 1 || response.Columns[0].Name != "locator" ||
		response.Columns[0].TypeOID != pgOIDJSON || len(response.Rows) != 1 ||
		string(response.Rows[0][0].Bytes) != string(locator) {
		t.Fatalf("global index lookup = %+v, want locator %s", response, locator)
	}

	lookup.MaxResultBytes = 1
	tooLarge := roundTrip(t, conn, lookup)
	if tooLarge.Kind != ResponseError || tooLarge.ErrorKind != ErrorResourceLimit {
		t.Fatalf("bounded global index lookup = %+v, want resource limit", tooLarge)
	}
	lookup.MaxResultBytes = 0
	lookup.GlobalIndexLookup.Incarnation++
	stale := roundTrip(t, conn, lookup)
	if stale.Kind != ResponseError || stale.ErrorKind != ErrorMalformedRequest {
		t.Fatalf("stale global index lookup = %+v, want fenced refusal", stale)
	}
}

// TestServerAdmissionBeforeExecution proves admission runs before any execution:
// a request the shard does not own is refused with the typed frame and its
// destructive SQL never runs.
func TestServerAdmissionBeforeExecution(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	// A not-owner request carrying a DROP must be refused, not executed.
	drop := ownedRequest(`DROP TABLE docs`)
	drop.Shard = "80-"
	resp := roundTrip(t, conn, drop)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorNotOwner {
		t.Fatalf("wrong-shard DROP = %+v, want NotOwner error frame", resp)
	}

	// The table and its row must survive, proving DROP never ran.
	sel := exec(t, conn, ownedRequest(`SELECT id FROM docs`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-refusal SELECT = %+v, want the surviving row", sel)
	}
}

// TestServerAdmissionErrors maps every admission divergence to its typed frame.
func TestServerAdmissionErrors(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	tests := []struct {
		name   string
		mutate func(*ShardRequest)
		want   ErrorKind
	}{
		{"wrong_distribution", func(r *ShardRequest) { r.Distribution = "other" }, ErrorNotOwner},
		{"wrong_shard", func(r *ShardRequest) { r.Shard = "80-" }, ErrorNotOwner},
		{"stale_routing_version", func(r *ShardRequest) { r.RoutingVersion = 2 }, ErrorRoutingVersion},
		{"stale_epoch", func(r *ShardRequest) { r.OwnershipEpoch = 6 }, ErrorOwnershipEpoch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := ownedRequest(`SELECT id FROM docs`)
			tc.mutate(req)
			resp := roundTrip(t, conn, req)
			if resp.Kind != ResponseError || resp.ErrorKind != tc.want {
				t.Fatalf("resp = %+v, want %s error frame", resp, tc.want)
			}
		})
	}
}

// TestServerResourceLimit proves a per-request row cap surfaces as a typed
// resource-limit frame rather than a partial or oversized result.
func TestServerResourceLimit(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	for _, id := range []string{"a", "b", "c"} {
		exec(t, conn, ownedRequest(
			`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
			StringParam(id), StringParam("x"), NumberParam("1")))
	}

	req := ownedRequest(`SELECT id FROM docs`)
	req.MaxRows = 1
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorResourceLimit {
		t.Fatalf("capped SELECT = %+v, want ResourceLimit error frame", resp)
	}

	// The connection remains usable: a request within the cap still succeeds.
	within := ownedRequest(`SELECT id FROM docs WHERE id = ?`, StringParam("a"))
	within.MaxRows = 1
	if got := exec(t, conn, within); got.Kind != ResponseRows || len(got.Rows) != 1 {
		t.Fatalf("within-cap SELECT = %+v, want one row", got)
	}
}

// TestServerDeadline proves an already-elapsed request budget surfaces as a
// typed deadline frame rather than executing.
func TestServerDeadline(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))
	exec(t, conn, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))

	req := ownedRequest(`SELECT id, name, n FROM docs ORDER BY id`)
	req.Deadline = time.Nanosecond
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorDeadlineExceeded {
		t.Fatalf("expired SELECT = %+v, want DeadlineExceeded error frame", resp)
	}
}

// TestServerMalformedRequest proves a body-level malformation is answered with a
// malformed-request frame and the same connection keeps serving.
func TestServerMalformedRequest(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	// A request naming more parameters than its body carries is a body-level
	// malformation the codec rejects without desynchronizing the stream.
	bad := ownedRequest(`SELECT id FROM docs`)
	bad.Params = []Param{NullParam()}
	raw := encodeRequest(t, bad)
	// Overwrite the trailing optional-position marker with an out-of-range value.
	raw[len(raw)-1] = 0xFF
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	resp, err := DecodeResponse(conn)
	if err != nil {
		t.Fatalf("DecodeResponse: %v", err)
	}
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorMalformedRequest {
		t.Fatalf("malformed request = %+v, want MalformedRequest error frame", resp)
	}

	// The stream stayed aligned: a well-formed request still succeeds.
	if got := exec(t, conn, ownedRequest(`SELECT id FROM docs`)); got.Kind != ResponseRows {
		t.Fatalf("post-malformed SELECT = %+v, want Rows", got)
	}
}

// TestServerRestart proves durability across a shard restart: data written by one
// server is readable by a fresh server opened over the same catalog path.
func TestServerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.vdb")

	db1 := openDB(t, path)
	srv1, err := NewServer(db1, testOwner(), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	conn1 := dial(t, srv1)
	exec(t, conn1, ownedRequest(ddlDocs))
	exec(t, conn1, ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?)`,
		StringParam("a"), StringParam("Ada"), NumberParam("7")))
	_ = conn1.Close()
	if err := srv1.Close(); err != nil {
		t.Fatalf("srv1.Close: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("db1.Close: %v", err)
	}

	db2 := openDB(t, path)
	srv2, err := NewServer(db2, testOwner(), Options{})
	if err != nil {
		t.Fatalf("reopen NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv2.Close() })
	conn2 := dial(t, srv2)
	sel := exec(t, conn2, ownedRequest(`SELECT id, name FROM docs`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-restart SELECT = %+v, want the persisted row", sel)
	}
	if got := cellText(t, sel, 0, 0); got != `"a"` {
		t.Fatalf("post-restart id = %s, want \"a\"", got)
	}
}

// TestServerCloseGraceful proves Close returns after in-flight connections drain.
func TestServerCloseGraceful(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s")
	}
	// A second Close is a no-op.
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestServeAndClose proves Serve accepts over a real listener and unblocks with
// ErrServerClosed on Close.
func TestServeAndClose(t *testing.T) {
	srv, _ := newServer(t, Options{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(l) }()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	exec(t, conn, ownedRequest(ddlDocs))
	_ = conn.Close()

	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("Serve returned %v, want ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Close")
	}
}

// TestNewServerValidation covers the constructor's rejections and defaults.
func TestNewServerValidation(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "v.vdb"))

	if _, err := NewServer(nil, testOwner(), Options{}); err == nil {
		t.Fatal("NewServer(nil db) = nil error")
	}
	if _, err := NewServer(db, Ownership{Shard: "-80"}, Options{}); err == nil {
		t.Fatal("NewServer(empty distribution) = nil error")
	}
	zeroEpoch := testOwner()
	zeroEpoch.Epoch = 0
	if _, err := NewServer(db, zeroEpoch, Options{}); err == nil {
		t.Fatal("NewServer(zero epoch) = nil error")
	}
	zeroRouting := testOwner()
	zeroRouting.RoutingVersion = 0
	if _, err := NewServer(db, zeroRouting, Options{}); err == nil {
		t.Fatal("NewServer(zero routing version) = nil error")
	}
	if _, err := NewServer(db, testOwner(), Options{MaxConnections: -2}); err == nil {
		t.Fatal("NewServer(MaxConnections=-2) = nil error")
	}
	if _, err := NewServer(db, testOwner(), Options{MaxReadFences: -1}); err == nil {
		t.Fatal("NewServer(MaxReadFences=-1) = nil error")
	}
	if _, err := NewServer(db, testOwner(), Options{MaxExchangeMailboxes: -1}); err == nil {
		t.Fatal("NewServer(MaxExchangeMailboxes=-1) = nil error")
	}
	srv, err := NewServer(db, testOwner(), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if srv.opts.MaxConnections != DefaultMaxConnections {
		t.Fatalf("MaxConnections default = %d, want %d",
			srv.opts.MaxConnections, DefaultMaxConnections)
	}
	if srv.opts.MaxResultRows != DefaultMaxResultRows {
		t.Fatalf("MaxResultRows default = %d, want %d",
			srv.opts.MaxResultRows, DefaultMaxResultRows)
	}
	if srv.opts.MaxReadFences != DefaultMaxReadFences {
		t.Fatalf("MaxReadFences default = %d, want %d",
			srv.opts.MaxReadFences, DefaultMaxReadFences)
	}
	if srv.opts.MaxExchangeMailboxes != DefaultMaxExchangeMailboxes ||
		srv.opts.MaxExchangeBufferBytes != DefaultMaxExchangeBufferBytes {
		t.Fatalf("exchange defaults = %d/%d, want %d/%d",
			srv.opts.MaxExchangeMailboxes, srv.opts.MaxExchangeBufferBytes,
			DefaultMaxExchangeMailboxes, DefaultMaxExchangeBufferBytes)
	}
}

func TestNewServerClaimsDurableFenceAndReleasesAfterDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serving-fence.vdb")
	db := openDB(t, path)
	owner := testOwner()
	srv1, err := NewServer(db, owner, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(db, owner, Options{}); !errors.Is(err, sqldriver.ErrShardStoreServingClaimed) {
		t.Fatalf("second live server = %v, want ErrShardStoreServingClaimed", err)
	}

	client, rawServer := net.Pipe()
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	server := &gatedCloseConn{
		Conn: rawServer, entered: closeEntered, release: closeRelease,
	}
	serveDone := make(chan struct{})
	go func() {
		srv1.ServeConn(server)
		close(serveDone)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		srv1.mu.Lock()
		admitted := len(srv1.conns) == 1
		srv1.mu.Unlock()
		if admitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- srv1.Close() }()
	select {
	case <-closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not begin draining the admitted connection")
	}
	if _, err := NewServer(db, owner, Options{}); !errors.Is(err, sqldriver.ErrShardStoreServingClaimed) {
		t.Fatalf("server claim released before connection drain = %v", err)
	}
	close(closeRelease)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close with admitted connection: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after connection drain was released")
	}
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close released before the admitted connection drained")
	}
	_ = client.Close()

	// Equal coordinates are reusable only after Close has drained and released
	// the first claim.
	srv2, err := NewServer(db, owner, Options{})
	if err != nil {
		t.Fatalf("equal server restart: %v", err)
	}
	if err := srv2.Close(); err != nil {
		t.Fatal(err)
	}

	staleEpoch := owner
	staleEpoch.Epoch--
	if _, err := NewServer(db, staleEpoch, Options{}); !errors.Is(err, distribution.ErrOwnershipEpoch) {
		t.Fatalf("stale epoch server = %v, want ErrOwnershipEpoch", err)
	}
	staleRouting := owner
	staleRouting.RoutingVersion--
	if _, err := NewServer(db, staleRouting, Options{}); !errors.Is(err, distribution.ErrRoutingVersion) {
		t.Fatalf("stale routing server = %v, want ErrRoutingVersion", err)
	}

	advanced := owner
	advanced.Epoch++
	advanced.RoutingVersion++
	srv3, err := NewServer(db, advanced, Options{})
	if err != nil {
		t.Fatalf("advanced server: %v", err)
	}
	if err := srv3.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewServerInvalidOptionsDoNotAdvanceServingFence(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "option-fence.vdb"))
	high := testOwner()
	high.Epoch += 100
	high.RoutingVersion += 100
	if _, err := NewServer(db, high, Options{MaxConnections: -2}); err == nil {
		t.Fatal("invalid options unexpectedly constructed server")
	}

	// If option validation happened after the durable claim, this lower but
	// otherwise valid owner would now be rejected as stale.
	srv, err := NewServer(db, testOwner(), Options{})
	if err != nil {
		t.Fatalf("valid owner after invalid options: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewServerRequiresMatchingDurableShardStore(t *testing.T) {
	t.Run("unbound", func(t *testing.T) {
		db, err := sqldriver.Open(filepath.Join(t.TempDir(), "ordinary.vdb"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		_, err = NewServer(db, testOwner(), Options{})
		if !errors.Is(err, sqldriver.ErrShardStoreUnbound) {
			t.Fatalf("NewServer(unbound) = %v, want ErrShardStoreUnbound", err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "shard.vdb")
		owner := testOwner()
		db, err := sqldriver.InitializeShardStore(path, sqldriver.ShardStoreBinding{
			Distribution: owner.Distribution, Shard: owner.Shard,
			AllocationGeneration: owner.AllocationGeneration,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		owner.AllocationGeneration++
		_, err = NewServer(db, owner, Options{})
		if !errors.Is(err, sqldriver.ErrShardStoreIdentityMismatch) {
			t.Fatalf("NewServer(mismatch) = %v, want ErrShardStoreIdentityMismatch", err)
		}
	})
}

// TestServerReadPolicyStrong proves the honored strong read policy is served.
func TestServerReadPolicyStrong(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	req := ownedRequest(`SELECT id FROM docs`)
	req.ReadPolicy = ReadStrong
	if got := exec(t, conn, req); got.Kind != ResponseRows {
		t.Fatalf("strong read = %+v, want Rows", got)
	} else if got.HasReadPosition || !got.ReadPosition.IsZero() {
		t.Fatalf("strong read claimed unimplemented applied position %+v", got.ReadPosition)
	}
}

// TestServerWriteReturningCommits proves a writable request executes a mutation
// with a RETURNING projection as one autocommitted statement: the returned
// rows are the rows the statement published, the completion count is absent
// from a row response, and a later read observes the committed row. A mutation
// without RETURNING still reports its affected-row count.
func TestServerWriteReturningCommits(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	req := ownedRequest(
		`INSERT INTO docs (id, name, n) VALUES (?, ?, ?) RETURNING id, n`,
		StringParam("a"), StringParam("Ada"), NumberParam("7"))
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseRows {
		t.Fatalf("INSERT RETURNING kind = %s, want Rows", resp.Kind)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("returned rows = %d, want 1", len(resp.Rows))
	}
	if got := cellText(t, resp, 0, 0); got != `"a"` {
		t.Fatalf("returned id = %s, want \"a\"", got)
	}
	if got := cellText(t, resp, 0, 1); got != `7` {
		t.Fatalf("returned n = %s, want 7", got)
	}

	// The statement committed: a later read observes the row.
	sel := exec(t, conn, ownedRequest(`SELECT id, n FROM docs WHERE id = 'a'`))
	if sel.Kind != ResponseRows || len(sel.Rows) != 1 {
		t.Fatalf("post-write read = %+v, want the committed row", sel)
	}
	if got := cellText(t, sel, 0, 1); got != `7` {
		t.Fatalf("committed n = %s, want 7", got)
	}

	// A RETURNING-less mutation still reports its affected-row count.
	upd := exec(t, conn, ownedRequest(
		`UPDATE docs SET "$doc" = ? WHERE id = 'a'`,
		DocumentParam(`{"id":"a","name":"Ada","n":8}`)))
	if upd.Kind != ResponseCompletion || upd.RowsAffected != 1 {
		t.Fatalf("UPDATE = %+v, want completion affecting 1 row", upd)
	}
}

// TestServerReadOnlyIntentRejectsMutations proves the safe-zero execution mode
// checks the parsed statement kind, including a mutation whose RETURNING clause
// would otherwise make ReturnsRows true, before any state changes.
func TestServerReadOnlyIntentRejectsMutations(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	exec(t, conn, ownedRequest(ddlDocs))

	req := ownedRequest(`INSERT INTO docs (id, name) VALUES ('blocked', 'nope') RETURNING id`)
	req.ExecutionMode = ExecutionReadOnly
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorReadOnly {
		t.Fatalf("read-only INSERT RETURNING = %+v, want ReadOnly error", resp)
	}

	rows := exec(t, conn, ownedRequest(`SELECT id FROM docs WHERE id = 'blocked'`))
	if rows.Kind != ResponseRows || len(rows.Rows) != 0 {
		t.Fatalf("blocked mutation left rows: %+v", rows)
	}
}

// TestServerReservedReadPoliciesFailClosed proves wire-valid future policies do
// not silently receive strong-read behavior without their promised metadata.
func TestServerReservedReadPoliciesFailClosed(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	tests := []struct {
		policy ReadPolicy
		want   ErrorKind
	}{
		{ReadSession, ErrorPositionUnsupported},
		{ReadStale, ErrorUnsupportedReadPolicy},
	}
	for _, tc := range tests {
		req := ownedRequest(`SELECT 1`)
		req.ReadPolicy = tc.policy
		resp := roundTrip(t, conn, req)
		if resp.Kind != ResponseError || resp.ErrorKind != tc.want {
			t.Fatalf("policy %s = %+v, want %s", tc.policy, resp, tc.want)
		}
	}
}

// TestServerMinimumPositionRejectedBeforeSQL proves the current service never executes a
// statement when a minimum is present. The intentionally invalid SQL would be
// classified MalformedRequest if preparation were reached.
func TestServerMinimumPositionRejectedBeforeSQL(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	req := ownedRequest(`this is intentionally not SQL`)
	req.HasMinPosition = true
	req.MinPosition = testPosition("tenant_data", "-80", 9)
	resp := roundTrip(t, conn, req)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorPositionUnsupported {
		t.Fatalf("minimum-position request = %+v, want PositionUnsupported", resp)
	}

	session := ownedRequest(`this is also intentionally not SQL`)
	session.ReadPolicy = ReadSession
	resp = roundTrip(t, conn, session)
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorPositionUnsupported {
		t.Fatalf("session-read request = %+v, want PositionUnsupported", resp)
	}
}

// TestClassifyCommitOutcomeUnknown proves the wire preserves indeterminate
// completion instead of mislabeling it as malformed and inviting a retry.
func TestClassifyCommitOutcomeUnknown(t *testing.T) {
	cause := errors.New("device sync failed")
	resp := classifyError(fmt.Errorf("commit: %w: %w", durable.ErrCommitOutcomeUnknown, cause))
	if resp.Kind != ResponseError || resp.ErrorKind != ErrorCommitOutcomeUnknown {
		t.Fatalf("classifyError = %+v, want CommitOutcomeUnknown", resp)
	}
}
