package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/query"
)

type blockingReplicatedSQLAdmissionOwner struct {
	*fakeReplicatedOwner
	entered chan struct{}
	release chan struct{}
}

func (owner *blockingReplicatedSQLAdmissionOwner) ReadLinearizableDataInto(
	ctx context.Context,
	_ raftservice.LinearizableDataReadRequest,
	_ *raftservice.LinearizableDataReadCut,
) error {
	select {
	case owner.entered <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	select {
	case <-owner.release:
		return raftservice.ErrInvalidOwner
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func TestReplicatedSQLWireRoundTripBoundsAndCapability(t *testing.T) {
	var inner bytes.Buffer
	if err := EncodeRequest(&inner, &ShardRequest{SQL: `SELECT COUNT(*) FROM docs`, Distribution: "data", Shard: "all", AllocationGeneration: 5, RoutingVersion: 1, OwnershipEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	req := ReplicatedRequest{Operation: ReplicatedQueryLeader, Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence(), Query: inner.Bytes(), MaxValueBytes: 4096}
	req.Authority = serviceauthz.Authority{Generation: 1}
	req.Authority.Node[0] = 1
	for _, encode := range []func(io.Writer, *ReplicatedRequest) error{EncodeReplicatedRequest, EncodeReplicatedRequestBorrowed} {
		var frame bytes.Buffer
		if err := encode(&frame, &req); err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeReplicatedRequest(&frame)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Operation != req.Operation || decoded.Fence != req.Fence || !bytes.Equal(decoded.Query, req.Query) || cap(decoded.Query) != len(decoded.Query) {
			t.Fatal("SQL envelope changed")
		}
	}
	for _, mutate := range []func(*ReplicatedRequest){
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityBackup },
		func(r *ReplicatedRequest) { r.MaxValueBytes = MaxReplicatedSQLResultBytes + 1 },
		func(r *ReplicatedRequest) { r.Query = make([]byte, MaxReplicatedSQLRequestBytes+1) },
		func(r *ReplicatedRequest) { r.Operation = ReplicatedProbe },
		func(r *ReplicatedRequest) { r.MinimumApplied = 1 },
	} {
		bad := req
		mutate(&bad)
		if err := EncodeReplicatedRequest(io.Discard, &bad); err == nil {
			t.Fatal("invalid SQL envelope accepted")
		}
	}
}

func TestDefaultReplicatedFrameBudgetAdmitsTwoWorstBoundSQLQueries(t *testing.T) {
	if replicatedSQLMaximumReservationBytes != 40<<20 {
		t.Fatalf("maximum SQL reservation = %d, want %d", replicatedSQLMaximumReservationBytes, 40<<20)
	}
	if DefaultReplicatedInFlightFrameBytes != 112<<20 {
		t.Fatalf("default frame budget = %d, want %d", DefaultReplicatedInFlightFrameBytes, 112<<20)
	}
	request := ReplicatedRequest{
		Operation: ReplicatedQueryLeader, Capability: serviceauthz.CapabilityDataRead,
		Fence: testReplicatedFence(), Query: make([]byte, MaxReplicatedSQLRequestBytes),
		MaxValueBytes: MaxReplicatedSQLResultBytes,
		Authority:     serviceauthz.Authority{Generation: 1},
	}
	request.Authority.Node[0] = 1
	var frame bytes.Buffer
	if err := EncodeReplicatedRequest(&frame, &request); err != nil {
		t.Fatal(err)
	}
	requestBytes := int64(frame.Len() - 5)
	budget := replicatedFrameByteBudget{limit: DefaultReplicatedInFlightFrameBytes}
	for query := range 2 {
		if !budget.reserve(requestBytes) || !budget.reserveSQL(t.Context(), replicatedSQLMaximumReservationBytes) {
			t.Fatalf("maximum query %d was not admitted: used=%d limit=%d", query, budget.used.Load(), budget.limit)
		}
	}
	usedByTwo := 2 * (requestBytes + replicatedSQLMaximumReservationBytes)
	if got := budget.used.Load(); got != usedByTwo {
		t.Fatalf("two-query reservation = %d, want %d", got, usedByTwo)
	}
	if remaining := budget.limit - usedByTwo; remaining < replication.MaxCommandBytes {
		t.Fatalf("two queries leave %d bytes, below one maximum command %d", remaining, replication.MaxCommandBytes)
	}

	// A third request body can be decoded and refused canonically. Its 40 MiB
	// execution reservation cannot cross the process-wide bound.
	if !budget.reserve(requestBytes) {
		t.Fatal("third maximum request body could not reach typed SQL admission")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if budget.reserveSQL(ctx, replicatedSQLMaximumReservationBytes) {
		t.Fatal("third maximum SQL execution escaped the two-query bound")
	}
	budget.release(requestBytes)
	for range 2 {
		budget.releaseSQL(replicatedSQLMaximumReservationBytes)
		budget.release(requestBytes)
	}
	if got := budget.used.Load(); got != 0 {
		t.Fatalf("released SQL budget = %d, want zero", got)
	}
}

func TestReplicatedServerSmallSQLAdmissionsOverlapWithoutMaximumReservation(t *testing.T) {
	state := testReplicatedServingState()
	state.Identity.Distribution, state.Identity.Shard = "data", "all"
	base := &fakeReplicatedOwner{state: state}
	owner := &blockingReplicatedSQLAdmissionOwner{
		fakeReplicatedOwner: base,
		entered:             make(chan struct{}, 9),
		release:             make(chan struct{}),
	}
	server, err := NewReplicatedServer(
		owner, DefaultReplicatedInFlightFrameBytes, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	inner := ShardRequest{
		Authority: authority, SQL: `SELECT 1`,
		Distribution: distribution.DistributionName(state.Identity.Distribution),
		Shard:        distribution.ShardID(state.Identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			state.Identity.AllocationGeneration,
		),
		RoutingVersion: distribution.RoutingVersion(state.Command.RoutingVersion),
		OwnershipEpoch: distribution.OwnershipEpoch(state.Command.OwnershipEpoch),
		ReadPolicy:     ReadStrong, ExecutionMode: ExecutionReadOnly,
		MaxRows: 1, MaxResultBytes: MaxReplicatedSQLResultBytes,
	}
	var encoded bytes.Buffer
	if err := EncodeRequest(&encoded, &inner); err != nil {
		t.Fatal(err)
	}
	request := &ReplicatedRequest{
		Operation: ReplicatedQueryLeader, Authority: authority,
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      replicatedWireState(state).Fence,
		Query:      encoded.Bytes(), MaxValueBytes: MaxReplicatedSQLResultBytes,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	responses := make(chan *ReplicatedResponse, 8)
	for range 8 {
		go func() { responses <- server.executeReplicated(ctx, request) }()
	}
	for query := range 8 {
		select {
		case <-owner.entered:
		case <-time.After(time.Second):
			t.Fatalf("maximum query %d did not overlap at the ReadIndex boundary", query)
		}
	}
	if got := server.Stats().InFlightFrameBytes; got != 8*replicatedSQLTiers[0].reservationBytes() {
		t.Fatalf("small-query reservation = %d", got)
	}
	// Fill the remaining process allowance to exercise cancellation while the
	// eight original queries hold their small reservations.
	rest := server.frames.limit - server.frames.used.Load()
	if !server.frames.reserve(rest) {
		t.Fatal("reserve remaining budget")
	}
	defer server.frames.release(rest)
	blocked, cancelBlocked := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancelBlocked()
	third := server.executeReplicated(blocked, request)
	if third.Kind != ReplicatedRefusal || third.Refusal != ReplicatedRefusalAdmissionBound {
		t.Fatalf("third overlapping query = %+v, want admission refusal", third)
	}
	select {
	case <-owner.entered:
		t.Fatal("third query reached ReadIndex after its admission refusal")
	default:
	}

	close(owner.release)
	for query := range 8 {
		select {
		case response := <-responses:
			if response.Kind != ReplicatedRefusal || response.Refusal != ReplicatedRefusalUnavailable {
				t.Fatalf("released query %d = %+v", query, response)
			}
		case <-time.After(time.Second):
			t.Fatalf("released query %d did not finish", query)
		}
	}
	if got := server.Stats().InFlightFrameBytes; got != rest {
		t.Fatalf("completed SQL reservations retained %d extra bytes", got-rest)
	}
}

func BenchmarkReplicatedSQLAdmissionWaves(b *testing.B) {
	for _, test := range []struct {
		name  string
		width int
	}{{"one", 1}, {"two", 2}} {
		b.Run(test.name, func(b *testing.B) {
			budget := replicatedFrameByteBudget{limit: DefaultReplicatedInFlightFrameBytes}
			b.ReportAllocs()
			for b.Loop() {
				for range test.width {
					if !budget.reserve(replicatedSQLMaximumReservationBytes) {
						b.Fatal("bounded SQL reservation refused")
					}
				}
				for range test.width {
					budget.release(replicatedSQLMaximumReservationBytes)
				}
			}
			b.ReportMetric(float64(test.width), "queries/wave")
		})
	}
}

func TestSQLBackpressureWakesOnReleaseAndRespectsBound(t *testing.T) {
	budget := &replicatedFrameByteBudget{limit: 10}
	if !budget.reserve(10) {
		t.Fatal("initial reservation")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	admitted := make(chan bool, 16)
	for range 16 {
		go func() { admitted <- budget.reserveSQL(ctx, 10) }()
	}
	deadline := time.After(time.Second)
	for budget.waiters.Load() != 16 {
		select {
		case <-deadline:
			t.Fatal("waiters not registered")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if budget.reserveSQL(t.Context(), 10) {
		t.Fatal("unbounded waiting queue")
	}
	if budget.used.Load() != 10 {
		t.Fatal("waiting allocated a workspace")
	}
	budget.release(10)
	select {
	case ok := <-admitted:
		if !ok {
			t.Fatal("released capacity not admitted")
		}
	case <-time.After(time.Second):
		t.Fatal("lost wakeup")
	}
	if budget.used.Load() != 10 {
		t.Fatal("reservation bound exceeded")
	}
	cancel()
	for range 15 {
		select {
		case ok := <-admitted:
			if ok {
				t.Fatal("cancelled waiter acquired")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation stuck")
		}
	}
	budget.releaseSQL(10)
	if budget.used.Load() != 0 || budget.waiters.Load() != 0 {
		t.Fatal("reservation or waiter leaked")
	}
}

func TestSQLExecutionQuotaPreservesNativeCapacity(t *testing.T) {
	budget := &replicatedFrameByteBudget{limit: DefaultReplicatedInFlightFrameBytes}
	charge := replicatedSQLTiers[0].reservationBytes()
	count := int(budget.sqlLimit() / charge)
	for range count {
		if !budget.reserveSQL(t.Context(), charge) {
			t.Fatal("small reservation refused")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if budget.reserveSQL(ctx, charge) {
		t.Fatal("SQL crossed execution quota")
	}
	if !budget.reserve(replication.MaxCommandBytes) {
		t.Fatal("SQL starved native command")
	}
	budget.release(replication.MaxCommandBytes)
	for range count {
		(&replicatedSQLLease{budget: budget, bytes: charge}).Release()
	}
	if budget.used.Load() != 0 || budget.sqlUsed != 0 {
		t.Fatal("accounting leaked")
	}
	// An impossible reservation refuses immediately even without a deadline.
	if budget.reserveSQL(t.Context(), budget.sqlLimit()+1) {
		t.Fatal("oversized reservation admitted")
	}
}

func TestReplicatedSQLResponseSizeMatchesCodec(t *testing.T) {
	responses := []*ShardResponse{
		{Kind: ResponseRows},
		{Kind: ResponseRows, Columns: []Column{{Name: "answer", TypeOID: 23}}, Rows: [][]Cell{{{Bytes: []byte("42")}}, {{Null: true}}, {{Bytes: []byte{}}}}},
		{Kind: ResponseRows, Columns: []Column{{Name: strings.Repeat("column", 12000)}}, Rows: [][]Cell{{{Bytes: []byte(strings.Repeat("x", 70000))}}}},
		NewErrorResponse(ErrorMalformedRequest, "invalid query"),
		NewErrorResponse(ErrorMalformedRequest, strings.Repeat("error", 14000)),
	}
	for i, response := range responses {
		var encoded bytes.Buffer
		if err := EncodeResponse(&encoded, response); err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{-1, 0, encoded.Len() - 1, encoded.Len(), encoded.Len() + 1} {
			if got, want := replicatedSQLResponseFits(response, limit), limit >= encoded.Len(); got != want {
				t.Fatalf("response %d size %d limit %d fits=%v want=%v", i, encoded.Len(), limit, got, want)
			}
		}
	}
	if replicatedSQLResponseFits(nil, 100) || replicatedSQLResponseFits(&ShardResponse{Kind: ResponseRows, Rows: [][]Cell{{{Null: true}}}}, 100) {
		t.Fatal("invalid shape accepted")
	}
}

func TestReplicatedSQLTierEscalationClassifiesOnlyGrowableLimits(t *testing.T) {
	small := replicatedSQLTiers[0]
	bytesErr := &query.ResultBudgetError{Rows: 1, RowLimit: 10, Bytes: 100000, ByteLimit: int64(small.resultBytes)}
	for _, test := range []struct {
		name    string
		err     error
		maximum int
		grow    bool
	}{
		{"bytes", bytesErr, MaxReplicatedSQLResultBytes, true},
		{"wrapped bytes", fmt.Errorf("wrapped: %w", bytesErr), MaxReplicatedSQLResultBytes, true},
		{"caller bytes", bytesErr, small.resultBytes, false},
		{"caller rows", &query.ResultBudgetError{Rows: 11, RowLimit: 10}, MaxReplicatedSQLResultBytes, false},
		{"untyped result", query.ErrResultBudget, MaxReplicatedSQLResultBytes, false},
		{"workspace", query.ErrWorkBudget, MaxReplicatedSQLResultBytes, true},
		{"intermediate", query.ErrIntermediateBudget, MaxReplicatedSQLResultBytes, true},
		{"aggregate", query.ErrAggregateBudget, MaxReplicatedSQLResultBytes, true},
		{"join", query.ErrJoinPairBudget, MaxReplicatedSQLResultBytes, true},
		{"cancel", context.Canceled, MaxReplicatedSQLResultBytes, false},
		{"syntax", fmt.Errorf("syntax error"), MaxReplicatedSQLResultBytes, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := small.canGrow(test.err, test.maximum); got != test.grow {
				t.Fatalf("grow=%v want=%v", got, test.grow)
			}
			if replicatedSQLTiers[len(replicatedSQLTiers)-1].canGrow(test.err, MaxReplicatedSQLResultBytes) {
				t.Fatal("final tier grows")
			}
		})
	}
}

func TestReplicatedSQLBudgetHintsBoundedAndConservative(t *testing.T) {
	var hints replicatedSQLBudgetHints
	key := sha256.Sum256([]byte("SELECT bucket, COUNT(*) FROM docs GROUP BY bucket"))
	if hints.lookup(key) != 0 {
		t.Fatal("cold query skipped smallest tier")
	}
	hints.record(key, 2)
	hints.record(key, 1)
	if hints.lookup(key) != 2 {
		t.Fatal("smaller concurrent result reduced proven allowance")
	}
	collision := key
	collision[1] ^= 1
	if hints.lookup(collision) != 0 {
		t.Fatal("colliding query inherited allowance")
	}
	hints.record(collision, 3)
	if hints.lookup(key) != 0 || hints.lookup(collision) != 3 {
		t.Fatal("collision did not replace fixed slot")
	}
	hints.record(collision, 1000)
	if hints.lookup(collision) != 3 {
		t.Fatal("invalid tier retained")
	}
	var workers sync.WaitGroup
	for i := range 32 {
		workers.Go(func() {
			for range 100 {
				hints.record(key, 1+i%3)
				if tier := hints.lookup(key); tier < 0 || tier >= len(replicatedSQLTiers) {
					t.Errorf("invalid concurrent tier %d", tier)
				}
			}
		})
	}
	workers.Wait()
	if hints.lookup(key) != 3 {
		t.Fatal("concurrent promotions lost largest allowance")
	}
}

type replicatedSQLPointPathOwner struct {
	*fakeReplicatedOwner
	pointErr  error
	dataErr   error
	pointCall int
	dataCall  int
}

func (owner *replicatedSQLPointPathOwner) ReadLinearizablePointInto(
	context.Context,
	raftservice.LinearizablePointReadRequest,
	*raftservice.LinearizablePointReadCut,
) error {
	owner.pointCall++
	return owner.pointErr
}

func (owner *replicatedSQLPointPathOwner) ReadLinearizableDataInto(
	context.Context,
	raftservice.LinearizableDataReadRequest,
	*raftservice.LinearizableDataReadCut,
) error {
	owner.dataCall++
	return owner.dataErr
}

func testReplicatedSQLPointCall(
	state raftservice.ServingState,
	primary PrimaryKeyReadRequest,
) (*ReplicatedRequest, *ShardRequest) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	inner := &ShardRequest{
		Authority: authority, SQL: "SELECT 1",
		Distribution: distribution.DistributionName(state.Identity.Distribution),
		Shard:        distribution.ShardID(state.Identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			state.Identity.AllocationGeneration,
		),
		RoutingVersion: distribution.RoutingVersion(state.Command.RoutingVersion),
		OwnershipEpoch: distribution.OwnershipEpoch(state.Command.OwnershipEpoch),
		ReadPolicy:     ReadStrong, ExecutionMode: ExecutionReadOnly,
		MaxRows: 1, MaxResultBytes: 4096, PrimaryKeyRead: primary,
	}
	request := &ReplicatedRequest{
		Operation: ReplicatedQueryLeader, Authority: authority,
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      replicatedWireState(state).Fence, MaxValueBytes: 4096,
	}
	return request, inner
}

func TestReplicatedSQLSinglePointReadRefusalsSkipDataSnapshot(t *testing.T) {
	primary := PrimaryKeyReadRequest{
		Relation: 1, MaxDocumentBytes: 1024,
		PrimaryPath: []byte("/id"), Keys: [][]byte{[]byte("k")},
	}
	for _, test := range []struct {
		name string
		err  error
		kind ReplicatedResponseKind
		code ReplicatedRefusalCode
	}{
		{name: "not leader", err: raftmodel.ErrNotLeader, kind: ReplicatedNotLeader},
		{name: "stale fence", err: raftservice.ErrServingFence, kind: ReplicatedRefusal, code: ReplicatedRefusalStaleFence},
		{name: "intent", err: replicatedstate.ErrTransactionIntentActive, kind: ReplicatedRefusal, code: ReplicatedRefusalReadIntentActive},
		{name: "buffer", err: replicatedstate.ErrReadBufferBound, kind: ReplicatedRefusal, code: ReplicatedRefusalReadBufferBound},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := testReplicatedServingState()
			state.Identity.Distribution, state.Identity.Shard = "data", "all"
			request, inner := testReplicatedSQLPointCall(state, primary)
			owner := &replicatedSQLPointPathOwner{
				fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
				pointErr:            test.err,
				dataErr:             errors.New("data snapshot path was reached"),
			}
			server := &ReplicatedServer{
				owner: owner, requestTimeout: time.Second,
				frames: replicatedFrameByteBudget{limit: 2 << 20},
			}
			response := server.executeReplicatedQueryCall(
				t.Context(), request, state, inner, nil,
			)
			if response.Kind != test.kind || response.Refusal != test.code {
				t.Fatalf("response=%+v, want kind=%v refusal=%v", response, test.kind, test.code)
			}
			if owner.pointCall != 1 || owner.dataCall != 0 {
				t.Fatalf("point/data calls=%d/%d, want 1/0", owner.pointCall, owner.dataCall)
			}
		})
	}
}

func TestReplicatedSQLPointAdmissionIncludesCatalogFrozenDocumentBound(t *testing.T) {
	state := testReplicatedServingState()
	state.Identity.Distribution, state.Identity.Shard = "data", "all"
	request, inner := testReplicatedSQLPointCall(state, PrimaryKeyReadRequest{
		Relation: 1, MaxDocumentBytes: 3 << 20,
		PrimaryPath: []byte("/id"), Keys: [][]byte{[]byte("k")},
	})
	owner := &replicatedSQLPointPathOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
		pointErr:            raftmodel.ErrNotLeader,
	}
	server := testReplicatedServer(owner)
	budget := replicatedSQLTiers[0]
	charge, ok := budget.reservationBytesForPoint(inner.PrimaryKeyRead.MaxDocumentBytes)
	if !ok || charge <= budget.reservationBytes() {
		t.Fatalf("point reservation = %d ok=%v, base reservation = %d", charge, ok, budget.reservationBytes())
	}
	server.frames.limit = charge - 1
	response, grow := server.executeReplicatedQueryTierCall(
		t.Context(), request, state, inner, owner, budget, MaxReplicatedSQLResultBytes, nil,
	)
	if grow || response.Kind != ReplicatedRefusal || response.Refusal != ReplicatedRefusalAdmissionBound {
		t.Fatalf("under-reserved point response=%+v grow=%v", response, grow)
	}
	if owner.pointCall != 0 || server.frames.used.Load() != 0 {
		t.Fatalf("point call/accounting = %d/%d, want 0/0", owner.pointCall, server.frames.used.Load())
	}

	// The exact charged amount reaches the point read, and the normal refusal
	// classification still releases the complete tier lease.
	server.frames.limit = charge
	response, grow = server.executeReplicatedQueryTierCall(
		t.Context(), request, state, inner, owner, budget, MaxReplicatedSQLResultBytes, nil,
	)
	if grow || response.Kind != ReplicatedNotLeader {
		t.Fatalf("exactly reserved point response=%+v grow=%v", response, grow)
	}
	if owner.pointCall != 1 || server.frames.used.Load() != 0 {
		t.Fatalf("exact point call/accounting = %d/%d, want 1/0", owner.pointCall, server.frames.used.Load())
	}
}

func TestReplicatedSQLUnsupportedPrimaryCandidatesRetainSnapshotPath(t *testing.T) {
	state := testReplicatedServingState()
	state.Identity.Distribution, state.Identity.Shard = "data", "all"
	base := PrimaryKeyReadRequest{
		PrimaryPath: []byte("/id"), Keys: [][]byte{[]byte("k")},
	}
	variants := []struct {
		name    string
		primary PrimaryKeyReadRequest
	}{
		{name: "absent", primary: PrimaryKeyReadRequest{}},
		{name: "legacy single key", primary: base},
		{name: "multiple keys", primary: PrimaryKeyReadRequest{
			Relation: 1, MaxDocumentBytes: 1024,
			PrimaryPath: []byte("/id"), Keys: [][]byte{[]byte("a"), []byte("b")},
		}},
	}
	for _, test := range variants {
		t.Run(test.name, func(t *testing.T) {
			request, inner := testReplicatedSQLPointCall(state, test.primary)
			owner := &replicatedSQLPointPathOwner{
				fakeReplicatedOwner: &fakeReplicatedOwner{state: state},
				pointErr:            raftmodel.ErrNotLeader,
				dataErr:             raftmodel.ErrNotLeader,
			}
			server := testReplicatedServer(owner)
			response := server.executeReplicatedQueryCall(
				t.Context(), request, state, inner, nil,
			)
			if response.Kind != ReplicatedNotLeader {
				t.Fatalf("response=%+v, want not-leader from snapshot path", response)
			}
			if owner.pointCall != 0 || owner.dataCall != 1 {
				t.Fatalf("point/data calls=%d/%d, want 0/1", owner.pointCall, owner.dataCall)
			}
		})
	}
}
