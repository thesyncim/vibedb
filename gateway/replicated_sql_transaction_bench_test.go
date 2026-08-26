package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	replicatedSQLBenchmarkParticipants []ReplicatedTransactionParticipant
	replicatedSQLBenchmarkResult       *Result
	replicatedSQLBenchmarkOutcome      ReplicatedTransactionResult
	replicatedSQLBenchmarkDigest       replication.Digest
)

func replicatedSQLBenchmarkQueries(perRelation int) []Query {
	queries := make([]Query, 0, perRelation+1)
	for index := range perRelation {
		queries = append(queries, Query{
			SQL: `DELETE FROM messages WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam(
				"message-" + strconv.Itoa(index),
			)},
		})
	}
	return append(queries, Query{
		SQL: `DELETE FROM logs WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam(
			"log-boundary",
		)},
	})
}

func replicatedSQLBenchmarkSingleParticipantQueries() []Query {
	return []Query{
		{
			SQL: `DELETE FROM accounts WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam(
				"account-singleton",
			)},
		},
		{
			SQL: `DELETE FROM messages WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam(
				"message-singleton",
			)},
		},
	}
}

func benchmarkReplicatedSQLPlanQueries(
	b testing.TB,
	queries []Query,
	wantParticipants int,
) (*Snapshot, *Executor, []Query, []ReplicatedTransactionParticipant) {
	b.Helper()
	snapshot, executor := replicatedSQLTransactionFixture(b, true)
	participants, handled, err := executor.planReplicatedSQLTransaction(
		context.Background(), snapshot, queries, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled || len(participants) != wantParticipants {
		b.Fatalf("warm plan: participants=%d handled=%v err=%v",
			len(participants), handled, err)
	}
	return snapshot, executor, queries, participants
}

func benchmarkReplicatedSQLPlan(
	b testing.TB,
	perRelation int,
) (*Snapshot, *Executor, []Query, []ReplicatedTransactionParticipant) {
	b.Helper()
	return benchmarkReplicatedSQLPlanQueries(
		b, replicatedSQLBenchmarkQueries(perRelation), 2,
	)
}

// TestReplicatedSQLTransactionAllocationGate pins the warmed lowering cost at
// the singleton multi-relation shape, ordinary two-participant shape, and the
// exact per-relation mutation boundary. The wide case must remain bounded by
// statement count and must not create a hidden participant-, relation-, or
// registry-sized arena.
func TestReplicatedSQLTransactionAllocationGate(t *testing.T) {
	for _, test := range []struct {
		name             string
		queries          func() []Query
		wantParticipants int
		maxAllocs        float64
	}{
		{
			name: "one-participant", queries: replicatedSQLBenchmarkSingleParticipantQueries,
			wantParticipants: 1, maxAllocs: 48,
		},
		{
			name: "two-participants", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(1)
			}, wantParticipants: 2, maxAllocs: 48,
		},
		{
			name: "per-relation-boundary", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(replicatedstate.MaxDistinctMutations)
			}, wantParticipants: 2, maxAllocs: 1024,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, executor, queries, _ := benchmarkReplicatedSQLPlanQueries(
				t, test.queries(), test.wantParticipants,
			)
			profile := executor.profileFor(ClassInteractive)
			allocations := testing.AllocsPerRun(100, func() {
				participants, handled, err := executor.planReplicatedSQLTransaction(
					context.Background(), snapshot, queries, profile,
				)
				if err != nil || !handled || len(participants) != test.wantParticipants {
					panic("replicated SQL allocation gate changed semantics")
				}
				replicatedSQLBenchmarkParticipants = participants
			})
			t.Logf("allocations/run=%.0f", allocations)
			if allocations > test.maxAllocs {
				t.Fatalf("allocations/run=%.0f, want <= %.0f", allocations, test.maxAllocs)
			}
		})
	}
}

// TestReplicatedSQLTransactionPerRelationBoundaryGate proves the successful
// wide benchmark is the exact state-machine boundary rather than an arbitrary
// convenient count. One additional mutation in that same relation is refused
// before orchestration.
func TestReplicatedSQLTransactionPerRelationBoundaryGate(t *testing.T) {
	snapshot, executor, queries, participants := benchmarkReplicatedSQLPlan(
		t, replicatedstate.MaxDistinctMutations,
	)
	foundBoundary := false
	for participantIndex := range participants {
		for batchIndex := range participants[participantIndex].Batches {
			batch := &participants[participantIndex].Batches[batchIndex]
			if batch.Relation == 2 && len(batch.Mutations) == replicatedstate.MaxDistinctMutations {
				foundBoundary = true
			}
		}
	}
	if !foundBoundary {
		t.Fatalf("no relation carried %d mutations: %+v",
			replicatedstate.MaxDistinctMutations, participants)
	}
	overflow := append([]Query(nil), queries...)
	overflow = append(overflow[:len(overflow)-1], Query{
		SQL: `DELETE FROM messages WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam(
			"message-overflow",
		)},
	}, overflow[len(overflow)-1])
	got, handled, err := executor.planReplicatedSQLTransaction(
		context.Background(), snapshot, overflow, executor.profileFor(ClassInteractive),
	)
	if len(got) != 0 || !handled || !errors.Is(err, ErrTransactionMutationLimit) {
		t.Fatalf("overflow participants=%d handled=%v err=%v", len(got), handled, err)
	}
}

// TestReplicatedTransactionRequestRegistryAllocationGate pins the idempotent
// terminal-hit path at zero allocations. Cold resolved entries may allocate
// only their entry, one in-flight call, and its completion channel; Forget must
// return the slot without retaining request-sized payloads.
func TestReplicatedTransactionRequestRegistryAllocationGate(t *testing.T) {
	_, _, queries, participants := benchmarkReplicatedSQLPlan(t, replicatedstate.MaxDistinctMutations)
	digest := replicatedSQLTransactionRequestDigest(queries)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := replication.ID128{1}
	if _, err = registry.Execute(context.Background(), id, digest, 7, participants); err != nil {
		t.Fatal(err)
	}
	terminalHit := testing.AllocsPerRun(1000, func() {
		outcome, executeErr := registry.Execute(
			context.Background(), id, digest, 7, participants,
		)
		if executeErr != nil || !outcome.Committed {
			panic("replicated registry terminal allocation gate changed semantics")
		}
		replicatedSQLBenchmarkOutcome = outcome.ReplicatedTransactionResult
	})
	if terminalHit != 0 {
		t.Fatalf("terminal-hit allocations/run=%.0f, want 0", terminalHit)
	}
	replayHit := testing.AllocsPerRun(1000, func() {
		outcome, found, replayErr := registry.Replay(context.Background(), id, digest)
		if replayErr != nil || !found || !outcome.Committed ||
			outcome.CatalogGeneration != 7 || outcome.ShardsFanned != len(participants) {
			panic("replicated registry replay allocation gate changed semantics")
		}
		replicatedSQLBenchmarkOutcome = outcome.ReplicatedTransactionResult
	})
	if replayHit != 0 {
		t.Fatalf("replay-hit allocations/run=%.0f, want 0", replayHit)
	}
	if err = registry.Forget(context.Background(), id, digest); err != nil {
		t.Fatal(err)
	}
	var sequence uint64
	coldResolved := testing.AllocsPerRun(1000, func() {
		sequence++
		var requestID replication.ID128
		binary.LittleEndian.PutUint64(requestID[:8], sequence+1)
		outcome, executeErr := registry.Execute(
			context.Background(), requestID, digest, 7, participants,
		)
		if executeErr != nil || !outcome.Committed {
			panic("replicated registry cold allocation gate changed semantics")
		}
		if forgetErr := registry.Forget(context.Background(), requestID, digest); forgetErr != nil {
			panic("replicated registry cold allocation gate leaked capacity")
		}
		replicatedSQLBenchmarkOutcome = outcome.ReplicatedTransactionResult
	})
	t.Logf("terminal-hit=%.0f replay-hit=%.0f cold-resolved=%.0f allocations/run",
		terminalHit, replayHit, coldResolved)
	if coldResolved > 5 {
		t.Fatalf("cold resolved allocations/run=%.0f, want <= 5", coldResolved)
	}
	if stats := registry.Stats(); stats.Entries != 0 {
		t.Fatalf("cold resolved entries retained: %+v", stats)
	}
}

func BenchmarkReplicatedSQLTransactionLowering(b *testing.B) {
	for _, test := range []struct {
		name             string
		queries          func() []Query
		wantParticipants int
	}{
		{
			name: "one-participant", queries: replicatedSQLBenchmarkSingleParticipantQueries,
			wantParticipants: 1,
		},
		{
			name: "two-participants", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(1)
			}, wantParticipants: 2,
		},
		{
			name: "per-relation-boundary", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(replicatedstate.MaxDistinctMutations)
			}, wantParticipants: 2,
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			snapshot, executor, queries, _ := benchmarkReplicatedSQLPlanQueries(
				b, test.queries(), test.wantParticipants,
			)
			profile := executor.profileFor(ClassInteractive)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				participants, handled, err := executor.planReplicatedSQLTransaction(
					context.Background(), snapshot, queries, profile,
				)
				if err != nil || !handled {
					b.Fatal(err)
				}
				replicatedSQLBenchmarkParticipants = participants
			}
		})
	}
}

func BenchmarkReplicatedSQLTransactionCachedRequest(b *testing.B) {
	for _, test := range []struct {
		name             string
		queries          func() []Query
		wantParticipants int
	}{
		{
			name: "one-participant", queries: replicatedSQLBenchmarkSingleParticipantQueries,
			wantParticipants: 1,
		},
		{
			name: "two-participants", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(1)
			}, wantParticipants: 2,
		},
		{
			name: "per-relation-boundary", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(replicatedstate.MaxDistinctMutations)
			}, wantParticipants: 2,
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			snapshot, executor, queries, _ := benchmarkReplicatedSQLPlanQueries(
				b, test.queries(), test.wantParticipants,
			)
			capture := new(replicatedSQLTransactionCapture)
			registry, err := NewReplicatedTransactionRequestRegistry(
				ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 1},
			)
			if err != nil {
				b.Fatal(err)
			}
			executor.replicatedTransactionRequests = registry
			requestID := replication.ID128{1}
			result, err := executor.ExecBatchRequest(context.Background(), requestID, queries)
			if err != nil || result == nil || capture.calls != 1 ||
				result.Generation != snapshot.Generation() {
				b.Fatalf("warm cached request result=%+v err=%v calls=%d",
					result, err, capture.calls)
			}
			// A cache hit must not pin or plan. With no published catalog either
			// operation would fail immediately, while Replay remains sufficient.
			executor.catalog = NewCatalogHolder(nil)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, executeErr := executor.ExecBatchRequest(
					context.Background(), requestID, queries,
				)
				if executeErr != nil || result.Generation != snapshot.Generation() {
					b.Fatal(executeErr)
				}
				replicatedSQLBenchmarkResult = result
			}
			if capture.calls != 1 {
				b.Fatalf("cached request executed %d times", capture.calls)
			}
		})
	}
}

func BenchmarkReplicatedTransactionRequestRegistry(b *testing.B) {
	_, _, queries, participants := benchmarkReplicatedSQLPlan(b, replicatedstate.MaxDistinctMutations)
	digest := replicatedSQLTransactionRequestDigest(queries)
	b.Run("terminal-hit", func(b *testing.B) {
		capture := new(replicatedSQLTransactionCapture)
		registry, newErr := NewReplicatedTransactionRequestRegistry(
			ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 1},
		)
		if newErr != nil {
			b.Fatal(newErr)
		}
		id := replication.ID128{1}
		if _, executeErr := registry.Execute(
			context.Background(), id, digest, 7, participants,
		); executeErr != nil {
			b.Fatal(executeErr)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			outcome, executeErr := registry.Execute(
				context.Background(), id, digest, 7, participants,
			)
			if executeErr != nil {
				b.Fatal(executeErr)
			}
			replicatedSQLBenchmarkOutcome = outcome.ReplicatedTransactionResult
		}
	})
	b.Run("cold-resolved-forget", func(b *testing.B) {
		capture := new(replicatedSQLTransactionCapture)
		registry, newErr := NewReplicatedTransactionRequestRegistry(
			ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 1},
		)
		if newErr != nil {
			b.Fatal(newErr)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := range b.N {
			var id replication.ID128
			binary.LittleEndian.PutUint64(id[:8], uint64(index+1))
			outcome, executeErr := registry.Execute(
				context.Background(), id, digest, 7, participants,
			)
			if executeErr != nil {
				b.Fatal(executeErr)
			}
			if forgetErr := registry.Forget(context.Background(), id, digest); forgetErr != nil {
				b.Fatal(forgetErr)
			}
			replicatedSQLBenchmarkOutcome = outcome.ReplicatedTransactionResult
		}
	})
}

func BenchmarkReplicatedSQLTransactionRequestDigest(b *testing.B) {
	for _, test := range []struct {
		name             string
		queries          func() []Query
		wantParticipants int
	}{
		{
			name: "one-participant", queries: replicatedSQLBenchmarkSingleParticipantQueries,
			wantParticipants: 1,
		},
		{
			name: "two-participants", queries: func() []Query {
				return replicatedSQLBenchmarkQueries(1)
			}, wantParticipants: 2,
		},
		{
			name: strconv.Itoa(replicatedstate.MaxDistinctMutations+1) + "-statements",
			queries: func() []Query {
				return replicatedSQLBenchmarkQueries(replicatedstate.MaxDistinctMutations)
			}, wantParticipants: 2,
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			queries := test.queries()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				digest := replicatedSQLTransactionRequestDigest(queries)
				replicatedSQLBenchmarkDigest = digest
			}
		})
	}
}
