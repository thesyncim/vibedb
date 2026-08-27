package gateway

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	durableSQLPerfDigest       replication.Digest
	durableSQLPerfParticipants []ReplicatedTransactionParticipant
)

const durableSQLWideLoweringMaxAllocations = 1024

func TestDurableSQLRequestExecutorRetainedStateGate(t *testing.T) {
	// The production executor retains only three shared services and two scalar
	// protocol bounds. Per-request plans, participants, results, and retry state
	// must remain in the replicated ledger rather than a gateway-local map.
	if got := unsafe.Sizeof(DurableSQLRequestExecutor{}); got > 64 {
		t.Fatalf("durable SQL executor retained state=%d bytes, want <=64", got)
	}
}

func TestDurableSQLRequestDigestAllocationGate(t *testing.T) {
	queries := durableSQLPerfQueries(64)
	allocations := testing.AllocsPerRun(1000, func() {
		durableSQLPerfDigest = replicatedSQLTransactionRequestDigest(queries)
	})
	if allocations != 0 {
		t.Fatalf("canonical request digest allocations/run=%v, want 0", allocations)
	}
}

func TestDurableSQLWideLoweringAllocationGate(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	queries := durableSQLPerfQueries(replicatedstate.MaxDistinctMutations)
	profile := planner.profileFor(ClassInteractive)
	allocations := testing.AllocsPerRun(20, func() {
		participants, handled, err := planner.planReplicatedSQLTransactionWithData(
			context.Background(), snapshot, queries, profile, nil,
		)
		if err != nil || !handled || len(participants) != 2 {
			panic("wide durable SQL lowering changed semantics")
		}
		durableSQLPerfParticipants = participants
	})
	// The bound is intentionally linear in caller statements and independent of
	// participant count, registry capacity, or a process-local retry cache.
	if allocations > durableSQLWideLoweringMaxAllocations {
		t.Fatalf("wide lowering allocations/run=%v, want <=%d",
			allocations, durableSQLWideLoweringMaxAllocations)
	}
}

func BenchmarkDurableSQLRequestDigest(b *testing.B) {
	for _, statements := range []int{1, 16, replicatedstate.MaxDistinctMutations} {
		queries := durableSQLPerfQueries(statements)
		b.Run(strconv.Itoa(statements), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(queries)))
			for range b.N {
				durableSQLPerfDigest = replicatedSQLTransactionRequestDigest(queries)
			}
		})
	}
}

func BenchmarkDurableSQLWideLowering(b *testing.B) {
	snapshot, planner := replicatedSQLTransactionFixture(b, true)
	profile := planner.profileFor(ClassInteractive)
	for _, statements := range []int{1, 16, replicatedstate.MaxDistinctMutations} {
		queries := durableSQLPerfQueries(statements)
		b.Run(strconv.Itoa(statements), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				participants, handled, err := planner.planReplicatedSQLTransactionWithData(
					context.Background(), snapshot, queries, profile, nil,
				)
				if err != nil || !handled {
					b.Fatal(err)
				}
				durableSQLPerfParticipants = participants
			}
		})
	}
}

func BenchmarkDurableSQLMultiRowInsertLowering(b *testing.B) {
	snapshot, planner, _ := replicatedSQLSplitTransactionFixture(b)
	profile := planner.profileFor(ClassInteractive)
	for _, rows := range []int{2, 16, 64} {
		query := durableSQLMultiRowInsert(rows)
		b.Run(strconv.Itoa(rows), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(rows * 32))
			for b.Loop() {
				participants, handled, err := planner.planReplicatedSQLTransactionWithData(
					context.Background(), snapshot, []Query{query}, profile, nil,
				)
				if err != nil || !handled || len(participants) == 0 {
					b.Fatal(err)
				}
				durableSQLPerfParticipants = participants
			}
		})
	}
}

func durableSQLPerfQueries(messages int) []Query {
	queries := make([]Query, 0, messages+1)
	for index := range messages {
		queries = append(queries, Query{
			SQL: `DELETE FROM messages WHERE id = ?`, Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringParam(
				"message-" + strconv.Itoa(index),
			)},
		})
	}
	return append(queries, Query{
		SQL: `DELETE FROM logs WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.StringParam("log-boundary")},
	})
}

func durableSQLMultiRowInsert(rows int) Query {
	var sql strings.Builder
	sql.WriteString("INSERT INTO messages VALUES ")
	params := make([]shardservice.Param, rows)
	for index := range rows {
		if index != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("(?)")
		params[index] = shardservice.DocumentParam(
			`{"id":"multi-row-` + strconv.Itoa(index) + `","n":1}`,
		)
	}
	return Query{SQL: sql.String(), Class: ClassInteractive, Params: params}
}
