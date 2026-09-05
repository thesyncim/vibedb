package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestInsertIgnoreValidatesSkipsAndRetainsExactCompletion(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("ignored")
	first := []byte(`{"n":1}`)
	for i, tc := range []struct {
		value []byte
		code  uint32
		rows  int64
	}{
		{first, ResultApplied, 1}, {[]byte(`{"n":2}`), ResultApplied, 0}, {[]byte(`invalid`), ResultInvalidDocument, 0},
	} {
		command := fixture.command(t, uint64(i+1), replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutIfAbsent, Key: key, Value: tc.value}}})
		if _, err := fixture.machine.ApplyNormal(normalMeta(uint64(3+i*2)), command); err != nil {
			t.Fatal(err)
		}
		completion, rows, witness := openMutationCompletion(t, fixture.machine, command)
		if completion.ResultCode != tc.code || rows != tc.rows {
			t.Fatalf("completion=%+v rows=%d", completion, rows)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(uint64(4+i*2)), command); err != nil {
			t.Fatal(err)
		}
		_, retriedRows, retried := openMutationCompletion(t, fixture.machine, command)
		if retriedRows != tc.rows || !bytes.Equal(retried, witness) {
			t.Fatal("retry changed outcome")
		}
	}
	raw, found, err := fixture.base.Collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(raw, first) {
		t.Fatalf("raw=%q found=%v err=%v", raw, found, err)
	}
	id := transactionCodecID(0xd2)
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutIfAbsent, Key: key, Value: []byte(`{"n":3}`)}}}}
	applyTransactionCommand(t, fixture.machine, 9, transactionTargetStageCommand(t, fixture, id, batches))
	applyTransactionCommand(t, fixture.machine, 10, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedPrepareTarget, 1))
	result := applyTransactionCommand(t, fixture.machine, 11, transactionTargetTransitionCommand(t, fixture, id, distributedtxn.ReplicatedApplyTarget, 2))
	if !result.AffectedRowsValid || result.AffectedRows != 0 {
		t.Fatalf("transaction skip=%+v", result)
	}
}
