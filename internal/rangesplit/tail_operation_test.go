package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestWitnessedTailOperationCanonicalBorrowedRoundTrip(t *testing.T) {
	want := TailOperation{Relation: 1, Kind: replication.MutationPut, Key: []byte("key"), Value: []byte(`{"a":2}`),
		BeforeWitness: TailBeforeWitness{Present: true, DocumentBytes: 7, Digest: sha256.Sum256([]byte(`{"a":1}`)), Point: distribution.KeyspacePoint{4}}}
	raw := make([]byte, tailBatchOperationHeaderBytes)
	putTailOperationHeader(raw, want)
	raw = append(raw, want.Key...)
	raw = append(raw, want.Value...)
	got, remaining, ok := openTailWireOperation(raw)
	if !ok || len(remaining) != 0 || got.Relation != want.Relation || got.Kind != want.Kind || got.BeforeWitness != want.BeforeWitness || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
		t.Fatal("witnessed frame mismatch")
	}
	if &got.Key[0] != &raw[56] || &got.Value[0] != &raw[59] {
		t.Fatal("operation copied borrowed payload")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, _, ok := openTailWireOperation(raw); !ok {
			panic("decode")
		}
	}); allocs != 0 {
		t.Fatalf("decode allocated %g", allocs)
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 0 }, func(b []byte) { b[0] |= 4 }, func(b []byte) { b[1] = 1 },
		func(b []byte) { binary.LittleEndian.PutUint16(b[2:4], 0) },
		func(b []byte) { binary.LittleEndian.PutUint16(b[2:4], uint16(replication.MaxRelationID+1)) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[4:8], 0) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[8:12], 0) },
		func(b []byte) { binary.LittleEndian.PutUint32(b[12:16], ^uint32(0)) },
		func(b []byte) { clear(b[24:56]) },
		func(b []byte) { b[0] &^= sourceCaptureBeforePresent },
		func(b []byte) { b[0] &^= sourceCaptureAfterPresent },
	} {
		changed := bytes.Clone(raw)
		mutate(changed)
		if _, _, ok := openTailWireOperation(changed); ok {
			t.Fatal("malformed operation accepted")
		}
	}
	for n := 0; n < len(raw); n++ {
		if _, _, ok := openTailWireOperation(raw[:n]); ok {
			t.Fatalf("truncated operation %d accepted", n)
		}
	}
	if validTailOperation(want, want) {
		t.Fatal("duplicate relation/key accepted")
	}
	previous := want
	previous.Relation = 2
	if validTailOperation(want, previous) {
		t.Fatal("reordered relation accepted")
	}
	previous = want
	previous.Key = []byte("z")
	if validTailOperation(want, previous) {
		t.Fatal("reordered key accepted")
	}
}

func TestTailBatchWirePreservesEveryLocalWitness(t *testing.T) {
	stage, batch, _, _ := witnessedStageFixture(t)
	opened := wireWitnessedBatch(t, batch)
	if err := stage.partitioner.VerifyTailBatch(opened, &TailBatchVerifyWorkspace{}); err != nil {
		t.Fatal(err)
	}
	local, wire := batch.Iterator(), opened.Iterator()
	for local.Next() {
		if !wire.Next() {
			t.Fatal("wire lost operation")
		}
		a, b := local.Operation(), wire.Operation()
		if a.Relation != b.Relation || a.Kind != b.Kind || a.BeforeWitness != b.BeforeWitness || !bytes.Equal(a.Key, b.Key) || !bytes.Equal(a.Value, b.Value) {
			t.Fatal("wire lost witness")
		}
	}
	if wire.Next() {
		t.Fatal("wire added operation")
	}
}
