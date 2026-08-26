package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func transactionRelationCodecFixture(
	t testing.TB,
) (replication.RelationBatchView, []byte) {
	t.Helper()
	fixture := newMachineFixture(t)
	command, err := replication.OpenCommand(testCommand(
		fixture.binding, 1, transactionCodecMutation(),
	))
	if err != nil {
		t.Fatal(err)
	}
	relations := command.RelationBatches()
	if !relations.Next() {
		t.Fatal("missing relation batch")
	}
	return relations.Batch(), command.Bytes()
}

func TestTransactionRelationPayloadPackedBorrowedCanonicalAndBounded(t *testing.T) {
	batch, owner := transactionRelationCodecFixture(t)
	id := transactionCodecID(241)
	record, err := AppendTransactionRelationPayload(nil, id, batch)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := TransactionRelationPayloadResidentBytes(len(batch.MutationBytes()))
	if err != nil {
		t.Fatal(err)
	}
	key, err := TransactionRelationPayloadStorageKey(id, batch.Relation)
	if err != nil {
		t.Fatal(err)
	}
	if got := uint64(len(key) + len(record)); got != wantBytes {
		t.Fatalf("resident bytes = %d, want %d", got, wantBytes)
	}
	if transactionRelationPayloadHeaderBytes != 48 {
		t.Fatalf("packed relation header = %d, want 48-byte space gate", transactionRelationPayloadHeaderBytes)
	}
	opened, err := OpenTransactionRelationPayload(record)
	if err != nil {
		t.Fatal(err)
	}
	if opened.ID != id || opened.Relation != batch.Relation ||
		opened.Count != uint32(batch.MutationCount()) ||
		!bytes.Equal(opened.MutationBytes(), batch.MutationBytes()) ||
		cap(opened.Bytes()) != len(opened.Bytes()) ||
		cap(opened.MutationBytes()) != len(opened.MutationBytes()) {
		t.Fatalf("opened packed relation = %+v", opened)
	}
	// Keep the command backing bytes alive: the source relation batch is a
	// borrowed view, while the packed record is independently owned.
	_ = owner
	record[len(record)-recordChecksumLen-1] ^= 1
	if _, err := OpenTransactionRelationPayload(record); !errors.Is(err, ErrTransactionStateCorrupt) {
		t.Fatalf("altered packed relation error = %v", err)
	}
}

func TestTransactionRelationPayloadOpenAllocationGate(t *testing.T) {
	batch, owner := transactionRelationCodecFixture(t)
	record, err := AppendTransactionRelationPayload(nil, transactionCodecID(242), batch)
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		view, openErr := OpenTransactionRelationPayload(record)
		if openErr != nil || len(view.MutationBytes()) == 0 {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("OpenTransactionRelationPayload allocations = %v, want 0", got)
	}
	_ = owner
}

func FuzzTransactionRelationPayloadCanonical(f *testing.F) {
	batch, owner := transactionRelationCodecFixture(f)
	record, err := AppendTransactionRelationPayload(nil, transactionCodecID(244), batch)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(record)
	f.Add(record[:len(record)-1])
	f.Add([]byte("VDBTREL"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		view, openErr := OpenTransactionRelationPayload(raw)
		if openErr != nil {
			return
		}
		canonical, appendErr := AppendTransactionRelationPayload(nil, view.ID, view.Batch)
		if appendErr != nil {
			t.Fatalf("accepted row cannot be re-encoded: %v", appendErr)
		}
		if !bytes.Equal(canonical, raw) {
			t.Fatal("accepted row has a noncanonical representation")
		}
	})
	_ = owner
}

func BenchmarkTransactionRelationPayloadOpen(b *testing.B) {
	batch, owner := transactionRelationCodecFixture(b)
	record, err := AppendTransactionRelationPayload(nil, transactionCodecID(243), batch)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(record)))
	b.ResetTimer()
	for range b.N {
		view, openErr := OpenTransactionRelationPayload(record)
		if openErr != nil || view.Count == 0 {
			b.Fatal(openErr)
		}
	}
	_ = owner
}
