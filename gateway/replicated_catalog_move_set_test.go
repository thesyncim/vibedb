package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestSubmitOperationsAtomicallyAdmitsCanonicalMoveSetAndSettlesUnknown(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	first := testReplicatedOperation(ReplicatedOperationRecord{ID: [32]byte{0x31},
		Kind: ReplicatedOperationMove, State: ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: 5, Cursor: [8]uint64{1}, Proof: [32]byte{0x41}})
	second := first
	second.ID, second.Proof = [32]byte{0x21}, [32]byte{0x42}
	second.Intent = append([]byte(nil), first.Intent...)
	second.IntentDigest = sha256.Sum256(second.Intent)

	client.unknownNext = true
	err := authority.SubmitOperations(context.Background(), []ReplicatedOperationRecord{first, second})
	if !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown move-set admission err=%v", err)
	}
	pending := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("move-set retry changed replicated command bytes")
	}
	ids, err := authority.ReadOperationIDs(context.Background())
	if err != nil || len(ids) != 2 || ids[0] != second.ID || ids[1] != first.ID {
		t.Fatalf("move-set directory=%x err=%v", ids, err)
	}
	for _, want := range []ReplicatedOperationRecord{first, second} {
		got, readErr := authority.ReadOperation(context.Background(), want.ID)
		if readErr != nil || !got.Equal(want) {
			t.Fatalf("operation %x got=%+v err=%v", want.ID, got, readErr)
		}
	}

	// Input order is irrelevant; exact replay is one idempotent transaction.
	if err = authority.SubmitOperations(context.Background(), []ReplicatedOperationRecord{second, first}); err != nil {
		t.Fatalf("exact move-set replay: %v", err)
	}
}

func TestSubmitOperationsConflictPublishesNoneOfNewMoveSet(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	existing := testReplicatedOperation(ReplicatedOperationRecord{ID: [32]byte{0x51},
		Kind: ReplicatedOperationMove, State: ReplicatedOperationPlanned, Revision: 1,
		CatalogGeneration: 5, Cursor: [8]uint64{1}, Proof: [32]byte{0x61}})
	if err := authority.SubmitOperation(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	conflict := existing
	conflict.Proof[0]++
	newRecord := existing
	newRecord.ID, newRecord.Proof = [32]byte{0x52}, [32]byte{0x62}
	newRecord.Intent = append([]byte(nil), existing.Intent...)
	newRecord.IntentDigest = sha256.Sum256(newRecord.Intent)
	if err := authority.SubmitOperations(context.Background(), []ReplicatedOperationRecord{newRecord, conflict}); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("conflicting move-set err=%v", err)
	}
	if _, err := authority.ReadOperation(context.Background(), newRecord.ID); !errors.Is(err, ErrReplicatedOperationMissing) {
		t.Fatalf("partial move-set admission err=%v", err)
	}
	ids, err := authority.ReadOperationIDs(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != existing.ID {
		t.Fatalf("directory after conflict=%x err=%v", ids, err)
	}
}
