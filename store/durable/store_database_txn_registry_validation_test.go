package durable

import (
	"errors"
	"testing"
)

func TestTxnLogRejectedMemberDoesNotContaminateRegistry(t *testing.T) {
	tests := []struct {
		name        string
		invalid     func(*testing.T, string) NamedCollection
		wantInvalid error
	}{
		{
			name: "wrong directory",
			invalid: func(t *testing.T, _ string) NamedCollection {
				return openTxnNamedCollection(
					t, t.TempDir(), "foreign", txnTestOptions(),
				)
			},
			wantInvalid: ErrTransactionLogDirectoryMismatch,
		},
		{
			name: "stale closed handle",
			invalid: func(t *testing.T, dir string) NamedCollection {
				stale := openTxnNamedCollection(
					t, dir, "stale", txnTestOptions(),
				)
				if err := stale.Collection.Close(); err != nil {
					t.Fatalf("close stale participant: %v", err)
				}
				return stale
			},
			wantInvalid: ErrTxnParticipant,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			a := openTxnNamedCollection(t, dir, "a", txnTestOptions())
			b := openTxnNamedCollection(t, dir, "b", txnTestOptions())
			invalid := tc.invalid(t, dir)
			log, err := NewTxnLog(dir, TxnLogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })

			err = UpdateCollections(
				log, []NamedCollection{a, invalid}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					left, err := batch.Collection("a")
					if err != nil {
						return err
					}
					right, err := batch.Collection(invalid.Name)
					if err != nil {
						return err
					}
					if err := left.Put([]byte("rejected"), []byte(`{"n":1}`)); err != nil {
						return err
					}
					return right.Put([]byte("rejected"), []byte(`{"n":1}`))
				},
			)
			if !errors.Is(err, tc.wantInvalid) {
				t.Fatalf("invalid commit = %v, want %v", err, tc.wantInvalid)
			}
			if registered := log.registeredCollections(); len(registered) != 0 {
				t.Fatalf("invalid commit registered %d collections, want 0", len(registered))
			}

			if err := UpdateCollections(
				log, []NamedCollection{a, b}, defaultTxnLimits(),
				func(batch *DatabaseBatch) error {
					return putTxnPair(t, batch, "a", "b")
				},
			); err != nil {
				t.Fatalf("valid A+B retry after rejected member: %v", err)
			}
			for _, member := range []NamedCollection{a, b} {
				doc, found := collectionDoc(t, member.Collection, "k")
				if !found || doc != `{"n":1}` {
					t.Fatalf("%s/k = %q,%v, want committed", member.Name, doc, found)
				}
			}
		})
	}
}
