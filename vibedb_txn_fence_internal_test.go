package vibedb

import (
	"path/filepath"
	"sync"
	"testing"
)

// TestFacadeTransactionFencesConcurrentPointPublication deterministically
// exercises the old lost-update window: a direct Put reaches the collection
// fence after COMMIT has validated but before it publishes. The Put must wait,
// then publish after the transaction, so its value is the final value.
func TestFacadeTransactionFencesConcurrentPointPublication(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []Option
	}{
		{name: "memory", options: []Option{WithDurability(Memory)}},
		{name: "durable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			collection := db.Collection("c")
			if _, err := collection.Put("k", []byte("{\"n\":0}")); err != nil {
				t.Fatal(err)
			}
			unrelated := db.Collection("other")
			if _, err := unrelated.Put("seed", []byte("{\"n\":0}")); err != nil {
				t.Fatal(err)
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Collection("c").Put("k", []byte("{\"n\":1}")); err != nil {
				t.Fatal(err)
			}

			blocked := make(chan struct{})
			pointDone := make(chan error, 1)
			var unrelatedErr error
			var blockedOnce sync.Once
			db.testDirectMutationBlocked = func(got *Collection) {
				if got == collection {
					blockedOnce.Do(func() { close(blocked) })
				}
			}
			db.testAfterTxValidation = func() {
				// A different collection has a different fence and must remain
				// writable while this transaction owns c's publication interval.
				_, unrelatedErr = unrelated.Put("live", []byte("{\"n\":1}"))
				go func() {
					_, putErr := collection.Put("k", []byte("{\"n\":2}"))
					pointDone <- putErr
				}()
				<-blocked
			}

			if err := tx.Commit(); err != nil {
				t.Fatalf("transaction commit: %v", err)
			}
			if unrelatedErr != nil {
				t.Fatalf("unrelated collection write was blocked or failed: %v", unrelatedErr)
			}
			if err := <-pointDone; err != nil {
				t.Fatalf("direct Put after transaction: %v", err)
			}
			got, ok, err := collection.Get("k")
			if err != nil || !ok {
				t.Fatalf("final Get = %q, %t, %v", got, ok, err)
			}
			if string(got) != "{\"n\":2}" {
				t.Fatalf("lost direct write: final document = %s, want {\"n\":2}", got)
			}
		})
	}
}
