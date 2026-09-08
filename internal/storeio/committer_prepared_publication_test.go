package storeio

import (
	"errors"
	"testing"
	"time"
)

func beginPreparedInlineTest(t *testing.T, generation uint64) (
	*Committer, *WriteTransaction, *PreparedInlinePublication,
) {
	t.Helper()
	committer, _, pageSize := newPortableCommitter(t, 4, 0)
	layout, err := MutableStoreLayout(uint32(pageSize))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := BeginWriteTransaction(committer, nil, 0, WriteTransactionOptions{
		StoreID:       testStoreID,
		Generation:    generation,
		PageSize:      uint32(pageSize),
		FileEnd:       layout.DataStart,
		NextLogicalID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	state := StateRoot{
		StoreID:       testStoreID,
		Generation:    generation,
		PageSize:      uint32(pageSize),
		MaxPageSize:   uint32(pageSize),
		NextLogicalID: tx.NextLogicalID(),
	}
	publication, err := tx.PrepareInlinePublication(state, InlineFreeDelta{})
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	return committer, tx, publication
}

func TestPreparedInlinePublicationCancelAndPublish(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		if err := publication.Cancel(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := committer.Close(); err != nil {
			t.Fatal(err)
		}
		if got := committer.PublishedGeneration(); got != 0 {
			t.Fatalf("cancel published generation %d, want 0", got)
		}
	})

	t.Run("publish", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		if err := publication.PublishInline(); err != nil {
			t.Fatal(err)
		}
		if tx.Abort() == nil || committer.PublishedGeneration() != 1 {
			t.Fatal("published transaction remained abortable or did not advance generation")
		}
		if err := committer.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPreparedInlinePublicationBlocksCloseAndFailure(t *testing.T) {
	t.Run("close-cancel", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		closed := make(chan error, 1)
		started := make(chan struct{})
		go func() {
			close(started)
			closed <- committer.Close()
		}()
		<-started
		select {
		case err := <-closed:
			t.Fatalf("Close completed before prepared token settled: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if err := publication.Cancel(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Abort(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not finish after prepared token cancellation")
		}
	})

	t.Run("close-publish", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		closed := make(chan error, 1)
		started := make(chan struct{})
		go func() {
			close(started)
			closed <- committer.Close()
		}()
		<-started
		select {
		case err := <-closed:
			t.Fatalf("Close completed before prepared token settled: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if err := publication.PublishInline(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Abort(); err == nil {
			t.Fatal("published transaction remained abortable")
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not finish after prepared token publication")
		}
	})

	t.Run("failure-cancel", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		failure := errors.New("prepared publication test failure")
		failed := make(chan struct{})
		started := make(chan struct{})
		go func() {
			close(started)
			committer.setFailure(failure)
			close(failed)
		}()
		<-started
		select {
		case <-failed:
			t.Fatal("worker failure crossed an active prepared publication")
		case <-time.After(20 * time.Millisecond):
		}
		if err := publication.Cancel(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Abort(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("failure did not finish after prepared token cancellation")
		}
		if got := committer.currentFailure(); !errors.Is(got, failure) {
			t.Fatalf("sticky failure = %v, want %v", got, failure)
		}
		if closeErr := committer.Close(); !errors.Is(closeErr, failure) {
			t.Fatalf("Close failure = %v, want %v", closeErr, failure)
		}
	})

	t.Run("failure-publish", func(t *testing.T) {
		committer, tx, publication := beginPreparedInlineTest(t, 1)
		failure := errors.New("prepared publication test failure")
		failed := make(chan struct{})
		started := make(chan struct{})
		go func() {
			close(started)
			committer.setFailure(failure)
			close(failed)
		}()
		<-started
		select {
		case <-failed:
			t.Fatal("worker failure crossed an active prepared publication")
		case <-time.After(20 * time.Millisecond):
		}
		if err := publication.PublishInline(); err != nil {
			t.Fatal(err)
		}
		if got := committer.PublishedGeneration(); got != 1 {
			t.Fatalf("published generation = %d, want 1", got)
		}
		if err := tx.Abort(); err == nil {
			t.Fatal("published transaction remained abortable")
		}
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("failure did not finish after prepared token publication")
		}
		if got := committer.currentFailure(); !errors.Is(got, failure) {
			t.Fatalf("sticky failure = %v, want %v", got, failure)
		}
		if closeErr := committer.Close(); !errors.Is(closeErr, failure) {
			t.Fatalf("Close failure = %v, want %v", closeErr, failure)
		}
	})
}
