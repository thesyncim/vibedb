package vibedb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFacadeReadOnlyCutPinsUnvisitedCollections(t *testing.T) {
	for _, profile := range []Durability{Memory, Buffered, Durable} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db := openSerializableDB(t, profile)
			defer db.Close()
			for _, name := range []string{"a", "b"} {
				if _, err := db.Collection(name).Put("k", []byte(`{"n":0}`)); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginReadOnly()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for _, name := range []string{"a", "b", "created_later"} {
				if _, err := db.Collection(name).Put("k", []byte(`{"n":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"a", "b"} {
				got, found, err := tx.Collection(name).Get("k")
				if err != nil || !found || string(got) != `{"n":0}` {
					t.Errorf("first read of %s = %s, %v, %v; want begin-cut value", name, got, found, err)
				}
			}
			if got, found, err := tx.Collection("created_later").Get("k"); err != nil || found {
				t.Errorf("collection created after BeginReadOnly = %s, %v, %v; want absent", got, found, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFacadeViewDoesNotFractureAtomicCommit(t *testing.T) {
	for _, profile := range []Durability{Memory, Durable} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db := openSerializableDB(t, profile)
			defer db.Close()
			for _, name := range []string{"a", "b"} {
				if _, err := db.Collection(name).Put("k", []byte(`{"n":0}`)); err != nil {
					t.Fatal(err)
				}
			}
			writer, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Rollback()
			for _, name := range []string{"a", "b"} {
				if _, err := writer.Collection(name).Put("k", []byte(`{"n":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			validated := make(chan struct{})
			release := make(chan struct{})
			db.testAfterTxValidation = func() {
				close(validated)
				<-release
			}
			committed := make(chan error, 1)
			go func() { committed <- writer.Commit() }()
			<-validated
			err = db.View(func(reader *Tx) error {
				first, firstFound, firstErr := reader.Collection("a").Get("k")
				close(release)
				if err := <-committed; err != nil {
					return err
				}
				second, secondFound, secondErr := reader.Collection("b").Get("k")
				if firstErr != nil || secondErr != nil || !firstFound || !secondFound ||
					string(first) != `{"n":0}` || string(second) != `{"n":0}` {
					return fmt.Errorf("view crossed atomic publication: a=%s (%v), b=%s (%v)", first, firstErr, second, secondErr)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFacadeNoWriteCommitRejectsFracturedReads(t *testing.T) {
	for _, profile := range []Durability{Memory, Durable} {
		for _, netZero := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/net-zero-%v", profile, netZero), func(t *testing.T) {
				db := openSerializableDB(t, profile)
				defer db.Close()
				for _, name := range []string{"a", "b"} {
					if _, err := db.Collection(name).Put("k", []byte(`{"n":0}`)); err != nil {
						t.Fatal(err)
					}
				}
				reader, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Rollback()
				first, found, err := reader.Collection("a").Get("k")
				if err != nil || !found || string(first) != `{"n":0}` {
					t.Fatalf("first read = %s, %v, %v", first, found, err)
				}
				if netZero {
					c := reader.Collection("a")
					if _, err := c.Put("temporary", []byte(`{"n":2}`)); err != nil {
						t.Fatal(err)
					}
					if _, err := c.Delete("temporary"); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Update(func(writer *Tx) error {
					for _, name := range []string{"a", "b"} {
						if _, err := writer.Collection(name).Put("k", []byte(`{"n":1}`)); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if _, found, err := reader.Collection("b").Get("k"); err != nil || !found {
					t.Fatalf("second read: found=%v err=%v", found, err)
				}
				if err := reader.Commit(); !errors.Is(err, ErrTxConflict) {
					t.Fatalf("no-write commit = %v, want conflict", err)
				}
			})
		}
	}
}

func TestFacadeRejectedTransactionCollectionsDoNotRetainHandles(t *testing.T) {
	for _, profile := range []Durability{Memory, Buffered, Durable} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db := openSerializableDB(t, profile)
			defer db.Close()
			if _, err := db.Collection("existing").Put("k", []byte(`{"n":0}`)); err != nil {
				t.Fatal(err)
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for i := range maxSerializableReadCollections {
				if _, _, err := tx.Collection(fmt.Sprintf("accepted_%03d", i)).Get("k"); err != nil {
					t.Fatal(err)
				}
			}
			retained := len(db.handles)
			for i := range maxSerializableReadCollections {
				if _, _, err := tx.Collection(fmt.Sprintf("rejected_%03d", i)).Get("k"); !errors.Is(err, ErrTxTooLarge) {
					t.Fatalf("rejected collection %d: %v", i, err)
				}
			}
			if len(db.handles) != retained {
				t.Fatalf("rejected transaction reads retained %d database handles; want %d", len(db.handles), retained)
			}
			if err := tx.Collection("existing").initialErr; err != nil {
				t.Fatalf("existing collection consumed the absent-name budget: %v", err)
			}
		})
	}
}

func TestFacadeReadOnlyCutDuringDisjointCommits(t *testing.T) {
	for _, profile := range []Durability{Memory, Buffered, Durable} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db := openSerializableDB(t, profile)
			defer db.Close()
			var writers []*Tx
			for _, name := range []string{"a", "b"} {
				if _, err := db.Collection(name).Put("k", []byte(`{"n":0}`)); err != nil {
					t.Fatal(err)
				}
				writer, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Rollback()
				if _, err := writer.Collection(name).Put("k", []byte(`{"n":1}`)); err != nil {
					t.Fatal(err)
				}
				writers = append(writers, writer)
			}
			validated := make(chan struct{}, len(writers))
			release := make(chan struct{})
			db.testAfterTxValidation = func() {
				validated <- struct{}{}
				<-release
			}
			var wg sync.WaitGroup
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() {
				unblock()
				wg.Wait()
			}()
			committed := make(chan error, len(writers))
			for _, writer := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					committed <- writer.Commit()
				}()
			}
			for range writers {
				select {
				case <-validated:
				case <-time.After(10 * time.Second):
					t.Fatal("disjoint commits did not concurrently reach publication")
				}
			}
			reader, err := db.BeginReadOnly()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback()
			unblock()
			for range writers {
				if err := <-committed; err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"a", "b"} {
				got, found, err := reader.Collection(name).Get("k")
				if err != nil || !found || string(got) != `{"n":0}` {
					t.Fatalf("read-only cut during disjoint commits: %s=%s, found=%v, err=%v", name, got, found, err)
				}
			}
		})
	}
}
