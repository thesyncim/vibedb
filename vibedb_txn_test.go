package vibedb_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFacadeTxCRUDAndQueryMatrix(t *testing.T) {
	t.Parallel()
	for _, profile := range []vibedb.Durability{
		vibedb.Durable, vibedb.Buffered, vibedb.Memory,
	} {
		profile := profile
		t.Run(profileName(profile), func(t *testing.T) {
			t.Parallel()
			db := openFacadeDB(t, profile)
			defer db.Close()

			if err := db.Update(func(tx *vibedb.Tx) error {
				users := tx.Collection("users")
				created, err := users.Put("u1", []byte(`{"name":"Ada"}`))
				if err != nil || !created {
					return fmt.Errorf("users put: created=%v err=%v", created, err)
				}
				got, ok, err := users.Get("u1")
				if err != nil || !ok || string(got) != `{"name":"Ada"}` {
					return fmt.Errorf("read-your-write get = %q,%v,%v", got, ok, err)
				}
				q := query.Select(query.Path("name")).
					Where(query.Cmp("name", query.Eq, "Ada"))
				result, err := users.Run(q)
				if err != nil {
					return err
				}
				if result.RowCount != 1 {
					return fmt.Errorf("query rows = %d", result.RowCount)
				}
				result.Release()
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *vibedb.Tx) error {
				_, err := tx.Collection("orders").Put("o1", []byte(`{"user":"u1"}`))
				return err
			}); err != nil {
				t.Fatal(err)
			}

			users := db.Collection("users")
			got, ok, err := users.Get("u1")
			if err != nil || !ok || string(got) != `{"name":"Ada"}` {
				t.Fatalf("after commit Get = %q,%v,%v", got, ok, err)
			}

			if profile == vibedb.Buffered {
				err := db.Update(func(tx *vibedb.Tx) error {
					if _, err := tx.Collection("users").Put("u2", []byte(`{"name":"Grace"}`)); err != nil {
						return err
					}
					if _, err := tx.Collection("orders").Put("o2", []byte(`{"user":"u2"}`)); err != nil {
						return err
					}
					return nil
				})
				if !errors.Is(err, vibedb.ErrTxUnsupportedLane) {
					t.Fatalf("buffered multi err = %v", err)
				}
				if _, ok, err := users.Get("u2"); err != nil || ok {
					t.Fatalf("refused write leaked: ok=%v err=%v", ok, err)
				}
				return
			}

			if err := db.Update(func(tx *vibedb.Tx) error {
				if _, err := tx.Collection("users").Put("u2", []byte(`{"name":"Grace"}`)); err != nil {
					return err
				}
				if _, err := tx.Collection("orders").Put("o2", []byte(`{"user":"u2"}`)); err != nil {
					return err
				}
				deleted, err := tx.Collection("orders").Delete("o1")
				if err != nil || !deleted {
					return fmt.Errorf("delete o1: %v deleted=%v", err, deleted)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := db.Collection("orders").Get("o1"); err != nil || ok {
				t.Fatalf("deleted key still present: ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestFacadeTxConflictRetry(t *testing.T) {
	t.Parallel()
	for _, profile := range []vibedb.Durability{vibedb.Durable, vibedb.Memory} {
		profile := profile
		t.Run(profileName(profile), func(t *testing.T) {
			t.Parallel()
			db := openFacadeDB(t, profile)
			defer db.Close()
			if _, err := db.Collection("c").Put("k", []byte(`{"n":1}`)); err != nil {
				t.Fatal(err)
			}

			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Collection("c").Put("k", []byte(`{"n":2}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Collection("c").Put("k", []byte(`{"n":3}`)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, vibedb.ErrTxConflict) {
				t.Fatalf("commit err = %v", err)
			}

			var attempts int
			err = db.Update(func(tx *vibedb.Tx) error {
				attempts++
				got, ok, err := tx.Collection("c").Get("k")
				if err != nil || !ok {
					return fmt.Errorf("get: %v ok=%v", err, ok)
				}
				if string(got) != `{"n":3}` {
					return fmt.Errorf("got %s", got)
				}
				_, err = tx.Collection("c").Put("k", []byte(`{"n":4}`))
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d", attempts)
			}
			got, ok, err := db.Collection("c").Get("k")
			if err != nil || !ok || string(got) != `{"n":4}` {
				t.Fatalf("final = %q,%v,%v", got, ok, err)
			}
		})
	}
}

func TestFacadeTxPanicRollback(t *testing.T) {
	t.Parallel()
	db := openFacadeDB(t, vibedb.Memory)
	defer db.Close()
	if _, err := db.Collection("c").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = db.Update(func(tx *vibedb.Tx) error {
			if _, err := tx.Collection("c").Put("k", []byte(`{"n":2}`)); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()
	got, ok, err := db.Collection("c").Get("k")
	if err != nil || !ok || string(got) != `{"n":1}` {
		t.Fatalf("after panic = %q,%v,%v", got, ok, err)
	}
}

func TestFacadeTxEscapedHandleInert(t *testing.T) {
	t.Parallel()
	db := openFacadeDB(t, vibedb.Durable)
	defer db.Close()
	var escaped *vibedb.TxCollection
	if err := db.Update(func(tx *vibedb.Tx) error {
		escaped = tx.Collection("c")
		_, err := escaped.Put("k", []byte(`{"n":1}`))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.Put("k", []byte(`{"n":2}`)); !errors.Is(err, vibedb.ErrTxDone) {
		t.Fatalf("escaped put = %v", err)
	}
	if _, _, err := escaped.Get("k"); !errors.Is(err, vibedb.ErrTxDone) {
		t.Fatalf("escaped get = %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, vibedb.ErrTxDone) {
		t.Fatalf("second commit = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback after commit = %v", err)
	}
}

func TestFacadeTxBoundsRefusals(t *testing.T) {
	t.Parallel()
	db, err := vibedb.Open(filepath.Join(t.TempDir(), "bounds.vdb"),
		vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
			Durability: vibedb.Memory,
			TxnLimits: durable.TxnLimits{
				MaxCollections: 1,
				MaxDocuments:   2,
				MaxBytes:       64,
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *vibedb.Tx) error {
		if _, err := tx.Collection("a").Put("k1", []byte(`{"n":1}`)); err != nil {
			return err
		}
		_, err := tx.Collection("b").Put("k1", []byte(`{"n":1}`))
		return err
	})
	if !errors.Is(err, vibedb.ErrTxTooLarge) {
		t.Fatalf("collections bound = %v", err)
	}

	err = db.Update(func(tx *vibedb.Tx) error {
		c := tx.Collection("a")
		if _, err := c.Put("k1", []byte(`{"n":1}`)); err != nil {
			return err
		}
		if _, err := c.Put("k2", []byte(`{"n":2}`)); err != nil {
			return err
		}
		_, err := c.Put("k3", []byte(`{"n":3}`))
		return err
	})
	if !errors.Is(err, vibedb.ErrTxTooLarge) {
		t.Fatalf("documents bound = %v", err)
	}

	big := make([]byte, 0, 128)
	big = append(big, `{"n":"`...)
	for len(big) < 100 {
		big = append(big, 'x')
	}
	big = append(big, `"}`...)
	err = db.Update(func(tx *vibedb.Tx) error {
		_, err := tx.Collection("a").Put("k", big)
		return err
	})
	if !errors.Is(err, vibedb.ErrTxTooLarge) {
		t.Fatalf("bytes bound = %v", err)
	}
}

func TestFacadeTxViewReadOnly(t *testing.T) {
	t.Parallel()
	db := openFacadeDB(t, vibedb.Memory)
	defer db.Close()
	if _, err := db.Collection("c").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	err := db.View(func(tx *vibedb.Tx) error {
		got, ok, err := tx.Collection("c").Get("k")
		if err != nil || !ok || string(got) != `{"n":1}` {
			return fmt.Errorf("view get = %q,%v,%v", got, ok, err)
		}
		_, err = tx.Collection("c").Put("k", []byte(`{"n":2}`))
		return err
	})
	if !errors.Is(err, vibedb.ErrTxReadOnly) {
		t.Fatalf("view mutate = %v", err)
	}
}

func TestFacadeTxNestedUpdate(t *testing.T) {
	t.Parallel()
	db := openFacadeDB(t, vibedb.Memory)
	defer db.Close()
	err := db.Update(func(tx *vibedb.Tx) error {
		return db.Update(func(*vibedb.Tx) error { return nil })
	})
	if !errors.Is(err, vibedb.ErrTxNested) {
		t.Fatalf("nested = %v", err)
	}
}

func TestFacadeTxUnknownOutcomePoisonsDatabase(t *testing.T) {
	db := openFacadeDB(t, vibedb.Durable)
	defer db.Close()
	if _, err := db.Collection("a").Put("seed", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Collection("b").Put("seed", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}

	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	defer restore()

	err := db.Update(func(tx *vibedb.Tx) error {
		if _, err := tx.Collection("a").Put("k", []byte(`{"n":1}`)); err != nil {
			return err
		}
		_, err := tx.Collection("b").Put("k", []byte(`{"n":1}`))
		return err
	})
	if !errors.Is(err, vibedb.ErrCommitOutcomeUnknown) {
		t.Fatalf("err = %v want ErrCommitOutcomeUnknown", err)
	}
	if _, err := db.Collection("a").Put("later", []byte(`{"n":2}`)); !errors.Is(err, vibedb.ErrCommitOutcomeUnknown) {
		t.Fatalf("later a = %v", err)
	}
	if _, err := db.Collection("b").Put("later", []byte(`{"n":2}`)); !errors.Is(err, vibedb.ErrCommitOutcomeUnknown) {
		t.Fatalf("later b = %v", err)
	}
}

func TestFacadeTxReadPathAllocations(t *testing.T) {
	db := openFacadeDB(t, vibedb.Memory)
	defer db.Close()
	if err := db.Update(func(tx *vibedb.Tx) error {
		c := tx.Collection("c")
		for i := 0; i < 32; i++ {
			if _, err := c.Put(fmt.Sprintf("k%d", i), []byte(`{"n":1}`)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginReadOnly()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	c := tx.Collection("c")
	dst := make([]byte, 0, 64)
	if _, ok, err := c.Append(dst[:0], "k0"); err != nil || !ok {
		t.Fatalf("warm Append: %v ok=%v", err, ok)
	}
	allocs := testing.AllocsPerRun(100, func() {
		out, ok, err := c.Append(dst[:0], "k0")
		if err != nil || !ok || len(out) == 0 {
			panic("Append failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("Append allocated %.2f times, want 0", allocs)
	}
}

func TestFacadeTxBeginCommitRaceWithPointWrites(t *testing.T) {
	db := openFacadeDB(t, vibedb.Memory)
	defer db.Close()
	if _, err := db.Collection("c").Put("k", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; !stop.Load(); i++ {
			if _, err := db.Collection("c").Put("k", []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
				t.Errorf("point write: %v", err)
				return
			}
			runtime.Gosched()
		}
	}()

	for i := 0; i < 200; i++ {
		tx, err := db.Begin()
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
		if _, err := tx.Collection("c").Put("k", []byte(`{"n":-1}`)); err != nil {
			_ = tx.Rollback()
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
		err = tx.Commit()
		if err != nil && !errors.Is(err, vibedb.ErrTxConflict) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("commit: %v", err)
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestFacadeTxFirstWriteCreatesInsideTransaction(t *testing.T) {
	t.Parallel()
	db := openFacadeDB(t, vibedb.Durable)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Collection("new").Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Collection("new").Get("k"); err != nil || ok {
		t.Fatalf("rollback must not create: ok=%v err=%v", ok, err)
	}

	if err := db.Update(func(tx *vibedb.Tx) error {
		_, err := tx.Collection("new").Put("k", []byte(`{"n":1}`))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Collection("new").Get("k")
	if err != nil || !ok || string(got) != `{"n":1}` {
		t.Fatalf("created inside txn = %q,%v,%v", got, ok, err)
	}
}

func openFacadeDB(t *testing.T, profile vibedb.Durability) *vibedb.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.vdb")
	db, err := vibedb.Open(path, vibedb.WithDurability(profile))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func profileName(profile vibedb.Durability) string {
	switch profile {
	case vibedb.Durable:
		return "durable"
	case vibedb.Buffered:
		return "buffered"
	case vibedb.Memory:
		return "memory"
	default:
		return fmt.Sprintf("profile-%d", profile)
	}
}
