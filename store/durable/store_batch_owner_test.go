package durable

import (
	"errors"
	"testing"
)

func requireClosedWriteBatch(t *testing.T, batch *WriteBatch) {
	t.Helper()
	if batch == nil {
		t.Fatal("missing retained WriteBatch")
	}
	if got := batch.Len(); got != 0 {
		t.Fatalf("closed WriteBatch.Len() = %d, want 0", got)
	}
	if err := batch.Put([]byte("late"), []byte(`{"v":9}`)); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed WriteBatch.Put = %v, want ErrBatchClosed", err)
	}
	if err := batch.Delete([]byte("late")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("closed WriteBatch.Delete = %v, want ErrBatchClosed", err)
	}
}

func requireClosedDatabaseBatch(
	t *testing.T,
	batch *DatabaseBatch,
	collection *Collection,
) {
	t.Helper()
	if batch == nil {
		t.Fatal("missing retained DatabaseBatch")
	}
	if _, err := batch.Collection("user"); !errors.Is(err, ErrTxnCollection) {
		t.Fatalf("closed DatabaseBatch.Collection = %v, want ErrTxnCollection", err)
	}
	if _, err := batch.CollectionHandle(collection); !errors.Is(err, ErrTxnCollection) {
		t.Fatalf("closed DatabaseBatch.CollectionHandle = %v, want ErrTxnCollection", err)
	}
}

func newBatchOwnerCheckpointGroupTestStore(
	t testing.TB, maxBatchDocuments int,
) ([]NamedCollection, *CheckpointGroup) {
	t.Helper()
	dir := t.TempDir()
	options := txnTestOptions()
	options.MaxBatchDocuments = maxBatchDocuments
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 8,
	})
	if err != nil {
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return members, group
}

func TestCollectionUpdateBatchOwnerCannotBeReused(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	var retained *WriteBatch
	var retainedCopy *WriteBatch
	if err := collection.Update(func(batch *WriteBatch) error {
		retained = batch
		copy := *batch
		retainedCopy = &copy
		if err := batch.Put([]byte("k"), []byte(`{"v":1}`)); err != nil {
			return err
		}
		if got := retainedCopy.Len(); got != 0 {
			t.Fatalf("shallow copy Len() = %d during owner callback, want 0", got)
		}
		if err := retainedCopy.Put([]byte("copy"), []byte(`{"v":1}`)); !errors.Is(err, ErrBatchClosed) {
			t.Fatalf("shallow copy Put = %v, want ErrBatchClosed", err)
		}
		if err := retainedCopy.Delete([]byte("k")); !errors.Is(err, ErrBatchClosed) {
			t.Fatalf("shallow copy Delete = %v, want ErrBatchClosed", err)
		}
		// A rejected shallow copy must not poison the actual callback handle.
		return batch.Put([]byte("k"), []byte(`{"v":2}`))
	}); err != nil {
		t.Fatal(err)
	}
	requireClosedWriteBatch(t, retained)
	requireClosedWriteBatch(t, retainedCopy)

	if err := collection.Update(func(batch *WriteBatch) error {
		if batch == retained {
			t.Fatal("next Update reused the public WriteBatch pointer")
		}
		requireClosedWriteBatch(t, retained)
		requireClosedWriteBatch(t, retainedCopy)
		return batch.Put([]byte("next"), []byte(`{"v":3}`))
	}); err != nil {
		t.Fatal(err)
	}
	if got, found, err := collection.AppendRaw(nil, []byte("k")); err != nil || !found || string(got) != `{"v":2}` {
		t.Fatalf("published k = %q/%v/%v, want v=2", got, found, err)
	}
	if got, found, err := collection.AppendRaw(nil, []byte("next")); err != nil || !found || string(got) != `{"v":3}` {
		t.Fatalf("published next = %q/%v/%v, want v=3", got, found, err)
	}
	if _, found, err := collection.AppendRaw(nil, []byte("copy")); err != nil || found {
		t.Fatalf("shallow-copy mutation became visible: found=%v err=%v", found, err)
	}
}

func TestCollectionUpdateReleasesBatchAfterCallbackFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		fail func(*WriteBatch) error
	}{
		{
			name: "error",
			fail: func(batch *WriteBatch) error {
				return errBatchOwnerCallback
			},
		},
		{
			name: "panic",
			fail: func(batch *WriteBatch) error {
				panic(errBatchOwnerCallback)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			collection, _ := openBatchCollection(t, testBatchOptions(8))
			var retained *WriteBatch
			invoke := func() (err error, recovered any) {
				defer func() { recovered = recover() }()
				err = collection.Update(func(batch *WriteBatch) error {
					retained = batch
					if putErr := batch.Put([]byte("failed"), []byte(`{"v":1}`)); putErr != nil {
						return putErr
					}
					return test.fail(batch)
				})
				return err, nil
			}
			err, recovered := invoke()
			if test.name == "error" {
				if !errors.Is(err, errBatchOwnerCallback) || recovered != nil {
					t.Fatalf("callback result = %v, panic=%v", err, recovered)
				}
			} else if err != nil || recovered != errBatchOwnerCallback {
				t.Fatalf("callback result = %v, panic=%v", err, recovered)
			}
			requireClosedWriteBatch(t, retained)
			if collection.Len() != 0 {
				t.Fatalf("failed callback published %d rows", collection.Len())
			}
			if err := collection.Update(func(batch *WriteBatch) error {
				requireClosedWriteBatch(t, retained)
				return batch.Put([]byte("ok"), []byte(`{"v":2}`))
			}); err != nil {
				t.Fatalf("Update after %s: %v", test.name, err)
			}
			if got, found, err := collection.AppendRaw(nil, []byte("ok")); err != nil || !found || string(got) != `{"v":2}` {
				t.Fatalf("post-failure row = %q/%v/%v", got, found, err)
			}
		})
	}
}

func TestCollectionUpdateHandledAdmissionErrorIsNonSticky(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(1))
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("k"), []byte(`{"v":1}`)); err != nil {
			return err
		}
		if err := batch.Put([]byte("overflow"), []byte(`{"v":2}`)); !errors.Is(err, ErrBatchTooLarge) {
			return errors.New("second mutation did not fail with ErrBatchTooLarge")
		}
		if batch.Len() != 1 {
			return errors.New("rejected mutation changed batch length")
		}
		return batch.Put([]byte("k"), []byte(`{"v":3}`))
	}); err != nil {
		t.Fatalf("Update after handled admission error: %v", err)
	}
	if got, found, err := collection.AppendRaw(nil, []byte("k")); err != nil || !found || string(got) != `{"v":3}` {
		t.Fatalf("final row = %q/%v/%v", got, found, err)
	}
	if _, found, err := collection.AppendRaw(nil, []byte("overflow")); err != nil || found {
		t.Fatalf("rejected row = found %v err %v", found, err)
	}
}

func TestUpdateCollectionsBatchOwnerCannotBeReused(t *testing.T) {
	_, members, log := newCheckpointGroupTestResources(t, "user")
	user := members[0].Collection
	var retained *DatabaseBatch
	var retainedCopy *DatabaseBatch
	var retainedWrite *WriteBatch
	var retainedWriteCopy *WriteBatch
	if err := UpdateCollections(log, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		retained = batch
		copy := *batch
		retainedCopy = &copy
		byName, err := batch.Collection("user")
		if err != nil {
			return err
		}
		byHandle, err := batch.CollectionHandle(user)
		if err != nil {
			return err
		}
		if byName != byHandle {
			return errors.New("DatabaseBatch lookup methods returned different handles")
		}
		retainedWrite = byName
		writeCopy := *byName
		retainedWriteCopy = &writeCopy
		if _, err := retainedCopy.Collection("user"); !errors.Is(err, ErrTxnCollection) {
			return errors.New("shallow DatabaseBatch copy remained active")
		}
		if _, err := retainedCopy.CollectionHandle(user); !errors.Is(err, ErrTxnCollection) {
			return errors.New("shallow DatabaseBatch handle lookup remained active")
		}
		if err := retainedWriteCopy.Put([]byte("copy"), []byte(`{"v":0}`)); !errors.Is(err, ErrBatchClosed) {
			return errors.New("shallow WriteBatch copy remained active")
		}
		return byName.Put([]byte("k"), []byte(`{"v":1}`))
	}); err != nil {
		t.Fatal(err)
	}
	requireClosedDatabaseBatch(t, retained, user)
	requireClosedDatabaseBatch(t, retainedCopy, user)
	requireClosedWriteBatch(t, retainedWrite)
	requireClosedWriteBatch(t, retainedWriteCopy)

	if err := UpdateCollections(log, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		if batch == retained || batch == retainedCopy {
			return errors.New("UpdateCollections reused a public DatabaseBatch pointer")
		}
		requireClosedDatabaseBatch(t, retained, user)
		requireClosedDatabaseBatch(t, retainedCopy, user)
		requireClosedWriteBatch(t, retainedWrite)
		requireClosedWriteBatch(t, retainedWriteCopy)
		write, err := batch.CollectionHandle(user)
		if err != nil {
			return err
		}
		return write.Put([]byte("k"), []byte(`{"v":2}`))
	}); err != nil {
		t.Fatal(err)
	}
	if got, found, err := user.AppendRaw(nil, []byte("k")); err != nil || !found || string(got) != `{"v":2}` {
		t.Fatalf("UpdateCollections row = %q/%v/%v", got, found, err)
	}
	if _, found, err := user.AppendRaw(nil, []byte("copy")); err != nil || found {
		t.Fatalf("shallow-copy mutation became visible: found=%v err=%v", found, err)
	}
}

func TestUpdateCollectionsBatchOwnerReleasedOnCallbackErrorOrPanic(t *testing.T) {
	for _, test := range []struct {
		name string
		fail func(*DatabaseBatch, *Collection) error
	}{
		{
			name: "error",
			fail: func(*DatabaseBatch, *Collection) error {
				return errBatchOwnerCallback
			},
		},
		{
			name: "panic",
			fail: func(*DatabaseBatch, *Collection) error {
				panic(errBatchOwnerCallback)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, members, log := newCheckpointGroupTestResources(t, "user")
			user := members[0].Collection
			var retained *DatabaseBatch
			var retainedWrite *WriteBatch
			invoke := func() (err error, recovered any) {
				defer func() { recovered = recover() }()
				err = UpdateCollections(log, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
					retained = batch
					write, lookupErr := batch.Collection("user")
					if lookupErr != nil {
						return lookupErr
					}
					retainedWrite = write
					if putErr := write.Put([]byte("failed"), []byte(`{"v":1}`)); putErr != nil {
						return putErr
					}
					return test.fail(batch, user)
				})
				return err, nil
			}
			err, recovered := invoke()
			if test.name == "error" {
				if !errors.Is(err, errBatchOwnerCallback) || recovered != nil {
					t.Fatalf("callback result = %v, panic=%v", err, recovered)
				}
			} else if err != nil || recovered != errBatchOwnerCallback {
				t.Fatalf("callback result = %v, panic=%v", err, recovered)
			}
			requireClosedDatabaseBatch(t, retained, user)
			requireClosedWriteBatch(t, retainedWrite)
			if user.Len() != 0 {
				t.Fatalf("failed UpdateCollections published %d rows", user.Len())
			}
			if err := UpdateCollections(log, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
				requireClosedDatabaseBatch(t, retained, user)
				requireClosedWriteBatch(t, retainedWrite)
				write, lookupErr := batch.CollectionHandle(user)
				if lookupErr != nil {
					return lookupErr
				}
				return write.Put([]byte("ok"), []byte(`{"v":2}`))
			}); err != nil {
				t.Fatalf("UpdateCollections after %s: %v", test.name, err)
			}
		})
	}
}

func TestCheckpointGroupBatchOwnerCannotBeReused(t *testing.T) {
	members, group := newBatchOwnerCheckpointGroupTestStore(t, 1)
	user := members[1].Collection
	var retained *DatabaseBatch
	var retainedCopy *DatabaseBatch
	var retainedWrite *WriteBatch
	var retainedWriteCopy *WriteBatch
	if err := group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		retained = batch
		copy := *batch
		retainedCopy = &copy
		byName, err := batch.Collection("user")
		if err != nil {
			return err
		}
		byHandle, err := batch.CollectionHandle(user)
		if err != nil {
			return err
		}
		if byName != byHandle {
			t.Fatal("DatabaseBatch lookup methods returned different handles")
		}
		retainedWrite = byName
		writeCopy := *byName
		retainedWriteCopy = &writeCopy
		if _, err := retainedCopy.Collection("user"); !errors.Is(err, ErrTxnCollection) {
			t.Fatalf("shallow DatabaseBatch Collection = %v, want ErrTxnCollection", err)
		}
		if _, err := retainedCopy.CollectionHandle(user); !errors.Is(err, ErrTxnCollection) {
			t.Fatalf("shallow DatabaseBatch CollectionHandle = %v, want ErrTxnCollection", err)
		}
		if retainedWriteCopy.Len() != 0 {
			t.Fatalf("shallow WriteBatch Len() = %d, want 0", retainedWriteCopy.Len())
		}
		if err := retainedWriteCopy.Put([]byte("copy"), []byte(`{"n":1}`)); !errors.Is(err, ErrBatchClosed) {
			t.Fatalf("shallow WriteBatch Put = %v, want ErrBatchClosed", err)
		}
		if err := byName.Put([]byte("k"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		if err := byName.Put([]byte("overflow"), []byte(`{"n":1}`)); !errors.Is(err, ErrBatchTooLarge) {
			return errors.New("second group mutation did not fail with ErrBatchTooLarge")
		}
		if byName.Len() != 1 {
			return errors.New("rejected group mutation changed batch length")
		}
		// The current owner remains usable after the handled admission error.
		if err := byName.Put([]byte("k"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		system, err := batch.CollectionHandle(members[0].Collection)
		if err != nil {
			return err
		}
		return system.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("Update with rejected shallow handles: %v", err)
	}
	requireClosedDatabaseBatch(t, retained, user)
	requireClosedDatabaseBatch(t, retainedCopy, user)
	requireClosedWriteBatch(t, retainedWrite)
	requireClosedWriteBatch(t, retainedWriteCopy)

	if err := group.Update(2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		if batch == retained || batch == retainedCopy {
			t.Fatal("CheckpointGroup reused a public DatabaseBatch pointer")
		}
		requireClosedDatabaseBatch(t, retained, user)
		requireClosedDatabaseBatch(t, retainedCopy, user)
		requireClosedWriteBatch(t, retainedWrite)
		requireClosedWriteBatch(t, retainedWriteCopy)
		write, err := batch.CollectionHandle(user)
		if err != nil {
			return err
		}
		return write.Put([]byte("k"), []byte(`{"n":2}`))
	}); err != nil {
		t.Fatalf("Update after retained-handle checks: %v", err)
	}
	if got, found, err := members[1].Collection.AppendRaw(nil, []byte("k")); err != nil || !found || string(got) != `{"n":2}` {
		t.Fatalf("group row = %q/%v/%v", got, found, err)
	}
}

func TestCheckpointGroupBatchOwnerReleasedOnCallbackErrorOrPanic(t *testing.T) {
	for _, test := range []struct {
		name string
		fail func(*DatabaseBatch, *Collection) error
	}{
		{
			name: "error",
			fail: func(batch *DatabaseBatch, user *Collection) error {
				return errBatchOwnerCallback
			},
		},
		{
			name: "panic",
			fail: func(batch *DatabaseBatch, user *Collection) error {
				panic(errBatchOwnerCallback)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, members, _, group := newCheckpointGroupTestStore(t, 8)
			user := members[1].Collection
			var retained *DatabaseBatch
			var retainedWrite *WriteBatch
			invoke := func() (err error, recovered any) {
				defer func() { recovered = recover() }()
				err = group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
					retained = batch
					write, lookupErr := batch.Collection("user")
					if lookupErr != nil {
						return lookupErr
					}
					retainedWrite = write
					if putErr := write.Put([]byte("failed"), []byte(`{"n":1}`)); putErr != nil {
						return putErr
					}
					return test.fail(batch, user)
				})
				return err, nil
			}
			err, recovered := invoke()
			if test.name == "error" {
				if !errors.Is(err, errBatchOwnerCallback) || recovered != nil {
					t.Fatalf("callback result = %v, panic=%v", err, recovered)
				}
			} else if err != nil || recovered != errBatchOwnerCallback {
				t.Fatalf("callback result = %v, panic=%v", err, recovered)
			}
			requireClosedDatabaseBatch(t, retained, user)
			requireClosedWriteBatch(t, retainedWrite)
			if members[1].Collection.Len() != 0 {
				t.Fatalf("failed group callback published %d rows", members[1].Collection.Len())
			}
			if err := group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
				requireClosedDatabaseBatch(t, retained, user)
				requireClosedWriteBatch(t, retainedWrite)
				write, lookupErr := batch.CollectionHandle(user)
				if lookupErr != nil {
					return lookupErr
				}
				return write.Put([]byte("ok"), []byte(`{"n":2}`))
			}); err != nil {
				t.Fatalf("Update after %s: %v", test.name, err)
			}
		})
	}
}

func TestWriteBatchOwnerWrapperHasOneWarmedAllocation(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	callback := func(*WriteBatch) error { return nil }
	allocations := testing.AllocsPerRun(1_000, func() {
		if err := collection.Update(callback); err != nil {
			panic(err)
		}
	})
	if allocations != 1 {
		t.Fatalf("empty public Collection.Update allocations = %.2f, want one owner wrapper", allocations)
	}
}

func TestPrivateUpdateReusesBatchOwnerWithoutAllocation(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(8))
	var retained *WriteBatch
	var retainedCopy *WriteBatch
	if err := collection.Update(func(batch *WriteBatch) error {
		retained = batch
		copy := *batch
		retainedCopy = &copy
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	requireClosedWriteBatch(t, retained)
	if err := collection.updatePrimaryBatchPrivate(func(batch *WriteBatch) error {
		if batch == retained || batch == retainedCopy {
			return errors.New("private update borrowed a public wrapper")
		}
		requireClosedWriteBatch(t, retained)
		requireClosedWriteBatch(t, retainedCopy)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	noop := func(*WriteBatch) error { return nil }
	allocations := testing.AllocsPerRun(1_000, func() {
		if err := collection.updatePrimaryBatchPrivate(noop); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warmed private empty Update allocations = %.2f, want 0", allocations)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if batch == retained || batch == retainedCopy {
			return errors.New("public Update reused a stale wrapper")
		}
		requireClosedWriteBatch(t, retained)
		requireClosedWriteBatch(t, retainedCopy)
		return batch.Put([]byte("ok"), []byte(`{"v":2}`))
	}); err != nil {
		t.Fatal(err)
	}
}

var errBatchOwnerCallback = errors.New("batch owner callback failure")
