package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestDurableUniqueIndexEnforcesPointWritesAndReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "native-unique-point-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(4)
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for key, raw := range map[string]string{
		"a": `{"u":"x"}`,
		"b": `{"u":"y"}`,
	} {
		if _, err := collection.Put([]byte(key), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	unique := store.IndexDefinition{
		Name: "u_unique", Paths: []string{"/u"},
	}
	if _, err := collection.CreateUniqueIndex(unique); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "u_ordinary", Paths: []string{"/u"},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := collection.Put(
		[]byte("b"), []byte(`{"u":"x"}`),
	); !errors.Is(err, store.ErrUniqueIndexViolation) {
		t.Fatalf("duplicate point Put = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
	assertDurableRaw(t, collection, "b", `{"u":"y"}`)
	if created, err := collection.Put(
		[]byte("a"), []byte(`{"u":"x","same":true}`),
	); err != nil || created {
		t.Fatalf("self replacement = created %v, err %v", created, err)
	}
	if _, err := collection.Put(
		[]byte("container"), []byte(`{"u":[1,2]}`),
	); !errors.Is(err, store.ErrIndexScalar) {
		t.Fatalf("container point Put = %v, want %v",
			err, store.ErrIndexScalar)
	}
	if _, found, err := collection.AppendRaw(nil, []byte("container")); err != nil || found {
		t.Fatalf("rejected container published: found=%v err=%v", found, err)
	}
	for _, key := range []string{"null-a", "null-b"} {
		if _, err := collection.Put(
			[]byte(key), []byte(`{"u":null}`),
		); err != nil {
			t.Fatalf("NULL-exempt Put %s: %v", key, err)
		}
	}
	if deleted, err := collection.Delete([]byte("a")); err != nil || !deleted {
		t.Fatalf("delete holder = %v,%v", deleted, err)
	}
	if _, err := collection.Put(
		[]byte("replacement"), []byte(`{"u":"x"}`),
	); err != nil {
		t.Fatalf("reuse deleted term: %v", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	collection, err = Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put(
		[]byte("after-reopen"), []byte(`{"u":"x"}`),
	); !errors.Is(err, store.ErrUniqueIndexViolation) {
		t.Fatalf("reopened duplicate Put = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	infos := snapshot.AppendIndexes(nil)
	_ = snapshot.Close()
	if len(infos) != 2 || infos[0].Unique == infos[1].Unique {
		t.Fatalf("mixed alias metadata = %+v", infos)
	}
}

func TestDurableUniqueIndexBatchUsesFinalImages(t *testing.T) {
	collection, _ := openBatchCollection(t, testBatchOptions(4))
	for key, raw := range map[string]string{
		"a": `{"u":"x"}`,
		"b": `{"u":"y"}`,
	} {
		if _, err := collection.Put([]byte(key), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collection.CreateUniqueIndex(store.IndexDefinition{
		Name: "u_unique", Paths: []string{"/u"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("a"), []byte(`{"u":"y"}`)); err != nil {
			return err
		}
		return batch.Put([]byte("b"), []byte(`{"u":"x"}`))
	}); err != nil {
		t.Fatalf("atomic unique swap: %v", err)
	}
	assertDurableRaw(t, collection, "a", `{"u":"y"}`)
	assertDurableRaw(t, collection, "b", `{"u":"x"}`)

	for name, mutate := range map[string]func(*WriteBatch) error{
		"duplicate final terms": func(batch *WriteBatch) error {
			if err := batch.Put([]byte("a"), []byte(`{"u":"z"}`)); err != nil {
				return err
			}
			return batch.Put([]byte("b"), []byte(`{"u":"z"}`))
		},
		"canonical numeric duplicate": func(batch *WriteBatch) error {
			if err := batch.Put([]byte("a"), []byte(`{"u":1}`)); err != nil {
				return err
			}
			return batch.Put([]byte("b"), []byte(`{"u":1.0}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := collection.Update(mutate); !errors.Is(
				err, store.ErrUniqueIndexViolation,
			) {
				t.Fatalf("Update = %v, want %v",
					err, store.ErrUniqueIndexViolation)
			}
			assertDurableRaw(t, collection, "a", `{"u":"y"}`)
			assertDurableRaw(t, collection, "b", `{"u":"x"}`)
		})
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Delete([]byte("b")); err != nil {
			return err
		}
		return batch.Put([]byte("c"), []byte(`{"u":"x"}`))
	}); err != nil {
		t.Fatalf("delete and reuse unique term: %v", err)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("c"), []byte(`{"u":"y"}`))
	}); !errors.Is(err, store.ErrUniqueIndexViolation) {
		t.Fatalf("untouched-row conflict = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		return batch.Put([]byte("c"), []byte(`{"u":{}}`))
	}); !errors.Is(err, store.ErrIndexScalar) {
		t.Fatalf("batch container = %v, want %v", err, store.ErrIndexScalar)
	}
	assertDurableRaw(t, collection, "c", `{"u":"x"}`)
}

func TestDurableUniqueIndexBulkValidation(t *testing.T) {
	options := testBatchOptions(2)
	options.Indexes = []store.IndexDefinition{{
		Name: "u_unique", Paths: []string{"/u"}, Unique: true,
	}}
	for name, test := range map[string]struct {
		records []PrimaryBulkBytesRecord
		wantErr error
	}{
		"canonical duplicate": {
			records: []PrimaryBulkBytesRecord{
				{Key: []byte("a"), Value: []byte(`{"u":1}`)},
				{Key: []byte("b"), Value: []byte(`{"u":1.0}`)},
			},
			wantErr: store.ErrUniqueIndexViolation,
		},
		"container": {
			records: []PrimaryBulkBytesRecord{
				{Key: []byte("a"), Value: []byte(`{"u":[]}`)},
			},
			wantErr: store.ErrIndexScalar,
		},
	} {
		t.Run(name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "native-unique-bulk-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := CreateFromByteRecords(test.records, file, options); !errors.Is(err, test.wantErr) {
				t.Fatalf("CreateFromByteRecords = %v, want %v",
					err, test.wantErr)
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 0 {
				t.Fatalf("rejected bulk file size = %d", info.Size())
			}
		})
	}

	file, err := os.CreateTemp(t.TempDir(), "native-unique-bulk-null-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromByteRecords([]PrimaryBulkBytesRecord{
		{Key: []byte("a"), Value: []byte(`{"u":null}`)},
		{Key: []byte("b"), Value: []byte(`{"u":null}`)},
	}, file, options); err != nil {
		t.Fatalf("NULL-exempt bulk: %v", err)
	}
	collection, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put(
		[]byte("c"), []byte(`{"u":null}`),
	); err != nil {
		t.Fatalf("NULL-exempt reopened Put: %v", err)
	}
}

func TestNonUniqueIndexCatalogHashRemainsLegacyCompatible(t *testing.T) {
	options := testBatchOptions(1)
	options.Indexes = []store.IndexDefinition{
		{Name: "a", Paths: []string{"/tenant", "/status"}},
		{Name: "z", Paths: []string{"/score"}},
	}
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	legacy := uint64(14695981039346656037)
	for _, alias := range normalized.pageCatalog.Definition().Indexes {
		legacy = fileIndexHashBytes(legacy, []byte(alias.Name))
		legacy = fileIndexHashBytes(
			legacy, []byte{0xff, byte(len(alias.Paths))},
		)
		for _, path := range alias.Paths {
			legacy = fileIndexHashBytes(legacy, []byte(path))
			legacy = fileIndexHashBytes(legacy, []byte{0})
		}
	}
	if normalized.indexCatalogHash != legacy {
		t.Fatalf("nonunique catalog hash = %#x, legacy %#x",
			normalized.indexCatalogHash, legacy)
	}
	if len(normalized.uniqueIndexIDs) != 0 {
		t.Fatalf("nonunique physical cache = %v, want empty",
			normalized.uniqueIndexIDs)
	}
	options.Indexes = append(options.Indexes, store.IndexDefinition{
		Name: "a_unique", Paths: []string{"/tenant", "/status"},
		Unique: true,
	})
	unique, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if unique.indexCatalogHash == legacy {
		t.Fatal("unique catalog retained nonunique legacy hash")
	}
	if len(unique.uniqueIndexIDs) != 1 ||
		unique.uniqueIndexIDs[0] != unique.indexNameIDs["a"] ||
		unique.uniqueIndexIDs[0] != unique.indexNameIDs["a_unique"] {
		t.Fatalf("alias-OR unique physical cache = %v, aliases = %v",
			unique.uniqueIndexIDs, unique.indexNameIDs)
	}
}

func TestDurableUniqueIndexExplicitReopenCannotDowngrade(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "native-unique-downgrade-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(1)
	options.Indexes = []store.IndexDefinition{{
		Name: "u_unique", Paths: []string{"/u"}, Unique: true,
	}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	downgrade := options
	downgrade.Indexes = []store.IndexDefinition{{
		Name: "u_unique", Paths: []string{"/u"}, Unique: false,
	}}
	if reopened, err := Open(file, downgrade); err == nil {
		_ = reopened.Close()
		t.Fatal("Open accepted explicit Unique:false for persisted Unique:true")
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatalf("zero-option unique reopen: %v", err)
	}
	defer reopened.Close()
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	infos := snapshot.AppendIndexes(nil)
	_ = snapshot.Close()
	if len(infos) != 1 || !infos[0].Unique {
		t.Fatalf("zero-option reopened unique metadata = %+v", infos)
	}
}

func TestRecoveryJournalUniqueSwapWithSmallerReopenBatch(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = 2
	options.Indexes = []store.IndexDefinition{{
		Name: "u_unique", Paths: []string{"/u"}, Unique: true,
	}}
	collection := buildIndexedPrimaryFile(
		t, t.TempDir(), "native-unique-replay-*",
		map[string][]byte{
			"a": []byte(`{"u":"x"}`),
			"b": []byte(`{"u":"y"}`),
		},
		options,
	)
	path := collection.file.Name()
	var captured *journalCrashImage
	previous := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("a"), []byte(`{"u":"y"}`)); err != nil {
			return err
		}
		return batch.Put([]byte("b"), []byte(`{"u":"x"}`))
	}); err != nil {
		recoveryJournalPostSyncHook = previous
		t.Fatal(err)
	}
	recoveryJournalPostSyncHook = previous
	if captured == nil {
		t.Fatal("did not capture unique swap journal")
	}
	crashPath := filepath.Join(t.TempDir(), "native-unique-replay.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	reopenOptions := options
	reopenOptions.MaxBatchDocuments = 1
	reopenOptions.MaxBatchBytes = 0
	file, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recovered, err := Open(file, reopenOptions)
	if err != nil {
		t.Fatalf("recover unique swap: %v", err)
	}
	defer recovered.Close()
	assertDurableRaw(t, recovered, "a", `{"u":"y"}`)
	assertDurableRaw(t, recovered, "b", `{"u":"x"}`)
}

func TestRecoveryJournalUniqueRejectsInvalidFinalImage(t *testing.T) {
	for name, test := range map[string]struct {
		base    map[string][]byte
		entries []storeio.RecoveryBatchEntry
		wantErr error
	}{
		"duplicate final terms": {
			entries: []storeio.RecoveryBatchEntry{
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("b"), Value: []byte(`{"u":1}`)},
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("c"), Value: []byte(`{"u":1.0}`)},
			},
			wantErr: store.ErrUniqueIndexViolation,
		},
		"untouched current row": {
			base: map[string][]byte{
				"a": []byte(`{"u":"held"}`),
			},
			entries: []storeio.RecoveryBatchEntry{
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("b"), Value: []byte(`{"u":"held"}`)},
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("c"), Value: []byte(`{"u":"free"}`)},
			},
			wantErr: store.ErrUniqueIndexViolation,
		},
		"container final value": {
			entries: []storeio.RecoveryBatchEntry{
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("b"), Value: []byte(`{"u":{}}`)},
				{Kind: storeio.RecoveryRecordKindPut, Key: []byte("c"), Value: []byte(`{"u":"free"}`)},
			},
			wantErr: store.ErrIndexScalar,
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := syncPrimaryJournalTestOptions()
			options.MaxBatchDocuments = 2
			options.Indexes = []store.IndexDefinition{{
				Name: "u_unique", Paths: []string{"/u"}, Unique: true,
			}}
			base := test.base
			if len(base) == 0 {
				base = map[string][]byte{
					"seed": []byte(`{"u":"seed"}`),
				}
			}
			collection := buildIndexedPrimaryFile(
				t, t.TempDir(), "native-unique-invalid-replay-*",
				base, options,
			)
			path := collection.file.Name()
			if err := collection.Close(); err != nil {
				t.Fatal(err)
			}
			appendUniqueRecoveryBatch(t, path, test.entries)
			beforeStore, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			beforeJournal, err := os.ReadFile(path + ".rjournal")
			if err != nil {
				t.Fatal(err)
			}

			reopen := options
			reopen.MaxBatchDocuments = 1
			reopen.MaxBatchBytes = 0
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			recovered, openErr := Open(file, reopen)
			if recovered != nil {
				_ = recovered.Close()
			}
			_ = file.Close()
			if !errors.Is(openErr, test.wantErr) {
				t.Fatalf("Open = %v, want %v", openErr, test.wantErr)
			}
			afterStore, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			afterJournal, err := os.ReadFile(path + ".rjournal")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(afterStore, beforeStore) {
				t.Fatal("rejected recovery changed the durable store")
			}
			if !bytes.Equal(afterJournal, beforeJournal) {
				t.Fatal("rejected recovery recycled or rewrote its journal")
			}
		})
	}
}

func TestRecoveryJournalUniqueEmptyBaseFinalImage(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = 2
	options.Indexes = []store.IndexDefinition{{
		Name: "u_unique", Paths: []string{"/u"}, Unique: true,
	}}
	file, err := os.CreateTemp(t.TempDir(), "native-unique-empty-replay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	path := collection.file.Name()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	appendUniqueRecoveryBatch(t, path, []storeio.RecoveryBatchEntry{
		{Kind: storeio.RecoveryRecordKindPut, Key: []byte("a"), Value: []byte(`{"u":"x"}`)},
		{Kind: storeio.RecoveryRecordKindPut, Key: []byte("b"), Value: []byte(`{"u":"y"}`)},
	})
	reopen := options
	reopen.MaxBatchDocuments = 1
	reopen.MaxBatchBytes = 0
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(file, reopen)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	assertDurableRaw(t, recovered, "a", `{"u":"x"}`)
	assertDurableRaw(t, recovered, "b", `{"u":"y"}`)
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err = Open(file, reopen)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer recovered.Close()
	defer file.Close()
	assertDurableRaw(t, recovered, "a", `{"u":"x"}`)
	if _, err := recovered.Put(
		[]byte("duplicate"), []byte(`{"u":"x"}`),
	); !errors.Is(err, store.ErrUniqueIndexViolation) {
		t.Fatalf("second-open unique enforcement = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
}

func TestRecoveryJournalUniqueSameLeafExactPressure(t *testing.T) {
	const (
		rows    = 65
		indexes = 17
	)
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = rows
	options.MaxBatchBytes = 1 << 20
	options.ResidentBytes = 64 << 20
	options.Indexes = make([]store.IndexDefinition, indexes)
	for index := range indexes {
		options.Indexes[index] = store.IndexDefinition{
			Name:   fmt.Sprintf("u%02d_unique", index),
			Paths:  []string{fmt.Sprintf("/u%02d", index)},
			Unique: true,
		}
	}
	documents := make(map[string][]byte, rows)
	for row := range rows {
		documents[fmt.Sprintf("k%03d", row)] =
			recoveryUniquePressureDocument(row, "old", indexes)
	}
	collection := buildIndexedPrimaryFile(
		t, t.TempDir(), "native-unique-pressure-replay-*",
		documents, options,
	)
	var bucket storeio.BucketID
	for row := range rows {
		route, ok := collection.primaryRouter.Load().Route(
			[]byte(fmt.Sprintf("k%03d", row)),
		)
		if !ok {
			t.Fatalf("route row %d", row)
		}
		if row == 0 {
			bucket = route.Bucket
		} else if route.Bucket != bucket {
			t.Fatalf("pressure fixture spans buckets %d and %d", bucket, route.Bucket)
		}
	}
	path := collection.file.Name()
	var captured *journalCrashImage
	previousPostSync := recoveryJournalPostSyncHook
	defer func() { recoveryJournalPostSyncHook = previousPostSync }()
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		for row := range rows {
			if err := batch.Put(
				[]byte(fmt.Sprintf("k%03d", row)),
				recoveryUniquePressureDocument(row, "new", indexes),
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recoveryJournalPostSyncHook = previousPostSync
	if captured == nil {
		t.Fatal("did not capture same-leaf unique pressure batch")
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	crashPath := filepath.Join(t.TempDir(), "native-unique-pressure-recovery.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	reopen := options
	reopen.MaxBatchDocuments = 1
	reopen.MaxBatchBytes = 0
	previousReplay := recoveryJournalReplayBatchEntryHook
	defer func() { recoveryJournalReplayBatchEntryHook = previousReplay }()
	pressureCheckpointed := false
	recoveryJournalReplayBatchEntryHook = func(
		replayed *Collection, _ storeio.RecoveryRecord, _ int,
	) error {
		if replayed.automaticCheckpoints.Load() != 0 {
			pressureCheckpointed = true
		}
		return nil
	}
	file, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(file, reopen)
	recoveryJournalReplayBatchEntryHook = previousReplay
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer recovered.Close()
	defer file.Close()
	if !pressureCheckpointed {
		t.Fatal("same-leaf exact overlay pressure did not checkpoint")
	}
	for _, row := range []int{0, rows / 2, rows - 1} {
		assertDurableRaw(
			t, recovered, fmt.Sprintf("k%03d", row),
			string(recoveryUniquePressureDocument(row, "new", indexes)),
		)
	}
	keys := primaryExactTestKeys(
		t, recovered, "u00_unique",
		primaryExactTestNeedle(t, `"new-00-000"`),
	)
	if len(keys) != 1 || keys[0] != "k000" {
		t.Fatalf("same-leaf recovered posting = %v, want [k000]", keys)
	}
}

func recoveryUniquePressureDocument(row int, state string, indexes int) []byte {
	document := make([]byte, 0, 32*indexes)
	document = append(document, '{')
	for index := range indexes {
		if index != 0 {
			document = append(document, ',')
		}
		document = fmt.Appendf(
			document, `"u%02d":"%s-%02d-%03d"`,
			index, state, index, row,
		)
	}
	return append(document, '}')
}

func appendUniqueRecoveryBatch(
	t *testing.T, path string, entries []storeio.RecoveryBatchEntry,
) {
	t.Helper()
	journalFile, err := os.OpenFile(path+".rjournal", os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(journalFile)
	if err != nil {
		_ = journalFile.Close()
		t.Fatal(err)
	}
	plan, err := journal.PrepareBatch(entries)
	if err == nil {
		_, err = journal.AppendPreparedBatch(
			journal.BaseGeneration()+1, entries, plan,
		)
	}
	if err == nil {
		err = journal.Sync(false)
	}
	if closeErr := journal.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupSequentialRecoveryBudgetsEntries(
	t *testing.T,
) {
	perOperation := uint64(primaryStructuralRetryLimit + 1)
	for name, test := range map[string]struct {
		entries uint64
		want    uint64
		ok      bool
	}{
		"empty": {
			entries: 0, want: 0, ok: true,
		},
		"entries": {
			entries: 3, want: 3 * perOperation, ok: true,
		},
		"largest fitting": {
			entries: ^uint64(0) / perOperation,
			want:    (^uint64(0) / perOperation) * perOperation,
			ok:      true,
		},
		"multiplication overflow": {
			entries: ^uint64(0)/perOperation + 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := checkpointGroupSequentialRecoveryGenerationBudget(
				test.entries,
			)
			if got != test.want || ok != test.ok {
				t.Fatalf("budget = %d,%t, want %d,%t",
					got, ok, test.want, test.ok)
			}
		})
	}
}

func assertDurableRaw(
	t *testing.T, collection *Collection, key, want string,
) {
	t.Helper()
	got, found, err := collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || !bytes.Equal(got, []byte(want)) {
		t.Fatalf("AppendRaw(%q) = %q,%v,%v, want %s,true,nil",
			key, got, found, err, want)
	}
}
