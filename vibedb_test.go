package vibedb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb"
	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/document"
)

func TestMemoryProfileCRUDAndLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-exist.vdb")
	db, err := vibedb.Open(path, vibedb.WithDurability(vibedb.Memory))
	if err != nil {
		t.Fatal(err)
	}
	if db.Durability() != vibedb.Memory {
		t.Fatalf("Durability = %v, want Memory", db.Durability())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Memory Open touched path: %v", err)
	}

	users := db.Collection("users")
	if users != db.Collection("users") {
		t.Fatal("Collection did not return a stable handle")
	}
	if users.Name() != "users" {
		t.Fatalf("Name = %q", users.Name())
	}
	if value, ok, err := users.Get("missing"); err != nil || ok || value != nil {
		t.Fatalf("empty Get = %q,%v,%v", value, ok, err)
	}
	created, err := users.Put("user:1", []byte(`{"name": "Ada"}`))
	if err != nil || !created {
		t.Fatalf("Put = created %v, err %v", created, err)
	}
	value, ok, err := users.Get("user:1")
	if err != nil || !ok || string(value) != `{"name":"Ada"}` {
		t.Fatalf("Get = %q,%v,%v", value, ok, err)
	}
	value[0] = '['
	again, ok, err := users.Get("user:1")
	if err != nil || !ok || string(again) != `{"name":"Ada"}` {
		t.Fatalf("Get returned borrowed bytes: %q,%v,%v", again, ok, err)
	}

	if err := users.CreateIndex("by_name", "/name"); err != nil {
		t.Fatal(err)
	}
	metrics, err := users.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Durability != vibedb.Memory || metrics.Documents != 1 ||
		metrics.PublishedGeneration == 0 || metrics.DurableGeneration != 0 {
		t.Fatalf("Metrics = %+v", metrics)
	}
	var keys []string
	if err := users.Range(func(key string, _ []byte) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"user:1"}) {
		t.Fatalf("Range keys = %v", keys)
	}
	deleted, err := users.Delete("user:1")
	if err != nil || !deleted {
		t.Fatalf("Delete = deleted %v, err %v", deleted, err)
	}
	if deleted, err := users.Delete("user:1"); err != nil || deleted {
		t.Fatalf("missing Delete = deleted %v, err %v", deleted, err)
	}
	if err := users.Close(); !errors.Is(err, vibedb.ErrManagedCollection) {
		t.Fatalf("managed Close error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := users.Get("user:1"); !errors.Is(err, vibedb.ErrClosed) {
		t.Fatalf("Get after Close error = %v", err)
	}
	if err := users.Range(nil); !errors.Is(err, vibedb.ErrClosed) {
		t.Fatalf("Range after Close error = %v", err)
	}
}

func TestOwnedDatabasePersistsDefaultAndBufferedProfiles(t *testing.T) {
	for _, test := range []struct {
		name    string
		profile vibedb.Durability
	}{
		{name: "durable", profile: vibedb.Durable},
		{name: "buffered", profile: vibedb.Buffered},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "application.vdb")
			db, err := vibedb.Open(path, vibedb.WithDurability(test.profile))
			if err != nil {
				t.Fatal(err)
			}
			users := db.Collection("users")
			if _, err := users.Put("user:1", []byte(`{"name":"Ada"}`)); err != nil {
				t.Fatal(err)
			}
			before, err := users.Metrics()
			if err != nil {
				t.Fatal(err)
			}
			if before.Documents != 1 || before.PublishedGeneration == 0 {
				t.Fatalf("Metrics before Flush = %+v", before)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			after, err := users.Metrics()
			if err != nil {
				t.Fatal(err)
			}
			if after.DurableGeneration != after.PublishedGeneration {
				t.Fatalf("Metrics after Flush = %+v", after)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := vibedb.Open(path, vibedb.WithDurability(test.profile))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			value, ok, err := reopened.Collection("users").Get("user:1")
			if err != nil || !ok || string(value) != `{"name":"Ada"}` {
				t.Fatalf("reopened Get = %q,%v,%v", value, ok, err)
			}
		})
	}
}

func TestProfilesReturnOneCanonicalJSONForm(t *testing.T) {
	input := []byte(` { "z" : 1, "a" : { "y": 2, "x" : 3 } } `)
	const want = `{"a":{"x":3,"y":2},"z":1}`
	for _, profile := range []vibedb.Durability{
		vibedb.Memory, vibedb.Buffered, vibedb.Durable,
	} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "canonical.vdb"),
				vibedb.WithDurability(profile),
			)
			if err != nil {
				t.Fatal(err)
			}
			values := db.Collection("values")
			if _, err := values.Put("k", input); err != nil {
				t.Fatal(err)
			}
			got, ok, err := values.Get("k")
			if err != nil || !ok || string(got) != want {
				t.Fatalf("Get = %q,%v,%v; want %q", got, ok, err, want)
			}
			if _, err := values.Put("", []byte(`{}`)); !errors.Is(err, vibedb.ErrKeyTooLarge) {
				t.Fatalf("empty-key Put error = %v", err)
			}
			if _, err := values.Put("empty", nil); !errors.Is(err, vibedb.ErrDocumentTooLarge) {
				t.Fatalf("empty-document Put error = %v", err)
			}
			if _, _, err := values.Get(""); !errors.Is(err, vibedb.ErrKeyTooLarge) {
				t.Fatalf("empty-key Get error = %v", err)
			}
			if _, err := values.Delete(""); !errors.Is(err, vibedb.ErrKeyTooLarge) {
				t.Fatalf("empty-key Delete error = %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenFileBorrowsDescriptor(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "collection.vdb"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	collection, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("OpenFile collection closed caller descriptor: %v", err)
	}

	reopened, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := reopened.Get("k")
	if err != nil || !ok || string(value) != `{"v":1}` {
		t.Fatalf("reopened Get = %q,%v,%v", value, ok, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	invalid, err := os.CreateTemp(t.TempDir(), "invalid-*.vdb")
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Close()
	if _, err := invalid.WriteString("not a vibedb collection"); err != nil {
		t.Fatal(err)
	}
	if _, err := vibedb.OpenFile(invalid, vibedb.AdvancedOptions{}); err == nil {
		t.Fatal("OpenFile accepted an invalid collection")
	}
	if _, err := invalid.Stat(); err != nil {
		t.Fatalf("failed OpenFile closed caller descriptor: %v", err)
	}
}

func TestOpenFileRejectsUnstableNamesBeforeMutation(t *testing.T) {
	assertEmpty := func(t *testing.T, file *os.File) {
		t.Helper()
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Size(); got != 0 {
			t.Fatalf("rejected OpenFile changed primary size to %d", got)
		}
	}

	t.Run("anonymous", func(t *testing.T) {
		primary, err := os.CreateTemp(t.TempDir(), "anonymous-*")
		if err != nil {
			t.Fatal(err)
		}
		anonymous := os.NewFile(primary.Fd(), "")
		if _, err := vibedb.OpenFile(anonymous, vibedb.AdvancedOptions{}); !errors.Is(err, vibedb.ErrInvalidOptions) {
			t.Fatalf("anonymous OpenFile error = %v", err)
		}
		assertEmpty(t, primary)
		_ = anonymous.Close()
		_ = primary.Close()
	})

	t.Run("unlinked", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows does not unlink an open file")
		}
		primary, err := os.CreateTemp(t.TempDir(), "unlinked-*")
		if err != nil {
			t.Fatal(err)
		}
		defer primary.Close()
		path := primary.Name()
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if _, err := vibedb.OpenFile(primary, vibedb.AdvancedOptions{}); !errors.Is(err, vibedb.ErrInvalidOptions) {
			t.Fatalf("unlinked OpenFile error = %v", err)
		}
		assertEmpty(t, primary)
		if _, err := os.Stat(path + ".rjournal"); !os.IsNotExist(err) {
			t.Fatalf("rejected OpenFile created journal: %v", err)
		}
	})

	t.Run("stale-name", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "primary.vjc")
		primary, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer primary.Close()
		moved := filepath.Join(directory, "moved.vjc")
		if err := os.Rename(path, moved); err != nil {
			t.Fatal(err)
		}
		replacement, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer replacement.Close()
		if _, err := vibedb.OpenFile(primary, vibedb.AdvancedOptions{}); !errors.Is(err, vibedb.ErrInvalidOptions) {
			t.Fatalf("stale-name OpenFile error = %v", err)
		}
		assertEmpty(t, primary)
		assertEmpty(t, replacement)
		for _, journal := range []string{path + ".rjournal", moved + ".rjournal"} {
			if _, err := os.Stat(journal); !os.IsNotExist(err) {
				t.Fatalf("rejected OpenFile created %s: %v", journal, err)
			}
		}
	})
}

func TestOpenFileRejectsPrimaryWithoutPortableJournalSuffixSpace(t *testing.T) {
	baseBytes := collectionname.MaxComponentBytes - len(collectionname.JournalSuffix) + 1
	path := filepath.Join(t.TempDir(), strings.Repeat("p", baseBytes))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{}); !errors.Is(err, vibedb.ErrInvalidOptions) {
		t.Fatalf("OpenFile overlong paired basename error = %v, want %v", err, vibedb.ErrInvalidOptions)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("OpenFile mutated primary to %d bytes before rejecting sidecar name", info.Size())
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("OpenFile created files before rejecting sidecar name: %v", entries)
	}
}

func TestOpenFileRejectsSymlinkPrimaryBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.vjc")
	link := filepath.Join(dir, "link.vjc")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	file, err := os.OpenFile(link, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{}); !errors.Is(err, vibedb.ErrInvalidOptions) {
		t.Fatalf("OpenFile symlink error = %v, want %v", err, vibedb.ErrInvalidOptions)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("OpenFile mutated symlink referent to %d bytes before rejection", info.Size())
	}
	for _, journal := range []string{link + collectionname.JournalSuffix, target + collectionname.JournalSuffix} {
		if _, err := os.Lstat(journal); !os.IsNotExist(err) {
			t.Fatalf("OpenFile created journal %q through a symlink path: %v", journal, err)
		}
	}
}

func TestZeroOptionReopenUsesPersistedAdmissionBounds(t *testing.T) {
	longKey := strings.Repeat("k", 128)
	tooLongKey := strings.Repeat("k", 129)
	// The ordinary facade creation default is 4 MiB. Persist a deliberately
	// larger contract, then exercise both limits with an options-free reopen.
	largeDocument := []byte(`"` + strings.Repeat("v", 4<<20) + `"`)
	advanced := vibedb.AdvancedOptions{Engine: durable.Options{
		MaxKeyBytes:       len(longKey),
		MaxDocumentBytes:  len(largeDocument),
		MaxBatchDocuments: 1,
	}}

	t.Run("owned-database", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "custom-bounds.vdb")
		db, err := vibedb.Open(path, vibedb.WithAdvancedOptions(advanced))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Collection("docs").Put("seed", []byte(`null`)); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		reopened, err := vibedb.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		collection := reopened.Collection("docs")
		if _, err := collection.Put(longKey, largeDocument); err != nil {
			t.Fatalf("zero-option reopened Put within persisted bounds: %v", err)
		}
		if _, err := collection.Put(tooLongKey, []byte(`null`)); !errors.Is(err, vibedb.ErrKeyTooLarge) {
			t.Fatalf("zero-option reopened Put beyond persisted key bound: %v", err)
		}
		got, found, err := collection.Get(longKey)
		if err != nil || !found || !slices.Equal(got, largeDocument) {
			t.Fatalf("zero-option reopened Get = (%d bytes,%v,%v)", len(got), found, err)
		}
	})

	t.Run("open-file", func(t *testing.T) {
		file, err := os.OpenFile(
			filepath.Join(t.TempDir(), "custom-bounds.vjc"),
			os.O_CREATE|os.O_RDWR, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		collection, err := vibedb.OpenFile(file, advanced)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collection.Put("seed", []byte(`null`)); err != nil {
			t.Fatal(err)
		}
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}

		reopened, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		if _, err := reopened.Put(longKey, largeDocument); err != nil {
			t.Fatalf("zero-option OpenFile Put within persisted bounds: %v", err)
		}
		if _, err := reopened.Put(tooLongKey, []byte(`null`)); !errors.Is(err, vibedb.ErrKeyTooLarge) {
			t.Fatalf("zero-option OpenFile Put beyond persisted key bound: %v", err)
		}
	})
}

func TestConcurrentLazyCollectionResolution(t *testing.T) {
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "concurrent.vdb"),
		vibedb.WithDurability(vibedb.Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	users := db.Collection("users")
	const writers = 16
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wait sync.WaitGroup
	for i := range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := users.Put("user:"+strconv.Itoa(i), []byte(`{"active":true}`))
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	metrics, err := users.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Documents != writers {
		t.Fatalf("Documents = %d, want %d", metrics.Documents, writers)
	}
}

func TestMetricsRemainCoherentDuringConcurrentPublication(t *testing.T) {
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "metrics.vdb"),
		vibedb.WithDurability(vibedb.Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("events")
	if _, err := collection.Put("seed", []byte(`null`)); err != nil {
		t.Fatal(err)
	}

	const mutations = 1_000
	done := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		defer close(done)
		for i := range mutations {
			if _, err := collection.Put(
				fmt.Sprintf("event:%04d", i), []byte(`{"ok":true}`),
			); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for {
			select {
			case <-done:
				return
			default:
				if err := collection.Flush(); err != nil {
					errs <- err
					return
				}
			}
		}
	}()

	for {
		metrics, err := collection.Metrics()
		if err != nil {
			t.Fatal(err)
		}
		if metrics.PublishedGeneration < metrics.Documents+1 {
			t.Fatalf("incoherent metrics = %+v", metrics)
		}
		if metrics.DurableGeneration > metrics.PublishedGeneration {
			t.Fatalf("durable generation leads sampled publication: %+v", metrics)
		}
		select {
		case <-done:
			workers.Wait()
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
			return
		default:
			runtime.Gosched()
		}
	}
}

func TestBufferedFacadeTracksPackedZeroAllocationPublication(t *testing.T) {
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "packed-metrics.vdb"),
		vibedb.WithDurability(vibedb.Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("events")
	document := []byte(`{"ok":true}`)
	if created, err := collection.Put("seed", document); err != nil || !created {
		t.Fatalf("seed Put = %v,%v", created, err)
	}
	// Keep the leaf non-empty while seed churns. Deleting the collection's final
	// row is deliberately structural and outside the packed point-mutation lane.
	if created, err := collection.Put("anchor", document); err != nil || !created {
		t.Fatalf("anchor Put = %v,%v", created, err)
	}
	before, err := collection.Metrics()
	if err != nil {
		t.Fatal(err)
	}

	putAllocs := testing.AllocsPerRun(50, func() {
		if created, putErr := collection.Put("seed", document); putErr != nil || created {
			panic("replacement Put failed")
		}
	})
	if putAllocs != 0 {
		t.Fatalf("warmed facade Put allocated %.2f times, want 0", putAllocs)
	}
	deleteRestoreAllocs := testing.AllocsPerRun(50, func() {
		if deleted, deleteErr := collection.Delete("seed"); deleteErr != nil || !deleted {
			panic("Delete failed")
		}
		if created, putErr := collection.Put("seed", document); putErr != nil || !created {
			panic("restore Put failed")
		}
	})
	if deleteRestoreAllocs != 0 {
		t.Fatalf("warmed facade delete+restore allocated %.2f times, want 0", deleteRestoreAllocs)
	}

	after, err := collection.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if after.Documents != 2 || after.PublishedGeneration <= before.PublishedGeneration {
		t.Fatalf("packed publication metrics did not advance coherently: before=%+v after=%+v", before, after)
	}
	if after.DurableGeneration > after.PublishedGeneration {
		t.Fatalf("durable generation leads packed publication: %+v", after)
	}
}

func TestMemoryMetricsRemainCoherentDuringConcurrentPublication(t *testing.T) {
	db, err := vibedb.Open("", vibedb.WithDurability(vibedb.Memory))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection := db.Collection("events")
	const mutations = 10_000
	done := make(chan struct{})
	errResult := make(chan error, 1)
	go func() {
		defer close(done)
		for i := range mutations {
			if _, err := collection.Put(
				fmt.Sprintf("event:%05d", i), []byte(`null`),
			); err != nil {
				errResult <- err
				return
			}
		}
	}()
	for {
		metrics, err := collection.Metrics()
		if err != nil {
			t.Fatal(err)
		}
		if metrics.PublishedGeneration != metrics.Documents {
			t.Fatalf("incoherent memory metrics = %+v", metrics)
		}
		select {
		case <-done:
			select {
			case err := <-errResult:
				t.Fatal(err)
			default:
			}
			return
		default:
			runtime.Gosched()
		}
	}
}

func TestRangePreservesCallbackErrorsAcrossProfiles(t *testing.T) {
	for _, profile := range []vibedb.Durability{
		vibedb.Memory, vibedb.Buffered, vibedb.Durable,
	} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "range-errors.vdb"),
				vibedb.WithDurability(profile),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			collection := db.Collection("docs")
			if _, err := collection.Put("k", []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			callbackErr := durable.ErrKeyTooLarge
			got := collection.Range(func(string, []byte) error {
				return callbackErr
			})
			if got != callbackErr {
				t.Fatalf("Range callback error = %v, want exact %v", got, callbackErr)
			}
		})
	}
}

func TestIndexedQuerySessionsAreEquivalentAcrossProfiles(t *testing.T) {
	compiled := query.Select(query.Path("name")).
		Where(query.Cmp("status", query.Eq, "active")).
		OrderBy("name", query.Asc)
	readNames := func(t *testing.T, result *query.Result) []string {
		t.Helper()
		column, ok := result.Column("name")
		if !ok {
			t.Fatal("query result has no name column")
		}
		values := make([]string, len(column.Cells))
		for i := range column.Cells {
			values[i] = string(column.Cells[i].JSON())
		}
		return values
	}

	for _, profile := range []vibedb.Durability{
		vibedb.Memory, vibedb.Buffered, vibedb.Durable,
	} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "query.vdb"),
				vibedb.WithDurability(profile),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			collection := db.Collection("users")
			for key, document := range map[string]string{
				"1": `{"name":"Ada","status":"active"}`,
				"2": `{"name":"Grace","status":"inactive"}`,
				"3": `{"name":"Linus","status":"active"}`,
			} {
				if _, err := collection.Put(key, []byte(document)); err != nil {
					t.Fatal(err)
				}
			}
			if err := collection.CreateIndex("by_status", "/status"); err != nil {
				t.Fatal(err)
			}

			oneOff, err := collection.Run(compiled)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := readNames(t, &oneOff), []string{`"Ada"`, `"Linus"`}; !slices.Equal(got, want) {
				t.Fatalf("one-off indexed query = %v, want %v", got, want)
			}
			oneOff.Release()

			session := collection.NewSession()
			defer session.Release()
			first, err := session.Run(compiled)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := readNames(t, first), []string{`"Ada"`, `"Linus"`}; !slices.Equal(got, want) {
				t.Fatalf("first session query = %v, want %v", got, want)
			}
			if _, err := collection.Put(
				"2", []byte(`{"name":"Grace","status":"active"}`),
			); err != nil {
				t.Fatal(err)
			}
			second, err := session.Run(compiled)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := readNames(t, second),
				[]string{`"Ada"`, `"Grace"`, `"Linus"`}; !slices.Equal(got, want) {
				t.Fatalf("refreshed session query = %v, want %v", got, want)
			}
			allocations := testing.AllocsPerRun(100, func() {
				result, runErr := session.Run(compiled)
				if runErr != nil || result.RowCount != 3 {
					panic("warmed indexed session query failed")
				}
			})
			if allocations != 0 {
				t.Fatalf("warmed Session.Run allocated %.2f times, want 0", allocations)
			}
		})
	}
}

func TestOperationsRaceDatabaseCloseSafely(t *testing.T) {
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "close-race.vdb"),
		vibedb.WithDurability(vibedb.Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	users := db.Collection("users")
	if _, err := users.Put("hot", []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			for {
				if _, _, err := users.Get("hot"); err != nil {
					if !errors.Is(err, vibedb.ErrClosed) {
						errs <- err
					}
					return
				}
				if _, err := users.Put("hot", []byte(`{"n":1}`)); err != nil {
					if !errors.Is(err, vibedb.ErrClosed) {
						errs <- err
					}
					return
				}
			}
		}()
	}
	for range workers {
		<-ready
	}
	close(start)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("operation-vs-Close error = %v", err)
	}
	if _, err := users.Put("hot", []byte(`{"n":2}`)); !errors.Is(err, vibedb.ErrClosed) {
		t.Fatalf("Put after Close error = %v", err)
	}
}

func TestResolvedAppendDoesNotAllocate(t *testing.T) {
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "alloc.vdb"),
		vibedb.WithDurability(vibedb.Buffered),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	users := db.Collection("users")
	if _, err := users.Put("user:1", []byte(`{"active":true}`)); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 64)
	if _, ok, err := users.Append(dst[:0], "user:1"); err != nil || !ok {
		t.Fatalf("warm Append = ok %v, err %v", ok, err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		out, ok, err := users.Append(dst[:0], "user:1")
		if err != nil || !ok || len(out) == 0 {
			panic("Append failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("resolved Append allocations = %v, want 0", allocations)
	}
}

func TestFacadeRejectsIncompatibleOptionsBeforeFilesystemAccess(t *testing.T) {
	base := t.TempDir()
	tests := []struct {
		name    string
		path    string
		options []vibedb.Option
	}{
		{
			name: "unknown profile", path: filepath.Join(base, "unknown"),
			options: []vibedb.Option{vibedb.WithDurability(vibedb.Durability(99))},
		},
		{
			name: "memory disk geometry", path: filepath.Join(base, "memory"),
			options: []vibedb.Option{vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
				Durability: vibedb.Memory,
				Engine:     durable.Options{ResidentBytes: 1 << 20},
			})},
		},
		{
			name: "conflicting engine publication", path: filepath.Join(base, "conflict"),
			options: []vibedb.Option{vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
				Durability: vibedb.Durable,
				Engine:     durable.Options{Durability: durable.DurabilityAsyncVisible},
			})},
		},
		{
			name: "invalid engine geometry", path: filepath.Join(base, "geometry"),
			options: []vibedb.Option{vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
				Engine: durable.Options{PageSize: 123},
			})},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := vibedb.Open(test.path, test.options...); !errors.Is(err, vibedb.ErrInvalidOptions) {
				t.Fatalf("Open error = %v", err)
			}
			if _, err := os.Stat(test.path); !os.IsNotExist(err) {
				t.Fatalf("invalid Open touched path: %v", err)
			}
		})
	}

	file, err := os.CreateTemp(base, "borrowed-*.vdb")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{Durability: vibedb.Memory}); !errors.Is(err, vibedb.ErrInvalidOptions) {
		t.Fatalf("Memory OpenFile error = %v", err)
	}
	if _, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{FileMode: 0o600}); !errors.Is(err, vibedb.ErrInvalidOptions) {
		t.Fatalf("mode-owning OpenFile error = %v", err)
	}
}

func TestConfiguredFileModeAppliesToLazyCollectionCreation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	path := filepath.Join(t.TempDir(), "permissions.vdb")
	db, err := vibedb.Open(path, vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
		Durability: vibedb.Buffered,
		FileMode:   0o700,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Collection("users").Put("k", []byte(`{"name":"Ada"}`)); err != nil {
		t.Fatal(err)
	}
	filename, ok := collectionname.Encode("users")
	if !ok {
		t.Fatal("test collection name did not encode")
	}
	info, err := os.Stat(filepath.Join(path, filename))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("collection mode = %#o, want %#o", got, os.FileMode(0o700))
	}
	journalInfo, err := os.Stat(filepath.Join(path, filename+collectionname.JournalSuffix))
	if err != nil {
		t.Fatal(err)
	}
	if got := journalInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("recovery journal mode = %#o, want %#o", got, os.FileMode(0o700))
	}
}

func TestInvalidLazyMutationsDoNotCreateCollectionFiles(t *testing.T) {
	t.Run("bounds and JSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "invalid-put.vdb")
		db, err := vibedb.Open(path, vibedb.WithDurability(vibedb.Buffered))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		users := db.Collection("users")
		for _, operation := range []func() error{
			func() error { _, err := users.Put("", []byte(`{}`)); return err },
			func() error { _, err := users.Put("k", nil); return err },
			func() error { _, err := users.Put("k", []byte(`{"broken":`)); return err },
		} {
			if err := operation(); err == nil {
				t.Fatal("invalid Put succeeded")
			}
		}
		assertDirectoryEmpty(t, path)
	})

	t.Run("schema", func(t *testing.T) {
		schema, err := store.CompileSchema(store.SchemaDefinition{
			Root: store.SchemaObject,
			Fields: []store.SchemaField{{
				Path: "/name", Types: store.SchemaString, Required: true,
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "invalid-schema.vdb")
		db, err := vibedb.Open(path, vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
			Durability: vibedb.Buffered,
			Engine:     durable.Options{Collection: store.Options{Schema: schema}},
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Collection("users").Put("k", []byte(`{"age":42}`)); !errors.Is(err, store.ErrSchemaViolation) {
			t.Fatalf("schema Put error = %v", err)
		}
		assertDirectoryEmpty(t, path)
	})

	t.Run("depth", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "invalid-depth.vdb")
		db, err := vibedb.Open(path, vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
			Durability: vibedb.Buffered,
			Engine: durable.Options{Collection: store.Options{
				IndexOptions: document.IndexOptions{MaxDepth: 1},
			}},
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Collection("users").Put("k", []byte(`[[]]`)); err == nil {
			t.Fatal("Put accepted a document beyond the configured depth")
		}
		assertDirectoryEmpty(t, path)
		users := db.Collection("users")
		if _, err := users.Put("valid", []byte(`[]`)); err != nil {
			t.Fatalf("valid first Put: %v", err)
		}
		if _, err := users.Put("invalid", []byte(`[[]]`)); err == nil {
			t.Fatal("resolved backend accepted a document beyond the configured depth")
		}
	})

	t.Run("index definition", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "invalid-index.vdb")
		db, err := vibedb.Open(path, vibedb.WithDurability(vibedb.Buffered))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for i, definition := range []struct {
			name  string
			paths []string
		}{
			{name: "", paths: []string{"/name"}},
			{name: "missing_paths"},
			{name: "bad_path", paths: []string{"not-a-pointer"}},
			{name: "duplicate", paths: []string{"/name", "/name"}},
		} {
			if err := db.Collection("users"+strconv.Itoa(i)).CreateIndex(
				definition.name, definition.paths...,
			); !errors.Is(err, store.ErrIndexDefinition) {
				t.Fatalf("CreateIndex(%d) error = %v", i, err)
			}
		}
		assertDirectoryEmpty(t, path)
	})
}

func assertDirectoryEmpty(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid operation created files: %v", entries)
	}
}

func TestOpenFreezesAdvancedDefinitions(t *testing.T) {
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{{
			Path: "/name", Types: store.SchemaString, Required: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	definitions := []store.IndexDefinition{{
		Name: "by_name", Paths: []string{"/name"},
	}}
	db, err := vibedb.Open(
		filepath.Join(t.TempDir(), "frozen.vdb"),
		vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
			Durability: vibedb.Buffered,
			Engine: durable.Options{
				Collection: store.Options{Schema: schema},
				Indexes:    definitions,
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	definitions[0].Name = "mutated"
	definitions[0].Paths[0] = "/mutated"
	schema.Hash = 0
	users := db.Collection("users")
	if _, err := users.Put("k", []byte(`{"name":"Ada"}`)); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex("by_name", "/name"); !errors.Is(err, store.ErrIndexExists) {
		t.Fatalf("CreateIndex after caller mutation error = %v", err)
	}
}

func TestOpenFreezesSchemaAcrossProfiles(t *testing.T) {
	for _, profile := range []vibedb.Durability{vibedb.Memory, vibedb.Buffered} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			schema, err := store.CompileSchema(store.SchemaDefinition{
				Root: store.SchemaObject,
				Fields: []store.SchemaField{{
					Path: "/name", Types: store.SchemaString, Required: true,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "frozen-schema.vdb"),
				vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{
					Durability: profile,
					Engine:     durable.Options{Collection: store.Options{Schema: schema}},
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			schema.Hash = 0
			users := db.Collection("users")
			if _, err := users.Put("valid", []byte(`{"name":"Ada"}`)); err != nil {
				t.Fatalf("valid Put after caller mutation: %v", err)
			}
			if _, err := users.Put("invalid", []byte(`{"age":42}`)); !errors.Is(err, store.ErrSchemaViolation) {
				t.Fatalf("schema violation after caller mutation = %v", err)
			}
		})
	}
}

func TestInvalidCollectionNameIsDeferredToOperation(t *testing.T) {
	db, err := vibedb.Open("", vibedb.WithDurability(vibedb.Memory))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	invalid := db.Collection(strings.Repeat("x", vibedb.MaxCollectionNameBytes+1))
	if _, err := invalid.Put("k", []byte(`{}`)); !errors.Is(err, vibedb.ErrInvalidCollectionName) {
		t.Fatalf("Put error = %v", err)
	}
}

func TestPortableLogicalCollectionNamesAcrossProfiles(t *testing.T) {
	const name = "../CON. /caf\u00e9\\events"
	for _, profile := range []vibedb.Durability{
		vibedb.Memory, vibedb.Buffered, vibedb.Durable,
	} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			db, err := vibedb.Open(
				filepath.Join(t.TempDir(), "portable-names.vdb"),
				vibedb.WithDurability(profile),
			)
			if err != nil {
				t.Fatal(err)
			}
			collection := db.Collection(name)
			if _, err := collection.Put("k", []byte(`{"ok":true}`)); err != nil {
				t.Fatalf("portable logical name Put: %v", err)
			}
			if got, found, err := collection.Get("k"); err != nil || !found || string(got) != `{"ok":true}` {
				t.Fatalf("portable logical name Get = (%q,%v,%v)", got, found, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func Example() {
	db, _ := vibedb.Open("", vibedb.WithDurability(vibedb.Memory))
	defer db.Close()

	users := db.Collection("users")
	_, _ = users.Put("user:1", []byte(`{"name":"Ada"}`))
	document, found, _ := users.Get("user:1")
	fmt.Println(found, string(document))

	// Output:
	// true {"name":"Ada"}
}
