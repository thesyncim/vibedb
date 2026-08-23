package durable

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func assertOpaqueValue(
	t *testing.T,
	collection *Collection,
	key, want []byte,
) {
	t.Helper()
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("AppendRaw(%q) = (%x,%v,%v), want (%x,true,nil)",
			key, got, found, err, want)
	}
}

func TestOpaqueValuesCreateUpdateAndZeroOptionReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "opaque-values-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testBatchOptions(4)
	options.OpaqueValues = true
	options.InlineValueBytes = 64
	options.MaxDocumentBytes = 2048
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if !collection.HasOpaqueValues() || collection.HasSchema() ||
		collection.HasIndexes() {
		t.Fatalf("opaque capability matrix = opaque:%v schema:%v indexes:%v",
			collection.HasOpaqueValues(), collection.HasSchema(),
			collection.HasIndexes())
	}
	if collection.primaryUnifiedOverlay != nil ||
		collection.primaryConcurrentContexts != nil {
		t.Fatal("opaque collection constructed a JSON overlay/concurrent lane")
	}
	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "value", Paths: []string{"/value"},
	}); !errors.Is(err, ErrPrimaryCutoverUnsupported) {
		t.Fatalf("CreateIndex on opaque collection = %v, want %v",
			err, ErrPrimaryCutoverUnsupported)
	}

	first := []byte{0xff, 0x00, '{', 'n', 'o', 't', '-', 'j', 's', 'o', 'n'}
	overflow := bytes.Repeat([]byte{0x00, 0xfe, 0x7f, 0x80}, 100)
	if created, err := collection.Put([]byte("first"), first); err != nil || !created {
		t.Fatalf("Put first = (%v,%v)", created, err)
	}
	if created, err := collection.Put([]byte("overflow"), overflow); err != nil || !created {
		t.Fatalf("Put overflow = (%v,%v)", created, err)
	}
	assertOpaqueValue(t, collection, []byte("first"), first)
	assertOpaqueValue(t, collection, []byte("overflow"), overflow)

	replacement := []byte{']', 0x00, 0xfd, '\n'}
	batched := []byte{0x80, 0x81, 0x00, 'x'}
	if err := collection.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("first"), replacement); err != nil {
			return err
		}
		return batch.Put([]byte("batched"), batched)
	}); err != nil {
		t.Fatal(err)
	}
	assertOpaqueValue(t, collection, []byte("first"), replacement)
	assertOpaqueValue(t, collection, []byte("batched"), batched)
	assertOpaqueValue(t, collection, []byte("overflow"), overflow)

	_, root := recoverFileCatalogRoot(t, file, uint32(options.PageSize))
	if root.Options&storeio.StateOptionOpaqueValues == 0 {
		t.Fatalf("opaque mode missing from state root options %#x", root.Options)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.HasOpaqueValues() || reopened.primaryUnifiedOverlay != nil ||
		reopened.primaryConcurrentContexts != nil {
		t.Fatalf("zero-option reopen lost opaque capability: opaque=%v overlay=%v concurrent=%v",
			reopened.HasOpaqueValues(), reopened.primaryUnifiedOverlay != nil,
			reopened.primaryConcurrentContexts != nil)
	}
	assertOpaqueValue(t, reopened, []byte("first"), replacement)
	assertOpaqueValue(t, reopened, []byte("batched"), batched)
	assertOpaqueValue(t, reopened, []byte("overflow"), overflow)
	postReopen := []byte{0x00, 0xff, 'r'}
	if created, err := reopened.Put([]byte("reopened"), postReopen); err != nil || !created {
		t.Fatalf("reopened Put = (%v,%v)", created, err)
	}
	assertOpaqueValue(t, reopened, []byte("reopened"), postReopen)
}

func TestOpaqueValuesRejectJSONOnlyOptionsAndBulk(t *testing.T) {
	base := testBatchOptions(2)
	base.OpaqueValues = true
	cases := map[string]func(*Options){
		"exact index": func(options *Options) {
			options.Indexes = []store.IndexDefinition{{Name: "x", Paths: []string{"/x"}}}
		},
		"skip index": func(options *Options) {
			options.SkipIndexes = []string{"/x"}
		},
		"schema": func(options *Options) {
			options.Collection.Schema = testDurableStoreSchema(t)
		},
		"shape tapes": func(options *Options) {
			options.Collection.ShapeTapes = true
		},
		"postings": func(options *Options) {
			options.Collection.Postings = true
		},
		"value dictionary": func(options *Options) {
			options.Collection.ValueDict = true
		},
		"structural index": func(options *Options) {
			options.Collection.IndexOptions = document.IndexOptions{
				MaxDepth: 8, HashKeys: true,
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			options := base
			mutate(&options)
			if _, err := NormalizeOptions(options); err == nil {
				t.Fatal("NormalizeOptions accepted opaque JSON-only options")
			}
		})
	}

	recordFile, err := os.CreateTemp(t.TempDir(), "opaque-record-bulk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer recordFile.Close()
	if _, err := CreateFromRecords([]PrimaryBulkRecord{{
		Key: "key", Value: []byte{0xff},
	}}, recordFile, base); !errors.Is(err, ErrPrimaryCutoverUnsupported) {
		t.Fatalf("CreateFromRecords opaque = %v, want %v",
			err, ErrPrimaryCutoverUnsupported)
	}

	primaryFile, err := os.CreateTemp(t.TempDir(), "opaque-primary-bulk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer primaryFile.Close()
	if _, err := CreateFromPrimary(
		seedPrimaryCollection(t), primaryFile, base,
	); !errors.Is(err, ErrPrimaryCutoverUnsupported) {
		t.Fatalf("CreateFromPrimary opaque = %v, want %v",
			err, ErrPrimaryCutoverUnsupported)
	}
}

func TestOpaqueValuesPersistedModeMismatchFailsClosed(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "ordinary-values-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file, Options{OpaqueValues: true}); err == nil {
		t.Fatal("Open reinterpreted an ordinary JSON collection as opaque")
	}
	if (*Collection)(nil).HasOpaqueValues() {
		t.Fatal("nil collection reports opaque values")
	}
}
