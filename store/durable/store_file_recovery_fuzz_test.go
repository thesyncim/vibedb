package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// Recovery is the one entry point that must accept bytes it did not write.
//
// Every other durable code path starts from a file this package produced, so a
// test can always say what the right answer is. Open cannot: it is handed a file
// by the operating system, and a torn write, a truncated copy, a half-restored
// backup, or a genuinely hostile file all arrive the same way. The contract for
// all of them is the same and is not "recover the data" — it is that the process
// survives, in bounded time, and that a store which does open never answers with
// something it could not have stored.
//
// So the assertion is internal consistency rather than a value oracle. There is
// no oracle for arbitrary bytes, but there is one for a store's own answers: the
// chunk-tree scan and the key directory are two independent traversals of the
// same collection, so they must agree, and whatever they return must be a
// document the store could have accepted in the first place, which means valid
// JSON. Either of those failing means recovery admitted a page it should have
// rejected.

func fuzzRecoveryOptions() Options {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 4
	options.InlineValueBytes = 256
	options.Indexes = []store.IndexDefinition{{Name: "status", Paths: []string{"/status"}}}
	options.ResidentBytes = 8 << 20
	options.BufferCount = 1024
	options.MaxRetiredExtents = 1024
	return options
}

// fuzzRecoverySeedImage builds one real store image so the mutator starts from
// bytes that already parse. Seeding with valid files is what makes this target
// reach the decoders at all: a fuzzer starting from noise spends its whole
// budget being rejected by the superblock magic.
func fuzzRecoverySeedImage(f *testing.F, build func(*Collection) error) []byte {
	f.Helper()
	path := filepath.Join(f.TempDir(), "seed")
	file, err := os.Create(path)
	if err != nil {
		f.Fatal(err)
	}
	collection, err := Create(file, fuzzRecoveryOptions())
	if err != nil {
		_ = file.Close()
		f.Fatal(err)
	}
	if err := build(collection); err != nil {
		_ = collection.Close()
		_ = file.Close()
		f.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		_ = file.Close()
		f.Fatal(err)
	}
	if err := file.Close(); err != nil {
		f.Fatal(err)
	}
	image, err := os.ReadFile(path)
	if err != nil {
		f.Fatal(err)
	}
	return image
}

// Given arbitrary bytes, when they are opened as a collection, then the open
// either fails or yields a store whose two traversals agree, whose documents are
// documents, and which closes — and never panics or runs unbounded.
func FuzzDurableRecovery(f *testing.F) {
	options := fuzzRecoveryOptions()

	// A store that has only ever been created, so the seed covers the smallest
	// legal image and the first-generation superblock.
	empty := fuzzRecoverySeedImage(f, func(*Collection) error { return nil })
	// A steady-state store: several chunks, an index, and an overflow value,
	// and a free set with something in it.
	populated := fuzzRecoverySeedImage(f, func(collection *Collection) error {
		for round := range 3 {
			for i := range 12 {
				padding := 40
				if (round+i)%3 == 0 {
					padding = 900
				}
				if _, err := collection.Put([]byte(commitCrashKey(i)),
					commitCrashDocument(round, i, padding)); err != nil {
					return err
				}
			}
			for i := round; i < 12; i += 3 {
				if _, err := collection.Delete([]byte(commitCrashKey(i))); err != nil {
					return err
				}
			}
		}
		return nil
	})
	f.Add(empty)
	f.Add(populated)
	// Truncations are the shape a torn write leaves, and they are the inputs most
	// likely to reach a decoder holding a length it cannot satisfy.
	f.Add(populated[:len(populated)/2])
	f.Add(populated[:len(populated)-1])
	f.Add(populated[:3*options.PageSize])
	f.Add([]byte(nil))
	f.Add([]byte("SJPAGE01"))

	directory := f.TempDir()
	path := filepath.Join(directory, "image")
	f.Fuzz(func(t *testing.T, image []byte) {
		// The cap is a little over twice the populated seed, which is about
		// 136 KiB. It is the "bounded time" half of the contract made explicit,
		// and it is also what keeps the campaign productive: the mutator grows
		// inputs given the chance, and a run whose corpus drifts to megabyte
		// images spends its budget writing files instead of reaching decoders.
		// Skipping before the write means an oversized candidate costs nothing.
		if len(image) > 320<<10 {
			t.Skip("input beyond the size this target bounds itself to")
		}
		if err := os.WriteFile(path, image, 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		collection, err := Open(file, options)
		if err != nil {
			// Rejecting a file is always allowed. What is not allowed is
			// rejecting it and keeping the writer lease, which would make the
			// process unable to open that file ever again.
			return
		}
		defer collection.Close()
		assertFuzzRecoveredStoreIsSelfConsistent(t, collection)
	})
}

func commitCrashKey(i int) string {
	return "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

// assertFuzzRecoveredStoreIsSelfConsistent checks the only properties a store
// recovered from unknown bytes can be held to.
//
// Every read here is allowed to fail: a page the roots reach may be torn, and
// reporting that is correct behaviour. What is not allowed is answering. A
// document returned by the scan that the key directory does not hold, or holds
// differently, means the two roots the superblock published describe different
// collections. A returned document that is not valid JSON means a payload was
// admitted without being validated, which is what the admission-time directory
// check exists to prevent and what a zero-copy reader would then hand to a
// caller as a borrowed view.
func assertFuzzRecoveredStoreIsSelfConsistent(t *testing.T, collection *Collection) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		return
	}
	defer snapshot.Close()

	scanned := make(map[string]string, 64)
	scanErr := snapshot.RangeRaw(func(key, value []byte) error {
		if _, duplicate := scanned[string(key)]; duplicate {
			t.Fatalf("the chunk-tree scan returned the key %q twice", key)
		}
		if !vibejson.Valid(value) {
			t.Fatalf("the scan returned %q for key %q, which is not a document the "+
				"store could have accepted", value, key)
		}
		scanned[string(key)] = string(value)
		return nil
	})
	if scanErr != nil {
		// A scan that stops partway still produced answers, and every answer it
		// did produce has to hold up.
		assertFuzzKeyDirectoryAgrees(t, snapshot, scanned)
		return
	}
	if uint64(len(scanned)) != snapshot.Len() {
		t.Fatalf("the chunk-tree scan completed with %d documents but the state root "+
			"says %d", len(scanned), snapshot.Len())
	}
	assertFuzzKeyDirectoryAgrees(t, snapshot, scanned)
	// The free log is replayed lazily, on the first mutation rather than at Open,
	// so a corrupt chain is not reached by any read above. Driving it here is what
	// puts the chain, the segment index, and the segments themselves under the
	// mutator.
	_ = collection.refreshReusable(collection.state.Load())
}

func assertFuzzKeyDirectoryAgrees(t *testing.T, snapshot *Snapshot, scanned map[string]string) {
	t.Helper()
	for key, want := range scanned {
		got, ok, err := snapshot.AppendRaw(nil, []byte(key))
		if err != nil {
			continue
		}
		if !ok {
			t.Fatalf("the chunk-tree scan holds %q but the key directory does not", key)
		}
		if string(got) != want {
			t.Fatalf("key %q is %q by the chunk-tree scan and %q by the key directory",
				key, want, got)
		}
	}
}
