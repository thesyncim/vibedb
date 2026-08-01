package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/conformance"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// TestNativeCapabilityMatrix consumes every native row in the public
// conformance manifest. Every listed operation expands into an independent
// subtest and fresh collection; indexed rows additionally compare both posting
// candidates and exact answers with an independent scan oracle before and
// after reopen.
func TestNativeCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.Native) {
		capability := capability
		t.Run(capability.ID, func(t *testing.T) {
			for _, lane := range capability.Lanes {
				lane := lane
				t.Run(string(lane), func(t *testing.T) {
					for _, keys := range capability.Keys {
						keys := keys
						t.Run(string(keys), func(t *testing.T) {
							for _, operation := range capability.Operations {
								operation := operation
								t.Run(string(operation), func(t *testing.T) {
									fixture := openNativeCapabilityFixture(
										t, lane, capability.Indexing == conformance.Indexed,
									)
									defer fixture.close(t)

									if capability.Transaction == conformance.Explicit {
										wantSupport := capability.Result == conformance.Success
										if got := fixture.collection.SupportsUpdate(); got != wantSupport {
											t.Fatalf("SupportsUpdate = %v, want %v", got, wantSupport)
										}
									}

									before := nativeCapabilityContent(t, fixture.collection)
									generation := fixture.collection.Generation()
									old, err := fixture.collection.Snapshot()
									if err != nil {
										t.Fatal(err)
									}

									callbackRan := false
									err = applyNativeCapability(
										fixture.collection, capability.Transaction, keys,
										operation, func() { callbackRan = true },
									)
									if capability.Result == conformance.DocumentedError {
										if !errors.Is(err, ErrPrimaryBatchUnsupportedLane) {
											t.Fatalf("Update error = %v, want ErrPrimaryBatchUnsupportedLane", err)
										}
										if callbackRan {
											t.Fatal("unsupported Update ran its callback")
										}
										if fixture.collection.Generation() != generation {
											t.Fatalf("refused Update advanced generation %d -> %d",
												generation, fixture.collection.Generation())
										}
										assertNativeCapabilityContent(t, fixture.collection, before)
										assertNativeCapabilityIndexes(t, fixture.collection)
										if fixture.collection.PersistenceError() != nil {
											t.Fatalf("capability refusal poisoned collection: %v",
												fixture.collection.PersistenceError())
										}
										if _, putErr := fixture.collection.Put(
											[]byte("after-refusal"), nativeCapabilityDoc("usable", 99),
										); putErr != nil {
											t.Fatalf("point write after capability refusal: %v", putErr)
										}
										_ = old.Close()
										return
									}
									if err != nil {
										t.Fatalf("capability execution: %v", err)
									}
									if capability.Transaction == conformance.Explicit && !callbackRan {
										t.Fatal("supported Update did not run its callback")
									}
									if fixture.collection.Generation() <= generation {
										t.Fatalf("successful capability did not publish: generation %d",
											fixture.collection.Generation())
									}
									if got := nativeCapabilitySnapshotContent(t, old); !nativeMapsEqual(got, before) {
										t.Fatalf("old snapshot crossed atomic cut: got=%v want=%v", got, before)
									}
									if err := old.Close(); err != nil {
										t.Fatal(err)
									}
									assertNativeCapabilityIndexes(t, fixture.collection)

									if capability.Atomic && capability.Rollback {
										assertNativeCapabilityRollback(t, fixture.collection,
											capability.Transaction)
									}
									if nativeCapabilityReopenGate(capability, keys) {
										want := nativeCapabilityContent(t, fixture.collection)
										fixture.reopen(t)
										assertNativeCapabilityContent(t, fixture.collection, want)
										assertNativeCapabilityIndexes(t, fixture.collection)
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

type nativeCapabilityFixture struct {
	path       string
	file       *os.File
	collection *Collection
	options    Options
}

func openNativeCapabilityFixture(
	t *testing.T, lane conformance.Lane, indexed bool,
) *nativeCapabilityFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capability.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := nativeCapabilityOptions(lane, indexed)
	createOptions := options
	if lane == conformance.SyncChainFence {
		createOptions.Durability = DurabilityAsyncVisible
	}
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"a", "b", "c", "d"} {
		if err := builder.Append(key, nativeCapabilityDoc("old", i)); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, file, createOptions); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	return &nativeCapabilityFixture{
		path: path, file: file, collection: collection, options: options,
	}
}

// Reopen is the persistence gate, not a reason to multiply every semantic
// permutation by another filesystem lifecycle. Point rows qualify each lane
// once; batch rows qualify the wider multi-key shape, which subsumes one key.
func nativeCapabilityReopenGate(
	capability conformance.Case, keys conformance.Keys,
) bool {
	if capability.Result == conformance.DocumentedError {
		return false
	}
	return capability.Transaction == conformance.Autocommit ||
		keys == conformance.MultipleKeys
}

func nativeCapabilityOptions(lane conformance.Lane, indexed bool) Options {
	options := Options{
		Collection: store.Options{ChunkDocuments: 4},
		Backend:    BackendPortable, WriteMode: WriteBuffered,
		ResidentBytes: 16 << 20, MaxKeyBytes: 128,
		InlineValueBytes: 512, MaxDocumentBytes: 2048,
		MaxBatchDocuments: 16, MaxSnapshotLeases: 32,
		MaxRetiredExtents: 4096, GroupLimit: 1,
	}
	switch lane {
	case conformance.SyncJournal, conformance.SyncChainFence:
		options.Durability = DurabilitySync
	case conformance.AsyncCOW:
		options.Durability = DurabilityAsyncVisible
	case conformance.BufferedVolatilePowerSafe:
		options.Durability = DurabilityBufferedVisible
	case conformance.BufferedVolatileFilesystem:
		options.Durability = DurabilityBufferedVisible
		options.CheckpointStrength = CheckpointFilesystem
	case conformance.BufferedJournalPowerSafe:
		options.Durability = DurabilityBufferedVisible
		options.RecoveryJournal = true
	case conformance.BufferedJournalFilesystem:
		options.Durability = DurabilityBufferedVisible
		options.CheckpointStrength = CheckpointFilesystem
		options.RecoveryJournal = true
	default:
		panic("unknown native conformance lane " + lane)
	}
	if indexed {
		options.Indexes = []store.IndexDefinition{{
			Name: "by_group", Paths: []string{"/group"},
		}}
	}
	return options
}

func (f *nativeCapabilityFixture) reopen(t *testing.T) {
	t.Helper()
	want := nativeCapabilityContent(t, f.collection)
	if err := f.collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.file, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.collection = reopened
	assertNativeCapabilityContent(t, f.collection, want)
}

func (f *nativeCapabilityFixture) close(t *testing.T) {
	t.Helper()
	if f.collection != nil {
		_ = f.collection.Close()
	}
	if f.file != nil {
		_ = f.file.Close()
	}
}

func applyNativeCapability(
	collection *Collection, transaction conformance.Transaction,
	keys conformance.Keys, operation conformance.Operation, callback func(),
) error {
	if transaction == conformance.Autocommit {
		callback()
		switch operation {
		case conformance.Insert:
			if keys == conformance.OneKey {
				_, err := collection.Put([]byte("insert-one"), nativeCapabilityDoc("insert", 10))
				return err
			}
			if _, err := collection.Put([]byte("insert-a"), nativeCapabilityDoc("insert", 20)); err != nil {
				return err
			}
			_, err := collection.Put([]byte("insert-b"), nativeCapabilityDoc("insert", 21))
			return err
		case conformance.Update:
			if _, err := collection.Put([]byte("a"), nativeCapabilityDoc("updated", 22)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			_, err := collection.Put([]byte("b"), nativeCapabilityDoc("updated", 23))
			return err
		case conformance.Delete:
			if _, err := collection.Delete([]byte("c")); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			_, err := collection.Delete([]byte("d"))
			return err
		case conformance.Mixed:
			if keys == conformance.OneKey {
				if _, err := collection.Put([]byte("c"), nativeCapabilityDoc("mixed-before", 12)); err != nil {
					return err
				}
				if _, err := collection.Delete([]byte("c")); err != nil {
					return err
				}
				_, err := collection.Put([]byte("c"), nativeCapabilityDoc("mixed-final", 13))
				return err
			}
			if _, err := collection.Put([]byte("mixed-new"), nativeCapabilityDoc("mixed-insert", 24)); err != nil {
				return err
			}
			if _, err := collection.Put([]byte("a"), nativeCapabilityDoc("mixed-update", 25)); err != nil {
				return err
			}
			_, err := collection.Delete([]byte("c"))
			return err
		default:
			return fmt.Errorf("unknown native capability operation %q", operation)
		}
	}
	return collection.Update(func(batch *WriteBatch) error {
		callback()
		switch operation {
		case conformance.Insert:
			if err := batch.Put([]byte("insert-a"), nativeCapabilityDoc("insert", 40)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Put([]byte("insert-b"), nativeCapabilityDoc("insert", 41))
		case conformance.Update:
			if err := batch.Put([]byte("a"), nativeCapabilityDoc("updated", 42)); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Put([]byte("b"), nativeCapabilityDoc("updated", 43))
		case conformance.Delete:
			if err := batch.Delete([]byte("c")); err != nil {
				return err
			}
			if keys == conformance.OneKey {
				return nil
			}
			return batch.Delete([]byte("d"))
		case conformance.Mixed:
			if keys == conformance.OneKey {
				if err := batch.Put([]byte("c"), nativeCapabilityDoc("mixed-before", 44)); err != nil {
					return err
				}
				if err := batch.Delete([]byte("c")); err != nil {
					return err
				}
				return batch.Put([]byte("c"), nativeCapabilityDoc("mixed-final", 45))
			}
			if err := batch.Put([]byte("mixed-new"), nativeCapabilityDoc("mixed-insert", 46)); err != nil {
				return err
			}
			if err := batch.Put([]byte("a"), nativeCapabilityDoc("mixed-update", 47)); err != nil {
				return err
			}
			return batch.Delete([]byte("c"))
		default:
			return fmt.Errorf("unknown native capability operation %q", operation)
		}
	})
}

func assertNativeCapabilityRollback(
	t *testing.T, collection *Collection, transaction conformance.Transaction,
) {
	t.Helper()
	want := nativeCapabilityContent(t, collection)
	generation := collection.Generation()
	if transaction == conformance.Explicit {
		err := collection.Update(func(batch *WriteBatch) error {
			if err := batch.Put([]byte("rollback-good"), nativeCapabilityDoc("rollback", 1)); err != nil {
				return err
			}
			return batch.Put([]byte("rollback-bad"), []byte(`{"group":`))
		})
		if err == nil {
			t.Fatal("malformed batch succeeded")
		}
	} else {
		if _, err := collection.Put([]byte("rollback-bad"), []byte(`{"group":`)); err == nil {
			t.Fatal("malformed point mutation succeeded")
		}
	}
	if collection.Generation() != generation {
		t.Fatalf("rejected mutation advanced generation %d -> %d",
			generation, collection.Generation())
	}
	assertNativeCapabilityContent(t, collection, want)
	assertNativeCapabilityIndexes(t, collection)
}

func nativeCapabilityDoc(group string, n int) []byte {
	return fmt.Appendf(nil, `{"group":%q,"n":%d}`, group, n)
}

func nativeCapabilityContent(t *testing.T, collection *Collection) map[string]string {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	return nativeCapabilitySnapshotContent(t, snapshot)
}

func nativeCapabilitySnapshotContent(t *testing.T, snapshot *Snapshot) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		out[string(key)] = string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertNativeCapabilityContent(
	t *testing.T, collection *Collection, want map[string]string,
) {
	t.Helper()
	if got := nativeCapabilityContent(t, collection); !nativeMapsEqual(got, want) {
		t.Fatalf("content = %v, want %v", got, want)
	}
}

func nativeMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func assertNativeCapabilityIndexes(t *testing.T, collection *Collection) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if len(snapshot.AppendIndexes(nil)) == 0 {
		return
	}
	want := map[string][]string{}
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		var doc struct {
			Group string `json:"group"`
		}
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}
		want[doc.Group] = append(want[doc.Group], string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want["absent-sentinel"] = nil
	for group, keys := range want {
		slices.Sort(keys)
		needleRaw, _ := json.Marshal(group)
		need, err := vibejson.RequiredIndexEntries(needleRaw)
		if err != nil {
			t.Fatal(err)
		}
		needle, err := vibejson.BuildIndex(
			needleRaw, make([]vibejson.IndexEntry, need),
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidates := range []bool{false, true} {
			var masks []store.Mask
			if candidates {
				masks, err = snapshot.AppendIndexCandidateMasks(
					nil, "by_group", needle,
				)
			} else {
				masks, err = snapshot.AppendIndexMasks(nil, "by_group", needle)
			}
			if err != nil {
				t.Fatalf("group=%q candidates=%v: %v", group, candidates, err)
			}
			var got []string
			if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
				got = append(got, string(key))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.Sort(got)
			if !slices.Equal(got, keys) {
				t.Fatalf("group=%q candidates=%v keys=%v want=%v",
					group, candidates, got, keys)
			}
		}
	}
}
