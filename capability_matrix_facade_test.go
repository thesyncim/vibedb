package vibedb_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb"
	"github.com/thesyncim/vibedb/internal/conformance"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestFacadeCapabilityMatrix is the API-authority adapter for native
// multi-collection rows. The durable adapter exhausts journal lanes; this
// adapter drives vibedb.Update through the Durable profile for journal-lane
// success, the Buffered profile for the unsupported-lane refusal, and the
// Memory profile's visibility-atomic dual (no crash dimension).
func TestFacadeCapabilityMatrix(t *testing.T) {
	for _, capability := range conformance.CasesFor(conformance.Native) {
		if !facadeCapabilityApplies(capability) {
			continue
		}
		capability := capability
		t.Run(capability.ID, func(t *testing.T) {
			for _, keys := range capability.Keys {
				keys := keys
				t.Run(string(keys), func(t *testing.T) {
					for _, operation := range capability.Operations {
						operation := operation
						t.Run(string(operation), func(t *testing.T) {
							for _, profile := range facadeProfilesFor(capability) {
								profile := profile
								t.Run(profileName(profile), func(t *testing.T) {
									runFacadeDatabaseTxnCapability(
										t, capability, profile, keys, operation,
									)
								})
							}
						})
					}
				})
			}
		})
	}
}

func facadeCapabilityApplies(capability conformance.Case) bool {
	for _, tables := range capability.Tables {
		if tables == conformance.MultipleTables {
			return true
		}
	}
	return false
}

func facadeProfilesFor(capability conformance.Case) []vibedb.Durability {
	if capability.Result == conformance.DocumentedError {
		return []vibedb.Durability{vibedb.Buffered}
	}
	return []vibedb.Durability{vibedb.Durable, vibedb.Memory}
}

func runFacadeDatabaseTxnCapability(
	t *testing.T, capability conformance.Case, profile vibedb.Durability,
	keys conformance.Keys, operation conformance.Operation,
) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "facade-capability.vdb")
	db := openFacadeCapabilityDB(t, path, profile, capability.Indexing == conformance.Indexed)
	defer db.Close()

	names := []string{"alpha", "beta"}
	before := map[string]map[string]string{}
	generations := map[string]uint64{}
	for _, name := range names {
		seedFacadeCapabilityCollection(t, db.Collection(name))
		before[name] = facadeCapabilityContent(t, db.Collection(name))
		generations[name] = facadePublishedGeneration(t, db.Collection(name))
	}

	err := db.Update(func(tx *vibedb.Tx) error {
		for _, name := range names {
			if applyErr := applyFacadeCapability(
				tx.Collection(name), keys, operation,
			); applyErr != nil {
				return applyErr
			}
		}
		return nil
	})
	if capability.Result == conformance.DocumentedError {
		if !errors.Is(err, vibedb.ErrTxUnsupportedLane) &&
			!errors.Is(err, durable.ErrDatabaseTransactionUnsupportedLane) {
			t.Fatalf("Update error = %v, want ErrTxUnsupportedLane", err)
		}
		for _, name := range names {
			collection := db.Collection(name)
			if facadePublishedGeneration(t, collection) != generations[name] {
				t.Fatalf("%s generation advanced on refusal", name)
			}
			assertFacadeCapabilityContent(t, collection, before[name])
		}
		return
	}
	if err != nil {
		t.Fatalf("capability execution: %v", err)
	}
	for _, name := range names {
		collection := db.Collection(name)
		if facadePublishedGeneration(t, collection) <= generations[name] {
			t.Fatalf("%s did not publish", name)
		}
	}
	if capability.Atomic && capability.Rollback {
		assertFacadeCapabilityRollback(t, db, names)
	}
	if profile != vibedb.Memory && keys == conformance.MultipleKeys {
		want := map[string]map[string]string{}
		for _, name := range names {
			want[name] = facadeCapabilityContent(t, db.Collection(name))
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := vibedb.Open(path, vibedb.WithDurability(profile))
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		for _, name := range names {
			assertFacadeCapabilityContent(t, reopened.Collection(name), want[name])
		}
	}
}

func openFacadeCapabilityDB(
	t *testing.T, path string, profile vibedb.Durability, indexed bool,
) *vibedb.Database {
	t.Helper()
	db, err := vibedb.Open(path, vibedb.WithDurability(profile))
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		for _, name := range []string{"alpha", "beta"} {
			if err := db.Collection(name).CreateIndex("by_group", "/group"); err != nil {
				t.Fatal(err)
			}
		}
	}
	return db
}

func seedFacadeCapabilityCollection(t *testing.T, collection *vibedb.Collection) {
	t.Helper()
	for i, key := range []string{"a", "b", "c", "d"} {
		if _, err := collection.Put(key, facadeCapabilityDoc("old", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Durable/Buffered seed Puts leave a legacy journal window; Flush folds it
	// so a later multi-collection Update can prepare against a conditional
	// journal. Memory Flush is a no-op.
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
}

func applyFacadeCapability(
	collection *vibedb.TxCollection, keys conformance.Keys, operation conformance.Operation,
) error {
	switch operation {
	case conformance.Insert:
		if _, err := collection.Put("insert-a", facadeCapabilityDoc("insert", 40)); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		_, err := collection.Put("insert-b", facadeCapabilityDoc("insert", 41))
		return err
	case conformance.Update:
		if _, err := collection.Put("a", facadeCapabilityDoc("updated", 42)); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		_, err := collection.Put("b", facadeCapabilityDoc("updated", 43))
		return err
	case conformance.Delete:
		if _, err := collection.Delete("c"); err != nil {
			return err
		}
		if keys == conformance.OneKey {
			return nil
		}
		_, err := collection.Delete("d")
		return err
	case conformance.Mixed:
		if keys == conformance.OneKey {
			if _, err := collection.Put("c", facadeCapabilityDoc("mixed-before", 44)); err != nil {
				return err
			}
			if _, err := collection.Delete("c"); err != nil {
				return err
			}
			_, err := collection.Put("c", facadeCapabilityDoc("mixed-final", 45))
			return err
		}
		if _, err := collection.Put("mixed-new", facadeCapabilityDoc("mixed-insert", 46)); err != nil {
			return err
		}
		if _, err := collection.Put("a", facadeCapabilityDoc("mixed-update", 47)); err != nil {
			return err
		}
		_, err := collection.Delete("c")
		return err
	default:
		return fmt.Errorf("unknown facade capability operation %q", operation)
	}
}

func assertFacadeCapabilityRollback(t *testing.T, db *vibedb.Database, names []string) {
	t.Helper()
	want := map[string]map[string]string{}
	generations := map[string]uint64{}
	for _, name := range names {
		want[name] = facadeCapabilityContent(t, db.Collection(name))
		generations[name] = facadePublishedGeneration(t, db.Collection(name))
	}
	err := db.Update(func(tx *vibedb.Tx) error {
		for _, name := range names {
			if _, err := tx.Collection(name).Put(
				"rollback-good", facadeCapabilityDoc("rollback", 1),
			); err != nil {
				return err
			}
		}
		_, err := tx.Collection(names[len(names)-1]).Put(
			"rollback-bad", []byte(`{"group":`),
		)
		return err
	})
	if err == nil {
		t.Fatal("malformed multi-collection facade Update succeeded")
	}
	for _, name := range names {
		collection := db.Collection(name)
		if facadePublishedGeneration(t, collection) != generations[name] {
			t.Fatalf("%s rejected sibling advanced generation", name)
		}
		assertFacadeCapabilityContent(t, collection, want[name])
	}
}

func facadeCapabilityDoc(group string, n int) []byte {
	return fmt.Appendf(nil, `{"group":%q,"n":%d}`, group, n)
}

func facadeCapabilityContent(t *testing.T, collection *vibedb.Collection) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := collection.Range(func(key string, document []byte) error {
		// Range may reuse the key/document buffers across callbacks.
		out[string(append([]byte(nil), key...))] = string(append([]byte(nil), document...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertFacadeCapabilityContent(
	t *testing.T, collection *vibedb.Collection, want map[string]string,
) {
	t.Helper()
	got := facadeCapabilityContent(t, collection)
	if len(got) != len(want) {
		t.Fatalf("content = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("content = %v, want %v", got, want)
		}
	}
}

func facadePublishedGeneration(t *testing.T, collection *vibedb.Collection) uint64 {
	t.Helper()
	metrics, err := collection.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	return metrics.PublishedGeneration
}
