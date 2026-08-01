package conformance

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const capabilityStart = "<!-- capability-matrix:start -->\n"
const capabilityEnd = "<!-- capability-matrix:end -->"

func TestCapabilityManifestCoversEveryDimension(t *testing.T) {
	entries := map[EntryPoint]bool{}
	indexing := map[Indexing]bool{}
	transactions := map[Transaction]bool{}
	keys := map[Keys]bool{}
	operations := map[Operation]bool{}
	lanes := map[Lane]bool{}
	ids := map[string]bool{}
	for _, c := range Cases {
		if c.ID == "" || ids[c.ID] {
			t.Fatalf("empty or duplicate capability id %q", c.ID)
		}
		ids[c.ID] = true
		entries[c.Entry] = true
		indexing[c.Indexing] = true
		transactions[c.Transaction] = true
		for _, value := range c.Keys {
			keys[value] = true
		}
		for _, value := range c.Operations {
			operations[value] = true
		}
		for _, value := range c.Lanes {
			lanes[value] = true
		}
		if len(c.Keys) == 0 || len(c.Operations) == 0 || len(c.Lanes) == 0 {
			t.Fatalf("capability %q has an empty executable dimension", c.ID)
		}
		if c.Result == DocumentedError && c.Error == "" {
			t.Fatalf("documented-error capability %q has no typed error", c.ID)
		}
		if c.Rollback && (!c.Atomic || c.Result != Success) {
			t.Fatalf("capability %q requests rollback without atomic success", c.ID)
		}
	}
	if len(entries) != 3 || len(indexing) != 2 || len(transactions) != 2 ||
		len(keys) != 2 || len(operations) != 4 || len(lanes) != 8 {
		t.Fatalf("manifest dimensions: entries=%v indexing=%v transactions=%v keys=%v operations=%v lanes=%v",
			entries, indexing, transactions, keys, operations, lanes)
	}
}

func TestCapabilityManifestCoversCartesianContract(t *testing.T) {
	type cell struct {
		entry       EntryPoint
		indexing    Indexing
		transaction Transaction
		keys        Keys
		operation   Operation
		lane        Lane
	}
	covered := map[cell]string{}
	for _, capability := range Cases {
		for _, keys := range capability.Keys {
			for _, operation := range capability.Operations {
				for _, lane := range capability.Lanes {
					key := cell{
						entry: capability.Entry, indexing: capability.Indexing,
						transaction: capability.Transaction, keys: keys,
						operation: operation, lane: lane,
					}
					if prior := covered[key]; prior != "" {
						t.Fatalf("capability cell %+v is claimed by both %q and %q",
							key, prior, capability.ID)
					}
					covered[key] = capability.ID
				}
			}
		}
	}
	for _, entry := range []EntryPoint{Native, DatabaseSQL, PGWire} {
		lanes := []Lane{SQLDefaultSyncJournal}
		if entry == Native {
			lanes = NativeLanes
		}
		for _, indexing := range []Indexing{Unindexed, Indexed} {
			for _, transaction := range []Transaction{Autocommit, Explicit} {
				for _, keys := range []Keys{OneKey, MultipleKeys} {
					for _, operation := range AllOperations {
						for _, lane := range lanes {
							key := cell{entry, indexing, transaction, keys, operation, lane}
							if covered[key] == "" {
								t.Fatalf("capability manifest is missing Cartesian cell %+v", key)
							}
						}
					}
				}
			}
		}
	}
}

func TestRemovedIndexedCapabilityErrorsDoNotReturn(t *testing.T) {
	root := filepath.Join("..", "..")
	forbidden := []string{
		"ErrTransaction" + "IndexedTable",
		"ErrPrimaryBatch" + "IndexedUnsupported",
		"transaction cannot mutate an " + "indexed table",
		"transactional batch on an indexed primary collection " +
			"is not yet supported",
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".md" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, text := range forbidden {
			if bytes.Contains(raw, []byte(text)) {
				t.Errorf("%s reintroduces removed indexed-capability text %q", path, text)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublicUpdateContractAcknowledgesTopologyPublication(t *testing.T) {
	root := filepath.Join("..", "..")
	paths := []string{
		filepath.Join(root, "store", "durable", "store_file_batch.go"),
		filepath.Join(root, "docs", "store.md"),
		filepath.Join(root, "docs", "architecture.md"),
		filepath.Join(root, "docs", "design", "hybrid-mutations.md"),
	}
	required := []string{
		"logical failure-atomic publication",
		"content-equivalent topology generation",
		"Generation may advance",
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		plain := strings.NewReplacer("//", " ", "`", "").Replace(string(raw))
		normalized := strings.Join(strings.Fields(plain), " ")
		for _, phrase := range required {
			if !strings.Contains(normalized, phrase) {
				t.Errorf("%s omits Update topology contract %q", path, phrase)
			}
		}
		for _, phrase := range []string{
			"leaves the collection exactly " + "as it was",
			"applies many mutations as one " + "generation",
			"publishes one failure-atomic " + "generation",
		} {
			if strings.Contains(normalized, phrase) {
				t.Errorf("%s retains stale Update contract %q", path, phrase)
			}
		}
	}
}

func TestPublicCapabilityTableMatchesExecutableManifest(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "capabilities.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, capabilityStart)
	end := strings.Index(text, capabilityEnd)
	if start < 0 || end < start {
		t.Fatalf("%s is missing capability matrix markers", path)
	}
	start += len(capabilityStart)
	want := RenderMarkdown()
	if got := text[start:end]; got != want {
		t.Fatalf("public capability table is stale; regenerate it from conformance.RenderMarkdown()\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}
