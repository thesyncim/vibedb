package replicatedstate

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func createTargetAt(t testing.TB, dir, name string, options durable.Options) CollectionTarget {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	if name == "system" {
		return systemTargetOf(collection)
	}
	return targetOf(collection)
}

func machineOptionsFor(user CollectionTarget) Options {
	return Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 2, MaxDocuments: user.Limits.MaxDistinctMutations + 2,
			MaxBytes: 64 << 20,
		},
		MaxCompletions: 128,
	}
}

func TestOpenRejectsForeignTransactionDirectoryWithoutMutation(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	system := createTargetAt(t, dirA, "system", durable.Options{})
	user := createTargetAt(t, dirB, "user", durable.Options{})
	log, err := durable.OpenTxnLog(dirA, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	_, err = Open(
		testBinding(), testBootstrap(), system, UserCollection{Name: "docs", Target: user},
		log, machineOptionsFor(user),
	)
	if !errors.Is(err, ErrInvalidCollection) ||
		!errors.Is(err, durable.ErrTransactionLogDirectoryMismatch) {
		t.Fatalf("Open error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dirA, "txn.vtm")); !os.IsNotExist(statErr) {
		t.Fatalf("construction minted transaction log: %v", statErr)
	}
	if system.Collection.Len() != 0 || user.Collection.Len() != 0 {
		t.Fatalf("construction mutated rows: system=%d user=%d", system.Collection.Len(), user.Collection.Len())
	}
}

func TestOpenRejectsSyncReopenedChainFence(t *testing.T) {
	dir := t.TempDir()
	async := durable.Options{Durability: durable.DurabilityAsyncVisible}
	path := filepath.Join(dir, "user.vdb")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, async)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err = durable.Open(file, durable.Options{Durability: durable.DurabilitySync})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	if collection.SupportsUpdate() {
		t.Fatal("fixture unexpectedly supports Update")
	}
	user := targetOf(collection)
	system := createTargetAt(t, dir, "system", durable.Options{})
	log, err := durable.OpenTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	_, err = Open(
		testBinding(), testBootstrap(), system, UserCollection{Name: "docs", Target: user},
		log, machineOptionsFor(user),
	)
	if !errors.Is(err, ErrSchemaProfile) {
		t.Fatalf("Open error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "txn.vtm")); !os.IsNotExist(statErr) {
		t.Fatalf("construction minted transaction log: %v", statErr)
	}
}

func TestBindingAndUserNameRejectEmbeddedNUL(t *testing.T) {
	binding := testBinding()
	binding.Distribution = "dist\x00other"
	if err := binding.validate(); !errors.Is(err, ErrInvalidBinding) {
		t.Fatalf("binding error = %v", err)
	}
	fixture := newMachineFixture(t)
	_, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs\x00other", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("user-name error = %v", err)
	}
}

func TestOpenTransactionByteProofCapsUserBatchAtCommandEnvelope(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "user", durable.Options{MaxBatchBytes: math.MaxInt})
	log, err := durable.OpenTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	maxSystem := len(stateKey) + 2*MaxStateEnvelopeBytes + 2 +
		33 + 2*MaxCompletionRecordBytes + 2
	required, ok := checkedTxnBytes(replication.MaxCommandBytes, maxSystem)
	if !ok {
		t.Fatal("test byte proof overflowed")
	}
	options := Options{TxnLimits: durable.TxnLimits{
		MaxCollections: 2,
		MaxDocuments:   user.Limits.MaxDistinctMutations + 2,
		MaxBytes:       required,
	}, MaxCompletions: 128}
	if _, err := Open(testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, log, options); err != nil {
		t.Fatalf("exact bounded transaction proof: %v", err)
	}
	options.TxnLimits.MaxBytes--
	if _, err := Open(testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, log, options); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("one-under bounded transaction proof error = %v", err)
	}
}
