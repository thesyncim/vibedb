package driver

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestReplicatedChildApplyReservationPersistsExactIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prepared-child.vdb")
	binding := testReplicatedBinding(181)
	binding.Distribution, binding.Shard = "orders", "right"
	binding.AllocationGeneration = 9
	database, err := InitializeShardStore(path, ShardStoreBinding{
		Distribution: "orders", Shard: "right", AllocationGeneration: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := database.BindReplicatedShardStore(binding, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Skipf("sealed replicated sidecars require strict allocation support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{Start: distribution.KeyspacePoint{0x80}, End: distribution.KeyspaceEnd{Max: true}}
	reserved := newReplicatedApplyMeta(
		base, strings.Repeat("a", storageIdentityBytes*2),
		strings.Repeat("b", storageIdentityBytes*2), options,
	).identity()
	if err = database.ReserveReplicatedChildApply(base, reserved); err != nil {
		t.Fatal(err)
	}
	if err = database.ReserveReplicatedChildApply(base, reserved); err != nil {
		t.Fatalf("exact reservation retry: %v", err)
	}
	forged := reserved
	forged.Storage = strings.Repeat("c", storageIdentityBytes*2)
	if err = database.ReserveReplicatedChildApply(base, forged); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("substituted reservation err=%v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = OpenReplicatedShardStore(path, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	recovered, present, err := database.ReplicatedChildApplyReservation(base)
	if err != nil || !present || recovered != reserved {
		t.Fatalf("recovered=%+v present=%v err=%v", recovered, present, err)
	}
}
