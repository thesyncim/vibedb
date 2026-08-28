package raftservice

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// Invoked by the real authenticated three-voter test after a committed write;
// this deliberately uses the production SQL apply source, not a fake barrier.
func testRF3DataReadCut(t *testing.T, ctx context.Context, owner *Owner,
	fence ServingFence, minimum uint64, key, want []byte,
) {
	t.Helper()
	request := LinearizableDataReadRequest{
		Fence: fence, Capability: serviceauthz.CapabilityDataRead,
		Relations: []replication.RelationID{1},
	}
	var cut LinearizableDataReadCut
	defer cut.Close()
	for range 2 {
		if err := owner.ReadLinearizableDataInto(ctx, request, &cut); err != nil {
			t.Fatalf("quorum data cut: %v", err)
		}
		data := cut.Data()
		if data == nil || data.Fence().Applied < minimum || !data.OwnsKey(1, key) {
			t.Fatalf("quorum data cut did not retain the committed ownership/floor")
		}
		snapshot, ok := data.Relation(1)
		if !ok {
			t.Fatal("admitted relation missing")
		}
		got, found, err := snapshot.AppendRaw(nil, key)
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("quorum data row=%q found=%v err=%v", got, found, err)
		}
		if err := owner.ReadLinearizableDataInto(ctx, request, &cut); !errors.Is(err, replicatedstate.ErrDataReadOpen) {
			t.Fatalf("live-cut overwrite=%v", err)
		}
		if err := cut.Close(); err != nil || cut.Data() != nil {
			t.Fatalf("data cut close=%v", err)
		}
		if err := cut.Close(); err != nil {
			t.Fatalf("second data cut close=%v", err)
		}
	}
	for _, capability := range []serviceauthz.Capability{
		0, serviceauthz.CapabilityBackup, serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityBackup,
	} {
		denied := request
		denied.Capability = capability
		if err := owner.ReadLinearizableDataInto(ctx, denied, &cut); !errors.Is(err, ErrInvalidOwner) || cut.Data() != nil {
			t.Fatalf("capability %v data cut=%v", capability, err)
		}
	}
	stale := request
	stale.Fence.Command.RelationManifestDigest[0] ^= 1
	if err := owner.ReadLinearizableDataInto(ctx, stale, &cut); !errors.Is(err, ErrServingFence) || cut.Data() != nil {
		t.Fatalf("stale data cut=%v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := owner.ReadLinearizableDataInto(canceled, request, &cut); !errors.Is(err, context.Canceled) || cut.Data() != nil {
		t.Fatalf("canceled data cut=%v", err)
	}
}
