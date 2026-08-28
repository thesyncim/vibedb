package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

// TestUnreleasedSchemaRolloutRollingRestartBoundary keeps binary rollout and
// data-schema rollout separate. A controller built for another schema-install
// contract cannot prepare the cut. Once the exact current contract is durably
// prepared, a fresh controller can finish it from replicated state, while a
// different target generation remains fenced.
func TestUnreleasedSchemaRolloutRollingRestartBoundary(t *testing.T) {
	authority, _, base := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, base)
	operationID := sha256.Sum256([]byte("rolling-schema-boundary"))

	oldBuildReceipts := append([]SchemaRolloutPreparedGroup(nil), receipts...)
	oldBuildReceipts[0].ContractDigest[0] ^= 0x80
	if _, err := authority.PrepareSchemaRollout(
		context.Background(), operationID, target, oldBuildReceipts,
	); !errors.Is(err, ErrSchemaRollout) {
		t.Fatalf("mixed-contract prepare = %v, want ErrSchemaRollout", err)
	}
	if current := authority.holder.Current(); current == nil ||
		current.Generation() != base.Generation() {
		t.Fatalf("failed prepare changed catalog generation to %v", current)
	}
	if _, err := authority.ReadOperation(context.Background(), operationID); err == nil {
		t.Fatal("failed mixed-contract prepare persisted an operation")
	}

	planned, err := authority.PrepareSchemaRollout(
		context.Background(), operationID, target, receipts,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Reconstruct the controller with only replicated authority and a stale
	// local catalog holder, as happens after a same-build rolling restart.
	restarted := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(base), 0xa7)
	complete, err := restarted.ActivateSchemaRollout(
		context.Background(), operationID, target,
	)
	if err != nil || complete.State != ReplicatedOperationComplete ||
		complete.Revision != planned.Revision+2 {
		t.Fatalf("restarted activation = %+v, %v", complete, err)
	}
	if installed := restarted.holder.Current(); installed == nil ||
		installed.Generation() != target.Generation() {
		t.Fatalf("restarted controller installed catalog %v", installed)
	}

	wrongTarget, _ := testSchemaRolloutTarget(t, target)
	if _, err = restarted.ActivateSchemaRollout(
		context.Background(), operationID, wrongTarget,
	); !errors.Is(err, ErrSchemaRolloutConflict) {
		t.Fatalf("different target generation after restart = %v, want conflict", err)
	}
}
