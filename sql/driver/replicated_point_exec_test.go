package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestReplicatedPointIntoZeroAllocationAndLiveIdentity(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	ctx := context.Background()
	options := replicatedReadReuseOptions()
	path := []byte(f.base.UserPrimaryKey)
	key := testReplicatedApplyKey(t, f.database, f.rows[0].doc)
	args := []any{f.rows[0].id}
	keys := [][]byte{key}
	params := []ParamType{ParamTypeText}
	var lease ReplicatedReadLease
	var cursor Cursor
	acquire := func(found bool) error {
		var raw []byte
		if found {
			raw = f.rows[0].doc
		}
		return f.claim.AcquireReplicatedPointReadInto(ctx, 1, key, found, raw, path,
			replicatedReadReusePointSQL, params, false, options, &lease)
	}
	run := func(found bool) {
		if err := acquire(found); err != nil {
			t.Fatal(err)
		}
		if err := lease.QueryCandidateKeysInto(ctx, args, path, keys, &cursor); err != nil {
			t.Fatal(err)
		}
		if cursor.Next() != found {
			t.Fatal("point presence differs")
		}
		if found && string(cursor.Cell(0).JSON()) != f.rows[0].values[0] {
			t.Fatal("point value differs")
		}
		if cursor.Next() {
			t.Fatal("extra point row")
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, found := range []bool{true, false, true} {
		run(found)
		if allocations := testing.AllocsPerRun(100, func() { run(found) }); allocations != 0 {
			t.Fatalf("found=%v: %.0f allocations, want zero", found, allocations)
		}
	}
	if err := acquire(true); err != nil {
		t.Fatal(err)
	}
	if lease.slot.reader.conn.tx != nil || lease.slot.reader.conn.pointRead == nil {
		t.Fatal("warm point rebuilt a transaction")
	}
	if err := acquire(true); !errors.Is(err, ErrReplicatedReadLeaseClosed) {
		t.Fatalf("active destination reused: %v", err)
	}
	stale := lease
	if err := stale.Finish(nil); !errors.Is(err, ErrReplicatedReadLeaseClosed) {
		t.Fatalf("copied handle released live owner: %v", err)
	}
	if err := lease.Finish(nil); err != nil {
		t.Fatal(err)
	}

	// Warm identity proofs must notice in-place mutations, not just a changed
	// catalog pointer. Exercise both independently validated identities.
	for _, mutate := range []func(){
		func() { f.claim.database.catalog.ReplicatedShardStore.RelationManifestDigest[0] ^= 1 },
		func() { f.claim.identity.ValidationDigest[0] ^= 1 },
	} {
		run(true)
		f.claim.database.mu.Lock()
		mutate()
		f.claim.database.mu.Unlock()
		err := acquire(true)
		f.claim.database.mu.Lock()
		mutate()
		f.claim.database.mu.Unlock()
		if !errors.Is(err, ErrReplicatedApplyMismatch) {
			t.Fatalf("warm proof admitted changed identity: %v", err)
		}
	}
	run(true)
	assertReplicatedReadReuseIdleState(t, f.claim)

	// Direct execution must retain the original candidate materialization
	// admission, even when the projection would otherwise fit the result cap.
	options.MemoryBytes = 64 << 10
	f.rows[0].doc = []byte(fmt.Sprintf(`{"id":%q,"score":1,"payload":%q}`, f.rows[0].id, strings.Repeat("x", 8192)))
	if err := acquire(true); err != nil {
		t.Fatal(err)
	}
	if err := lease.QueryCandidateKeysInto(ctx, args, path, keys, &cursor); !errors.Is(err, errPointMaterializationTooLarge) {
		t.Fatalf("direct point skipped materialization admission: %v", err)
	}
	_ = lease.Abort(errors.New("expected bounded refusal"))
}
