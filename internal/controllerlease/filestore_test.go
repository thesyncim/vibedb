package controllerlease

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestFileStoreDurabilityCASMonotonicityAndCorruptionFailClosed(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	ctx := t.Context()
	key := "vibedb/controller/split"

	// 1. Initial read on empty dir returns not-found.
	if _, ok, err := store.Read(ctx, key); err != nil || ok {
		t.Fatalf("initial Read = (%v, %v), want (false, nil)", ok, err)
	}

	// 2. CAS create with expected == nil succeeds.
	rec1 := LeaseRecord{
		Holder:            "node-1",
		Epoch:             1,
		ExpiresAtUnixNano: time.Now().Add(10 * time.Second).UnixNano(),
	}
	swapped, got, err := store.CompareAndSwap(ctx, key, nil, rec1)
	if err != nil || !swapped || got != rec1 {
		t.Fatalf("CAS create = (%v, %+v, %v)", swapped, got, err)
	}

	// 3. Reopen FileStore on the same dir: record survives restart.
	reopened, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen FileStore: %v", err)
	}
	loaded, ok, err := reopened.Read(ctx, key)
	if err != nil || !ok || loaded != rec1 {
		t.Fatalf("reopened Read = (%+v, %v, %v), want %+v", loaded, ok, err, rec1)
	}

	// 4. Stale expected == nil fails cleanly returning current record.
	swapped, got, err = reopened.CompareAndSwap(ctx, key, nil, rec1)
	if err != nil || swapped || got != rec1 {
		t.Fatalf("duplicate CAS create = (%v, %+v, %v), want (false, rec1, nil)", swapped, got, err)
	}

	// 5. Epoch downgrade rejected with ErrStaleEpoch.
	rec2 := LeaseRecord{Holder: "node-2", Epoch: 2, ExpiresAtUnixNano: rec1.ExpiresAtUnixNano + 1000}
	if swapped, _, err := reopened.CompareAndSwap(ctx, key, &rec1, rec2); err != nil || !swapped {
		t.Fatalf("CAS advance to epoch 2 = (%v, %v)", swapped, err)
	}
	downgrade := LeaseRecord{Holder: "node-1", Epoch: 1, ExpiresAtUnixNano: rec2.ExpiresAtUnixNano + 1000}
	if _, _, err := reopened.CompareAndSwap(ctx, key, &rec2, downgrade); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("epoch downgrade err = %v, want ErrStaleEpoch", err)
	}

	// 6. Corrupting the lease file fails closed with ErrInvalidRecord (never resets epoch).
	dataPath, _ := reopened.keyPaths(key)
	raw, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(dataPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopened.Read(ctx, key); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("corrupted Read err = %v, want ErrInvalidRecord", err)
	}
}

func TestElectorFailoverOverFileStore(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	clock := newFakeClock(time.Date(2026, 11, 27, 19, 0, 0, 0, time.UTC))
	e1, err := NewElector(s1, Config{
		Key:      "vibedb/controller/split",
		Holder:   "proc-1",
		LeaseTTL: 10 * time.Second,
		Clock:    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := NewElector(s2, Config{
		Key:      "vibedb/controller/split",
		Holder:   "proc-2",
		LeaseTTL: 10 * time.Second,
		Clock:    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	if ok, epoch, err := e1.Step(ctx); err != nil || !ok || epoch != 1 {
		t.Fatalf("e1 acquire = (%v, %d, %v)", ok, epoch, err)
	}
	if err := e1.Release(ctx); err != nil {
		t.Fatalf("e1 release: %v", err)
	}
	if ok, epoch, err := e2.Step(ctx); err != nil || !ok || epoch != 2 {
		t.Fatalf("e2 handoff over FileStore = (%v, %d, %v), want (true, 2, nil)", ok, epoch, err)
	}
}
