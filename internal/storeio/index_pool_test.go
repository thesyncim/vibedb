package storeio

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestIndexPoolConcurrentReuse(t *testing.T) {
	const (
		indexes = 64
		workers = 8
		loops   = 10_000
	)
	pool := newIndexPool(indexes)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for range loops {
				var index uint32
				for {
					var ok bool
					index, ok = pool.pop()
					if ok {
						break
					}
					runtime.Gosched()
				}
				pool.push(index)
			}
		}()
	}
	group.Wait()

	seen := make([]bool, indexes)
	for range indexes {
		index, ok := pool.pop()
		if !ok {
			t.Fatal("pool lost an index")
		}
		if index >= indexes || seen[index] {
			t.Fatalf("invalid or duplicate index %d", index)
		}
		seen[index] = true
	}
	if _, ok := pool.pop(); ok {
		t.Fatal("pool returned more indexes than initialized")
	}
}

func TestIndexPoolMaximumDeviceIndex(t *testing.T) {
	const count = 1 << 16
	pool := newIndexPool(count)
	for want := range uint32(count) {
		got, ok := pool.pop()
		if !ok || got != want {
			t.Fatalf("pop = (%d, %v), want (%d, true)", got, ok, want)
		}
	}
	if _, ok := pool.pop(); ok {
		t.Fatal("maximum-sized pool returned an extra index")
	}
}

func TestIndexPoolBulkTransferIsExact(t *testing.T) {
	pool := newIndexPool(8)
	indexes := make([]uint32, 6)
	if !pool.popN(indexes) {
		t.Fatal("bulk pop rejected an available exact reservation")
	}
	if got := pool.availableCount(); got != 2 {
		t.Fatalf("available after bulk pop = %d, want 2", got)
	}
	unchanged := []uint32{91, 92, 93}
	if pool.popN(unchanged) {
		t.Fatal("oversized bulk pop succeeded")
	}
	if got := unchanged; got[0] != 91 || got[1] != 92 || got[2] != 93 {
		t.Fatalf("failed bulk pop changed destination: %v", got)
	}
	pool.pushN(indexes)
	if got := pool.availableCount(); got != 8 {
		t.Fatalf("available after bulk return = %d, want 8", got)
	}
	seen := make([]bool, 8)
	for range seen {
		index, ok := pool.pop()
		if !ok || index >= uint32(len(seen)) || seen[index] {
			t.Fatalf("invalid index after bulk round trip: (%d,%v)", index, ok)
		}
		seen[index] = true
	}
}

func TestIndexPoolBulkReleaseWakesEveryExactWaiter(t *testing.T) {
	pool := newIndexPool(2)
	held := make([]uint32, 2)
	if !pool.popN(held) {
		t.Fatal("drain pool")
	}
	large := pool.prepareWait()
	small := pool.prepareWait()
	pool.pushN(held[:1])
	for name, notify := range map[string]<-chan struct{}{
		"large": large,
		"small": small,
	} {
		select {
		case <-notify:
		case <-time.After(time.Second):
			t.Fatalf("%s exact waiter remained parked after release", name)
		}
		pool.finishWait()
	}
}
