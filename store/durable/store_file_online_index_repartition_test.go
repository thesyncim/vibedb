package durable

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

type indexRepartitionGate struct {
	context.Context
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (c *indexRepartitionGate) Err() error {
	c.calls++
	if c.calls == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestOnlineIndexRepartitionReleasesWriterBetweenSplits(t *testing.T) {
	for _, action := range []string{"write", "cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			built, keys, values := buildRedundantPrimaryCorpus(t, 2048)
			options := Options{Backend: BackendPortable, ResidentBytes: 64 << 20}
			file := createPrimaryPointFile(t, built, options, "wide.vjc")
			collection, err := Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			before, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer before.Close()
			initialRoutes := collection.primaryRouter.Load().Len()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gate := &indexRepartitionGate{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(gate.release) }) }
			done := make(chan error, 1)
			go func() {
				collection.writer.Lock()
				err := collection.repartitionPrimaryForExactIndexLocked(gate)
				collection.writer.Unlock()
				done <- err
				close(done)
			}()
			defer func() { unblock(); <-done }()
			select {
			case <-gate.entered:
			case err := <-done:
				t.Fatalf("fixture did not exercise multiple repartition slices: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("repartition did not reach the second slice")
			}
			if routes := collection.primaryRouter.Load().Len(); routes <= initialRoutes {
				t.Fatal("gate did not follow a real structural split")
			}
			foreground := make(chan error, 1)
			go func() {
				var err error
				switch action {
				case "write":
					_, err = collection.Put([]byte(keys[0]), []byte(`{"id":0,"group":9999}`))
				case "cancel":
					cancel()
					// Even a canceled build must not retain the writer while its
					// coordinator is paused at a cancellation checkpoint.
					collection.writer.Lock()
					collection.writer.Unlock()
				case "close":
					err = before.Close()
					if err == nil {
						err = collection.Close()
					}
				}
				foreground <- err
				close(foreground)
			}()
			defer func() { unblock(); <-foreground }()
			select {
			case err := <-foreground:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("repartition retained writer across slices")
			}
			unblock()
			err = <-done
			switch action {
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel=%v", err)
				}
				return
			case "close":
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("close=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			router := collection.primaryRouter.Load()
			for rank := 0; rank < router.Len(); rank++ {
				route, _ := router.RouteAtRank(rank)
				lease, err := router.AcquireLeaf(collection.cache, route)
				if err != nil {
					t.Fatal(err)
				}
				stripe, ok := storeio.AdmittedCompactPrimaryStripe(lease.Page(), collection.storeID, route.Bucket)
				valid := ok && stripe.Len() <= storeio.CommonPrimaryLeafWideSlots
				lease.Release()
				if !valid {
					t.Fatal("repartition skipped a wide leaf after concurrent publication")
				}
			}
			old, found, err := before.AppendRaw(nil, []byte(keys[0]))
			if err != nil || !found || !bytes.Equal(old, values[0]) {
				t.Fatalf("old reader changed: found=%t err=%v", found, err)
			}
			if _, err := collection.CreateIndex(store.IndexDefinition{Name: "by_group", Paths: []string{"/group"}}); err != nil {
				t.Fatal(err)
			}
			got := primaryExactTestKeys(t, collection, "by_group", primaryExactTestNeedle(t, "9999"))
			if len(got) != 1 || got[0] != keys[0] {
				t.Fatalf("concurrent write absent from built index: %v", got)
			}
		})
	}
}
