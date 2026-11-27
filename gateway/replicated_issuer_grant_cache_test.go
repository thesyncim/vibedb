package gateway

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func makeTestIssuerGrant(t testing.TB, id uint64) ReplicatedIssuerLaneGrant {
	t.Helper()
	var inst replication.ID128
	binary.LittleEndian.PutUint64(inst[:8], id+1)
	var principal requestledger.PrincipalID
	binary.LittleEndian.PutUint64(principal[:8], id+101)
	sum := sha256.Sum256(inst[:])
	var tenant requestledger.Digest
	copy(tenant[:], sum[:])
	grant, err := replicatedIssuerGrantFor(ReplicatedIssuerOpen{
		Installation: inst,
		Epoch:        1,
		LaneOrdinal:  uint16(id % uint64(MaxReplicatedIssuerLanes)),
	}, requestledger.ScopeAuthenticated, principal, tenant)
	if err != nil {
		t.Fatalf("replicatedIssuerGrantFor: %v", err)
	}
	return grant
}

func TestReplicatedIssuerGrantCacheLockFreeConcurrentAndZeroAlloc(t *testing.T) {
	cache := newReplicatedIssuerGrantCache(256)
	grants := make([]ReplicatedIssuerLaneGrant, 64)
	refs := make([]ReplicatedIssuerReference, 64)
	for i := range grants {
		grants[i] = makeTestIssuerGrant(t, uint64(i))
		refs[i] = ReplicatedIssuerReference{
			Installation: grants[i].Installation,
			Epoch:        grants[i].Epoch,
			LaneOrdinal:  grants[i].LaneOrdinal,
			GrantDigest:  grants[i].GrantDigest,
		}
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range len(grants) {
				idx := (i + worker*5) % len(grants)
				cache.put(grants[idx])
				if got, ok := cache.get(refs[idx]); ok && got.GrantDigest != grants[idx].GrantDigest {
					t.Errorf("grant digest mismatch")
				}
			}
		}(g)
	}
	wg.Wait()

	allocs := testing.AllocsPerRun(100, func() {
		if _, ok := cache.get(refs[0]); !ok {
			t.Fatal("expected hit")
		}
		cache.put(grants[0])
	})
	if allocs != 0 {
		t.Fatalf("warm get+put allocs = %v, want 0", allocs)
	}
}

func BenchmarkReplicatedIssuerGrantCacheParallel(b *testing.B) {
	cache := newReplicatedIssuerGrantCache(MaxCachedReplicatedIssuerGrants)
	refs := make([]ReplicatedIssuerReference, 64)
	for i := range refs {
		g := makeTestIssuerGrant(b, uint64(i))
		cache.put(g)
		refs[i] = ReplicatedIssuerReference{
			Installation: g.Installation,
			Epoch:        g.Epoch,
			LaneOrdinal:  g.LaneOrdinal,
			GrantDigest:  g.GrantDigest,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := cache.get(refs[i&63]); !ok {
				b.Fatal("miss")
			}
			i++
		}
	})
}
