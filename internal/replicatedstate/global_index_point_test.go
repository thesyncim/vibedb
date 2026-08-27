package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestGlobalIndexProfileStoredKeyMapper(t *testing.T) {
	profile := GlobalIndexProfile{IndexID: 7, Incarnation: 9, LocatorCount: 2,
		KeyEncoding: GlobalIndexKeyCanonicalTuple, KeyArity: 1,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, BucketBits: 17}
	index := []distribution.Scalar{distribution.NewString("email@example.test")}
	key, err := distribution.CurrentTupleCodec.AppendTuple(nil, index)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("tenant"), distribution.NewString("row")})
	if err != nil {
		t.Fatal(err)
	}
	stored := append(bytes.Clone(key), locator...)
	want, err := distribution.NewNativeMapperWithBucketBits(1, 17).PointFor(index)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := profile.GlobalIndexStorageKeyPoint(stored); !ok || got != want {
		t.Fatal("nonunique index point")
	}
	unique := profile
	unique.Unique = true
	if got, ok := unique.GlobalIndexStorageKeyPoint(key); !ok || got != want {
		t.Fatal("unique index point")
	}
	if _, ok := unique.GlobalIndexStorageKeyPoint(stored); ok {
		t.Fatal("unique accepted locator suffix")
	}
	for _, invalid := range [][]byte{nil, key, stored[:len(stored)-1], append(bytes.Clone(stored), 0)} {
		if _, ok := profile.GlobalIndexStorageKeyPoint(invalid); ok {
			t.Fatal("invalid stored key accepted")
		}
	}
	for name, change := range map[string]func(*GlobalIndexProfile){
		"index":         func(p *GlobalIndexProfile) { p.IndexID = 0 },
		"incarnation":   func(p *GlobalIndexProfile) { p.Incarnation = 0 },
		"no locator":    func(p *GlobalIndexProfile) { p.LocatorCount = 0 },
		"locator bound": func(p *GlobalIndexProfile) { p.LocatorCount = 9 },
		"encoding":      func(p *GlobalIndexProfile) { p.KeyEncoding++ },
		"no arity":      func(p *GlobalIndexProfile) { p.KeyArity = 0 },
		"arity bound":   func(p *GlobalIndexProfile) { p.KeyArity = distribution.KeyspaceWidth + 1 },
		"tuple":         func(p *GlobalIndexProfile) { p.TupleVersion++ },
		"mapper":        func(p *GlobalIndexProfile) { p.MapperVersion++ },
		"buckets":       func(p *GlobalIndexProfile) { p.BucketBits = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := profile
			change(&invalid)
			if invalid.Valid() {
				t.Fatal("unsupported profile accepted")
			}
			if _, ok := invalid.GlobalIndexStorageKeyPoint(stored); ok {
				t.Fatal("unsupported profile mapped")
			}
		})
	}
	if n := testing.AllocsPerRun(1000, func() { _, _ = profile.GlobalIndexStorageKeyPoint(stored) }); n != 0 {
		t.Fatalf("mapper allocations %v", n)
	}
}
