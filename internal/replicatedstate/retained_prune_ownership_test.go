package replicatedstate

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func testRetainedPruneGlobalKey(t testing.TB, profile GlobalIndexProfile, inside bool) []byte {
	t.Helper()
	retained := distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}}}
	for i := 0; i < 4096; i++ {
		scalars := []distribution.Scalar{distribution.NewString(fmt.Sprintf("index-%d", i))}
		if !profile.Unique {
			for j := uint8(0); j < profile.LocatorCount; j++ {
				scalars = append(scalars, distribution.NewString(fmt.Sprintf("locator-%d", j)))
			}
		}
		key, err := distribution.CurrentTupleCodec.AppendTuple(nil, scalars)
		if err != nil {
			t.Fatal(err)
		}
		point, ok := profile.GlobalIndexStorageKeyPoint(key)
		if ok && retained.Contains(point) == inside {
			return key
		}
	}
	t.Fatal("no key in requested test range")
	return nil
}

func TestGlobalRetainedPruneUsesOwnSchemaBoundPlacement(t *testing.T) {
	retained := distribution.KeyRange{End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}}}
	for _, unique := range []bool{false, true} {
		profile := testGlobalIndexProfile(11, 1, 1, unique)
		inside := testRetainedPruneGlobalKey(t, profile, true)
		outside := testRetainedPruneGlobalKey(t, profile, false)
		if got := validateGlobalRetainedPrune(profile, inside, retained); got != MutationValidationWrongShard {
			t.Fatalf("unique=%v retained key validation=%v", unique, got)
		}
		if got := validateGlobalRetainedPrune(profile, outside, retained); got != MutationValidationAccept {
			t.Fatalf("unique=%v moved key validation=%v", unique, got)
		}
		for _, key := range [][]byte{nil, {0xff}, outside[:len(outside)-1], append(append([]byte(nil), outside...), 0)} {
			if got := validateGlobalRetainedPrune(profile, key, retained); got != MutationValidationInvalid {
				t.Fatalf("unique=%v malformed key %x validation=%v", unique, key, got)
			}
		}
		invalid := profile
		invalid.MapperVersion++
		if got := validateGlobalRetainedPrune(invalid, outside, retained); got != MutationValidationInvalid {
			t.Fatal("unsupported schema profile authorized deletion")
		}
		if n := testing.AllocsPerRun(100, func() {
			if validateGlobalRetainedPrune(profile, outside, retained) != MutationValidationAccept {
				panic("unstable placement")
			}
		}); n != 0 {
			t.Fatalf("placement validation allocations=%v", n)
		}
	}
}
