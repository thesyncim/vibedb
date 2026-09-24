package storeio

import "testing"

// Catalog-content flags (tin declarations) may appear in a later generation
// but never disappear; every other option is fixed at creation.
func TestOptionsFollowSeparatesMonotonicFromFixedOptions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		older, newer  uint32
		wantFollowing bool
	}{
		{"unchanged", StateOptionSchema, StateOptionSchema, true},
		{"tin declared", StateOptionSchema, StateOptionSchema | StateOptionTinIndexes, true},
		{"tin retained", StateOptionTinIndexes, StateOptionTinIndexes, true},
		{"tin cleared", StateOptionTinIndexes, 0, false},
		{"fixed option added", 0, StateOptionSkipIndexes, false},
		{"fixed option removed", StateOptionOpaqueValues, 0, false},
	} {
		if got := optionsFollow(tc.older, tc.newer); got != tc.wantFollowing {
			t.Fatalf("%s: optionsFollow(%b,%b)=%t", tc.name, tc.older, tc.newer, got)
		}
	}
}
