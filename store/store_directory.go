package store

import "github.com/thesyncim/vibedb/internal/storekey"

// Location is the exact pointer-free row address — chunk plus stable slot —
// used everywhere a document is named without its key: by the key directory,
// by the index and scan results a Snapshot returns.
// Addresses returned by an index are ordered by chunk then stable slot and
// remain valid only with the Snapshot that produced them. The fields are
// exported so query workspaces can combine candidate masks without converting
// them to keys.
//
// An alias, rather than a mirror plus conversions, keeps lookup and mutation
// calls eligible for the same inlining and register ABI as before the
// directory moved out of this package.
type Location = storekey.Location

type storeKeyNode = storekey.Node

func storeKeyLookup(root *storeKeyNode, hash uint64, key string) (Location, bool) {
	return storekey.Lookup(root, hash, key)
}

func storeKeyInsert(root *storeKeyNode, hash uint64, key string, loc Location) *storeKeyNode {
	return storekey.Insert(root, hash, key, loc)
}

func storeKeyDelete(root *storeKeyNode, hash uint64, key string) *storeKeyNode {
	return storekey.Delete(root, hash, key)
}
