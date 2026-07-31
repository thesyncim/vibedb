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
// The alias keeps the public row address identical to the internal directory's
// pointer-free value without conversions.
type Location = storekey.Location
