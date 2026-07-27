package store

import (
	"math/bits"

	"github.com/thesyncim/vibejson"
)

// Snapshot posting probes expose the online index without exposing mutable
// Segment internals. Every probe remains exact while an index is building or
// being reclaimed: indexed chunks use postings and uncovered chunks use the
// existing scan fallback. Results are ordered by stable chunk/slot position.

type storeProbeKind uint8

const (
	storeProbeExists storeProbeKind = iota + 1
	storeProbeContains
)

// WhereExistsKeys returns the keys whose root object contains path. path is a
// decoded top-level member name, matching [Segment.WhereExists].
func (s Snapshot) WhereExistsKeys(path string) []string {
	return s.AppendWhereExistsKeys(nil, path)
}

// AppendWhereExistsKeys is [Snapshot.WhereExistsKeys] with caller-owned
// result storage. It appends to dst and, given sufficient capacity, performs
// no heap allocation on either the posting or scan path.
func (s Snapshot) AppendWhereExistsKeys(dst []string, path string) []string {
	return s.appendWhereKeys(dst, storeProbeExists, path, vibejson.Index{})
}

// WhereContainsKeys returns the keys whose top-level value at path contains
// needle according to [Node.Contains]. Invalid JSON returns an error.
func (s Snapshot) WhereContainsKeys(path string, needle []byte) ([]string, error) {
	return s.AppendWhereContainsKeys(nil, path, needle)
}

// AppendWhereContainsKeys is [Snapshot.WhereContainsKeys] with caller-owned
// result storage. It leaves dst unchanged when needle is invalid. Repeated hot
// paths should build the needle once and call AppendWhereContainsIndexKeys.
func (s Snapshot) AppendWhereContainsKeys(dst []string, path string, needle []byte) ([]string, error) {
	index, err := vibejson.ContainsIndex(needle)
	if err != nil {
		return dst, err
	}
	return s.AppendWhereContainsIndexKeys(dst, path, index), nil
}

// WhereContainsIndexKeys returns the keys whose top-level value at path
// contains needle according to [Node.Contains]. A prebuilt needle avoids parse
// and allocation work across repeated calls.
func (s Snapshot) WhereContainsIndexKeys(path string, needle vibejson.Index) []string {
	return s.AppendWhereContainsIndexKeys(nil, path, needle)
}

// AppendWhereContainsIndexKeys is [Snapshot.WhereContainsIndexKeys] with
// caller-owned result storage. With sufficient dst capacity it allocates
// nothing. Build needle once with caller-owned IndexEntry storage when the
// complete operation must be allocation-free.
func (s Snapshot) AppendWhereContainsIndexKeys(dst []string, path string, needle vibejson.Index) []string {
	return s.appendWhereKeys(dst, storeProbeContains, path, needle)
}

func (s Snapshot) appendWhereKeys(dst []string, kind storeProbeKind, path string, needle vibejson.Index) []string {
	if s.state == nil {
		return dst
	}
	s.state.Chunks.Each(func(_ uint32, chunk *Chunk) bool {
		var storage [MaxChunkDocuments]int
		var rows []int
		switch kind {
		case storeProbeExists:
			rows = chunk.Docs.AppendWhereExists(storage[:0], path)
		case storeProbeContains:
			rows = chunk.Docs.AppendWhereContainsIndex(storage[:0], path, needle)
		}
		if len(rows) == 0 {
			return true
		}
		// A rebuilt Segment is dense while collection slots are stable and may be
		// sparse. Invert the at-most-64 ordinal map on the stack.
		var slots [MaxChunkDocuments]uint8
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			slots[chunk.Ord[slot]] = uint8(slot)
		}
		for _, row := range rows {
			dst = append(dst, chunk.Key(int(slots[row])))
		}
		return true
	})
	return dst
}

// AppendWhereExistsKeys probes the current snapshot. See the Snapshot method.
func (c *Collection) AppendWhereExistsKeys(dst []string, path string) []string {
	snap10, _ := c.Snapshot()
	return snap10.AppendWhereExistsKeys(dst, path)
}

// WhereExistsKeys probes the current snapshot. See the Snapshot method.
func (c *Collection) WhereExistsKeys(path string) []string {
	snap9, _ := c.Snapshot()
	return snap9.WhereExistsKeys(path)
}

// AppendWhereContainsKeys probes the current snapshot. See the Snapshot
// method.
func (c *Collection) AppendWhereContainsKeys(dst []string, path string, needle []byte) ([]string, error) {
	snap8, _ := c.Snapshot()
	return snap8.AppendWhereContainsKeys(dst, path, needle)
}

// WhereContainsKeys probes the current snapshot. See the Snapshot method.
func (c *Collection) WhereContainsKeys(path string, needle []byte) ([]string, error) {
	snap7, _ := c.Snapshot()
	return snap7.WhereContainsKeys(path, needle)
}

// AppendWhereContainsIndexKeys probes the current snapshot. See the Snapshot
// method.
func (c *Collection) AppendWhereContainsIndexKeys(dst []string, path string, needle vibejson.Index) []string {
	snap6, _ := c.Snapshot()
	return snap6.AppendWhereContainsIndexKeys(dst, path, needle)
}

// WhereContainsIndexKeys probes the current snapshot. See the Snapshot method.
func (c *Collection) WhereContainsIndexKeys(path string, needle vibejson.Index) []string {
	snap5, _ := c.Snapshot()
	return snap5.WhereContainsIndexKeys(path, needle)
}
