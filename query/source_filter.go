package query

import (
	"unsafe"

	"github.com/thesyncim/vibedb/store/durable"
)

// FileKeyFilter restricts physical visibility before SQL evaluation. The
// filter must remain immutable while an execution uses it, including workers.
type FileKeyFilter interface {
	Keep([]byte) bool
}

// FileFilterSource owns the reusable adapter for a filtered durable source.
// Filtering preserves persistent candidate indexes, but bypasses base-only
// covering/count shortcuts. Stats.RowsTotal reports the physical base count.
type FileFilterSource struct{ filter FileKeyFilter }

func NewFileFilterSource(filter FileKeyFilter) FileFilterSource {
	return FileFilterSource{filter: filter}
}

func FromFileFiltered(snapshot *durable.Snapshot, filter *FileFilterSource) Source {
	return Source{kind: sourceFileFiltered, file: snapshot, payload: unsafe.Pointer(filter)}
}

// Reuse the existing bounded candidate/batch executor's deletion mechanism.
// There are no staged rows, and cardinality metadata remains a physical bound;
// COUNT and other SQL aggregates are computed from the filtered batches.
func (filter *FileFilterSource) Lookup(key []byte) ([]byte, bool, bool) {
	return nil, false, !filter.filter.Keep(key)
}
func (*FileFilterSource) RangeInserts(func([]byte) error) error { return nil }
func (*FileFilterSource) RangePresent(func([]byte) error) error { return nil }
func (*FileFilterSource) LenDelta() int64                       { return 0 }
