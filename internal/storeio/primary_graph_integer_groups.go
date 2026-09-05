package storeio

import "fmt"

// UnifiedIntegerGroupFilter is the strict storage-native shape for one integer
// GROUP BY path and, optionally, one integer SUM path. Both paths are resolved
// against compact stripe templates; no document or JSON segment is rebuilt.
// The filter is single-consumer and reusable across snapshots.
type UnifiedIntegerGroupFilter struct {
	group  UnifiedHoleResolver
	sum    UnifiedHoleResolver
	hasSum bool
}

// UnifiedIntegerGroupProgress reports rows visited by a successful strict
// grouped scan. A declined scan resets the value to zero so a caller cannot
// mistake partial progress for an authoritative result.
type UnifiedIntegerGroupProgress struct {
	Scanned int
}

// NewUnifiedIntegerGroupFilter constructs the compact integer grouping lane.
// sumPath may be nil to request COUNT(*) groups only.
func NewUnifiedIntegerGroupFilter(
	groupPath, sumPath []byte,
) (*UnifiedIntegerGroupFilter, error) {
	f := &UnifiedIntegerGroupFilter{}
	if err := f.group.SetPath(groupPath); err != nil {
		return nil, err
	}
	if len(sumPath) != 0 {
		if err := f.sum.SetPath(sumPath); err != nil {
			return nil, err
		}
		f.hasSum = true
	}
	return f, nil
}

// FilterIntegerGroups scans every leaf in physical order. A false return is
// an all-or-nothing decline: any unsupported target stream, absent/null value,
// noncompact storage, overflow row, or malformed geometry leaves no usable
// aggregate state. visit receives the snapshot-relative physical row ordinal,
// which lets the query layer preserve first-seen order for unordered results.
func (c *PrimaryGraphCursor) FilterIntegerGroups(
	f *UnifiedIntegerGroupFilter,
	progress *UnifiedIntegerGroupProgress,
	shapeSeen []int,
	shapeWork []IntegerGroupShapeWorkspace,
	visit func(row uint64, group, sum int64) error,
) (supported bool, err error) {
	if c == nil || f == nil || progress == nil || visit == nil {
		return false, nil
	}
	if c.done {
		return true, nil
	}
	for {
		base := progress.Scanned
		if base < 0 {
			*progress = UnifiedIntegerGroupProgress{}
			return false, fmt.Errorf("%w: integer group row count", ErrCommonPrimaryLeafCorrupt)
		}
		ok, scanErr := c.leaf.VisitResolvedIntegerGroups(
			&f.group, func() *UnifiedHoleResolver {
				if f.hasSum {
					return &f.sum
				}
				return nil
			}(), shapeSeen, shapeWork,
			func(row int, group, sum int64) error {
				if row < 0 || base > int(^uint(0)>>1)-row {
					return fmt.Errorf("%w: integer group row ordinal", ErrCommonPrimaryLeafCorrupt)
				}
				return visit(uint64(base+row), group, sum)
			},
		)
		if scanErr != nil {
			*progress = UnifiedIntegerGroupProgress{}
			c.Close()
			return false, scanErr
		}
		if !ok {
			*progress = UnifiedIntegerGroupProgress{}
			return false, nil
		}
		if c.leaf.Len() > int(^uint(0)>>1)-progress.Scanned {
			*progress = UnifiedIntegerGroupProgress{}
			return false, fmt.Errorf("%w: integer group scanned rows", ErrCommonPrimaryLeafCorrupt)
		}
		progress.Scanned += c.leaf.Len()
		if err := c.advanceLeaf(); err != nil {
			*progress = UnifiedIntegerGroupProgress{}
			c.Close()
			return false, err
		}
		if c.done {
			return true, nil
		}
	}
}
