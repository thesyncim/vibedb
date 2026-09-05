package storeio

import "fmt"

// UnifiedProjectionFilter is reusable state for a set of direct scalar paths.
// Resolvers are kept separate from per-leaf stream views so the filter can be
// reused across a warmed execution without retaining page-backed bytes.
type UnifiedProjectionFilter struct {
	resolvers []UnifiedHoleResolver
}

// NewUnifiedProjectionFilter builds a direct scalar projection filter. Paths
// are RFC 6901 pointers; each resolver owns a copy of its path.
func NewUnifiedProjectionFilter(paths [][]byte) (*UnifiedProjectionFilter, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("%w: empty unified projection", ErrInvalidWrite)
	}
	f := &UnifiedProjectionFilter{
		resolvers: make([]UnifiedHoleResolver, len(paths)),
	}
	for i, path := range paths {
		if err := f.resolvers[i].SetPath(path); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// UnifiedProjectionProgress reports rows delivered by a successful projected
// scan. A declined or failed scan resets it to zero.
type UnifiedProjectionProgress struct {
	Scanned int
}

// VisitProjected walks the cursor's bounded primary range in lexical order,
// decoding only the selected scalar fields. Values borrow the supplied value
// scratch until visit returns. A positive limit stops before later rows; a
// negative limit scans the complete range.
func (c *PrimaryGraphCursor) VisitProjected(
	f *UnifiedProjectionFilter,
	progress *UnifiedProjectionProgress,
	shapeSeen []int,
	shapeWork []UnifiedProjectionShapeWorkspace,
	streamWork []UnifiedProjectionStreamWorkspace,
	fields []UnifiedProjectionField,
	valueScratch []byte,
	limit int,
	visit func(row uint64, fields []UnifiedProjectionField) error,
) (supported, stopped bool, scratch []byte, err error) {
	// Page-backed streams must disappear before any cursor lease is released,
	// including error paths and movement to the next leaf.
	clearViews := func() {
		clear(streamWork)
		clear(fields)
	}
	defer func() {
		clearViews()
		if progress != nil && (!supported || err != nil) {
			*progress = UnifiedProjectionProgress{}
		}
	}()
	if c == nil || f == nil || progress == nil || visit == nil ||
		len(f.resolvers) == 0 || len(c.prefix) != 0 {
		return false, false, valueScratch, nil
	}
	if c.done {
		return true, false, valueScratch, nil
	}
	if limit == 0 {
		return true, true, valueScratch, nil
	}
	if len(shapeSeen) == 0 || len(shapeWork) == 0 ||
		len(streamWork) < len(f.resolvers) ||
		len(fields) < len(f.resolvers) {
		return false, false, valueScratch, nil
	}
	remaining := limit
	for {
		leafLimit := c.leaf.Len()
		if len(c.upper) != 0 {
			leafLimit, err = c.upperRankFromCurrent(c.upper)
			if err != nil {
				*progress = UnifiedProjectionProgress{}
				clearViews()
				c.Close()
				return false, false, valueScratch, err
			}
		}
		if leafLimit < c.row || leafLimit > c.leaf.Len() {
			*progress = UnifiedProjectionProgress{}
			clearViews()
			c.Close()
			return false, false, valueScratch, ErrCommonPrimaryLeafCorrupt
		}
		end := leafLimit
		stopsAtLimit := false
		if remaining > 0 && end-c.row > remaining {
			end = c.row + remaining
			stopsAtLimit = true
		}
		before := progress.Scanned
		if before < 0 || before > int(^uint(0)>>1)-(end-c.row) {
			clearViews()
			c.Close()
			return false, false, valueScratch, ErrCommonPrimaryLeafCorrupt
		}
		ok, localStopped, valueScratch, scanErr := c.leaf.visitResolvedProjectionRange(
			c.row, end, stopsAtLimit, f.resolvers, shapeSeen, shapeWork,
			streamWork, fields, valueScratch,
			func(row int, values []UnifiedProjectionField) error {
				if row < c.row || before < 0 || before > int(^uint(0)>>1)-(row-c.row) {
					return fmt.Errorf("%w: projected row ordinal", ErrCommonPrimaryLeafCorrupt)
				}
				return visit(uint64(before+row-c.row), values)
			},
		)
		if scanErr != nil {
			*progress = UnifiedProjectionProgress{}
			clearViews()
			c.Close()
			return false, false, valueScratch, scanErr
		}
		if !ok {
			*progress = UnifiedProjectionProgress{}
			clearViews()
			c.Close()
			return false, false, valueScratch, nil
		}
		delivered := end - c.row
		progress.Scanned += delivered
		c.row = end
		if remaining > 0 {
			remaining -= delivered
		}
		if localStopped || remaining == 0 && limit > 0 {
			return true, true, valueScratch, nil
		}
		if end < leafLimit {
			return true, false, valueScratch, nil
		}
		clearViews()
		if leafLimit < c.leaf.Len() {
			// The upper fence is inside this leaf. No later leaf belongs to
			// the range, even if its selected fields would be unsupported.
			c.Close()
			return true, false, valueScratch, nil
		}
		if err := c.advanceLeaf(); err != nil {
			*progress = UnifiedProjectionProgress{}
			c.Close()
			return false, false, valueScratch, err
		}
		if c.done {
			return true, false, valueScratch, nil
		}
	}
}

// visitResolvedProjectionRange is the leaf-local part of VisitProjected. It
// deliberately uses shapeOrdinal rather than the physical key or a rendered
// document, so a range beginning in the middle of a leaf remains aligned with
// each shape's compact stream.
func (v *CompactPrimaryStripeView) visitResolvedProjectionRange(
	start, end int,
	stopAtLimit bool,
	resolvers []UnifiedHoleResolver,
	shapeSeen []int,
	shapeWork []UnifiedProjectionShapeWorkspace,
	streamWork []UnifiedProjectionStreamWorkspace,
	fields []UnifiedProjectionField,
	valueScratch []byte,
	visit func(row int, fields []UnifiedProjectionField) error,
) (supported, stopped bool, scratch []byte, err error) {
	if v == nil || start < 0 || end < start || end > v.rows {
		return false, false, valueScratch, nil
	}
	if start == end {
		return true, stopAtLimit && end < v.rows, valueScratch, nil
	}
	if len(resolvers) == 0 || len(fields) < len(resolvers) ||
		len(shapeSeen) < v.shapeCount || len(shapeWork) < v.shapeCount ||
		len(streamWork) < v.shapeCount*len(resolvers) || visit == nil {
		return false, false, valueScratch, nil
	}
	clear(shapeSeen[:v.shapeCount])
	// Shapes are prepared on first encounter below. A leaf can carry many
	// shapes whose rows fall after the range limit; parsing and binding all of
	// them here would spend the same work the projection lane is meant to
	// avoid. Stream views are page-backed and are cleared by VisitProjected's
	// leaf transition/defer, so each leaf starts with fresh shape metadata.
	clear(shapeWork[:v.shapeCount])
	clear(streamWork[:v.shapeCount*len(resolvers)])
	for row := start; row < end; row++ {
		if v.IsOverflow(row) {
			return false, false, valueScratch, nil
		}
		shape := v.rowShape(row)
		if shape < 0 || shape >= v.shapeCount {
			return false, false, valueScratch, nil
		}
		if !shapeWork[shape].prepared {
			base := shape * len(resolvers)
			shapeWork[shape].prepared = true
			if !prepareUnifiedProjectionShape(
				v, shape, resolvers, &shapeWork[shape],
				streamWork[base:base+len(resolvers)], valueScratch,
			) {
				shapeWork[shape].unsupported = true
				return false, false, valueScratch, nil
			}
		}
		if shapeWork[shape].unsupported {
			return false, false, valueScratch, nil
		}
		ordinal := v.shapeOrdinal(row, shape)
		if ordinal < 0 || ordinal >= shapeWork[shape].rows {
			return false, false, valueScratch, nil
		}
		base := shape * len(resolvers)
		valueScratch = valueScratch[:0]
		for field := range resolvers {
			stream := &streamWork[base+field]
			if stream.hole == UnifiedHoleAbsent {
				fields[field] = UnifiedProjectionField{
					Kind: UnifiedProjectionFieldMissing,
				}
				continue
			}
			var ok bool
			valueScratch, ok = compactProjectionFieldAt(
				&stream.view, ordinal, valueScratch, &fields[field], &stream.state,
			)
			if !ok {
				// Native and dictionary fields do not touch valueScratch. The
				// remaining codecs are bounded by compactProjectionFieldAt and
				// decline before appendValue can invalidate an earlier view.
				return false, false, valueScratch, nil
			}
		}
		if err := visit(row, fields[:len(resolvers)]); err != nil {
			return false, false, valueScratch, err
		}
		shapeSeen[shape]++
	}
	for shape := 0; shape < v.shapeCount; shape++ {
		if shapeSeen[shape] > shapeWork[shape].rows {
			return false, false, valueScratch, nil
		}
	}
	return true, stopAtLimit && end < v.rows, valueScratch, nil
}
