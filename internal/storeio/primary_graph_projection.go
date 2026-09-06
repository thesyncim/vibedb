package storeio

import (
	"errors"
	"fmt"
)

// ErrUnifiedProjectionFallbackUnsupported asks the cursor to decline the
// native range when a caller's bounded reconstruction scratch cannot hold a
// fallback row. It is kept separate from storage/callback errors so the query
// can retry the immutable range through its generic reader.
var ErrUnifiedProjectionFallbackUnsupported = errors.New(
	"storeio: bounded projection fallback unsupported",
)

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
	Scanned       int
	Matched       int
	NativeMatched int
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
	fieldCount := 0
	if f != nil {
		fieldCount = len(f.resolvers)
	}
	return c.VisitProjectedMatch(
		f, progress, fieldCount, shapeSeen, shapeWork, streamWork,
		fields, valueScratch, limit, nil, visit, nil,
	)
}

// VisitProjectedMatch walks one bounded range with a filter prefix. The prefix
// is decoded for every row and passed to match; the remaining fields are decoded
// only when match accepts the row. A row that cannot be answered by the compact
// scalar streams is reconstructed through the same cursor and handed to
// fallback, which lets the caller preserve progress without rescanning the
// prefix. fallback receives either inline JSON or an overflow reference (never
// both) and reports whether the row was accepted after its generic recheck.
func (c *PrimaryGraphCursor) VisitProjectedMatch(
	f *UnifiedProjectionFilter,
	progress *UnifiedProjectionProgress,
	filterCount int,
	shapeSeen []int,
	shapeWork []UnifiedProjectionShapeWorkspace,
	streamWork []UnifiedProjectionStreamWorkspace,
	fields []UnifiedProjectionField,
	valueScratch []byte,
	limit int,
	match func(row uint64, fields []UnifiedProjectionField) (bool, error),
	visit func(row uint64, fields []UnifiedProjectionField) error,
	fallback func(row uint64, raw []byte, overflow PageRef) (bool, error),
) (supported, stopped bool, scratch []byte, err error) {
	return c.VisitProjectedMatchWithReserve(
		f, progress, filterCount, shapeSeen, shapeWork, streamWork,
		fields, valueScratch, limit, match, visit, fallback, nil,
	)
}

// VisitProjectedMatchWithReserve is VisitProjectedMatch with one optional
// callback for lazily growing the caller-owned fallback reconstruction buffer.
// The ordinary entry point remains a thin wrapper for storage callers that do
// not need generic row reconstruction.
func (c *PrimaryGraphCursor) VisitProjectedMatchWithReserve(
	f *UnifiedProjectionFilter,
	progress *UnifiedProjectionProgress,
	filterCount int,
	shapeSeen []int,
	shapeWork []UnifiedProjectionShapeWorkspace,
	streamWork []UnifiedProjectionStreamWorkspace,
	fields []UnifiedProjectionField,
	valueScratch []byte,
	limit int,
	match func(row uint64, fields []UnifiedProjectionField) (bool, error),
	visit func(row uint64, fields []UnifiedProjectionField) error,
	fallback func(row uint64, raw []byte, overflow PageRef) (bool, error),
	reserveFallback func(required int) ([]byte, error),
) (supported, stopped bool, scratch []byte, err error) {
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
		len(f.resolvers) == 0 || len(c.prefix) != 0 || filterCount < 0 ||
		filterCount > len(f.resolvers) ||
		(match == nil && filterCount != 0 && filterCount != len(f.resolvers)) {
		return false, false, valueScratch, nil
	}
	if c.done {
		return true, false, valueScratch, nil
	}
	if limit == 0 {
		return true, true, valueScratch, nil
	}
	fieldCount := len(f.resolvers)
	if len(shapeSeen) == 0 || len(shapeWork) == 0 ||
		len(streamWork) < fieldCount || len(fields) < fieldCount {
		return false, false, valueScratch, nil
	}

	// A fallback row is still one evaluated row. Keep the cursor's physical row
	// ordinal moving before the callback so a callback that takes a slow overflow
	// path cannot leave the cursor positioned on the same row.
	fallbackRow := func(row int, ordinal uint64, ref PageRef) (bool, bool, error) {
		if fallback == nil {
			return false, false, nil
		}
		if ref != (PageRef{}) {
			accepted, fallbackErr := fallback(ordinal, nil, ref)
			if fallbackErr == ErrUnifiedProjectionFallbackUnsupported {
				return false, false, nil
			}
			return accepted, true, fallbackErr
		}
		required, ok := c.leaf.valueLength(row)
		if !ok {
			return false, false, nil
		}
		if required > cap(c.spliceScratch) {
			if reserveFallback == nil {
				return false, false, nil
			}
			buffer, reserveErr := reserveFallback(required)
			if reserveErr != nil {
				return false, false, reserveErr
			}
			if cap(buffer) < required {
				return false, false, ErrUnifiedProjectionFallbackUnsupported
			}
			c.spliceScratch = buffer[:0]
		}
		dst := c.spliceScratch[:0:cap(c.spliceScratch)]
		dst, ok = c.leaf.AppendValue(dst, row)
		if !ok || len(dst) != required || cap(dst) != cap(c.spliceScratch) {
			return false, false, nil
		}
		c.spliceScratch = dst
		accepted, fallbackErr := fallback(ordinal, c.spliceScratch, PageRef{})
		if fallbackErr == ErrUnifiedProjectionFallbackUnsupported {
			return false, false, nil
		}
		return accepted, true, fallbackErr
	}

	for {
		leafLimit := c.leaf.Len()
		if len(c.upper) != 0 {
			leafLimit, err = c.upperRankFromCurrent(c.upper)
			if err != nil {
				c.Close()
				return false, false, valueScratch, err
			}
		}
		if leafLimit < c.row || leafLimit > c.leaf.Len() {
			c.Close()
			return false, false, valueScratch, ErrCommonPrimaryLeafCorrupt
		}
		shapeCount := c.leaf.ShapeCount()
		if shapeCount > len(shapeSeen) || shapeCount > len(shapeWork) ||
			(shapeCount > 0 && fieldCount > len(streamWork)/shapeCount) {
			// The caller deliberately admits only a bounded shape slab. Consume
			// this leaf through the same cursor so rows already delivered from
			// earlier leaves are not visited again by the generic retry.
			for c.row < leafLimit {
				row := c.row
				ordinal := uint64(progress.Scanned)
				ref, overflow := c.leaf.OverflowRef(row)
				var accepted, handled bool
				if overflow {
					accepted, handled, err = fallbackRow(row, ordinal, ref)
				} else {
					accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
				}
				if err != nil {
					return false, false, valueScratch, err
				}
				if !handled {
					c.Close()
					return false, false, valueScratch, nil
				}
				c.row++
				progress.Scanned++
				if accepted {
					progress.Matched++
					if limit > 0 && progress.Matched >= limit {
						return true, true, valueScratch, nil
					}
				}
			}
			if leafLimit < c.leaf.Len() {
				c.Close()
				return true, false, valueScratch, nil
			}
			clearViews()
			if err := c.advanceLeaf(); err != nil {
				c.Close()
				return false, false, valueScratch, err
			}
			if c.done {
				return true, false, valueScratch, nil
			}
			continue
		}
		clear(shapeSeen[:shapeCount])
		clear(shapeWork[:shapeCount])
		clear(streamWork[:shapeCount*fieldCount])

		for c.row < leafLimit {
			row := c.row
			ordinal := uint64(progress.Scanned)
			var accepted bool
			var handled bool
			shape := -1
			if ref, overflow := c.leaf.OverflowRef(row); overflow {
				accepted, handled, err = fallbackRow(row, ordinal, ref)
				if !handled && err == nil {
					c.Close()
					return false, false, valueScratch, nil
				}
			} else {
				shape = c.leaf.rowShape(row)
				if shape < 0 || shape >= shapeCount {
					// A malformed shape cannot be reconstructed as an inline row;
					// let the complete caller-owned fallback decide the range.
					accepted, handled, err = false, false, nil
					if fallback != nil {
						accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
					}
					if !handled && err == nil {
						c.Close()
						return false, false, valueScratch, nil
					}
				} else {
					meta := &shapeWork[shape]
					base := shape * fieldCount
					if !meta.prepared {
						meta.prepared = true
						if !prepareUnifiedProjectionShape(
							&c.leaf, shape, f.resolvers, meta,
							streamWork[base:base+fieldCount], valueScratch,
						) {
							meta.unsupported = true
						}
					}
					if meta.unsupported {
						accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
						if !handled && err == nil {
							c.Close()
							return false, false, valueScratch, nil
						}
					} else {
						rowOrdinal := c.leaf.shapeOrdinal(row, shape)
						if rowOrdinal < 0 || rowOrdinal >= meta.rows {
							accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
							if !handled && err == nil {
								c.Close()
								return false, false, valueScratch, nil
							}
						} else {
							valueScratch = valueScratch[:0]
							for field := 0; field < filterCount; field++ {
								stream := &streamWork[base+field]
								if stream.hole == UnifiedHoleAbsent {
									fields[field] = UnifiedProjectionField{Kind: UnifiedProjectionFieldMissing}
									continue
								}
								var fieldOK bool
								valueScratch, fieldOK = compactProjectionFieldAt(
									&stream.view, stream.view.shapeCoordinate(row, rowOrdinal), valueScratch,
									&fields[field], &stream.state,
								)
								if !fieldOK {
									accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
									if !handled && err == nil {
										c.Close()
										return false, false, valueScratch, nil
									}
									break
								}
							}
							if !handled && err == nil {
								if match == nil {
									accepted = true
								} else {
									accepted, err = match(ordinal, fields[:filterCount])
								}
								if err == nil && accepted {
									for field := filterCount; field < fieldCount; field++ {
										stream := &streamWork[base+field]
										if stream.hole == UnifiedHoleAbsent {
											fields[field] = UnifiedProjectionField{Kind: UnifiedProjectionFieldMissing}
											continue
										}
										var fieldOK bool
										valueScratch, fieldOK = compactProjectionFieldAt(
											&stream.view, stream.view.shapeCoordinate(row, rowOrdinal), valueScratch,
											&fields[field], &stream.state,
										)
										if !fieldOK {
											accepted, handled, err = fallbackRow(row, ordinal, PageRef{})
											if !handled && err == nil {
												c.Close()
												return false, false, valueScratch, nil
											}
											break
										}
									}
								}
								if err == nil && !handled && accepted {
									err = visit(ordinal, fields[:fieldCount])
								}
							}
						}
					}
				}
			}
			if err != nil {
				return false, false, valueScratch, err
			}
			if shape >= 0 && shape < shapeCount {
				shapeSeen[shape]++
			}
			c.row++
			progress.Scanned++
			if accepted {
				progress.Matched++
				if !handled {
					progress.NativeMatched++
				}
				if limit > 0 && progress.Matched >= limit {
					return true, true, valueScratch, nil
				}
			}
		}
		for shape := 0; shape < shapeCount; shape++ {
			if shapeSeen[shape] > shapeWork[shape].rows {
				c.Close()
				return false, false, valueScratch, nil
			}
		}
		if leafLimit < c.leaf.Len() {
			c.Close()
			return true, false, valueScratch, nil
		}
		clearViews()
		if err := c.advanceLeaf(); err != nil {
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
				&stream.view, stream.view.shapeCoordinate(row, ordinal), valueScratch, &fields[field], &stream.state,
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
