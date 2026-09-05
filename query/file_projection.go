package query

import (
	"bytes"
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const (
	// The native lane is intentionally a small prefix. A leaf with more
	// shapes, or a selected scalar whose spelling exceeds this scratch, safely
	// declines to the generic reader and does not inflate a cold RF3 read.
	fileProjectionShapeBudget = 16
	fileProjectionValueBytes  = 4 << 10
)

// resizeFileProjection clears every element that will be outside the next
// logical view. Projection fields and cells can borrow page or text storage,
// so retaining a stale tail across a plan change would pin data the storage
// callback has already released.
func resizeFileProjection[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	full := s[:cap(s)]
	clear(full[n:])
	return full[:n]
}

// fileProjectionPredicateEligible admits only predicates whose leaves consume
// classified scalar cells. Raw containment and join/mark leaves require the
// complete document or an execution binding, so admitting them here would
// silently change either their error or NULL semantics.
func fileProjectionPredicateEligible(p *compiledPredicate) bool {
	if p == nil {
		return true
	}
	switch p.kind {
	case predCmp, predCmpBound, predCmpPath,
		predCorrelationKnown, predIn, predLike, predExists,
		predIsNull, predIsString:
		return true
	case predAnd, predOr, predNot:
		for _, kid := range p.kids {
			if !fileProjectionPredicateEligible(kid) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// fileProjectionBase proves the deliberately narrow query shape for the
// storage-native primary projector. A bound predicate path certifies that the
// range already contains the complete WHERE result; an unbound range uses the
// existing compiled predicate over the decoded filter prefix.
func (p *plan) fileProjectionBase(
	span *FileRangeSource,
	ordered bool,
) (covered bool, ok bool) {
	if p == nil || span == nil || !ordered || span.orderedPath == "" ||
		!p.hasLimit || p.limit <= 0 ||
		p.grouped || p.hasAggregate || len(p.joins) != 0 ||
		len(p.marks) != 0 || p.requiresSQLDomainScan() ||
		len(p.order) != 1 || p.order[0].dir != Asc ||
		p.order[0].value < 0 || p.order[0].value >= len(p.valuePaths) ||
		p.valuePaths[p.order[0].value].indexPath() != span.orderedPath {
		return false, false
	}
	covered = span.predicatePath != ""
	if covered && span.predicatePath != span.orderedPath {
		return false, false
	}
	if !covered && !fileProjectionPredicateEligible(p.where) {
		return false, false
	}
	for _, column := range p.columns {
		// Aggregates, semantic-only columns, COUNT(*), and synthetic
		// cardinality columns have no direct scalar field to decode.
		if column.agg != aggNone || column.value < 0 ||
			column.value >= len(p.valuePaths) {
			return false, false
		}
		path := p.valuePaths[column.value]
		if path.join != joinPathOuter {
			return false, false
		}
		canonical := path.indexPath()
		// The storage projection lane is scalar-only. The empty pointer and
		// root pointer can name a whole document/container, so let the generic
		// reader preserve its exact semantics for those paths.
		if canonical == "" || canonical == "/" {
			return false, false
		}
	}
	for _, ordinal := range p.filterCols {
		if ordinal < 0 || ordinal >= len(p.valuePaths) ||
			p.valuePaths[ordinal].join != joinPathOuter {
			return false, false
		}
		canonical := p.valuePaths[ordinal].indexPath()
		if canonical == "" || canonical == "/" {
			return false, false
		}
	}
	return covered, len(p.columns) != 0
}

// fileProjectionFieldCount counts the deduplicated storage slots: residual
// filter columns form the prefix, followed by output columns not already in
// that prefix. Covered ranges omit filter-only columns entirely.
func (p *plan) fileProjectionFieldCount(
	span *FileRangeSource,
	ordered bool,
) (int, bool) {
	covered, ok := p.fileProjectionBase(span, ordered)
	if !ok {
		return 0, false
	}
	count := 0
	if !covered {
		count = len(p.filterCols)
	}
	for columnIndex, column := range p.columns {
		seen := false
		if !covered {
			for _, ordinal := range p.filterCols {
				if ordinal == column.value {
					seen = true
					break
				}
			}
		}
		if !seen {
			for previous := 0; previous < columnIndex; previous++ {
				if p.columns[previous].value == column.value {
					seen = true
					break
				}
			}
		}
		if !seen {
			count++
		}
	}
	return count, count > 0
}

// fileProjectionSpec fills the storage path order and maps result columns to
// those slots. It performs the same proof as fileProjectionFieldCount without
// allocating a temporary registry.
func (p *plan) fileProjectionSpec(
	paths []string,
	ordinals []int,
	outputs []int,
	span *FileRangeSource,
	ordered bool,
) (filterCount int, ok bool) {
	covered, ok := p.fileProjectionBase(span, ordered)
	if !ok {
		return 0, false
	}
	fieldCount, ok := p.fileProjectionFieldCount(span, ordered)
	if !ok || cap(paths) < fieldCount || cap(ordinals) < fieldCount ||
		cap(outputs) < len(p.columns) {
		return 0, false
	}
	paths = paths[:fieldCount]
	ordinals = ordinals[:fieldCount]
	outputs = outputs[:len(p.columns)]
	at := 0
	if !covered {
		for _, ordinal := range p.filterCols {
			ordinals[at] = ordinal
			paths[at] = p.valuePaths[ordinal].indexPath()
			at++
		}
		filterCount = at
	}
	for column, planColumn := range p.columns {
		duplicate := -1
		for previous := 0; previous < column; previous++ {
			if p.columns[previous].value == planColumn.value {
				duplicate = previous
				break
			}
		}
		if duplicate >= 0 {
			// A repeated output path shares the first output column's owned
			// Cell. Keep the storage field unique while preserving duplicate
			// result columns in their original order.
			outputs[column] = -duplicate - 1
			continue
		}
		field := -1
		for candidate := 0; candidate < at; candidate++ {
			if ordinals[candidate] == planColumn.value {
				field = candidate
				break
			}
		}
		if field < 0 {
			field = at
			ordinals[at] = planColumn.value
			paths[at] = p.valuePaths[planColumn.value].indexPath()
			at++
		}
		outputs[column] = field
	}
	return filterCount, at == fieldCount
}

// fileProjectionMetadataBytes charges the actual caller-owned projection
// capacities. The storage wrapper never allocates replacement shape, stream,
// or field slices, so these bytes have to be admitted before any of them grow.
func fileProjectionMetadataBytes(
	shapeSeenCap, shapeWorkCap, streamCap, fieldCap, cellCap, pathCap,
	ordinalCap, outputMapCap, scalarCap, slotCap int,
) int64 {
	if shapeSeenCap < 0 || shapeWorkCap < 0 || streamCap < 0 ||
		fieldCap < 0 || cellCap < 0 || pathCap < 0 || ordinalCap < 0 ||
		outputMapCap < 0 || scalarCap < 0 || slotCap < 0 {
		return math.MaxInt64
	}
	bytes := saturatedProduct(
		int64(shapeSeenCap), int64(unsafe.Sizeof(int(0))),
	)
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(shapeWorkCap), int64(unsafe.Sizeof(storeio.UnifiedProjectionShapeWorkspace{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(streamCap), int64(unsafe.Sizeof(storeio.UnifiedProjectionStreamWorkspace{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(fieldCap), int64(unsafe.Sizeof(storeio.UnifiedProjectionField{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(cellCap), int64(unsafe.Sizeof(Cell{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(pathCap), int64(unsafe.Sizeof("")),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(ordinalCap), int64(unsafe.Sizeof(int(0))),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(outputMapCap), int64(unsafe.Sizeof(int(0))),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(scalarCap), int64(unsafe.Sizeof(scalar{})),
	))
	return saturatedBytes(bytes, saturatedProduct(
		int64(slotCap), int64(unsafe.Sizeof([]scalar(nil))),
	))
}

func fileProjectionFilterPathBytes(path string) int64 {
	// NewProjectionFilter copies the encoded path once for SetPath, then the
	// resolver retains decoded path bytes and a segment table. Account the
	// temporary encoded copy as well as conservative append capacities for the
	// retained resolver slices before the filter is constructed.
	pathCap := max(8, len(path)*2)
	segments := 1
	for i := 1; i < len(path); i++ {
		if path[i] == '/' {
			segments++
		}
	}
	segmentCap := max(1, segments*2)
	bytes := saturatedProduct(int64(len(path)), 2)
	bytes = saturatedBytes(bytes, int64(pathCap))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(segmentCap), int64(unsafe.Sizeof([2]int32{})),
	))
	return bytes
}

func fileProjectionFilterBytesForRange(
	p *plan,
	span *FileRangeSource,
	ordered bool,
) int64 {
	fieldCount, ok := p.fileProjectionFieldCount(span, ordered)
	if !ok {
		return math.MaxInt64
	}
	bytes := int64(unsafe.Sizeof(durable.ProjectionFilter{}))
	bytes = saturatedBytes(bytes, int64(unsafe.Sizeof(storeio.UnifiedProjectionFilter{})))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(fieldCount), int64(unsafe.Sizeof(storeio.UnifiedHoleResolver{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(fieldCount), int64(unsafe.Sizeof("")),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		int64(fieldCount), int64(unsafe.Sizeof([]byte(nil))),
	))
	covered, _ := p.fileProjectionBase(span, ordered)
	add := func(ordinal int) {
		if ordinal < 0 || ordinal >= len(p.valuePaths) {
			bytes = math.MaxInt64
			return
		}
		bytes = saturatedBytes(bytes, fileProjectionFilterPathBytes(
			p.valuePaths[ordinal].indexPath(),
		))
	}
	if !covered {
		for _, ordinal := range p.filterCols {
			add(ordinal)
		}
	}
	for source, column := range p.columns {
		seen := false
		if !covered {
			for _, previous := range p.filterCols {
				if previous == column.value {
					seen = true
					break
				}
			}
		}
		if !seen {
			for previous := 0; previous < source; previous++ {
				if p.columns[previous].value == column.value {
					seen = true
					break
				}
			}
		}
		if !seen {
			add(column.value)
		}
	}
	return bytes
}

func (s *fileSmallScan) projectionSpecMatches(
	p *plan,
	span *FileRangeSource,
	ordered bool,
	fieldCount, filterCount int,
) bool {
	if s.projection == nil || s.projectionPlan != p ||
		len(s.projectionPaths) != fieldCount ||
		len(s.projectionOrdinals) != fieldCount ||
		len(s.projectionOutput) != len(p.columns) ||
		s.projectionFilterCount != filterCount {
		return false
	}
	covered, ok := p.fileProjectionBase(span, ordered)
	if !ok {
		return false
	}
	at := 0
	check := func(ordinal int) bool {
		if at >= fieldCount || ordinal < 0 || ordinal >= len(p.valuePaths) ||
			s.projectionOrdinals[at] != ordinal ||
			s.projectionPaths[at] != p.valuePaths[ordinal].indexPath() {
			return false
		}
		at++
		return true
	}
	if !covered {
		for _, ordinal := range p.filterCols {
			if !check(ordinal) {
				return false
			}
		}
	}
	for column, planColumn := range p.columns {
		duplicate := -1
		for previous := 0; previous < column; previous++ {
			if p.columns[previous].value == planColumn.value {
				duplicate = previous
				break
			}
		}
		if duplicate >= 0 {
			if s.projectionOutput[column] != -duplicate-1 {
				return false
			}
			continue
		}
		field := -1
		for candidate := 0; candidate < at; candidate++ {
			if s.projectionOrdinals[candidate] == planColumn.value {
				field = candidate
				break
			}
		}
		if field < 0 {
			field = at
			if !check(planColumn.value) {
				return false
			}
		}
		if s.projectionOutput[column] != field {
			return false
		}
	}
	return at == fieldCount
}

// ensureFileProjectionWorkspace chooses the largest compact-shape prefix that
// fits the current work budget. A storage implementation may decline a leaf
// with more shapes than this prefix; the query then retries the same snapshot
// range through the generic reader. This keeps a small MemoryBytes target from
// paying for a 256-shape by N-field slab that it cannot use.
func (s *fileSmallScan) ensureFileProjectionWorkspace(
	fieldCount int,
	scalarCount int,
	outputCount int,
	filterBytes int64,
	opts normalizedFileOptions,
) bool {
	if fieldCount <= 0 || scalarCount < 0 || outputCount <= 0 {
		return false
	}
	work := s.work.activeHeapWorkBudget()
	remaining := work.remaining()
	if remaining <= 0 {
		return false
	}
	maxInt := int64(^uint(0) >> 1)
	if int64(fieldCount) > maxInt/int64(unsafe.Sizeof(storeio.UnifiedProjectionStreamWorkspace{})) ||
		int64(scalarCount) > maxInt/int64(unsafe.Sizeof(scalar{})) {
		return false
	}
	perShape := int64(unsafe.Sizeof(int(0))) +
		int64(unsafe.Sizeof(storeio.UnifiedProjectionShapeWorkspace{})) +
		int64(fieldCount)*int64(unsafe.Sizeof(storeio.UnifiedProjectionStreamWorkspace{}))
	if perShape <= 0 {
		return false
	}

	scratchCap := opts.batchBytes
	if scratchCap < 1 {
		scratchCap = 1
	}
	if scratchCap > remaining {
		scratchCap = remaining
	}
	fieldBytes := saturatedProduct(
		int64(fieldCount), int64(unsafe.Sizeof(storeio.UnifiedProjectionField{})),
	)
	if fieldBytes >= remaining {
		return false
	}
	shapeCount := min(storeio.UnifiedProjectionMaxShapes, fileProjectionShapeBudget)
	if scratchCap > fileProjectionValueBytes {
		scratchCap = fileProjectionValueBytes
	}
	for {
		available := remaining - fieldBytes
		if scratchCap >= available {
			if available <= 1 {
				return false
			}
			scratchCap = max(int64(1), available/2)
			continue
		}
		available -= scratchCap
		if available >= perShape {
			byBudget := available / perShape
			if byBudget < int64(shapeCount) {
				shapeCount = int(byBudget)
			}
			break
		}
		if scratchCap <= 1 {
			return false
		}
		scratchCap /= 2
	}
	if shapeCount <= 0 {
		return false
	}
	if int64(shapeCount) > maxInt/int64(fieldCount) {
		return false
	}
	streamCount := int64(shapeCount) * int64(fieldCount)
	shapeTarget := shapeCount
	streamTarget := int(streamCount)
	if int64(streamTarget) > maxInt || int64(outputCount) > maxInt {
		return false
	}
	shapeCap := max(cap(s.projectionShapes), shapeTarget)
	shapeWorkCap := max(cap(s.projectionShape), shapeTarget)
	streamCap := max(cap(s.projectionStream), streamTarget)
	fieldCap := max(cap(s.projectionFields), fieldCount)
	cellCap := max(cap(s.cells), outputCount)
	pathCap := max(cap(s.projectionPaths), fieldCount)
	ordinalCap := max(cap(s.projectionOrdinals), fieldCount)
	outputMapCap := max(cap(s.projectionOutput), outputCount)
	scalarCap := max(cap(s.projectionScalars), scalarCount)
	slotCap := max(cap(s.projectionSlots), scalarCount)
	valueCap := max(cap(s.projectionValues), int(scratchCap))
	metadata := fileProjectionMetadataBytes(
		shapeCap, shapeWorkCap, streamCap, fieldCap, cellCap, pathCap,
		ordinalCap, outputMapCap, scalarCap, slotCap,
	)
	total := saturatedBytes(metadata, int64(valueCap))
	total = saturatedBytes(total, filterBytes)
	if err := work.admit("durable projected-range workspace", total); err != nil {
		return false
	}
	if cap(s.projectionShapes) < shapeTarget {
		s.projectionShapes = make([]int, shapeTarget)
	} else {
		s.projectionShapes = resizeFileProjection(s.projectionShapes, shapeTarget)
	}
	if cap(s.projectionPaths) < fieldCount {
		s.projectionPaths = make([]string, fieldCount)
	} else {
		s.projectionPaths = resizeFileProjection(s.projectionPaths, fieldCount)
	}
	if cap(s.projectionShape) < shapeTarget {
		s.projectionShape = make([]storeio.UnifiedProjectionShapeWorkspace, shapeTarget)
	} else {
		s.projectionShape = resizeFileProjection(s.projectionShape, shapeTarget)
	}
	if cap(s.projectionStream) < streamTarget {
		s.projectionStream = make([]storeio.UnifiedProjectionStreamWorkspace, streamTarget)
	} else {
		s.projectionStream = resizeFileProjection(s.projectionStream, streamTarget)
	}
	if cap(s.projectionFields) < fieldCount {
		s.projectionFields = make([]storeio.UnifiedProjectionField, fieldCount)
	} else {
		s.projectionFields = resizeFileProjection(s.projectionFields, fieldCount)
	}
	if cap(s.cells) < outputCount {
		s.cells = make([]Cell, outputCount)
	} else {
		s.cells = resizeFileProjection(s.cells, outputCount)
	}
	if cap(s.projectionOrdinals) < fieldCount {
		s.projectionOrdinals = make([]int, fieldCount)
	} else {
		s.projectionOrdinals = resizeFileProjection(s.projectionOrdinals, fieldCount)
	}
	if cap(s.projectionOutput) < outputCount {
		s.projectionOutput = make([]int, outputCount)
	} else {
		s.projectionOutput = resizeFileProjection(s.projectionOutput, outputCount)
	}
	if cap(s.projectionScalars) < scalarCount {
		s.projectionScalars = make([]scalar, scalarCount)
	} else {
		s.projectionScalars = resizeFileProjection(s.projectionScalars, scalarCount)
	}
	if cap(s.projectionSlots) < scalarCount {
		s.projectionSlots = make([][]scalar, scalarCount)
	} else {
		s.projectionSlots = resizeFileProjection(s.projectionSlots, scalarCount)
	}
	if cap(s.projectionValues) < int(scratchCap) {
		s.projectionValues = make([]byte, 0, int(scratchCap))
	} else {
		s.projectionValues = s.projectionValues[:0]
	}
	return true
}

// projectedTextBytes reports the decoded-string workspace needed for one
// callback. It runs before classification, so a borrowed storage field can
// never grow the query's decoded text arena without first passing admission.
func projectedTextBytes(
	fields []storeio.UnifiedProjectionField,
	cancel *CancelFlag,
) (int, error) {
	need := 0
	for i, field := range fields {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return 0, err
		}
		raw := field.JSON
		if len(raw) > 0 && raw[0] == '"' && bytes.IndexByte(raw, '\\') >= 0 {
			if need > int(^uint(0)>>1)-len(raw) {
				return 0, &WorkBudgetError{
					Resource: "durable projected decoded-text workspace",
					Bytes:    math.MaxInt64,
					Limit:    math.MaxInt64,
				}
			}
			need += len(raw)
		}
	}
	return need, nil
}

func projectedFieldScalar(field storeio.UnifiedProjectionField, text *[]byte) scalar {
	switch field.Kind {
	case storeio.UnifiedProjectionFieldInteger:
		return scalar{kind: kindNumber, isInt: true, ival: field.Integer}
	case storeio.UnifiedProjectionFieldMissing:
		return scalar{kind: kindNull}
	default:
		return classifyRawInto(vibejson.RawValue{Src: field.JSON}, text)
	}
}

// classifyProjectedFields fills the one-row scalar slots for a compact filter
// range. Its text arena remains live while an accepted row is materialized so
// output-only fields can use the separate late arena.
func (s *fileSmallScan) classifyProjectedFields(
	fields []storeio.UnifiedProjectionField,
	start, end int,
	text *[]byte,
	reserved *int64,
) error {
	if start < 0 || end < start || end > len(fields) || end > len(s.projectionOrdinals) {
		return &WorkBudgetError{
			Resource: "durable projected field range",
			Bytes:    math.MaxInt64,
			Limit:    math.MaxInt64,
		}
	}
	need, err := projectedTextBytes(fields[start:end], s.work.cancel)
	if err != nil {
		return err
	}
	if reserved == nil {
		return &WorkBudgetError{
			Resource: "durable projected text reservation",
			Bytes:    math.MaxInt64,
			Limit:    math.MaxInt64,
		}
	}
	targetCap := cap(*text)
	if targetCap < need {
		targetCap = growCap(targetCap, need)
	}
	if err := s.work.activeHeapWorkBudget().admitHighWater(
		"durable projected decoded-text workspace",
		int64(targetCap), reserved,
	); err != nil {
		return err
	}
	*text = (*text)[:0]
	if cap(*text) < need {
		*text = make([]byte, 0, targetCap)
	}
	for field := start; field < end; field++ {
		ordinal := s.projectionOrdinals[field]
		if ordinal < 0 || ordinal >= len(s.projectionScalars) || ordinal >= len(s.projectionSlots) {
			return &WorkBudgetError{
				Resource: "durable projected scalar slots",
				Bytes:    math.MaxInt64,
				Limit:    math.MaxInt64,
			}
		}
		s.projectionScalars[ordinal] = projectedFieldScalar(fields[field], text)
		s.projectionSlots[ordinal] = s.projectionScalars[ordinal : ordinal+1]
	}
	return nil
}

// prepareProjectedLateText admits the escaped-string arena for the output
// fields of one accepted row. Native integers, dictionary values, and clean
// JSON spellings do not need this arena, so an output-only covered projection
// can stay on the scalar hot path without allocating or staging slots.
func (s *fileSmallScan) prepareProjectedLateText(
	fields []storeio.UnifiedProjectionField,
	start, end int,
) error {
	if start < 0 || end < start || end > len(fields) {
		return &WorkBudgetError{
			Resource: "durable projected late field range",
			Bytes:    math.MaxInt64,
			Limit:    math.MaxInt64,
		}
	}
	if start == end {
		s.work.lateText = s.work.lateText[:0]
		return nil
	}
	need, err := projectedTextBytes(fields[start:end], s.work.cancel)
	if err != nil {
		return err
	}
	s.work.lateText = s.work.lateText[:0]
	targetCap := cap(s.work.lateText)
	if targetCap < need {
		targetCap = growCap(targetCap, need)
	}
	if err := s.work.activeHeapWorkBudget().admitHighWater(
		"durable projected decoded-text workspace",
		int64(targetCap), &s.projectionLateTextReserved,
	); err != nil {
		return err
	}
	if cap(s.work.lateText) < need {
		s.work.lateText = make([]byte, 0, targetCap)
	}
	return nil
}

func (s *fileSmallScan) fillProjectedFallback(raw []byte) error {
	return s.fillProjectedFallbackRange(raw, 0, len(s.projectionOrdinals))
}

func (s *fileSmallScan) fillProjectedFallbackRange(raw []byte, start, end int) error {
	if start < 0 || end < start || end > len(s.projectionOrdinals) ||
		end > len(s.projectionFields) {
		return &WorkBudgetError{
			Resource: "durable projected fallback field range",
			Bytes:    math.MaxInt64,
			Limit:    math.MaxInt64,
		}
	}
	root := vibejson.RawValue{Src: raw}
	var indexed vibejson.Index
	var err error
	if end-start > 1 {
		// A fallback row can carry several selected fields. Build one reusable
		// tape for that row, then resolve every pointer against it; repeatedly
		// parsing the complete document for each field makes wide fallback rows
		// pay the validation and indexing cost once per column.
		entries := int64(cap(s.work.eval.entries))
		if err := s.work.activeHeapWorkBudget().admitContainmentTape(
			entries, &s.work.eval.entriesReserved,
		); err != nil {
			return err
		}
		indexed, err = s.work.eval.containsTapeFrom(raw, 16)
		if err != nil {
			return err
		}
	}
	for field := start; field < end; field++ {
		ordinal := s.projectionOrdinals[field]
		if ordinal < 0 || ordinal >= len(s.p.valuePaths) {
			return &WorkBudgetError{
				Resource: "durable projected path slots",
				Bytes:    math.MaxInt64,
				Limit:    math.MaxInt64,
			}
		}
		var value vibejson.RawValue
		var found bool
		if end-start > 1 {
			node, ok, pointerErr := indexed.PointerCompiled(
				s.p.valuePaths[ordinal].pointer,
			)
			if pointerErr != nil {
				return pointerErr
			}
			if ok {
				value = node.Raw()
				found = true
			}
		} else {
			value, found, err = root.PointerCompiled(s.p.valuePaths[ordinal].pointer)
			if err != nil {
				return err
			}
		}
		if !found {
			s.projectionFields[field] = storeio.UnifiedProjectionField{
				Kind: storeio.UnifiedProjectionFieldMissing,
			}
			continue
		}
		s.projectionFields[field] = storeio.UnifiedProjectionField{
			JSON: value.Bytes(),
			Kind: storeio.UnifiedProjectionFieldBorrowedJSON,
		}
	}
	return nil
}

func (s *fileSmallScan) projectedResultFromFields(
	result *Result,
	payload *int64,
	fields []storeio.UnifiedProjectionField,
	filterCount int,
) error {
	fieldCount := len(s.projectionOrdinals)
	if filterCount < 0 || filterCount > fieldCount || len(fields) < fieldCount ||
		len(s.projectionOutput) != len(s.p.columns) {
		return &WorkBudgetError{
			Resource: "durable projected output fields",
			Bytes:    math.MaxInt64,
			Limit:    math.MaxInt64,
		}
	}
	if err := s.prepareProjectedLateText(fields, filterCount, fieldCount); err != nil {
		return err
	}
	rowPayload := int64(0)
	rowCells := s.cells[:len(s.p.columns)]
	for column := range s.p.columns {
		field := s.projectionOutput[column]
		if field < 0 {
			// Duplicate output paths point at the first result column for that
			// path. The storage field and its owned payload are shared, while
			// the result still receives a cell in the duplicate column.
			source := -field - 1
			if source < 0 || source >= column {
				return &WorkBudgetError{
					Resource: "durable projected output slots",
					Bytes:    math.MaxInt64,
					Limit:    math.MaxInt64,
				}
			}
			cell := rowCells[source]
			add := projectedCellPayloadBytes(cell)
			if add < 0 || rowPayload > math.MaxInt64-add {
				return result.resultByteBudgetError(result.RowCount+1, math.MaxInt64)
			}
			rowPayload += add
			rowCells[column] = cell
			continue
		}
		if field >= fieldCount {
			return &WorkBudgetError{
				Resource: "durable projected output slots",
				Bytes:    math.MaxInt64,
				Limit:    math.MaxInt64,
			}
		}
		var cell Cell
		if field < filterCount {
			ordinal := s.projectionOrdinals[field]
			if ordinal < 0 || ordinal >= len(s.projectionScalars) {
				return &WorkBudgetError{
					Resource: "durable projected output scalar",
					Bytes:    math.MaxInt64,
					Limit:    math.MaxInt64,
				}
			}
			cell = cellFromScalar(s.projectionScalars[ordinal])
		} else {
			value := fields[field]
			switch value.Kind {
			case storeio.UnifiedProjectionFieldInteger:
				cell = Cell{
					kind: TypeNumber, flag: cellInteger,
					word: uint64(value.Integer),
				}
			case storeio.UnifiedProjectionFieldMissing:
				cell = Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
			default:
				cell = cellFromScalar(classifyRawInto(
					vibejson.RawValue{Src: value.JSON}, &s.work.lateText,
				))
			}
		}
		add := projectedCellPayloadBytes(cell)
		if add < 0 || rowPayload > math.MaxInt64-add {
			return result.resultByteBudgetError(result.RowCount+1, math.MaxInt64)
		}
		rowPayload += add
		rowCells[column] = cell
	}
	nextPayload := saturatedBytes(*payload, rowPayload)
	required, err := result.checkResultBudget(
		len(s.p.columns), result.RowCount+1, nextPayload,
	)
	if err != nil {
		return err
	}
	result.resultBytesUsed = required
	for column, cell := range rowCells {
		if field := s.projectionOutput[column]; field < 0 {
			source := -field - 1
			if source < 0 || source >= column {
				return &WorkBudgetError{
					Resource: "durable projected output slots",
					Bytes:    math.MaxInt64,
					Limit:    math.MaxInt64,
				}
			}
			cell = rowCells[source]
		} else {
			cell = result.ownProjectedCell(cell)
			rowCells[column] = cell
		}
		result.Columns[column].Cells = append(
			result.Columns[column].Cells, cell,
		)
	}
	*payload = nextPayload
	result.RowCount++
	return nil
}

// tryFileProjected attempts the complete native projected primary range. A
// storage Unsupported result is the only ordinary fallback case: provisional
// cells, result bytes, work budget, and stats are discarded before the raw
// reader retries the same immutable snapshot and bounds. Callback failures,
// cancellation, and result-budget failures are semantic execution errors and
// therefore remain handled by this lane.
func (s *fileSmallScan) tryFileProjected(
	snapshot *durable.Snapshot,
	span *FileRangeSource,
	opts normalizedFileOptions,
	stats *ExecStats,
) (handled bool, err error) {
	work := s.work.activeHeapWorkBudget()
	mark := work.checkpoint()
	initialStats := *stats
	initialPayload := s.payload
	defer func() {
		if handled {
			return
		}
		work.rollback(mark)
		// Native projection admission is provisional until the storage cursor
		// proves the complete range supported. A generic retry must begin with
		// fresh per-execution high-water markers, even though the warm buffers
		// themselves remain retained for the next attempt.
		s.work.heapWorkTextReserved = 0
		s.work.eval.entriesReserved = 0
		s.projectionTextReserved = 0
		s.projectionLateTextReserved = 0
		s.projectionFallbackReserved = 0
	}()
	fieldCount, ok := s.p.fileProjectionFieldCount(span, s.ordered)
	if !ok {
		return false, nil
	}
	covered, _ := s.p.fileProjectionBase(span, s.ordered)
	filterCount := 0
	scalarCount := 0
	if !covered {
		filterCount = len(s.p.filterCols)
		for _, ordinal := range s.p.filterCols {
			if ordinal < 0 || ordinal == int(^uint(0)>>1) {
				return false, nil
			}
			if ordinal+1 > scalarCount {
				scalarCount = ordinal + 1
			}
		}
	}
	// Prepared compilers can refill the same plan object. Validate its complete
	// resolver/mapping configuration before reusing a storage filter.
	replaceFilter := !s.projectionSpecMatches(
		s.p, span, s.ordered, fieldCount, filterCount,
	)
	filterBytes := s.projectionFilter
	if replaceFilter {
		filterBytes = fileProjectionFilterBytesForRange(s.p, span, s.ordered)
	}
	// Every retained arena is live even when this execution does not happen to
	// use the corresponding path. Admit their current capacities before sizing
	// the shape slab so the remaining budget describes all native state that can
	// coexist. The per-arena reservations make subsequent row growth charge only
	// the high-water increase.
	retained := int64(cap(s.work.text))
	retained = saturatedBytes(retained, int64(cap(s.work.lateText)))
	retained = saturatedBytes(retained, int64(cap(s.projectionFallback)))
	retained = saturatedBytes(retained, saturatedProduct(
		int64(cap(s.work.eval.entries)),
		int64(unsafe.Sizeof(vibejson.IndexEntry{})),
	))
	if retained > work.remaining() {
		return false, nil
	}
	if err := work.admitHighWater(
		"durable projected decoded-text workspace",
		int64(cap(s.work.text)), &s.projectionTextReserved,
	); err != nil {
		return false, nil
	}
	if err := work.admitHighWater(
		"durable projected decoded-text workspace",
		int64(cap(s.work.lateText)), &s.projectionLateTextReserved,
	); err != nil {
		return false, nil
	}
	if err := work.admitHighWater(
		"durable projected fallback workspace",
		int64(cap(s.projectionFallback)), &s.projectionFallbackReserved,
	); err != nil {
		return false, nil
	}
	if err := work.admitContainmentTape(
		int64(cap(s.work.eval.entries)), &s.work.eval.entriesReserved,
	); err != nil {
		return false, nil
	}
	if !s.ensureFileProjectionWorkspace(
		fieldCount, scalarCount, len(s.p.columns), filterBytes, opts,
	) {
		return false, nil
	}
	filterCount, ok = s.p.fileProjectionSpec(
		s.projectionPaths[:0], s.projectionOrdinals[:0],
		s.projectionOutput[:0], span, s.ordered,
	)
	if !ok {
		return false, nil
	}
	paths := s.projectionPaths[:fieldCount]
	// The filter constructor is covered by the admission above. Publish its
	// byte estimate together with the filter, so a declined replacement cannot
	// corrupt the charge for the previous plan on a later execution.
	if replaceFilter {
		filter, err := durable.NewProjectionFilter(paths)
		if err != nil {
			// The path scratch already contains the candidate configuration.
			// Invalidate the old filter rather than pair it with those paths.
			s.projection = nil
			s.projectionPlan = nil
			s.projectionFilter = 0
			return false, nil
		}
		s.projection = filter
		s.projectionPlan = s.p
		s.projectionFilter = filterBytes
	}
	s.projectionFilterCount = filterCount
	var match func(uint64, []storeio.UnifiedProjectionField) (bool, error)
	lastFilterRow := uint64(^uint64(0))
	lastFilterAccepted := false
	if !covered {
		match = func(row uint64, fields []storeio.UnifiedProjectionField) (bool, error) {
			if err := s.classifyProjectedFields(
				fields, 0, filterCount, &s.work.text, &s.projectionTextReserved,
			); err != nil {
				return false, err
			}
			lastFilterRow = row
			lastFilterAccepted = true
			if s.p.where == nil {
				return true, nil
			}
			matched := s.p.where.eval(s.projectionSlots, 0, &s.work.eval)
			if err := s.work.eval.firstError(); err != nil {
				return false, err
			}
			lastFilterRow = row
			lastFilterAccepted = matched
			return matched, nil
		}
	}
	visit := func(_ uint64, fields []storeio.UnifiedProjectionField) error {
		return s.projectedResultFromFields(
			&s.e.Result, &s.payload, fields, filterCount,
		)
	}
	fallback := func(row uint64, raw []byte) (bool, error) {
		preserveFilter := !covered && lastFilterRow == row && lastFilterAccepted
		if preserveFilter {
			if err := s.fillProjectedFallbackRange(raw, filterCount, fieldCount); err != nil {
				return false, err
			}
		} else if err := s.fillProjectedFallback(raw); err != nil {
			return false, err
		}
		accepted := preserveFilter || covered
		if !covered && !preserveFilter {
			matched, matchErr := match(row, s.projectionFields[:filterCount])
			if matchErr != nil || !matched {
				return false, matchErr
			}
			accepted = true
		}
		if err := s.projectedResultFromFields(
			&s.e.Result, &s.payload, s.projectionFields, filterCount,
		); err != nil {
			return false, err
		}
		return accepted, nil
	}
	projected, scratch, err := snapshot.FilterProjectedRangeWithMatchScratch(
		s.projection,
		filterCount, span.lower, span.upper, span.lowerExclusive,
		s.projectionShapes, s.projectionShape, s.projectionStream,
		s.projectionFields, s.projectionValues,
		&s.projectionFallback,
		func(required int64) ([]byte, error) {
			if required < 0 || required > int64(^uint(0)>>1) {
				return nil, &WorkBudgetError{
					Resource: "durable projected fallback workspace",
					Bytes:    math.MaxInt64,
					Limit:    work.limit,
				}
			}
			target := required
			if current := int64(cap(s.projectionFallback)); current > target {
				target = current
			}
			if err := work.admitHighWater(
				"durable projected fallback workspace",
				target, &s.projectionFallbackReserved,
			); err != nil {
				return nil, err
			}
			if int64(cap(s.projectionFallback)) >= required {
				return s.projectionFallback[:0], nil
			}
			s.projectionFallback = make([]byte, 0, int(required))
			return s.projectionFallback, nil
		},
		s.p.limit,
		match, visit, fallback,
	)
	s.projectionValues = scratch
	if err != nil {
		return true, err
	}
	if err := s.work.checkCanceled(); err != nil {
		return true, err
	}
	if !projected.Supported {
		// Storage may have called the visitor for a prefix before discovering a
		// later unsupported shape or stream. Those cells are provisional until
		// Supported is true and must be removed before the generic retry.
		s.e.Result.abortResult()
		*stats = initialStats
		s.payload = initialPayload
		clear(s.cells)
		s.cells = s.cells[:0]
		s.work.text = s.work.text[:0]
		s.work.lateText = s.work.lateText[:0]
		s.work.heapWorkTextReserved = 0
		s.projectionTextReserved = 0
		s.projectionLateTextReserved = 0
		work.rollback(mark)
		return false, nil
	}
	s.stats.RowsScanned = uint64(projected.Scanned)
	s.stats.ProjectedRows = uint64(projected.NativeMatched)
	s.stats.Workers = 1
	if projected.Matched > 0 {
		s.stats.Batches = 1
		s.stats.PeakBatchRows = projected.Matched
		s.stats.PeakBatchBytes = int64(cap(s.projectionValues))
		s.stats.BufferedBytes = max(s.stats.BufferedBytes, int64(cap(s.projectionValues)))
	}
	return true, nil
}

func (s *fileSmallScan) releaseFileProjection() {
	if s == nil {
		return
	}
	clear(s.projectionShapes)
	clear(s.projectionShape)
	clear(s.projectionStream)
	clear(s.projectionFields)
	clear(s.projectionValues)
	clear(s.cells)
	clear(s.projectionOrdinals)
	clear(s.projectionOutput)
	clear(s.projectionScalars)
	clear(s.projectionSlots)
	clear(s.projectionFallback)
	s.projection = nil
	s.projectionPlan = nil
	s.projectionFilter = 0
	s.projectionPaths = nil
	s.projectionShapes = nil
	s.projectionShape = nil
	s.projectionStream = nil
	s.projectionFields = nil
	s.projectionValues = nil
	s.cells = nil
	s.projectionOrdinals = nil
	s.projectionOutput = nil
	s.projectionScalars = nil
	s.projectionSlots = nil
	s.projectionFallback = nil
	s.projectionTextReserved = 0
	s.projectionLateTextReserved = 0
	s.projectionFallbackReserved = 0
}
