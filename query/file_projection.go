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

// fileProjectionPaths proves the deliberately narrow query shape for the
// storage-native primary projection lane. The range certificate says that the
// complete predicate is already represented by the native primary bounds, so
// the callback only has to classify and retain the selected scalar fields.
// Keeping this proof separate from runFileSmall is important: a failed proof
// must leave the ordinary raw scan as the only semantic implementation.
func (p *plan) fileProjectionFieldCount(
	span *FileRangeSource,
	ordered bool,
) (int, bool) {
	if p == nil || span == nil || !ordered ||
		span.orderedPath == "" || span.predicatePath == "" ||
		span.predicatePath != span.orderedPath ||
		!p.hasLimit || p.limit <= 0 ||
		p.grouped || p.hasAggregate || len(p.joins) != 0 ||
		len(p.marks) != 0 || p.requiresSQLDomainScan() ||
		len(p.order) != 1 || p.order[0].dir != Asc ||
		p.order[0].value < 0 || p.order[0].value >= len(p.valuePaths) ||
		p.valuePaths[p.order[0].value].indexPath() != span.orderedPath {
		return 0, false
	}
	for _, column := range p.columns {
		// Aggregates, semantic-only columns, COUNT(*), and synthetic
		// cardinality columns have no direct scalar field to decode.
		if column.agg != aggNone || column.value < 0 ||
			column.value >= len(p.valuePaths) {
			return 0, false
		}
		path := p.valuePaths[column.value]
		if path.join != joinPathOuter {
			return 0, false
		}
		canonical := path.indexPath()
		// The storage projection lane is scalar-only. The empty pointer and
		// root pointer can name a whole document/container, so let the generic
		// reader preserve its exact semantics for those paths.
		if canonical == "" || canonical == "/" {
			return 0, false
		}
	}
	return len(p.columns), len(p.columns) != 0
}

func (p *plan) fileProjectionPaths(
	dst []string,
	span *FileRangeSource,
	ordered bool,
) ([]string, bool) {
	fieldCount, ok := p.fileProjectionFieldCount(span, ordered)
	if !ok || cap(dst) < fieldCount {
		return nil, false
	}
	dst = dst[:fieldCount]
	for i, column := range p.columns {
		dst[i] = p.valuePaths[column.value].indexPath()
	}
	return dst, true
}

// fileProjectionMetadataBytes charges the actual caller-owned projection
// capacities. The storage wrapper never allocates replacement shape, stream,
// or field slices, so these bytes have to be admitted before any of them grow.
func fileProjectionMetadataBytes(
	shapeSeenCap, shapeWorkCap, streamCap, fieldCap, cellCap, pathCap int,
) int64 {
	if shapeSeenCap < 0 || shapeWorkCap < 0 || streamCap < 0 ||
		fieldCap < 0 || cellCap < 0 || pathCap < 0 {
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
	return saturatedBytes(bytes, saturatedProduct(
		int64(pathCap), int64(unsafe.Sizeof("")),
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

func fileProjectionFilterBytesForPlan(p *plan) int64 {
	if p == nil {
		return 0
	}
	count := int64(len(p.columns))
	bytes := int64(unsafe.Sizeof(durable.ProjectionFilter{}))
	bytes = saturatedBytes(bytes, int64(unsafe.Sizeof(storeio.UnifiedProjectionFilter{})))
	bytes = saturatedBytes(bytes, saturatedProduct(
		count, int64(unsafe.Sizeof(storeio.UnifiedHoleResolver{})),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		count, int64(unsafe.Sizeof("")),
	))
	bytes = saturatedBytes(bytes, saturatedProduct(
		count, int64(unsafe.Sizeof([]byte(nil))),
	))
	for _, column := range p.columns {
		if column.value < 0 || column.value >= len(p.valuePaths) {
			return math.MaxInt64
		}
		bytes = saturatedBytes(bytes, fileProjectionFilterPathBytes(
			p.valuePaths[column.value].indexPath(),
		))
	}
	return bytes
}

// ensureFileProjectionWorkspace chooses the largest compact-shape prefix that
// fits the current work budget. A storage implementation may decline a leaf
// with more shapes than this prefix; the query then retries the same snapshot
// range through the generic reader. This keeps a small MemoryBytes target from
// paying for a 256-shape by N-field slab that it cannot use.
func (s *fileSmallScan) ensureFileProjectionWorkspace(
	fieldCount int,
	filterBytes int64,
	opts normalizedFileOptions,
) bool {
	if fieldCount <= 0 {
		return false
	}
	work := s.work.activeHeapWorkBudget()
	remaining := work.remaining()
	if remaining <= 0 {
		return false
	}
	maxInt := int64(^uint(0) >> 1)
	if int64(fieldCount) > maxInt/int64(unsafe.Sizeof(storeio.UnifiedProjectionStreamWorkspace{})) {
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
	shapeCap := max(cap(s.projectionShapes), shapeTarget)
	shapeWorkCap := max(cap(s.projectionShape), shapeTarget)
	streamCap := max(cap(s.projectionStream), streamTarget)
	fieldCap := max(cap(s.projectionFields), fieldCount)
	cellCap := max(cap(s.cells), fieldCount)
	pathCap := max(cap(s.projectionPaths), fieldCount)
	valueCap := max(cap(s.projectionValues), int(scratchCap))
	metadata := fileProjectionMetadataBytes(
		shapeCap, shapeWorkCap, streamCap, fieldCap, cellCap, pathCap,
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
	if cap(s.cells) < fieldCount {
		s.cells = make([]Cell, fieldCount)
	} else {
		s.cells = resizeFileProjection(s.cells, fieldCount)
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

// projectedResultInto classifies one storage callback and immediately moves
// every selected scalar into Result ownership. The callback sees borrowed
// JSON only until it returns; no projected field is retained in a Result or
// reusable Exec workspace.
func (s *fileSmallScan) projectedResultInto(
	result *Result,
	fields []storeio.UnifiedProjectionField,
	payload *int64,
) error {
	if err := s.work.checkCanceled(); err != nil {
		return err
	}
	fieldBytes := 0
	for _, field := range fields {
		if fieldBytes > int(^uint(0)>>1)-len(field.JSON) {
			return &WorkBudgetError{
				Resource: "durable projected field payload",
				Bytes:    math.MaxInt64,
				Limit:    math.MaxInt64,
			}
		}
		fieldBytes += len(field.JSON)
	}
	// Storage bounds its append into projectionValues, but retain this guard at
	// the callback boundary as well: compressed stream decoding must never be
	// followed by an unbounded query-side copy.
	if fieldBytes > cap(s.projectionValues) {
		return &WorkBudgetError{
			Resource: "durable projected field payload",
			Bytes:    int64(fieldBytes),
			Limit:    int64(cap(s.projectionValues)),
		}
	}
	textNeed, err := projectedTextBytes(fields, s.work.cancel)
	if err != nil {
		return err
	}
	if err := s.work.admitDecodedText(textNeed); err != nil {
		return err
	}
	s.work.text = s.work.text[:0]
	if cap(s.work.text) < textNeed {
		s.work.text = make([]byte, 0, growCap(cap(s.work.text), textNeed))
	}
	rowPayload := int64(0)
	rowCells := s.cells[:0]
	if cap(rowCells) < len(fields) {
		rowCells = make([]Cell, len(fields))
	} else {
		rowCells = rowCells[:len(fields)]
	}
	for field, value := range fields {
		scalarValue := classifyRawInto(
			vibejson.RawValue{Src: value.JSON}, &s.work.text,
		)
		cell := cellFromScalar(scalarValue)
		add := projectedCellPayloadBytes(cell)
		if add < 0 || rowPayload > math.MaxInt64-add {
			return result.resultByteBudgetError(result.RowCount+1, math.MaxInt64)
		}
		rowPayload += add
		rowCells[field] = cell
	}
	nextPayload := saturatedBytes(*payload, rowPayload)
	required, err := result.checkResultBudget(
		len(fields), result.RowCount+1, nextPayload,
	)
	if err != nil {
		return err
	}
	result.resultBytesUsed = required
	for field, cell := range rowCells {
		result.Columns[field].Cells = append(
			result.Columns[field].Cells,
			result.ownProjectedCell(cell),
		)
	}
	*payload = nextPayload
	result.RowCount++
	s.cells = rowCells
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
) (bool, error) {
	work := s.work.activeHeapWorkBudget()
	mark := work.checkpoint()
	initialStats := *stats
	initialPayload := s.payload
	fieldCount, ok := s.p.fileProjectionFieldCount(span, s.ordered)
	if !ok {
		work.rollback(mark)
		return false, nil
	}
	// Prepared compilers can refill the same plan object. Validate its path
	// configuration as well as its identity before reusing a storage filter.
	// Compiled path spellings own stable storage across compiler rewinds.
	replaceFilter := s.projectionPlan != s.p || s.projection == nil ||
		len(s.projectionPaths) != fieldCount
	if !replaceFilter {
		for i, column := range s.p.columns {
			if s.projectionPaths[i] != s.p.valuePaths[column.value].indexPath() {
				replaceFilter = true
				break
			}
		}
	}
	filterBytes := s.projectionFilter
	if replaceFilter {
		filterBytes = fileProjectionFilterBytesForPlan(s.p)
	}
	if !s.ensureFileProjectionWorkspace(fieldCount, filterBytes, opts) {
		work.rollback(mark)
		return false, nil
	}
	paths, ok := s.p.fileProjectionPaths(s.projectionPaths[:0], span, s.ordered)
	if !ok {
		work.rollback(mark)
		return false, nil
	}
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
			work.rollback(mark)
			return false, nil
		}
		s.projection = filter
		s.projectionPlan = s.p
		s.projectionFilter = filterBytes
	}
	projected, scratch, err := snapshot.FilterProjectedRangeWithScratch(
		s.projection,
		span.lower, span.upper, span.lowerExclusive,
		s.projectionShapes, s.projectionShape, s.projectionStream,
		s.projectionFields, s.projectionValues, s.p.limit,
		func(_ uint64, fields []storeio.UnifiedProjectionField) error {
			return s.projectedResultInto(&s.e.Result, fields, &s.payload)
		},
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
		work.rollback(mark)
		return false, nil
	}
	s.stats.RowsScanned = uint64(projected.Scanned)
	s.stats.ProjectedRows = uint64(projected.Scanned)
	s.stats.Workers = 1
	if projected.Scanned > 0 {
		s.stats.Batches = 1
		s.stats.PeakBatchRows = projected.Scanned
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
}
