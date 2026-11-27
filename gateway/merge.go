package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/distributedagg"
	queryengine "github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

// The shard-result merge: a single-shard route streams through unchanged; a
// multi-shard route without an ORDER BY concatenates the shard row sets under
// the aggregate cap; a multi-shard route with an ORDER BY runs a k-way merge
// over the shards' already-ordered rows, mirroring the query engine's rowHeap
// shape (container/heap plus a comparator). Each shard ran the full SQL,
// including its own ORDER BY and LIMIT, so merging the per-shard heads yields the
// correct global order, and the global LIMIT is a trim after the merge.
//
// The wire carries each cell as opaque JSON bytes with one type OID, so the
// cross-shard sort comparator decodes the JSON of each sort-key column and
// compares under the same total order the local executor sorts by: null < bool
// < number < string < container, numbers by exact decimal value, strings by
// decoded content, containers by raw source bytes. OFFSET and HAVING have no
// cross-shard merge here and are out of scope; a caller must not rely on them.

// OrderKey names one sort-key column of the shard result and its direction. The
// merge decodes only these columns. Null placement matches the local executor,
// including an explicitly authored NULLS FIRST/LAST override.
type OrderKey struct {
	// Column is the zero-based index of the sort column in the result rows.
	Column int
	// Desc reverses the comparison for this column.
	Desc bool
	// Nulls overrides the default placement and must match each shard's order.
	Nulls sqlast.WindowNullOrder
}

// ErrUnmergeableResult reports a multi-shard response that is not a row set (for
// example a completion frame), which the read merge cannot combine. Cross-shard
// writes are out of scope, so this fails closed.
var ErrUnmergeableResult = errors.New("gateway: multi-shard result is not a row set and cannot be merged")

// ErrMergeColumn reports a sort key that names a column outside a shard's row
// width, so a malformed merge specification fails closed rather than panicking.
var ErrMergeColumn = errors.New("gateway: order key column is out of range for the shard result")

// ErrMergeSchema reports a shard response whose columns or row widths do not
// match the other responses. Merging such data would silently shift values
// into the wrong output columns.
var ErrMergeSchema = errors.New("gateway: shard result schemas do not match")

// ErrMergeValue reports an invalid JSON sort-key cell. Heap comparators cannot
// return errors, so k-way merge validates every key once before heap admission.
var ErrMergeValue = errors.New("gateway: shard result contains an invalid JSON merge key")

// ErrMergeAggregate reports a malformed or non-algebraic shard aggregate
// state. The gateway fails closed rather than treating partial states as rows.
var ErrMergeAggregate = errors.New("gateway: shard aggregate states cannot be merged")

// scalar kinds, ordered to match the local executor's cross-type total order.
const (
	ckNull uint8 = iota
	ckBool
	ckNumber
	ckString
	ckContainer
)

var groupedNullJSON = [...]byte{'n', 'u', 'l', 'l'}

// cellValue is one decoded sort-key value: its kind and just enough of its
// content to compare exactly. A number keeps its spelling and an int64 fast path.
type cellValue struct {
	kind  uint8
	bval  bool
	isInt bool
	ival  int64
	num   []byte
	valid bool
	sval  []byte
	raw   []byte
}

// classifyCell decodes one wire cell into a comparable value under the query's
// total order. An explicit null cell and a null literal are one value.
func classifyCell(c shardservice.Cell) cellValue {
	var text []byte
	return classifyCellInto(c, &text)
}

func classifyCellInto(c shardservice.Cell, text *[]byte) cellValue {
	if c.Null {
		return cellValue{kind: ckNull, valid: true}
	}
	b := c.Bytes
	if len(b) == 0 {
		return cellValue{kind: ckNull, valid: true}
	}
	if b[0] <= ' ' || b[len(b)-1] <= ' ' {
		b = bytes.TrimSpace(b)
		if len(b) == 0 {
			return cellValue{kind: ckNull, valid: true}
		}
	}
	// Single-pass fast paths for the dominant canonical wire tokens: signed
	// JSON integers fitting in int64, unescaped ASCII strings, null, and
	// booleans. Any complex or invalid token falls through to vibejson.Valid.
	switch b[0] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		if i, ok := parseCanonicalJSONInt64(b); ok {
			return cellValue{kind: ckNumber, isInt: true, ival: i, num: b, valid: true}
		}
	case '"':
		if len(b) >= 2 && b[len(b)-1] == '"' {
			inner := b[1 : len(b)-1]
			plain := true
			for i := 0; i < len(inner); i++ {
				ch := inner[i]
				if ch < 0x20 || ch >= 0x80 || ch == '"' || ch == '\\' {
					plain = false
					break
				}
			}
			if plain {
				return cellValue{kind: ckString, sval: inner, valid: true}
			}
		}
	case 'n':
		if len(b) == 4 && b[1] == 'u' && b[2] == 'l' && b[3] == 'l' {
			return cellValue{kind: ckNull, valid: true}
		}
	case 't':
		if len(b) == 4 && b[1] == 'r' && b[2] == 'u' && b[3] == 'e' {
			return cellValue{kind: ckBool, bval: true, valid: true, raw: b}
		}
	case 'f':
		if len(b) == 5 && b[1] == 'a' && b[2] == 'l' && b[3] == 's' && b[4] == 'e' {
			return cellValue{kind: ckBool, bval: false, valid: true, raw: b}
		}
	}
	valid := vibejson.Valid(b)
	raw := vibejson.RawValue{Src: b}
	switch raw.Kind() {
	case jsondoc.Null:
		return cellValue{kind: ckNull, valid: valid}
	case jsondoc.Bool:
		value, _ := raw.Bool()
		return cellValue{kind: ckBool, bval: value, valid: valid, raw: b}
	case jsondoc.String:
		if !valid {
			return cellValue{kind: ckString, valid: false, raw: b}
		}
		if value, clean := raw.StringBytes(); clean {
			return cellValue{kind: ckString, sval: value, valid: true}
		}
		mark := len(*text)
		value, ok, err := raw.AppendText(*text)
		if err == nil && ok {
			*text = value
			return cellValue{kind: ckString, sval: value[mark:], valid: true}
		}
		return cellValue{kind: ckString, valid: false, raw: b}
	case jsondoc.Number:
		v := cellValue{kind: ckNumber, num: b, valid: valid}
		if !v.valid {
			return v
		}
		if i, ok := raw.Int64(); ok {
			v.isInt, v.ival = true, i
		}
		return v
	default:
		return cellValue{kind: ckContainer, raw: b, valid: valid}
	}
}

func parseCanonicalJSONInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		return 0, len(b) == i+1
	}
	if b[i] < '1' || b[i] > '9' {
		return 0, false
	}
	const maxPos = uint64(1<<63 - 1)
	const maxNeg = uint64(1 << 63)
	limit := maxPos
	if neg {
		limit = maxNeg
	}
	var mag uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if mag > (limit-d)/10 {
			return 0, false
		}
		mag = mag*10 + d
	}
	if neg {
		return -int64(mag), true
	}
	return int64(mag), true
}

// compareCells returns the sign of a - b under the query's total order over JSON
// values.
func compareCells(a, b cellValue) int {
	return compareCellPtrs(&a, &b)
}

func compareCellPtrs(a, b *cellValue) int {
	if a.kind != b.kind {
		if a.kind < b.kind {
			return -1
		}
		return 1
	}
	switch a.kind {
	case ckNull:
		return 0
	case ckBool:
		switch {
		case a.bval == b.bval:
			return 0
		case !a.bval:
			return -1
		default:
			return 1
		}
	case ckNumber:
		if !a.valid || !b.valid {
			return bytes.Compare(a.num, b.num)
		}
		if a.isInt && b.isInt {
			switch {
			case a.ival < b.ival:
				return -1
			case a.ival > b.ival:
				return 1
			default:
				return 0
			}
		}
		return compareNumberSpelling(a.num, b.num)
	case ckString:
		return bytes.Compare(a.sval, b.sval)
	default:
		return bytes.Compare(a.raw, b.raw)
	}
}

// compareNumberSpelling compares two JSON numbers without expanding their
// decimal exponents. A short hostile value such as 1e1000000000 therefore has
// work proportional to its spelling rather than its mathematical magnitude.
func compareNumberSpelling(a, b []byte) int {
	return queryengine.CompareValidatedJSONNumbers(a, b)
}

// shardRun is one shard's already-ordered rows plus the decoded sort keys of
// each row and a cursor. idx breaks ties deterministically by shard position.
type shardRun struct {
	rows [][]shardservice.Cell
	keys []cellValue // flat row-major slab: row r, key k at keys[r*width+k]
	pos  int
	idx  int
}

func mergeRunLess(a, b *shardRun, order []OrderKey) bool {
	c := compareHeads(a, b, order)
	if c != 0 {
		return c < 0
	}
	return a.idx < b.idx
}

func mergeHeapSiftDown(runs []*shardRun, order []OrderKey, i int) {
	n := len(runs)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		best := left
		if right := left + 1; right < n && mergeRunLess(runs[right], runs[left], order) {
			best = right
		}
		if !mergeRunLess(runs[best], runs[i], order) {
			break
		}
		runs[i], runs[best] = runs[best], runs[i]
		i = best
	}
}

// compareHeads compares the current head rows of two runs across the sort keys,
// negating each descending key, and returns the first non-zero sign.
func compareHeads(a, b *shardRun, order []OrderKey) int {
	width := len(order)
	ka := a.keys[a.pos*width : (a.pos+1)*width]
	kb := b.keys[b.pos*width : (b.pos+1)*width]
	for k := range order {
		c := compareOrderedCellPtrs(&ka[k], &kb[k], order[k])
		if c != 0 {
			return c
		}
	}
	return 0
}

func compareOrderedCellPtrs(a, b *cellValue, order OrderKey) int {
	if order.Nulls != sqlast.WindowNullsDefault && (a.kind == ckNull || b.kind == ckNull) {
		if a.kind == b.kind {
			return 0
		}
		if (a.kind == ckNull) == (order.Nulls == sqlast.WindowNullsFirst) {
			return -1
		}
		return 1
	}
	cmp := compareCellPtrs(a, b)
	if order.Desc {
		return -cmp
	}
	return cmp
}

type groupedSortEntry struct {
	row     []shardservice.Cell
	keys    []cellValue
	text    []byte
	ordinal int
}

func compareGroupedSortEntries(a, b *groupedSortEntry, order []OrderKey) int {
	for key := range order {
		comparison := compareOrderedCellPtrs(&a.keys[key], &b.keys[key], order[key])
		if comparison != 0 {
			return comparison
		}
	}
	switch {
	case a.ordinal < b.ordinal:
		return -1
	case a.ordinal > b.ordinal:
		return 1
	default:
		return 0
	}
}

func groupedTopKSiftDown(entries []groupedSortEntry, order []OrderKey, i int) {
	// Max-heap in requested order so the worst retained row is at entries[0].
	n := len(entries)
	moving := entries[i]
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		worst := left
		if right := left + 1; right < n &&
			compareGroupedSortEntries(&entries[right], &entries[left], order) > 0 {
			worst = right
		}
		if compareGroupedSortEntries(&entries[worst], &moving, order) <= 0 {
			break
		}
		entries[i] = entries[worst]
		i = worst
	}
	entries[i] = moving
}

// finalizeGroupedRows applies the coordinator-only stage after exact grouped
// aggregation. ORDER BY with a strict LIMIT retains only K decoded key tuples;
// ORDER BY without LIMIT performs one bounded full sort. LIMIT without order
// trims stable first-appearance order without allocating.
func finalizeGroupedRows(
	rows [][]shardservice.Cell,
	order []OrderKey,
	limit int,
	maxBytes uint64,
) ([][]shardservice.Cell, error) {
	return finalizeGroupedRowsWindow(rows, order, 0, limit, limit > 0, maxBytes)
}

func finalizeGroupedRowsWindow(
	rows [][]shardservice.Cell,
	order []OrderKey,
	offset int,
	limit int,
	hasLimit bool,
	maxBytes uint64,
) ([][]shardservice.Cell, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("%w: grouped LIMIT is negative", ErrMergeAggregate)
	}
	if len(order) == 0 {
		return sliceGroupedWindow(rows, offset, limit, hasLimit), nil
	}
	for _, key := range order {
		if key.Column < 0 || (len(rows) != 0 && key.Column >= len(rows[0])) {
			return nil, ErrMergeColumn
		}
	}
	if len(rows) < 2 {
		return sliceGroupedWindow(rows, offset, limit, hasLimit), nil
	}
	retain := len(rows)
	if hasLimit {
		if offset > int(^uint(0)>>1)-limit {
			return nil, fmt.Errorf("%w: grouped OFFSET + LIMIT overflows", ErrMergeAggregate)
		}
		retain = min(retain, offset+limit)
		if retain == 0 {
			return rows[:0], nil
		}
	}
	if hasLimit && retain < len(rows) {
		selected, err := groupedTopK(rows, order, retain, maxBytes)
		if err != nil {
			return nil, err
		}
		return sliceGroupedWindow(selected, offset, limit, true), nil
	}
	ordered, err := groupedFullSort(rows, order, maxBytes)
	if err != nil {
		return nil, err
	}
	return sliceGroupedWindow(ordered, offset, limit, hasLimit), nil
}

func sliceGroupedWindow(rows [][]shardservice.Cell, offset, limit int, hasLimit bool) [][]shardservice.Cell {
	if offset >= len(rows) {
		return rows[:0]
	}
	rows = rows[offset:]
	if hasLimit && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func groupedTopK(
	rows [][]shardservice.Cell,
	order []OrderKey,
	limit int,
	maxBytes uint64,
) ([][]shardservice.Cell, error) {
	maxText := 0
	for _, row := range rows {
		maxText = max(maxText, groupedEscapedTextBytes(row, order))
	}
	if err := admitGroupedSortState(limit+1, len(order), maxText, maxBytes); err != nil {
		return nil, err
	}
	hasEscapedText := maxText > 0
	keySlab := make([]cellValue, (limit+1)*len(order))
	entries := make([]groupedSortEntry, limit)
	for row := 0; row < limit; row++ {
		entries[row].keys = keySlab[row*len(order) : (row+1)*len(order)]
		if err := prepareGroupedSortEntry(&entries[row], rows[row], order, row, hasEscapedText); err != nil {
			return nil, err
		}
	}
	for i := limit/2 - 1; i >= 0; i-- {
		groupedTopKSiftDown(entries, order, i)
	}
	candidate := groupedSortEntry{keys: keySlab[limit*len(order):]}
	if len(order) == 1 && !hasEscapedText {
		ord0 := order[0]
		col0 := ord0.Column
		for row := limit; row < len(rows); row++ {
			if len(candidate.text) != 0 {
				candidate.text = candidate.text[:0]
			}
			kv := classifyCellInto(rows[row][col0], &candidate.text)
			if !kv.valid {
				return nil, fmt.Errorf("%w: grouped row %d column %d", ErrMergeValue, row, col0)
			}
			cmp := compareOrderedCellPtrs(&kv, &entries[0].keys[0], ord0)
			if cmp > 0 || (cmp == 0 && row >= entries[0].ordinal) {
				continue
			}
			candidate.row = rows[row]
			candidate.ordinal = row
			candidate.keys[0] = kv
			displaced := entries[0]
			entries[0] = candidate
			candidate = displaced
			groupedTopKSiftDown(entries, order, 0)
		}
	} else {
		for row := limit; row < len(rows); row++ {
			if err := prepareGroupedSortEntry(&candidate, rows[row], order, row, hasEscapedText); err != nil {
				return nil, err
			}
			if compareGroupedSortEntries(&candidate, &entries[0], order) >= 0 {
				continue
			}
			displaced := entries[0]
			entries[0] = candidate
			candidate = displaced
			groupedTopKSiftDown(entries, order, 0)
		}
	}
	slices.SortFunc(entries, func(a, b groupedSortEntry) int {
		return compareGroupedSortEntries(&a, &b, order)
	})
	out := make([][]shardservice.Cell, limit)
	for row := range out {
		out[row] = entries[row].row
	}
	return out, nil
}

func groupedFullSort(
	rows [][]shardservice.Cell,
	order []OrderKey,
	maxBytes uint64,
) ([][]shardservice.Cell, error) {
	totalText := 0
	for _, row := range rows {
		text := groupedEscapedTextBytes(row, order)
		if totalText > int(^uint(0)>>1)-text {
			return nil, fmt.Errorf("%w: grouped sort text accounting overflow", ErrMergeAggregate)
		}
		totalText += text
	}
	if err := admitGroupedSortState(len(rows), len(order), totalText, maxBytes); err != nil {
		return nil, err
	}
	entries := make([]groupedSortEntry, len(rows))
	keySlab := make([]cellValue, len(rows)*len(order))
	text := make([]byte, 0, totalText)
	for row := range rows {
		entries[row] = groupedSortEntry{
			row: rows[row], keys: keySlab[row*len(order) : (row+1)*len(order)], ordinal: row,
		}
		for key := range order {
			entries[row].keys[key] = classifyCellInto(rows[row][order[key].Column], &text)
			if !entries[row].keys[key].valid {
				return nil, fmt.Errorf("%w: grouped row %d column %d", ErrMergeValue, row, order[key].Column)
			}
		}
	}
	slices.SortFunc(entries, func(a, b groupedSortEntry) int {
		return compareGroupedSortEntries(&a, &b, order)
	})
	out := make([][]shardservice.Cell, len(rows))
	for row := range out {
		out[row] = entries[row].row
	}
	return out, nil
}

func prepareGroupedSortEntry(
	entry *groupedSortEntry,
	row []shardservice.Cell,
	order []OrderKey,
	ordinal int,
	hasEscapedText bool,
) error {
	if hasEscapedText {
		textBytes := groupedEscapedTextBytes(row, order)
		if cap(entry.text) < textBytes {
			entry.text = make([]byte, 0, textBytes)
		} else {
			entry.text = entry.text[:0]
		}
	} else if len(entry.text) != 0 {
		entry.text = entry.text[:0]
	}
	entry.row, entry.ordinal = row, ordinal
	for key := range order {
		entry.keys[key] = classifyCellInto(row[order[key].Column], &entry.text)
		if !entry.keys[key].valid {
			return fmt.Errorf("%w: grouped row %d column %d", ErrMergeValue, ordinal, order[key].Column)
		}
	}
	return nil
}

func groupedEscapedTextBytes(row []shardservice.Cell, order []OrderKey) int {
	total := 0
	for _, key := range order {
		cell := row[key.Column]
		if cell.Null || len(cell.Bytes) < 2 {
			continue
		}
		b := cell.Bytes
		if b[0] <= ' ' {
			b = bytes.TrimLeft(b, " \t\r\n")
		}
		if len(b) >= 2 && b[0] == '"' && bytes.IndexByte(b, '\\') >= 0 {
			total += len(b)
		}
	}
	return total
}

func admitGroupedSortState(rows, keys, textBytes int, maxBytes uint64) error {
	if rows < 0 || keys < 0 || textBytes < 0 {
		return fmt.Errorf("%w: grouped sort state is invalid", ErrMergeAggregate)
	}
	maxInt := int(^uint(0) >> 1)
	if keys != 0 && rows > maxInt/keys {
		return fmt.Errorf("%w: grouped sort key cardinality overflows address space", ErrMergeAggregate)
	}
	perRow := uint64(48)
	if keys != 0 {
		if uint64(keys) > (^uint64(0)-perRow)/96 {
			return fmt.Errorf("%w: grouped sort state accounting overflow", ErrMergeAggregate)
		}
		perRow += uint64(keys) * 96
	}
	if uint64(rows) > (^uint64(0)-uint64(textBytes))/perRow {
		return fmt.Errorf("%w: grouped sort state accounting overflow", ErrMergeAggregate)
	}
	stateBytes := uint64(rows)*perRow + uint64(textBytes)
	if maxBytes != 0 && stateBytes > maxBytes {
		return fmt.Errorf("%w: grouped sort state %d bytes exceeds %d-byte cap",
			ErrMergeAggregate, stateBytes, maxBytes)
	}
	return nil
}

// mergeRows combines the shard responses into one ordered result. With no order
// keys it concatenates in target order; with order keys it k-way merges the
// already-ordered shards. A positive limit trims the merged rows. Columns come
// from the first shard's row frame, and every response must be a row set.
func mergeRows(results []*shardservice.ShardResponse, order []OrderKey, limit int) ([]shardservice.Column, [][]shardservice.Cell, error) {
	return mergeRowsWindow(results, order, 0, limit, limit > 0)
}

func mergeRowsWindow(results []*shardservice.ShardResponse, order []OrderKey, offset, limit int, hasLimit bool) ([]shardservice.Column, [][]shardservice.Cell, error) {
	if len(results) == 0 {
		return nil, nil, nil
	}
	columns, err := validateRowResults(results)
	if err != nil {
		return nil, nil, err
	}
	if hasLimit && limit == 0 {
		return columns, nil, nil
	}
	if len(order) == 0 {
		rows := concatRows(results, 0)
		return columns, sliceGroupedWindow(rows, offset, limit, hasLimit), nil
	}
	window := 0
	if hasLimit {
		window = offset + limit
	}
	rows, err := kwayMerge(results, order, window)
	if err != nil {
		return nil, nil, err
	}
	return columns, sliceGroupedWindow(rows, offset, limit, hasLimit), nil
}

func validateRowResults(results []*shardservice.ShardResponse) ([]shardservice.Column, error) {
	if len(results) == 0 || results[0] == nil {
		return nil, ErrUnmergeableResult
	}
	columns := results[0].Columns
	for _, resp := range results {
		if resp == nil || resp.Kind != shardservice.ResponseRows {
			return nil, ErrUnmergeableResult
		}
		if len(resp.Columns) != len(columns) {
			return nil, ErrMergeSchema
		}
		for i := range columns {
			if resp.Columns[i] != columns[i] {
				return nil, ErrMergeSchema
			}
		}
		for _, row := range resp.Rows {
			if len(row) != len(columns) {
				return nil, ErrMergeSchema
			}
		}
	}
	return columns, nil
}

// mergeAggregateRows combines one shard-local row per target. COUNT and SUM
// use exact arithmetic; MIN/MAX preserve an exact contributing shard spelling.
func mergeAggregateRows(
	results []*shardservice.ShardResponse,
	kinds []sqlast.AggKind,
	maxBytes uint64,
) ([]shardservice.Column, [][]shardservice.Cell, error) {
	columns, err := validateRowResults(results)
	if err != nil {
		return nil, nil, err
	}
	if len(columns) != len(kinds) {
		return nil, nil, ErrMergeSchema
	}
	for _, response := range results {
		if len(response.Rows) != 1 {
			return nil, nil, fmt.Errorf("%w: shard returned %d aggregate rows, want one", ErrMergeAggregate, len(response.Rows))
		}
	}
	row := make([]shardservice.Cell, len(kinds))
	for column, kind := range kinds {
		cells := make([]shardservice.Cell, len(results))
		for shard := range results {
			cells[shard] = results[shard].Rows[0][column]
		}
		program, programErr := distributedAggregateKind(kind)
		if programErr != nil || program == distributedagg.None {
			err = fmt.Errorf("%w: aggregate %s has no combiner", ErrMergeAggregate, kind)
		} else {
			row[column], err = distributedagg.MergeCells(program, cells, maxBytes)
		}
		if err != nil {
			return nil, nil, errors.Join(ErrMergeAggregate, err)
		}
	}
	return columns, [][]shardservice.Cell{row}, nil
}

type groupedAggregateMerger struct {
	shared *distributedagg.Merger
}

func newGroupedAggregateMerger(
	kinds []sqlast.AggKind,
	groupKeys []int,
	maxBytes uint64,
) (*groupedAggregateMerger, error) {
	program, keys, err := distributedAggregateProgram(kinds, groupKeys)
	if err != nil {
		return nil, err
	}
	shared, err := distributedagg.NewMerger(program, keys, maxBytes)
	if err != nil {
		return nil, errors.Join(ErrMergeAggregate, err)
	}
	return &groupedAggregateMerger{shared: shared}, nil
}

// mergeGroupedAggregateRows combines shard-local GROUP BY rows by the query
// engine's exact composite group identity, then finalizes COUNT/SUM/MIN/MAX.
func mergeGroupedAggregateRows(
	results []*shardservice.ShardResponse,
	kinds []sqlast.AggKind,
	groupKeys []int,
	maxBytes uint64,
) ([]shardservice.Column, [][]shardservice.Cell, error) {
	columns, err := validateRowResults(results)
	if err != nil {
		return nil, nil, err
	}
	if len(columns) != len(kinds) {
		return nil, nil, ErrMergeSchema
	}
	merger, err := newGroupedAggregateMerger(kinds, groupKeys, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	hint := 0
	for shard := range results {
		if len(results[shard].Rows) > hint {
			hint = len(results[shard].Rows)
		}
	}
	merger.reserve(hint)
	for shard := range results {
		for row := range results[shard].Rows {
			if err := merger.add(results[shard].Rows[row]); err != nil {
				return nil, nil, fmt.Errorf("%w: shard %d row %d: %v", ErrMergeAggregate, shard, row, err)
			}
		}
	}
	rows, err := merger.finish()
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func (m *groupedAggregateMerger) reserve(groupsHint int) {
	if m == nil || m.shared == nil {
		return
	}
	m.shared.Reserve(groupsHint)
}

func (m *groupedAggregateMerger) add(row []shardservice.Cell) error {
	if m == nil || m.shared == nil {
		return ErrMergeAggregate
	}
	if err := m.shared.Add(row); err != nil {
		return errors.Join(ErrMergeAggregate, err)
	}
	return nil
}

func (m *groupedAggregateMerger) finish() ([][]shardservice.Cell, error) {
	if m == nil || m.shared == nil {
		return nil, ErrMergeAggregate
	}
	rows, err := m.shared.Finish()
	if err != nil {
		return nil, errors.Join(ErrMergeAggregate, err)
	}
	return rows, nil
}

// concatRows appends every shard's rows in target order, trimming at a positive
// limit.
func concatRows(results []*shardservice.ShardResponse, limit int) [][]shardservice.Cell {
	var out [][]shardservice.Cell
	for _, resp := range results {
		out = append(out, resp.Rows...)
		if limit > 0 && len(out) >= limit {
			return out[:limit]
		}
	}
	return out
}

// kwayMerge merges the shards' already-ordered rows into one globally ordered
// stream, decoding each row's sort-key columns once, and trims at a positive
// limit.
func kwayMerge(results []*shardservice.ShardResponse, order []OrderKey, limit int) ([][]shardservice.Cell, error) {
	runs := make([]*shardRun, 0, len(results))
	width := len(order)
	total := 0
	for idx, resp := range results {
		if len(resp.Rows) == 0 {
			continue
		}
		keySlab := make([]cellValue, len(resp.Rows)*width)
		var text []byte
		for r, row := range resp.Rows {
			base := r * width
			for k := range order {
				col := order[k].Column
				if col < 0 || col >= len(row) {
					return nil, ErrMergeColumn
				}
				kv := classifyCellInto(row[col], &text)
				if !kv.valid {
					return nil, fmt.Errorf("%w: shard %d row %d column %d", ErrMergeValue, idx, r, col)
				}
				keySlab[base+k] = kv
			}
		}
		runs = append(runs, &shardRun{rows: resp.Rows, keys: keySlab, idx: idx})
		total += len(resp.Rows)
	}
	for i := len(runs)/2 - 1; i >= 0; i-- {
		mergeHeapSiftDown(runs, order, i)
	}

	capacity := total
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	out := make([][]shardservice.Cell, 0, capacity)
	for len(runs) > 0 {
		top := runs[0]
		out = append(out, top.rows[top.pos])
		if limit > 0 && len(out) >= limit {
			break
		}
		top.pos++
		if top.pos < len(top.rows) {
			mergeHeapSiftDown(runs, order, 0)
		} else {
			last := len(runs) - 1
			runs[0] = runs[last]
			runs[last] = nil
			runs = runs[:last]
			if len(runs) > 1 {
				mergeHeapSiftDown(runs, order, 0)
			}
		}
	}
	return out, nil
}
