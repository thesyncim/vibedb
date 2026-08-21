package gateway

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"slices"
	"strconv"
	"strings"

	queryplanner "github.com/thesyncim/vibedb/planner"
	queryengine "github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
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
// merge decodes only these columns. Nulls sort with the kind order — first under
// ascending, last under descending — mirroring the local executor, which has no
// separate NULLS FIRST/LAST control.
type OrderKey struct {
	// Column is the zero-based index of the sort column in the result rows.
	Column int
	// Desc reverses the comparison for this column.
	Desc bool
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
	b := bytes.TrimSpace(c.Bytes)
	if len(b) == 0 {
		return cellValue{kind: ckNull, valid: true}
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

// compareCells returns the sign of a - b under the query's total order over JSON
// values.
func compareCells(a, b cellValue) int {
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
	keys [][]cellValue
	pos  int
	idx  int
}

// mergeHeap is the k-way merge frontier: one shardRun per shard, ordered by the
// current head row under the sort keys. It mirrors the query engine's rowHeap.
type mergeHeap struct {
	runs  []*shardRun
	order []OrderKey
}

func (h mergeHeap) Len() int { return len(h.runs) }

func (h mergeHeap) Less(i, j int) bool {
	c := compareHeads(h.runs[i], h.runs[j], h.order)
	if c != 0 {
		return c < 0
	}
	return h.runs[i].idx < h.runs[j].idx
}

func (h mergeHeap) Swap(i, j int) { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }

func (h *mergeHeap) Push(x any) { h.runs = append(h.runs, x.(*shardRun)) }

func (h *mergeHeap) Pop() any {
	n := len(h.runs) - 1
	x := h.runs[n]
	h.runs = h.runs[:n]
	return x
}

// compareHeads compares the current head rows of two runs across the sort keys,
// negating each descending key, and returns the first non-zero sign.
func compareHeads(a, b *shardRun, order []OrderKey) int {
	ka, kb := a.keys[a.pos], b.keys[b.pos]
	for k := range order {
		c := compareCells(ka[k], kb[k])
		if order[k].Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

type groupedSortEntry struct {
	row     []shardservice.Cell
	keys    []cellValue
	text    []byte
	ordinal int
}

func compareGroupedSortEntries(a, b *groupedSortEntry, order []OrderKey) int {
	for key := range order {
		comparison := compareCells(a.keys[key], b.keys[key])
		if order[key].Desc {
			comparison = -comparison
		}
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

type groupedTopKHeap struct {
	entries []groupedSortEntry
	order   []OrderKey
}

func (h groupedTopKHeap) Len() int { return len(h.entries) }
func (h groupedTopKHeap) Less(i, j int) bool {
	// Reverse the requested order so the worst retained row is the root.
	return compareGroupedSortEntries(&h.entries[i], &h.entries[j], h.order) > 0
}
func (h groupedTopKHeap) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *groupedTopKHeap) Push(value any) {
	h.entries = append(h.entries, value.(groupedSortEntry))
}
func (h *groupedTopKHeap) Pop() any {
	last := len(h.entries) - 1
	value := h.entries[last]
	h.entries = h.entries[:last]
	return value
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
	if limit < 0 {
		return nil, fmt.Errorf("%w: grouped LIMIT is negative", ErrMergeAggregate)
	}
	if len(order) == 0 {
		if limit > 0 && limit < len(rows) {
			return rows[:limit], nil
		}
		return rows, nil
	}
	for _, key := range order {
		if key.Column < 0 || (len(rows) != 0 && key.Column >= len(rows[0])) {
			return nil, ErrMergeColumn
		}
	}
	if len(rows) < 2 {
		return rows, nil
	}
	if limit > 0 && limit < len(rows) {
		return groupedTopK(rows, order, limit, maxBytes)
	}
	return groupedFullSort(rows, order, maxBytes)
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
	keySlab := make([]cellValue, (limit+1)*len(order))
	h := &groupedTopKHeap{
		entries: make([]groupedSortEntry, limit), order: order,
	}
	for row := 0; row < limit; row++ {
		h.entries[row].keys = keySlab[row*len(order) : (row+1)*len(order)]
		if err := prepareGroupedSortEntry(&h.entries[row], rows[row], order, row); err != nil {
			return nil, err
		}
	}
	heap.Init(h)
	candidate := groupedSortEntry{keys: keySlab[limit*len(order):]}
	for row := limit; row < len(rows); row++ {
		if err := prepareGroupedSortEntry(&candidate, rows[row], order, row); err != nil {
			return nil, err
		}
		if compareGroupedSortEntries(&candidate, &h.entries[0], order) >= 0 {
			continue
		}
		displaced := h.entries[0]
		h.entries[0] = candidate
		candidate = displaced
		heap.Fix(h, 0)
	}
	slices.SortFunc(h.entries, func(a, b groupedSortEntry) int {
		return compareGroupedSortEntries(&a, &b, order)
	})
	out := make([][]shardservice.Cell, limit)
	for row := range out {
		out[row] = h.entries[row].row
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
) error {
	textBytes := groupedEscapedTextBytes(row, order)
	if cap(entry.text) < textBytes {
		entry.text = make([]byte, 0, textBytes)
	} else {
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
		if !cell.Null && len(cell.Bytes) >= 2 && cell.Bytes[0] == '"' &&
			bytes.IndexByte(cell.Bytes, '\\') >= 0 {
			total += len(cell.Bytes)
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
	if len(results) == 0 {
		return nil, nil, nil
	}
	columns, err := validateRowResults(results)
	if err != nil {
		return nil, nil, err
	}
	if len(order) == 0 {
		return columns, concatRows(results, limit), nil
	}
	rows, err := kwayMerge(results, order, limit)
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
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
		switch kind {
		case sqlast.AggCount:
			row[column], err = mergeCounts(cells, maxBytes)
		case sqlast.AggSum:
			row[column], err = mergeSums(cells, maxBytes)
		case sqlast.AggMin:
			row[column], err = mergeExtrema(cells, false)
		case sqlast.AggMax:
			row[column], err = mergeExtrema(cells, true)
		default:
			err = fmt.Errorf("%w: aggregate %s has no combiner", ErrMergeAggregate, kind)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return columns, [][]shardservice.Cell{row}, nil
}

type groupedExtreme struct {
	cell   shardservice.Cell
	number []byte
	set    bool
}

type exactCount struct {
	small uint64
	wide  big.Int
	large bool
}

func (c *exactCount) add(value uint64) {
	if !c.large && ^uint64(0)-c.small >= value {
		c.small += value
		return
	}
	if !c.large {
		c.wide.SetUint64(c.small)
		c.large = true
	}
	var term big.Int
	term.SetUint64(value)
	c.wide.Add(&c.wide, &term)
}

func (c *exactCount) addBig(value *big.Int) {
	if !c.large {
		c.wide.SetUint64(c.small)
		c.large = true
	}
	c.wide.Add(&c.wide, value)
}

func (c *exactCount) append(dst []byte) []byte {
	if c.large {
		return c.wide.Append(dst, 10)
	}
	return strconv.AppendUint(dst, c.small, 10)
}

func (c *exactCount) retainedBytes() uint64 {
	if !c.large {
		return 0
	}
	return retainedBigIntBytes(&c.wide)
}

type exactSignedInteger struct {
	small int64
	wide  big.Int
	large bool
}

func (i *exactSignedInteger) add(value int64) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	if !i.large && !((value > 0 && i.small > maxInt64-value) ||
		(value < 0 && i.small < minInt64-value)) {
		i.small += value
		return
	}
	if !i.large {
		i.wide.SetInt64(i.small)
		i.large = true
	}
	var term big.Int
	term.SetInt64(value)
	i.wide.Add(&i.wide, &term)
}

func (i *exactSignedInteger) addBig(value *big.Int) {
	if !i.large {
		i.wide.SetInt64(i.small)
		i.large = true
	}
	i.wide.Add(&i.wide, value)
}

func (i *exactSignedInteger) append(dst []byte) []byte {
	if i.large {
		return i.wide.Append(dst, 10)
	}
	return strconv.AppendInt(dst, i.small, 10)
}

func (i *exactSignedInteger) setRat(dst *big.Rat) {
	if i.large {
		dst.SetInt(&i.wide)
		return
	}
	var value big.Int
	value.SetInt64(i.small)
	dst.SetInt(&value)
}

func (i *exactSignedInteger) retainedBytes() uint64 {
	if !i.large {
		return 0
	}
	return retainedBigIntBytes(&i.wide)
}

type exactSum struct {
	integer  exactSignedInteger
	rational big.Rat
	set      bool
	fraction bool
}

type groupedOutputArena struct {
	chunks  [][]byte
	current []byte
}

func (a *groupedOutputArena) reserve(need int) int {
	if cap(a.current)-len(a.current) < need {
		size := max(4<<10, 2*cap(a.current))
		if size > 64<<10 {
			size = 64 << 10
		}
		if size < need {
			size = need
		}
		a.current = make([]byte, 0, size)
		a.chunks = append(a.chunks, a.current)
	}
	return len(a.current)
}

func (a *groupedOutputArena) appendCount(value *exactCount) []byte {
	need := 21
	if value.large {
		need = value.wide.BitLen()/3 + 2
	}
	start := a.reserve(need)
	a.current = value.append(a.current)
	return a.current[start:len(a.current):len(a.current)]
}

func (a *groupedOutputArena) appendInteger(value *exactSignedInteger) []byte {
	need := 21
	if value.large {
		need = value.wide.BitLen()/3 + 3
	}
	start := a.reserve(need)
	a.current = value.append(a.current)
	return a.current[start:len(a.current):len(a.current)]
}

func (s *exactSum) add(
	cell shardservice.Cell,
	maxBytes uint64,
	scratch []byte,
) ([]byte, error) {
	if cell.Null {
		return scratch, nil
	}
	spelling := bytes.TrimSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return scratch, fmt.Errorf("%w: SUM state %q exceeds exact numeric admission", ErrMergeAggregate, cell.Bytes)
	}
	raw := vibejson.RawValue{Src: spelling}
	if value, ok := raw.Int64(); ok {
		if s.fraction {
			var term big.Rat
			term.SetInt64(value)
			s.rational.Add(&s.rational, &term)
		} else {
			s.integer.add(value)
		}
		s.set = true
		return scratch, nil
	}
	if isPlainJSONInteger(spelling) && vibejson.Valid(spelling) {
		var value big.Int
		if _, ok := value.SetString(byteview.String(spelling), 10); !ok {
			return scratch, fmt.Errorf("%w: SUM state %q is not an exact number", ErrMergeAggregate, cell.Bytes)
		}
		if s.fraction {
			var term big.Rat
			term.SetInt(&value)
			s.rational.Add(&s.rational, &term)
		} else {
			s.integer.addBig(&value)
		}
		s.set = true
		return scratch, nil
	}

	canonical, canonicalErr := queryplanner.AppendCanonicalScalarJSON(scratch, spelling)
	if canonicalErr != nil || len(canonical) == 0 || canonical[0] == '"' ||
		canonical[0] == 't' || canonical[0] == 'f' || canonical[0] == 'n' ||
		!queryplanner.CanonicalNumberFitsDecimalBytes(byteview.String(canonical), maxBytes) {
		return canonical, fmt.Errorf("%w: SUM state %q exceeds exact numeric admission", ErrMergeAggregate, cell.Bytes)
	}
	var value big.Rat
	if _, ok := value.SetString(byteview.String(canonical)); !ok {
		return canonical, fmt.Errorf("%w: SUM state %q is not an exact number", ErrMergeAggregate, cell.Bytes)
	}
	if !s.fraction {
		s.integer.setRat(&s.rational)
		s.fraction = true
	}
	s.rational.Add(&s.rational, &value)
	s.set = true
	return canonical, nil
}

func (s *exactSum) retainedBytes() uint64 {
	if s.fraction {
		return retainedBigRatBytes(&s.rational)
	}
	return s.integer.retainedBytes()
}

func isPlainJSONInteger(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
	}
	if start == len(value) {
		return false
	}
	for _, char := range value[start:] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// groupedAggregateMerger is a columnar final-aggregation state. Each aggregate
// kind owns a dense slice indexed by group ID, avoiding one map entry or heap
// object per (group, column). KeyInterner supplies stable dense IDs and owns one
// copy of each composite exact-value group key.
type groupedAggregateMerger struct {
	kinds     []sqlast.AggKind
	groupKeys []int
	rows      []shardservice.Cell
	width     int
	counts    [][]exactCount
	sums      [][]exactSum
	extrema   [][]groupedExtreme

	interner      store.KeyInterner
	key           []byte
	numberScratch []byte
	keyEncoder    queryengine.JSONGroupKeyEncoder
	output        groupedOutputArena
	retained      uint64
	maxBytes      uint64
}

func newGroupedAggregateMerger(
	kinds []sqlast.AggKind,
	groupKeys []int,
	maxBytes uint64,
) (*groupedAggregateMerger, error) {
	if len(kinds) == 0 || len(groupKeys) == 0 {
		return nil, fmt.Errorf("%w: grouped program has no columns or keys", ErrMergeAggregate)
	}
	seen := make([]bool, len(kinds))
	for _, column := range groupKeys {
		if column < 0 || column >= len(kinds) || seen[column] || kinds[column] != sqlast.AggNone {
			return nil, fmt.Errorf("%w: grouped key program is invalid", ErrMergeAggregate)
		}
		seen[column] = true
	}
	for column, kind := range kinds {
		if kind == sqlast.AggNone && !seen[column] {
			return nil, fmt.Errorf("%w: grouped projection column %d is not a key", ErrMergeAggregate, column)
		}
	}
	m := &groupedAggregateMerger{
		kinds: kinds, groupKeys: groupKeys, maxBytes: maxBytes,
		counts: make([][]exactCount, len(kinds)), sums: make([][]exactSum, len(kinds)),
		extrema: make([][]groupedExtreme, len(kinds)),
	}
	return m, nil
}

// mergeGroupedAggregateRows combines shard-local GROUP BY rows by the query
// engine's exact composite group identity, then finalizes COUNT/SUM/MIN/MAX.
// Result order is stable first appearance in route/row order; grouped ORDER BY
// remains a separate post-finalization stage and is rejected by planning.
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

func (m *groupedAggregateMerger) add(row []shardservice.Cell) error {
	m.key = m.key[:0]
	var rawKeyBytes uint64
	for keyOrdinal, column := range m.groupKeys {
		value := row[column].Bytes
		if row[column].Null {
			value = groupedNullJSON[:]
		}
		if ^uint64(0)-rawKeyBytes < uint64(len(value))+16 {
			return fmt.Errorf("group key byte accounting overflow")
		}
		rawKeyBytes += uint64(len(value)) + 16
		if m.maxBytes != 0 &&
			(m.retained > m.maxBytes || rawKeyBytes > m.maxBytes-m.retained) {
			return fmt.Errorf("group key %d exceeds grouped state byte cap", keyOrdinal)
		}
		var ok bool
		m.key, ok = m.keyEncoder.Append(m.key, value)
		if !ok {
			return fmt.Errorf("group key column %d is not valid JSON", column)
		}
	}
	before := m.interner.Len()
	id := m.interner.Intern(m.key)
	if int(id) == before {
		if err := m.addGroup(row, len(m.key)); err != nil {
			return err
		}
	}
	group := int(id)
	for column, kind := range m.kinds {
		cell := row[column]
		switch kind {
		case sqlast.AggNone:
			continue
		case sqlast.AggCount:
			previous := m.counts[column][group].retainedBytes()
			if err := addCount(&m.counts[column][group], cell, m.maxBytes); err != nil {
				return err
			}
			if err := m.growState(previous, m.counts[column][group].retainedBytes()); err != nil {
				return err
			}
		case sqlast.AggSum:
			previous := m.sums[column][group].retainedBytes()
			var err error
			m.numberScratch, err = m.sums[column][group].add(
				cell, m.maxBytes, m.numberScratch[:0],
			)
			if err != nil {
				return err
			}
			if err := m.growState(previous, m.sums[column][group].retainedBytes()); err != nil {
				return err
			}
		case sqlast.AggMin:
			if err := addExtreme(&m.extrema[column][group], cell, false); err != nil {
				return err
			}
		case sqlast.AggMax:
			if err := addExtreme(&m.extrema[column][group], cell, true); err != nil {
				return err
			}
		default:
			return fmt.Errorf("aggregate %s has no grouped combiner", kind)
		}
	}
	return nil
}

func (m *groupedAggregateMerger) addGroup(row []shardservice.Cell, keyBytes int) error {
	// The fixed charge deliberately overestimates slice headers, cell headers,
	// interner directory load, and columnar accumulator metadata. Exact key and
	// big-number backing bytes are charged separately as they grow.
	charge := uint64(96 + keyBytes + len(row)*192)
	if err := m.charge(charge); err != nil {
		return err
	}
	if m.width == 0 {
		m.width = len(row)
	}
	m.rows = append(m.rows, row...)
	for column, kind := range m.kinds {
		switch kind {
		case sqlast.AggCount:
			m.counts[column] = append(m.counts[column], exactCount{})
		case sqlast.AggSum:
			m.sums[column] = append(m.sums[column], exactSum{})
		case sqlast.AggMin, sqlast.AggMax:
			m.extrema[column] = append(m.extrema[column], groupedExtreme{})
		}
	}
	return nil
}

func (m *groupedAggregateMerger) growState(previous, current uint64) error {
	if current <= previous {
		return nil
	}
	delta := current - previous
	if err := m.charge(delta); err != nil {
		return err
	}
	return nil
}

func (m *groupedAggregateMerger) charge(bytes uint64) error {
	if ^uint64(0)-m.retained < bytes {
		return fmt.Errorf("grouped state byte accounting overflow")
	}
	m.retained += bytes
	if m.maxBytes != 0 && m.retained > m.maxBytes {
		return fmt.Errorf("grouped state %d bytes exceeds %d-byte cap", m.retained, m.maxBytes)
	}
	return nil
}

func (m *groupedAggregateMerger) finish() ([][]shardservice.Cell, error) {
	groups := m.interner.Len()
	var outputBytes uint64
	for group := 0; group < groups; group++ {
		for column, kind := range m.kinds {
			cell := &m.rows[group*m.width+column]
			switch kind {
			case sqlast.AggNone:
			case sqlast.AggCount:
				*cell = shardservice.Cell{Bytes: m.output.appendCount(&m.counts[column][group])}
			case sqlast.AggSum:
				state := &m.sums[column][group]
				if !state.set {
					*cell = shardservice.Cell{Null: true}
				} else if !state.fraction {
					*cell = shardservice.Cell{Bytes: m.output.appendInteger(&state.integer)}
				} else {
					spelling, err := exactDecimalString(&state.rational, m.maxBytes)
					if err != nil {
						return nil, err
					}
					*cell = shardservice.Cell{Bytes: []byte(spelling)}
				}
			case sqlast.AggMin, sqlast.AggMax:
				state := &m.extrema[column][group]
				if state.set {
					*cell = state.cell
				} else {
					*cell = shardservice.Cell{Null: true}
				}
			}
			if ^uint64(0)-outputBytes < uint64(len(cell.Bytes)) {
				return nil, fmt.Errorf("%w: grouped output byte accounting overflow", ErrMergeAggregate)
			}
			outputBytes += uint64(len(cell.Bytes))
			if m.maxBytes != 0 && outputBytes > m.maxBytes {
				return nil, fmt.Errorf("%w: grouped output %d bytes exceeds %d-byte cap",
					ErrMergeAggregate, outputBytes, m.maxBytes)
			}
		}
	}
	rows := make([][]shardservice.Cell, groups)
	for group := range rows {
		start := group * m.width
		rows[group] = m.rows[start : start+m.width : start+m.width]
	}
	return rows, nil
}

func retainedBigIntBytes(value *big.Int) uint64 {
	return uint64(cap(value.Bits())) * uint64(bits.UintSize/8)
}

func retainedBigRatBytes(value *big.Rat) uint64 {
	return retainedBigIntBytes(value.Num()) + retainedBigIntBytes(value.Denom())
}

func mergeCounts(cells []shardservice.Cell, maxBytes uint64) (shardservice.Cell, error) {
	var total exactCount
	for _, cell := range cells {
		if err := addCount(&total, cell, maxBytes); err != nil {
			return shardservice.Cell{}, err
		}
	}
	spelling := total.append(nil)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return shardservice.Cell{}, fmt.Errorf("%w: COUNT state exceeds aggregate byte cap", ErrMergeAggregate)
	}
	return shardservice.Cell{Bytes: spelling}, nil
}

func addCount(total *exactCount, cell shardservice.Cell, maxBytes uint64) error {
	if cell.Null {
		return fmt.Errorf("%w: COUNT state is null", ErrMergeAggregate)
	}
	spelling := bytes.TrimSpace(cell.Bytes)
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return fmt.Errorf("%w: COUNT state exceeds aggregate byte cap", ErrMergeAggregate)
	}
	if count, ok := (vibejson.RawValue{Src: spelling}).Uint64(); ok {
		total.add(count)
		return nil
	}
	var count big.Int
	if !isPlainJSONInteger(spelling) || !vibejson.Valid(spelling) {
		return fmt.Errorf("%w: COUNT state %q is not a non-negative integer", ErrMergeAggregate, cell.Bytes)
	}
	if _, ok := count.SetString(byteview.String(spelling), 10); !ok || count.Sign() < 0 {
		return fmt.Errorf("%w: COUNT state %q is not a non-negative integer", ErrMergeAggregate, cell.Bytes)
	}
	total.addBig(&count)
	return nil
}

func mergeSums(cells []shardservice.Cell, maxBytes uint64) (shardservice.Cell, error) {
	var total exactSum
	var scratch []byte
	for _, cell := range cells {
		var err error
		scratch, err = total.add(cell, maxBytes, scratch[:0])
		if err != nil {
			return shardservice.Cell{}, err
		}
	}
	if !total.set {
		return shardservice.Cell{Null: true}, nil
	}
	if !total.fraction {
		spelling := total.integer.append(nil)
		if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
			return shardservice.Cell{}, fmt.Errorf("%w: SUM state exceeds aggregate byte cap", ErrMergeAggregate)
		}
		return shardservice.Cell{Bytes: spelling}, nil
	}
	spelling, err := exactDecimalString(&total.rational, maxBytes)
	if err != nil {
		return shardservice.Cell{}, err
	}
	return shardservice.Cell{Bytes: []byte(spelling)}, nil
}

func mergeExtrema(cells []shardservice.Cell, maximum bool) (shardservice.Cell, error) {
	var state groupedExtreme
	for _, cell := range cells {
		if err := addExtreme(&state, cell, maximum); err != nil {
			return shardservice.Cell{}, err
		}
	}
	if !state.set {
		return shardservice.Cell{Null: true}, nil
	}
	return state.cell, nil
}

func addExtreme(state *groupedExtreme, cell shardservice.Cell, maximum bool) error {
	if cell.Null {
		return nil
	}
	candidate := classifyCell(cell)
	if candidate.kind != ckNumber || !candidate.valid {
		return fmt.Errorf("%w: MIN/MAX state %q is not numeric", ErrMergeAggregate, cell.Bytes)
	}
	if !state.set {
		state.cell, state.number, state.set = cell, candidate.num, true
		return nil
	}
	comparison := queryengine.CompareValidatedJSONNumbers(candidate.num, state.number)
	if (maximum && comparison > 0) || (!maximum && comparison < 0) {
		state.cell, state.number = cell, candidate.num
	}
	return nil
}

func exactDecimalString(value *big.Rat, maxBytes uint64) (string, error) {
	denominator := new(big.Int).Set(value.Denom())
	twos, fives := 0, 0
	for denominator.Bit(0) == 0 {
		denominator.Rsh(denominator, 1)
		twos++
	}
	five := big.NewInt(5)
	var remainder big.Int
	for {
		remainder.Mod(denominator, five)
		if remainder.Sign() != 0 {
			break
		}
		denominator.Quo(denominator, five)
		fives++
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return "", fmt.Errorf("%w: SUM state is not a finite decimal", ErrMergeAggregate)
	}
	scale := max(twos, fives)
	coefficient := new(big.Int).Set(value.Num())
	if twos < scale {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(scale-twos)), nil))
	}
	if fives < scale {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(scale-fives)), nil))
	}
	negative := coefficient.Sign() < 0
	coefficient.Abs(coefficient)
	digits := coefficient.String()
	needed := len(digits)
	if scale >= len(digits) {
		needed = scale + 2
	} else if scale != 0 {
		needed++
	}
	if negative {
		needed++
	}
	if maxBytes != 0 && uint64(needed) > maxBytes {
		return "", fmt.Errorf("%w: SUM state exceeds aggregate byte cap", ErrMergeAggregate)
	}
	if scale == 0 {
		if negative && digits != "0" {
			return "-" + digits, nil
		}
		return digits, nil
	}
	if scale >= len(digits) {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	point := len(digits) - scale
	digits = digits[:point] + "." + digits[point:]
	digits = strings.TrimRight(strings.TrimRight(digits, "0"), ".")
	if negative && digits != "0" {
		digits = "-" + digits
	}
	return digits, nil
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
	h := &mergeHeap{order: order}
	total := 0
	for idx, resp := range results {
		if len(resp.Rows) == 0 {
			continue
		}
		keys := make([][]cellValue, len(resp.Rows))
		for r, row := range resp.Rows {
			kv := make([]cellValue, len(order))
			for k := range order {
				col := order[k].Column
				if col < 0 || col >= len(row) {
					return nil, ErrMergeColumn
				}
				kv[k] = classifyCell(row[col])
				if !kv[k].valid {
					return nil, fmt.Errorf("%w: shard %d row %d column %d", ErrMergeValue, idx, r, col)
				}
			}
			keys[r] = kv
		}
		h.runs = append(h.runs, &shardRun{rows: resp.Rows, keys: keys, idx: idx})
		total += len(resp.Rows)
	}
	heap.Init(h)

	capacity := total
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	out := make([][]shardservice.Cell, 0, capacity)
	for h.Len() > 0 {
		top := h.runs[0]
		out = append(out, top.rows[top.pos])
		if limit > 0 && len(out) >= limit {
			break
		}
		top.pos++
		if top.pos < len(top.rows) {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
	return out, nil
}
