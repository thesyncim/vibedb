package gateway

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	queryplanner "github.com/thesyncim/vibedb/planner"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
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

// cellValue is one decoded sort-key value: its kind and just enough of its
// content to compare exactly. A number keeps its spelling and an int64 fast path.
type cellValue struct {
	kind  uint8
	bval  bool
	isInt bool
	ival  int64
	num   string
	sval  string
	raw   []byte
}

// classifyCell decodes one wire cell into a comparable value under the query's
// total order. An explicit null cell and a null literal are one value.
func classifyCell(c shardservice.Cell) cellValue {
	if c.Null {
		return cellValue{kind: ckNull}
	}
	b := bytes.TrimSpace(c.Bytes)
	if len(b) == 0 {
		return cellValue{kind: ckNull}
	}
	switch b[0] {
	case 'n':
		return cellValue{kind: ckNull}
	case 't':
		return cellValue{kind: ckBool, bval: true}
	case 'f':
		return cellValue{kind: ckBool, bval: false}
	case '"':
		var s string
		if err := json.Unmarshal(b, &s); err == nil {
			return cellValue{kind: ckString, sval: s}
		}
		return cellValue{kind: ckString, sval: string(b)}
	case '{', '[':
		return cellValue{kind: ckContainer, raw: b}
	default:
		v := cellValue{kind: ckNumber, num: string(b)}
		if i, err := strconv.ParseInt(v.num, 10, 64); err == nil {
			v.isInt, v.ival = true, i
		}
		return v
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
		return strings.Compare(a.sval, b.sval)
	default:
		return bytes.Compare(a.raw, b.raw)
	}
}

// compareNumberSpelling compares two JSON numbers without expanding their
// decimal exponents. A short hostile value such as 1e1000000000 therefore has
// work proportional to its spelling rather than its mathematical magnitude.
func compareNumberSpelling(a, b string) int {
	ca, erra := queryplanner.CanonicalScalarJSON(a)
	cb, errb := queryplanner.CanonicalScalarJSON(b)
	if erra != nil || errb != nil {
		return strings.Compare(a, b)
	}
	comparison, err := queryplanner.CompareCanonicalScalarJSON(ca, cb)
	if err != nil {
		return strings.Compare(a, b)
	}
	return comparison
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

func mergeCounts(cells []shardservice.Cell, maxBytes uint64) (shardservice.Cell, error) {
	var total big.Int
	for _, cell := range cells {
		if cell.Null {
			return shardservice.Cell{}, fmt.Errorf("%w: COUNT state is null", ErrMergeAggregate)
		}
		var count big.Int
		if _, ok := count.SetString(string(bytes.TrimSpace(cell.Bytes)), 10); !ok || count.Sign() < 0 {
			return shardservice.Cell{}, fmt.Errorf("%w: COUNT state %q is not a non-negative integer", ErrMergeAggregate, cell.Bytes)
		}
		total.Add(&total, &count)
	}
	spelling := total.String()
	if maxBytes != 0 && uint64(len(spelling)) > maxBytes {
		return shardservice.Cell{}, fmt.Errorf("%w: COUNT state exceeds aggregate byte cap", ErrMergeAggregate)
	}
	return shardservice.Cell{Bytes: []byte(spelling)}, nil
}

func mergeSums(cells []shardservice.Cell, maxBytes uint64) (shardservice.Cell, error) {
	var total big.Rat
	hasValue := false
	for _, cell := range cells {
		if cell.Null {
			continue
		}
		spelling := string(bytes.TrimSpace(cell.Bytes))
		canonical, canonicalErr := queryplanner.CanonicalScalarJSON(spelling)
		if canonicalErr != nil || !queryplanner.CanonicalNumberFitsDecimalBytes(canonical, maxBytes) {
			return shardservice.Cell{}, fmt.Errorf("%w: SUM state %q exceeds exact numeric admission", ErrMergeAggregate, cell.Bytes)
		}
		value, ok := new(big.Rat).SetString(canonical)
		if !ok {
			return shardservice.Cell{}, fmt.Errorf("%w: SUM state %q is not an exact number", ErrMergeAggregate, cell.Bytes)
		}
		total.Add(&total, value)
		hasValue = true
	}
	if !hasValue {
		return shardservice.Cell{Null: true}, nil
	}
	spelling, err := exactDecimalString(&total, maxBytes)
	if err != nil {
		return shardservice.Cell{}, err
	}
	return shardservice.Cell{Bytes: []byte(spelling)}, nil
}

func mergeExtrema(cells []shardservice.Cell, maximum bool) (shardservice.Cell, error) {
	var chosen shardservice.Cell
	chosenCanonical := ""
	hasValue := false
	for _, cell := range cells {
		if cell.Null {
			continue
		}
		candidate := classifyCell(cell)
		if candidate.kind != ckNumber {
			return shardservice.Cell{}, fmt.Errorf("%w: MIN/MAX state %q is not numeric", ErrMergeAggregate, cell.Bytes)
		}
		canonical, err := queryplanner.CanonicalScalarJSON(string(bytes.TrimSpace(cell.Bytes)))
		if err != nil || len(canonical) == 0 || canonical[0] == '"' || canonical == "true" || canonical == "false" || canonical == "null" {
			return shardservice.Cell{}, fmt.Errorf("%w: MIN/MAX state %q is not an exact JSON number", ErrMergeAggregate, cell.Bytes)
		}
		if !hasValue {
			chosen, chosenCanonical, hasValue = cell, canonical, true
			continue
		}
		comparison, err := queryplanner.CompareCanonicalScalarJSON(canonical, chosenCanonical)
		if err != nil {
			return shardservice.Cell{}, fmt.Errorf("%w: MIN/MAX state comparison: %v", ErrMergeAggregate, err)
		}
		if (maximum && comparison > 0) || (!maximum && comparison < 0) {
			chosen, chosenCanonical = cell, canonical
		}
	}
	if !hasValue {
		return shardservice.Cell{Null: true}, nil
	}
	return chosen, nil
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
