package gateway

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/shardservice"
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

// compareNumberSpelling compares two JSON number spellings by exact decimal
// value via math/big, never by rounding through float64, so equal decimals of
// any spelling compare equal. A spelling big.Rat cannot parse falls back to a
// lexical compare, which cannot occur for validated JSON numbers.
func compareNumberSpelling(a, b string) int {
	ra, oka := new(big.Rat).SetString(a)
	rb, okb := new(big.Rat).SetString(b)
	if oka && okb {
		return ra.Cmp(rb)
	}
	return strings.Compare(a, b)
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
	columns := results[0].Columns
	for _, resp := range results {
		if resp == nil || resp.Kind != shardservice.ResponseRows {
			return nil, nil, ErrUnmergeableResult
		}
		if len(resp.Columns) != len(columns) {
			return nil, nil, ErrMergeSchema
		}
		for i := range columns {
			if resp.Columns[i] != columns[i] {
				return nil, nil, ErrMergeSchema
			}
		}
		for _, row := range resp.Rows {
			if len(row) != len(columns) {
				return nil, nil, ErrMergeSchema
			}
		}
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
