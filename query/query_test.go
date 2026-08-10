package query

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Given a small Segment and a battery of query shapes, when the compiled
// executor runs, then every column-oriented Result agrees with a naive
// reference executor that decodes each document with encoding/json and
// evaluates the query in plain Go, under the portable and SIMD backends.
//
// This is an exhaustive differential over a bounded domain, not a proof: the
// pool of documents, the sequence lengths, the four storage modes, and the
// query battery are all finite and small, and TestExhaustiveQueryDifferential
// reports the (segment × storage × query) count it covered as evidence. The
// reference is deliberately independent — a different decode path
// (encoding/json with json.Number), a different number comparator (math/big),
// and a different grouping strategy (linear search) — so agreement is a real
// cross-check of the tape-scan executor's exact-decimal comparisons, its group
// partition, and its stable ordering.

// --- fixtures -------------------------------------------------------------

// docPool is the bounded document domain: object roots with subsets of a small
// field set taking scalar values including repeats, nulls, strings, numbers,
// and bools; duplicate keys; empty objects; and non-object roots. Every query
// path in the exhaustive battery is a single top-level field, so array
// indexing never enters and the reference's field resolution is a plain map
// lookup.
var docPool = [][]byte{
	[]byte(`{}`),
	[]byte(`{"a":1}`),
	[]byte(`{"a":2,"b":1}`),
	[]byte(`{"a":null,"b":"x"}`),
	[]byte(`{"a":true,"b":false,"c":0}`),
	[]byte(`{"a":"x","b":2,"c":3}`),
	[]byte(`{"a":1,"a":2}`), // duplicate key: last wins
	[]byte(`[1,2,3]`),       // non-object root: every field is absent
}

// storageMode toggles the two Segment storage options the extractors have
// distinct paths for, so the battery runs against each tape form.
type storageMode struct {
	name       string
	hashKeys   bool
	shapeTapes bool
}

var storageModes = []storageMode{
	{"plain", false, false},
	{"hashed", true, false},
	{"shaped", false, true},
	{"hashed+shaped", true, true},
}

func buildSegment(t testing.TB, docs [][]byte, mode storageMode) *store.Segment {
	t.Helper()
	set := &store.Segment{}
	set.Options = document.IndexOptions{HashKeys: mode.hashKeys}
	set.ShapeTapes = mode.shapeTapes
	for _, d := range docs {
		if _, err := set.Append(d); err != nil {
			t.Fatalf("Append(%s): %v", d, err)
		}
	}
	return set
}

// queryBattery returns the compiled-once query shapes exercised against every
// segment: one per projection, aggregate, predicate, group-by, order-by, and
// limit form of interest. Each is reused across every segment and storage mode,
// exercising the compile-once/run-many contract.
func queryBattery() []*Query {
	fields := []string{"a", "b", "c"}
	var qs []*Query

	for _, f := range fields {
		qs = append(qs, Select(Path(f)))
		qs = append(qs, Select(Count(f)))
		qs = append(qs, Select(Sum(f)))
		qs = append(qs, Select(Avg(f)))
		qs = append(qs, Select(Min(f)))
		qs = append(qs, Select(Max(f)))
	}
	qs = append(qs,
		Select(Path("a"), Path("b")),
		Select(Path("a"), Path("b"), Path("c")),
		Select(Count()),
		Select(Count(), Sum("a"), Avg("b"), Min("c"), Max("a")),
	)

	// WHERE over every operator, literal type, and combinator, paired with a
	// projection or an aggregate so both the filter and the reduction are
	// checked.
	preds := []Predicate{
		Cmp("a", Eq, 1),
		Cmp("a", Ne, 1),
		Cmp("a", Lt, 2),
		Cmp("a", Le, 1),
		Cmp("a", Gt, 0),
		Cmp("a", Ge, 2),
		Cmp("a", Eq, "x"),
		Cmp("a", Eq, true),
		Cmp("b", Gt, 1),
		Exists("a"),
		Not(Exists("a")),
		IsNull("a"),
		Not(IsNull("a")),
		And(Exists("a"), Cmp("b", Ge, 1)),
		Or(Cmp("a", Eq, 1), Cmp("a", Eq, "x")),
		And(Not(IsNull("a")), Or(Cmp("b", Lt, 2), Exists("c"))),
		// Memberships, and the disjunctions that compile into them. The
		// reference evaluates the builder tree, so a rewrite that changed
		// which rows are accepted would show up here as a disagreement.
		In("a", 1),
		In("a", 1, 2),
		In("a", 1, "x", true),
		In("a", 2, 2, 1), // duplicates and disorder must not matter
		In("a"),          // an empty membership accepts nothing
		Not(In("a", 1, 2)),
		Or(In("a", 1), In("b", 2)),
		And(In("a", 1, 2), Exists("b")),
		Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Cmp("a", Eq, true)),               // fully coalesced
		Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Cmp("b", Eq, 1)),                  // partially coalesced
		Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Exists("c")),                      // coalesced beside a non-equality
		Or(Cmp("a", Eq, 1), Cmp("a", Eq, 2), Cmp("b", Gt, 0)),                  // coalesced beside an inequality
		Or(Cmp("a", Eq, 1), Cmp("b", Eq, 1), Cmp("a", Eq, 2), Cmp("b", Eq, 2)), // two memberships
	}
	for _, p := range preds {
		qs = append(qs, Select(Path("a"), Path("b")).Where(p))
		qs = append(qs, Select(Count(), Sum("a")).Where(p))
	}

	// GROUP BY, with and without WHERE, ordering, and limits.
	qs = append(qs,
		Select(Path("a"), Count()).GroupBy("a"),
		Select(Path("a"), Sum("b"), Avg("b"), Min("b"), Max("b"), Count("b")).GroupBy("a"),
		Select(Path("a"), Path("b"), Count()).GroupBy("a", "b"),
		Select(Path("a"), Count()).Where(Exists("a")).GroupBy("a"),
		Select(Path("a"), Count()).GroupBy("a").OrderBy("a", Asc),
		Select(Path("a"), Count()).GroupBy("a").OrderBy("a", Desc),
		Select(Path("a"), Sum("b")).GroupBy("a").OrderBy("a", Asc).Limit(2),
	)

	// ORDER BY and LIMIT over plain projections.
	qs = append(qs,
		Select(Path("a"), Path("b")).OrderBy("a", Asc),
		Select(Path("a"), Path("b")).OrderBy("a", Desc),
		Select(Path("a")).OrderBy("a", Asc).OrderBy("b", Desc),
		Select(Path("a")).OrderBy("a", Asc).Limit(2),
		Select(Path("a")).Where(Exists("a")).OrderBy("a", Asc).Limit(3),
	)
	return qs
}

// --- the exhaustive differential ------------------------------------------

func TestExhaustiveQueryDifferential(t *testing.T) {
	segments := enumerateSegments(docPool, 3)
	battery := queryBattery()

	pairs := 0
	for _, docs := range segments {
		decoded := decodeDocs(t, docs)
		for _, mode := range storageModes {
			set := buildSegment(t, docs, mode)
			for qi, q := range battery {
				got, err := q.Run(FromSegment(set))
				if err != nil {
					t.Fatalf("query %d %s over %s: Run: %v", qi, mode.name, docs, err)
				}
				want := referenceRun(t, q, decoded)
				if diff := compareResults(got, want); diff != "" {
					t.Fatalf("query %d %s over %v: %s", qi, mode.name, jsonStrings(docs), diff)
				}
				pairs++
			}
		}
	}
	t.Logf("exhaustive differential: %d segments × %d storage modes × %d queries = %d (segment × storage × query) checks",
		len(segments), len(storageModes), len(battery), pairs)
}

// enumerateSegments returns every ordered document sequence of length 1..maxLen
// drawn from pool, the bounded family of Segments the battery runs against.
func enumerateSegments(pool [][]byte, maxLen int) [][][]byte {
	var out [][][]byte
	var rec func(prefix [][]byte)
	rec = func(prefix [][]byte) {
		if len(prefix) > 0 {
			cp := make([][]byte, len(prefix))
			copy(cp, prefix)
			out = append(out, cp)
		}
		if len(prefix) == maxLen {
			return
		}
		for _, d := range pool {
			rec(append(prefix, d))
		}
	}
	rec(nil)
	return out
}

// --- naive reference executor ---------------------------------------------

func decodeDocs(t testing.TB, docs [][]byte) []any {
	t.Helper()
	out := make([]any, len(docs))
	for i, d := range docs {
		dec := json.NewDecoder(strings.NewReader(string(d)))
		dec.UseNumber()
		if err := dec.Decode(&out[i]); err != nil {
			t.Fatalf("decode %s: %v", d, err)
		}
	}
	return out
}

// referenceRun evaluates q's semantics over decoded documents independently of
// the executor: encoding/json values, math/big number comparison, and linear
// grouping.
func referenceRun(t testing.TB, q *Query, docs []any) refResult {
	return referenceRunJoined(q, docs, nil)
}

// referenceRunJoined is referenceRun with an extra row filter, the seam
// join_test.go plugs its naive nested-loop semi-join into. It is a parameter
// rather than a second copy of the dispatch below so that the join differential
// and the query differential are checking against literally the same reference
// projection, aggregation, grouping, ordering, and limiting.
func referenceRunJoined(q *Query, docs []any, joined func(doc any) bool) refResult {
	grouped := len(q.groupBy) > 0
	hasAgg := false
	for _, c := range q.columns {
		if c.isAggregate() {
			hasAgg = true
		}
	}
	singleRow := hasAgg && !grouped

	var selected []int
	for i := range docs {
		if q.hasWhere && !refEval(q.where, docs[i]) {
			continue
		}
		if joined != nil && !joined(docs[i]) {
			continue
		}
		selected = append(selected, i)
	}

	headers := make([]string, len(q.columns))
	for i, c := range q.columns {
		headers[i] = c.header
	}

	switch {
	case grouped:
		return refGrouped(q, docs, selected, headers)
	case singleRow:
		return refAggregate(q, docs, selected, headers)
	default:
		return refProjection(q, docs, selected, headers)
	}
}

type refResult struct {
	headers []string
	rows    [][]refCell
}

type refCell struct {
	kind ValueType
	b    bool
	num  string // exact spelling for a projected number
	numF float64
	agg  bool // computed number (compare by float64)
	s    string
	raw  []byte
}

func refProjection(q *Query, docs []any, selected []int, headers []string) refResult {
	res := refResult{headers: headers}
	type row struct {
		cells []refCell
		order []refScalar
	}
	rows := make([]row, 0, len(selected))
	for _, i := range selected {
		cells := make([]refCell, len(q.columns))
		for c, col := range q.columns {
			cells[c] = refCellFromScalar(refClassify(refResolve(col.spec, docs[i])))
		}
		rows = append(rows, row{cells: cells, order: refOrderKeys(q, docs[i])})
	}
	idx := stableOrder(len(rows), func(a, b int) int { return refCompareOrder(q, rows[a].order, rows[b].order) })
	idx = applyLimit(q, idx)
	for _, i := range idx {
		res.rows = append(res.rows, rows[i].cells)
	}
	return res
}

func refAggregate(q *Query, docs []any, selected []int, headers []string) refResult {
	accs := make([]refAcc, len(q.columns))
	for _, i := range selected {
		refAccumulate(q, accs, docs[i])
	}
	res := refResult{headers: headers}
	cells := refAggregateCells(q, accs, nil)
	idx := applyLimit(q, []int{0})
	if len(idx) == 1 {
		res.rows = append(res.rows, cells)
	}
	return res
}

func refGrouped(q *Query, docs []any, selected []int, headers []string) refResult {
	type grp struct {
		keys []refScalar // parallel to q.groupBy
		accs []refAcc
	}
	var groups []*grp
	findGroup := func(keys []refScalar) *grp {
		for _, g := range groups {
			same := true
			for k := range keys {
				if refCompare(g.keys[k], keys[k]) != 0 {
					same = false
					break
				}
			}
			if same {
				return g
			}
		}
		g := &grp{keys: keys, accs: make([]refAcc, len(q.columns))}
		groups = append(groups, g)
		return g
	}
	for _, i := range selected {
		keys := make([]refScalar, len(q.groupBy))
		for k, gp := range q.groupBy {
			keys[k] = refClassify(refResolve(gp, docs[i]))
		}
		g := findGroup(keys)
		refAccumulate(q, g.accs, docs[i])
	}

	res := refResult{headers: headers}
	type row struct {
		cells []refCell
		order []refScalar
	}
	var rows []row
	for _, g := range groups {
		cells := refAggregateCellsGrouped(q, g.accs, g.keys)
		order := make([]refScalar, len(q.orderBy))
		for oi, o := range q.orderBy {
			order[oi] = g.keys[groupIndex(q, o.path)]
		}
		rows = append(rows, row{cells: cells, order: order})
	}
	idx := stableOrder(len(rows), func(a, b int) int { return refCompareOrder(q, rows[a].order, rows[b].order) })
	idx = applyLimit(q, idx)
	for _, i := range idx {
		res.rows = append(res.rows, rows[i].cells)
	}
	return res
}

// refAcc mirrors aggAcc.
type refAcc struct {
	count int
	n     int
	sum   float64
	min   float64
	max   float64
}

func refAccumulate(q *Query, accs []refAcc, doc any) {
	for c, col := range q.columns {
		switch col.agg {
		case aggCount:
			value, resolved := refResolve(col.spec, doc)
			if col.spec == "" || resolved && value != nil {
				accs[c].count++
			}
		case aggSum, aggAvg, aggMin, aggMax:
			f, ok := refNumber(refResolve(col.spec, doc))
			if !ok {
				continue
			}
			a := &accs[c]
			if a.n == 0 {
				a.min, a.max = f, f
			} else {
				if f < a.min {
					a.min = f
				}
				if f > a.max {
					a.max = f
				}
			}
			a.sum += f
			a.n++
		}
	}
}

func refAggregateCells(q *Query, accs []refAcc, _ []refScalar) []refCell {
	cells := make([]refCell, len(q.columns))
	for c, col := range q.columns {
		cells[c] = refAggCell(col, accs[c])
	}
	return cells
}

func refAggregateCellsGrouped(q *Query, accs []refAcc, keys []refScalar) []refCell {
	cells := make([]refCell, len(q.columns))
	for c, col := range q.columns {
		if col.agg == aggNone {
			cells[c] = refCellFromScalar(keys[groupIndex(q, col.spec)])
			continue
		}
		cells[c] = refAggCell(col, accs[c])
	}
	return cells
}

func refAggCell(col Column, a refAcc) refCell {
	switch col.agg {
	case aggCount:
		return refCell{kind: TypeNumber, agg: true, numF: float64(a.count)}
	case aggSum:
		return refNumericOrNull(a.n, a.sum)
	case aggAvg:
		if a.n == 0 {
			return refCell{kind: TypeNull}
		}
		return refCell{kind: TypeNumber, agg: true, numF: a.sum / float64(a.n)}
	case aggMin:
		return refNumericOrNull(a.n, a.min)
	default: // aggMax
		return refNumericOrNull(a.n, a.max)
	}
}

func refNumericOrNull(n int, v float64) refCell {
	if n == 0 {
		return refCell{kind: TypeNull}
	}
	return refCell{kind: TypeNumber, agg: true, numF: v}
}

func groupIndex(q *Query, path string) int {
	for i, g := range q.groupBy {
		if g == path {
			return i
		}
	}
	return 0
}

func refOrderKeys(q *Query, doc any) []refScalar {
	keys := make([]refScalar, len(q.orderBy))
	for i, o := range q.orderBy {
		keys[i] = refClassify(refResolve(o.path, doc))
	}
	return keys
}

func refCompareOrder(q *Query, a, b []refScalar) int {
	for i, o := range q.orderBy {
		c := refCompare(a[i], b[i])
		if o.dir == Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func applyLimit(q *Query, idx []int) []int {
	if q.hasLimit && len(idx) > q.limit {
		return idx[:q.limit]
	}
	return idx
}

// stableOrder returns the indices 0..n-1 stably sorted by cmp.
func stableOrder(n int, cmp func(a, b int) int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	// insertion sort: stable and adequate for the small reference sets.
	for i := 1; i < n; i++ {
		for j := i; j > 0 && cmp(idx[j-1], idx[j]) > 0; j-- {
			idx[j-1], idx[j] = idx[j], idx[j-1]
		}
	}
	return idx
}

// --- reference scalar model ------------------------------------------------

type refScalar struct {
	kind    scalarKind
	present bool
	b       bool
	num     string
	s       string
	raw     []byte
}

func refResolve(spec string, doc any) (any, bool) {
	if spec == "" {
		return doc, true
	}
	if spec[0] == '/' {
		return refResolvePointer(strings.Split(spec[1:], "/"), doc, true)
	}
	segs := strings.Split(spec, ".")
	if len(segs) == 1 {
		m, ok := doc.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[spec]
		return v, ok
	}
	return refResolvePointer(segs, doc, false)
}

func refResolvePointer(tokens []string, doc any, escaped bool) (any, bool) {
	cur := doc
	for _, tok := range tokens {
		if escaped {
			tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		}
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[tok]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			idx, err := strconv.Atoi(tok)
			if err != nil || idx < 0 || idx >= len(c) {
				return nil, false
			}
			cur = c[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

func refClassify(v any, present bool) refScalar {
	if !present {
		return refScalar{kind: kindNull, present: false}
	}
	switch x := v.(type) {
	case nil:
		return refScalar{kind: kindNull, present: true}
	case bool:
		return refScalar{kind: kindBool, present: true, b: x}
	case json.Number:
		return refScalar{kind: kindNumber, present: true, num: string(x)}
	case string:
		return refScalar{kind: kindString, present: true, s: x}
	default:
		raw, _ := json.Marshal(x)
		return refScalar{kind: kindContainer, present: true, raw: raw}
	}
}

func refPresent(v any, present bool) bool { return present }

func refNumber(v any, present bool) (float64, bool) {
	if !present {
		return 0, false
	}
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	f, err := n.Float64()
	if err != nil {
		return 0, false
	}
	return f, true
}

func refCompare(a, b refScalar) int {
	if a.kind != b.kind {
		if a.kind < b.kind {
			return -1
		}
		return 1
	}
	switch a.kind {
	case kindNull:
		return 0
	case kindBool:
		switch {
		case a.b == b.b:
			return 0
		case !a.b:
			return -1
		default:
			return 1
		}
	case kindNumber:
		ra := ratOf(a.num)
		rb := ratOf(b.num)
		return ra.Cmp(rb)
	case kindString:
		return strings.Compare(a.s, b.s)
	default:
		return strings.Compare(string(a.raw), string(b.raw))
	}
}

func ratOf(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("reference: bad number " + s)
	}
	return r
}

func refEval(p Predicate, doc any) bool {
	switch p.kind {
	case predCmp:
		cell := refClassify(refResolve(p.path, doc))
		if cell.kind == kindNull {
			return false
		}
		lit := refClassify(refLiteralValue(p.value))
		c := refCompare(cell, lit)
		switch p.op {
		case Eq:
			return c == 0
		case Ne:
			return c != 0
		case Lt:
			return c < 0
		case Le:
			return c <= 0
		case Gt:
			return c > 0
		default:
			return c >= 0
		}
	case predIn:
		// The reference walks the alternatives linearly and compares with its
		// own comparator, so it is a genuine independent check of both the
		// compiled binary search and the OR-to-membership rewrite.
		cell := refClassify(refResolve(p.path, doc))
		if cell.kind == kindNull {
			return false
		}
		for _, v := range p.values {
			if refCompare(cell, refClassify(refLiteralValue(v))) == 0 {
				return true
			}
		}
		return false
	case predExists:
		return refPresent(refResolve(p.path, doc))
	case predIsNull:
		return refClassify(refResolve(p.path, doc)).kind == kindNull
	case predAnd:
		for _, k := range p.kids {
			if !refEval(k, doc) {
				return false
			}
		}
		return true
	case predOr:
		for _, k := range p.kids {
			if refEval(k, doc) {
				return true
			}
		}
		return false
	case predNot:
		return !refEval(p.kids[0], doc)
	case predContains:
		ok, err := vibejson.RawContains(mustResolveRaw(p.path, doc), []byte(p.json))
		return ok && err == nil
	default:
		return false
	}
}

// mustResolveRaw re-encodes a resolved value to JSON for the containment
// reference; only the targeted containment test reaches it.
func mustResolveRaw(path string, doc any) []byte {
	v, ok := refResolve(path, doc)
	if !ok {
		return nil
	}
	raw, _ := json.Marshal(v)
	return raw
}

func refLiteralValue(value any) (any, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		return v, true
	case int:
		return json.Number(strconv.FormatInt(int64(v), 10)), true
	case int8:
		return json.Number(strconv.FormatInt(int64(v), 10)), true
	case int16:
		return json.Number(strconv.FormatInt(int64(v), 10)), true
	case int32:
		return json.Number(strconv.FormatInt(int64(v), 10)), true
	case int64:
		return json.Number(strconv.FormatInt(v, 10)), true
	case uint, uint8, uint16, uint32, uint64:
		return json.Number(fmt.Sprintf("%d", v)), true
	case float32:
		return json.Number(strconv.FormatFloat(float64(v), 'g', -1, 64)), true
	case float64:
		return json.Number(strconv.FormatFloat(v, 'g', -1, 64)), true
	default:
		return nil, false
	}
}

func refCellFromScalar(s refScalar) refCell {
	switch s.kind {
	case kindNull:
		return refCell{kind: TypeNull}
	case kindBool:
		return refCell{kind: TypeBool, b: s.b}
	case kindNumber:
		f, _ := ratOf(s.num).Float64()
		return refCell{kind: TypeNumber, num: s.num, numF: f}
	case kindString:
		return refCell{kind: TypeString, s: s.s}
	default:
		return refCell{kind: TypeJSON, raw: s.raw}
	}
}

// --- comparison ------------------------------------------------------------

func compareResults(got Result, want refResult) string {
	if len(got.Columns) != len(want.headers) {
		return fmt.Sprintf("column count: got %d want %d", len(got.Columns), len(want.headers))
	}
	for c := range got.Columns {
		if got.Columns[c].Header != want.headers[c] {
			return fmt.Sprintf("column %d header: got %q want %q", c, got.Columns[c].Header, want.headers[c])
		}
	}
	if got.RowCount != len(want.rows) {
		return fmt.Sprintf("row count: got %d want %d\n%s\n%s", got.RowCount, len(want.rows), dumpResult(got), dumpRef(want))
	}
	for r := 0; r < got.RowCount; r++ {
		for c := range got.Columns {
			if diff := compareCell(got.Columns[c].Cells[r], want.rows[r][c]); diff != "" {
				return fmt.Sprintf("row %d col %q: %s\n%s\n%s", r, got.Columns[c].Header, diff, dumpResult(got), dumpRef(want))
			}
		}
	}
	return ""
}

func compareCell(g Cell, w refCell) string {
	if g.Kind() != w.kind {
		return fmt.Sprintf("kind: got %v want %v", g.Kind(), w.kind)
	}
	switch w.kind {
	case TypeNull:
		return ""
	case TypeBool:
		gb, _ := g.Bool()
		if gb != w.b {
			return fmt.Sprintf("bool: got %v want %v", gb, w.b)
		}
	case TypeNumber:
		gf, _ := g.Float64()
		if w.agg {
			if gf != w.numF {
				return fmt.Sprintf("number(agg): got %v want %v", gf, w.numF)
			}
			return ""
		}
		if ratOf(string(g.JSON())).Cmp(ratOf(w.num)) != 0 {
			return fmt.Sprintf("number: got %s want %s", g.JSON(), w.num)
		}
	case TypeString:
		gs, _ := g.Text()
		if gs != w.s {
			return fmt.Sprintf("string: got %q want %q", gs, w.s)
		}
	case TypeJSON:
		if !jsonEqual(g.JSON(), w.raw) {
			return fmt.Sprintf("json: got %s want %s", g.JSON(), w.raw)
		}
	}
	return ""
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xb) == string(yb)
}

func dumpResult(r Result) string {
	var b strings.Builder
	b.WriteString("got: ")
	for c := range r.Columns {
		fmt.Fprintf(&b, "[%s]", r.Columns[c].Header)
	}
	b.WriteByte('\n')
	for row := 0; row < r.RowCount; row++ {
		for c := range r.Columns {
			fmt.Fprintf(&b, "%s ", r.Columns[c].Cells[row])
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func dumpRef(r refResult) string {
	var b strings.Builder
	b.WriteString("want:\n")
	for _, row := range r.rows {
		for _, c := range row {
			switch c.kind {
			case TypeNumber:
				if c.agg {
					fmt.Fprintf(&b, "%v ", c.numF)
				} else {
					fmt.Fprintf(&b, "%s ", c.num)
				}
			case TypeString:
				fmt.Fprintf(&b, "%q ", c.s)
			case TypeBool:
				fmt.Fprintf(&b, "%v ", c.b)
			case TypeNull:
				b.WriteString("null ")
			default:
				fmt.Fprintf(&b, "%s ", c.raw)
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func jsonStrings(docs [][]byte) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = string(d)
	}
	return out
}

// --- exact decimal comparator ---------------------------------------------

// Given two validated JSON number spellings, when compareNumberBytes orders
// them, then the sign agrees with math/big's exact rational comparison, and
// two spellings share a group key exactly when they are equal — the invariant
// that keeps WHERE, GROUP BY, and ORDER BY consistent past float64's mantissa.
func TestNumberOrderingMatchesBigRat(t *testing.T) {
	spellings := []string{
		"0", "-0", "0.0", "0e5",
		"1", "-1", "1.0", "-1.0", "10e-1", "100e-2", "0.1e1",
		"2", "1.5", "-1.5", "15e-1", "3.14", "314e-2",
		"100", "1e2", "1000", "10000e-1", "0.001", "1e-3",
		"9007199254740992", "9007199254740993", "-9007199254740993",
		"123.45", "12345e-2", "123456789012345678", "1.23456789012345678e17",
	}
	for _, a := range spellings {
		for _, b := range spellings {
			gotSign := sign(compareNumberBytes([]byte(a), []byte(b)))
			wantSign := ratOf(a).Cmp(ratOf(b))
			if gotSign != wantSign {
				t.Fatalf("compareNumberBytes(%s,%s)=%d want %d", a, b, gotSign, wantSign)
			}
			ka := string(appendNumberKey(nil, []byte(a)))
			kb := string(appendNumberKey(nil, []byte(b)))
			if (ka == kb) != (wantSign == 0) {
				t.Fatalf("group key equality for (%s,%s): keysEqual=%v valueEqual=%v", a, b, ka == kb, wantSign == 0)
			}
		}
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}

// --- targeted edges and defined semantics ---------------------------------

func mustSegment(t testing.TB, docs ...string) *store.Segment {
	t.Helper()
	set := &store.Segment{}
	for _, d := range docs {
		if _, err := set.Append([]byte(d)); err != nil {
			t.Fatalf("Append(%s): %v", d, err)
		}
	}
	return set
}

func mustRun(t testing.TB, q *Query, set *store.Segment) Result {
	t.Helper()
	r, err := q.Run(FromSegment(set))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return r
}

func floatCol(t testing.TB, r Result, header string) []float64 {
	t.Helper()
	col, ok := r.Column(header)
	if !ok {
		t.Fatalf("no column %q", header)
	}
	out := make([]float64, len(col.Cells))
	for i, c := range col.Cells {
		f, ok := c.Float64()
		if !ok {
			t.Fatalf("column %q row %d not numeric: %s", header, i, c)
		}
		out[i] = f
	}
	return out
}

func TestQueryEmptySet(t *testing.T) {
	set := mustSegment(t)
	if got := mustRun(t, Select(Path("a")), set); got.RowCount != 0 {
		t.Fatalf("projection over empty set: RowCount=%d want 0", got.RowCount)
	}
	agg := mustRun(t, Select(Count(), Sum("a"), Avg("a"), Min("a"), Max("a")), set)
	if agg.RowCount != 1 {
		t.Fatalf("aggregate over empty set: RowCount=%d want 1", agg.RowCount)
	}
	if c, _ := agg.Column("count(*)"); !isCountZero(c.Cells[0]) {
		t.Fatalf("count(*) over empty = %s want 0", c.Cells[0])
	}
	for _, h := range []string{"sum(a)", "avg(a)", "min(a)", "max(a)"} {
		c, _ := agg.Column(h)
		if !c.Cells[0].IsNull() {
			t.Fatalf("%s over empty = %s want null", h, c.Cells[0])
		}
	}
}

func isCountZero(c Cell) bool {
	n, ok := c.Int64()
	return ok && n == 0
}

func TestQueryAllFiltered(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":2}`, `{"a":3}`)
	filter := Cmp("a", Gt, 100)
	if got := mustRun(t, Select(Path("a")).Where(filter), set); got.RowCount != 0 {
		t.Fatalf("all-filtered projection: RowCount=%d want 0", got.RowCount)
	}
	agg := mustRun(t, Select(Count(), Sum("a")).Where(filter), set)
	if c, _ := agg.Column("count(*)"); !isCountZero(c.Cells[0]) {
		t.Fatalf("count over all-filtered = %s want 0", c.Cells[0])
	}
	if c, _ := agg.Column("sum(a)"); !c.Cells[0].IsNull() {
		t.Fatalf("sum over all-filtered = %s want null", c.Cells[0])
	}
}

func TestQueryOrderByStability(t *testing.T) {
	set := mustSegment(t, `{"a":1,"id":0}`, `{"a":1,"id":1}`, `{"a":1,"id":2}`, `{"a":1,"id":3}`)
	for _, dir := range []Direction{Asc, Desc} {
		got := mustRun(t, Select(Path("id")).OrderBy("a", dir), set)
		ids := floatCol(t, got, "id")
		for i, v := range ids {
			if v != float64(i) {
				t.Fatalf("dir=%v tie order not stable: %v", dir, ids)
			}
		}
	}
}

func TestQueryLimit(t *testing.T) {
	set := mustSegment(t, `{"a":5}`, `{"a":3}`, `{"a":1}`, `{"a":4}`, `{"a":2}`)
	base := func(n int) *Query { return Select(Path("a")).OrderBy("a", Asc).Limit(n) }
	if got := floatCol(t, mustRun(t, base(3), set), "a"); !equalFloats(got, []float64{1, 2, 3}) {
		t.Fatalf("Limit(3)=%v want [1 2 3]", got)
	}
	if got := mustRun(t, base(0), set); got.RowCount != 0 {
		t.Fatalf("Limit(0): RowCount=%d want 0", got.RowCount)
	}
	if got := mustRun(t, base(10), set); got.RowCount != 5 {
		t.Fatalf("Limit(10): RowCount=%d want 5", got.RowCount)
	}
	if got := mustRun(t, Select(Path("a")).OrderBy("a", Asc).Limit(-1), set); got.RowCount != 5 {
		t.Fatalf("Limit(-1): RowCount=%d want 5 (unlimited)", got.RowCount)
	}
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestQueryContains(t *testing.T) {
	set := mustSegment(t,
		`{"tags":["a","b","c"]}`,
		`{"tags":["a"]}`,
		`{"tags":["x"]}`,
		`{"tags":"nope"}`,
		`{"m":{"x":1,"y":2}}`,
	)
	got := mustRun(t, Select(Count()).Where(Contains("tags", `["a","b"]`)), set)
	if c, _ := got.Column("count(*)"); !countIs(c.Cells[0], 1) {
		t.Fatalf("array containment count = %s want 1", c.Cells[0])
	}
	obj := mustRun(t, Select(Count()).Where(Contains("m", `{"x":1}`)), set)
	if c, _ := obj.Column("count(*)"); !countIs(c.Cells[0], 1) {
		t.Fatalf("object containment count = %s want 1", c.Cells[0])
	}
}

func countIs(c Cell, want int64) bool {
	n, ok := c.Int64()
	return ok && n == want
}

func TestQueryPointerAndNestedPaths(t *testing.T) {
	set := mustSegment(t,
		`{"user":{"name":"amy","age":30},"xs":[10,20,30]}`,
		`{"user":{"name":"bob","age":25},"xs":[40,50]}`,
	)
	got := mustRun(t, Select(Path("/user/name"), Path("user.age"), Path("/xs/1")), set)
	names, _ := got.Column("/user/name")
	if s, _ := names.Cells[0].Text(); s != "amy" {
		t.Fatalf("pointer name row0 = %q want amy", s)
	}
	if got2 := floatCol(t, got, "user.age"); !equalFloats(got2, []float64{30, 25}) {
		t.Fatalf("dotted nested age = %v want [30 25]", got2)
	}
	if got3 := floatCol(t, got, "/xs/1"); !equalFloats(got3, []float64{20, 50}) {
		t.Fatalf("array index = %v want [20 50]", got3)
	}
	sum := mustRun(t, Select(Sum("user.age")).Where(Cmp("user.age", Ge, 30)), set)
	if c, _ := sum.Column("sum(user.age)"); mustFloat(t, c.Cells[0]) != 30 {
		t.Fatalf("filtered nested sum = %s want 30", c.Cells[0])
	}
}

func mustFloat(t testing.TB, c Cell) float64 {
	t.Helper()
	f, ok := c.Float64()
	if !ok {
		t.Fatalf("cell not numeric: %s", c)
	}
	return f
}

// TestQueryExactIntegerEquality is the capability edge over a float64 engine:
// two integers one apart past 2^53 stay distinct in equality and grouping.
func TestQueryExactIntegerEquality(t *testing.T) {
	set := mustSegment(t, `{"n":9007199254740992}`, `{"n":9007199254740993}`)
	groups := mustRun(t, Select(Path("n"), Count()).GroupBy("n"), set)
	if groups.RowCount != 2 {
		t.Fatalf("grouping 2^53 and 2^53+1: RowCount=%d want 2 (float64 would merge)", groups.RowCount)
	}
	eq := mustRun(t, Select(Count()).Where(Cmp("n", Eq, int64(9007199254740993))), set)
	if c, _ := eq.Column("count(*)"); !countIs(c.Cells[0], 1) {
		t.Fatalf("exact equality count = %s want 1", c.Cells[0])
	}
}

// TestQueryNumberSpellingEquality checks that all spellings of one value are
// one value to comparison and grouping.
func TestQueryNumberSpellingEquality(t *testing.T) {
	set := mustSegment(t, `{"n":1}`, `{"n":1.0}`, `{"n":10e-1}`, `{"n":100e-2}`)
	groups := mustRun(t, Select(Path("n"), Count()).GroupBy("n"), set)
	if groups.RowCount != 1 {
		t.Fatalf("spellings of 1 grouped into %d groups want 1", groups.RowCount)
	}
	if c, _ := groups.Column("count(*)"); !countIs(c.Cells[0], 4) {
		t.Fatalf("group count = %s want 4", c.Cells[0])
	}
	eq := mustRun(t, Select(Count()).Where(Cmp("n", Eq, 1)), set)
	if c, _ := eq.Column("count(*)"); !countIs(c.Cells[0], 4) {
		t.Fatalf("equality to 1 count = %s want 4", c.Cells[0])
	}
}

func TestQueryExistsVsIsNull(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":null}`, `{"b":2}`)
	cases := []struct {
		pred Predicate
		want int64
	}{
		{Exists("a"), 2},      // present, including the explicit null
		{IsNull("a"), 2},      // null value or absent path
		{Not(IsNull("a")), 1}, // present and non-null
		{Not(Exists("a")), 1}, // absent only
	}
	for _, tc := range cases {
		got := mustRun(t, Select(Count()).Where(tc.pred), set)
		if c, _ := got.Column("count(*)"); !countIs(c.Cells[0], tc.want) {
			t.Fatalf("predicate count = %s want %d", c.Cells[0], tc.want)
		}
	}
}

func TestQueryDuplicateKeysLastWins(t *testing.T) {
	set := mustSegment(t, `{"a":1,"a":2}`)
	proj := mustRun(t, Select(Path("a")), set)
	if c, _ := proj.Column("a"); mustFloat(t, c.Cells[0]) != 2 {
		t.Fatalf("duplicate key projection = %s want 2", c.Cells[0])
	}
	agg := mustRun(t, Select(Sum("a")), set)
	if c, _ := agg.Column("sum(a)"); mustFloat(t, c.Cells[0]) != 2 {
		t.Fatalf("duplicate key sum = %s want 2", c.Cells[0])
	}
}

func TestQueryCrossTypeComparison(t *testing.T) {
	set := mustSegment(t, `{"a":1}`, `{"a":"x"}`, `{"a":true}`, `{"a":null}`)
	check := func(p Predicate, want int64) {
		t.Helper()
		got := mustRun(t, Select(Count()).Where(p), set)
		c, _ := got.Column("count(*)")
		if !countIs(c.Cells[0], want) {
			t.Fatalf("cross-type predicate count = %s want %d", c.Cells[0], want)
		}
	}
	check(Cmp("a", Eq, 1), 1)    // only the number 1
	check(Cmp("a", Eq, "x"), 1)  // only the string
	check(Cmp("a", Eq, true), 1) // only the bool
	check(Cmp("a", Ne, 1), 2)    // string and bool differ from a number; null never matches
}

func TestQueryCompileErrors(t *testing.T) {
	set := mustSegment(t, `{"a":1}`)
	cases := []struct {
		name string
		q    *Query
	}{
		{"no columns", Select()},
		{"projection not grouped", Select(Path("a"), Path("b")).GroupBy("a")},
		{"aggregate mixed without group by", Select(Path("a"), Sum("b"))},
		{"unsupported literal", Select(Path("a")).Where(Cmp("a", Eq, struct{}{}))},
		{"bad pointer", Select(Path("/a/~9"))},
		{"order by ungrouped path", Select(Path("a"), Count()).GroupBy("a").OrderBy("b", Asc)},
		{"bad contains literal", Select(Count()).Where(Contains("a", `{bad`))},
	}
	for _, tc := range cases {
		if _, err := tc.q.Run(FromSegment(set)); err == nil {
			t.Fatalf("%s: expected compile error, got nil", tc.name)
		}
	}
}
