package query

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// An Op is a scalar comparison operator for Cmp.
type Op uint8

const (
	Eq Op = iota // =
	Ne           // !=
	Lt           // <
	Le           // <=
	Gt           // >
	Ge           // >=
)

// A Predicate is a WHERE condition: a leaf comparison, containment, existence,
// or null test, or a boolean combination of predicates. Predicates are plain
// values built by the constructors below; they compile with the query. The
// zero Predicate is not a valid condition — build one with a constructor.
type Predicate struct {
	kind        predKind
	path        string
	op          Op
	value       any    // Cmp literal, inferred at compile
	json        string // Contains needle
	pattern     string // Like pattern, using '%' and '_' wildcards
	insensitive bool   // ILIKE uses Unicode simple case folding
	values      []any  // In alternatives, inferred at compile
	kids        []Predicate
}

type predKind uint8

const (
	predCmp predKind = iota
	predContains
	predExists
	predIsNull
	predAnd
	predOr
	predNot
	predIn
	// predInBound is a membership whose alternatives are not known when the
	// plan compiles: a join's inner side has to run before anyone can say what
	// the outer path must match. The node carries a slot instead of literals,
	// and the executor fills that slot before the scan starts. It is a separate
	// kind rather than a flag on predIn so the compile-time membership keeps
	// exactly the code it has today — no extra load, no extra branch — on the
	// path that already had its set.
	predInBound
	// predAntiBound is the explicitly two-valued anti-semi counterpart used by
	// decorrelated NOT EXISTS. It is not represented as predNot(predInBound):
	// NULL/missing outer keys have no partner and therefore match the anti leaf,
	// independent of SQL predicate-negation rewrites around ordinary leaves.
	predAntiBound
	// predLike matches a string column against a SQL LIKE pattern.
	predLike
	// predIsString is an internal SQL three-valued-logic guard for LIKE. It is
	// not exported because the builder surface has no general JSON type tests.
	predIsString
)

// Cmp compares the value at path against a typed literal. The literal's Go
// type fixes the comparison type: bool, string, any signed or unsigned
// integer, or float32/float64. Comparison is within type — numbers by exact
// decimal value, strings by content, bools by value — with a defined
// cross-type order (null < bool < number < string); a null or absent value
// never satisfies a comparison (test it with IsNull or Exists). An unsupported
// literal type is reported when the query compiles.
func Cmp(path string, op Op, value any) Predicate {
	return Predicate{kind: predCmp, path: path, op: op, value: value}
}

// In tests whether the value at path equals any of the given literals, the
// membership form of [Cmp] with [Eq]. Each literal follows Cmp's typing rules,
// and a null or absent value satisfies no alternative (test it with IsNull).
// In with no values matches nothing.
//
// In is not sugar for a disjunction of equalities. The alternatives are sorted
// and deduplicated once at compile time, so the per-row test is a binary search
// rather than one comparison per alternative — the difference between O(log n)
// and O(n) work on every row of a scan, and on every candidate an index probe
// hands back for exact recheck.
func In(path string, values ...any) Predicate {
	return Predicate{kind: predIn, path: path, values: values}
}

// Like tests whether the string at path matches a SQL LIKE pattern. '%' matches
// any sequence, '_' matches one Unicode code point, and backslash escapes the
// following pattern character. Non-string, null, and absent values do not
// match. Use ILike for case-insensitive matching.
func Like(path, pattern string) Predicate {
	return Predicate{kind: predLike, path: path, pattern: pattern}
}

// ILike is Like with Unicode simple case folding for literal pattern
// characters. Wildcards and escapes retain the same meaning as Like.
func ILike(path, pattern string) Predicate {
	return Predicate{kind: predLike, path: path, pattern: pattern, insensitive: true}
}

func isString(path string) Predicate {
	return Predicate{kind: predIsString, path: path}
}

// Contains tests PostgreSQL jsonb containment (@>): whether the value at path
// contains jsonLiteral, which must be one JSON document. It compiles to the
// core's RawContains, whose exact-decimal number comparison and structural
// semantics this inherits. An absent value contains nothing.
func Contains(path, jsonLiteral string) Predicate {
	return Predicate{kind: predContains, path: path, json: jsonLiteral}
}

// Exists tests whether path is present in the document, whatever its value —
// including an explicit null. An absent path is not present.
func Exists(path string) Predicate {
	return Predicate{kind: predExists, path: path}
}

// IsNull tests whether the value at path is null or the path is absent, the
// two the query treats as one.
func IsNull(path string) Predicate {
	return Predicate{kind: predIsNull, path: path}
}

// And is the conjunction of its operands; with no operands it is always true.
func And(preds ...Predicate) Predicate {
	return Predicate{kind: predAnd, kids: preds}
}

// Or is the disjunction of its operands; with no operands it is always false.
func Or(preds ...Predicate) Predicate {
	return Predicate{kind: predOr, kids: preds}
}

// Not is the negation of pred.
func Not(pred Predicate) Predicate {
	return Predicate{kind: predNot, kids: []Predicate{pred}}
}

// A literal is a typed WHERE constant, resolved from Cmp's value at compile.
type literal struct {
	kind  scalarKind
	bval  bool
	num   []byte
	isInt bool
	ival  int64
	sval  string
}

// makeLiteral infers a typed literal from a Go value. Integers keep an exact
// int64 fast path; a uint64 beyond int64 and every float keep an exact decimal
// spelling instead, so comparison never rounds. The spelling is copied into
// c's text arena, so it lives exactly as long as the plan that compares
// against it.
//
// *Number and *string are accepted alongside their value spellings because
// converting a string-shaped value to an any copies its header onto the heap,
// one allocation per literal on a path whose whole contract is to have none,
// while a pointer-shaped value needs no box at all. SQL lowering hands the
// arena's own pointer across; a caller writing Cmp by hand naturally uses the
// value spellings.
func (c *compiler) makeLiteral(value any) (literal, error) {
	switch v := value.(type) {
	case bool:
		return literal{kind: kindBool, bval: v}, nil
	case *bool:
		if v == nil {
			return literal{}, fmt.Errorf("query: nil *bool literal")
		}
		return literal{kind: kindBool, bval: *v}, nil
	case string:
		return literal{kind: kindString, sval: v}, nil
	case *string:
		if v == nil {
			return literal{}, fmt.Errorf("query: nil *string literal")
		}
		return literal{kind: kindString, sval: *v}, nil
	case Number:
		return c.numberLiteral(v)
	case *Number:
		if v == nil {
			return literal{}, fmt.Errorf("query: nil *query.Number literal")
		}
		return c.numberLiteral(*v)
	case int:
		return c.intLiteral(int64(v)), nil
	case int8:
		return c.intLiteral(int64(v)), nil
	case int16:
		return c.intLiteral(int64(v)), nil
	case int32:
		return c.intLiteral(int64(v)), nil
	case int64:
		return c.intLiteral(v), nil
	case *int64:
		if v == nil {
			return literal{}, fmt.Errorf("query: nil *int64 literal")
		}
		return c.intLiteral(*v), nil
	case uint:
		return c.uintLiteral(uint64(v)), nil
	case uint8:
		return c.intLiteral(int64(v)), nil
	case uint16:
		return c.intLiteral(int64(v)), nil
	case uint32:
		return c.intLiteral(int64(v)), nil
	case uint64:
		return c.uintLiteral(v), nil
	case float32:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return literal{}, fmt.Errorf("query: non-finite numeric literal %v", v)
		}
		return c.floatLiteral(f), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return literal{}, fmt.Errorf("query: non-finite numeric literal %v", v)
		}
		return c.floatLiteral(v), nil
	case *float64:
		if v == nil {
			return literal{}, fmt.Errorf("query: nil *float64 literal")
		}
		if math.IsNaN(*v) || math.IsInf(*v, 0) {
			return literal{}, fmt.Errorf("query: non-finite numeric literal %v", *v)
		}
		return c.floatLiteral(*v), nil
	default:
		return literal{}, fmt.Errorf("query: unsupported literal type %T", value)
	}
}

// numberLiteral types an exact decimal spelling from SQL or a bound value.
// The int64 fast path is taken when the spelling denotes one, so a membership
// over ordinary integer keys compares by integer rather than by digits.
func (c *compiler) numberLiteral(v Number) (literal, error) {
	if err := v.validate(); err != nil {
		return literal{}, fmt.Errorf("query: literal: %w", err)
	}
	if i, ok := int64Spelling(string(v)); ok {
		return c.intLiteral(i), nil
	}
	return literal{kind: kindNumber, num: c.bytes(byteview.Bytes(string(v)))}, nil
}

// int64Spelling reads a plain decimal integer, reporting false for a fraction,
// an exponent, or a magnitude past int64.
//
// It does not call strconv.ParseInt, whose failure path builds a
// *strconv.NumError holding a copy of the input: two heap allocations spent on
// every non-integer literal in every compile, to describe a rejection the
// caller immediately discards for the exact-decimal path.
func int64Spelling(text string) (int64, bool) {
	negative := false
	if len(text) > 0 && text[0] == '-' {
		negative = true
		text = text[1:]
	}
	// Nineteen digits is the widest run that cannot overflow the uint64
	// accumulator, so the loop needs no per-digit overflow check.
	if len(text) == 0 || len(text) > 19 {
		return 0, false
	}
	var magnitude uint64
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		magnitude = magnitude*10 + uint64(c-'0')
	}
	if negative {
		// Written as a uint64 bound because the int64 minimum has no positive
		// counterpart to compare against; the conversion below wraps onto it
		// exactly, which is the answer wanted.
		if magnitude > 1<<63 {
			return 0, false
		}
		return -int64(magnitude), true
	}
	if magnitude > math.MaxInt64 {
		return 0, false
	}
	return int64(magnitude), true
}

func (c *compiler) intLiteral(i int64) literal {
	buf := strconv.AppendInt(c.tmp[:0], i, 10)
	c.tmp = buf
	return literal{kind: kindNumber, num: c.bytes(buf), isInt: true, ival: i}
}

func (c *compiler) uintLiteral(u uint64) literal {
	if u <= math.MaxInt64 {
		return c.intLiteral(int64(u))
	}
	buf := strconv.AppendUint(c.tmp[:0], u, 10)
	c.tmp = buf
	return literal{kind: kindNumber, num: c.bytes(buf)}
}

func (c *compiler) floatLiteral(f float64) literal {
	buf := strconv.AppendFloat(c.tmp[:0], f, 'g', -1, 64)
	c.tmp = buf
	return literal{kind: kindNumber, num: c.bytes(buf)}
}

// A compiledPredicate is a WHERE tree resolved for repeated evaluation: leaf
// predicates reference a column by index, comparisons carry their classified
// literal, and containment carries its validated needle bytes. A leaf also
// carries its posting probe (postNone when unpostable), the descriptor
// candidates.go uses to prune candidate rows through Segment.Postings.
type compiledPredicate struct {
	kind        predKind
	col         int
	op          Op
	lit         scalar
	needle      vibejson.Index
	pattern     string
	insensitive bool
	probe       postProbe
	boundPath   string
	containPlan *compiledPredicate
	kids        []*compiledPredicate

	// In membership: lits is sorted by compareScalar and deduplicated, so
	// eval binary-searches it. needles holds the matching exact index needle
	// per alternative, in the same order, or is nil when any alternative has
	// no scalar needle and the whole membership is therefore unindexable.
	lits    []scalar
	needles []vibejson.Index

	// slot addresses this node's late binding in the executing [Workspace],
	// for predInBound and predAntiBound. It is an index rather than a pointer because a
	// compiled plan is immutable and shared by every concurrent execution: the
	// values a join collects belong to one Exec, so the plan may name where to
	// find them but must never hold them.
	slot int
}

// inLinearMax is the membership size below which eval compares linearly rather
// than binary-searching. A handful of predictable, cache-resident comparisons
// beats a branchy search; the crossover is where the mispredicted binary search
// starts winning on comparison count.
const inLinearMax = 8

// compilePredicate resolves a predicate tree, registering every path it reads
// in reg so the executor extracts each needed column once. Every node, child
// list, membership set, and needle tape comes from c's arenas, so a warmed
// recompilation of the same shape allocates none of them.
func (c *compiler) compilePredicate(p Predicate, reg *pathRegistry) (*compiledPredicate, error) {
	switch p.kind {
	case predCmp:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		lit, err := c.makeLiteral(p.value)
		if err != nil {
			return nil, err
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predCmp, col: col, op: p.op, lit: classifyLiteral(lit)}
		// Every equality compiles its exact scalar needle for declared collection
		// indexes, including nested paths. The older Segment posting family is
		// limited to one top-level field and receives the same needle only when
		// that narrower contract applies.
		if p.op == Eq {
			if needle, ok := c.eqNeedle(cp.lit); ok {
				idx, err := c.buildNeedleIndex(needle)
				if err != nil {
					return nil, err
				}
				cp.needle = idx
				if reg.paths[col].single {
					cp.probe = postProbe{kind: postEq, path: reg.paths[col].name, needle: idx}
				}
			}
		}
		return cp, nil
	case predIn:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		lits := c.lits.alloc(len(p.values))[:0]
		for _, value := range p.values {
			lit, err := c.makeLiteral(value)
			if err != nil {
				return nil, err
			}
			lits = append(lits, classifyLiteral(lit))
		}
		slices.SortStableFunc(lits, compareScalar)
		lits = slices.CompactFunc(lits, func(a, b scalar) bool { return compareScalar(a, b) == 0 })
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predIn, col: col, op: Eq, lits: lits}
		// One alternative without a scalar needle makes the whole membership
		// unindexable: the index could then answer only part of the set, and a
		// partial candidate mask is not a sound superset of the rows In accepts.
		needles := c.idxs.alloc(len(lits))[:0]
		for _, lit := range lits {
			raw, ok := c.eqNeedle(lit)
			if !ok {
				needles = nil
				break
			}
			idx, err := c.buildNeedleIndex(raw)
			if err != nil {
				return nil, err
			}
			needles = append(needles, idx)
		}
		cp.needles = needles
		// The Segment posting layer addresses one top-level field, so a
		// membership prunes there only when every alternative has a needle and
		// the path is that narrow shape. Declared collection indexes are matched
		// later, against the snapshot's own catalog.
		if len(needles) == len(lits) && len(lits) > 0 && reg.paths[col].single {
			cp.probe = postProbe{kind: postIn, path: reg.paths[col].name}
		}
		return cp, nil
	case predLike:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		if err := validateLikePattern(p.pattern); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidPattern, err)
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{
			kind: predLike, col: col,
			pattern: c.internString(p.pattern), insensitive: p.insensitive,
		}
		return cp, nil
	case predContains:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		needle, scalarNeedle, err := c.containsNeedleIndex(p.json)
		if err != nil {
			return nil, fmt.Errorf("query: Contains literal: %w", err)
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predContains, col: col, needle: needle}
		// Only a scalar needle over a single top-level field prunes: the value
		// buckets index scalars, and a structured needle would fall to a full
		// scan inside WhereContains anyway, so leaving it unpostable avoids
		// scanning twice.
		if scalarNeedle && reg.paths[col].single {
			cp.probe = postProbe{kind: postContains, path: reg.paths[col].name, needle: needle}
		}
		if key, value, ok, probeErr := c.singleScalarObjectContainmentProbe(needle); probeErr != nil {
			return nil, probeErr
		} else if ok {
			base := reg.paths[col].indexPath()
			// Segment postings address one top-level field. A root-object
			// containment predicate can therefore use the same derived scalar
			// equality as a sound candidate bound; nested forms are handled by
			// declared collection indexes below.
			if base == "" {
				cp.probe = postProbe{kind: postEq, path: key, needle: value}
			}
		}
		cp.containPlan, err = c.scalarObjectContainmentPlan(
			needle, reg.paths[col].indexPath(),
		)
		if err != nil {
			return nil, err
		}
		return cp, nil
	case predExists:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predExists, col: col}
		if reg.paths[col].single {
			cp.probe = postProbe{kind: postExists, path: reg.paths[col].name}
		}
		return cp, nil
	case predIsNull:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predIsNull, col: col}
		return cp, nil
	case predIsString:
		col, err := c.addPath(reg, p.path)
		if err != nil {
			return nil, err
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: predIsString, col: col}
		return cp, nil
	case predAnd, predOr, predNot:
		kids := c.kids.alloc(len(p.kids))[:0]
		for _, kid := range p.kids {
			ck, err := c.compilePredicate(kid, reg)
			if err != nil {
				return nil, err
			}
			kids = append(kids, ck)
		}
		if p.kind == predOr {
			kids = c.coalesceEqualityDisjuncts(kids, reg)
			if len(kids) == 1 {
				// A disjunction of one is that operand. Collapsing it means a
				// rewritten OR is indistinguishable from the membership a
				// caller could have written by hand, down to the plan.
				return kids[0], nil
			}
		}
		cp := c.nodes.one()
		*cp = compiledPredicate{kind: p.kind, kids: kids}
		return cp, nil
	default:
		return nil, fmt.Errorf("query: invalid predicate")
	}
}

const maxIndexedContainmentLeaves = 64

type scalarContainmentLeaf struct {
	path string
	raw  []byte
}

// scalarObjectContainmentPlan lowers an object needle made entirely of
// non-container leaves to exact path equalities. Object containment is exactly
// the conjunction of those leaves under the core's last-duplicate rule.
// Arrays and empty objects carry structural information that equality indexes
// cannot prove, so the rewrite is deliberately all-or-nothing.
func (c *compiler) scalarObjectContainmentPlan(needle vibejson.Index, base string) (*compiledPredicate, error) {
	c.leaves = c.leaves[:0]
	if !c.appendScalarContainmentLeaves(needle.Root(), base) ||
		len(c.leaves) == 0 || len(c.leaves) > maxIndexedContainmentLeaves {
		return nil, nil
	}
	kids := c.kids.alloc(len(c.leaves))[:0]
	for _, leaf := range c.leaves {
		value, err := c.buildNeedleIndex(leaf.raw)
		if err != nil {
			return nil, err
		}
		kid := c.nodes.one()
		*kid = compiledPredicate{
			kind: predCmp, col: -1, op: Eq, needle: value,
			boundPath: leaf.path,
		}
		kids = append(kids, kid)
	}
	if len(kids) == 1 {
		return kids[0], nil
	}
	cp := c.nodes.one()
	*cp = compiledPredicate{kind: predAnd, kids: kids}
	return cp, nil
}

// indexPath returns the declared-index spelling for a comparison leaf.
// Scalar containment lowering sets boundPath without registering a value
// column: the derived equality exists only to choose an exact index and must
// never turn an array-shaped containment non-match into a JSON Pointer error.
func (p *compiledPredicate) indexPath(paths []compiledPath) string {
	if p.boundPath != "" {
		return p.boundPath
	}
	return paths[p.col].indexPath()
}

// An effectiveMember is one object member after the last-duplicate rule has
// been applied, paired with the node its final occurrence names.
type effectiveMember struct {
	key   string
	value vibejson.Node
}

func (c *compiler) appendScalarContainmentLeaves(node vibejson.Node, base string) bool {
	count, ok := node.ObjectLen()
	if !ok || count == 0 {
		return false
	}
	// The member list is carved off the shared scratch with stack discipline:
	// this walk recurses into nested objects, and one flat reused slice would
	// let an inner object's members overwrite the outer object's before the
	// outer loop had visited them.
	base0 := len(c.members)
	defer func() { c.members = c.members[:base0] }()
	iterator, _ := node.ObjectIter()
	for {
		key, value, ok := iterator.Next()
		if !ok {
			break
		}
		decoded, textOK := key.AppendText(c.tmp[:0])
		if !textOK {
			return false
		}
		c.tmp = decoded
		name := c.intern(decoded)
		if position := memberIndex(c.members[base0:], name); position >= 0 {
			c.members[base0+position].value = value
			continue
		}
		c.members = append(c.members, effectiveMember{key: name, value: value})
	}
	for i := base0; i < len(c.members); i++ {
		member := c.members[i]
		path := c.pointerChild(base, member.key)
		switch member.value.Kind() {
		case document.Object:
			if !c.appendScalarContainmentLeaves(member.value, path) {
				return false
			}
		case document.Array, document.Invalid:
			return false
		default:
			c.leaves = append(c.leaves, scalarContainmentLeaf{
				path: path, raw: member.value.Raw().Bytes(),
			})
			if len(c.leaves) > maxIndexedContainmentLeaves {
				return false
			}
		}
	}
	return true
}

// memberIndex finds an already-recorded member by name. A needle object holds
// a handful of members, so this scans rather than hashing, for the same reason
// pathRegistry does.
func memberIndex(members []effectiveMember, name string) int {
	for i := range members {
		if members[i].key == name {
			return i
		}
	}
	return -1
}

// pointerChild renders base + "/" + escaped(key) into the text arena, rather
// than concatenating two heap strings per needle leaf.
func (c *compiler) pointerChild(base, key string) string {
	buf := append(c.tmp[:0], base...)
	buf = append(buf, '/')
	buf = appendEscapedPointerSegment(buf, key)
	c.tmp = buf
	return c.intern(buf)
}

// singleScalarObjectContainmentProbe recognizes the exact implication
//
//	container @> {key: scalar}  =>  container/key = scalar
//
// for a one-member object needle. The derived equality is safe as a Segment
// posting bound because JSON object containment resolves that member through
// the same last-key and exact-scalar semantics as an exact index. Declared
// collection indexes use scalarObjectContainmentPlan for wider nested scalar
// objects. Compilation may allocate for an escaped key or the tiny scalar
// tape; execution does not.
func (c *compiler) singleScalarObjectContainmentProbe(needle vibejson.Index) (string, vibejson.Index, bool, error) {
	root := needle.Root()
	count, ok := root.ObjectLen()
	if !ok || count != 1 {
		return "", vibejson.Index{}, false, nil
	}
	it, _ := root.ObjectIter()
	key, value, _ := it.Next()
	switch value.Kind() {
	case document.Array, document.Object, document.Invalid:
		return "", vibejson.Index{}, false, nil
	}
	decoded, _ := key.AppendText(c.tmp[:0])
	c.tmp = decoded
	valueNeedle, err := c.buildNeedleIndex(value.Raw().Bytes())
	if err != nil {
		return "", vibejson.Index{}, false, err
	}
	return c.intern(decoded), valueNeedle, true, nil
}

// containsNeedleScalar validates that s is exactly one JSON document — the
// requirement RawContains places on a containment needle — and reports whether
// that document is a scalar (as opposed to an array or object). It reuses the
// core validator by building the needle's index once; the root kind then tells
// the compiler whether the value postings can prune the leaf.
func (c *compiler) containsNeedleIndex(s string) (vibejson.Index, bool, error) {
	// The index reads the needle text and never writes it, so it views the
	// string in place instead of copying it. Copying would be a per-needle
	// allocation to duplicate bytes the predicate already retains — and the
	// arena spelling of the same needle is a view of arena storage anyway.
	idx, err := c.buildNeedleIndex(byteview.Bytes(s))
	if err != nil {
		return vibejson.Index{}, false, err
	}
	switch idx.Root().Kind() {
	case document.Array, document.Object:
		return idx, false, nil
	default:
		return idx, true, nil
	}
}

// buildNeedleIndex indexes one needle onto c's tape arena. src must outlive
// the plan, since the index borrows rather than copies it.
func (c *compiler) buildNeedleIndex(src []byte) (vibejson.Index, error) {
	entries, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		return vibejson.Index{}, err
	}
	return vibejson.BuildIndex(src, c.tape.alloc(entries))
}

// readsColumn reports whether evaluating p reads value column col. It is what
// splits the scan into a filter phase and a late-materialization phase, so it
// must answer for eval and only for eval: containPlan is deliberately not
// walked, because that derived equality tree exists to choose index candidates
// and is never evaluated against an extracted column. Counting it would pull
// columns that do not exist into the filter phase.
// A boolean node reads no column of its own, and its col field is left at the
// zero value rather than at -1 — so the leaf test has to be reached through the
// kind, never through col alone, or every conjunction, disjunction, and
// negation would claim to read value column zero and drag the first projected
// path into the filter phase.
//
// A join leaf (predInBound or predAntiBound) is a leaf like any other: it carries the outer join
// path's real column index, so it reaches the default arm and classifies that
// column into the filter phase, which is where a predicate's column belongs. It
// must not be added to the boolean arm above — a join leaf reads a column, and
// treating it as a node that reads none would leave WHERE evaluating against a
// column the filter phase never extracted.
func (p *compiledPredicate) readsColumn(col int) bool {
	if p == nil {
		return false
	}
	switch p.kind {
	case predAnd, predOr, predNot:
		for _, kid := range p.kids {
			if kid.readsColumn(col) {
				return true
			}
		}
		return false
	default:
		return p.col == col
	}
}

// The comparison-acceptance mask is the three-bit set of compareScalar signs an
// operator accepts: bit 0 for "less", bit 1 for "equal", bit 2 for "greater".
// Every one of the six operators is one of these sets, which is what lets a
// column scan decide the operator once and then test a whole column with a
// shift instead of re-entering a six-way switch on every row.
const (
	acceptLess uint8 = 1 << iota
	acceptEqual
	acceptGreater
)

func acceptMask(op Op) uint8 {
	switch op {
	case Eq:
		return acceptEqual
	case Ne:
		return acceptLess | acceptGreater
	case Lt:
		return acceptLess
	case Le:
		return acceptLess | acceptEqual
	case Gt:
		return acceptGreater
	case Ge:
		return acceptGreater | acceptEqual
	default:
		return 0
	}
}

// selectSpan is eval applied to a range of one column in one pass, appending
// the rows it accepts. Hoisting the predicate kind and the comparison operator out
// of the row loop is the entire point: the generic path re-decides what kind of
// predicate this is and which comparison it wants on every row of the scan, and
// the answer is the same for all of them.
//
// It declines, returning false, for the shapes where that hoisting demonstrably
// buys nothing: containment builds a structural index per row and a boolean
// tree's cost is its operands rather than its dispatch, so both keep the
// general path instead of growing a second implementation to hold in step with
// it.
func (p *compiledPredicate) selectSpan(dst []int, cols [][]scalar, lo, hi int) ([]int, bool) {
	switch p.kind {
	case predCmp:
		col, lit, accept := cols[p.col][lo:hi], p.lit, acceptMask(p.op)
		for row, cell := range col {
			// A null or absent cell satisfies no comparison, the same rule
			// evalCmp applies; test it with IsNull or Exists.
			if cell.kind != kindNull && accept&(1<<uint(compareScalar(cell, lit)+1)) != 0 {
				dst = append(dst, lo+row)
			}
		}
		return dst, true
	case predIn:
		col := cols[p.col][lo:hi]
		for row := range col {
			if p.member(col[row]) {
				dst = append(dst, lo+row)
			}
		}
		return dst, true
	case predExists:
		col := cols[p.col][lo:hi]
		for row, cell := range col {
			if present(cell) {
				dst = append(dst, lo+row)
			}
		}
		return dst, true
	case predIsNull:
		col := cols[p.col][lo:hi]
		for row, cell := range col {
			if cell.kind == kindNull {
				dst = append(dst, lo+row)
			}
		}
		return dst, true
	case predIsString:
		col := cols[p.col][lo:hi]
		for row, cell := range col {
			if cell.kind == kindString {
				dst = append(dst, lo+row)
			}
		}
		return dst, true
	default:
		// predInBound declines with the rest. Hoisting would buy nothing: a
		// join leaf's per-row cost is the membership search or the keyed probe
		// behind it, not the switch that dispatches to it, and a span form
		// would need the evaluator scratch threaded through a signature that
		// exists precisely to avoid carrying one.
		return dst, false
	}
}

// eval evaluates the predicate for one row against the extracted columns. s is
// the per-evaluator scratch: the containment indexer's entry storage, and the
// join bindings a predInBound leaf reads plus the probe state it writes.
//
// It is a parameter rather than a Workspace field because the filter phase runs
// on several goroutines over disjoint row ranges, each of which needs its own
// mutable scratch. A join leaf makes that concrete rather than theoretical — a
// lookup-bound one decodes a document and counts a probe per row — so the
// scratch is what keeps the split safe rather than merely tidy.
func (p *compiledPredicate) eval(cols [][]scalar, row int, s *evalScratch) bool {
	switch p.kind {
	case predCmp:
		return evalCmp(cols[p.col][row], p.op, p.lit)
	case predIn:
		return p.member(cols[p.col][row])
	case predLike:
		cell := cols[p.col][row]
		return cell.kind == kindString && likeMatch(p.pattern, cell.sval, p.insensitive)
	case predInBound:
		return s.binds[p.slot].matches(cols[p.col][row], &s.probes[p.slot])
	case predAntiBound:
		// This is existential negation, not SQL NOT applied to an UNKNOWN
		// comparison. NULL/missing cannot find an equality partner, so matches
		// returns false and the anti predicate keeps the row.
		return !s.binds[p.slot].matches(cols[p.col][row], &s.probes[p.slot])
	case predContains:
		cell := cols[p.col][row]
		if len(cell.raw) == 0 {
			return false // absent haystack contains nothing
		}
		haystack, err := s.containsTape(cell.raw)
		return err == nil && haystack.Root().Contains(p.needle.Root())
	case predExists:
		return present(cols[p.col][row])
	case predIsNull:
		return cols[p.col][row].kind == kindNull
	case predIsString:
		return cols[p.col][row].kind == kindString
	case predAnd:
		for _, kid := range p.kids {
			if !kid.eval(cols, row, s) {
				return false
			}
		}
		return true
	case predOr:
		for _, kid := range p.kids {
			if kid.eval(cols, row, s) {
				return true
			}
		}
		return false
	default: // predNot
		return !p.kids[0].eval(cols, row, s)
	}
}

// validateLikePattern checks the only malformed pattern this matcher cannot
// interpret: a trailing escape. The default SQL LIKE escape is backslash; an
// explicit ESCAPE clause is rejected by the SQL parser because this compact
// matcher deliberately has one fixed escape rule.
func validateLikePattern(pattern string) error {
	for i := 0; i < len(pattern); {
		r, n := utf8.DecodeRuneInString(pattern[i:])
		if r == '\\' {
			if i+n == len(pattern) {
				return fmt.Errorf("trailing backslash escape")
			}
			_, next := utf8.DecodeRuneInString(pattern[i+n:])
			i += n + next
			continue
		}
		i += n
	}
	return nil
}

// likeMatch implements the usual greedy LIKE matcher without allocating per
// row. The last '%' is a backtracking point: when the literal after it does
// not fit, the wildcard consumes one more input code point and matching
// resumes at the first literal after the wildcard. Pattern and input are UTF-8
// strings, so '_' and escaped literals operate on Unicode code points rather
// than bytes.
func likeMatch(pattern, text string, insensitive bool) bool {
	pi, ti := 0, 0
	star, starText := -1, -1
	for ti < len(text) {
		kind, r, next, ok := likeToken(pattern, pi)
		switch {
		case ok && kind == '%':
			star, starText = next, ti
			pi = next
			continue
		case ok && (kind == '_' || (kind == 'c' && runeEqual(r, runeAt(text, ti), insensitive))):
			pi = next
			ti += runeSize(text, ti)
			continue
		case star >= 0:
			starText += runeSize(text, starText)
			ti = starText
			pi = star
			continue
		default:
			return false
		}
	}
	for pi < len(pattern) {
		kind, _, next, ok := likeToken(pattern, pi)
		if !ok || kind != '%' {
			return false
		}
		pi = next
	}
	return true
}

// likeToken returns a pattern token. kind is '%' or '_' for wildcards and
// 'c' for a literal code point, including one escaped by backslash.
func likeToken(pattern string, pos int) (kind byte, literal rune, next int, ok bool) {
	if pos >= len(pattern) {
		return 0, 0, pos, false
	}
	r, n := utf8.DecodeRuneInString(pattern[pos:])
	if r == '%' || r == '_' {
		return byte(r), 0, pos + n, true
	}
	if r == '\\' {
		if pos+n >= len(pattern) {
			return 0, 0, pos, false
		}
		literal, m := utf8.DecodeRuneInString(pattern[pos+n:])
		return 'c', literal, pos + n + m, true
	}
	return 'c', r, pos + n, true
}

func runeAt(s string, pos int) rune {
	r, _ := utf8.DecodeRuneInString(s[pos:])
	return r
}

func runeSize(s string, pos int) int {
	_, n := utf8.DecodeRuneInString(s[pos:])
	return n
}

func runeEqual(a, b rune, insensitive bool) bool {
	if a == b || !insensitive {
		return a == b
	}
	for folded := unicode.SimpleFold(a); folded != a; folded = unicode.SimpleFold(folded) {
		if folded == b {
			return true
		}
	}
	return false
}

// coalesceEqualityDisjuncts rewrites the equalities within a disjunction into
// memberships, one per column. A disjunction of equalities on one path is a
// membership by definition — x = 1 OR x = 2 is exactly x IN (1, 2) — so this
// is a normalization, not a heuristic, and it applies whatever front end
// produced the tree: the builder's Or and SQL's OR both reach it.
//
// The win is on the other side of candidate generation. A disjunction tests
// its operands one at a time, so a row costs one comparison per alternative
// until one matches; a membership compares against a sorted set. That is the
// difference between linear and logarithmic work on every row of a scan and on
// every candidate an index probe hands back for exact recheck.
//
// Operands that are not single-column equalities pass through untouched, and a
// column contributing only one equality keeps it — a membership of one would
// add a search over a one-element set to save nothing. Groups are emitted in
// order of first appearance so a rewritten plan is reproducible.
// A columnCount is one column's equality tally within a disjunction, plus the
// position of the membership it was merged into once one exists.
type columnCount struct {
	col  int
	n    int
	slot int // index in the rewritten operand list, or -1 before merging
}

// mergeableEquality reports whether a compiled operand can join a membership.
// A leaf whose literal has no scalar needle cannot merge: the membership would
// then be unindexable as a whole, trading an index probe for a search.
// Equality literals always have one, so this only excludes the derived
// containment equalities, which carry a boundPath instead of a registered
// column.
func mergeableEquality(p *compiledPredicate) bool {
	return p.kind == predCmp && p.op == Eq && p.col >= 0 && p.boundPath == ""
}

func (c *compiler) coalesceEqualityDisjuncts(kids []*compiledPredicate, reg *pathRegistry) []*compiledPredicate {
	// The tally is a linearly scanned slice rather than the two maps this
	// used to build. A disjunction names one or two columns, so the maps were
	// two allocations and a hash per operand spent to avoid a comparison per
	// operand. It is safe to share one scratch slice across the whole
	// compilation because every operand is already compiled by the time this
	// runs, so no recursion can re-enter it.
	counts := c.counts[:0]
	for _, kid := range kids {
		if !mergeableEquality(kid) {
			continue
		}
		if i := countIndex(counts, kid.col); i >= 0 {
			counts[i].n++
		} else {
			counts = append(counts, columnCount{col: kid.col, n: 1, slot: -1})
		}
	}
	c.counts = counts

	merging := false
	for _, tally := range counts {
		if tally.n > 1 {
			merging = true
			break
		}
	}
	if !merging {
		return kids
	}

	out := c.kids.alloc(len(kids))[:0]
	for _, kid := range kids {
		i := -1
		if mergeableEquality(kid) {
			i = countIndex(counts, kid.col)
		}
		if i < 0 || counts[i].n < 2 {
			out = append(out, kid)
			continue
		}
		if counts[i].slot < 0 {
			counts[i].slot = len(out)
			merged := c.nodes.one()
			*merged = compiledPredicate{
				kind: predIn, col: kid.col, op: Eq,
				lits:    c.lits.alloc(counts[i].n)[:0],
				needles: c.idxs.alloc(counts[i].n)[:0],
			}
			out = append(out, merged)
		}
		merged := out[counts[i].slot]
		merged.lits = append(merged.lits, kid.lit)
		merged.needles = append(merged.needles, kid.needle)
	}

	for _, tally := range counts {
		if tally.slot >= 0 {
			finishMembership(out[tally.slot], reg)
		}
	}
	return out
}

// countIndex finds a column's tally, or -1.
func countIndex(counts []columnCount, col int) int {
	for i := range counts {
		if counts[i].col == col {
			return i
		}
	}
	return -1
}

// finishMembership puts a membership assembled from separate equalities into
// the same shape compilePredicate builds directly: alternatives sorted and
// deduplicated so eval can search them, needles kept in step, and the posting
// probe set when the path is the narrow shape the Segment posting layer
// addresses.
func finishMembership(p *compiledPredicate, reg *pathRegistry) {
	// Sorted and compacted in place, in step, rather than through a permuted
	// copy: the permutation and the two output slices were three allocations
	// per rewritten disjunction, and a membership assembled this way holds a
	// handful of alternatives, which is where insertion sort belongs anyway.
	sortMembership(p.lits, p.needles)
	kept := 0
	for i := range p.lits {
		if kept > 0 && compareScalar(p.lits[kept-1], p.lits[i]) == 0 {
			continue // an alternative repeated in the source disjunction
		}
		p.lits[kept], p.needles[kept] = p.lits[i], p.needles[i]
		kept++
	}
	p.lits, p.needles = p.lits[:kept], p.needles[:kept]
	// A membership is indexable only if every alternative carries a needle, so
	// one missing needle drops the whole set rather than leaving a partial —
	// and therefore unsound — candidate bound. An equality literal always has
	// one; this holds the invariant against a future literal kind that does
	// not, which would otherwise fail silently as a wrong index probe.
	for _, needle := range p.needles {
		if needle.Root().Kind() == document.Invalid {
			p.needles = nil
			return
		}
	}
	if reg.paths[p.col].single {
		p.probe = postProbe{kind: postIn, path: reg.paths[p.col].name}
	}
}

// sortMembership stably sorts alternatives and their needles together, keeping
// the two in step. It is an insertion sort because the input is a handful of
// alternatives merged out of one disjunction, and because a stable sort is
// what makes the following compaction keep the first of each repeated
// alternative — the same alternative finishMembership's permuted predecessor
// kept.
func sortMembership(lits []scalar, needles []vibejson.Index) {
	for i := 1; i < len(lits); i++ {
		for j := i; j > 0 && compareScalar(lits[j-1], lits[j]) > 0; j-- {
			lits[j-1], lits[j] = lits[j], lits[j-1]
			needles[j-1], needles[j] = needles[j], needles[j-1]
		}
	}
}

// member reports whether cell equals one of a membership's sorted, deduplicated
// alternatives. A null or absent cell satisfies none of them, the same rule
// evalCmp applies. Small sets compare linearly because the branch predictor
// handles a short run better than a search; larger ones binary-search.
func (p *compiledPredicate) member(cell scalar) bool {
	if cell.kind == kindNull {
		return false
	}
	if len(p.lits) <= inLinearMax {
		for i := range p.lits {
			if compareScalar(p.lits[i], cell) == 0 {
				return true
			}
		}
		return false
	}
	lo, hi := 0, len(p.lits)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if compareScalar(p.lits[mid], cell) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(p.lits) && compareScalar(p.lits[lo], cell) == 0
}

// evalCmp evaluates one comparison. A null or absent cell never satisfies a
// value comparison — the SQL-like rule that keeps null out of ordered results
// and out of the filter.
func evalCmp(cell scalar, op Op, lit scalar) bool {
	if cell.kind == kindNull {
		return false
	}
	c := compareScalar(cell, lit)
	switch op {
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
	case Ge:
		return c >= 0
	default:
		return false
	}
}

// present reports whether a classified cell came from a path that resolved to
// a value — including an explicit null, whose raw bytes are non-empty — as
// opposed to an absent path, whose classification carries no bytes.
func present(cell scalar) bool {
	return cell.kind != kindNull || len(cell.raw) > 0
}
