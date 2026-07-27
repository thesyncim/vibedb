package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The JSON document front end. A query is itself a JSON object, so the same
// text that travels over a wire or sits in a config file compiles to the same
// immutable compiled plan the builder and the SQL subset produce. [New] takes that
// object as Go literals; [Parse] takes it as JSON bytes. Neither retains the
// document: it is lowered to Columns and Predicates and discarded.
//
//	{
//	  "select":  ["profile.country", {"total": {"$sum": "score"}}],
//	  "where":   {"tenant": "acme", "score": {"$gte": 5}},
//	  "join":    [{"from": "plans", "as": "p", "on": {"plan_id": "$key"}, "where": {"tier": "pro"}}],
//	  "groupBy": ["profile.country"],
//	  "orderBy": [{"profile.country": "desc"}],
//	  "limit":   20
//	}
//
// Sibling keys of an object conjoin, so the common all-of filter needs no
// explicit operator. Every clause accepts a bare scalar wherever it accepts a
// one-element array, so "groupBy": "team" and "groupBy": ["team"] agree, and so
// does a lone join object in place of a one-element join list.
//
// A join clause reads a second collection; see [Join]. "on" maps a driving path
// to a joined one, and "$key" on the joined side names that collection's
// primary key. "as" names the joined collection for the rest of the query,
// which turns the clause from a filter into SQL's inner join: with it,
// "as": "o" makes "o.total" a column of the joined document and the result
// carries one row per matching pair.
//
// Paths are the same spec the builder's [Path] takes: dotted
// ("profile.country") or an RFC 6901 pointer ("/profile/country").
//
// Both forms are read through one lowering: see qvalue in json_value.go, which
// is why Parse never materializes the document as Go maps and slices. The
// lowering is a method on [Compiler] because everything it retains is carved
// from that compiler's arenas; the package-level New and Parse are the same
// lowering driven by a throwaway compiler nothing else can reach, which is
// what makes the Query they return the sole owner of its storage.

// M is a JSON object written as a Go literal. Its values are the JSON value
// space: M, A, string, bool, nil, any Go numeric type, and [Number].
type M map[string]any

// A is a JSON array written as a Go literal, with the same element space as M.
type A []any

// Number is a JSON number held as its original decimal spelling, so a value
// too large for float64 — an integer past the 2^53 mantissa, say — compiles to
// an exact literal rather than a rounded one. [Parse] produces Number for every
// number it reads. Comparison against a Number is by exact decimal value, the
// same rule the engine applies to document numbers.
type Number string

// New compiles a query document written as Go literals. A malformed document
// or a document that violates a plan rule is reported here rather than at Run.
//
//	q, err := query.New(query.M{
//		"select": query.A{"team", query.M{"n": query.M{"$count": nil}}},
//		"where":  query.M{"active": true, "score": query.M{"$gte": 5}},
//		"groupBy": "team",
//	})
//
// The returned Query owns everything it retains and carries no lifetime
// caveat. It compiles through a [Compiler] that nothing else can reach, so the
// arenas the plan points into are reachable only through that plan; a caller
// that compiles in a loop and wants the storage back instead should hold its
// own Compiler and use [Compiler.New].
func New(doc M) (*Query, error) {
	var c Compiler
	c.forOneShot()
	dst := new(Query)
	if err := c.New(dst, doc); err != nil {
		return nil, err
	}
	return dst, nil
}

// Parse compiles a query document from JSON text. It is exactly New over the
// parsed document, with every number preserved as its original spelling, so a
// query that arrives as bytes and one written as Go literals compile to the
// same plan.
//
// Parse borrows src only while it compiles. Everything the returned Query
// retains — paths, aliases, literals, and containment needles — is copied, so
// src may be reused or modified as soon as Parse returns. Like [New] the
// returned Query owns its storage outright; [Compiler.Parse] is the reusable
// form for a caller compiling many queries.
func Parse(src []byte) (*Query, error) {
	var c Compiler
	c.forOneShot()
	dst := new(Query)
	if err := c.Parse(dst, src); err != nil {
		return nil, err
	}
	return dst, nil
}

// buildQuery lowers a query document onto the builder's own representation.
// The clauses are collected in one pass and applied in a fixed order, because
// the select list must be in place before the grouping rules can be checked
// against it.
func (c *Compiler) buildQuery(dst *Query, root qvalue) error {
	if root.kind() != qObject {
		return fmt.Errorf("query: a query document must be a JSON object, not %s", root.describeKind())
	}

	var selectClause, whereClause, joinClause, groupClause, orderClause, limitClause qvalue
	err := root.fields(c, func(key qkey, value qvalue) error {
		switch {
		case key.equals("select"):
			selectClause = value
		case key.equals("where"):
			whereClause = value
		case key.equals("join"):
			joinClause = value
		case key.equals("groupBy"):
			groupClause = value
		case key.equals("orderBy"):
			orderClause = value
		case key.equals("limit"):
			limitClause = value
		default:
			return fmt.Errorf(
				"query: unknown query clause %q: expected select, where, join, groupBy, orderBy, or limit", key)
		}
		return nil
	})
	if err != nil {
		return err
	}

	columns, err := c.buildSelect(dst.columns, selectClause)
	if err != nil {
		return err
	}
	dst.columns = columns

	if !whereClause.missing() {
		predicate, err := c.buildPredicate(whereClause, at("where"))
		if err != nil {
			return err
		}
		dst.Where(predicate)
	}
	if !joinClause.missing() {
		joins, err := c.buildJoins(dst.joins, joinClause)
		if err != nil {
			return err
		}
		dst.joins = joins
	}
	if !groupClause.missing() {
		paths, err := c.buildPaths(dst.groupBy, groupClause, at("groupBy"))
		if err != nil {
			return err
		}
		dst.groupBy = paths
	}
	if !orderClause.missing() {
		if err := c.buildOrderBy(dst, orderClause); err != nil {
			return err
		}
	}
	if !limitClause.missing() {
		n, ok := limitClause.wholeNumberOf()
		if !ok {
			return fmt.Errorf("query: limit: expected a whole number, not %s", limitClause.describeKind())
		}
		dst.Limit(int(n))
	}
	return nil
}

// buildJoins lowers the join clause into dst. It accepts one join object or an
// array of them, the same bare-scalar-or-array shape every other clause has.
func (c *Compiler) buildJoins(dst []Join, spec qvalue) ([]Join, error) {
	if spec.kind() == qObject {
		join, err := c.buildJoin(spec, at("join"))
		if err != nil {
			return nil, err
		}
		return append(dst, join), nil
	}
	if spec.kind() != qArray {
		return nil, fmt.Errorf(
			"query: join: expected a join object or an array of them, not %s", spec.describeKind())
	}
	if spec.length() == 0 {
		return nil, fmt.Errorf(
			"query: join: an empty list joins nothing; omit the clause instead")
	}
	dst = reserve(dst, spec.length())
	err := spec.elements(func(index int, element qvalue) error {
		join, err := c.buildJoin(element, at("join").elem(index))
		if err != nil {
			return err
		}
		dst = append(dst, join)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// buildJoin lowers one join clause: the inner collection, the key mapping, and
// the optional inner filter.
//
// A join here is a semi-join — it filters the driving collection and returns
// none of the inner collection's columns — so a clause that tries to project,
// group, or order the inner side is rejected by name rather than ignored. That
// is a deliberate refusal, not a gap: returning inner columns changes the
// result's cardinality and schema and needs a fan-out plan, and silently
// dropping the request would answer a question the caller did not ask.
func (c *Compiler) buildJoin(spec qvalue, where loc) (Join, error) {
	if spec.kind() != qObject {
		return Join{}, fmt.Errorf(
			"query: %s: a join is an object, not %s", where, spec.describeKind())
	}
	var fromClause, onClause, whereClause, asClause qvalue
	err := spec.fields(c, func(key qkey, value qvalue) error {
		switch {
		case key.equals("from"):
			fromClause = value
		case key.equals("on"):
			onClause = value
		case key.equals("where"):
			whereClause = value
		case key.equals("as"):
			asClause = value
		case key.equals("select"):
			return fmt.Errorf(
				"query: %s: %q is not a join key: name the joined collection with "+
					"\"as\" and project its columns from the query's own select list",
				where, key)
		default:
			return fmt.Errorf(
				"query: %s: unknown join key %q: expected from, as, on, or where", where, key)
		}
		return nil
	})
	if err != nil {
		return Join{}, err
	}

	collection, ok := fromClause.text(c)
	if !ok {
		if fromClause.missing() {
			return Join{}, fmt.Errorf(
				"query: %s: a join needs \"from\", the name of the collection to match against", where)
		}
		return Join{}, fmt.Errorf(
			"query: %s: \"from\" is a collection name, not %s", where, fromClause.describeKind())
	}
	if collection == "" {
		return Join{}, fmt.Errorf("query: %s: \"from\" must name a collection", where.name("from"))
	}
	if onClause.missing() {
		return Join{}, fmt.Errorf(
			"query: %s: a join needs \"on\", a one-entry {outerPath: innerPath} object", where)
	}
	if onClause.kind() != qObject {
		return Join{}, fmt.Errorf(
			"query: %s: \"on\" is a one-entry {outerPath: innerPath} object, not %s",
			where, onClause.describeKind())
	}
	outerKey, innerValue, ok := onClause.onlyField(c)
	if !ok {
		return Join{}, fmt.Errorf(
			"query: %s: \"on\" holds exactly one entry, the outer path mapped to the inner path, not %d",
			where, onClause.length())
	}
	if outerKey.isOperator() {
		return Join{}, fmt.Errorf(
			"query: %s: the outer side of \"on\" is a path, not the operator %q",
			where.name("on"), outerKey)
	}
	innerPath, ok := innerValue.text(c)
	if !ok {
		return Join{}, fmt.Errorf(
			"query: %s: the inner side of \"on\" is a path or %q, not %s",
			where.name("on").field(outerKey), JoinKey, innerValue.describeKind())
	}
	if innerPath != JoinKey && len(innerPath) > 0 && innerPath[0] == '$' {
		return Join{}, fmt.Errorf(
			"query: %s: unknown inner join target %q: expected a path or %q",
			where.name("on").field(outerKey), innerPath, JoinKey)
	}

	join := JoinOn(collection, outerKey.owned(c), innerPath)
	if !asClause.missing() {
		alias, ok := asClause.text(c)
		if !ok || alias == "" {
			return Join{}, fmt.Errorf(
				"query: %s: \"as\" names the joined collection for the rest of the query "+
					"and must be a non-empty string, not %s", where, asClause.describeKind())
		}
		join = join.As(alias)
	}
	if !whereClause.missing() {
		predicate, err := c.buildPredicate(whereClause, where.name("where"))
		if err != nil {
			return Join{}, err
		}
		join = join.Where(predicate)
	}
	return join, nil
}

// wholeDocument is the default projection: the row itself, the shape a
// document store returns when a query names no columns.
var wholeDocument = Column{agg: aggNone, spec: "", header: "*"}

// buildSelect lowers the select clause into dst. An absent clause projects the
// whole document.
func (c *Compiler) buildSelect(dst []Column, spec qvalue) ([]Column, error) {
	if spec.missing() {
		return append(dst, wholeDocument), nil
	}
	if spec.kind() != qArray {
		column, err := c.buildColumn(spec, at("select"))
		if err != nil {
			return nil, err
		}
		return append(dst, column), nil
	}
	if spec.length() == 0 {
		return nil, fmt.Errorf(
			"query: select: an empty list projects nothing; omit the clause for the whole document")
	}
	dst = reserve(dst, spec.length())
	err := spec.elements(func(index int, element qvalue) error {
		column, err := c.buildColumn(element, at("select").elem(index))
		if err != nil {
			return err
		}
		dst = append(dst, column)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// buildColumn lowers one select entry: a bare path, an aggregate, or either of
// those under an output name.
func (c *Compiler) buildColumn(spec qvalue, where loc) (Column, error) {
	if path, ok := spec.text(c); ok {
		return Path(path), nil
	}
	if spec.kind() != qObject {
		return Column{}, fmt.Errorf(
			"query: %s: a column is a path string or an object, not %s", where, spec.describeKind())
	}
	name, value, ok := spec.onlyField(c)
	if !ok {
		return Column{}, fmt.Errorf(
			"query: %s: a column object holds exactly one entry, an aggregate or an output name, not %d",
			where, spec.length())
	}
	if name.isOperator() {
		return c.buildAggregate(name, value, where)
	}
	named, err := c.buildNamedColumn(value, where.field(name))
	if err != nil {
		return Column{}, err
	}
	named.header = name.owned(c)
	return named, nil
}

// buildNamedColumn lowers the value side of an aliased column.
func (c *Compiler) buildNamedColumn(spec qvalue, where loc) (Column, error) {
	if path, ok := spec.text(c); ok {
		return Path(path), nil
	}
	name, value, ok := spec.onlyField(c)
	if !ok {
		return Column{}, fmt.Errorf(
			"query: %s: a named column is a path string or a one-entry aggregate object", where)
	}
	if !name.isOperator() {
		return Column{}, fmt.Errorf(
			"query: %s: %q is not an aggregate; aggregate names begin with $", where, name)
	}
	return c.buildAggregate(name, value, where)
}

// buildAggregate lowers one $-prefixed reduction.
func (c *Compiler) buildAggregate(op qkey, arg qvalue, where loc) (Column, error) {
	if op.equals("$count") {
		switch arg.kind() {
		case qNull:
			return Count(), nil
		case qBool:
			if enabled, _ := arg.boolean(); !enabled {
				return Column{}, fmt.Errorf("query: %s: $count takes true, a path, or null", where)
			}
			return Count(), nil
		case qString:
			path, _ := arg.text(c)
			if path == "*" {
				return Count(), nil
			}
			return c.countColumn(path), nil
		case qObject:
			if arg.length() == 0 {
				return Count(), nil
			}
		}
		return Column{}, fmt.Errorf(
			"query: %s: $count takes a path, \"*\", {}, true, or null, not %s", where, arg.describeKind())
	}

	// The reduction is named by kind and header spelling rather than by the
	// exported constructor, because the header has to be interned into the
	// text arena and Sum and its siblings concatenate one onto the heap.
	var (
		kind aggKind
		name string
	)
	switch {
	case op.equals("$sum"):
		kind, name = aggSum, "sum"
	case op.equals("$avg"):
		kind, name = aggAvg, "avg"
	case op.equals("$min"):
		kind, name = aggMin, "min"
	case op.equals("$max"):
		kind, name = aggMax, "max"
	default:
		return Column{}, fmt.Errorf(
			"query: %s: unknown aggregate %q: expected $count, $sum, $avg, $min, or $max", where, op)
	}
	path, ok := arg.text(c)
	if !ok {
		return Column{}, fmt.Errorf(
			"query: %s: %s takes a path string, not %s", where, op, arg.describeKind())
	}
	return c.aggColumn(kind, name, path), nil
}

// buildPredicate lowers a filter object. Sibling keys conjoin.
func (c *Compiler) buildPredicate(spec qvalue, where loc) (Predicate, error) {
	if spec.kind() != qObject {
		return Predicate{}, fmt.Errorf(
			"query: %s: a filter is an object, not %s", where, spec.describeKind())
	}
	conjuncts := c.operands(spec.length())
	err := spec.fields(c, func(key qkey, value qvalue) error {
		predicate, err := c.buildFilterEntry(key, value, where)
		if err != nil {
			return err
		}
		conjuncts = append(conjuncts, predicate)
		return nil
	})
	if err != nil {
		return Predicate{}, err
	}
	if len(conjuncts) == 1 {
		return conjuncts[0], nil
	}
	return And(conjuncts...), nil
}

// buildFilterEntry lowers one filter entry: a boolean combinator, or a path
// constrained by a literal or an operator object.
func (c *Compiler) buildFilterEntry(key qkey, value qvalue, where loc) (Predicate, error) {
	switch {
	case key.equals("$and"), key.equals("$or"):
		operands, err := c.buildPredicateList(value, where.field(key))
		if err != nil {
			return Predicate{}, err
		}
		if key.equals("$and") {
			return And(operands...), nil
		}
		return Or(operands...), nil
	case key.equals("$not"):
		inner, err := c.buildPredicate(value, where.field(key))
		if err != nil {
			return Predicate{}, err
		}
		return c.not(inner), nil
	case key.isOperator():
		return Predicate{}, fmt.Errorf(
			"query: %s: unknown operator %q: expected $and, $or, or $not at filter level", where, key)
	}
	return c.buildPathFilter(key.owned(c), value, where.field(key))
}

// buildPredicateList lowers the operand array of $and or $or.
func (c *Compiler) buildPredicateList(spec qvalue, where loc) ([]Predicate, error) {
	if spec.kind() != qArray {
		return nil, fmt.Errorf("query: %s: expected an array of filters, not %s", where, spec.describeKind())
	}
	operands := c.operands(spec.length())
	err := spec.elements(func(index int, element qvalue) error {
		predicate, err := c.buildPredicate(element, where.elem(index))
		if err != nil {
			return err
		}
		operands = append(operands, predicate)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return operands, nil
}

// buildPathFilter lowers the constraint on one path: a bare literal is
// equality (null is the null test), an operator object is the conjunction of
// its operators.
func (c *Compiler) buildPathFilter(path string, spec qvalue, where loc) (Predicate, error) {
	switch spec.kind() {
	case qObject:
		if spec.length() == 0 {
			return Predicate{}, fmt.Errorf("query: %s: an operator object needs at least one operator", where)
		}
		conjuncts := c.operands(spec.length())
		err := spec.fields(c, func(key qkey, value qvalue) error {
			if !key.isOperator() {
				return fmt.Errorf(
					"query: %s: %q is not an operator; address a nested field by its path (%q) "+
						"or test structure with $contains", where, key, path+"."+key.String())
			}
			predicate, err := c.buildOperator(path, key, value, where.field(key))
			if err != nil {
				return err
			}
			conjuncts = append(conjuncts, predicate)
			return nil
		})
		if err != nil {
			return Predicate{}, err
		}
		if len(conjuncts) == 1 {
			return conjuncts[0], nil
		}
		return And(conjuncts...), nil
	case qArray:
		return Predicate{}, fmt.Errorf(
			"query: %s: match one of several values with $in, or an array value with $contains", where)
	case qNull:
		return IsNull(path), nil
	}
	value, err := c.comparableLiteral(spec, where)
	if err != nil {
		return Predicate{}, err
	}
	return Cmp(path, Eq, value), nil
}

// buildOperator lowers one $-prefixed operator applied to path.
func (c *Compiler) buildOperator(path string, name qkey, arg qvalue, where loc) (Predicate, error) {
	switch {
	case name.equals("$eq"), name.equals("$ne"):
		if arg.isNull() {
			if name.equals("$eq") {
				return IsNull(path), nil
			}
			return c.not(IsNull(path)), nil
		}
		value, err := c.comparableLiteral(arg, where)
		if err != nil {
			return Predicate{}, err
		}
		if name.equals("$eq") {
			return Cmp(path, Eq, value), nil
		}
		return Cmp(path, Ne, value), nil

	case name.equals("$lt"), name.equals("$lte"), name.equals("$gt"), name.equals("$gte"):
		value, err := c.comparableLiteral(arg, where)
		if err != nil {
			return Predicate{}, err
		}
		var op Op
		switch {
		case name.equals("$lt"):
			op = Lt
		case name.equals("$lte"):
			op = Le
		case name.equals("$gt"):
			op = Gt
		default:
			op = Ge
		}
		return Cmp(path, op, value), nil

	case name.equals("$in"), name.equals("$nin"):
		if arg.kind() != qArray {
			return Predicate{}, fmt.Errorf(
				"query: %s: %s takes an array, not %s", where, name, arg.describeKind())
		}
		// The comparable alternatives become one In, whose sorted set the
		// executor binary-searches. A null alternative is not a comparison at
		// all — it is the null test — so it joins as a separate disjunct
		// rather than degrading the membership into a chain of equalities.
		values := c.boxes.alloc(arg.length())[:0]
		nullable := false
		err := arg.elements(func(index int, element qvalue) error {
			if element.isNull() {
				nullable = true
				return nil
			}
			value, err := c.comparableLiteral(element, where.elem(index))
			if err != nil {
				return err
			}
			values = append(values, value)
			return nil
		})
		if err != nil {
			return Predicate{}, err
		}
		var membership Predicate
		switch {
		case !nullable:
			membership = In(path, values...)
		case len(values) == 0:
			membership = IsNull(path)
		default:
			membership = Or(c.pair(In(path, values...), IsNull(path))...)
		}
		if name.equals("$in") {
			return membership, nil
		}
		return c.not(membership), nil

	case name.equals("$exists"):
		present, ok := arg.boolean()
		if !ok {
			return Predicate{}, fmt.Errorf(
				"query: %s: $exists takes true or false, not %s", where, arg.describeKind())
		}
		if present {
			return Exists(path), nil
		}
		return c.not(Exists(path)), nil

	case name.equals("$null"):
		isNull, ok := arg.boolean()
		if !ok {
			return Predicate{}, fmt.Errorf(
				"query: %s: $null takes true or false, not %s", where, arg.describeKind())
		}
		if isNull {
			return IsNull(path), nil
		}
		return c.not(IsNull(path)), nil

	case name.equals("$contains"):
		// A parsed needle is already JSON text in the document, so this copies
		// it rather than re-serializing it; a literal needle is rendered.
		needle, err := arg.rawJSON(c, c.needle[:0], where)
		if err != nil {
			return Predicate{}, err
		}
		c.needle = needle
		return Contains(path, c.intern(needle)), nil

	case name.equals("$not"):
		inner, err := c.buildPathFilter(path, arg, where)
		if err != nil {
			return Predicate{}, err
		}
		return c.not(inner), nil

	default:
		return Predicate{}, fmt.Errorf(
			"query: %s: unknown operator %q: expected $eq, $ne, $lt, $lte, $gt, $gte, $in, $nin, "+
				"$exists, $null, $contains, or $not", where, name)
	}
}

// comparableLiteral narrows a document value to one a comparison accepts: a
// string, a bool, or a number. Containers and null are rejected here so the
// caller sees a message naming the operator that would have accepted them.
func (c *Compiler) comparableLiteral(spec qvalue, where loc) (any, error) {
	switch spec.kind() {
	case qBool, qString, qNumber:
		value, ok := spec.literal(c)
		if !ok {
			return nil, fmt.Errorf("query: %s: unreadable literal", where)
		}
		switch number := value.(type) {
		case Number:
			if err := number.validate(); err != nil {
				return nil, fmt.Errorf("query: %s: %w", where, err)
			}
		case *Number:
			if err := number.validate(); err != nil {
				return nil, fmt.Errorf("query: %s: %w", where, err)
			}
		}
		return value, nil
	case qNull:
		return nil, fmt.Errorf("query: %s: compare against null with $null or $exists", where)
	case qObject:
		return nil, fmt.Errorf("query: %s: compare against an object with $contains", where)
	case qArray:
		return nil, fmt.Errorf("query: %s: compare against several values with $in", where)
	default:
		return nil, fmt.Errorf("query: %s: unsupported literal type %s", where, spec.describeKind())
	}
}

// validate reports whether n is one JSON number. Parse only ever produces
// valid spellings; a hand-written Number is checked here so a malformed
// literal fails at compile rather than silently never matching.
//
// It takes no breadcrumb and wraps no prefix, so that nothing on the success
// path — which is every numeric literal in every query — has to be
// materialized. A loc handed in here would escape to the heap for the sake of
// the error branch, one allocation per literal compiled.
func (n Number) validate() error {
	text := string(n)
	if text == "" || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) ||
		!vibejson.Valid(byteview.Bytes(text)) {
		return fmt.Errorf("%q is not a JSON number", text)
	}
	return nil
}

// buildPaths lowers a clause that names one or more paths into dst.
func (c *Compiler) buildPaths(dst []string, spec qvalue, where loc) ([]string, error) {
	if path, ok := spec.text(c); ok {
		return append(dst, path), nil
	}
	if spec.kind() != qArray {
		return nil, fmt.Errorf(
			"query: %s: expected a path or an array of paths, not %s", where, spec.describeKind())
	}
	dst = reserve(dst, spec.length())
	err := spec.elements(func(index int, element qvalue) error {
		path, ok := element.text(c)
		if !ok {
			return fmt.Errorf(
				"query: %s: expected a path string, not %s", where.elem(index), element.describeKind())
		}
		dst = append(dst, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// buildOrderBy lowers the sort clause: a path sorts ascending, and an object
// pairs a path with a direction.
func (c *Compiler) buildOrderBy(q *Query, spec qvalue) error {
	if spec.kind() != qArray {
		return c.buildOrderKey(q, spec, at("orderBy"))
	}
	q.orderBy = reserve(q.orderBy, spec.length())
	return spec.elements(func(index int, element qvalue) error {
		return c.buildOrderKey(q, element, at("orderBy").elem(index))
	})
}

// buildOrderKey lowers one sort key.
func (c *Compiler) buildOrderKey(q *Query, spec qvalue, where loc) error {
	if path, ok := spec.text(c); ok {
		q.OrderBy(path, Asc)
		return nil
	}
	name, value, ok := spec.onlyField(c)
	if !ok {
		return fmt.Errorf(
			"query: %s: expected a path string or a one-entry {path: direction} object", where)
	}
	direction, err := c.buildDirection(value, where.field(name))
	if err != nil {
		return err
	}
	q.OrderBy(name.owned(c), direction)
	return nil
}

// buildDirection reads a sort direction as "asc"/"desc" or as 1/-1.
func (c *Compiler) buildDirection(spec qvalue, where loc) (Direction, error) {
	if text, ok := spec.text(c); ok {
		switch strings.ToLower(text) {
		case "asc", "ascending":
			return Asc, nil
		case "desc", "descending":
			return Desc, nil
		}
		return Asc, fmt.Errorf(
			"query: %s: unknown sort direction %q: expected \"asc\" or \"desc\"", where, text)
	}
	n, ok := spec.wholeNumberOf()
	if !ok {
		return Asc, fmt.Errorf(
			"query: %s: expected \"asc\", \"desc\", 1, or -1, not %s", where, spec.describeKind())
	}
	switch n {
	case 1:
		return Asc, nil
	case -1:
		return Desc, nil
	}
	return Asc, fmt.Errorf("query: %s: expected 1 or -1 for a numeric sort direction, not %d", where, n)
}

// wholeNumber narrows a Go numeric literal to an integer.
func wholeNumber(spec any) (int64, bool) {
	switch value := spec.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		// Widened before comparing: uint is 32 bits on some platforms, where
		// the untyped bound would overflow it and fail to compile.
		return int64(value), uint64(value) <= 1<<62
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), value <= 1<<62
	case float32:
		return int64(value), float64(value) == float64(int64(value))
	case float64:
		return int64(value), value == float64(int64(value))
	case Number:
		n, err := strconv.ParseInt(string(value), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

// appendJSONLiteral renders a Go literal as JSON text, the form [Contains]
// takes. Numbers keep their exact spelling.
func (c *Compiler) appendJSONLiteral(dst []byte, spec any, where loc) ([]byte, error) {
	switch value := spec.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, value), nil
	case string:
		return appendJSONString(dst, value), nil
	case Number:
		if err := value.validate(); err != nil {
			return nil, fmt.Errorf("query: %s: %w", where, err)
		}
		return append(dst, value...), nil
	}
	if object, ok := asObject(spec); ok {
		dst = append(dst, '{')
		keys, mark := c.pushSortedKeys(object)
		for i, key := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			var err error
			dst, err = c.appendJSONLiteral(dst, object[key], where)
			if err != nil {
				c.keys = c.keys[:mark]
				return nil, err
			}
		}
		c.keys = c.keys[:mark]
		return append(dst, '}'), nil
	}
	if elements, ok := asArray(spec); ok {
		dst = append(dst, '[')
		for i, element := range elements {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = c.appendJSONLiteral(dst, element, where)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	}
	if out, ok := appendNumericText(dst, spec); ok {
		return out, nil
	}
	return nil, fmt.Errorf("query: %s: unsupported JSON literal type %T", where, spec)
}

// appendNumericText appends a Go numeric value's exact JSON spelling. It
// appends rather than formatting to a string because a needle is assembled
// into one buffer, and a string per number was an allocation for storage
// thrown away on the next append.
func appendNumericText(dst []byte, spec any) ([]byte, bool) {
	switch value := spec.(type) {
	case int:
		return strconv.AppendInt(dst, int64(value), 10), true
	case int8:
		return strconv.AppendInt(dst, int64(value), 10), true
	case int16:
		return strconv.AppendInt(dst, int64(value), 10), true
	case int32:
		return strconv.AppendInt(dst, int64(value), 10), true
	case int64:
		return strconv.AppendInt(dst, value, 10), true
	case uint:
		return strconv.AppendUint(dst, uint64(value), 10), true
	case uint8:
		return strconv.AppendUint(dst, uint64(value), 10), true
	case uint16:
		return strconv.AppendUint(dst, uint64(value), 10), true
	case uint32:
		return strconv.AppendUint(dst, uint64(value), 10), true
	case uint64:
		return strconv.AppendUint(dst, value, 10), true
	case float32:
		return strconv.AppendFloat(dst, float64(value), 'g', -1, 32), true
	case float64:
		return strconv.AppendFloat(dst, value, 'g', -1, 64), true
	default:
		return dst, false
	}
}

// isNumericLiteral reports whether spec is one of the Go numeric spellings a
// query document accepts.
func isNumericLiteral(spec any) bool {
	switch spec.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

// asObject accepts both M and a plain map[string]any, so a document decoded by
// another JSON package needs no conversion.
func asObject(spec any) (M, bool) {
	switch value := spec.(type) {
	case M:
		return value, true
	case map[string]any:
		return M(value), true
	default:
		return nil, false
	}
}

// asArray accepts both A and a plain []any.
func asArray(spec any) (A, bool) {
	switch value := spec.(type) {
	case A:
		return value, true
	case []any:
		return A(value), true
	default:
		return nil, false
	}
}

// pushSortedKeys orders a literal object's keys so a document built from a Go
// map lowers to the same predicate tree on every run. Conjunction and
// disjunction are commutative, so the order is free to choose and worth
// fixing. A parsed document needs none of this: its member order is the
// document's own.
//
// The names are pushed onto one shared stack and the caller truncates back to
// the returned mark, because the walk that needs them recurses: a fresh slice
// per object was one allocation per object in every Go-literal document, and a
// single flat reused slice would let a nested object overwrite the names its
// parent had not finished visiting.
func (c *Compiler) pushSortedKeys(object M) (keys []string, mark int) {
	mark = len(c.keys)
	for key := range object {
		c.keys = append(c.keys, key)
	}
	// Resliced only after the last append, so a growth that moves the backing
	// array cannot leave keys viewing the old one.
	keys = c.keys[mark:]
	slices.Sort(keys)
	return keys, mark
}
