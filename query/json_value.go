package query

import (
	"bytes"
	"fmt"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The reading half of the JSON document front end.
//
// A query document reaches the lowering in one of two forms: Go literals (M,
// A, and scalars) or a parsed JSON document. Lowering them separately would
// mean two implementations of one grammar, and converting the parsed form into
// literals — which is what this front end originally did — costs one map per
// object and one slice per array before any compilation begins: 62 allocations
// and roughly 3.8 KB for an ordinary query, spent only to be thrown away.
//
// qvalue is the single reading surface both forms present. It is a concrete
// struct with a discriminator rather than an interface: a Node is several
// words wide, so boxing one into an interface allocates per value and would
// reintroduce exactly the cost this removes. Traversing a parsed document
// through qvalue allocates nothing — ObjectIter and ArrayIter are index walks,
// and member names are compared against the source bytes in place.
//
// Everything the compiled plan retains is copied out of the document, so
// borrowing the caller's source is safe: makeLiteral copies a number's
// spelling, and paths, aliases, and containment needles are copied where they
// are retained.

// A qkind is the JSON kind of a query-document value, uniform across backings.
type qkind uint8

const (
	qInvalid qkind = iota
	qNull
	qBool
	qNumber
	qString
	qArray
	qObject
)

// A qvalue is one value in a query document, backed either by a Go literal or
// by a node in a parsed document.
type qvalue struct {
	lit    any
	node   vibejson.Node
	isNode bool
	kindOf qkind
}

// litValue wraps a Go literal.
func litValue(v any) qvalue { return qvalue{lit: v, kindOf: literalKind(v)} }

// nodeValue wraps a node in a parsed document.
func nodeValue(n vibejson.Node) qvalue {
	return qvalue{node: n, isNode: true, kindOf: nodeKind(n)}
}

func literalKind(v any) qkind {
	switch v.(type) {
	case nil:
		return qNull
	case bool:
		return qBool
	case string:
		return qString
	case Number:
		return qNumber
	}
	if _, ok := asObject(v); ok {
		return qObject
	}
	if _, ok := asArray(v); ok {
		return qArray
	}
	if isNumericLiteral(v) {
		return qNumber
	}
	return qInvalid
}

func nodeKind(n vibejson.Node) qkind {
	switch n.Kind() {
	case document.Null:
		return qNull
	case document.Bool:
		return qBool
	case document.Number:
		return qNumber
	case document.String:
		return qString
	case document.Array:
		return qArray
	case document.Object:
		return qObject
	default:
		return qInvalid
	}
}

func (q qvalue) kind() qkind  { return q.kindOf }
func (q qvalue) isNull() bool { return q.kindOf == qNull }

// missing reports whether a clause was left unspecified. The zero qvalue is
// what an absent clause lowers to, and an explicit JSON null means the same
// thing as omitting the clause, so both answer true.
func (q qvalue) missing() bool { return q.kindOf == qInvalid || q.kindOf == qNull }

// boolean returns the value of a JSON boolean.
func (q qvalue) boolean() (bool, bool) {
	if q.kindOf != qBool {
		return false, false
	}
	if q.isNode {
		return q.node.Bool()
	}
	b, ok := q.lit.(bool)
	return b, ok
}

// text returns a JSON string's decoded content as a string the plan may
// retain. A literal-backed string is already owned by the caller's document
// and is returned as is; a parsed one is interned into the compiler's text
// arena, which it would need to be in order to outlive the document anyway.
func (q qvalue) text(c *Compiler) (string, bool) {
	if q.kindOf != qString {
		return "", false
	}
	if !q.isNode {
		s, ok := q.lit.(string)
		return s, ok
	}
	raw, ok := q.node.StringBytes()
	if !ok {
		return "", false
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return c.intern(raw), true
	}
	decoded, ok := q.node.AppendText(c.scratch[:0])
	if !ok {
		return "", false
	}
	c.scratch = decoded
	return c.intern(decoded), true
}

// literal returns the value as something makeLiteral accepts.
//
// A literal-backed value hands back the caller's own box, so the common
// document written as Go literals costs nothing here at all. A parsed one is
// interned and then handed across as a pointer: converting a string or a
// Number to an any copies its header onto the heap, which would be one
// allocation per WHERE literal, while a pointer-shaped value needs no box.
// makeLiteral reads *string and *Number for exactly this reason.
func (q qvalue) literal(c *Compiler) (any, bool) {
	if !q.isNode {
		switch q.kindOf {
		case qBool, qString, qNumber:
			return q.lit, true
		}
		return nil, false
	}
	switch q.kindOf {
	case qBool:
		b, ok := q.node.Bool()
		if !ok {
			return nil, false
		}
		// The two boxed booleans are package values, because boxing a bool
		// read out of a variable is not guaranteed to reuse a static one.
		if b {
			return boxedTrue, true
		}
		return boxedFalse, true
	case qString:
		s, ok := q.text(c)
		if !ok {
			return nil, false
		}
		box := c.strs.one()
		*box = s
		return box, true
	case qNumber:
		text, ok := q.node.NumberText()
		if !ok {
			return nil, false
		}
		box := c.nums.one()
		*box = Number(c.internString(text))
		return box, true
	default:
		return nil, false
	}
}

// boxedTrue and boxedFalse are the two boolean literals, boxed once. They are
// never written after initialization.
var (
	boxedTrue  any = true
	boxedFalse any = false
)

// rawJSON appends the value's JSON text. A parsed value already has one in the
// document, so a containment needle read from JSON text needs no
// re-serialization at all; a literal is rendered.
func (q qvalue) rawJSON(c *Compiler, dst []byte, at loc) ([]byte, error) {
	if q.isNode {
		return append(dst, q.node.Raw().Bytes()...), nil
	}
	return c.appendJSONLiteral(dst, q.lit, at)
}

// length returns the member or element count of an object or array.
func (q qvalue) length() int {
	if !q.isNode {
		if object, ok := asObject(q.lit); ok {
			return len(object)
		}
		if elements, ok := asArray(q.lit); ok {
			return len(elements)
		}
		return 0
	}
	if n, ok := q.node.ObjectLen(); ok {
		return n
	}
	if n, ok := q.node.ArrayLen(); ok {
		return n
	}
	return 0
}

// describeKind names a value's JSON kind for an error message.
func (q qvalue) describeKind() string {
	switch q.kindOf {
	case qNull:
		return "null"
	case qBool:
		return "a boolean"
	case qNumber:
		return "a number"
	case qString:
		return "a string"
	case qArray:
		return "an array"
	case qObject:
		return "an object"
	default:
		if q.isNode {
			return "an unsupported value"
		}
		return fmt.Sprintf("%T", q.lit)
	}
}

// A qkey is an object member's name. It compares without allocating, and
// allocates only where the name is retained.
type qkey struct {
	s      string
	b      []byte
	isNode bool
	// viaScratch marks a name that was unescaped into the shared scratch
	// buffer rather than read in place from the document. Only such a name can
	// be overwritten by a later member, so only such a name must be copied to
	// outlive the visit that produced it.
	viaScratch bool
}

// equals compares the name against a constant, allocating nothing in either
// backing.
func (k qkey) equals(s string) bool {
	if k.isNode {
		return vibejson.BytesEqualString(k.b, s)
	}
	return k.s == s
}

// isOperator reports whether the name is $-prefixed.
func (k qkey) isOperator() bool {
	if k.isNode {
		return len(k.b) > 0 && k.b[0] == '$'
	}
	return len(k.s) > 0 && k.s[0] == '$'
}

// owned returns the name as a string the plan may retain, interning a
// document-backed one into the compiler's text arena.
func (k qkey) owned(c *Compiler) string {
	if k.isNode {
		return c.intern(k.b)
	}
	return k.s
}

// String renders the name for an error message. It copies rather than
// interning, because an error outlives the compilation that produced it and
// must not be left describing whatever the next compile wrote over the arena.
func (k qkey) String() string {
	if k.isNode {
		return string(k.b)
	}
	return k.s
}

// fields visits an object's members, reporting the first error fn returns.
//
// A parsed object is visited in document order, which is already stable. A
// literal object is visited in sorted key order, because Go randomizes map
// iteration and a plan whose predicate tree reordered itself between
// compilations of one document would not be reproducible.
func (q qvalue) fields(c *Compiler, fn func(key qkey, value qvalue) error) error {
	if q.kindOf != qObject {
		return nil
	}
	if !q.isNode {
		object, _ := asObject(q.lit)
		keys, base := c.pushSortedKeys(object)
		defer func() { c.keys = c.keys[:base] }()
		for _, name := range keys {
			if err := fn(qkey{s: name}, litValue(object[name])); err != nil {
				return err
			}
		}
		return nil
	}
	iterator, ok := q.node.ObjectIter()
	if !ok {
		return nil
	}
	for {
		key, value, more := iterator.Next()
		if !more {
			return nil
		}
		name, viaScratch, err := objectKeyBytes(key, &c.scratch)
		if err != nil {
			return err
		}
		if err := fn(qkey{b: name, isNode: true, viaScratch: viaScratch}, nodeValue(value)); err != nil {
			return err
		}
	}
}

// objectKeyBytes reads a member name, unescaping only when it has escapes. It
// reports whether the result came from scratch, which is what tells a caller
// holding the name past this visit that it must copy first. An unescaped name
// borrows the document, which outlives the whole compilation.
func objectKeyBytes(key vibejson.Node, scratch *[]byte) ([]byte, bool, error) {
	raw, ok := key.StringBytes()
	if !ok {
		return nil, false, fmt.Errorf("query: unreadable object key in query document")
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw, false, nil
	}
	decoded, ok := key.AppendText((*scratch)[:0])
	if !ok {
		return nil, false, fmt.Errorf("query: unreadable object key in query document")
	}
	*scratch = decoded
	return decoded, true, nil
}

// onlyField returns a one-member object's single member, with a name that stays
// valid past the visit.
//
// Most such names are operators — $sum, $in, $count — that are only compared
// and never retained, so copying every one of them was the single largest
// remaining source of allocations in Parse. A name read in place from the
// document already outlives the compilation and is returned as is; only one
// unescaped into shared scratch is copied, because a later member would
// overwrite it. Callers that retain a name still call owned().
func (q qvalue) onlyField(c *Compiler) (qkey, qvalue, bool) {
	if q.kindOf != qObject || q.length() != 1 {
		return qkey{}, qvalue{}, false
	}
	var (
		name  qkey
		value qvalue
	)
	// The object holds exactly one member, so the visit runs exactly once and
	// cannot return an error from fn.
	if err := q.fields(c, func(k qkey, v qvalue) error {
		if k.viaScratch {
			k = qkey{s: k.owned(c)}
		}
		name, value = k, v
		return nil
	}); err != nil {
		return qkey{}, qvalue{}, false
	}
	return name, value, true
}

// elements visits an array's elements, reporting the first error fn returns.
func (q qvalue) elements(fn func(index int, value qvalue) error) error {
	if q.kindOf != qArray {
		return nil
	}
	if !q.isNode {
		list, _ := asArray(q.lit)
		for i, element := range list {
			if err := fn(i, litValue(element)); err != nil {
				return err
			}
		}
		return nil
	}
	iterator, ok := q.node.ArrayIter()
	if !ok {
		return nil
	}
	for i := 0; ; i++ {
		element, more := iterator.Next()
		if !more {
			return nil
		}
		if err := fn(i, nodeValue(element)); err != nil {
			return err
		}
	}
}

// wholeNumberOf narrows a value to an integer, accepting every Go numeric
// spelling and any number whose value has no fraction.
func (q qvalue) wholeNumberOf() (int64, bool) {
	if q.kindOf != qNumber {
		return 0, false
	}
	if !q.isNode {
		return wholeNumber(q.lit)
	}
	if i, ok := q.node.Int64(); ok {
		return i, true
	}
	f, ok := q.node.Float64()
	if !ok {
		return 0, false
	}
	return int64(f), f == float64(int64(f))
}
