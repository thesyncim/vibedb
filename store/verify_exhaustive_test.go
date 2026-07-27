package store

// Exhaustive differential testing over a bounded input domain.
//
// This file enumerates every well-formed JSON document up to a small depth and
// node bound over a fixed terminal alphabet, and checks — on every enumerated
// document — that the novel index representations are observably identical to
// the classic reference tape. It is exhaustive over that bounded domain, not a
// proof of correctness for all inputs: the enumerated domain size, logged by
// each test, is the strength of the evidence, and raising the bound constants
// widens the domain the checks cover.
//
// The generator (exhaustiveGenerate) produces both the exact JSON bytes and an
// independent abstract syntax tree (exhaustiveValue) for every document. The
// AST is the from-scratch reference: it is built by the enumerator, never by
// the library, so comparing a parsed index against it is a genuine cross-check
// rather than a tautology. Four properties are checked:
//
//  1. Narrow shape-tape versus classic (TestExhaustiveEquivalence): a
//     shape-deduplicated Segment holding conforming documents in the 8-byte
//     narrow width widens to the byte-identical classic tape, and every
//     accessor over it agrees with the classic reference and the AST.
//  2. Wide shape-tape versus classic (same test, wideValueTapes seam): the
//     16-byte dedup width, forced on the identical documents, widens to the
//     same classic tape.
//  3. Pointer/navigation two-rule invariant (same test): every reachable JSON
//     Pointer resolves to the node an independent recursive descent reaches,
//     across the classic index, compiled pointers, and the batch
//     AppendPointer path that reads the narrow slab directly.
//  4. Containment versus an independent reference (TestExhaustiveContainsVsReference):
//     for every ordered pair of documents in a smaller bound, Node.Contains
//     and RawContains agree with a from-scratch inductive definition of
//     PostgreSQL's jsonb @> operator written here as exhaustiveContains.
//
// A disagreement on any enumerated input is a real bug; each test reports the
// minimal counterexample the enumeration reached. The alphabet and bounds are
// the tunable surface; see the bexMain* and bexPair* constants below.

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The enumeration bounds. Each is a raisable constant: widening any of them
// enlarges the enumerated domain the checks cover. depth bounds nesting, nodes
// bounds the total value count, and width caps the direct members of a
// container. At the default main bound the width cap does not bind — the node
// bound is the sole limit — so the default enumerates the genuine, unrestricted
// space of documents with depth <= bexMainDepth and at most bexMainNodes value
// nodes. The width cap exists only to keep a raised node bound from exploding
// faster than intended; see correctness-checks.md for the combinatorics.
const (
	bexMainDepth = 3
	bexMainNodes = 4
	bexMainWidth = 3

	bexPairDepth = 3
	bexPairNodes = 3
	bexPairWidth = 2
)

// bexDomainCeiling fails the enumeration rather than running for hours if the
// bound constants are raised without accounting for the combinatorial blowup.
// A raised bound that legitimately exceeds it should raise this constant too.
const bexDomainCeiling = 6_000_000

// exhaustiveValue is one node of the independent AST the enumerator builds
// alongside a document's bytes. It is the specification every parsed index is
// checked against; nothing in the library constructs it. A scalar carries its
// exact spelling and decoded value; a container carries its ordered children,
// including duplicate object keys in document order.
type exhaustiveValue struct {
	kind document.Kind
	json []byte // exact compact JSON serialization of this value

	numRaw  string // Number: the source spelling
	strDec  string // String: decoded content
	strEsc  bool   // String: the spelling carries escape sequences
	boolVal bool   // Bool: the value

	elems []*exhaustiveValue // Array: ordered elements
	keys  []string           // Object: ordered decoded key names (aligns with vals)
	vals  []*exhaustiveValue // Object: ordered member values

	nodes int // value-node count: scalars and empty containers 1, else 1+sum(children)
	depth int // nesting depth: scalars and empty containers 1, else 1+max(child depth)
}

// bexKeyAlphabet is the object key alphabet. Duplicates in a document arise
// from an ordered key sequence repeating a name; the empty key is included so
// the "" pointer token and empty-key lookups are exercised.
var bexKeyAlphabet = []string{"a", "b", ""}

// exhaustiveTerminals returns the fixed terminal alphabet: every scalar
// spelling plus the two empty containers, each a single-node, depth-one value.
// The returned values are shared across the enumeration and never mutated.
func exhaustiveTerminals() []*exhaustiveValue {
	terms := []*exhaustiveValue{
		bexNumber("0"), bexNumber("-1"), bexNumber("1"),
		bexNumber("10"), bexNumber("1.5"), bexNumber("1e2"),
		bexString(`""`, "", false),
		bexString(`"a"`, "a", false),
		bexString(`"ab"`, "ab", false),
		bexString(`"a\n"`, "a\n", true),
		bexString(`"\uD83D\uDE00"`, "\U0001F600", true),
		bexBool(true), bexBool(false), bexNull(),
		bexEmptyArray(), bexEmptyObject(),
	}
	return terms
}

func bexNumber(spelling string) *exhaustiveValue {
	return &exhaustiveValue{kind: document.Number, json: []byte(spelling), numRaw: spelling, nodes: 1, depth: 1}
}

func bexString(jsonSpelling, decoded string, escaped bool) *exhaustiveValue {
	return &exhaustiveValue{kind: document.String, json: []byte(jsonSpelling), strDec: decoded, strEsc: escaped, nodes: 1, depth: 1}
}

func bexBool(v bool) *exhaustiveValue {
	s := "false"
	if v {
		s = "true"
	}
	return &exhaustiveValue{kind: document.Bool, json: []byte(s), boolVal: v, nodes: 1, depth: 1}
}

func bexNull() *exhaustiveValue {
	return &exhaustiveValue{kind: document.Null, json: []byte("null"), nodes: 1, depth: 1}
}

func bexEmptyArray() *exhaustiveValue {
	return &exhaustiveValue{kind: document.Array, json: []byte("[]"), nodes: 1, depth: 1}
}

func bexEmptyObject() *exhaustiveValue {
	return &exhaustiveValue{kind: document.Object, json: []byte("{}"), nodes: 1, depth: 1}
}

// bexMakeArray builds a non-empty array value from an ordered element list.
func bexMakeArray(elems []*exhaustiveValue) *exhaustiveValue {
	var buf bytes.Buffer
	buf.WriteByte('[')
	nodes, depth := 1, 1
	for i, e := range elems {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(e.json)
		nodes += e.nodes
		if e.depth+1 > depth {
			depth = e.depth + 1
		}
	}
	buf.WriteByte(']')
	return &exhaustiveValue{kind: document.Array, json: buf.Bytes(), elems: elems, nodes: nodes, depth: depth}
}

// bexMakeObject builds a non-empty object value from ordered keys and values.
// Keys are emitted with plain quotes; the alphabet contains no character that
// requires escaping, so the decoded key equals the spelled key.
func bexMakeObject(keys []string, vals []*exhaustiveValue) *exhaustiveValue {
	var buf bytes.Buffer
	buf.WriteByte('{')
	nodes, depth := 1, 1
	for i, v := range vals {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(keys[i])
		buf.WriteString(`":`)
		buf.Write(v.json)
		nodes += v.nodes
		if v.depth+1 > depth {
			depth = v.depth + 1
		}
	}
	buf.WriteByte('}')
	ks := append([]string(nil), keys...)
	return &exhaustiveValue{kind: document.Object, json: buf.Bytes(), keys: ks, vals: vals, nodes: nodes, depth: depth}
}

// exhaustiveGenerate enumerates every document with depth <= maxDepth, at most
// maxNodes value nodes, and at most maxWidth direct members per container. The
// result is deterministic and free of duplicate serializations.
func exhaustiveGenerate(maxDepth, maxNodes, maxWidth int) []*exhaustiveValue {
	memo := map[[2]int][]*exhaustiveValue{}
	var gen func(depth, nodes int) []*exhaustiveValue
	gen = func(depth, nodes int) []*exhaustiveValue {
		if depth < 1 || nodes < 1 {
			return nil
		}
		key := [2]int{depth, nodes}
		if v, ok := memo[key]; ok {
			return v
		}
		out := exhaustiveTerminals()
		if depth >= 2 && nodes >= 2 {
			children := gen(depth-1, nodes-1)
			seqs := bexChildSeqs(children, maxWidth, nodes-1)
			for _, seq := range seqs {
				out = append(out, bexMakeArray(seq))
				for _, keys := range bexKeyAssignments(len(seq)) {
					out = append(out, bexMakeObject(keys, seq))
				}
			}
		}
		memo[key] = out
		return out
	}
	return gen(maxDepth, maxNodes)
}

// bexChildSeqs enumerates every ordered non-empty child sequence with length
// in [1, width] whose total node count is at most budget.
func bexChildSeqs(children []*exhaustiveValue, width, budget int) [][]*exhaustiveValue {
	var all [][]*exhaustiveValue
	frontier := [][]*exhaustiveValue{nil}
	for l := 1; l <= width; l++ {
		var next [][]*exhaustiveValue
		for _, pre := range frontier {
			used := 0
			for _, c := range pre {
				used += c.nodes
			}
			for _, c := range children {
				if used+c.nodes > budget {
					continue
				}
				seq := make([]*exhaustiveValue, len(pre)+1)
				copy(seq, pre)
				seq[len(pre)] = c
				next = append(next, seq)
				all = append(all, seq)
			}
		}
		frontier = next
		if len(frontier) == 0 {
			break
		}
	}
	return all
}

// bexKeyAssignments enumerates every length-n key sequence over the key
// alphabet, so an object's members range over all orderings and duplications
// of the available keys.
func bexKeyAssignments(n int) [][]string {
	total := 1
	for i := 0; i < n; i++ {
		total *= len(bexKeyAlphabet)
	}
	out := make([][]string, 0, total)
	for a := 0; a < total; a++ {
		ks := make([]string, n)
		x := a
		for i := 0; i < n; i++ {
			ks[i] = bexKeyAlphabet[x%len(bexKeyAlphabet)]
			x /= len(bexKeyAlphabet)
		}
		out = append(out, ks)
	}
	return out
}

// bexEffectiveMembers returns each distinct object key mapped to its last
// member value, the effective value jsonb keeps and Get resolves. Keys are
// returned in first-seen order for deterministic reporting.
func bexEffectiveMembers(o *exhaustiveValue) (order []string, last map[string]*exhaustiveValue) {
	last = map[string]*exhaustiveValue{}
	for i, k := range o.keys {
		if _, seen := last[k]; !seen {
			order = append(order, k)
		}
		last[k] = o.vals[i]
	}
	return order, last
}

// TestExhaustiveEquivalence checks properties 1, 2, and 4 over the full main
// bound: shape-tape widening (narrow and wide widths) is byte-identical to the
// classic tape, every accessor over the classic representation agrees with the
// independent AST, and every reachable pointer resolves as an independent
// recursive descent says it must.
func TestExhaustiveEquivalence(t *testing.T) {
	// -short (and the race/checkptr instrumentation runs that pass it) covers a
	// smaller bound; the full domain is the default. See testIterations.
	depth, nodes, width := bexMainDepth, testIterations(bexMainNodes, 3), testIterations(bexMainWidth, 2)
	docs := exhaustiveGenerate(depth, nodes, width)
	if len(docs) > bexDomainCeiling {
		t.Fatalf("enumerated domain %d exceeds ceiling %d; raise bexDomainCeiling deliberately", len(docs), bexDomainCeiling)
	}

	var narrowTaped, wideTaped, classicStored, pointers int
	for _, hashKeys := range []bool{false, true} {
		opts := document.IndexOptions{HashKeys: hashKeys}
		for _, doc := range docs {
			n, w, c, p := bexCheckDocument(t, doc, opts)
			if !hashKeys { // count the domain once
				narrowTaped += n
				wideTaped += w
				classicStored += c
				pointers += p
			}
			if t.Failed() {
				return
			}
		}
	}

	t.Logf("main bound depth<=%d nodes<=%d width<=%d: %d documents enumerated (x2 for HashKeys off/on)",
		depth, nodes, width, len(docs))
	t.Logf("shape-tape storage exercised: %d narrow-taped, %d wide-taped, %d classic-stored documents", narrowTaped, wideTaped, classicStored)
	t.Logf("pointer/navigation invariant: %d reachable pointers resolved per option", pointers)
}

// bexCheckDocument runs every per-document property for one document under one
// option set and returns the storage classification for reporting.
func bexCheckDocument(t *testing.T, doc *exhaustiveValue, opts document.IndexOptions) (narrow, wide, classic, pointers int) {
	t.Helper()
	src := doc.json

	// The classic reference: a fresh, exactly sized BuildIndex.
	need, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		t.Fatalf("RequiredIndexEntries(%s): %v", src, err)
		return
	}
	ref, err := vibejson.BuildIndexOptions(src, make([]vibejson.IndexEntry, need), opts)
	if err != nil {
		t.Fatalf("BuildIndexOptions(%s): %v", src, err)
		return
	}

	// Property 1/4 semantic core: the classic representation matches the AST.
	bexCheckNode(t, ref.Root(), doc, string(src))
	pointers = bexCheckPointers(t, ref, doc, src)

	// Property 1: the narrow shape-tape widens to the byte-identical classic
	// tape. A conforming document dedups on its second sighting.
	narrowSet := &Segment{Options: opts, ShapeTapes: true}
	if _, err := narrowSet.Append(src); err != nil {
		t.Fatalf("narrow Append(%s): %v", src, err)
	}
	if _, err := narrowSet.Append(src); err != nil {
		t.Fatalf("narrow Append(%s): %v", src, err)
	}
	bexAssertWidenEqual(t, narrowSet, ref, doc, "narrow")
	if r := narrowSet.ShapeTapeRefAt(1); r.Rec != nil {
		if !r.Narrow {
			t.Fatalf("doc %s stored dedup but not narrow width", src)
		}
		narrow = 1
		bexCheckAppendPointer(t, narrowSet, doc, src)
	} else {
		classic = 1
	}

	// Property 2: the wide dedup width, forced by the test seam on the same
	// document, widens to the same classic tape.
	wideSet := &Segment{Options: opts, ShapeTapes: true, wideValueTapes: true}
	if _, err := wideSet.Append(src); err != nil {
		t.Fatalf("wide Append(%s): %v", src, err)
	}
	if _, err := wideSet.Append(src); err != nil {
		t.Fatalf("wide Append(%s): %v", src, err)
	}
	bexAssertWidenEqual(t, wideSet, ref, doc, "wide")
	if r := wideSet.ShapeTapeRefAt(1); r.Rec != nil {
		if r.Narrow {
			t.Fatalf("wide seam left doc %s narrow", src)
		}
		wide = 1
		bexCheckAppendPointer(t, wideSet, doc, src)
	}
	return narrow, wide, classic, pointers
}

// bexAssertWidenEqual checks every stored copy of a document in a shape-tape
// Segment widens to the classic reference: identical entries and identical
// source bytes. Both the first-sighting classic copy and the deduplicated
// copies must agree, which pins the widening synthesis byte for byte.
func bexAssertWidenEqual(t *testing.T, set *Segment, ref vibejson.Index, doc *exhaustiveValue, label string) {
	t.Helper()
	for i := 0; i < set.Len(); i++ {
		got := set.Doc(i)
		if !bytes.Equal(got.Src, ref.Src) {
			t.Fatalf("%s: Doc(%d) src %q != classic %q", label, i, got.Src, ref.Src)
		}
		if !reflect.DeepEqual(got.Entries, ref.Entries) {
			t.Fatalf("%s: Doc(%d) of %s widened to entries %v, classic %v", label, i, doc.json, got.Entries, ref.Entries)
		}
		// A second call must return the same cached storage.
		if again := set.Doc(i); len(again.Entries) > 0 && &again.Entries[0] != &got.Entries[0] {
			t.Fatalf("%s: Doc(%d) returned fresh storage on the second call", label, i)
		}
	}
}

// bexCheckNode recursively asserts that a parsed Node exposes exactly the
// value the AST specifies through every accessor: kind, raw bytes, scalar
// conversions, container lengths, indexed and keyed lookups, and iteration.
// Cross-kind accessors are checked to reject, so no accessor silently accepts
// the wrong kind.
func bexCheckNode(t *testing.T, n vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	if got := n.Kind(); got != a.kind {
		t.Fatalf("%s: Kind %v, want %v", path, got, a.kind)
	}
	if got := n.Raw().Bytes(); !bytes.Equal(got, a.json) {
		t.Fatalf("%s: Raw %q, want %q", path, got, a.json)
	}
	switch a.kind {
	case document.Null:
		if !n.IsNull() {
			t.Fatalf("%s: IsNull false for null", path)
		}
		bexRejectContainers(t, n, path)
	case document.Bool:
		if b, ok := n.Bool(); !ok || b != a.boolVal {
			t.Fatalf("%s: Bool = (%v,%v), want (%v,true)", path, b, ok, a.boolVal)
		}
		if n.IsNull() {
			t.Fatalf("%s: IsNull true for bool", path)
		}
		bexRejectContainers(t, n, path)
	case document.Number:
		bexCheckNumber(t, n, a, path)
		bexRejectContainers(t, n, path)
	case document.String:
		bexCheckString(t, n, a, path)
		bexRejectContainers(t, n, path)
	case document.Array:
		l, ok := n.ArrayLen()
		if !ok || l != len(a.elems) {
			t.Fatalf("%s: ArrayLen = (%d,%v), want %d", path, l, ok, len(a.elems))
		}
		if _, ok := n.ObjectLen(); ok {
			t.Fatalf("%s: ObjectLen ok on array", path)
		}
		if _, ok := n.Get("a"); ok {
			t.Fatalf("%s: Get ok on array", path)
		}
		if _, ok := n.Index(-1); ok {
			t.Fatalf("%s: Index(-1) ok on array", path)
		}
		if _, ok := n.Index(len(a.elems)); ok {
			t.Fatalf("%s: Index(len) ok on array", path)
		}
		for i, e := range a.elems {
			c, ok := n.Index(i)
			if !ok {
				t.Fatalf("%s: Index(%d) absent", path, i)
			}
			bexCheckNode(t, c, e, fmt.Sprintf("%s/%d", path, i))
		}
		bexCheckArrayIter(t, n, a, path)
	case document.Object:
		l, ok := n.ObjectLen()
		if !ok || l != len(a.keys) {
			t.Fatalf("%s: ObjectLen = (%d,%v), want %d", path, l, ok, len(a.keys))
		}
		if _, ok := n.ArrayLen(); ok {
			t.Fatalf("%s: ArrayLen ok on object", path)
		}
		if _, ok := n.Index(0); ok {
			t.Fatalf("%s: Index ok on object", path)
		}
		if _, ok := n.Get("no-such-key"); ok {
			t.Fatalf("%s: Get of absent key ok", path)
		}
		order, last := bexEffectiveMembers(a)
		for _, k := range order {
			g, ok := n.Get(k)
			if !ok {
				t.Fatalf("%s: Get(%q) absent", path, k)
			}
			bexCheckNode(t, g, last[k], fmt.Sprintf("%s/%s", path, k))
		}
		bexCheckObjectIter(t, n, a, path)
	}
}

// bexRejectContainers asserts scalar Nodes reject container accessors.
func bexRejectContainers(t *testing.T, n vibejson.Node, path string) {
	t.Helper()
	if _, ok := n.ArrayLen(); ok {
		t.Fatalf("%s: ArrayLen ok on scalar", path)
	}
	if _, ok := n.ObjectLen(); ok {
		t.Fatalf("%s: ObjectLen ok on scalar", path)
	}
	if _, ok := n.Index(0); ok {
		t.Fatalf("%s: Index ok on scalar", path)
	}
	if _, ok := n.Get("a"); ok {
		t.Fatalf("%s: Get ok on scalar", path)
	}
}

// bexCheckNumber checks every numeric accessor against strconv-derived
// expectations, independently reproducing the integer/float classification.
func bexCheckNumber(t *testing.T, n vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	if got, ok := n.NumberBytes(); !ok || !bytes.Equal(got, a.json) {
		t.Fatalf("%s: NumberBytes = (%q,%v), want %q", path, got, ok, a.json)
	}
	wantF, _ := strconv.ParseFloat(a.numRaw, 64)
	if f, ok := n.Float64(); !ok || f != wantF {
		t.Fatalf("%s: Float64 = (%v,%v), want (%v,true)", path, f, ok, wantF)
	}
	plainInt := bexIsPlainInt(a.numRaw)
	wantI, ierr := strconv.ParseInt(a.numRaw, 10, 64)
	gotI, iok := n.Int64()
	if plainInt && ierr == nil {
		if !iok || gotI != wantI {
			t.Fatalf("%s: Int64 = (%d,%v), want (%d,true)", path, gotI, iok, wantI)
		}
	} else if iok {
		t.Fatalf("%s: Int64 ok on non-integer %q", path, a.numRaw)
	}
	wantU, uerr := strconv.ParseUint(a.numRaw, 10, 64)
	gotU, uok := n.Uint64()
	if plainInt && a.numRaw[0] != '-' && uerr == nil {
		if !uok || gotU != wantU {
			t.Fatalf("%s: Uint64 = (%d,%v), want (%d,true)", path, gotU, uok, wantU)
		}
	} else if uok {
		t.Fatalf("%s: Uint64 ok on %q", path, a.numRaw)
	}
	if _, ok := n.Bool(); ok {
		t.Fatalf("%s: Bool ok on number", path)
	}
	if _, ok := n.StringBytes(); ok {
		t.Fatalf("%s: StringBytes ok on number", path)
	}
}

// bexCheckString checks the string accessors: a clean spelling exposes its
// content as a source alias, an escaped spelling declines StringBytes but
// decodes through AppendText, and both yield the AST's decoded content.
func bexCheckString(t *testing.T, n vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	sb, ok := n.StringBytes()
	if a.strEsc {
		if ok {
			t.Fatalf("%s: StringBytes ok on escaped string", path)
		}
	} else {
		if !ok || string(sb) != a.strDec {
			t.Fatalf("%s: StringBytes = (%q,%v), want %q", path, sb, ok, a.strDec)
		}
	}
	dec, ok := n.AppendText(nil)
	if !ok || string(dec) != a.strDec {
		t.Fatalf("%s: AppendText = (%q,%v), want %q", path, dec, ok, a.strDec)
	}
	if _, ok := n.Bool(); ok {
		t.Fatalf("%s: Bool ok on string", path)
	}
	if _, ok := n.Int64(); ok {
		t.Fatalf("%s: Int64 ok on string", path)
	}
	if _, ok := n.Float64(); ok {
		t.Fatalf("%s: Float64 ok on string", path)
	}
}

// bexCheckArrayIter asserts ArrayIter yields exactly the AST's elements in
// order, by raw bytes.
func bexCheckArrayIter(t *testing.T, n vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	it, ok := n.ArrayIter()
	if !ok {
		t.Fatalf("%s: ArrayIter declined", path)
	}
	for i, e := range a.elems {
		c, ok := it.Next()
		if !ok {
			t.Fatalf("%s: ArrayIter ended at %d", path, i)
		}
		if got := c.Raw().Bytes(); !bytes.Equal(got, e.json) {
			t.Fatalf("%s: ArrayIter[%d] = %q, want %q", path, i, got, e.json)
		}
	}
	if _, ok := it.Next(); ok {
		t.Fatalf("%s: ArrayIter yielded past the end", path)
	}
}

// bexCheckObjectIter asserts ObjectIter yields every member — duplicates
// included — in document order, by decoded key and raw value.
func bexCheckObjectIter(t *testing.T, n vibejson.Node, a *exhaustiveValue, path string) {
	t.Helper()
	it, ok := n.ObjectIter()
	if !ok {
		t.Fatalf("%s: ObjectIter declined", path)
	}
	for i := range a.keys {
		key, value, ok := it.Next()
		if !ok {
			t.Fatalf("%s: ObjectIter ended at %d", path, i)
		}
		gotKey, _ := key.AppendText(nil)
		if string(gotKey) != a.keys[i] {
			t.Fatalf("%s: ObjectIter key[%d] = %q, want %q", path, i, gotKey, a.keys[i])
		}
		if got := value.Raw().Bytes(); !bytes.Equal(got, a.vals[i].json) {
			t.Fatalf("%s: ObjectIter value[%d] = %q, want %q", path, i, got, a.vals[i].json)
		}
	}
	if _, _, ok := it.Next(); ok {
		t.Fatalf("%s: ObjectIter yielded past the end", path)
	}
}

// bexCheckPointers checks the pointer/navigation invariant: for every pointer
// an independent recursive descent can reach, Node.Pointer and the compiled
// pointer both resolve to the AST-specified node. A few unreachable pointers
// are checked to be reported absent, not misresolved. It returns the number of
// reachable pointers checked.
func bexCheckPointers(t *testing.T, idx vibejson.Index, doc *exhaustiveValue, src []byte) int {
	t.Helper()
	var walk func(a *exhaustiveValue, tokens []string)
	count := 0
	walk = func(a *exhaustiveValue, tokens []string) {
		ptr := bexPointerString(tokens)
		got, ok, err := idx.Pointer(ptr)
		if err != nil || !ok {
			t.Fatalf("%s: Pointer(%q) = (_,%v,%v), want present", src, ptr, ok, err)
		}
		if g := got.Raw().Bytes(); !bytes.Equal(g, a.json) {
			t.Fatalf("%s: Pointer(%q) = %q, want %q", src, ptr, g, a.json)
		}
		cp, err := vibejson.CompilePointer(ptr)
		if err != nil {
			t.Fatalf("%s: CompilePointer(%q): %v", src, ptr, err)
		}
		gc, ok, err := idx.PointerCompiled(cp)
		if err != nil || !ok || !bytes.Equal(gc.Raw().Bytes(), a.json) {
			t.Fatalf("%s: PointerCompiled(%q) diverged from Pointer", src, ptr)
		}
		count++
		switch a.kind {
		case document.Array:
			for i, e := range a.elems {
				walk(e, append(tokens, strconv.Itoa(i)))
			}
		case document.Object:
			order, last := bexEffectiveMembers(a)
			for _, k := range order {
				walk(last[k], append(tokens, k))
			}
		}
	}
	walk(doc, nil)

	// Unreachable pointers resolve to absence without error.
	for _, ptr := range []string{"/no-such-key", "/0/0/0", "/zz"} {
		if _, ok, err := idx.Pointer(ptr); ok && err == nil && !bexPointerReachable(doc, ptr) {
			t.Fatalf("%s: Pointer(%q) unexpectedly present", src, ptr)
		}
	}
	return count
}

// bexCheckAppendPointer exercises the batch AppendPointer path, which reads a
// narrow shape tape's value slab directly rather than through widening. Every
// reachable single-token pointer must return the AST-specified bytes for each
// stored copy of the document.
func bexCheckAppendPointer(t *testing.T, set *Segment, doc *exhaustiveValue, src []byte) {
	t.Helper()
	if doc.kind != document.Object {
		return
	}
	order, last := bexEffectiveMembers(doc)
	for _, k := range order {
		cp, err := vibejson.CompilePointer(bexPointerString([]string{k}))
		if err != nil {
			t.Fatalf("%s: CompilePointer(/%s): %v", src, k, err)
		}
		got, err := set.AppendPointer(nil, cp)
		if err != nil {
			t.Fatalf("%s: AppendPointer(/%s): %v", src, k, err)
		}
		if len(got) != set.Len() {
			t.Fatalf("%s: AppendPointer returned %d values, want %d", src, len(got), set.Len())
		}
		for i := range got {
			if g := got[i].Bytes(); !bytes.Equal(g, last[k].json) {
				t.Fatalf("%s: AppendPointer(/%s)[%d] = %q, want %q", src, k, i, g, last[k].json)
			}
		}
	}
}

// bexPointerString renders a JSON Pointer from decoded tokens per RFC 6901.
func bexPointerString(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	var b strings.Builder
	for _, tok := range tokens {
		b.WriteByte('/')
		b.WriteString(strings.NewReplacer("~", "~0", "/", "~1").Replace(tok))
	}
	return b.String()
}

// bexPointerReachable reports whether a pointer string addresses a real node
// of the AST, used to avoid a false alarm when a probe pointer happens to be
// reachable in some document.
func bexPointerReachable(a *exhaustiveValue, ptr string) bool {
	if ptr == "" {
		return true
	}
	cur := a
	for _, tok := range strings.Split(ptr, "/")[1:] {
		tok = strings.NewReplacer("~1", "/", "~0", "~").Replace(tok)
		switch cur.kind {
		case document.Object:
			_, last := bexEffectiveMembers(cur)
			nv, ok := last[tok]
			if !ok {
				return false
			}
			cur = nv
		case document.Array:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(cur.elems) {
				return false
			}
			cur = cur.elems[i]
		default:
			return false
		}
	}
	return true
}

// bexIsPlainInt reports whether a number spelling is an optional minus sign
// followed by digits — the tape's plain-integer classification, reproduced
// here independently.
func bexIsPlainInt(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' {
		i = 1
	}
	if i == len(s) {
		return false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// TestExhaustiveContainsVsReference checks property 3: over every ordered pair
// of documents in the pair bound, Node.Contains and RawContains agree with an
// independent from-scratch definition of jsonb @>. Any disagreement is a real
// bug and is reported with the minimal counterexample the enumeration found.
func TestExhaustiveContainsVsReference(t *testing.T) {
	// -short shrinks the pair domain (which grows as the square of the document
	// count) to a representative sample; the full bound is the default.
	depth, nodes, width := bexPairDepth, testIterations(bexPairNodes, 2), bexPairWidth
	docs := exhaustiveGenerate(depth, nodes, width)
	if n := len(docs) * len(docs); n > bexDomainCeiling*40 {
		t.Fatalf("pair domain %d exceeds the pair ceiling; raise the bound deliberately", n)
	}

	// Build every index once; the pair loop reuses them.
	idxs := make([]vibejson.Index, len(docs))
	for i, d := range docs {
		need, err := vibejson.RequiredIndexEntries(d.json)
		if err != nil {
			t.Fatalf("RequiredIndexEntries(%s): %v", d.json, err)
		}
		idx, err := vibejson.BuildIndex(d.json, make([]vibejson.IndexEntry, need))
		if err != nil {
			t.Fatalf("BuildIndex(%s): %v", d.json, err)
		}
		idxs[i] = idx
	}

	pairs := 0
	trueCount := 0
	// RawContains rebuilds both indices per call, so it is checked on a strided
	// sample rather than every pair; Node.Contains covers every pair.
	const rawStride = 977
	for hi := range docs {
		hRoot := idxs[hi].Root()
		for ni := range docs {
			want := exhaustiveContains(docs[hi], docs[ni], true)
			got := hRoot.Contains(idxs[ni].Root())
			if got != want {
				t.Fatalf("Contains counterexample: haystack %s needle %s: Contains=%v oracle=%v",
					docs[hi].json, docs[ni].json, got, want)
			}
			if want {
				trueCount++
			}
			if (hi*len(docs)+ni)%rawStride == 0 {
				raw, err := vibejson.RawContains(docs[hi].json, docs[ni].json)
				if err != nil {
					t.Fatalf("RawContains(%s,%s): %v", docs[hi].json, docs[ni].json, err)
				}
				if raw != want {
					t.Fatalf("RawContains counterexample: haystack %s needle %s: Raw=%v oracle=%v",
						docs[hi].json, docs[ni].json, raw, want)
				}
			}
			pairs++
		}
	}

	t.Logf("pair bound depth<=%d nodes<=%d width<=%d: %d documents, %d ordered pairs checked against the @> reference (%d contained)",
		depth, nodes, width, len(docs), pairs, trueCount)
}

// exhaustiveContains is the independent reference for PostgreSQL's jsonb
// containment operator @>, written as a direct inductive definition over the
// AST. It is deliberately not the library's evaluator: the differential check
// is the two agreeing on every enumerated pair. topLevel carries the one
// documented special case — a top-level array contains a scalar equal to some
// element — which does not nest.
func exhaustiveContains(h, n *exhaustiveValue, topLevel bool) bool {
	if topLevel && h.kind == document.Array && bexIsScalar(n.kind) {
		for _, e := range h.elems {
			if bexScalarEqual(e, n) {
				return true
			}
		}
		return false
	}
	switch n.kind {
	case document.Object:
		if h.kind != document.Object {
			return false
		}
		order, nLast := bexEffectiveMembers(n)
		_, hLast := bexEffectiveMembers(h)
		for _, k := range order {
			hv, ok := hLast[k]
			if !ok || !exhaustiveContains(hv, nLast[k], false) {
				return false
			}
		}
		return true
	case document.Array:
		if h.kind != document.Array {
			return false
		}
		for _, e := range n.elems {
			matched := false
			for _, c := range h.elems {
				if c.kind == e.kind && exhaustiveContains(c, e, false) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	default:
		return bexScalarEqual(h, n)
	}
}

// bexIsScalar reports whether a kind is a JSON scalar.
func bexIsScalar(k document.Kind) bool {
	return k == document.Null || k == document.Bool || k == document.Number || k == document.String
}

// bexScalarEqual is the oracle's scalar equality: same kind and same value,
// with numbers compared by exact rational value and strings by decoded
// content. Containers are never scalar-equal.
func bexScalarEqual(a, b *exhaustiveValue) bool {
	if a.kind != b.kind {
		return false
	}
	switch a.kind {
	case document.Null:
		return true
	case document.Bool:
		return a.boolVal == b.boolVal
	case document.Number:
		ra, _ := new(big.Rat).SetString(bexNormalizeNumber(a.numRaw))
		rb, _ := new(big.Rat).SetString(bexNormalizeNumber(b.numRaw))
		return ra != nil && rb != nil && ra.Cmp(rb) == 0
	case document.String:
		return a.strDec == b.strDec
	default:
		return false
	}
}

// bexNormalizeNumber rewrites a JSON number spelling into a form big.Rat
// accepts, translating scientific notation into an explicit power of ten.
func bexNormalizeNumber(s string) string {
	i := strings.IndexAny(s, "eE")
	if i < 0 {
		return s
	}
	mant := s[:i]
	exp, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return s
	}
	if exp >= 0 {
		return mant + "e" + strconv.Itoa(exp)
	}
	// big.Rat accepts "mant/10^k" for negative exponents.
	den := "1"
	for j := 0; j < -exp; j++ {
		den += "0"
	}
	return mant + "/" + den
}
