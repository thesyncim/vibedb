package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibejson"
)

// The versioned native front end is intentionally separate from Parse. Parse
// is the permissive, allocation-tuned legacy document syntax and changing its
// duplicate, null, path, or shorthand rules would be a compatibility break.
//
// This file is the strict syntax boundary only. It owns and validates a typed
// syntax tree, including exact number spellings and parameter references. A
// later lowering step can normalize this tree into shared logical nodes
// without teaching either the executor or a storage backend about JSON member
// shapes. Keeping the entry point package-private until lowering, binding,
// budgets, and backend parity exist also prevents an incomplete "v1" from
// becoming a public compatibility promise.

const (
	nativeQueryDialect       = "vibedb-query"
	nativeQueryVersion       = 1
	nativeMaxDocumentBytes   = 1 << 20
	nativeMaxDepth           = 64
	nativeMaxPredicateNodes  = 1024
	nativeMaxBooleanFanIn    = 256
	nativeMaxLiteralBytes    = 512 << 10
	nativeMaxMembershipItems = 4096
	nativeMaxParameters      = 256
	nativeMaxProjection      = 256
	nativeMaxSortKeys        = 16
	nativeMaxExists          = 8
	nativeMaxCollectionBytes = 240
	nativeMaxNameBytes       = 255
	nativeMaxPointerBytes    = 16 << 10
	nativeMaxLimit           = 1_000_000
)

type nativeSyntaxError struct {
	code    string
	pointer string
	detail  string
}

func (e *nativeSyntaxError) Error() string {
	if e == nil {
		return "query: invalid native query"
	}
	at := e.pointer
	if at == "" {
		at = "<root>"
	}
	return fmt.Sprintf("query: native %s at %s: %s", e.code, at, e.detail)
}

func nativeSyntaxErr(code, pointer, format string, args ...any) error {
	return &nativeSyntaxError{
		code: code, pointer: pointer, detail: fmt.Sprintf(format, args...),
	}
}

func nativePointerMember(parent, name string) string {
	name = strings.ReplaceAll(name, "~", "~0")
	name = strings.ReplaceAll(name, "/", "~1")
	return parent + "/" + name
}

func nativePointerElement(parent string, index int) string {
	return parent + "/" + strconv.Itoa(index)
}

type nativeMember struct {
	name  string
	value qvalue
}

type nativeQuerySyntax struct {
	dialect string
	version int
	from    string

	where    *nativePredicateSyntax
	exists   []nativeJoinSyntax
	join     *nativeJoinSyntax
	selects  []nativeSelectSyntax
	orderBy  []nativeOrderSyntax
	limit    nativeOperandSyntax
	hasLimit bool
}

type nativeSelectSyntax struct {
	name string
	path nativePathSyntax
}

type nativeOrderSyntax struct {
	path      nativePathSyntax
	direction Direction
}

type nativeJoinSyntax struct {
	from  string
	alias string
	outer nativeJoinPathSyntax
	inner nativeJoinPathSyntax
	where *nativePredicateSyntax
}

type nativePathSyntax struct {
	spec  string
	alias string
}

type nativeJoinPathSyntax struct {
	spec string
	key  bool
}

type nativeOperandKind uint8

const (
	nativeOperandInvalid nativeOperandKind = iota
	nativeOperandScalar
	nativeOperandParameter
	nativeOperandList
)

type nativeOperandSyntax struct {
	kind   nativeOperandKind
	scalar nativeScalarSyntax
	list   []nativeScalarSyntax
	param  string
}

type nativeScalarSyntax struct {
	kind    qkind
	text    string
	boolean bool
}

type nativeParser struct {
	compiler       Compiler
	predicateNodes int
	literalBytes   int
	parameters     map[string]struct{}
}

// parseNativeQuerySyntax parses and validates the strict, versioned wire
// grammar without changing the legacy Parse/New behavior. The returned tree
// owns every string it retains; src may be reused as soon as this returns.
func parseNativeQuerySyntax(src []byte) (nativeQuerySyntax, error) {
	if len(src) > nativeMaxDocumentBytes {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"limit_exceeded", "", "document is %d bytes; maximum is %d",
			len(src), nativeMaxDocumentBytes,
		)
	}
	var parser nativeParser
	value, err := vibejson.ParseOptions(src, vibejson.Options{
		MaxDepth: nativeMaxDepth,
		ZeroCopy: true,
	})
	if err != nil {
		var syntaxErr *vibejson.SyntaxError
		if errors.As(err, &syntaxErr) &&
			syntaxErr.Message == "maximum nesting depth exceeded" {
			return nativeQuerySyntax{}, nativeSyntaxErr(
				"limit_exceeded", "",
				"JSON nesting exceeds %d levels", nativeMaxDepth,
			)
		}
		return nativeQuerySyntax{}, nativeSyntaxErr("malformed_json", "", "%v", err)
	}
	root := nodeValue(value.Node())
	if err := parser.rejectDuplicateMembers(root, ""); err != nil {
		return nativeQuerySyntax{}, err
	}
	return parser.parseQuery(root)
}

func (p *nativeParser) parseQuery(root qvalue) (nativeQuerySyntax, error) {
	if root.kind() != qObject {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"invalid_document", "", "query document must be an object, not %s",
			root.describeKind(),
		)
	}
	members, err := p.members(root, "")
	if err != nil {
		return nativeQuerySyntax{}, err
	}
	values := make(map[string]qvalue, len(members))
	for _, member := range members {
		switch member.name {
		case "dialect", "version", "from", "where", "exists", "join",
			"select", "orderBy", "limit":
			values[member.name] = member.value
		default:
			return nativeQuerySyntax{}, nativeSyntaxErr(
				"unknown_member", nativePointerMember("", member.name),
				"unknown query member %q", member.name,
			)
		}
	}
	for _, required := range []string{"dialect", "version", "from"} {
		if _, ok := values[required]; !ok {
			return nativeQuerySyntax{}, nativeSyntaxErr(
				"missing_member", "", "required member %q is absent", required,
			)
		}
	}

	dialect, err := p.requiredText(values["dialect"], "/dialect", "dialect")
	if err != nil {
		return nativeQuerySyntax{}, err
	}
	if dialect != nativeQueryDialect {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"unsupported_dialect", "/dialect", "got %q; expected %q",
			dialect, nativeQueryDialect,
		)
	}
	versionText, ok := p.numberText(values["version"])
	if !ok {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"invalid_version", "/version", "version must be the integer token %d",
			nativeQueryVersion,
		)
	}
	if versionText != strconv.Itoa(nativeQueryVersion) {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"unsupported_version", "/version", "version %s is not supported", versionText,
		)
	}
	from, err := p.requiredText(values["from"], "/from", "collection")
	if err != nil {
		return nativeQuerySyntax{}, err
	}
	if !nativeValidCollectionName(from) {
		return nativeQuerySyntax{}, nativeSyntaxErr(
			"invalid_collection", "/from", "%q is not a portable collection name", from,
		)
	}

	out := nativeQuerySyntax{
		dialect: dialect, version: nativeQueryVersion, from: from,
	}
	if value, ok := values["where"]; ok {
		predicate, parseErr := p.parsePredicate(value, "/where", 1)
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.where = &predicate
	}
	if value, ok := values["exists"]; ok {
		exists, parseErr := p.parseExists(value, "/exists")
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.exists = exists
	}
	if value, ok := values["join"]; ok {
		join, parseErr := p.parseJoin(value, "/join", true)
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.join = &join
	}
	if value, ok := values["select"]; ok {
		selects, parseErr := p.parseSelect(value, "/select")
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.selects = selects
	}
	if value, ok := values["orderBy"]; ok {
		order, parseErr := p.parseOrderBy(value, "/orderBy")
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.orderBy = order
	}
	if value, ok := values["limit"]; ok {
		limit, parseErr := p.parseLimit(value, "/limit")
		if parseErr != nil {
			return nativeQuerySyntax{}, parseErr
		}
		out.limit, out.hasLimit = limit, true
	}
	if err := p.validateQualifiedPaths(&out); err != nil {
		return nativeQuerySyntax{}, err
	}
	return out, nil
}

func (p *nativeParser) members(value qvalue, pointer string) ([]nativeMember, error) {
	if value.kind() != qObject {
		return nil, nativeSyntaxErr(
			"invalid_object", pointer, "expected an object, not %s",
			value.describeKind(),
		)
	}
	out := make([]nativeMember, 0, value.length())
	seen := make(map[string]struct{}, value.length())
	err := value.fields(&p.compiler, func(key qkey, member qvalue) error {
		name := key.String()
		if _, duplicate := seen[name]; duplicate {
			return nativeSyntaxErr(
				"duplicate_member", nativePointerMember(pointer, name),
				"member %q occurs more than once", name,
			)
		}
		seen[name] = struct{}{}
		out = append(out, nativeMember{name: name, value: member})
		return nil
	})
	return out, err
}

// rejectDuplicateMembers is a grammar-independent preflight. Duplicate names
// fail even inside a value whose shape is otherwise invalid, so error
// classification cannot depend on which clause parser happens to visit first.
func (p *nativeParser) rejectDuplicateMembers(value qvalue, pointer string) error {
	switch value.kind() {
	case qObject:
		if value.length() > nativeMaxPredicateNodes {
			return nativeSyntaxErr(
				"limit_exceeded", pointer,
				"object has %d members; maximum is %d",
				value.length(), nativeMaxPredicateNodes,
			)
		}
		members, err := p.members(value, pointer)
		if err != nil {
			return err
		}
		for _, member := range members {
			if err := p.rejectDuplicateMembers(
				member.value, nativePointerMember(pointer, member.name),
			); err != nil {
				return err
			}
		}
	case qArray:
		return value.elements(func(index int, element qvalue) error {
			return p.rejectDuplicateMembers(
				element, nativePointerElement(pointer, index),
			)
		})
	}
	return nil
}

func (p *nativeParser) requiredText(value qvalue, pointer, subject string) (string, error) {
	text, ok := value.text(&p.compiler)
	if !ok || !utf8.ValidString(text) {
		return "", nativeSyntaxErr(
			"invalid_operand", pointer, "%s must be a valid UTF-8 string", subject,
		)
	}
	return strings.Clone(text), nil
}

func (p *nativeParser) numberText(value qvalue) (string, bool) {
	if value.kind() != qNumber || !value.isNode {
		return "", false
	}
	text, ok := value.node.NumberText()
	if !ok {
		return "", false
	}
	return strings.Clone(text), true
}

func (p *nativeParser) parseExists(value qvalue, pointer string) ([]nativeJoinSyntax, error) {
	if value.kind() != qArray || value.length() == 0 {
		return nil, nativeSyntaxErr(
			"invalid_clause", pointer, "exists must be a non-empty array",
		)
	}
	if value.length() > nativeMaxExists {
		return nil, nativeSyntaxErr(
			"limit_exceeded", pointer, "exists has %d entries; maximum is %d",
			value.length(), nativeMaxExists,
		)
	}
	out := make([]nativeJoinSyntax, 0, value.length())
	err := value.elements(func(index int, element qvalue) error {
		join, err := p.parseJoin(
			element, nativePointerElement(pointer, index), false,
		)
		if err != nil {
			return err
		}
		out = append(out, join)
		return nil
	})
	return out, err
}

func (p *nativeParser) parseJoin(
	value qvalue, pointer string, fanOut bool,
) (nativeJoinSyntax, error) {
	members, err := p.members(value, pointer)
	if err != nil {
		return nativeJoinSyntax{}, err
	}
	values := make(map[string]qvalue, len(members))
	for _, member := range members {
		allowed := member.name == "from" || member.name == "on" ||
			member.name == "where" || fanOut && member.name == "as"
		if !allowed {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"unknown_member", nativePointerMember(pointer, member.name),
				"unknown %s member %q", nativeJoinKind(fanOut), member.name,
			)
		}
		values[member.name] = member.value
	}
	for _, required := range []string{"from", "on"} {
		if _, ok := values[required]; !ok {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"missing_member", pointer, "%s requires %q",
				nativeJoinKind(fanOut), required,
			)
		}
	}
	if fanOut {
		if _, ok := values["as"]; !ok {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"missing_member", pointer, "join requires \"as\"",
			)
		}
	}

	from, err := p.requiredText(
		values["from"], nativePointerMember(pointer, "from"), "collection",
	)
	if err != nil {
		return nativeJoinSyntax{}, err
	}
	if !nativeValidCollectionName(from) {
		return nativeJoinSyntax{}, nativeSyntaxErr(
			"invalid_collection", nativePointerMember(pointer, "from"),
			"%q is not a portable collection name", from,
		)
	}
	out := nativeJoinSyntax{from: from}
	if fanOut {
		alias, aliasErr := p.requiredText(
			values["as"], nativePointerMember(pointer, "as"), "alias",
		)
		if aliasErr != nil {
			return nativeJoinSyntax{}, aliasErr
		}
		if !nativeValidAlias(alias) {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"invalid_alias", nativePointerMember(pointer, "as"),
				"%q is not a valid alias", alias,
			)
		}
		out.alias = alias
	}

	onPointer := nativePointerMember(pointer, "on")
	onMembers, err := p.members(values["on"], onPointer)
	if err != nil {
		return nativeJoinSyntax{}, err
	}
	if len(onMembers) != 2 {
		return nativeJoinSyntax{}, nativeSyntaxErr(
			"invalid_join", onPointer, "on must contain exactly outer and inner",
		)
	}
	onValues := make(map[string]qvalue, 2)
	for _, member := range onMembers {
		if member.name != "outer" && member.name != "inner" {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"unknown_member", nativePointerMember(onPointer, member.name),
				"unknown join condition member %q", member.name,
			)
		}
		onValues[member.name] = member.value
	}
	for _, side := range []string{"outer", "inner"} {
		value, ok := onValues[side]
		if !ok {
			return nativeJoinSyntax{}, nativeSyntaxErr(
				"missing_member", onPointer, "on requires %q", side,
			)
		}
		pathPointer := nativePointerMember(onPointer, side)
		spec, textErr := p.requiredText(value, pathPointer, "join path")
		if textErr != nil {
			return nativeJoinSyntax{}, textErr
		}
		path, pathErr := nativeParseJoinPath(spec, pathPointer)
		if pathErr != nil {
			return nativeJoinSyntax{}, pathErr
		}
		if side == "outer" {
			out.outer = path
		} else {
			out.inner = path
		}
	}
	if whereValue, ok := values["where"]; ok {
		wherePointer := nativePointerMember(pointer, "where")
		where, whereErr := p.parsePredicate(whereValue, wherePointer, 1)
		if whereErr != nil {
			return nativeJoinSyntax{}, whereErr
		}
		out.where = &where
	}
	return out, nil
}

func nativeJoinKind(fanOut bool) string {
	if fanOut {
		return "join"
	}
	return "exists clause"
}

func (p *nativeParser) parseSelect(value qvalue, pointer string) ([]nativeSelectSyntax, error) {
	if value.kind() != qArray || value.length() == 0 {
		return nil, nativeSyntaxErr(
			"invalid_clause", pointer, "select must be a non-empty array",
		)
	}
	if value.length() > nativeMaxProjection {
		return nil, nativeSyntaxErr(
			"limit_exceeded", pointer, "select has %d entries; maximum is %d",
			value.length(), nativeMaxProjection,
		)
	}
	out := make([]nativeSelectSyntax, 0, value.length())
	names := make(map[string]struct{}, value.length())
	err := value.elements(func(index int, element qvalue) error {
		at := nativePointerElement(pointer, index)
		members, err := p.members(element, at)
		if err != nil {
			return err
		}
		if len(members) != 2 {
			return nativeSyntaxErr(
				"invalid_projection", at, "projection must contain exactly name and path",
			)
		}
		values := make(map[string]qvalue, 2)
		for _, member := range members {
			if member.name != "name" && member.name != "path" {
				return nativeSyntaxErr(
					"unknown_member", nativePointerMember(at, member.name),
					"unknown projection member %q", member.name,
				)
			}
			values[member.name] = member.value
		}
		nameValue, hasName := values["name"]
		pathValue, hasPath := values["path"]
		if !hasName || !hasPath {
			return nativeSyntaxErr(
				"missing_member", at, "projection requires name and path",
			)
		}
		name, err := p.requiredText(
			nameValue, nativePointerMember(at, "name"), "output name",
		)
		if err != nil {
			return err
		}
		if !nativeValidName(name) {
			return nativeSyntaxErr(
				"invalid_name", nativePointerMember(at, "name"),
				"%q is not a valid output name", name,
			)
		}
		if _, duplicate := names[name]; duplicate {
			return nativeSyntaxErr(
				"duplicate_name", nativePointerMember(at, "name"),
				"output name %q occurs more than once", name,
			)
		}
		names[name] = struct{}{}
		spec, err := p.requiredText(
			pathValue, nativePointerMember(at, "path"), "projection path",
		)
		if err != nil {
			return err
		}
		path, err := nativeParseResultPath(spec, nativePointerMember(at, "path"))
		if err != nil {
			return err
		}
		out = append(out, nativeSelectSyntax{name: name, path: path})
		return nil
	})
	return out, err
}

func (p *nativeParser) parseOrderBy(value qvalue, pointer string) ([]nativeOrderSyntax, error) {
	if value.kind() != qArray || value.length() == 0 {
		return nil, nativeSyntaxErr(
			"invalid_clause", pointer, "orderBy must be a non-empty array",
		)
	}
	if value.length() > nativeMaxSortKeys {
		return nil, nativeSyntaxErr(
			"limit_exceeded", pointer, "orderBy has %d entries; maximum is %d",
			value.length(), nativeMaxSortKeys,
		)
	}
	out := make([]nativeOrderSyntax, 0, value.length())
	err := value.elements(func(index int, element qvalue) error {
		at := nativePointerElement(pointer, index)
		members, err := p.members(element, at)
		if err != nil {
			return err
		}
		if len(members) != 2 {
			return nativeSyntaxErr(
				"invalid_order", at, "sort key must contain exactly path and direction",
			)
		}
		values := make(map[string]qvalue, 2)
		for _, member := range members {
			if member.name != "path" && member.name != "direction" {
				return nativeSyntaxErr(
					"unknown_member", nativePointerMember(at, member.name),
					"unknown sort-key member %q", member.name,
				)
			}
			values[member.name] = member.value
		}
		pathValue, hasPath := values["path"]
		directionValue, hasDirection := values["direction"]
		if !hasPath || !hasDirection {
			return nativeSyntaxErr(
				"missing_member", at, "sort key requires path and direction",
			)
		}
		spec, err := p.requiredText(
			pathValue, nativePointerMember(at, "path"), "sort path",
		)
		if err != nil {
			return err
		}
		path, err := nativeParseResultPath(spec, nativePointerMember(at, "path"))
		if err != nil {
			return err
		}
		directionText, err := p.requiredText(
			directionValue, nativePointerMember(at, "direction"), "sort direction",
		)
		if err != nil {
			return err
		}
		var direction Direction
		switch directionText {
		case "asc":
			direction = Asc
		case "desc":
			direction = Desc
		default:
			return nativeSyntaxErr(
				"invalid_direction", nativePointerMember(at, "direction"),
				"direction must be \"asc\" or \"desc\", not %q", directionText,
			)
		}
		out = append(out, nativeOrderSyntax{path: path, direction: direction})
		return nil
	})
	return out, err
}

func (p *nativeParser) parseLimit(value qvalue, pointer string) (nativeOperandSyntax, error) {
	if value.kind() == qObject {
		return p.parseParameter(value, pointer)
	}
	text, ok := p.numberText(value)
	if !ok || text == "" {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_limit", pointer, "limit must be a non-negative integer token or parameter",
		)
	}
	for _, c := range text {
		if c < '0' || c > '9' {
			return nativeOperandSyntax{}, nativeSyntaxErr(
				"invalid_limit", pointer, "limit must use an integer token, not %s", text,
			)
		}
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil || n > nativeMaxLimit {
		return nativeOperandSyntax{}, nativeSyntaxErr(
			"invalid_limit", pointer, "limit %s is outside [0,%d]", text, nativeMaxLimit,
		)
	}
	return nativeOperandSyntax{
		kind:   nativeOperandScalar,
		scalar: nativeScalarSyntax{kind: qNumber, text: text},
	}, nil
}

func (p *nativeParser) validateQualifiedPaths(query *nativeQuerySyntax) error {
	alias := ""
	if query.join != nil {
		alias = query.join.alias
	}
	joinedProjection := false
	for index, projection := range query.selects {
		if projection.path.alias == "" {
			continue
		}
		if projection.path.alias != alias {
			return nativeSyntaxErr(
				"unknown_alias", "/select/"+strconv.Itoa(index)+"/path",
				"path names alias %q; query join names %q",
				projection.path.alias, alias,
			)
		}
		joinedProjection = true
	}
	for index, order := range query.orderBy {
		if order.path.alias != "" && order.path.alias != alias {
			return nativeSyntaxErr(
				"unknown_alias", "/orderBy/"+strconv.Itoa(index)+"/path",
				"path names alias %q; query join names %q",
				order.path.alias, alias,
			)
		}
	}
	if query.join != nil {
		if len(query.selects) == 0 {
			return nativeSyntaxErr(
				"missing_member", "/join", "fan-out join requires select",
			)
		}
		if !joinedProjection {
			return nativeSyntaxErr(
				"invalid_projection", "/select",
				"fan-out join must project at least one path from alias %q", alias,
			)
		}
	}
	return nil
}

func nativeParseJoinPath(spec, pointer string) (nativeJoinPathSyntax, error) {
	if spec == JoinKey {
		return nativeJoinPathSyntax{spec: spec, key: true}, nil
	}
	if err := nativeValidatePointer(spec); err != nil {
		return nativeJoinPathSyntax{}, nativeSyntaxErr(
			"invalid_path", pointer, "%q is not an RFC 6901 pointer: %v", spec, err,
		)
	}
	return nativeJoinPathSyntax{spec: spec}, nil
}

func nativeParseResultPath(spec, pointer string) (nativePathSyntax, error) {
	if strings.HasPrefix(spec, "@") {
		slash := strings.IndexByte(spec, '/')
		alias, path := spec[1:], ""
		if slash >= 0 {
			alias, path = spec[1:slash], spec[slash:]
		}
		if !nativeValidAlias(alias) {
			return nativePathSyntax{}, nativeSyntaxErr(
				"invalid_alias", pointer, "%q has an invalid source alias", spec,
			)
		}
		if err := nativeValidatePointer(path); err != nil {
			return nativePathSyntax{}, nativeSyntaxErr(
				"invalid_path", pointer, "%q has an invalid qualified pointer: %v",
				spec, err,
			)
		}
		return nativePathSyntax{spec: strings.Clone(spec), alias: strings.Clone(alias)}, nil
	}
	if err := nativeValidatePointer(spec); err != nil {
		return nativePathSyntax{}, nativeSyntaxErr(
			"invalid_path", pointer, "%q is not an RFC 6901 pointer: %v", spec, err,
		)
	}
	return nativePathSyntax{spec: strings.Clone(spec)}, nil
}

func nativeValidatePointer(spec string) error {
	if len(spec) > nativeMaxPointerBytes {
		return fmt.Errorf("pointer is %d bytes; maximum is %d", len(spec), nativeMaxPointerBytes)
	}
	if spec != "" && spec[0] != '/' {
		return fmt.Errorf("pointer must be empty or begin with '/'")
	}
	_, err := vibejson.CompilePointer(spec)
	return err
}

func nativeValidCollectionName(name string) bool {
	return len(name) > 0 && len(name) <= nativeMaxCollectionBytes &&
		utf8.ValidString(name) && name != "." && name != ".." &&
		!strings.ContainsAny(name, "\x00/\\") && !strings.HasSuffix(name, ".vjc")
}

func nativeValidName(name string) bool {
	return len(name) > 0 && len(name) <= nativeMaxNameBytes &&
		utf8.ValidString(name) && !strings.ContainsRune(name, 0)
}

func nativeValidAlias(alias string) bool {
	if len(alias) == 0 || len(alias) > nativeMaxNameBytes {
		return false
	}
	first := alias[0]
	if first != '_' && (first < 'A' || first > 'Z') && (first < 'a' || first > 'z') {
		return false
	}
	for i := 1; i < len(alias); i++ {
		c := alias[i]
		if c != '_' && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') &&
			(c < '0' || c > '9') {
			return false
		}
	}
	return true
}
