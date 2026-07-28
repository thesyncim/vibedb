package query

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestNativeSyntaxFullDocument(t *testing.T) {
	got := mustParseNativeSyntax(t, `{
		"dialect": "vibedb-query",
		"version": 1,
		"from": "orders",
		"where": {
			"$and": [
				{"/total": {"$gte": {"$param": "minimum"}}},
				{"/state": {"$in": ["open", null, true, -0, 1.2300e+02]}},
				{"/deleted": {"$missing": true}}
			]
		},
		"exists": [{
			"from": "accounts",
			"on": {"outer": "/account_id", "inner": "$key"},
			"where": {"/enabled": true}
		}],
		"join": {
			"from": "customers",
			"as": "customer",
			"on": {"outer": "/customer_id", "inner": "$key"},
			"where": {"/tier": "pro"}
		},
		"select": [
			{"name": "order_id", "path": "/id"},
			{"name": "customer_name", "path": "@customer/name"},
			{"name": "total", "path": "/total"}
		],
		"orderBy": [
			{"path": "/created_at", "direction": "desc"},
			{"path": "@customer/rank", "direction": "asc"}
		],
		"limit": {"$param": "rows"}
	}`)

	if got.dialect != nativeQueryDialect || got.version != nativeQueryVersion ||
		got.from != "orders" {
		t.Fatalf("envelope = (%q, %d, %q)", got.dialect, got.version, got.from)
	}

	if got.where == nil || got.where.kind != nativePredicateAnd ||
		len(got.where.children) != 3 {
		t.Fatalf("where = %#v", got.where)
	}
	minimum := got.where.children[0]
	if minimum.kind != nativePredicateField || minimum.path != "/total" ||
		minimum.operator != nativeFieldGe ||
		minimum.operand.kind != nativeOperandParameter ||
		minimum.operand.param != "minimum" {
		t.Fatalf("minimum predicate = %#v", minimum)
	}
	states := got.where.children[1]
	if states.kind != nativePredicateField || states.path != "/state" ||
		states.operator != nativeFieldIn ||
		states.operand.kind != nativeOperandList ||
		len(states.operand.list) != 5 {
		t.Fatalf("state predicate = %#v", states)
	}
	if states.operand.list[0].kind != qString || states.operand.list[0].text != "open" ||
		states.operand.list[1].kind != qNull ||
		states.operand.list[2].kind != qBool || !states.operand.list[2].boolean ||
		states.operand.list[3].kind != qNumber || states.operand.list[3].text != "-0" ||
		states.operand.list[4].kind != qNumber ||
		states.operand.list[4].text != "1.2300e+02" {
		t.Fatalf("state operands = %#v", states.operand.list)
	}
	missing := got.where.children[2]
	if missing.path != "/deleted" || missing.operator != nativeFieldMissing ||
		missing.operand.kind != nativeOperandInvalid {
		t.Fatalf("missing predicate = %#v", missing)
	}

	if len(got.exists) != 1 {
		t.Fatalf("exists count = %d", len(got.exists))
	}
	exists := got.exists[0]
	if exists.from != "accounts" || exists.alias != "" ||
		exists.outer.spec != "/account_id" || exists.outer.key ||
		exists.inner.spec != JoinKey || !exists.inner.key ||
		exists.where == nil || exists.where.path != "/enabled" ||
		exists.where.operator != nativeFieldEq ||
		exists.where.operand.scalar.kind != qBool ||
		!exists.where.operand.scalar.boolean {
		t.Fatalf("exists = %#v", exists)
	}

	if got.join == nil {
		t.Fatal("join is nil")
	}
	if got.join.from != "customers" || got.join.alias != "customer" ||
		got.join.outer.spec != "/customer_id" || got.join.outer.key ||
		got.join.inner.spec != JoinKey || !got.join.inner.key ||
		got.join.where == nil || got.join.where.path != "/tier" ||
		got.join.where.operand.scalar.kind != qString ||
		got.join.where.operand.scalar.text != "pro" {
		t.Fatalf("join = %#v", *got.join)
	}

	if len(got.selects) != 3 ||
		got.selects[0].name != "order_id" ||
		got.selects[0].path.spec != "/id" ||
		got.selects[0].path.alias != "" ||
		got.selects[1].name != "customer_name" ||
		got.selects[1].path.spec != "@customer/name" ||
		got.selects[1].path.alias != "customer" ||
		got.selects[2].name != "total" ||
		got.selects[2].path.spec != "/total" {
		t.Fatalf("select = %#v", got.selects)
	}
	if len(got.orderBy) != 2 ||
		got.orderBy[0].path.spec != "/created_at" ||
		got.orderBy[0].direction != Desc ||
		got.orderBy[1].path.spec != "@customer/rank" ||
		got.orderBy[1].path.alias != "customer" ||
		got.orderBy[1].direction != Asc {
		t.Fatalf("orderBy = %#v", got.orderBy)
	}
	if !got.hasLimit || got.limit.kind != nativeOperandParameter ||
		got.limit.param != "rows" {
		t.Fatalf("limit = (%v, %#v)", got.hasLimit, got.limit)
	}
}

func TestNativeSyntaxOwnsDecodedStringsAndExactNumbers(t *testing.T) {
	src := []byte(`{
		"dialect":"vibedb-\u0071uery",
		"version":1,
		"from":"ord\u0065rs",
		"where":{"/na\u006de":{"$eq":"v\u0061lue"},"/n":1.2300e+02},
		"select":[{"name":"na\u006de","path":"/na\u006de"}]
	}`)
	got, err := parseNativeQuerySyntax(src)
	if err != nil {
		t.Fatalf("parseNativeQuerySyntax: %v", err)
	}
	for i := range src {
		src[i] = 'x'
	}

	if got.dialect != nativeQueryDialect || got.from != "orders" ||
		got.where == nil || got.where.kind != nativePredicateAnd ||
		len(got.where.children) != 2 ||
		got.where.children[0].path != "/name" ||
		got.where.children[0].operand.scalar.text != "value" ||
		got.where.children[1].operand.scalar.text != "1.2300e+02" ||
		len(got.selects) != 1 || got.selects[0].name != "name" ||
		got.selects[0].path.spec != "/name" {
		t.Fatalf("parsed syntax changed with source mutation: %#v", got)
	}
}

func TestNativeSyntaxEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		code    string
		pointer string
	}{
		{
			name: "root must be object", src: `[]`,
			code: "invalid_document",
		},
		{
			name: "malformed JSON", src: `{`,
			code: "malformed_json",
		},
		{
			name: "dialect required", src: `{"version":1,"from":"orders"}`,
			code: "missing_member",
		},
		{
			name: "version required", src: `{"dialect":"vibedb-query","from":"orders"}`,
			code: "missing_member",
		},
		{
			name: "from required", src: `{"dialect":"vibedb-query","version":1}`,
			code: "missing_member",
		},
		{
			name: "dialect type", src: `{"dialect":1,"version":1,"from":"orders"}`,
			code: "invalid_operand", pointer: "/dialect",
		},
		{
			name: "unsupported dialect", src: `{"dialect":"sql","version":1,"from":"orders"}`,
			code: "unsupported_dialect", pointer: "/dialect",
		},
		{
			name: "version type", src: `{"dialect":"vibedb-query","version":"1","from":"orders"}`,
			code: "invalid_version", pointer: "/version",
		},
		{
			name: "version spelling", src: `{"dialect":"vibedb-query","version":1.0,"from":"orders"}`,
			code: "unsupported_version", pointer: "/version",
		},
		{
			name: "unsupported version", src: `{"dialect":"vibedb-query","version":2,"from":"orders"}`,
			code: "unsupported_version", pointer: "/version",
		},
		{
			name: "collection type", src: `{"dialect":"vibedb-query","version":1,"from":false}`,
			code: "invalid_operand", pointer: "/from",
		},
		{
			name: "collection slash", src: `{"dialect":"vibedb-query","version":1,"from":"a/b"}`,
			code: "invalid_collection", pointer: "/from",
		},
		{
			name: "collection storage suffix", src: `{"dialect":"vibedb-query","version":1,"from":"a.vjc"}`,
			code: "invalid_collection", pointer: "/from",
		},
		{
			name: "unknown root member", src: nativeSyntaxDocument(`"extra":true`),
			code: "unknown_member", pointer: "/extra",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(t, test.src, test.code, test.pointer)
		})
	}
}

func TestNativeSyntaxRejectsDecodedDuplicateMembers(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		pointer string
	}{
		{
			name: "root",
			src: `{
				"dialect":"vibedb-query","version":1,
				"from":"orders","fr\u006fm":"other"
			}`,
			pointer: "/from",
		},
		{
			name: "projection",
			src: nativeSyntaxDocument(
				`"select":[{"name":"a","na\u006de":"b","path":"/a"}]`,
			),
			pointer: "/select/0/name",
		},
		{
			name: "field operator",
			src: nativeSyntaxDocument(
				`"where":{"/a":{"$eq":1,"\u0024eq":2}}`,
			),
			pointer: "/where/~1a/$eq",
		},
		{
			name: "join condition",
			src: nativeSyntaxDocument(
				`"join":{"from":"inner","as":"i","on":{"outer":"/a","\u006futer":"/b","inner":"$key"}}`,
			),
			pointer: "/join/on/outer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(t, test.src, "duplicate_member", test.pointer)
		})
	}
}

func TestNativeSyntaxStrictClauseShapesAndUnknownMembers(t *testing.T) {
	tests := []struct {
		name    string
		clause  string
		code    string
		pointer string
	}{
		{
			name: "where object", clause: `"where":[]`,
			code: "invalid_object", pointer: "/where",
		},
		{
			name: "select array", clause: `"select":{}`,
			code: "invalid_clause", pointer: "/select",
		},
		{
			name: "select nonempty", clause: `"select":[]`,
			code: "invalid_clause", pointer: "/select",
		},
		{
			name: "select unknown", clause: `"select":[{"name":"a","extra":"/a"}]`,
			code: "unknown_member", pointer: "/select/0/extra",
		},
		{
			name: "order array", clause: `"orderBy":{}`,
			code: "invalid_clause", pointer: "/orderBy",
		},
		{
			name: "order nonempty", clause: `"orderBy":[]`,
			code: "invalid_clause", pointer: "/orderBy",
		},
		{
			name: "order unknown", clause: `"orderBy":[{"path":"/a","dir":"asc"}]`,
			code: "unknown_member", pointer: "/orderBy/0/dir",
		},
		{
			name: "order exact fields", clause: `"orderBy":[{"path":"/a"}]`,
			code: "invalid_order", pointer: "/orderBy/0",
		},
		{
			name: "direction closed", clause: `"orderBy":[{"path":"/a","direction":"ASC"}]`,
			code: "invalid_direction", pointer: "/orderBy/0/direction",
		},
		{
			name: "exists array", clause: `"exists":{}`,
			code: "invalid_clause", pointer: "/exists",
		},
		{
			name: "exists nonempty", clause: `"exists":[]`,
			code: "invalid_clause", pointer: "/exists",
		},
		{
			name: "join object", clause: `"join":[]`,
			code: "invalid_object", pointer: "/join",
		},
		{
			name: "join unknown", clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"},"extra":true}`,
			code: "unknown_member", pointer: "/join/extra",
		},
		{
			name: "on unknown", clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","left":"$key"}}`,
			code: "unknown_member", pointer: "/join/on/left",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(test.clause), test.code, test.pointer,
			)
		})
	}
}

func TestNativeSyntaxPreservesExactNumbersAndChecksLimits(t *testing.T) {
	got := mustParseNativeSyntax(t, nativeSyntaxDocument(
		`"where":{"/n":{"$in":[0,-0,1,1.0,1e0,1.2300e+02]}}`,
	))
	if got.where == nil || got.where.operand.kind != nativeOperandList {
		t.Fatalf("where = %#v", got.where)
	}
	want := []string{"0", "-0", "1", "1.0", "1e0", "1.2300e+02"}
	if len(got.where.operand.list) != len(want) {
		t.Fatalf("numbers = %#v", got.where.operand.list)
	}
	for i, text := range want {
		scalar := got.where.operand.list[i]
		if scalar.kind != qNumber || scalar.text != text {
			t.Errorf("number %d = (%d, %q), want (%d, %q)",
				i, scalar.kind, scalar.text, qNumber, text)
		}
	}

	for _, text := range []string{"0", strconv.Itoa(nativeMaxLimit)} {
		t.Run("limit "+text, func(t *testing.T) {
			query := mustParseNativeSyntax(
				t, nativeSyntaxDocument(`"limit":`+text),
			)
			if !query.hasLimit || query.limit.kind != nativeOperandScalar ||
				query.limit.scalar.kind != qNumber ||
				query.limit.scalar.text != text {
				t.Fatalf("limit = (%v, %#v)", query.hasLimit, query.limit)
			}
		})
	}
	parameter := mustParseNativeSyntax(
		t, nativeSyntaxDocument(`"limit":{"$param":"rows"}`),
	)
	if !parameter.hasLimit || parameter.limit.kind != nativeOperandParameter ||
		parameter.limit.param != "rows" {
		t.Fatalf("parameter limit = (%v, %#v)", parameter.hasLimit, parameter.limit)
	}

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "negative", value: "-1"},
		{name: "fraction spelling", value: "1.0"},
		{name: "exponent spelling", value: "1e0"},
		{name: "too large", value: strconv.Itoa(nativeMaxLimit + 1)},
		{name: "string", value: `"1"`},
		{name: "boolean", value: "true"},
		{name: "null", value: "null"},
	} {
		t.Run("invalid "+test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(`"limit":`+test.value),
				"invalid_limit", "/limit",
			)
		})
	}
	assertNativeSyntaxError(
		t,
		nativeSyntaxDocument(`"limit":{"value":1}`),
		"invalid_operand",
		"/limit",
	)
}

func TestNativeSyntaxPredicateOperators(t *testing.T) {
	got := mustParseNativeSyntax(t, nativeSyntaxDocument(`"where":{
		"/v":{
			"$eq":null,
			"$ne":{"$param":"different"},
			"$lt":"z",
			"$lte":-0,
			"$gt":1.00,
			"$gte":{"$param":"minimum"},
			"$in":[],
			"$nin":{"$param":"excluded"},
			"$exists":true,
			"$null":true,
			"$missing":true,
			"$nullish":true
		}
	}`))
	if got.where == nil || got.where.kind != nativePredicateAnd {
		t.Fatalf("where = %#v", got.where)
	}
	wantOperators := []nativeFieldOperator{
		nativeFieldEq,
		nativeFieldNe,
		nativeFieldLt,
		nativeFieldLe,
		nativeFieldGt,
		nativeFieldGe,
		nativeFieldIn,
		nativeFieldNotIn,
		nativeFieldExists,
		nativeFieldNull,
		nativeFieldMissing,
		nativeFieldNullish,
	}
	if len(got.where.children) != len(wantOperators) {
		t.Fatalf("operator count = %d, want %d", len(got.where.children), len(wantOperators))
	}
	for i, operator := range wantOperators {
		child := got.where.children[i]
		if child.kind != nativePredicateField || child.path != "/v" ||
			child.operator != operator {
			t.Errorf("operator %d = %#v, want %d", i, child, operator)
		}
	}
	if got.where.children[0].operand.scalar.kind != qNull {
		t.Errorf("$eq operand = %#v", got.where.children[0].operand)
	}
	if got.where.children[1].operand.kind != nativeOperandParameter ||
		got.where.children[1].operand.param != "different" {
		t.Errorf("$ne operand = %#v", got.where.children[1].operand)
	}
	if got.where.children[2].operand.scalar.kind != qString ||
		got.where.children[2].operand.scalar.text != "z" {
		t.Errorf("$lt operand = %#v", got.where.children[2].operand)
	}
	if got.where.children[3].operand.scalar.text != "-0" ||
		got.where.children[4].operand.scalar.text != "1.00" {
		t.Errorf("ordered number spellings = (%#v, %#v)",
			got.where.children[3].operand, got.where.children[4].operand)
	}
	if got.where.children[6].operand.kind != nativeOperandList ||
		len(got.where.children[6].operand.list) != 0 {
		t.Errorf("$in operand = %#v", got.where.children[6].operand)
	}
	if got.where.children[7].operand.kind != nativeOperandParameter ||
		got.where.children[7].operand.param != "excluded" {
		t.Errorf("$nin operand = %#v", got.where.children[7].operand)
	}
	for i := 8; i < len(got.where.children); i++ {
		if got.where.children[i].operand.kind != nativeOperandInvalid {
			t.Errorf("presence operand %d = %#v", i, got.where.children[i].operand)
		}
	}
}

func TestNativeSyntaxBooleanPredicates(t *testing.T) {
	got := mustParseNativeSyntax(t, nativeSyntaxDocument(`"where":{
		"$not":{"$or":[
			{"/a":1},
			{"/b":{"$ne":2}}
		]}
	}`))
	if got.where == nil || got.where.kind != nativePredicateNot ||
		len(got.where.children) != 1 {
		t.Fatalf("$not = %#v", got.where)
	}
	or := got.where.children[0]
	if or.kind != nativePredicateOr || len(or.children) != 2 ||
		or.children[0].path != "/a" ||
		or.children[0].operator != nativeFieldEq ||
		or.children[1].path != "/b" ||
		or.children[1].operator != nativeFieldNe {
		t.Fatalf("$or = %#v", or)
	}

	empty := mustParseNativeSyntax(
		t, nativeSyntaxDocument(`"where":{"$and":[]}`),
	)
	if empty.where == nil || empty.where.kind != nativePredicateAnd ||
		len(empty.where.children) != 0 {
		t.Fatalf("empty $and = %#v", empty.where)
	}
}

func TestNativeSyntaxPredicateRejections(t *testing.T) {
	tests := []struct {
		name    string
		where   string
		code    string
		pointer string
	}{
		{
			name: "empty predicate", where: `{}`,
			code: "invalid_predicate", pointer: "/where",
		},
		{
			name: "boolean mixed with field", where: `{"$and":[],"/a":1}`,
			code: "invalid_predicate", pointer: "/where",
		},
		{
			name: "unknown boolean", where: `{"$xor":[]}`,
			code: "invalid_operator", pointer: "/where/$xor",
		},
		{
			name: "and array", where: `{"$and":{}}`,
			code: "invalid_operand", pointer: "/where/$and",
		},
		{
			name: "not predicate", where: `{"$not":[]}`,
			code: "invalid_object", pointer: "/where/$not",
		},
		{
			name: "unknown field operator", where: `{"/a":{"$wat":1}}`,
			code: "invalid_operator", pointer: "/where/~1a/$wat",
		},
		{
			name: "empty operator object", where: `{"/a":{}}`,
			code: "invalid_predicate", pointer: "/where/~1a",
		},
		{
			name: "array shorthand", where: `{"/a":[1]}`,
			code: "invalid_operand", pointer: "/where/~1a",
		},
		{
			name: "ordered boolean", where: `{"/a":{"$gt":true}}`,
			code: "invalid_operand", pointer: "/where/~1a/$gt",
		},
		{
			name: "membership scalar", where: `{"/a":{"$in":1}}`,
			code: "invalid_operand", pointer: "/where/~1a/$in",
		},
		{
			name: "membership container", where: `{"/a":{"$in":[{}]}}`,
			code: "invalid_operand", pointer: "/where/~1a/$in/0",
		},
		{
			name: "presence must be true", where: `{"/a":{"$exists":false}}`,
			code: "invalid_operand", pointer: "/where/~1a/$exists",
		},
		{
			name: "parameter exact object", where: `{"/a":{"$eq":{"$param":"x","extra":1}}}`,
			code: "invalid_operand", pointer: "/where/~1a/$eq",
		},
		{
			name: "parameter name type", where: `{"/a":{"$eq":{"$param":1}}}`,
			code: "invalid_operand", pointer: "/where/~1a/$eq/$param",
		},
		{
			name: "parameter name nonempty", where: `{"/a":{"$eq":{"$param":""}}}`,
			code: "invalid_parameter", pointer: "/where/~1a/$eq/$param",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(`"where":`+test.where),
				test.code, test.pointer,
			)
		})
	}
}

func TestNativeSyntaxRFC6901PathsOnly(t *testing.T) {
	got := mustParseNativeSyntax(t, nativeSyntaxDocument(
		`"where":{"":true,"/a~1b/~0c":false}`,
	))
	if got.where == nil || got.where.kind != nativePredicateAnd ||
		len(got.where.children) != 2 ||
		got.where.children[0].path != "" ||
		got.where.children[1].path != "/a~1b/~0c" {
		t.Fatalf("paths = %#v", got.where)
	}

	tests := []struct {
		name    string
		clause  string
		pointer string
	}{
		{
			name: "dotted predicate", clause: `"where":{"user.name":"x"}`,
			pointer: "/where/user.name",
		},
		{
			name: "bad pointer escape", clause: `"where":{"/a~2b":"x"}`,
			pointer: "/where/~1a~02b",
		},
		{
			name: "dotted projection", clause: `"select":[{"name":"name","path":"user.name"}]`,
			pointer: "/select/0/path",
		},
		{
			name: "dotted sort", clause: `"orderBy":[{"path":"user.name","direction":"asc"}]`,
			pointer: "/orderBy/0/path",
		},
		{
			name: "dotted join", clause: `"join":{"from":"inner","as":"i","on":{"outer":"user.id","inner":"$key"}}`,
			pointer: "/join/on/outer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(test.clause),
				"invalid_path", test.pointer,
			)
		})
	}
}

func TestNativeSyntaxJoinAndAliasRules(t *testing.T) {
	tests := []struct {
		name    string
		clause  string
		code    string
		pointer string
	}{
		{
			name:   "exists forbids alias",
			clause: `"exists":[{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"}}]`,
			code:   "unknown_member", pointer: "/exists/0/as",
		},
		{
			name:   "join requires alias",
			clause: `"join":{"from":"inner","on":{"outer":"/a","inner":"$key"}}`,
			code:   "missing_member", pointer: "/join",
		},
		{
			name:   "alias grammar",
			clause: `"join":{"from":"inner","as":"1inner","on":{"outer":"/a","inner":"$key"}}`,
			code:   "invalid_alias", pointer: "/join/as",
		},
		{
			name:   "on exact shape",
			clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a"}}`,
			code:   "invalid_join", pointer: "/join/on",
		},
		{
			name:   "join requires projection",
			clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"}}`,
			code:   "missing_member", pointer: "/join",
		},
		{
			name:   "join requires joined projection",
			clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"}},"select":[{"name":"a","path":"/a"}]`,
			code:   "invalid_projection", pointer: "/select",
		},
		{
			name:   "unknown projection alias",
			clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"}},"select":[{"name":"a","path":"@other/a"}]`,
			code:   "unknown_alias", pointer: "/select/0/path",
		},
		{
			name:   "unknown sort alias",
			clause: `"join":{"from":"inner","as":"i","on":{"outer":"/a","inner":"$key"}},"select":[{"name":"a","path":"@i/a"}],"orderBy":[{"path":"@other/a","direction":"asc"}]`,
			code:   "unknown_alias", pointer: "/orderBy/0/path",
		},
		{
			name:   "qualified path without join",
			clause: `"select":[{"name":"a","path":"@i/a"}]`,
			code:   "unknown_alias", pointer: "/select/0/path",
		},
		{
			name:   "invalid qualified alias",
			clause: `"select":[{"name":"a","path":"@1i/a"}]`,
			code:   "invalid_alias", pointer: "/select/0/path",
		},
		{
			name:   "duplicate output name",
			clause: `"select":[{"name":"a","path":"/a"},{"name":"a","path":"/b"}]`,
			code:   "duplicate_name", pointer: "/select/1/name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(test.clause), test.code, test.pointer,
			)
		})
	}
}

func TestNativeSyntaxStructuralBounds(t *testing.T) {
	assertNativeSyntaxError(
		t,
		strings.Repeat(" ", nativeMaxDocumentBytes+1),
		"limit_exceeded",
		"",
	)
	assertNativeSyntaxError(
		t,
		strings.Repeat("[", nativeMaxDepth+1)+"0"+
			strings.Repeat("]", nativeMaxDepth+1),
		"limit_exceeded",
		"",
	)

	tests := []struct {
		name    string
		clause  string
		code    string
		pointer string
	}{
		{
			name: "projection count",
			clause: `"select":[` +
				nativeSyntaxRepeatedJSON(`{"name":"a","path":"/a"}`, nativeMaxProjection+1) +
				`]`,
			code: "limit_exceeded", pointer: "/select",
		},
		{
			name: "sort count",
			clause: `"orderBy":[` +
				nativeSyntaxRepeatedJSON(`{"path":"/a","direction":"asc"}`, nativeMaxSortKeys+1) +
				`]`,
			code: "limit_exceeded", pointer: "/orderBy",
		},
		{
			name: "exists count",
			clause: `"exists":[` +
				nativeSyntaxRepeatedJSON(
					`{"from":"inner","on":{"outer":"/a","inner":"$key"}}`,
					nativeMaxExists+1,
				) +
				`]`,
			code: "limit_exceeded", pointer: "/exists",
		},
		{
			name: "boolean fan in",
			clause: `"where":{"$and":[` +
				nativeSyntaxRepeatedJSON(`{"/a":1}`, nativeMaxBooleanFanIn+1) +
				`]}`,
			code: "limit_exceeded", pointer: "/where/$and",
		},
		{
			name: "membership count",
			clause: `"where":{"/a":{"$in":[` +
				nativeSyntaxRepeatedJSON(`0`, nativeMaxMembershipItems+1) +
				`]}}`,
			code: "limit_exceeded", pointer: "/where/~1a/$in",
		},
		{
			name: "literal bytes",
			clause: `"where":{"/a":{"$eq":"` +
				strings.Repeat("x", nativeMaxLiteralBytes+1) +
				`"}}`,
			code: "limit_exceeded", pointer: "/where/~1a/$eq",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertNativeSyntaxError(
				t, nativeSyntaxDocument(test.clause), test.code, test.pointer,
			)
		})
	}

	longPointer := "/" + strings.Repeat("a", nativeMaxPointerBytes)
	assertNativeSyntaxError(
		t,
		nativeSyntaxDocument(`"where":{`+strconv.Quote(longPointer)+`:1}`),
		"invalid_path",
		nativePointerMember("/where", longPointer),
	)

	var parameters strings.Builder
	for i := 0; i <= nativeMaxParameters; i++ {
		if i > 0 {
			parameters.WriteByte(',')
		}
		path := "/p" + strconv.Itoa(i)
		name := "p" + strconv.Itoa(i)
		parameters.WriteString(strconv.Quote(path))
		parameters.WriteString(`:{"$eq":{"$param":`)
		parameters.WriteString(strconv.Quote(name))
		parameters.WriteString(`}}`)
	}
	assertNativeSyntaxError(
		t,
		nativeSyntaxDocument(`"where":{`+parameters.String()+`}`),
		"limit_exceeded",
		"/where/~1p256/$eq/$param",
	)
}

func nativeSyntaxDocument(clauses string) string {
	const prefix = `{"dialect":"vibedb-query","version":1,"from":"orders"`
	if clauses == "" {
		return prefix + "}"
	}
	return prefix + "," + clauses + "}"
}

func mustParseNativeSyntax(t *testing.T, src string) nativeQuerySyntax {
	t.Helper()
	query, err := parseNativeQuerySyntax([]byte(src))
	if err != nil {
		t.Fatalf("parseNativeQuerySyntax() error = %v", err)
	}
	return query
}

func assertNativeSyntaxError(
	t *testing.T, src, code, pointer string,
) *nativeSyntaxError {
	t.Helper()
	_, err := parseNativeQuerySyntax([]byte(src))
	if err == nil {
		t.Fatalf("parseNativeQuerySyntax() error = nil, want %s at %q", code, pointer)
	}
	var syntaxErr *nativeSyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("error type = %T (%v), want *nativeSyntaxError", err, err)
	}
	if syntaxErr.code != code || syntaxErr.pointer != pointer {
		t.Fatalf("error = %s at %q (%v), want %s at %q",
			syntaxErr.code, syntaxErr.pointer, syntaxErr, code, pointer)
	}
	return syntaxErr
}

func nativeSyntaxRepeatedJSON(item string, count int) string {
	var out strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteString(item)
	}
	return out.String()
}
