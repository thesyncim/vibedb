package query

import (
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestRawEqualsScalar(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		lit  scalar
		want bool
	}{
		{name: "clean string", raw: `"PT"`, lit: scalar{kind: kindString, sval: "PT"}, want: true},
		{name: "escaped string", raw: `"P\u0054"`, lit: scalar{kind: kindString, sval: "PT"}, want: true},
		{name: "wrong string", raw: `"US"`, lit: scalar{kind: kindString, sval: "PT"}},
		{name: "integer spelling", raw: `1`, lit: scalar{kind: kindNumber, num: []byte("1"), isInt: true, ival: 1}, want: true},
		{name: "decimal spelling", raw: `1.0`, lit: scalar{kind: kindNumber, num: []byte("1"), isInt: true, ival: 1}, want: true},
		{name: "different number", raw: `1.01`, lit: scalar{kind: kindNumber, num: []byte("1"), isInt: true, ival: 1}},
		{name: "true", raw: `true`, lit: scalar{kind: kindBool, bval: true}, want: true},
		{name: "false", raw: `false`, lit: scalar{kind: kindBool, bval: false}, want: true},
		{name: "null never equals", raw: `null`, lit: scalar{kind: kindString, sval: "null"}},
		{name: "missing never equals", raw: ``, lit: scalar{kind: kindString, sval: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var text []byte
			got := rawEqualsScalar(vibejson.RawValue{Src: []byte(tc.raw)}, tc.lit, &text)
			if got != tc.want {
				t.Fatalf("rawEqualsScalar(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRawTopLevelScalarMatch(t *testing.T) {
	lit := scalar{kind: kindString, sval: "PT"}
	cases := []struct {
		name     string
		doc      string
		want     bool
		complete bool
	}{
		{name: "flat hit", doc: `{"nested":{"country":"PT"},"country":"PT"}`, want: true, complete: true},
		{name: "nested only", doc: `{"nested":{"country":"PT"}}`, complete: true},
		{name: "last duplicate wins", doc: `{"country":"PT","country":"US"}`, complete: true},
		{name: "escaped key falls back", doc: `{"coun\u0074ry":"PT"}`, complete: false},
		{name: "leading space", doc: "  {\"country\":\"PT\"}", want: true, complete: true},
		{name: "array root", doc: `[ {"country":"PT"} ]`, complete: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var text []byte
			got, complete := rawTopLevelScalarMatch(
				[]byte(tc.doc), "country", lit, &text,
			)
			if got != tc.want || complete != tc.complete {
				t.Fatalf("rawTopLevelScalarMatch(%q) = (%v, %v), want (%v, %v)",
					tc.doc, got, complete, tc.want, tc.complete)
			}
		})
	}
}
