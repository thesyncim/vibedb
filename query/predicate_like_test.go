package query

import (
	"strings"
	"testing"
)

func TestLikeBacktrackingVisitsEveryRuneBoundary(t *testing.T) {
	for _, tc := range []struct {
		pattern     string
		text        string
		insensitive bool
		want        bool
	}{
		{pattern: "%ab", text: "aab", want: true},
		{pattern: "%ab", text: "éaab", want: true},
		{pattern: "%界b", text: "éa界b", want: true},
		{pattern: "%AB", text: "éaab", insensitive: true, want: true},
		{pattern: "%ab", text: "éaac", want: false},
		{pattern: "%%a%%b", text: "éa界b", want: true},
	} {
		if got := likeMatch(tc.pattern, tc.text, tc.insensitive); got != tc.want {
			t.Errorf("likeMatch(%q, %q, %v) = %v, want %v",
				tc.pattern, tc.text, tc.insensitive, got, tc.want)
		}
	}
}

func TestLikeMatcherExhaustiveAgainstReference(t *testing.T) {
	patterns := generatedStrings([]string{
		"a", "b", "A", "%", "_", `\%`, `\_`, `\\`, "é",
	}, 3)
	texts := generatedStrings([]string{"a", "b", "A", "é"}, 4)
	for _, insensitive := range []bool{false, true} {
		for _, pattern := range patterns {
			for _, value := range texts {
				want := referenceLike(pattern, value, insensitive)
				if got := likeMatch(pattern, value, insensitive); got != want {
					t.Fatalf("likeMatch(%q, %q, %v) = %v, want %v",
						pattern, value, insensitive, got, want)
				}
			}
		}
	}
}

func generatedStrings(atoms []string, max int) []string {
	result := []string{""}
	frontier := []string{""}
	for range max {
		next := make([]string, 0, len(frontier)*len(atoms))
		for _, prefix := range frontier {
			for _, atom := range atoms {
				next = append(next, prefix+atom)
			}
		}
		result = append(result, next...)
		frontier = next
	}
	return result
}

type referenceLikeToken struct {
	kind rune
	lit  rune
}

func referenceLike(pattern, value string, insensitive bool) bool {
	patternRunes := []rune(pattern)
	tokens := make([]referenceLikeToken, 0, len(patternRunes))
	for i := 0; i < len(patternRunes); i++ {
		switch patternRunes[i] {
		case '\\':
			i++
			if i == len(patternRunes) {
				return false
			}
			tokens = append(tokens, referenceLikeToken{kind: 'c', lit: patternRunes[i]})
		case '%', '_':
			tokens = append(tokens, referenceLikeToken{kind: patternRunes[i]})
		default:
			tokens = append(tokens, referenceLikeToken{kind: 'c', lit: patternRunes[i]})
		}
	}
	text := []rune(value)
	type state struct{ pattern, text int }
	memo := make(map[state]bool, (len(tokens)+1)*(len(text)+1))
	seen := make(map[state]bool, len(memo))
	var match func(int, int) bool
	match = func(pi, ti int) bool {
		key := state{pi, ti}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		var result bool
		switch {
		case pi == len(tokens):
			result = ti == len(text)
		case tokens[pi].kind == '%':
			result = match(pi+1, ti) || ti < len(text) && match(pi, ti+1)
		case ti == len(text):
			result = false
		case tokens[pi].kind == '_':
			result = match(pi+1, ti+1)
		default:
			equal := tokens[pi].lit == text[ti]
			if insensitive {
				equal = strings.EqualFold(string(tokens[pi].lit), string(text[ti]))
			}
			result = equal && match(pi+1, ti+1)
		}
		memo[key] = result
		return result
	}
	return match(0, 0)
}
