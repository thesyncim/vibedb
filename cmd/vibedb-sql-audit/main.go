// Command vibedb-sql-audit inventories SQL feature evidence in a local
// source repository. It never connects to an application database.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type evidence struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Kind string `json:"kind"`
}

type feature struct {
	ID       string     `json:"id"`
	Pattern  string     `json:"pattern"`
	Evidence []evidence `json:"evidence"`
}

type report struct {
	SourceContentSHA256 string    `json:"source_content_sha256"`
	SourceRevision      string    `json:"source_revision"`
	SourceDirty         bool      `json:"source_dirty"`
	Roots               []string  `json:"roots"`
	Files               int       `json:"files"`
	Fragments           int       `json:"fragments"`
	Features            []feature `json:"features"`
}

// These are evidence classifiers, not a SQL parser or a compatibility claim.
// Go string literals include ORM fragments as well as complete statements.
// SQL is deliberately not copied into the report; retain only source locations.
var patterns = []struct{ id, pattern string }{
	{"coalesce", `\bCOALESCE\s*\(`},
	{"greatest_least", `\b(?:GREATEST|LEAST)\s*\(`},
	{"nullif", `\bNULLIF\s*\(`},
	{"boolean_tests", `\bIS\s+(?:NOT\s+)?(?:TRUE|FALSE|UNKNOWN)\b`},
	{"null_ordering", `\bNULLS\s+(?:FIRST|LAST)\b`},
	{"composite_primary_keys", `\bPRIMARY\s+KEY\s*\([^)]*,`},
	{"on_conflict_target", `\b(?:ON\s+)?CONFLICT\s*\(`},
	{"on_conflict", `\b(?:ON\s+)?CONFLICT\b`},
	{"returning", `\bRETURNING\b`},
	{"jsonb", `\bJSONB\b|type:jsonb`},
	{"json_functions", `\b(?:JSONB?_(?:SET|TYPEOF|ARRAY_LENGTH|ARRAY_ELEMENTS(?:_TEXT)?|EACH(?:_TEXT)?|BUILD_OBJECT|BUILD_ARRAY|OBJECT_AGG|AGG|STRIP_NULLS|PATH_EXISTS)|TO_JSONB?|ARRAY_TO_JSON)\s*\(`},
	{"json_operators", `->>?|#>>?|@>|<@|\?\||\?&`},
	{"array_functions", `\b(?:UNNEST|ARRAY_(?:AGG|LENGTH|APPEND|PREPEND|REMOVE|CAT|TO_STRING|POSITION)|CARDINALITY|STRING_TO_ARRAY)\s*\(`},
	{"array_syntax", `\bARRAY\s*\[|::\s*(?:TEXT|VARCHAR|BIGINT|INT|INTEGER|UUID)\s*\[\]|\b(?:ANY|ALL)\s*\(`},
	{"timestamps_intervals", `\b(?:TIMESTAMPTZ|TIMESTAMP|INTERVAL|DATE_TRUNC|EXTRACT|NOW|CURRENT_TIMESTAMP|STATEMENT_TIMESTAMP|CLOCK_TIMESTAMP)\b`},
	{"uuid_serial", `\b(?:UUID|BIGSERIAL|SERIAL|GEN_RANDOM_UUID|UUID_GENERATE_V4|NEXTVAL)\b`},
	{"type_modifiers", `\b(?:VARCHAR|CHAR|NUMERIC|DECIMAL)\s*\(\s*\d`},
	{"schema_defaults_constraints", `\b(?:DEFAULT|CHECK|REFERENCES|GENERATED|FOREIGN\s+KEY)\b`},
	{"indexes", `\b(?:CREATE\s+(?:UNIQUE\s+)?INDEX|STORING|INCLUDE|USING\s+(?:BTREE|GIN|GIST|HASH))\b`},
	{"locking", `\b(?:FOR\s+(?:NO\s+KEY\s+)?UPDATE|FOR\s+(?:KEY\s+)?SHARE|SKIP\s+LOCKED|NOWAIT|PG_ADVISORY\w*)\b`},
	{"update_from_delete_using", `\bUPDATE\b[\s\S]*\bFROM\b|\bDELETE\b[\s\S]*\bUSING\b`},
	{"mutation_cte", `\bAS\s*\(\s*(?:INSERT|UPDATE|DELETE)\b`},
	{"distinct_on", `\bDISTINCT\s+ON\s*\(`},
	{"aggregate_extensions", `\b(?:COUNT|SUM|AVG)\s*\(\s*DISTINCT|\bFILTER\s*\(\s*WHERE|\b(?:STRING_AGG|BOOL_OR|BOOL_AND|JSONB?_AGG)\s*\(`},
	{"row_comparisons", `\([^()]*,[^()]*\)\s*(?:IN\b|[<>]=?|=)\s*\(`},
	{"string_functions", `\b(?:LOWER|UPPER|LENGTH|CHAR_LENGTH|REPLACE|CONCAT|CONCAT_WS|SUBSTRING|SPLIT_PART|TRIM|REGEXP_REPLACE|ENCODE|DECODE|MD5)\s*\(`},
	{"null_safe_comparison", `\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b`},
	{"ddl_alter", `\bALTER\s+(?:TABLE|INDEX|TYPE)\b`},
	{"copy", `\bCOPY\s+\w|COPY\s*\(`},
	{"session_catalog", `\b(?:SET\s+(?:LOCAL|SESSION|TIME|SEARCH_PATH|STATEMENT_TIMEOUT|IDLE_IN_TRANSACTION_SESSION_TIMEOUT)|PG_CATALOG\.|INFORMATION_SCHEMA\.|PG_CLASS\b)`},
	{"full_text_search_excluded", `\b(?:TSVECTOR|TSQUERY|TO_TSVECTOR|TO_TSQUERY|WEBSEARCH_TO_TSQUERY|PLAINTO_TSQUERY|TS_RANK|TS_RANK_CD)\b|@@`},
}

func main() {
	repo := flag.String("repo", "", "local source repository (required)")
	rootsFlag := flag.String("roots", ".", "comma-separated repository-relative source directories")
	flag.Parse()
	if *repo == "" {
		fmt.Fprintln(os.Stderr, "usage: vibedb-sql-audit -repo /path/to/repo [-roots dir1,dir2]")
		os.Exit(2)
	}
	roots := strings.Split(*rootsFlag, ",")
	for i := range roots {
		roots[i] = strings.TrimSpace(roots[i])
		if roots[i] == "" || filepath.IsAbs(roots[i]) || roots[i] == ".." || strings.HasPrefix(roots[i], "../") {
			fmt.Fprintln(os.Stderr, "roots must name repository-relative directories")
			os.Exit(2)
		}
	}
	r, err := audit(*repo, roots)
	if err == nil {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(r)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func audit(repo string, roots []string) (*report, error) {
	git := func(args ...string) ([]byte, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		return cmd.Output()
	}
	revision, err := git("rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	status, err := git("status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return nil, err
	}
	files, err := git(append([]string{"ls-files", "-z", "--"}, roots...)...)
	if err != nil {
		return nil, err
	}
	r := &report{SourceRevision: strings.TrimSpace(string(revision)), SourceDirty: len(status) != 0, Roots: roots}
	digest := sha256.New()
	compiled := make([]*regexp.Regexp, len(patterns))
	seen := make([]map[evidence]bool, len(patterns))
	for i, p := range patterns {
		r.Features = append(r.Features, feature{ID: p.id, Pattern: p.pattern, Evidence: []evidence{}})
		compiled[i] = regexp.MustCompile("(?i)" + p.pattern)
		seen[i] = make(map[evidence]bool)
	}
	visit := func(text, path string, line int, kind string) {
		r.Fragments++
		for i, re := range compiled {
			if !re.MatchString(text) {
				continue
			}
			e := evidence{Path: path, Line: line, Kind: kind}
			if !seen[i][e] {
				r.Features[i].Evidence = append(r.Features[i].Evidence, e)
				seen[i][e] = true
			}
		}
	}
	for _, path := range strings.Split(string(files), "\x00") {
		if !inScope(path) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(path)))
		if err != nil {
			return nil, err
		}
		r.Files++
		fmt.Fprintf(digest, "%s\x00%d\x00", path, len(data))
		digest.Write(data)
		if strings.HasSuffix(path, ".sql") {
			// Classify whole migrations so multiline constraints and CTEs are
			// visible. Locations identify the file; SQL comments are removed.
			text := sqlComments.ReplaceAllString(string(data), " ")
			for i, re := range compiled {
				if re.MatchString(text) {
					r.Features[i].Evidence = append(r.Features[i].Evidence, evidence{Path: path, Line: 1, Kind: "schema"})
				}
			}
			r.Fragments++
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, data, 0)
		if err != nil {
			return nil, err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && len(call.Args) != 0 {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					prefix := ""
					switch selector.Sel.Name {
					case "OnConflict":
						prefix = "ON CONFLICT "
					case "Returning":
						prefix = "RETURNING "
					case "DistinctOn":
						prefix = "DISTINCT ON ("
					case "For":
						prefix = "FOR "
					}
					if literal, ok := call.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING && prefix != "" {
						if value, err := strconv.Unquote(literal.Value); err == nil {
							visit(prefix+value, path, fset.Position(call.Pos()).Line, "orm_clause")
						}
					}
				}
			}
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			kind := "go_literal"
			if strings.Contains(value, `pg:"`) || strings.Contains(value, `bun:"`) {
				kind = "model_tag"
			}
			visit(value, path, fset.Position(literal.Pos()).Line, kind)
			return true
		})
	}
	for i := range r.Features {
		sort.Slice(r.Features[i].Evidence, func(a, b int) bool {
			x, y := r.Features[i].Evidence[a], r.Features[i].Evidence[b]
			if x.Path != y.Path {
				return x.Path < y.Path
			}
			return x.Line < y.Line
		})
	}
	r.SourceContentSHA256 = fmt.Sprintf("%x", digest.Sum(nil))
	return r, nil
}

var sqlComments = regexp.MustCompile(`(?s)/\*.*?\*/|--[^\n]*`)

func inScope(path string) bool {
	if strings.HasSuffix(path, "_test.go") || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".sql")) {
		return false
	}
	return !strings.Contains(path, "/mock") && !strings.Contains(path, "_mock.") && !strings.Contains(path, "/testdata/")
}
