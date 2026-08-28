package coverage

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var requiredBenchmarkCoverage = []struct {
	dimension string
	cases     []string
}{
	{"durability", []string{"buffered-visible", "checkpointed", "strongest sync"}},
	{"indexes", []string{"none", "one exact", "several exact"}},
	{"document size", []string{"inline", "mixed", "overflow-heavy"}},
	{"working set", []string{"fits cache", "larger than cache", "larger than RAM"}},
	{"cache state", []string{"hot reopen", "cold reopen"}},
	{"concurrency", []string{"1", "8", "32", "saturation"}},
	{"snapshot pressure", []string{"none", "long pinned"}},
	{"interfaces", []string{"native", "database/sql", "pgwire"}},
	{"lifecycle", []string{"open", "recovery", "checkpoint", "verify", "repack"}},
	{"latency", []string{"p50", "p95", "p99", "p99.9", "max"}},
	{"storage", []string{"logical", "allocated", "write amplification"}},
	{"stability", []string{"long churn", "periodic crashes"}},
}

func TestBenchmarkCoverageManifestCoversRequiredMatrix(t *testing.T) {
	manifest := BenchmarkCoverageManifest()
	wantCells := 0
	for _, dimension := range requiredBenchmarkCoverage {
		wantCells += len(dimension.cases)
	}
	if len(manifest) != wantCells {
		t.Fatalf("coverage cells = %d, want %d", len(manifest), wantCells)
	}

	cell := 0
	for _, required := range requiredBenchmarkCoverage {
		for _, requiredCase := range required.cases {
			lane := manifest[cell]
			if lane.Dimension != required.dimension || lane.Case != requiredCase {
				t.Fatalf(
					"coverage cell %d = %q/%q, want %q/%q",
					cell, lane.Dimension, lane.Case, required.dimension, requiredCase,
				)
			}
			cell++
		}
	}
}

// TestBenchmarkCoverageExitGateIsComplete keeps the review exit criterion
// executable. Adding a required cell, or weakening an existing cell back to a
// diagnostic/gap, must fail CI until a dedicated evidence harness exists.
func TestBenchmarkCoverageExitGateIsComplete(t *testing.T) {
	manifest := BenchmarkCoverageManifest()
	implemented, diagnostic, gaps := 0, 0, 0
	for _, lane := range manifest {
		switch lane.Status {
		case CoverageImplemented:
			implemented++
		case CoverageDiagnostic:
			diagnostic++
		case CoverageGap:
			gaps++
		}
	}
	if implemented != 38 || diagnostic != 0 || gaps != 0 {
		t.Fatalf("competitive evidence exit gate = %d implemented/%d diagnostic/%d gaps; want 38/0/0",
			implemented, diagnostic, gaps)
	}
}

func TestBenchmarkCoverageClaimsRequireExecutableEvidence(t *testing.T) {
	seen := make(map[string]struct{})
	for _, lane := range BenchmarkCoverageManifest() {
		name := lane.Dimension + "/" + lane.Case
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("duplicate coverage cell %q", name)
		}
		seen[name] = struct{}{}
		if strings.TrimSpace(lane.Boundary) == "" {
			t.Errorf("%s has no current-boundary statement", name)
		}

		switch lane.Status {
		case CoverageImplemented:
			if len(lane.Targets) == 0 {
				t.Errorf("implemented lane %s has no executable target", name)
			}
			hasHarness := false
			for _, target := range lane.Targets {
				hasHarness = hasHarness || target.Kind == CoverageCommand
			}
			if !hasHarness {
				t.Errorf("implemented lane %s has no dedicated command harness", name)
			}
		case CoverageDiagnostic:
			if len(lane.Targets) == 0 {
				t.Errorf("diagnostic lane %s has no executable target", name)
			}
		case CoverageGap:
			if len(lane.Targets) != 0 {
				t.Errorf("gap lane %s claims executable evidence", name)
			}
		default:
			t.Errorf("%s has invalid status %q", name, lane.Status)
		}
	}
}

func TestBenchmarkCoverageTargetsResolve(t *testing.T) {
	repoRoot := benchmarkCoverageRepoRoot(t)
	for _, lane := range BenchmarkCoverageManifest() {
		laneName := lane.Dimension + "/" + lane.Case
		for _, target := range lane.Targets {
			t.Run(laneName+"/"+target.Label, func(t *testing.T) {
				validateCoverageTarget(t, repoRoot, target)
			})
		}
	}
}

func TestBenchmarkCoverageDocumentIsGenerated(t *testing.T) {
	repoRoot := benchmarkCoverageRepoRoot(t)
	path := filepath.Join(repoRoot, "bench", "competitive", "COVERAGE.md")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := RenderBenchmarkCoverageMarkdown()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s does not match the executable manifest; run `cd bench/competitive && go generate .`", path)
	}
}

func TestBenchmarkCoverageEvidenceCatalogIsComplete(t *testing.T) {
	manifest := BenchmarkCoverageManifest()
	catalog, ids := benchmarkCoverageEvidenceCatalog(manifest)
	if len(catalog) == 0 {
		t.Fatal("evidence catalog is empty")
	}
	for i, item := range catalog {
		wantID := fmt.Sprintf("E%02d", i+1)
		if item.id != wantID {
			t.Fatalf("catalog item %d id = %q, want %q", i, item.id, wantID)
		}
		if item.command == "" || ids[item.command] != item.id {
			t.Fatalf("catalog item %s is not indexed by its command", item.id)
		}
	}
	for _, lane := range manifest {
		for _, target := range lane.Targets {
			if ids[coverageCommand(target)] == "" {
				t.Fatalf("%s/%s target %q is absent from the catalog",
					lane.Dimension, lane.Case, target.Label)
			}
		}
	}
}

func benchmarkCoverageRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller did not return the test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func validateCoverageTarget(t *testing.T, repoRoot string, target CoverageTarget) {
	t.Helper()
	if strings.TrimSpace(target.Label) == "" {
		t.Fatal("target has no label")
	}
	cleanPackage := filepath.ToSlash(filepath.Clean(target.Package))
	if cleanPackage != target.Package || cleanPackage == "." ||
		strings.HasPrefix(cleanPackage, "../") || filepath.IsAbs(target.Package) {
		t.Fatalf("package %q is not a clean repository-relative path", target.Package)
	}
	packageDir := filepath.Join(repoRoot, filepath.FromSlash(target.Package))
	info, err := os.Stat(packageDir)
	if err != nil {
		t.Fatalf("target package %q: %v", target.Package, err)
	}
	if !info.IsDir() {
		t.Fatalf("target package %q is not a directory", target.Package)
	}
	validateCoverageEnv(t, target.Env)

	switch target.Kind {
	case CoverageCommand:
		if target.Symbol != "" {
			t.Fatalf("command target unexpectedly names symbol %q", target.Symbol)
		}
		if !strings.HasPrefix(target.Package, "bench/competitive/cmd/") && target.Package != "bench/rf3chaos" {
			t.Fatalf("command target %q is outside the competitive command module", target.Package)
		}
		flags, hasMain := coverageCommandSurface(t, packageDir)
		if !hasMain {
			t.Fatalf("command target %q has no func main", target.Package)
		}
		seenFlags := make(map[string]struct{}, len(target.Args))
		for _, arg := range target.Args {
			if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
				t.Fatalf("command argument %q is not an explicit flag", arg)
			}
			name := coverageFlagName(arg)
			if name == "" {
				t.Fatalf("command argument %q has no flag name", arg)
			}
			if _, duplicate := seenFlags[name]; duplicate {
				t.Fatalf("command repeats flag -%s", name)
			}
			seenFlags[name] = struct{}{}
			if _, ok := flags[name]; !ok {
				t.Fatalf("command package %q no longer declares flag -%s", target.Package, name)
			}
		}
	case CoverageTest, CoverageBenchmark:
		if len(target.Args) != 0 {
			t.Fatalf("%s target unexpectedly has command arguments", target.Kind)
		}
		if target.Symbol == "" {
			t.Fatalf("%s target has no symbol", target.Kind)
		}
		prefix := "Test"
		if target.Kind == CoverageBenchmark {
			prefix = "Benchmark"
		}
		if !strings.HasPrefix(target.Symbol, prefix) {
			t.Fatalf("%s target symbol %q does not start with %s", target.Kind, target.Symbol, prefix)
		}
		if !coverageTestSymbolExists(t, packageDir, target.Symbol) {
			t.Fatalf("target symbol %s was not found in %s", target.Symbol, target.Package)
		}
	default:
		t.Fatalf("unknown target kind %q", target.Kind)
	}
	validateCoverageOutputContract(t, packageDir, target)
}

func validateCoverageEnv(t *testing.T, env []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(env))
	for _, assignment := range env {
		name, _, ok := strings.Cut(assignment, "=")
		if !ok || !validCoverageEnvName(name) {
			t.Fatalf("invalid environment assignment %q", assignment)
		}
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("duplicate environment variable %q", name)
		}
		seen[name] = struct{}{}
	}
}

func validCoverageEnvName(name string) bool {
	if name == "" || !((name[0] >= 'A' && name[0] <= 'Z') || name[0] == '_') {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func coverageCommandSurface(t *testing.T, dir string) (map[string]struct{}, bool) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	flags := make(map[string]struct{})
	hasMain := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(
			token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, 0,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "main" {
				hasMain = true
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			argument := -1
			switch selector.Sel.Name {
			case "String", "Bool", "Int", "Int64", "Uint", "Uint64", "Duration", "Float64":
				argument = 0
			case "StringVar", "BoolVar", "IntVar", "Int64Var", "UintVar", "Uint64Var", "DurationVar", "Float64Var":
				argument = 1
			}
			if argument < 0 || argument >= len(call.Args) {
				return true
			}
			literal, ok := call.Args[argument].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			name, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("invalid flag literal %s: %v", literal.Value, err)
			}
			flags[name] = struct{}{}
			return true
		})
	}
	return flags, hasMain
}

func coverageTestSymbolExists(t *testing.T, dir, symbol string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(
			token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.SkipObjectResolution,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == symbol {
				return true
			}
		}
	}
	return false
}

func validateCoverageOutputContract(t *testing.T, dir string, target CoverageTarget) {
	t.Helper()
	if len(target.OutputTokens) == 0 {
		if target.OutputFunc != "" {
			t.Fatalf("output function %q has no required tokens", target.OutputFunc)
		}
		return
	}
	if target.OutputFunc == "" {
		t.Fatal("output tokens have no owning function")
	}
	literals, ok := coverageFunctionStringLiterals(t, dir, target.OutputFunc)
	if !ok {
		t.Fatalf("output function %q was not found", target.OutputFunc)
	}
	seen := make(map[string]struct{}, len(target.OutputTokens))
	for _, required := range target.OutputTokens {
		if required == "" {
			t.Fatal("empty required output token")
		}
		if _, duplicate := seen[required]; duplicate {
			t.Fatalf("duplicate required output token %q", required)
		}
		seen[required] = struct{}{}
		found := false
		for _, literal := range literals {
			if strings.Contains(literal, required) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf(
				"output function %s no longer declares token %q",
				target.OutputFunc, required,
			)
		}
	}
}

func coverageFunctionStringLiterals(t *testing.T, dir, function string) ([]string, bool) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var literals []string
	found := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file, err := parser.ParseFile(
			token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.SkipObjectResolution,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != function || fn.Body == nil {
				continue
			}
			found = true
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("invalid string literal %s: %v", literal.Value, err)
				}
				literals = append(literals, value)
				return true
			})
		}
	}
	return literals, found
}
