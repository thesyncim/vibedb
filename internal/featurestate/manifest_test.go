package featurestate

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repositoryRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("featurestate: cannot locate manifest test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestDistributedManifestEvidence(t *testing.T) {
	root := repositoryRoot(t)
	seen := make(map[string]struct{}, len(Distributed))
	for row, feature := range Distributed {
		if strings.TrimSpace(feature.Name) == "" {
			t.Fatalf("row %d has no feature name", row)
		}
		if _, duplicate := seen[feature.Name]; duplicate {
			t.Fatalf("duplicate feature %q", feature.Name)
		}
		seen[feature.Name] = struct{}{}
		stages := []struct {
			name  string
			stage Stage
		}{
			{"primitive", feature.Primitive}, {"integrated", feature.Integrated},
			{"shipped", feature.Shipped}, {"qualification", feature.Qualification},
		}
		for _, item := range stages {
			if item.stage.Status > StatusYes || strings.TrimSpace(item.stage.Contract) == "" {
				t.Fatalf("%s %s has invalid state", feature.Name, item.name)
			}
			if item.stage.Status != StatusNo && len(item.stage.Evidence) == 0 {
				t.Fatalf("%s %s has no evidence", feature.Name, item.name)
			}
			for _, evidence := range item.stage.Evidence {
				if evidence.Path == "" || evidence.Symbol == "" || filepath.IsAbs(evidence.Path) ||
					strings.Contains(evidence.Path, "..") {
					t.Fatalf("%s %s has invalid evidence %+v", feature.Name, item.name, evidence)
				}
				contents, err := os.ReadFile(filepath.Join(root, evidence.Path))
				if err != nil {
					t.Fatalf("%s %s evidence %q: %v", feature.Name, item.name, evidence.Path, err)
				}
				declared, err := declaresGoSymbol(evidence.Path, contents, evidence.Symbol)
				if err != nil {
					t.Fatalf("%s %s evidence %q cannot be parsed: %v",
						feature.Name, item.name, evidence.Path, err)
				}
				if !declared {
					t.Fatalf("%s %s evidence %q does not declare symbol %q",
						feature.Name, item.name, evidence.Path, evidence.Symbol)
				}
			}
		}
		if feature.Integrated.Status > feature.Primitive.Status ||
			feature.Shipped.Status > feature.Integrated.Status {
			t.Fatalf("%s advances beyond its prerequisite: primitive=%s integrated=%s shipped=%s",
				feature.Name, feature.Primitive.Status.label(), feature.Integrated.Status.label(),
				feature.Shipped.Status.label())
		}
	}
}

// declaresGoSymbol accepts only an exact top-level Go declaration. In
// particular, a comment, selector, call site, or test invocation cannot make
// an evidence reference pass. Test and benchmark entry points are ordinary
// function declarations and are recognized by the same rule.
func declaresGoSymbol(path string, source []byte, want string) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return false, err
	}
	for _, declaration := range file.Decls {
		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			if declaration.Name.Name == want {
				return true, nil
			}
		case *ast.GenDecl:
			for _, specification := range declaration.Specs {
				switch specification := specification.(type) {
				case *ast.TypeSpec:
					if specification.Name.Name == want {
						return true, nil
					}
				case *ast.ValueSpec:
					for _, name := range specification.Names {
						if name.Name == want {
							return true, nil
						}
					}
				}
			}
		}
	}
	return false, nil
}

func TestDeclaresGoSymbolRejectsTextualUseSites(t *testing.T) {
	source := []byte(`package evidence

// CommentOnly is not evidence.
type DeclaredType struct{}
func (DeclaredType) DeclaredMethod() {}
func TestDeclared(t any) {}
func BenchmarkDeclared(b any) {}
func use() { _ = CommentOnly; DeclaredMethod() }
`)
	for _, symbol := range []string{"DeclaredType", "DeclaredMethod", "TestDeclared", "BenchmarkDeclared"} {
		declared, err := declaresGoSymbol("evidence.go", source, symbol)
		if err != nil || !declared {
			t.Fatalf("declaration %q: declared=%v err=%v", symbol, declared, err)
		}
	}
	for _, symbol := range []string{"CommentOnly", "use-site"} {
		declared, err := declaresGoSymbol("evidence.go", source, symbol)
		if err != nil || declared {
			t.Fatalf("textual use %q: declared=%v err=%v", symbol, declared, err)
		}
	}
}

func TestDistributedFeatureStateGenerated(t *testing.T) {
	path := filepath.Join(repositoryRoot(t), "docs", "distributed-feature-state.md")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := RenderMarkdown()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s is stale; run go generate ./internal/featurestate", path)
	}
}
