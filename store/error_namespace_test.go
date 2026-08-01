package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestPublicErrorsUseVibeDBNamespace(t *testing.T) {
	t.Parallel()
	errors := map[string]error{
		"ErrBuilderClosed":      ErrBuilderClosed,
		"ErrCheckpointTooLarge": ErrCheckpointTooLarge,
		"ErrCollectionExists":   ErrCollectionExists,
		"ErrCollectionName":     ErrCollectionName,
		"ErrDuplicateKey":       ErrDuplicateKey,
		"ErrIndexArity":         ErrIndexArity,
		"ErrIndexDefinition":    ErrIndexDefinition,
		"ErrIndexExists":        ErrIndexExists,
		"ErrIndexNotFound":      ErrIndexNotFound,
		"ErrIndexScalar":        ErrIndexScalar,
		"ErrMaskChunk":          ErrMaskChunk,
		"ErrMaskOrder":          ErrMaskOrder,
		"ErrSchemaDefinition":   ErrSchemaDefinition,
		"ErrSchemaViolation":    ErrSchemaViolation,
		"ErrTooLarge":           ErrTooLarge,
	}
	for name, err := range errors {
		if err == nil || !strings.HasPrefix(err.Error(), "vibedb: ") {
			t.Errorf("%s = %q, want vibedb namespace", name, err)
		}
	}
}

// TestOwnedSourceHasNoLegacyErrorNamespace makes the product identity rule
// mechanical for every non-test Go string owned by the heap store, durable
// store, and their private I/O layer. Errors returned by the vibejson parser
// keep their dependency namespace; only locally-authored strings are covered.
func TestOwnedSourceHasNoLegacyErrorNamespace(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate the store package")
	}
	storeDir := filepath.Dir(thisFile)
	directories := []string{
		storeDir,
		filepath.Join(storeDir, "durable"),
		filepath.Join(storeDir, "..", "internal", "storeio"),
	}
	for _, directory := range directories {
		err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") ||
				strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && strings.HasPrefix(value, "vibejson:") {
					t.Errorf("%s contains legacy store-owned error %q", path, value)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", directory, err)
		}
	}
}
