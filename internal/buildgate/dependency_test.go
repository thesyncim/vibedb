package buildgate

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestServingHotPathsDoNotImportStandardJSON makes the byte-native encoding
// contract a build failure instead of a convention. Transitive dependencies
// remain outside this gate: it protects VibeDB's request-ledger and gateway
// production sources from directly admitting encoding/json onto serving paths.
func TestServingHotPathsDoNotImportStandardJSON(t *testing.T) {
	repository := repositoryRoot(t)
	hotPaths := []string{
		"internal/requestledger",
		"gateway",
		"cmd/vibedb-gateway",
	}

	for _, hotPath := range hotPaths {
		hotPath := hotPath
		t.Run(strings.ReplaceAll(hotPath, "/", "_"), func(t *testing.T) {
			root := filepath.Join(repository, filepath.FromSlash(hotPath))
			err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
					strings.HasSuffix(entry.Name(), "_test.go") {
					return nil
				}

				file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
				if err != nil {
					return err
				}
				for _, imported := range file.Imports {
					name, err := strconv.Unquote(imported.Path.Value)
					if err != nil {
						return err
					}
					if name == "encoding/json" {
						relative, relErr := filepath.Rel(repository, path)
						if relErr != nil {
							relative = path
						}
						t.Errorf("%s directly imports encoding/json; keep serving codecs byte-native", relative)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root not found")
		}
		directory = parent
	}
}
