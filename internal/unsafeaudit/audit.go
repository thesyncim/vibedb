// Package unsafeaudit owns the executable inventory guard for UNSAFE.md.
//
// UNSAFE.md carries a marked block listing every non-test Go file in the root
// module that imports "unsafe". CollectUnsafeFiles recomputes that set from the
// actual import declarations with go/parser, and RenderBlock renders the exact
// bytes the document must contain. The package test byte-compares the rendered
// block against the checked-in file, so the inventory cannot drift from the
// code without failing the build. This mirrors the docs/capabilities.md
// <!-- capability-matrix --> golden block guarded by the conformance package.
package unsafeaudit

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// BlockStart and BlockEnd delimit the machine-generated file list in UNSAFE.md.
// The start marker owns its trailing newline so the rendered body begins on the
// line after it, matching the docs/capabilities.md marker convention.
const (
	BlockStart = "<!-- unsafe-file-list:start -->\n"
	BlockEnd   = "<!-- unsafe-file-list:end -->"
)

// unsafeImportPath is the quoted import spec go/parser reports for the unsafe
// pseudo-package. Comparing against the parsed spec, not raw file text, keeps a
// commented-out or string-literal "unsafe" from counting.
const unsafeImportPath = `"unsafe"`

// CollectUnsafeFiles returns the sorted, slash-separated, root-relative paths of
// every non-test Go file under root that imports "unsafe". Directories with
// their own go.mod (nested modules), plus testdata, vendor, and .git, are
// excluded so the result is exactly the root module's production surface.
//
// Import detection uses go/parser rather than text matching: a file counts only
// when its parsed import declarations name the unsafe pseudo-package. Build
// constraints are ignored, so platform-specific files (io_uring, Darwin
// hole-punch, per-architecture SIMD) are always accounted regardless of the
// host that runs the audit.
func CollectUnsafeFiles(root string) ([]string, error) {
	nested, err := nestedModuleDirs(root)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var files []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "testdata":
				return filepath.SkipDir
			}
			if path != root && nested[path] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		for _, spec := range parsed.Imports {
			if spec.Path.Value == unsafeImportPath {
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				files = append(files, filepath.ToSlash(rel))
				break
			}
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Strings(files)
	return files, nil
}

// nestedModuleDirs returns the set of directories under root, other than root
// itself, that declare their own go.mod. The root module ends at each such
// boundary.
func nestedModuleDirs(root string) (map[string]bool, error) {
	dirs := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Name() == "go.mod" {
			if dir := filepath.Dir(path); dir != root {
				dirs[dir] = true
			}
		}
		return nil
	})
	return dirs, err
}

// RenderBlock returns the exact body that must appear between BlockStart and
// BlockEnd in UNSAFE.md: a stated count followed by a fenced, sorted list. The
// count is derived from the list, so the printed number and the enumeration can
// never disagree. The returned string ends with a newline; the end marker
// follows it on its own line.
func RenderBlock(root string) (string, error) {
	files, err := CollectUnsafeFiles(root)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b,
		"The root module contains %d non-test Go files that import `unsafe`:\n\n",
		len(files),
	)
	b.WriteString("```text\n")
	for _, file := range files {
		b.WriteString(file)
		b.WriteByte('\n')
	}
	b.WriteString("```\n")
	return b.String(), nil
}
