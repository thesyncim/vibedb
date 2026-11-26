package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

// ErrCheckViolation reports that an INSERT or UPDATE produced a document that
// violated one of the table's declared CHECK constraints (PostgreSQL 23514).
var ErrCheckViolation = errors.New("vibedb: CHECK constraint violated")

// applyColumnDefaults injects declared DEFAULT column values into a JSON object
// document for any top-level column missing from document. When the table has
// no declared defaults, this is a fast scalar no-op.
func applyColumnDefaults(
	document []byte,
	meta *tableMeta,
	maxDocumentBytes int,
) ([]byte, error) {
	if meta == nil || meta.Schema == nil || len(meta.Schema.Fields) == 0 {
		return document, nil
	}
	hasDefault := false
	for i := range meta.Schema.Fields {
		if meta.Schema.Fields[i].DefaultSQL != "" {
			hasDefault = true
			break
		}
	}
	if !hasDefault {
		return document, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(document, &obj); err != nil {
		return document, nil
	}
	if obj == nil {
		obj = make(map[string]json.RawMessage)
	}
	changed := false
	for i := range meta.Schema.Fields {
		f := meta.Schema.Fields[i]
		if f.DefaultSQL == "" {
			continue
		}
		key := strings.TrimPrefix(f.Path, "/")
		if key == "" || strings.Contains(key, "/") {
			continue
		}
		if _, exists := obj[key]; exists {
			continue
		}
		val, err := evalDefaultExpressionJSON(f.DefaultSQL, maxDocumentBytes)
		if err != nil {
			return nil, fmt.Errorf("vibedb: evaluate DEFAULT for %q: %w", key, err)
		}
		obj[key] = val
		changed = true
	}
	if !changed {
		return document, nil
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	if maxDocumentBytes > 0 && len(out) > maxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}
	return out, nil
}

func evalDefaultExpressionJSON(defaultSQL string, maxDocumentBytes int) (json.RawMessage, error) {
	stmt, err := query.PrepareStatement("SELECT " + defaultSQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Release()
	var exec query.Exec
	defer exec.Release()
	cur, err := stmt.RunInto(&exec, query.Source{}, nil)
	if err != nil {
		return nil, err
	}
	if !cur.Next() {
		return json.RawMessage("null"), nil
	}
	cell := cur.Cell(0)
	if cell.IsNull() {
		return json.RawMessage("null"), nil
	}
	raw := cell.JSON()
	if cell.Kind() == query.TypeNumber {
		if integer, ok := canonicalComputedInteger(raw, maxDocumentBytes); ok {
			raw = integer
		}
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return json.RawMessage(cp), nil
}

// validateCheckConstraints evaluates every declared CHECK constraint against
// document using the native compiled predicate engine. Per SQL 3-valued logic,
// TRUE and NULL pass; only FALSE violates the CHECK constraint.
func validateCheckConstraints(meta *tableMeta, document []byte) error {
	if meta == nil || len(meta.Checks) == 0 {
		return nil
	}
	var rawSrc query.ValidatedRawSource
	rawSrc.Bind(document)
	src := query.FromValidatedRaw(&rawSrc)
	var exec query.Exec
	defer exec.Release()
	for _, checkSQL := range meta.Checks {
		if strings.TrimSpace(checkSQL) == "" {
			continue
		}
		stmt, err := query.PrepareStatement("SELECT 1 FROM _check WHERE NOT (" + checkSQL + ")")
		if err != nil {
			return fmt.Errorf("vibedb: compile CHECK (%s): %w", checkSQL, err)
		}
		cur, runErr := stmt.RunInto(&exec, src, nil)
		violated := runErr == nil && cur.Next()
		stmt.Release()
		if runErr != nil {
			return fmt.Errorf("vibedb: evaluate CHECK (%s): %w", checkSQL, runErr)
		}
		if violated {
			return fmt.Errorf("%w: (%s)", ErrCheckViolation, checkSQL)
		}
	}
	return nil
}
