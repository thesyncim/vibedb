package driver

import (
	"errors"
	"fmt"
	"strings"

	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

// ErrCheckViolation reports that an INSERT or UPDATE produced a document that
// violated one of the table's declared CHECK constraints (PostgreSQL 23514).
var ErrCheckViolation = errors.New("vibedb: CHECK constraint violated")

type defaultFieldSlot struct {
	key        string
	defaultSQL string
	value      []byte
	present    bool
}

func isJSONWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// applyColumnDefaults injects declared DEFAULT column values into a JSON object
// document for any top-level column missing from document. When the table has
// no declared defaults or all defaulted columns are already present, this is a
// zero-allocation pass.
func applyColumnDefaults(
	document []byte,
	meta *tableMeta,
	maxDocumentBytes int,
) ([]byte, error) {
	if meta == nil || meta.Schema == nil || len(meta.Schema.Fields) == 0 {
		return document, nil
	}
	var slotsBuf [16]defaultFieldSlot
	slots := slotsBuf[:0]
	for i := range meta.Schema.Fields {
		f := meta.Schema.Fields[i]
		if f.DefaultSQL == "" {
			continue
		}
		key := strings.TrimPrefix(f.Path, "/")
		if key == "" || strings.Contains(key, "/") {
			continue
		}
		dup := false
		for j := range slots {
			if slots[j].key == key {
				dup = true
				break
			}
		}
		if !dup {
			slots = append(slots, defaultFieldSlot{
				key:        key,
				defaultSQL: f.DefaultSQL,
			})
		}
	}
	if len(slots) == 0 {
		return document, nil
	}
	idx, poolBuf, err := buildPooledJSONIndex(document)
	if err != nil {
		return document, nil
	}
	defer releasePooledJSONIndex(poolBuf)
	root := idx.Root()
	iter, isObj := root.ObjectIter()
	if !isObj && root.Kind() != jsondoc.Null {
		return document, nil
	}
	existingMembers := 0
	if isObj {
		var keyScratch [64]byte
		keyText := keyScratch[:0]
		for {
			key, _, hasNext := iter.NextRaw()
			if !hasNext {
				break
			}
			existingMembers++
			var keyBytes []byte
			if raw, clean := key.StringBytes(); clean {
				keyBytes = raw
			} else {
				var textErr error
				keyText, _, textErr = key.AppendText(keyText[:0])
				if textErr != nil {
					return document, nil
				}
				keyBytes = keyText
			}
			for j := range slots {
				if !slots[j].present && string(keyBytes) == slots[j].key {
					slots[j].present = true
					break
				}
			}
		}
	}
	missing := 0
	for i := range slots {
		if !slots[i].present {
			missing++
		}
	}
	if missing == 0 {
		return document, nil
	}
	for i := range slots {
		if slots[i].present {
			continue
		}
		val, evalErr := evalDefaultExpressionJSON(slots[i].defaultSQL, maxDocumentBytes)
		if evalErr != nil {
			return nil, fmt.Errorf("vibedb: evaluate DEFAULT for %q: %w", slots[i].key, evalErr)
		}
		slots[i].value = val
	}

	var prefix []byte
	totalBytes := 0
	if existingMembers == 0 {
		totalBytes = 2 // '{' and '}'
		if missing > 1 {
			totalBytes += missing - 1
		}
	} else {
		openIdx := 0
		for openIdx < len(document) && isJSONWhitespace(document[openIdx]) {
			openIdx++
		}
		closeIdx := len(document) - 1
		for closeIdx > openIdx && isJSONWhitespace(document[closeIdx]) {
			closeIdx--
		}
		prefix = document[openIdx:closeIdx]
		totalBytes = len(prefix) + 1 + missing // prefix + '}' + one comma per injected field
	}
	for i := range slots {
		if slots[i].present {
			continue
		}
		add := jsonStringEncodedLen(byteview.Bytes(slots[i].key)) + 1 + len(slots[i].value)
		if maxDocumentBytes > 0 && (totalBytes > maxDocumentBytes || add > maxDocumentBytes-totalBytes) {
			return nil, durable.ErrDocumentTooLarge
		}
		totalBytes += add
	}
	if maxDocumentBytes > 0 && totalBytes > maxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}

	out := make([]byte, 0, totalBytes)
	wrote := existingMembers
	if existingMembers == 0 {
		out = append(out, '{')
	} else {
		out = append(out, prefix...)
	}
	for i := range slots {
		if slots[i].present {
			continue
		}
		if wrote > 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, slots[i].key)
		out = append(out, ':')
		out = append(out, slots[i].value...)
		wrote++
	}
	out = append(out, '}')
	return out, nil
}

func evalDefaultExpressionJSON(defaultSQL string, maxDocumentBytes int) ([]byte, error) {
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
		return jsonNullBytes, nil
	}
	cell := cur.Cell(0)
	if cell.IsNull() {
		return jsonNullBytes, nil
	}
	raw := cell.JSON()
	if cell.Kind() == query.TypeNumber {
		if integer, ok := canonicalComputedInteger(raw, maxDocumentBytes); ok {
			raw = integer
		}
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return cp, nil
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
