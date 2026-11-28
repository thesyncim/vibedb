package driver

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
)

// ErrCheckViolation reports that an INSERT or UPDATE produced a document that
// violated one of the table's declared CHECK constraints (PostgreSQL 23514).
var ErrCheckViolation = errors.New("vibedb: CHECK constraint violated")

type defaultFieldSlot struct {
	key         string
	defaultSQL  string
	identity    string
	identitySeq string
	value       []byte
	present     bool
	replace     bool
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
	return applyColumnDefaultsAndIdentity(
		document, meta, maxDocumentBytes, sqlast.InsertOverridingNone, nil,
	)
}

func applyColumnDefaultsAndIdentity(
	document []byte,
	meta *tableMeta,
	maxDocumentBytes int,
	overriding sqlast.InsertOverriding,
	seqProvider query.SequenceProvider,
) ([]byte, error) {
	if meta == nil || meta.Schema == nil || len(meta.Schema.Fields) == 0 {
		return document, nil
	}
	var slotsBuf [16]defaultFieldSlot
	slots := slotsBuf[:0]
	for i := range meta.Schema.Fields {
		f := meta.Schema.Fields[i]
		if f.DefaultSQL == "" && f.Identity == "" {
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
				key:         key,
				defaultSQL:  f.DefaultSQL,
				identity:    f.Identity,
				identitySeq: f.IdentitySequence,
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
	replacements := 0
	for i := range slots {
		if slots[i].present {
			switch slots[i].identity {
			case "always":
				switch overriding {
				case sqlast.InsertOverridingNone:
					return nil, fmt.Errorf(
						"%w: column %q is an identity column defined as GENERATED ALWAYS; use OVERRIDING SYSTEM VALUE to override",
						ErrIdentityAlwaysViolation, slots[i].key,
					)
				case sqlast.InsertOverridingUserValue:
					slots[i].replace = true
					replacements++
				case sqlast.InsertOverridingSystemValue:
					// User-supplied value is explicitly allowed.
				}
			case "by_default":
				if overriding == sqlast.InsertOverridingUserValue {
					slots[i].replace = true
					replacements++
				}
			}
		} else {
			missing++
		}
	}
	if missing == 0 && replacements == 0 {
		return document, nil
	}
	for i := range slots {
		if slots[i].present && !slots[i].replace {
			continue
		}
		if slots[i].identity != "" && slots[i].identitySeq != "" {
			if seqProvider == nil {
				return nil, fmt.Errorf("vibedb: evaluate IDENTITY for %q: sequence provider unavailable", slots[i].key)
			}
			v, seqErr := seqProvider.NextVal(slots[i].identitySeq)
			if seqErr != nil {
				return nil, fmt.Errorf("vibedb: evaluate IDENTITY for %q: %w", slots[i].key, seqErr)
			}
			slots[i].value = strconv.AppendInt(nil, v, 10)
			continue
		}
		if slots[i].identitySeq != "" && seqProvider != nil &&
			slots[i].defaultSQL == "nextval('"+escapeSQLSingleQuote(slots[i].identitySeq)+"')" {
			v, seqErr := seqProvider.NextVal(slots[i].identitySeq)
			if seqErr != nil {
				return nil, fmt.Errorf("vibedb: evaluate DEFAULT for %q: %w", slots[i].key, seqErr)
			}
			slots[i].value = strconv.AppendInt(nil, v, 10)
			continue
		}
		val, evalErr := evalDefaultExpressionJSON(slots[i].defaultSQL, maxDocumentBytes, seqProvider)
		if evalErr != nil {
			return nil, fmt.Errorf("vibedb: evaluate DEFAULT for %q: %w", slots[i].key, evalErr)
		}
		slots[i].value = val
	}

	if replacements > 0 && isObj {
		return rewriteJSONObjectWithSlots(root, slots, maxDocumentBytes)
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

func rewriteJSONObjectWithSlots(
	root vibejson.Node,
	slots []defaultFieldSlot,
	maxDocumentBytes int,
) ([]byte, error) {
	iter, _ := root.ObjectIter()
	out := make([]byte, 0, 128)
	out = append(out, '{')
	wrote := 0
	var keyScratch [64]byte
	keyText := keyScratch[:0]
	for {
		key, val, hasNext := iter.NextRaw()
		if !hasNext {
			break
		}
		var keyStr string
		if raw, clean := key.StringBytes(); clean {
			keyStr = string(raw)
		} else {
			var textErr error
			keyText, _, textErr = key.AppendText(keyText[:0])
			if textErr != nil {
				return nil, textErr
			}
			keyStr = string(keyText)
		}
		if wrote > 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, keyStr)
		out = append(out, ':')
		replaced := false
		for i := range slots {
			if slots[i].replace && slots[i].key == keyStr {
				out = append(out, slots[i].value...)
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, val.Bytes()...)
		}
		wrote++
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
	if maxDocumentBytes > 0 && len(out) > maxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}
	return out, nil
}

func evalDefaultExpressionJSON(defaultSQL string, maxDocumentBytes int, seqProvider query.SequenceProvider) ([]byte, error) {
	stmt, err := query.PrepareStatement("SELECT " + defaultSQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Release()
	var exec query.Exec
	exec.Options.Sequences = seqProvider
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
