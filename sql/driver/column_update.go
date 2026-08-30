package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

// ApplyColumnAssignments materializes a declared-column UPDATE over one
// canonical document. It is used under the local writer lock and by the RF3
// coordinator before emitting a digest-guarded replacement. Existing values
// remain raw JSON bytes; only assigned scalars are encoded.
func ApplyColumnAssignments(document []byte, assignments []sqlast.UpdateAssignment, args []any, maxBytes int) ([]byte, error) {
	return applyColumnAssignments(document, nil, assignments, args, maxBytes)
}

// ApplyColumnAssignmentsWithExcluded materializes the conflict branch of an
// INSERT ... ON CONFLICT DO UPDATE. Ordinary operands are bound exactly as they
// are for UPDATE; OperandExcluded copies the effective top-level value from
// the candidate INSERT document. Missing candidate fields become JSON null,
// matching this dialect's existing missing/NULL scalar model.
func ApplyColumnAssignmentsWithExcluded(
	document []byte,
	excluded []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	maxBytes int,
) ([]byte, error) {
	return applyColumnAssignments(document, excluded, assignments, args, maxBytes)
}

func applyColumnAssignments(
	document []byte,
	excluded []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	maxBytes int,
) ([]byte, error) {
	if len(assignments) == 0 {
		return nil, errors.New("vibedb: column UPDATE has no assignments")
	}
	type materializedAssignment struct {
		key        []byte
		value      []byte
		lastMember int
	}
	materialized := make([]materializedAssignment, len(assignments))
	byColumn := make(map[string]int, len(assignments))
	hasExcluded := false
	for i := range assignments {
		var value []byte
		if assignments[i].Value.Kind == sqlast.OperandExcluded {
			if excluded == nil {
				return nil, errors.New("vibedb: EXCLUDED is only available to ON CONFLICT DO UPDATE")
			}
			value = []byte("null")
			hasExcluded = true
		} else {
			var err error
			value, err = encodeAssignmentScalar(assignments[i].Value, args)
			if err != nil {
				return nil, err
			}
		}
		key, err := json.Marshal(assignments[i].Column)
		if err != nil {
			return nil, fmt.Errorf("vibedb: encode column name: %w", err)
		}
		materialized[i] = materializedAssignment{
			key: key, value: value,
			lastMember: -1,
		}
		byColumn[assignments[i].Column] = i
	}
	if hasExcluded {
		candidate, err := vibejson.ParseOptions(excluded, vibejson.Options{ZeroCopy: true})
		if err != nil {
			return nil, fmt.Errorf("vibedb: EXCLUDED requires an object document: %w", err)
		}
		candidateIter, ok := candidate.Node().ObjectIter()
		if !ok {
			return nil, errors.New("vibedb: EXCLUDED requires an object document")
		}
		keyText := make([]byte, 0, 64)
		for {
			key, value, ok := candidateIter.NextRaw()
			if !ok {
				break
			}
			var column string
			if raw, clean := key.StringBytes(); clean {
				column = string(raw)
			} else {
				var textErr error
				keyText, _, textErr = key.AppendText(keyText[:0])
				if textErr != nil {
					return nil, fmt.Errorf(
						"vibedb: decode EXCLUDED column name: %w", textErr,
					)
				}
				column = string(keyText)
			}
			for i := range assignments {
				if assignments[i].Value.Kind == sqlast.OperandExcluded &&
					assignments[i].Value.Text == column {
					// Candidate JSON follows the engine's established
					// last-occurrence-wins lookup rule. The borrowed bytes
					// remain live until this materialization returns.
					materialized[i].value = value.Bytes()
				}
			}
		}
		for i := range assignments {
			if assignments[i].Value.Kind != sqlast.OperandExcluded {
				continue
			}
			kind := (vibejson.RawValue{Src: materialized[i].value}).Kind()
			if kind == jsondoc.Object || kind == jsondoc.Array {
				return nil, fmt.Errorf(
					"vibedb: EXCLUDED column %q is not a scalar",
					assignments[i].Value.Text,
				)
			}
		}
	}

	parsed, err := vibejson.ParseOptions(document, vibejson.Options{ZeroCopy: true})
	if err != nil {
		return nil, fmt.Errorf("vibedb: column UPDATE requires an object document: %w", err)
	}
	root := parsed.Node()
	iter, ok := root.ObjectIter()
	if !ok {
		return nil, errors.New("vibedb: column UPDATE requires an object document")
	}
	memberCount, _ := root.ObjectLen()
	keyText := make([]byte, 0, 64)
	for member := 0; ; member++ {
		key, _, ok := iter.Next()
		if !ok {
			break
		}
		var column string
		if raw, clean := key.StringBytes(); clean {
			column = string(raw)
		} else {
			keyText, _ = key.AppendText(keyText[:0])
			column = string(keyText)
		}
		if assignment, assigned := byColumn[column]; assigned {
			// JSON lookup semantics are last-occurrence-wins. Replacing only the
			// effective member preserves every shadowed duplicate byte-for-byte.
			materialized[assignment].lastMember = member
		}
	}

	replacementAt := make(map[int]int, len(materialized))
	missing := 0
	for i := range materialized {
		if materialized[i].lastMember < 0 {
			missing++
			continue
		}
		replacementAt[materialized[i].lastMember] = i
	}

	// Compute the exact compact output size before allocating it. This both
	// enforces the document limit and prevents a large assignment from causing
	// an over-limit transient allocation.
	updatedBytes := 2 // opening and closing braces
	outputMembers := memberCount + missing
	if outputMembers > 1 {
		updatedBytes += outputMembers - 1
	}
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, ok := iter.NextRaw()
		if !ok {
			break
		}
		valueBytes := value.Bytes()
		if assignment, replace := replacementAt[member]; replace {
			valueBytes = materialized[assignment].value
		}
		add := len(key.Bytes()) + 1 + len(valueBytes)
		if updatedBytes > maxBytes || add > maxBytes-updatedBytes {
			return nil, durable.ErrDocumentTooLarge
		}
		updatedBytes += add
	}
	for i := range materialized {
		if materialized[i].lastMember >= 0 {
			continue
		}
		add := len(materialized[i].key) + 1 + len(materialized[i].value)
		if updatedBytes > maxBytes || add > maxBytes-updatedBytes {
			return nil, durable.ErrDocumentTooLarge
		}
		updatedBytes += add
	}
	if updatedBytes > maxBytes {
		return nil, durable.ErrDocumentTooLarge
	}

	updated := make([]byte, 0, updatedBytes)
	updated = append(updated, '{')
	wrote := 0
	iter, _ = root.ObjectIter()
	for member := 0; ; member++ {
		key, value, ok := iter.NextRaw()
		if !ok {
			break
		}
		if wrote != 0 {
			updated = append(updated, ',')
		}
		updated = key.AppendJSON(updated)
		updated = append(updated, ':')
		if assignment, replace := replacementAt[member]; replace {
			updated = append(updated, materialized[assignment].value...)
		} else {
			updated = value.AppendJSON(updated)
		}
		wrote++
	}
	for i := range materialized {
		if materialized[i].lastMember >= 0 {
			continue
		}
		if wrote != 0 {
			updated = append(updated, ',')
		}
		updated = append(updated, materialized[i].key...)
		updated = append(updated, ':')
		updated = append(updated, materialized[i].value...)
		wrote++
	}
	updated = append(updated, '}')
	return updated, nil
}

// validateUpsertColumnAssignments resolves both sides of the deliberately flat
// conflict SET list against the current table incarnation. EXCLUDED is a row
// namespace, not a way to reach arbitrary undeclared JSON members through SQL.
func validateUpsertColumnAssignments(
	table string,
	meta *tableMeta,
	assignments []sqlast.UpdateAssignment,
) error {
	if err := validateDeclaredColumnAssignments(table, meta, assignments); err != nil {
		return err
	}
	for i := range assignments {
		value := assignments[i].Value
		if value.Kind != sqlast.OperandExcluded ||
			declaredTopLevelColumn(meta, value.Text) {
			continue
		}
		return &query.RelationColumnError{
			Relation: table,
			Column:   value.Text,
			Pos:      value.Pos,
		}
	}
	return nil
}

// validateDeclaredColumnAssignments resolves UPDATE's deliberately flat SET
// targets against the current catalog incarnation. Keeping this check beside
// materialization gives prepare, autocommit, transaction, and capture paths
// one definition of a declared column. In particular, a stale prepared plan
// cannot start writing a newly recreated table with a different schema.
func validateDeclaredColumnAssignments(
	table string,
	meta *tableMeta,
	assignments []sqlast.UpdateAssignment,
) error {
	if len(assignments) == 0 {
		return nil
	}
	for i := range assignments {
		if !declaredTopLevelColumn(meta, assignments[i].Column) {
			return &query.RelationColumnError{
				Relation: table,
				Column:   assignments[i].Column,
				Pos:      assignments[i].Pos,
			}
		}
	}
	return nil
}

func declaredTopLevelColumn(meta *tableMeta, column string) bool {
	pointer := appendUpdateColumnPointer(nil, column)
	if meta == nil || meta.Schema == nil {
		return false
	}
	for i := range meta.Schema.Fields {
		if meta.Schema.Fields[i].Path == string(pointer) {
			return true
		}
	}
	return false
}

// validateColumnAssignmentBindings makes parameter failures independent of
// row cardinality. UPDATE must reject an unsupported or malformed SET value
// even when its predicate selects no rows, just as whole-document UPDATE does.
func validateColumnAssignmentBindings(
	assignments []sqlast.UpdateAssignment,
	args []any,
) error {
	for i := range assignments {
		if assignments[i].Value.Kind == sqlast.OperandExcluded {
			continue
		}
		if _, err := encodeAssignmentScalar(assignments[i].Value, args); err != nil {
			return err
		}
	}
	return nil
}

func appendUpdateColumnPointer(dst []byte, column string) []byte {
	dst = append(dst, '/')
	for i := 0; i < len(column); i++ {
		switch column[i] {
		case '~':
			dst = append(dst, '~', '0')
		case '/':
			dst = append(dst, '~', '1')
		default:
			dst = append(dst, column[i])
		}
	}
	return dst
}

func encodeAssignmentScalar(operand sqlast.Operand, args []any) (json.RawMessage, error) {
	value, err := flatInsertOperandValue(operand, args)
	if err != nil {
		return nil, err
	}
	for {
		switch v := value.(type) {
		case *bool:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *int64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *float64:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *string:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		case *query.Number:
			if v == nil {
				value = nil
			} else {
				value = *v
			}
		default:
			goto encode
		}
	}
encode:
	switch v := value.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case query.Number:
		if !validJSONNumber(string(v)) {
			return nil, errors.New("vibedb: invalid numeric parameter")
		}
		return json.RawMessage(v), nil
	case vibejson.RawValue:
		number, ok := v.NumberBytes()
		if !ok || !validJSONNumber(string(number)) {
			return nil, errors.New("vibedb: invalid numeric parameter")
		}
		return json.RawMessage(number), nil
	case []byte:
		if !utf8.Valid(v) {
			return nil, errors.New("vibedb: column string must be valid UTF-8")
		}
		encoded, err := json.Marshal(string(v))
		return encoded, err
	case string:
		if !utf8.ValidString(v) {
			return nil, errors.New("vibedb: column string must be valid UTF-8")
		}
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("vibedb: numeric parameters must be finite")
		}
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("vibedb: numeric parameters must be finite")
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("vibedb: encode column value: %w", err)
	}
	return encoded, nil
}

func validJSONNumber(value string) bool {
	if value == "" || !json.Valid([]byte(value)) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.UseNumber()
	var number json.Number
	return decoder.Decode(&number) == nil
}
