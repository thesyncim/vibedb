package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
)

// ApplyColumnAssignments materializes a declared-column UPDATE over one
// canonical document. It is used under the local writer lock and by the RF3
// coordinator before emitting a digest-guarded replacement. Existing values
// remain raw JSON bytes; only assigned scalars are encoded.
func ApplyColumnAssignments(document []byte, assignments []sqlast.UpdateAssignment, args []any, maxBytes int) ([]byte, error) {
	if len(assignments) == 0 {
		return nil, errors.New("vibedb: column UPDATE has no assignments")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	fields := make(map[string]json.RawMessage, len(assignments)+8)
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("vibedb: column UPDATE requires an object document: %w", err)
	}
	if fields == nil {
		return nil, errors.New("vibedb: column UPDATE requires an object document")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("vibedb: invalid trailing JSON in document")
	}
	for i := range assignments {
		encoded, err := encodeAssignmentScalar(assignments[i].Value, args)
		if err != nil {
			return nil, err
		}
		fields[assignments[i].Column] = encoded
	}
	updated, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("vibedb: encode column UPDATE: %w", err)
	}
	if len(updated) > maxBytes {
		return nil, durable.ErrDocumentTooLarge
	}
	return updated, nil
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
	case []byte:
		if !utf8.Valid(v) {
			return nil, errors.New("vibedb: column string must be valid UTF-8")
		}
		encoded, err := json.Marshal(string(v))
		return encoded, err
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
