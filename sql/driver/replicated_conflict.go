package driver

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

const replicatedConflictAssignmentLimit = 1024

var errReplicatedConflictProgram = errors.New("vibedb: invalid replicated conflict assignment program")

// DirectReplicatedConflictAssignments identifies the closed first version of
// the native conflict program: bound scalar constants and EXCLUDED columns.
// No SQL source, parameter references, or current-row preimage is replicated.
func DirectReplicatedConflictAssignments(action *sqlast.InsertConflictUpdate) bool {
	if action == nil || len(action.Assignments) == 0 || len(action.Assignments) > replicatedConflictAssignmentLimit {
		return false
	}
	for _, assignment := range action.Assignments {
		if assignment.Expr != nil {
			return false
		}
		switch assignment.Value.Kind {
		case sqlast.OperandNull, sqlast.OperandString, sqlast.OperandNumber, sqlast.OperandBool, sqlast.OperandParam, sqlast.OperandExcluded:
		default:
			return false
		}
	}
	return true
}

// ValidateReplicatedConflictAssignments shares local SQL name resolution with
// a coordinator's authenticated declaration, before it creates any mutation.
func ValidateReplicatedConflictAssignments(info TableInfo, action *sqlast.InsertConflictUpdate) error {
	if !DirectReplicatedConflictAssignments(action) {
		return errReplicatedConflictProgram
	}
	meta := &tableMeta{PrimaryKey: info.PrimaryKey, Schema: &schemaMeta{}}
	for _, column := range info.Columns {
		meta.Schema.Fields = append(meta.Schema.Fields, schemaFieldMeta{Path: column.Path, Types: uint16(column.Types), Required: column.Required})
	}
	return validateUpsertColumnAssignments(info.Name, info.Name, meta, action.Assignments)
}

// EncodeReplicatedConflictValue retains each authored direct assignment once.
// Constants are bound and validated now. EXCLUDED values are resolved only in
// the conflict branch, so an insert need not evaluate an unused assignment.
func EncodeReplicatedConflictValue(candidate []byte, action *sqlast.InsertConflictUpdate, args []any) ([]byte, error) {
	if !DirectReplicatedConflictAssignments(action) {
		return nil, errReplicatedConflictProgram
	}
	program := binary.LittleEndian.AppendUint16(nil, uint16(len(action.Assignments)))
	seen := make(map[string]bool, len(action.Assignments))
	for _, assignment := range action.Assignments {
		column := assignment.Column
		if column == "" || len(column) > 65535 || !utf8.ValidString(column) || seen[column] {
			return nil, errReplicatedConflictProgram
		}
		seen[column] = true
		kind := byte(0)
		var value []byte
		if assignment.Value.Kind == sqlast.OperandExcluded {
			kind = 1
			value = []byte(assignment.Value.Text)
			if len(value) == 0 || len(value) > 65535 || !utf8.Valid(value) {
				return nil, errReplicatedConflictProgram
			}
		} else {
			var err error
			value, err = encodeAssignmentScalar(assignment.Value, args)
			if err != nil {
				return nil, err
			}
			value, err = vibejson.AppendCanonicalize(nil, value)
			if err != nil {
				return nil, err
			}
			if _, err = decodeConflictConstant(value); err != nil {
				return nil, err
			}
		}
		if len(program)+7+len(column)+len(value) > replication.MaxMutationValueBytes {
			return nil, errReplicatedConflictProgram
		}
		program = append(program, kind)
		program = binary.LittleEndian.AppendUint16(program, uint16(len(column)))
		program = binary.LittleEndian.AppendUint32(program, uint32(len(value)))
		program = append(program, column...)
		program = append(program, value...)
	}
	return replication.AppendConflictValue(nil, candidate, program)
}

func decodeConflictConstant(value []byte) (sqlast.Operand, error) {
	if err := vibejson.Validate(value); err != nil {
		return sqlast.Operand{}, errReplicatedConflictProgram
	}
	raw := vibejson.RawValue{Src: value}
	switch raw.Kind() {
	case jsondoc.Null:
		return sqlast.Operand{Kind: sqlast.OperandNull}, nil
	case jsondoc.Bool:
		b, ok := raw.Bool()
		if !ok {
			return sqlast.Operand{}, errReplicatedConflictProgram
		}
		return sqlast.Operand{Kind: sqlast.OperandBool, Bool: b}, nil
	case jsondoc.Number:
		text, ok := raw.NumberText()
		if !ok {
			return sqlast.Operand{}, errReplicatedConflictProgram
		}
		return sqlast.Operand{Kind: sqlast.OperandNumber, Text: text}, nil
	case jsondoc.String:
		text, ok, err := raw.Text()
		if err != nil || !ok {
			return sqlast.Operand{}, errReplicatedConflictProgram
		}
		return sqlast.Operand{Kind: sqlast.OperandString, Text: text}, nil
	default:
		return sqlast.Operand{}, errReplicatedConflictProgram
	}
}

func decodeReplicatedConflictAssignments(program []byte, schema *store.Schema) ([]sqlast.UpdateAssignment, error) {
	if len(program) < 2 || len(program) > replication.MaxMutationValueBytes || schema == nil {
		return nil, errReplicatedConflictProgram
	}
	count := int(binary.LittleEndian.Uint16(program[:2]))
	program = program[2:]
	if count == 0 || count > replicatedConflictAssignmentLimit {
		return nil, errReplicatedConflictProgram
	}
	assignments := make([]sqlast.UpdateAssignment, 0, count)
	seen := make(map[string]bool, count)
	fields := schema.Definition().Fields
	declared := func(column string) bool {
		pointer := string(appendUpdateColumnPointer(nil, column))
		for _, field := range fields {
			if field.Path == pointer {
				return true
			}
		}
		return false
	}
	for i := 0; i < count; i++ {
		if len(program) < 7 {
			return nil, errReplicatedConflictProgram
		}
		kind := program[0]
		n := int(binary.LittleEndian.Uint16(program[1:3]))
		m := uint64(binary.LittleEndian.Uint32(program[3:7]))
		program = program[7:]
		if n == 0 || n > len(program) || m > uint64(len(program)-n) || m == 0 {
			return nil, errReplicatedConflictProgram
		}
		column := string(program[:n])
		value := program[n : n+int(m)]
		program = program[n+int(m):]
		if !utf8.ValidString(column) || seen[column] || !declared(column) {
			return nil, errReplicatedConflictProgram
		}
		seen[column] = true
		var operand sqlast.Operand
		switch kind {
		case 0:
			var err error
			operand, err = decodeConflictConstant(value)
			if err != nil {
				return nil, err
			}
		case 1:
			if len(value) > 65535 || !utf8.Valid(value) || !declared(string(value)) {
				return nil, errReplicatedConflictProgram
			}
			operand = sqlast.Operand{Kind: sqlast.OperandExcluded, Text: string(value)}
		default:
			return nil, errReplicatedConflictProgram
		}
		assignments = append(assignments, sqlast.UpdateAssignment{Column: column, Value: operand})
	}
	if len(program) != 0 {
		return nil, errReplicatedConflictProgram
	}
	return assignments, nil
}

// MaterializeConflict runs at the replicated apply/prepare point over the
// current participant snapshot. The candidate is validated on both branches;
// assignment evaluation happens only for an existing row. The state machine
// independently validates and fences the returned final document.
func (v *replicatedSQLMutationValidator) MaterializeConflict(key, candidate, program, current []byte, found bool) ([]byte, replicatedstate.MutationValidation) {
	if validation := v.ValidatePut(key, candidate); validation != replicatedstate.MutationValidationAccept {
		return nil, validation
	}
	assignments, err := decodeReplicatedConflictAssignments(program, v.schema)
	if err != nil {
		return nil, replicatedstate.MutationValidationInvalid
	}
	if !found {
		return candidate, replicatedstate.MutationValidationAccept
	}
	value, err := ApplyColumnAssignmentsWithExcluded(current, candidate, assignments, nil, v.maxDocumentBytes)
	if err == nil {
		value, err = canonicalMutationCapturePostimage(value, v.maxDocumentBytes)
	}
	if errors.Is(err, durable.ErrDocumentTooLarge) {
		return nil, replicatedstate.MutationValidationTargetBound
	}
	if err != nil {
		return nil, replicatedstate.MutationValidationInvalid
	}
	return value, replicatedstate.MutationValidationAccept
}
