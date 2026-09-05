package driver

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

const replicatedConflictAssignmentLimit = 1024
const replicatedConflictWorkBytes = 16 << 20

var errReplicatedConflictProgram = errors.New("vibedb: invalid replicated conflict program")

// ReplicatedConflictProgram identifies actions requiring atomic branch evaluation.
// An unconditional whole-document replacement retains the ordinary put path.
func ReplicatedConflictProgram(action *sqlast.InsertConflictUpdate) bool {
	if action == nil || len(action.Assignments) > replicatedConflictAssignmentLimit {
		return false
	}
	if len(action.Assignments) == 0 {
		return action.WholeDocument() && action.Where != nil
	}
	for _, assignment := range action.Assignments {
		if assignment.Expr != nil {
			if assignment.Value.Kind != sqlast.OperandExpression {
				return false
			}
			continue
		}
		switch assignment.Value.Kind {
		case sqlast.OperandNull, sqlast.OperandString, sqlast.OperandNumber, sqlast.OperandBool, sqlast.OperandParam, sqlast.OperandExcluded:
		default:
			return false
		}
	}
	return true
}

// ValidateReplicatedConflictAction resolves both row namespaces against
// the authenticated declaration and compiles the same projection as local SQL.
func ValidateReplicatedConflictAction(info TableInfo, action *sqlast.InsertConflictUpdate) error {
	if !ReplicatedConflictProgram(action) {
		return errReplicatedConflictProgram
	}
	meta := &tableMeta{PrimaryKey: info.PrimaryKey, Schema: &schemaMeta{}}
	for _, column := range info.Columns {
		meta.Schema.Fields = append(meta.Schema.Fields, schemaFieldMeta{Path: column.Path, Types: uint16(column.Types), Required: column.Required})
	}
	if err := validateUpsertConflictAction(info.Name, info.Name, meta, action); err != nil {
		return err
	}
	template, _, err := encodeConflictTemplate(action, nil)
	if err != nil {
		return err
	}
	action, params, err := decodeConflictTemplate(template)
	if err != nil {
		return err
	}
	statement, err := prepareReplicatedConflict(action, params)
	if statement != nil {
		statement.Release()
	}
	return err
}

// EncodeReplicatedConflictValue replicates a template and only the parameters
// it references. Candidate-only binds and the current row are never included in
// the program. Dense remapping makes the template independent of INSERT arity.
func EncodeReplicatedConflictValue(candidate []byte, action *sqlast.InsertConflictUpdate, args []any, parameterTypes []query.ParameterType) ([]byte, error) {
	if !ReplicatedConflictProgram(action) {
		return nil, errReplicatedConflictProgram
	}
	template, ordinals, err := encodeConflictTemplate(action, parameterTypes)
	if err != nil {
		return nil, err
	}
	program := binary.LittleEndian.AppendUint32(nil, uint32(len(template)))
	program = append(program, template...)
	bindings := make([][]byte, len(ordinals))
	for ordinal, dense := range ordinals {
		value, err := encodeAssignmentScalar(sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: ordinal}, args)
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
		bindings[dense] = value
	}
	for _, value := range bindings {
		if len(program) > replication.MaxMutationValueBytes-4-len(value) {
			return nil, errReplicatedConflictProgram
		}
		program = binary.LittleEndian.AppendUint32(program, uint32(len(value)))
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

func openConflictProgram(program []byte) (template, bindings []byte, err error) {
	if len(program) < 8 || len(program) > replication.MaxMutationValueBytes {
		return nil, nil, errReplicatedConflictProgram
	}
	n := uint64(binary.LittleEndian.Uint32(program[:4]))
	if n < 4 || n > uint64(len(program)-4) {
		return nil, nil, errReplicatedConflictProgram
	}
	return program[4 : 4+int(n) : 4+int(n)], program[4+int(n):], nil
}

func validateConflictTemplateSchema(action *sqlast.InsertConflictUpdate, schema *store.Schema) error {
	if schema == nil {
		return errReplicatedConflictProgram
	}
	meta := &tableMeta{Schema: &schemaMeta{}}
	for _, field := range schema.Definition().Fields {
		meta.Schema.Fields = append(meta.Schema.Fields, schemaFieldMeta{Path: field.Path, Types: uint16(field.Types), Required: field.Required})
	}
	return validateUpsertConflictAction("", "", meta, action)
}

func decodeConflictBindings(bindings []byte, args []any) error {
	for i := range args {
		if len(bindings) < 4 {
			return errReplicatedConflictProgram
		}
		n := uint64(binary.LittleEndian.Uint32(bindings[:4]))
		bindings = bindings[4:]
		if n == 0 || n > uint64(len(bindings)) {
			return errReplicatedConflictProgram
		}
		value, err := decodeConflictConstant(bindings[:int(n)])
		if err != nil {
			return err
		}
		bindings = bindings[int(n):]
		switch value.Kind {
		case sqlast.OperandNull:
			args[i] = nil
		case sqlast.OperandString:
			args[i] = value.Text
		case sqlast.OperandNumber:
			args[i] = query.Number(value.Text)
		case sqlast.OperandBool:
			args[i] = value.Bool
		default:
			return errReplicatedConflictProgram
		}
	}
	if len(bindings) != 0 {
		return errReplicatedConflictProgram
	}
	return nil
}

func decodeReplicatedConflictAction(program []byte, schema *store.Schema) (*sqlast.InsertConflictUpdate, error) {
	template, bindings, err := openConflictProgram(program)
	if err != nil {
		return nil, err
	}
	action, params, err := decodeConflictTemplate(template)
	if err != nil {
		return nil, err
	}
	if err = validateConflictTemplateSchema(action, schema); err != nil {
		return nil, err
	}
	if err = decodeConflictBindings(bindings, make([]any, len(params))); err != nil {
		return nil, err
	}
	return action, nil
}

func prepareReplicatedConflict(action *sqlast.InsertConflictUpdate, params []query.ParameterType) (*query.DMLStatement, error) {
	return query.PrepareParsedDMLWithParameterTypes("", &sqlast.Statement{Kind: sqlast.KindInsert, Insert: &sqlast.InsertStmt{
		Table: "__vibedb_conflict_input", Params: len(params), OnConflictUpdate: action,
	}}, params)
}

// One most-recent template per relation bounds retained compilation state.
// It is protected by the validator mutex, including detached snapshot audits.
// A changed binding does not replace the template or retain previous values.
type replicatedConflictPlan struct {
	template  []byte
	statement *query.DMLStatement
	args      []any
	exec      query.Exec
}

func (v *replicatedSQLMutationValidator) conflictPlan(template []byte) (*replicatedConflictPlan, error) {
	if v.conflict != nil && bytes.Equal(v.conflict.template, template) {
		return v.conflict, nil
	}
	action, params, err := decodeConflictTemplate(template)
	if err != nil {
		return nil, err
	}
	if err = validateConflictTemplateSchema(action, v.schema); err != nil {
		return nil, err
	}
	statement, err := prepareReplicatedConflict(action, params)
	if err != nil {
		return nil, err
	}
	if v.conflict != nil {
		v.conflict.statement.Release()
		v.conflict.exec.Release()
	}
	v.conflict = &replicatedConflictPlan{
		template: bytes.Clone(template), statement: statement, args: make([]any, len(params)),
		exec: query.Exec{Options: query.ExecOptions{Workers: 1, ResultRows: 1,
			MemoryBytes: replicatedConflictWorkBytes, ResultBytes: replicatedConflictWorkBytes,
			IntermediateBytes: replicatedConflictWorkBytes, AggregateBytes: replicatedConflictWorkBytes}},
	}
	return v.conflict, nil
}

// MaterializeConflict evaluates against the participant's atomic snapshot.
// Candidate, names and bindings are checked on both branches; lazy RHS runtime
// evaluation happens only after an existing row passes its condition. Every
// RHS sees the same preimage; a skipped condition returns current and false.
// The state machine independently validates/fences the final canonical document.
func (v *replicatedSQLMutationValidator) MaterializeConflict(key, candidate, program, current []byte, found bool) ([]byte, bool, replicatedstate.MutationValidation) {
	if validation := v.ValidatePut(key, candidate); validation != replicatedstate.MutationValidationAccept {
		return nil, false, validation
	}
	template, bindings, err := openConflictProgram(program)
	if err != nil {
		return nil, false, replicatedstate.MutationValidationInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	plan, err := v.conflictPlan(template)
	if err != nil {
		return nil, false, replicatedstate.MutationValidationInvalid
	}
	defer clear(plan.args)
	if err = decodeConflictBindings(bindings, plan.args); err != nil {
		return nil, false, replicatedstate.MutationValidationInvalid
	}
	if !found {
		// No projection will run on an insert. Still validate bindings before
		// accepting it; the conflict branch binds once in the shared evaluator.
		if err = plan.statement.ValidateConflictUpdateExpressionBindings(plan.args); err != nil {
			return nil, false, replicatedstate.MutationValidationInvalid
		}
		return candidate, true, replicatedstate.MutationValidationAccept
	}
	value, matched, err := materializeConflictUpdate(plan.statement, &plan.exec, current, candidate, plan.args, v.maxDocumentBytes)
	if err == nil && matched {
		value, err = canonicalMutationCapturePostimage(value, v.maxDocumentBytes)
	}
	if errors.Is(err, durable.ErrDocumentTooLarge) || errors.Is(err, query.ErrResultBudget) || errors.Is(err, query.ErrIntermediateBudget) || errors.Is(err, query.ErrAggregateBudget) || errors.Is(err, query.ErrWorkBudget) {
		return nil, false, replicatedstate.MutationValidationTargetBound
	}
	if err != nil {
		return nil, false, replicatedstate.MutationValidationInvalid
	}
	return value, matched, replicatedstate.MutationValidationAccept
}
