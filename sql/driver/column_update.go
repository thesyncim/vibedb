package driver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
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
	return applyColumnAssignments(document, nil, assignments, args, nil, maxBytes)
}

// applyColumnAssignmentsWithExpressions materializes an ordinary declared-
// column UPDATE using the one-row computed-assignment projection returned by
// query.DMLStatement.EvaluateUpdateExpressions. Direct operands and computed
// cells are collected before the document is patched, so every right-hand side
// observes the same pre-update row.
func applyColumnAssignmentsWithExpressions(
	document []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	evaluated *query.Cursor,
	maxBytes int,
) ([]byte, error) {
	return applyColumnAssignments(
		document, nil, assignments, args, evaluated, maxBytes,
	)
}

func materializeColumnAssignments(
	statement *query.DMLStatement,
	exec *query.Exec,
	document []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	maxBytes int,
) ([]byte, error) {
	if statement == nil || !statement.HasUpdateExpressions() {
		return ApplyColumnAssignments(document, assignments, args, maxBytes)
	}
	cursor, err := statement.EvaluateUpdateExpressions(
		exec, document, args, maxBytes,
	)
	if err != nil {
		return nil, err
	}
	return applyColumnAssignmentsWithExpressions(
		document, assignments, args, &cursor, maxBytes,
	)
}

// MaterializePreparedUpdateAssignments evaluates and applies one prepared
// UPDATE assignment list over document. Every computed right-hand side sees
// the same pre-update document, and direct assignments are patched only after
// all computed values have been collected. statement and exec are mutable,
// single-consumer query state owned by the caller; document and args are
// borrowed only for this call, while the returned document is owned.
//
// This is the pre-storage materialization boundary. It intentionally does not
// validate a table schema, routing key, or canonical storage representation;
// distributed coordinators must perform those checks before retaining or
// publishing the returned bytes.
func MaterializePreparedUpdateAssignments(
	statement *query.DMLStatement,
	exec *query.Exec,
	document []byte,
	args []any,
	maxBytes int,
) ([]byte, error) {
	if statement == nil || statement.Tree() == nil ||
		statement.Tree().Kind != sqlast.KindUpdate ||
		statement.Tree().Update == nil {
		return nil, errors.New("vibedb: prepared UPDATE assignments require an UPDATE statement")
	}
	return materializeColumnAssignments(
		statement, exec, document,
		statement.Tree().Update.Assignments, args, maxBytes,
	)
}

// materializeConflictColumnAssignments evaluates the computed half of an
// INSERT ... ON CONFLICT DO UPDATE assignment list over one immutable pair of
// row images, then feeds those values through the same byte-preserving patcher
// as direct EXCLUDED assignments. The current and candidate documents remain
// distinct namespaces during evaluation and every right-hand side is collected
// before the first target column is changed.
func materializeConflictColumnAssignments(
	statement *query.DMLStatement,
	exec *query.Exec,
	document []byte,
	excluded []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	maxBytes int,
) ([]byte, error) {
	if statement == nil || !statement.HasConflictUpdateExpressions() {
		return ApplyColumnAssignmentsWithExcluded(
			document, excluded, assignments, args, maxBytes,
		)
	}
	cursor, err := statement.EvaluateConflictUpdateExpressions(
		exec, document, excluded, args, maxBytes,
	)
	if err != nil {
		return nil, err
	}
	return applyColumnAssignments(
		document, excluded, assignments, args, &cursor, maxBytes,
	)
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
	return applyColumnAssignments(document, excluded, assignments, args, nil, maxBytes)
}

func applyColumnAssignments(
	document []byte,
	excluded []byte,
	assignments []sqlast.UpdateAssignment,
	args []any,
	evaluated *query.Cursor,
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
	expressions := 0
	for i := range assignments {
		switch {
		case assignments[i].Expr != nil &&
			assignments[i].Value.Kind != sqlast.OperandExpression:
			return nil, fmt.Errorf(
				"vibedb: UPDATE assignment %q carries an expression without its expression marker",
				assignments[i].Column,
			)
		case assignments[i].Expr == nil &&
			assignments[i].Value.Kind == sqlast.OperandExpression:
			return nil, fmt.Errorf(
				"vibedb: UPDATE assignment %q requires a computed expression",
				assignments[i].Column,
			)
		case assignments[i].Expr != nil:
			expressions++
		}
	}
	if expressions != 0 {
		if evaluated == nil {
			return nil, fmt.Errorf(
				"vibedb: UPDATE has %d computed assignment(s) but no evaluated row",
				expressions,
			)
		}
		if !evaluated.Next() {
			return nil, fmt.Errorf(
				"vibedb: UPDATE computed assignment projection returned no row: %w",
				query.ErrScalarResultShape,
			)
		}
	} else if evaluated != nil {
		return nil, errors.New(
			"vibedb: direct UPDATE assignments were given an expression result",
		)
	}
	expression := 0
	for i := range assignments {
		var value []byte
		if assignments[i].Expr != nil {
			cell := evaluated.Cell(expression)
			expression++
			value = cell.JSON()
			if cell.Kind() == query.TypeNumber {
				if integer, ok := canonicalComputedInteger(
					value, maxBytes,
				); ok {
					value = integer
				}
			}
			kind := (vibejson.RawValue{Src: value}).Kind()
			if kind == jsondoc.Object || kind == jsondoc.Array {
				return nil, &query.ScalarTypeError{
					Pos:       assignments[i].Expr.Pos,
					Operation: "UPDATE SET",
					Left:      cell.Kind(),
					Right:     query.TypeAny,
				}
			}
		} else if assignments[i].Value.Kind == sqlast.OperandExcluded {
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
	if expression != expressions {
		return nil, fmt.Errorf(
			"vibedb: UPDATE consumed %d of %d computed assignments: %w",
			expression, expressions, query.ErrScalarResultShape,
		)
	}
	if evaluated != nil && evaluated.Next() {
		return nil, fmt.Errorf(
			"vibedb: UPDATE computed assignment projection returned more than one row: %w",
			query.ErrScalarResultShape,
		)
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

// canonicalComputedInteger converts an exact JSON number whose mathematical
// value is integral into the exponent-free spelling required by INTEGER schema
// fields. The scalar runtime deliberately emits normalized scientific notation
// (for example 12 as 1.2e1); SELECT keeps that representation, while assignment
// materialization applies the target document's JSON integer boundary. Growth
// is capped before allocation so an attacker-sized exponent cannot bypass the
// document limit.
func canonicalComputedInteger(value []byte, maxBytes int) ([]byte, bool) {
	if len(value) == 0 || maxBytes <= 0 {
		return nil, false
	}
	index := 0
	negative := false
	if value[index] == '-' {
		negative = true
		index++
		if index == len(value) {
			return nil, false
		}
	}
	integerStart := index
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	integerEnd := index
	if integerEnd == integerStart {
		return nil, false
	}
	fractionStart, fractionEnd := index, index
	if index < len(value) && value[index] == '.' {
		index++
		fractionStart = index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		fractionEnd = index
		if fractionEnd == fractionStart {
			return nil, false
		}
	}
	exponent := int64(0)
	if index < len(value) && (value[index] == 'e' || value[index] == 'E') {
		index++
		start := index
		if index < len(value) && (value[index] == '+' || value[index] == '-') {
			index++
		}
		digits := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if digits == index {
			return nil, false
		}
		parsed, err := strconv.ParseInt(string(value[start:index]), 10, 64)
		if err != nil {
			return nil, false
		}
		exponent = parsed
	}
	if index != len(value) {
		return nil, false
	}
	fractionDigits := fractionEnd - fractionStart
	if exponent < math.MinInt64+int64(fractionDigits) {
		return nil, false
	}
	scale := exponent - int64(fractionDigits)
	totalDigits := integerEnd - integerStart + fractionDigits
	digitAt := func(ordinal int) byte {
		integerDigits := integerEnd - integerStart
		if ordinal < integerDigits {
			return value[integerStart+ordinal]
		}
		return value[fractionStart+ordinal-integerDigits]
	}
	nonzero := false
	for ordinal := 0; ordinal < totalDigits; ordinal++ {
		if digitAt(ordinal) != '0' {
			nonzero = true
			break
		}
	}
	if !nonzero {
		return []byte{'0'}, true
	}
	outputDigits := totalDigits
	zeros := int64(0)
	if scale < 0 {
		if scale == math.MinInt64 {
			return nil, false
		}
		remove := -scale
		if remove >= int64(totalDigits) {
			return nil, false
		}
		outputDigits -= int(remove)
		for ordinal := outputDigits; ordinal < totalDigits; ordinal++ {
			if digitAt(ordinal) != '0' {
				return nil, false
			}
		}
	} else {
		zeros = scale
	}
	first := 0
	for first < outputDigits && digitAt(first) == '0' {
		first++
	}
	if first == outputDigits {
		return []byte{'0'}, true
	}
	width := int64(outputDigits-first) + zeros
	if negative {
		width++
	}
	if width <= 0 || width > int64(maxBytes) || width > int64(^uint(0)>>1) {
		return nil, false
	}
	integer := make([]byte, 0, int(width))
	if negative {
		integer = append(integer, '-')
	}
	for ordinal := first; ordinal < outputDigits; ordinal++ {
		integer = append(integer, digitAt(ordinal))
	}
	for ; zeros > 0; zeros-- {
		integer = append(integer, '0')
	}
	return integer, true
}

// mutationTargetRelation returns the range name visible to conflict RHS paths
// while leaving the caller's physical table name untouched.
func mutationTargetRelation(table, alias string) string {
	if alias != "" {
		return alias
	}
	return table
}

// validateUpsertColumnAssignments resolves the deliberately flat conflict SET
// list against the current table incarnation in authored order: each target is
// bound before that assignment's direct EXCLUDED value or scalar expression,
// then binding advances to the next assignment. EXCLUDED is a row namespace,
// not a way to reach arbitrary undeclared JSON members through SQL.
func validateUpsertColumnAssignments(
	table string,
	targetRelation string,
	meta *tableMeta,
	assignments []sqlast.UpdateAssignment,
) error {
	for i := range assignments {
		assignment := &assignments[i]
		if err := validateDeclaredColumnAssignment(
			table, meta, *assignment,
		); err != nil {
			return err
		}
		value := assignment.Value
		if value.Kind == sqlast.OperandExcluded &&
			!declaredTopLevelColumn(meta, value.Text) {
			return &query.RelationColumnError{
				Relation: "EXCLUDED",
				Column:   value.Text,
				Pos:      value.Pos,
			}
		}
		if err := validateUpsertScalarColumns(
			targetRelation, meta, assignment.Expr,
		); err != nil {
			return err
		}
	}
	return nil
}

// validateUpsertScalarColumns rechecks both row namespaces against the live
// table incarnation. Query preparation deliberately has no SQL catalog, so the
// driver must repeat this at execution as well as prepare time to keep a stale
// prepared statement from reading a column that disappeared with a recreated
// table. Conflict expressions are intentionally flat SQL-column expressions:
// source zero is the current row and source one is EXCLUDED.
func validateUpsertScalarColumns(
	targetRelation string,
	meta *tableMeta,
	expression *sqlast.ScalarExpr,
) error {
	if expression == nil {
		return nil
	}
	if err := validateUpsertScalarPath(targetRelation, meta, expression.Path); err != nil {
		return err
	}
	if err := validateUpsertScalarColumns(targetRelation, meta, expression.Left); err != nil {
		return err
	}
	if err := validateUpsertScalarColumns(targetRelation, meta, expression.Right); err != nil {
		return err
	}
	for i := range expression.Whens {
		when := &expression.Whens[i]
		if err := validateUpsertPredicateColumns(targetRelation, meta, when.Predicate); err != nil {
			return err
		}
		if err := validateUpsertScalarColumns(targetRelation, meta, when.Match); err != nil {
			return err
		}
		if err := validateUpsertScalarColumns(targetRelation, meta, when.Result); err != nil {
			return err
		}
	}
	return validateUpsertScalarColumns(targetRelation, meta, expression.Else)
}

func validateUpsertPredicateColumns(
	targetRelation string,
	meta *tableMeta,
	expression *sqlast.Expr,
) error {
	if expression == nil {
		return nil
	}
	if err := validateUpsertScalarPath(targetRelation, meta, expression.Path); err != nil {
		return err
	}
	if err := validateUpsertScalarPath(targetRelation, meta, expression.RightPath); err != nil {
		return err
	}
	if err := validateUpsertScalarColumns(targetRelation, meta, expression.ScalarLeft); err != nil {
		return err
	}
	if err := validateUpsertScalarColumns(targetRelation, meta, expression.ScalarRight); err != nil {
		return err
	}
	for i := range expression.Kids {
		if err := validateUpsertPredicateColumns(targetRelation, meta, expression.Kids[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateUpsertScalarPath(
	targetRelation string,
	meta *tableMeta,
	path *sqlast.PathExpr,
) error {
	if path == nil {
		return nil
	}
	relation := targetRelation
	unresolved := false
	switch path.Source {
	case 0:
	case 1:
		relation = "EXCLUDED"
	case sqlast.ConflictUnresolvedSource:
		unresolved = true
	default:
		return fmt.Errorf(
			"vibedb: ON CONFLICT expression path at byte %d has invalid source %d",
			path.Pos, path.Source,
		)
	}
	if len(path.Segments) != 1 || path.Segments[0].IsIndex {
		return &query.RelationColumnError{
			Relation: relation,
			Column:   path.Spec(),
			Pos:      path.Pos,
		}
	}
	column := path.Segments[0].Key
	if unresolved {
		matches := 0
		if declaredTopLevelColumn(meta, column) {
			// The target row and EXCLUDED expose the same declared columns.
			// A bare declared name therefore matches both namespaces.
			matches = 2
		}
		return &query.RelationColumnError{
			Relation: relation,
			Column:   column,
			Matches:  matches,
			Pos:      path.Pos,
		}
	}
	if declaredTopLevelColumn(meta, column) {
		return nil
	}
	return &query.RelationColumnError{
		Relation: relation,
		Column:   column,
		Pos:      path.Pos,
	}
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
		if err := validateDeclaredColumnAssignment(
			table, meta, assignments[i],
		); err != nil {
			return err
		}
	}
	return nil
}

func validateDeclaredColumnAssignment(
	table string,
	meta *tableMeta,
	assignment sqlast.UpdateAssignment,
) error {
	if declaredTopLevelColumn(meta, assignment.Column) {
		return nil
	}
	return &query.RelationColumnError{
		Relation: table,
		Column:   assignment.Column,
		Pos:      assignment.Pos,
	}
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
		if assignments[i].Expr != nil {
			if assignments[i].Value.Kind != sqlast.OperandExpression {
				return fmt.Errorf(
					"vibedb: UPDATE assignment %q carries an expression without its expression marker",
					assignments[i].Column,
				)
			}
			continue
		}
		if assignments[i].Value.Kind == sqlast.OperandExpression {
			return fmt.Errorf(
				"vibedb: UPDATE assignment %q has an expression marker without an expression",
				assignments[i].Column,
			)
		}
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
