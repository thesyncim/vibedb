package gateway

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The distributed write path: one mutating statement routed to the single shard
// that owns its rows, dispatched through the same pinned-generation machinery
// as reads, and executed by the shard as one local statement.
//
// A write is dispatchable only when every row it touches is provably resident on
// exactly one shard of the pinned generation:
//
//   - an INSERT routes every VALUES row by its shard key and is admitted only
//     when all rows resolve to the same leader target;
//   - an UPDATE or DELETE routes its WHERE predicate and is admitted only when
//     it resolves to one shard (an empty route is a local no-op), and an
//     UPDATE's whole-document replacement must not move a row to another shard.
//
// Everything else — a scatter, an unbounded predicate, a cross-shard batch, or
// a statement kind with no single-shard form (DDL, TRUNCATE, INSERT ... SELECT)
// — is refused before any network I/O, so a write never partially commits.
// Cross-shard batches use this same per-statement proof before ExecBatch groups
// their single-owner statements into durable participants.

// ErrDistributedWriteUnsupported reports a write shape the distributed layer
// cannot execute against more than zero shards of a generation. It wraps a
// typed refusal; the planner fails before dispatch rather than dispatching a
// semantically incomplete scatter.
var ErrDistributedWriteUnsupported = errors.New("gateway: distributed write plan is unsupported")

// ErrWriteScatter reports an UPDATE or DELETE whose predicate does not resolve
// to exactly one shard.
var ErrWriteScatter = errors.New("gateway: write predicate does not resolve to a single shard")

// ErrWriteCrossShard reports an INSERT whose VALUES rows do not all route to
// the same shard.
var ErrWriteCrossShard = errors.New("gateway: insert rows route to more than one shard")

// ErrWriteShardKeyMove reports a whole-document UPDATE whose replacement
// document would move the row to a different shard.
var ErrWriteShardKeyMove = errors.New("gateway: update replacement document would move the row to another shard")

// ErrExecRequiresMutation reports a non-mutating statement submitted to
// [Executor.Exec].
var ErrExecRequiresMutation = errors.New("gateway: Exec requires a mutating statement")

// prepareWrite validates one mutating statement against the pinned catalog
// generation and records the routing metadata [PreparedPlan.BindWrite] binds
// against later. Like the read path it fails closed: a table with no placement,
// a statement kind with no single-shard form, or a shape that cannot be proven
// single-shard never becomes a dispatchable plan.
func (s *Snapshot) prepareWrite(plan *PreparedPlan) error {
	stmt := &plan.statement
	var (
		where       *sqlast.Expr
		hasWhere    bool
		wholeDocIns bool
	)
	switch stmt.Kind {
	case sqlast.KindInsert:
		ins := stmt.Insert
		if ins == nil {
			return &PlanError{Reason: "malformed insert statement", cause: ErrDistributedWriteUnsupported}
		}
		if ins.Source != nil {
			plan.alwaysReason = "INSERT with a query source requires a distributed source plan"
			break
		}
		if len(ins.Rows) == 0 {
			plan.alwaysReason = "an INSERT requires at least one VALUES row"
			break
		}
		wholeDocIns = len(ins.Columns) == 0
	case sqlast.KindUpdate:
		upd := stmt.Update
		if upd == nil {
			return &PlanError{Reason: "malformed update statement", cause: ErrDistributedWriteUnsupported}
		}
		if upd.Filter == nil || upd.Filter.Where == nil {
			plan.alwaysReason = "an UPDATE without a shard-key predicate is a cross-shard scatter"
			break
		}
		where = upd.Filter.Where
		hasWhere = true
	case sqlast.KindDelete:
		del := stmt.Delete
		if del == nil {
			return &PlanError{Reason: "malformed delete statement", cause: ErrDistributedWriteUnsupported}
		}
		if del.Filter == nil || del.Filter.Where == nil {
			plan.alwaysReason = "a DELETE without a shard-key predicate is a cross-shard scatter"
			break
		}
		where = del.Filter.Where
		hasWhere = true
	default:
		return &WriteNotSupportedError{Kind: stmt.Kind}
	}

	plan.table = stmt.Table()
	placement, spec, manifest, ok := s.plannerTableFor(plan.table)
	if !ok {
		return &PlanError{
			Table: plan.table, Reason: "no placement in pinned catalog generation",
			cause: ErrTableNotPlaced,
		}
	}
	plan.distribution = placement.Distribution
	plan.spec = spec
	plan.manifest = manifest
	plan.params = stmt.Params()
	if hasWhere {
		plan.constraints = sqldriver.CompileConstraintProgram(placement.Columns, where)
	}

	// Compile the shard-key pointers once per plan: whole-document inserts and
	// UPDATE replacement documents both route by reading them out of a JSON
	// document.
	if stmt.Kind == sqlast.KindUpdate || wholeDocIns {
		pointers := make([]vibejson.CompiledPointer, len(placement.Columns))
		for i, col := range placement.Columns {
			p, err := vibejson.CompilePointer(col)
			if err != nil {
				return &PlanError{
					Table: plan.table, Reason: "shard-key column " + col + " is not a compilable JSON pointer",
					cause: ErrDistributedWriteUnsupported,
				}
			}
			pointers[i] = p
		}
		plan.writeKeyPointers = pointers
	}
	// A flat insert supplies each shard-key ordinal from one named top-level
	// column. Prove every ordinal is named before the plan can be bound; a
	// missing ordinal means the row document's shard key is unknown at routing
	// time and the insert is not single-shard-provable.
	if stmt.Kind == sqlast.KindInsert && !wholeDocIns {
		ins := stmt.Insert
		keyColumns := make([]int, len(placement.Columns))
		for ordinal, col := range placement.Columns {
			i, ok := -1, false
			for candidate := range ins.Columns {
				if vibejson.BytesEqualString(ins.Columns[candidate].AppendPointer(nil), col) {
					i, ok = candidate, true
					break
				}
			}
			if !ok {
				plan.alwaysReason = "shard-key column " + col + " is not a top-level insert column"
				break
			}
			keyColumns[ordinal] = i
		}
		if plan.alwaysReason == "" {
			plan.writeKeyColumns = keyColumns
		}
	}
	return nil
}

// BoundWritePlan is the immutable execution-specific result of binding typed
// parameters to a prepared write plan. rowKeys holds one full shard key per
// INSERT VALUES row; constraints hold the UPDATE or DELETE predicate domains.
// A write plan routes to at most one shard, so neither field can carry a
// scatter.
type BoundWritePlan struct {
	generation   uint64
	table        string
	distribution distribution.DistributionName
	spec         distribution.DistributionSpec
	manifest     *distribution.Manifest

	kind        sqlast.Kind
	constraints distribution.BoundConstraints
	rowKeys     [][]distribution.Scalar
	// keyPointers holds one compiled shard-key pointer per ordinal for a
	// whole-document insert or UPDATE; it is nil otherwise.
	keyPointers []vibejson.CompiledPointer
	// updateDoc holds the UPDATE's whole-document replacement bytes, materialized
	// from its bound operand. It is nil unless the plan is a whole-document UPDATE;
	// the executor re-reads its shard key from it to prove the replacement cannot
	// move a row to another shard.
	updateDoc []byte
}

// BindWrite applies params to a prepared write plan without reparsing SQL.
// args use the ordinary sql/driver scalar vocabulary. An INSERT extracts each
// row's shard key from its bound values; an UPDATE or DELETE binds its WHERE
// predicate and, for a whole-document UPDATE, proves the replacement document
// cannot move the row to another shard.
func (p *PreparedPlan) BindWrite(args []any) (*BoundWritePlan, error) {
	if p == nil || p.manifest == nil {
		return nil, &PlanError{Reason: "incomplete prepared write plan", cause: ErrDistributedWriteUnsupported}
	}
	if len(args) != p.params {
		return nil, &PlanError{
			Table:  p.table,
			Reason: fmt.Sprintf("got %d parameters, want %d", len(args), p.params),
			cause:  ErrPlanParameters,
		}
	}
	if p.alwaysReason != "" {
		// A shape refused at prepare time is unbindable by design: it can never
		// resolve to one shard for any parameter values, so refuse before binding
		// or routing rather than carrying a plan that routeWrite would reject.
		return nil, &PlanError{
			Table: p.table, Reason: p.alwaysReason, cause: ErrDistributedWriteUnsupported,
		}
	}
	bound := &BoundWritePlan{
		generation:   p.generation,
		table:        p.table,
		distribution: p.distribution,
		spec:         p.spec,
		manifest:     p.manifest,
		kind:         p.statement.Kind,
		keyPointers:  p.writeKeyPointers,
	}
	switch p.statement.Kind {
	case sqlast.KindInsert:
		keys, err := p.bindInsertRowKeys(args)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
		bound.rowKeys = keys
	case sqlast.KindUpdate, sqlast.KindDelete:
		if p.constraints == nil {
			return nil, &PlanError{
				Table:  p.table,
				Reason: "write predicate was not compiled at prepare time",
				cause:  ErrDistributedWriteUnsupported,
			}
		}
		cons, err := p.constraints.Bind(args)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
		bound.constraints = cons
		if p.statement.Kind == sqlast.KindUpdate {
			doc, err := writeOperandDocument(p.statement.Update.Doc, args)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
			}
			bound.updateDoc = doc
		}
	}
	return bound, nil
}

// bindInsertRowKeys extracts one full shard key per VALUES row. A whole-document
// row parses the document once and reads every shard-key pointer out of it; a
// flat row reads each shard-key ordinal from the bound operand of its named
// top-level column. A missing, null, or non-scalar shard-key value is a routing
// error: the row cannot be proven single-shard.
func (p *PreparedPlan) bindInsertRowKeys(args []any) ([][]distribution.Scalar, error) {
	ins := p.statement.Insert
	if ins == nil {
		return nil, errors.New("insert has no statement body")
	}
	keys := make([][]distribution.Scalar, len(ins.Rows))
	var doc []byte
	for i, row := range ins.Rows {
		var (
			key []distribution.Scalar
			err error
		)
		if len(ins.Columns) == 0 {
			doc, err = writeOperandDocument(row.Values[0], args)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i, err)
			}
			key, err = writeDocShardKey(doc, p.writeKeyPointers)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i, err)
			}
		} else {
			key = make([]distribution.Scalar, 0, len(p.writeKeyColumns))
			for ordinal, colIdx := range p.writeKeyColumns {
				value := writeOperandValue(row.Values[colIdx], args)
				scalar, err := writeScalarFromValue(value)
				if err != nil {
					return nil, fmt.Errorf("row %d: shard-key column ordinal %d: %w", i, ordinal, err)
				}
				key = append(key, scalar)
			}
		}
		keys[i] = key
	}
	return keys, nil
}

// writeOperandDocument returns the JSON document bytes an insert operand
// carries: the bound document parameter for a placeholder, or the operand's
// literal text for an inline literal.
func writeOperandDocument(op sqlast.Operand, args []any) ([]byte, error) {
	switch op.Kind {
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return nil, fmt.Errorf("parameter %d is out of range", op.Ordinal+1)
		}
		switch value := args[op.Ordinal].(type) {
		case vibejson.RawValue:
			if err := vibejson.Validate(value.Bytes()); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return value.Bytes(), nil
		case []byte:
			if err := vibejson.Validate(value); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return value, nil
		case string:
			document := byteview.Bytes(value)
			if err := vibejson.Validate(document); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return document, nil
		default:
			return nil, errors.New("document parameter is not a JSON document")
		}
	case sqlast.OperandString, sqlast.OperandJSON:
		document := byteview.Bytes(op.Text)
		if err := vibejson.Validate(document); err != nil {
			return nil, fmt.Errorf("document literal is invalid JSON: %w", err)
		}
		return document, nil
	default:
		return nil, fmt.Errorf("operand kind %d is not a JSON document", op.Kind)
	}
}

// writeOperandValue materializes one insert operand in the same byte-native
// vocabulary used by shardservice: unquoted strings are []byte and exact
// numbers/JSON literals are vibejson.RawValue.
func writeOperandValue(op sqlast.Operand, args []any) any {
	switch op.Kind {
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return nil
		}
		return args[op.Ordinal]
	case sqlast.OperandString:
		return byteview.Bytes(op.Text)
	case sqlast.OperandNumber:
		return vibejson.RawValue{Src: byteview.Bytes(op.Text)}
	case sqlast.OperandJSON:
		return vibejson.RawValue{Src: byteview.Bytes(op.Text)}
	case sqlast.OperandBool:
		return op.Bool
	default:
		return nil
	}
}

// writeScalarFromValue converts a bound shard-key value to a placement scalar.
// It accepts the closed gateway parameter vocabulary — UTF-8 bytes and a
// vibejson exact number —
// and refuses every other value, so a non-scalar shard key never narrows or
// misroutes a write.
func writeScalarFromValue(value any) (distribution.Scalar, error) {
	switch v := value.(type) {
	case []byte:
		return distribution.NewString(byteview.String(v)), nil
	case string:
		return distribution.NewString(v), nil
	case vibejson.RawValue:
		number, ok := v.NumberBytes()
		if !ok {
			return distribution.Scalar{}, errors.New("shard-key value is not a JSON number")
		}
		return distribution.NewNumber(byteview.String(number))
	case interface{ String() string }:
		// Compatibility for callers binding encoding/json.Number directly. New
		// distributed traffic reaches this path as vibejson.RawValue.
		number := byteview.Bytes(v.String())
		raw := vibejson.RawValue{Src: number}
		if !vibejson.Valid(number) || raw.Kind() != jsondoc.Number {
			return distribution.Scalar{}, errors.New("shard-key value is not a JSON number")
		}
		return distribution.NewNumber(byteview.String(number))
	default:
		return distribution.Scalar{}, errors.New("shard-key value is not a string or number")
	}
}

// writeDocShardKey reads every shard-key pointer out of one JSON document,
// mirroring the driver's primary-key extraction: a missing, null, or
// non-scalar value is a routing error.
func writeDocShardKey(doc []byte, pointers []vibejson.CompiledPointer) ([]distribution.Scalar, error) {
	needed, err := vibejson.RequiredIndexEntries(doc)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON document: %w", err)
	}
	index, err := vibejson.BuildIndex(doc, make([]vibejson.IndexEntry, needed))
	if err != nil {
		return nil, fmt.Errorf("invalid JSON document: %w", err)
	}
	root := index.Root()
	key := make([]distribution.Scalar, 0, len(pointers))
	var scratch []byte
	for i, ptr := range pointers {
		node, found, err := root.PointerCompiled(ptr)
		if err != nil {
			return nil, fmt.Errorf("shard-key column %d: %w", i, err)
		}
		if !found {
			return nil, fmt.Errorf("shard-key column %d is missing", i)
		}
		value := node.Raw()
		if value.IsNull() {
			return nil, fmt.Errorf("shard-key column %d is null", i)
		}
		switch value.Kind() {
		case jsondoc.String:
			if text, ok := value.StringBytes(); ok {
				key = append(key, distribution.NewString(byteview.String(text)))
				break
			}
			if cap(scratch) < len(doc) {
				scratch = make([]byte, 0, len(doc))
			}
			start := len(scratch)
			scratch, ok, err := value.AppendText(scratch)
			if err != nil {
				return nil, fmt.Errorf("shard-key column %d has an invalid JSON string: %w", i, err)
			}
			if !ok {
				return nil, fmt.Errorf("shard-key column %d has an invalid JSON string", i)
			}
			key = append(key, distribution.NewString(byteview.String(scratch[start:])))
		case jsondoc.Number:
			number, ok := value.NumberText()
			if !ok {
				return nil, fmt.Errorf("shard-key column %d is not a valid JSON number", i)
			}
			scalar, err := distribution.NewNumber(number)
			if err != nil {
				return nil, fmt.Errorf("shard-key column %d: %w", i, err)
			}
			key = append(key, scalar)
		default:
			return nil, fmt.Errorf("shard-key column %d must be a JSON string or number", i)
		}
	}
	return key, nil
}
