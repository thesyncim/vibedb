package query

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
	"strconv"
	"unsafe"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// statementRelationJoin is the prepared, relation-valued join pipeline used
// whenever SQL has more than one FROM item. The ordinary no-join statement
// keeps this pointer nil and reaches none of this state or dispatch.
type statementRelationJoin struct {
	operands []relationJoinOperand
	sources  []relationJoinSource
	stages   []relationJoinStage
	merged   []relationJoinMerged
	paths    []relationJoinPreparedPath

	ordinalSpec []string
	specData    []byte

	spools      [2]relationSpool
	activeBytes [2]int64
	final       *relationSpool

	buckets []int32
	next    []int32
	hashes  []uint64
	matched []uint32
	epoch   uint32
	seed    maphash.Seed
	seeded  bool

	leftText    []byte
	rightText   []byte
	literalText []byte
	eval        evalScratch

	lastPairs     []uint64
	lastHash      []bool
	lastBuildRows []uint64
}

type relationJoinOperand struct {
	ref      *sqlast.TableRef
	physical bool
	names    []string
	offset   int
	columns  int

	query  *Query
	cursor Statement
	stmt   *Statement
	cte    *statementCTEReference
	exec   Exec
	spool  relationSpool
	active int64
	bound  *relationSpool
}

type relationJoinSource struct {
	physical    bool
	names       []string
	offset      int
	columns     int
	ordinalSpec []string
}

type relationJoinMerged struct {
	join    int
	name    string
	ordinal int
}

type relationJoinPreparedPath struct {
	ast  *sqlast.PathExpr
	path relationJoinPath
}

type relationJoinPath struct {
	source int
	column int
	root   bool
	spec   string
	suffix vibejson.CompiledPointer
}

type relationJoinKey struct {
	left  relationJoinPath
	right relationJoinPath
}

type relationJoinUsing struct {
	name    string
	ordinal int
	left    relationJoinPath
	right   relationJoinPath
}

type relationJoinContains struct {
	expr   *sqlast.Expr
	needle vibejson.Index
}

type relationJoinStage struct {
	ref           *sqlast.TableRef
	index         int
	leftColumns   int
	rightOffset   int
	outputColumns int
	keys          []relationJoinKey
	using         []relationJoinUsing
	contains      []relationJoinContains
	algorithm     string
}

func (s *Statement) relationJoin() *statementRelationJoin {
	if s == nil || s.nested == nil {
		return nil
	}
	return s.nested.relationJoin
}

// requiresGeneralizedRelationJoin selects the relation pipeline only when the
// existing storage-aware join cannot represent the SQL shape faithfully. The
// legacy subset remains untouched: physical INNER/LEFT single-key joins
// against source zero, with at most one fan-out and only predicates its
// pre-join narrowing rules can preserve. This check runs once at prepare and
// is never reached by statements without a JOIN.
func requiresGeneralizedRelationJoin(tree *sqlast.SelectStmt) bool {
	if tree == nil || len(tree.From) <= 1 {
		return false
	}
	if tree.From[0].Kind != sqlast.RelationCollection {
		return true
	}
	for i := 1; i < len(tree.From); i++ {
		ref := &tree.From[i]
		if ref.Kind != sqlast.RelationCollection ||
			(ref.Join != sqlast.JoinInner && ref.Join != sqlast.JoinLeft) ||
			ref.On == nil || ref.On.Residual || len(ref.On.Keys) != 1 {
			return true
		}
		key := &ref.On.Keys[0]
		if key.Left == nil || key.Right == nil ||
			key.Left.Source != 0 || key.Right.Source != i {
			return true
		}
		// SQL has no spelling for the programmatic JoinKey pseudo-column:
		// quoted "$key" is an ordinary JSON field. Every SQL clause can
		// therefore fan out, so a chain requires the generalized pair space.
		if i > 1 {
			return true
		}
	}
	if tree.Where == nil {
		return false
	}
	terms := []*sqlast.Expr{tree.Where}
	if tree.Where.Kind == sqlast.ExprAnd {
		terms = tree.Where.Kids
	}
	for _, term := range terms {
		source, mixed := exprSource(term)
		if mixed {
			return true
		}
		if source > 0 && tree.From[source].Join == sqlast.JoinLeft {
			return true
		}
	}
	return false
}

func (s *Statement) prepareRelationJoin(argBase int) error {
	if len(s.tree.From) <= 1 {
		return nil
	}
	// The parser exposes correlated occurrences through a cold LateralSpec
	// sidecar. Until this executor has a parameterized APPLY input, compiling
	// those paths as ordinary child-local paths would silently read the wrong
	// values. Refuse before allocating join state or preparing any child. An
	// explicitly LATERAL but decorrelated child is exactly the ordinary derived
	// relation plan and remains safe.
	for i := range s.tree.From {
		ref := &s.tree.From[i]
		if ref.Lateral == nil {
			continue
		}
		if ref.Lateral.Decorrelated && len(ref.Lateral.Bindings) == 0 &&
			len(ref.Lateral.References) == 0 {
			continue
		}
		return sqlast.NewFeatureNotSupportedError(
			s.text, ref.Lateral.Pos,
			"correlated LATERAL execution is not supported yet; remove the outer reference or use a decorrelated derived relation",
		)
	}
	j := new(statementRelationJoin)
	s.ensureNested().relationJoin = j
	j.operands = make([]relationJoinOperand, len(s.tree.From))
	j.sources = make([]relationJoinSource, len(s.tree.From))
	j.stages = make([]relationJoinStage, len(s.tree.From)-1)
	j.lastPairs = make([]uint64, len(j.stages))
	j.lastHash = make([]bool, len(j.stages))
	j.lastBuildRows = make([]uint64, len(j.stages))

	columns := 0
	for i := range s.tree.From {
		ref := &s.tree.From[i]
		op := &j.operands[i]
		op.ref = ref
		op.offset = columns
		switch ref.Kind {
		case sqlast.RelationCollection:
			op.physical = true
			op.names = []string{"*"}
			op.columns = 1
			op.query = Select(Path(""))
			op.cursor.outputs = 1
		case sqlast.RelationDerived:
			if ref.Query == nil {
				return fmt.Errorf("query: joined derived relation %q has no query", ref.Alias)
			}
			child, err := prepareTreeInContext(
				s.text, ref.Query, 0, s.cteCatalog(),
				argBase+ref.Query.ParamBase,
			)
			if err != nil {
				return err
			}
			op.stmt = child
			op.names = append(op.names, child.Columns()...)
			op.columns = len(op.names)
		case sqlast.RelationCTE:
			catalog := s.cteCatalog()
			if catalog == nil {
				return fmt.Errorf("query: CTE reference %q has no lexical definition", ref.Name)
			}
			def := catalog.find(ref.Query)
			if def == nil {
				return fmt.Errorf("query: CTE reference %q is not prepared", ref.Name)
			}
			def.references++
			op.cte = &statementCTEReference{def: def, owner: s}
			if def.firstReference == nil {
				def.firstReference = op.cte
			}
			op.names = def.names
			op.columns = len(op.names)
		default:
			return fmt.Errorf("query: unknown relation kind %d", ref.Kind)
		}
		columns += op.columns
		source := &j.sources[i]
		source.physical = op.physical
		source.names = op.names
		source.offset = op.offset
		source.columns = op.columns
		source.ordinalSpec = j.appendOrdinalSpecs(op.offset, op.columns)

		if i == 0 {
			continue
		}
		cond := ref.On
		if cond != nil && cond.Using {
			for _, name := range cond.UsingColumns {
				j.merged = append(j.merged, relationJoinMerged{
					join: i, name: name, ordinal: columns,
				})
				columns++
			}
		}
		j.stages[i-1] = relationJoinStage{
			ref: ref, index: i, leftColumns: op.offset,
			rightOffset: op.offset, outputColumns: columns,
		}
	}

	for i := range j.stages {
		stage := &j.stages[i]
		cond := stage.ref.On
		if stage.ref.Join == sqlast.JoinCross {
			stage.algorithm = "nested-loop-cross"
			continue
		}
		if cond == nil {
			return fmt.Errorf("query: joined relation %q has no condition", stage.ref.Alias)
		}
		stage.keys = make([]relationJoinKey, len(cond.Keys))
		for k := range cond.Keys {
			left, err := j.preparePath(cond.Keys[k].Left)
			if err != nil {
				return s.positionRelationJoinError(err, cond.Keys[k].Left)
			}
			right, err := j.preparePath(cond.Keys[k].Right)
			if err != nil {
				return s.positionRelationJoinError(err, cond.Keys[k].Right)
			}
			right.column -= stage.rightOffset
			stage.keys[k] = relationJoinKey{left: left, right: right}
		}
		stage.algorithm = "bounded-nested-loop"
		if len(stage.keys) != 0 {
			stage.algorithm = "composite-hash"
		}
		if cond.Using {
			stage.using = make([]relationJoinUsing, len(cond.Keys))
			for k := range cond.Keys {
				merged := j.findMerged(stage.index, cond.UsingColumns[k])
				stage.using[k] = relationJoinUsing{
					name: cond.UsingColumns[k], ordinal: merged,
					left: stage.keys[k].left, right: stage.keys[k].right,
				}
			}
		}
		if cond.Expr != nil {
			if err := j.prepareExpr(cond.Expr, stage); err != nil {
				return err
			}
		}
	}
	return s.validateRelationReferences()
}

func (s *Statement) positionRelationJoinError(err error, path *sqlast.PathExpr) error {
	if column, ok := err.(*RelationColumnError); ok && path != nil {
		column.Pos = path.Pos
		if column.Matches == 0 {
			column.Pos = relationColumnPosition(s.text, path.Pos)
		}
	}
	return err
}

func (j *statementRelationJoin) appendOrdinalSpecs(offset, count int) []string {
	start := len(j.ordinalSpec)
	for ordinal := offset; ordinal < offset+count; ordinal++ {
		mark := len(j.specData)
		j.specData = append(j.specData, '/')
		j.specData = strconv.AppendInt(j.specData, int64(ordinal), 10)
		j.ordinalSpec = append(j.ordinalSpec, byteview.String(
			j.specData[mark:len(j.specData):len(j.specData)],
		))
	}
	return j.ordinalSpec[start:len(j.ordinalSpec)]
}

func (j *statementRelationJoin) findMerged(join int, name string) int {
	for i := range j.merged {
		if j.merged[i].join == join && j.merged[i].name == name {
			return j.merged[i].ordinal
		}
	}
	return -1
}

func (j *statementRelationJoin) sourceBinding(source int) relationBinding {
	if j == nil || source < 0 || source >= len(j.sources) {
		return relationBinding{}
	}
	b := &j.sources[source]
	return relationBinding{names: b.names, ordinalSpec: b.ordinalSpec}
}

func (j *statementRelationJoin) resolve(source int, name, relation string) (int, error) {
	if source < 0 || source >= len(j.sources) {
		return -1, &RelationColumnError{Relation: relation, Column: name}
	}
	b := &j.sources[source]
	if b.physical {
		return b.offset, nil
	}
	found, matches := -1, 0
	for i := range b.names {
		if b.names[i] == name {
			found = b.offset + i
			matches++
		}
	}
	if matches != 1 {
		return -1, &RelationColumnError{
			Relation: relation, Column: name, Matches: matches,
		}
	}
	return found, nil
}

func (j *statementRelationJoin) preparePath(path *sqlast.PathExpr) (relationJoinPath, error) {
	if path == nil {
		return relationJoinPath{}, fmt.Errorf("query: nil generalized join path")
	}
	for i := range j.paths {
		if j.paths[i].ast == path {
			return j.paths[i].path, nil
		}
	}
	prepared := relationJoinPath{source: path.Source}
	segments := path.Segments
	if path.MergedUsing != 0 {
		if len(segments) == 0 {
			return prepared, fmt.Errorf("query: merged USING path has no name")
		}
		prepared.column = j.findMerged(path.MergedUsing, segments[0].Key)
		if prepared.column < 0 {
			return prepared, fmt.Errorf(
				"query: merged USING column %q is not prepared", segments[0].Key,
			)
		}
		segments = segments[1:]
	} else {
		if path.Source < 0 || path.Source >= len(j.sources) {
			return prepared, fmt.Errorf("query: relation source %d is out of range", path.Source)
		}
		binding := &j.sources[path.Source]
		if binding.physical {
			prepared.column = binding.offset
		} else {
			if len(segments) == 0 {
				return prepared, fmt.Errorf("query: relation wildcard is not a scalar join key")
			}
			ordinal, err := j.resolve(path.Source, segments[0].Key, "")
			if err != nil {
				if column, ok := err.(*RelationColumnError); ok {
					column.Relation = "relation"
				}
				return prepared, err
			}
			prepared.column = ordinal
			segments = segments[1:]
		}
	}
	prepared.root = len(segments) == 0
	if !prepared.root {
		var data []byte
		data = appendRelationJoinPointer(data, segments)
		prepared.spec = string(data)
		compiled, err := vibejson.CompilePointer(prepared.spec)
		if err != nil {
			return prepared, err
		}
		prepared.suffix = compiled
	}
	j.paths = append(j.paths, relationJoinPreparedPath{ast: path, path: prepared})
	return prepared, nil
}

func appendRelationJoinPointer(dst []byte, segments []sqlast.Segment) []byte {
	for _, segment := range segments {
		dst = append(dst, '/')
		if segment.IsIndex {
			dst = strconv.AppendInt(dst, int64(segment.Index), 10)
			continue
		}
		for i := 0; i < len(segment.Key); i++ {
			switch segment.Key[i] {
			case '~':
				dst = append(dst, '~', '0')
			case '/':
				dst = append(dst, '~', '1')
			default:
				dst = append(dst, segment.Key[i])
			}
		}
	}
	return dst
}

func (j *statementRelationJoin) prepareExpr(expr *sqlast.Expr, stage *relationJoinStage) error {
	if expr == nil {
		return nil
	}
	if expr.Path != nil {
		if _, err := j.preparePath(expr.Path); err != nil {
			return err
		}
	}
	if expr.RightPath != nil {
		if _, err := j.preparePath(expr.RightPath); err != nil {
			return err
		}
	}
	if expr.Kind == sqlast.ExprContains {
		needle, err := vibejson.ContainsIndex(byteview.Bytes(expr.Value.Text))
		if err != nil {
			return fmt.Errorf("query: invalid ON containment operand: %w", err)
		}
		stage.contains = append(stage.contains, relationJoinContains{
			expr: expr, needle: needle,
		})
	}
	if expr.Kind == sqlast.ExprLike && expr.Value.Kind == sqlast.OperandString {
		if err := validateLikePattern(expr.Value.Text); err != nil {
			return fmt.Errorf("query: invalid LIKE pattern: %w", err)
		}
	}
	for _, kid := range expr.Kids {
		if err := j.prepareExpr(kid, stage); err != nil {
			return err
		}
	}
	return nil
}

func (j *statementRelationJoin) preparedPath(path *sqlast.PathExpr) relationJoinPath {
	for i := range j.paths {
		if j.paths[i].ast == path {
			return j.paths[i].path
		}
	}
	return relationJoinPath{column: -1}
}

func (j *statementRelationJoin) containsNeedle(
	stage *relationJoinStage,
	expr *sqlast.Expr,
) *vibejson.Index {
	for i := range stage.contains {
		if stage.contains[i].expr == expr {
			return &stage.contains[i].needle
		}
	}
	return nil
}

func (j *statementRelationJoin) run(
	owner *Statement,
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
) (Source, error) {
	for i := range j.lastPairs {
		j.lastPairs[i] = 0
		j.lastHash[i] = false
		j.lastBuildRows[i] = 0
	}
	for i := range j.operands {
		if err := j.materializeOperand(owner, parent, src, args, frame, i); err != nil {
			j.releaseExecution(frame)
			return Source{}, err
		}
	}
	left := j.operands[0].bound
	leftSlot := -1
	for i := range j.stages {
		target := i & 1
		if j.activeBytes[target] != 0 {
			frame.intermediate.release(j.activeBytes[target])
			j.activeBytes[target] = 0
		}
		j.spools[target].reset()
		charge, pairs, hashed, err := j.runStage(
			&j.stages[i], left, j.operands[i+1].bound,
			&j.spools[target], parent.Options, args, frame,
		)
		if err != nil {
			j.releaseExecution(frame)
			return Source{}, err
		}
		j.activeBytes[target] = charge
		j.lastPairs[i] = uint64(pairs)
		j.lastHash[i] = hashed
		if j.stages[i].ref.Join == sqlast.JoinRight {
			j.lastBuildRows[i] = uint64(left.rows)
		} else {
			j.lastBuildRows[i] = uint64(j.operands[i+1].boundRows())
		}
		if leftSlot >= 0 && leftSlot != target {
			frame.intermediate.release(j.activeBytes[leftSlot])
			j.activeBytes[leftSlot] = 0
			j.spools[leftSlot].reset()
		}
		left = &j.spools[target]
		leftSlot = target
	}
	j.final = left
	j.publishStats(left)
	return fromRelationSpool(left), nil
}

func (j *statementRelationJoin) materializeOperand(
	owner *Statement,
	parent *Exec,
	src Source,
	args []any,
	frame *statementFrame,
	index int,
) error {
	op := &j.operands[index]
	op.bound = nil
	op.spool.reset()
	op.active = 0
	if err := cancellationError(parent.Options.Cancel); err != nil {
		return err
	}
	if op.cte != nil {
		ref := op.cte
		if ref.mode() == cteSharedMaterialized {
			if err := ref.def.ensureMaterialized(parent, src, owner, frame); err != nil {
				return err
			}
			op.bound = &ref.def.spool
			return nil
		}
		charge, err := ref.def.materializeInto(
			parent, src, owner, frame, &op.spool,
			"joined CTE reference-local spool",
		)
		if err != nil {
			return err
		}
		op.active = charge
		ref.activeBytes = charge
		op.bound = &op.spool
		return nil
	}

	var cursor Cursor
	if op.physical {
		nestedSource, err := src.subquerySource(owner.Collection(), op.ref.Name)
		if err != nil {
			return err
		}
		op.exec.Options = parent.Options
		op.exec.Options.ResultRows = -1
		op.exec.Options.ResultBytes = frame.intermediate.remaining()
		if op.exec.Options.ResultBytes == 0 {
			return &IntermediateBudgetError{
				Resource: "joined physical relation result",
				Bytes:    saturatedBytes(frame.intermediate.used, 1),
				Limit:    frame.intermediate.limit,
			}
		}
		if err := op.query.RunInto(&op.exec, nestedSource); err != nil {
			var resultErr *ResultBudgetError
			if errorsAsResultBudget(err, &resultErr, op.exec.Options.ResultBytes) {
				return &IntermediateBudgetError{
					Resource: "joined physical relation result",
					Bytes:    saturatedBytes(frame.intermediate.used, resultErr.Bytes),
					Limit:    frame.intermediate.limit,
				}
			}
			return err
		}
		cursor = op.cursor.cursor(&op.exec.Result)
	} else {
		n := op.stmt.NumParams()
		base := op.ref.Query.ParamBase
		if base < 0 || base+n > len(args) {
			return fmt.Errorf("query: invalid joined derived placeholder range")
		}
		nestedSource, err := src.subquerySource(owner.Collection(), op.stmt.Collection())
		if err != nil {
			return err
		}
		op.exec.Options = parent.Options
		cursor, err = op.stmt.runIntoFrame(
			&op.exec, nestedSource, args[base:base+n], frame,
			"joined derived query result",
		)
		if err != nil {
			return err
		}
	}

	resultBytes := op.exec.Result.resultBytesUsed
	if err := frame.intermediate.reserve("joined operand result", resultBytes); err != nil {
		return err
	}
	charge, err := op.spool.materialize(
		cursor, op.columns, frame, parent.Options.Cancel,
		"joined operand spool",
	)
	frame.intermediate.release(resultBytes)
	clearExecBorrowedViews(&op.exec)
	if op.stmt != nil {
		op.stmt.releaseRelations(frame)
	}
	if err != nil {
		return err
	}
	op.active = charge
	op.bound = &op.spool
	return nil
}

func errorsAsResultBudget(
	err error,
	target **ResultBudgetError,
	limit int64,
) bool {
	if !errors.As(err, target) {
		return false
	}
	return (*target).ByteLimit == limit
}

const relationJoinEmpty = int32(-1)

func (j *statementRelationJoin) runStage(
	stage *relationJoinStage,
	left *relationSpool,
	right *relationSpool,
	out *relationSpool,
	options ExecOptions,
	args []any,
	frame *statementFrame,
) (charge int64, pairs int, hashed bool, err error) {
	if left == nil || right == nil {
		return 0, 0, false, fmt.Errorf("query: generalized join has an unbound operand")
	}
	pairLimit, err := normalizeJoinPairBytes(options)
	if err != nil {
		return 0, 0, false, err
	}
	buildRows := right.rows
	buildRight := true
	if stage.ref.Join == sqlast.JoinRight {
		buildRows = left.rows
		buildRight = false
	}
	baseBytes, err := j.prepareStageWorkspace(
		stage, left, right, buildRows, buildRight, pairLimit, options.Cancel,
	)
	if err != nil {
		return 0, 0, false, err
	}
	hashed = len(stage.keys) != 0
	var payload int64
	pairs, err = j.walkStage(
		stage, left, right, nil, args, options.Cancel,
		pairLimit, baseBytes, &payload,
	)
	if err != nil {
		return 0, 0, hashed, err
	}
	charge = relationSpoolRetainedBytes(pairs, stage.outputColumns, payload)
	if charge == math.MaxInt64 {
		return 0, 0, hashed, &IntermediateBudgetError{
			Resource: "generalized join relation",
			Bytes:    math.MaxInt64,
			Limit:    frame.intermediate.limit,
		}
	}
	if err := frame.intermediate.reserve("generalized join relation", charge); err != nil {
		return 0, 0, hashed, err
	}
	if err := cancellationError(options.Cancel); err != nil {
		frame.intermediate.release(charge)
		return 0, 0, hashed, err
	}
	if err := out.begin(pairs, stage.outputColumns, payload); err != nil {
		frame.intermediate.release(charge)
		return 0, 0, hashed, err
	}
	filled, err := j.walkStage(
		stage, left, right, out, args, options.Cancel,
		pairLimit, baseBytes, nil,
	)
	if err != nil || filled != pairs {
		out.reset()
		frame.intermediate.release(charge)
		if err != nil {
			return 0, 0, hashed, err
		}
		return 0, 0, hashed, fmt.Errorf(
			"query: generalized join changed between sizing and publication",
		)
	}
	return charge, pairs, hashed, nil
}

func (j *statementRelationJoin) prepareStageWorkspace(
	stage *relationJoinStage,
	left, right *relationSpool,
	buildRows int,
	buildRight bool,
	limit int64,
	cancel *CancelFlag,
) (int64, error) {
	matchedBytes := int64(0)
	if stage.ref.Join == sqlast.JoinFull {
		matchedBytes = saturatedProduct(int64(right.rows), int64(unsafe.Sizeof(uint32(0))))
	}
	base := matchedBytes
	if len(stage.keys) != 0 {
		bucketCount, err := joinBuildBuckets(buildRows)
		if err != nil {
			return 0, err
		}
		base = saturatedBytes(base,
			saturatedProduct(int64(buildRows), int64(unsafe.Sizeof(uint64(0))+unsafe.Sizeof(int32(0)))))
		base = saturatedBytes(base,
			saturatedProduct(int64(bucketCount), int64(unsafe.Sizeof(int32(0)))))
		if limit >= 0 && base > limit {
			return 0, &JoinPairBudgetError{Bytes: base, Limit: limit}
		}
		if !j.seeded {
			j.seed = maphash.MakeSeed()
			j.seeded = true
		}
		j.hashes = resize(j.hashes, buildRows)
		j.next = resize(j.next, buildRows)
		if cap(j.buckets) < bucketCount {
			j.buckets = make([]int32, bucketCount)
		} else {
			j.buckets = j.buckets[:bucketCount]
		}
		for i := range j.buckets {
			j.buckets[i] = relationJoinEmpty
		}
		for row := 0; row < buildRows; row++ {
			if err := cancellationCheckpoint(cancel, row); err != nil {
				return 0, err
			}
			hash, known, err := j.hashStageRow(
				stage, left, right, row, buildRight,
			)
			if err != nil {
				return 0, err
			}
			if !known {
				j.next[row] = relationJoinEmpty
				j.hashes[row] = 0
				continue
			}
			j.hashes[row] = hash
			j.next[row] = 0
		}
		mask := uint64(bucketCount - 1)
		for row := buildRows - 1; row >= 0; row-- {
			if j.next[row] == relationJoinEmpty {
				continue
			}
			slot := j.hashes[row] & mask
			j.next[row] = j.buckets[slot]
			j.buckets[slot] = int32(row)
		}
	} else if limit >= 0 && base > limit {
		return 0, &JoinPairBudgetError{Bytes: base, Limit: limit}
	}
	if stage.ref.Join == sqlast.JoinFull {
		if cap(j.matched) < right.rows {
			j.matched = make([]uint32, right.rows)
		} else {
			j.matched = j.matched[:right.rows]
		}
		j.epoch++
		if j.epoch == 0 {
			clear(j.matched)
			j.epoch = 1
		}
	}
	return base, cancellationError(cancel)
}

func (j *statementRelationJoin) hashStageRow(
	stage *relationJoinStage,
	left, right *relationSpool,
	row int,
	buildRight bool,
) (uint64, bool, error) {
	hash := uint64(0x9e3779b97f4a7c15)
	for i := range stage.keys {
		path := stage.keys[i].right
		spool := right
		if !buildRight {
			path = stage.keys[i].left
			spool = left
		}
		j.rightText = j.rightText[:0]
		value, err := relationJoinPathScalar(spool, row, path, &j.rightText)
		if err != nil {
			return 0, false, err
		}
		if value.kind == kindNull {
			return 0, false, nil
		}
		part := hashJoinValue(j.seed, value)
		hash ^= bits.RotateLeft64(part+uint64(i)*0x517cc1b727220a95, (i*17+11)&63)
		hash *= 0x9ddfea08eb382d69
	}
	return hash, true, nil
}

func relationJoinPathScalar(
	spool *relationSpool,
	row int,
	path relationJoinPath,
	text *[]byte,
) (scalar, error) {
	if spool == nil || path.column < 0 || path.column >= len(spool.columns) ||
		row < 0 || row >= spool.rows {
		return scalar{kind: kindNull}, nil
	}
	root := spool.columns[path.column][row]
	if path.root {
		return root, nil
	}
	raw := root.raw
	if len(raw) == 0 {
		return scalar{kind: kindNull}, nil
	}
	resolved, ok, err := path.suffix.GetRawTrusted(raw)
	if err != nil {
		return scalar{}, err
	}
	if !ok {
		return scalar{kind: kindNull}, nil
	}
	return classifyRawInto(resolved, text), nil
}

func (j *statementRelationJoin) probeHash(
	stage *relationJoinStage,
	left, right *relationSpool,
	lrow, rrow int,
	probeRight bool,
) (uint64, bool, error) {
	row := rrow
	if !probeRight {
		row = lrow
	}
	return j.hashStageRow(stage, left, right, row, probeRight)
}

func (j *statementRelationJoin) keysMatch(
	stage *relationJoinStage,
	left, right *relationSpool,
	lrow, rrow int,
) (bool, error) {
	for i := range stage.keys {
		j.leftText = j.leftText[:0]
		lv, err := relationJoinPathScalar(
			left, lrow, stage.keys[i].left, &j.leftText,
		)
		if err != nil {
			return false, err
		}
		j.rightText = j.rightText[:0]
		rv, err := relationJoinPathScalar(
			right, rrow, stage.keys[i].right, &j.rightText,
		)
		if err != nil {
			return false, err
		}
		if lv.kind == kindNull || rv.kind == kindNull || compareScalar(lv, rv) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (j *statementRelationJoin) walkStage(
	stage *relationJoinStage,
	left, right *relationSpool,
	out *relationSpool,
	args []any,
	cancel *CancelFlag,
	pairLimit int64,
	baseBytes int64,
	payload *int64,
) (int, error) {
	pairs := 0
	emit := func(lrow, rrow int) error {
		next, err := relationJoinNextPair(baseBytes, pairs, pairLimit)
		if err != nil {
			return err
		}
		if out != nil {
			if err := j.writePair(stage, left, right, out, pairs, lrow, rrow); err != nil {
				return err
			}
		} else if payload != nil {
			for i := range stage.using {
				value, err := j.usingValue(stage, left, right, i, lrow, rrow)
				if err != nil {
					return err
				}
				if relationJoinOwnsDecodedText(value) {
					*payload = saturatedBytes(*payload, int64(len(value.sval)))
				}
			}
		}
		pairs = next
		return nil
	}

	match := func(lrow, rrow int) (bool, error) {
		if len(stage.keys) != 0 {
			matched, err := j.keysMatch(stage, left, right, lrow, rrow)
			if err != nil || !matched {
				return matched, err
			}
		}
		cond := stage.ref.On
		if cond == nil || cond.Expr == nil || !cond.Residual {
			return true, nil
		}
		value, err := j.evalJoinExpr(stage, cond.Expr, left, right, lrow, rrow, args)
		return value == triTrue, err
	}

	if stage.ref.Join == sqlast.JoinRight {
		for rrow := 0; rrow < right.rows; rrow++ {
			if err := cancellationCheckpoint(cancel, rrow); err != nil {
				return pairs, err
			}
			found := false
			if len(stage.keys) == 0 {
				for lrow := 0; lrow < left.rows; lrow++ {
					ok, err := match(lrow, rrow)
					if err != nil {
						return pairs, err
					}
					if !ok {
						continue
					}
					found = true
					if err := emit(lrow, rrow); err != nil {
						return pairs, err
					}
				}
			} else {
				hash, known, err := j.probeHash(stage, left, right, -1, rrow, true)
				if err != nil {
					return pairs, err
				}
				if known {
					for at := j.buckets[hash&uint64(len(j.buckets)-1)]; at != relationJoinEmpty; at = j.next[at] {
						if j.hashes[at] != hash {
							continue
						}
						ok, err := match(int(at), rrow)
						if err != nil {
							return pairs, err
						}
						if !ok {
							continue
						}
						found = true
						if err := emit(int(at), rrow); err != nil {
							return pairs, err
						}
					}
				}
			}
			if !found {
				if err := emit(-1, rrow); err != nil {
					return pairs, err
				}
			}
		}
		return pairs, cancellationError(cancel)
	}

	for lrow := 0; lrow < left.rows; lrow++ {
		if err := cancellationCheckpoint(cancel, lrow); err != nil {
			return pairs, err
		}
		found := false
		if len(stage.keys) == 0 {
			for rrow := 0; rrow < right.rows; rrow++ {
				ok, err := match(lrow, rrow)
				if err != nil {
					return pairs, err
				}
				if !ok {
					continue
				}
				found = true
				if stage.ref.Join == sqlast.JoinFull && out == nil {
					j.matched[rrow] = j.epoch
				}
				if err := emit(lrow, rrow); err != nil {
					return pairs, err
				}
			}
		} else {
			hash, known, err := j.probeHash(stage, left, right, lrow, -1, false)
			if err != nil {
				return pairs, err
			}
			if known {
				for at := j.buckets[hash&uint64(len(j.buckets)-1)]; at != relationJoinEmpty; at = j.next[at] {
					if j.hashes[at] != hash {
						continue
					}
					ok, err := match(lrow, int(at))
					if err != nil {
						return pairs, err
					}
					if !ok {
						continue
					}
					found = true
					if stage.ref.Join == sqlast.JoinFull && out == nil {
						j.matched[at] = j.epoch
					}
					if err := emit(lrow, int(at)); err != nil {
						return pairs, err
					}
				}
			}
		}
		if !found && (stage.ref.Join == sqlast.JoinLeft || stage.ref.Join == sqlast.JoinFull) {
			if err := emit(lrow, -1); err != nil {
				return pairs, err
			}
		}
	}
	if stage.ref.Join == sqlast.JoinFull {
		for rrow := 0; rrow < right.rows; rrow++ {
			if j.matched[rrow] == j.epoch {
				continue
			}
			if err := emit(-1, rrow); err != nil {
				return pairs, err
			}
		}
	}
	return pairs, cancellationError(cancel)
}

func relationJoinPairBytes(base int64, pairs int) int64 {
	perPair := int64(unsafe.Sizeof(int(0))) * 2
	return saturatedBytes(base, saturatedProduct(int64(pairs), perPair))
}

func relationJoinNextPair(base int64, pairs int, limit int64) (int, error) {
	if pairs == math.MaxInt {
		return pairs, &JoinPairBudgetError{
			Pairs: math.MaxInt, Bytes: math.MaxInt64, Limit: limit,
		}
	}
	next := pairs + 1
	bytes := relationJoinPairBytes(base, next)
	if limit >= 0 && bytes > limit {
		return pairs, &JoinPairBudgetError{
			Pairs: next, Bytes: bytes, Limit: limit,
		}
	}
	return next, nil
}

func (j *statementRelationJoin) writePair(
	stage *relationJoinStage,
	left, right, out *relationSpool,
	row, lrow, rrow int,
) error {
	for column := 0; column < stage.leftColumns; column++ {
		if lrow < 0 {
			out.columns[column][row] = scalar{kind: kindNull, raw: nullBytes}
		} else {
			out.columns[column][row] = left.columns[column][lrow]
		}
	}
	for column := 0; column < len(right.columns); column++ {
		if rrow < 0 {
			out.columns[stage.rightOffset+column][row] = scalar{kind: kindNull, raw: nullBytes}
		} else {
			out.columns[stage.rightOffset+column][row] = right.columns[column][rrow]
		}
	}
	for i := range stage.using {
		value, err := j.usingValue(stage, left, right, i, lrow, rrow)
		if err != nil {
			return err
		}
		if relationJoinOwnsDecodedText(value) {
			start := len(out.data)
			if err := out.appendOwnedBytes(byteview.Bytes(value.sval), nil); err != nil {
				return err
			}
			value.sval = byteview.String(out.data[start:len(out.data):len(out.data)])
		}
		out.columns[stage.using[i].ordinal][row] = value
	}
	return nil
}

func (j *statementRelationJoin) usingValue(
	stage *relationJoinStage,
	left, right *relationSpool,
	using, lrow, rrow int,
) (scalar, error) {
	var lv, rv scalar
	var err error
	if lrow >= 0 {
		j.leftText = j.leftText[:0]
		lv, err = relationJoinPathScalar(
			left, lrow, stage.using[using].left, &j.leftText,
		)
		if err != nil {
			return scalar{}, err
		}
	}
	if rrow >= 0 {
		j.rightText = j.rightText[:0]
		rv, err = relationJoinPathScalar(
			right, rrow, stage.using[using].right, &j.rightText,
		)
		if err != nil {
			return scalar{}, err
		}
	}
	if lv.kind != kindNull {
		return lv, nil
	}
	if rv.kind != kindNull {
		return rv, nil
	}
	// A USING output is a declared SQL column. COALESCE(NULL, NULL) is
	// explicit NULL even when either document omitted its key.
	return scalar{kind: kindNull, raw: nullBytes}, nil
}

func relationJoinOwnsDecodedText(value scalar) bool {
	if value.kind != kindString {
		return false
	}
	for _, b := range value.raw {
		if b == '\\' {
			return true
		}
	}
	return false
}

func (j *statementRelationJoin) evalJoinExpr(
	stage *relationJoinStage,
	expr *sqlast.Expr,
	left, right *relationSpool,
	lrow, rrow int,
	args []any,
) (tri, error) {
	switch expr.Kind {
	case sqlast.ExprConstant:
		if expr.Value.Kind != sqlast.OperandBool {
			return triFalse, fmt.Errorf("query: ON constant must be boolean")
		}
		return boolTri(expr.Value.Bool), nil
	case sqlast.ExprAnd:
		out := triTrue
		for _, kid := range expr.Kids {
			value, err := j.evalJoinExpr(stage, kid, left, right, lrow, rrow, args)
			if err != nil {
				return triFalse, err
			}
			if value == triFalse {
				return triFalse, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprOr:
		out := triFalse
		for _, kid := range expr.Kids {
			value, err := j.evalJoinExpr(stage, kid, left, right, lrow, rrow, args)
			if err != nil {
				return triFalse, err
			}
			if value == triTrue {
				return triTrue, nil
			}
			if value == triUnknown {
				out = triUnknown
			}
		}
		return out, nil
	case sqlast.ExprNot:
		value, err := j.evalJoinExpr(stage, expr.Kids[0], left, right, lrow, rrow, args)
		return notTri(value), err
	}

	cell, err := j.joinExprPath(stage, expr.Path, left, right, lrow, rrow, &j.leftText)
	if err != nil {
		return triFalse, err
	}
	var value tri
	switch expr.Kind {
	case sqlast.ExprCompare:
		if expr.RightPath != nil {
			rightCell, err := j.joinExprPath(
				stage, expr.RightPath, left, right, lrow, rrow, &j.rightText,
			)
			if err != nil {
				return triFalse, err
			}
			if cell.kind == kindNull || rightCell.kind == kindNull {
				value = triUnknown
			} else {
				value = boolTri(acceptSign(compareScalar(cell, rightCell), Op(expr.Op)))
			}
		} else {
			lit, known, err := j.joinOperand(expr.Value, args)
			if err != nil {
				return triFalse, err
			}
			value = compareTri(cell, havingLit{value: lit, known: known}, Op(expr.Op))
		}
	case sqlast.ExprIsNull:
		value = boolTri(cell.kind == kindNull)
	case sqlast.ExprIsMissing:
		value = boolTri(cell.kind == kindNull && len(cell.raw) == 0)
	case sqlast.ExprBetween:
		lo, known, err := j.joinOperand(expr.List[0], args)
		if err != nil {
			return triFalse, err
		}
		lower := compareTri(cell, havingLit{value: lo, known: known}, Ge)
		hi, known, err := j.joinOperand(expr.List[1], args)
		if err != nil {
			return triFalse, err
		}
		upper := compareTri(cell, havingLit{value: hi, known: known}, Le)
		value = andTri(lower, upper)
	case sqlast.ExprIn:
		if cell.kind == kindNull {
			value = triUnknown
			break
		}
		value = triFalse
		for _, operand := range expr.List {
			lit, known, err := j.joinOperand(operand, args)
			if err != nil {
				return triFalse, err
			}
			if !known {
				if value == triFalse {
					value = triUnknown
				}
				continue
			}
			if compareScalar(cell, lit) == 0 {
				value = triTrue
				break
			}
		}
	case sqlast.ExprLike:
		lit, known, err := j.joinOperand(expr.Value, args)
		if err != nil {
			return triFalse, err
		}
		if !known || cell.kind == kindNull {
			value = triUnknown
			break
		}
		if lit.kind != kindString {
			return triFalse, fmt.Errorf("query: ON LIKE pattern must be a string")
		}
		if err := validateLikePattern(lit.sval); err != nil {
			return triFalse, fmt.Errorf("query: invalid LIKE pattern: %w", err)
		}
		value = boolTri(cell.kind == kindString && likeMatch(lit.sval, cell.sval, expr.Insensitive))
	case sqlast.ExprContains:
		if cell.kind == kindNull {
			value = triUnknown
			break
		}
		needle := j.containsNeedle(stage, expr)
		if needle == nil {
			return triFalse, fmt.Errorf("query: ON containment needle is not prepared")
		}
		haystack, err := j.eval.containsTape(cell.raw)
		if err != nil {
			return triFalse, err
		}
		value = boolTri(haystack.Root().Contains(needle.Root()))
	default:
		return triFalse, fmt.Errorf("query: unsupported ON expression kind %d", expr.Kind)
	}
	if expr.Negated {
		value = notTri(value)
	}
	return value, nil
}

func (j *statementRelationJoin) joinExprPath(
	stage *relationJoinStage,
	path *sqlast.PathExpr,
	left, right *relationSpool,
	lrow, rrow int,
	text *[]byte,
) (scalar, error) {
	*text = (*text)[:0]
	prepared := j.preparedPath(path)
	if prepared.column < 0 {
		return scalar{}, fmt.Errorf("query: ON path is not prepared")
	}
	if path.MergedUsing == 0 && prepared.source == stage.index {
		prepared.column -= stage.rightOffset
		return relationJoinPathScalar(right, rrow, prepared, text)
	}
	return relationJoinPathScalar(left, lrow, prepared, text)
}

func (j *statementRelationJoin) joinOperand(
	operand sqlast.Operand,
	args []any,
) (scalar, bool, error) {
	j.literalText = j.literalText[:0]
	switch operand.Kind {
	case sqlast.OperandString:
		return scalar{kind: kindString, sval: operand.Text}, true, nil
	case sqlast.OperandNumber:
		return joinNumberScalar(byteview.Bytes(operand.Text)), true, nil
	case sqlast.OperandBool:
		return scalar{kind: kindBool, bval: operand.Bool}, true, nil
	case sqlast.OperandParam:
		if operand.Ordinal < 0 || operand.Ordinal >= len(args) {
			return scalar{}, false, fmt.Errorf("query: ON placeholder ordinal is out of range")
		}
		return j.joinArgument(args[operand.Ordinal])
	default:
		return scalar{}, false, fmt.Errorf("query: invalid ON operand")
	}
}

func joinNumberScalar(number []byte) scalar {
	value := scalar{kind: kindNumber, num: number, raw: number}
	if integer, ok := int64Spelling(byteview.String(number)); ok {
		value.isInt, value.ival = true, integer
	}
	return value
}

func (j *statementRelationJoin) joinArgument(arg any) (scalar, bool, error) {
	appendInt := func(value int64) scalar {
		j.literalText = strconv.AppendInt(j.literalText[:0], value, 10)
		out := joinNumberScalar(j.literalText)
		out.isInt, out.ival = true, value
		return out
	}
	appendUint := func(value uint64) scalar {
		j.literalText = strconv.AppendUint(j.literalText[:0], value, 10)
		return joinNumberScalar(j.literalText)
	}
	appendFloat := func(value float64, bits int) (scalar, bool, error) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return scalar{}, false, fmt.Errorf("query: non-finite ON numeric literal %v", value)
		}
		j.literalText = strconv.AppendFloat(j.literalText[:0], value, 'g', -1, bits)
		return joinNumberScalar(j.literalText), true, nil
	}
	switch value := arg.(type) {
	case nil:
		return scalar{}, false, nil
	case bool:
		return scalar{kind: kindBool, bval: value}, true, nil
	case *bool:
		if value == nil {
			return scalar{}, false, nil
		}
		return scalar{kind: kindBool, bval: *value}, true, nil
	case string:
		return scalar{kind: kindString, sval: value}, true, nil
	case *string:
		if value == nil {
			return scalar{}, false, nil
		}
		return scalar{kind: kindString, sval: *value}, true, nil
	case []byte:
		return scalar{kind: kindString, sval: byteview.String(value)}, true, nil
	case Number:
		if err := value.validate(); err != nil {
			return scalar{}, false, fmt.Errorf("query: ON literal: %w", err)
		}
		return joinNumberScalar(byteview.Bytes(string(value))), true, nil
	case *Number:
		if value == nil {
			return scalar{}, false, nil
		}
		if err := value.validate(); err != nil {
			return scalar{}, false, fmt.Errorf("query: ON literal: %w", err)
		}
		return joinNumberScalar(byteview.Bytes(string(*value))), true, nil
	case int:
		return appendInt(int64(value)), true, nil
	case int8:
		return appendInt(int64(value)), true, nil
	case int16:
		return appendInt(int64(value)), true, nil
	case int32:
		return appendInt(int64(value)), true, nil
	case int64:
		return appendInt(value), true, nil
	case *int64:
		if value == nil {
			return scalar{}, false, nil
		}
		return appendInt(*value), true, nil
	case uint:
		return appendUint(uint64(value)), true, nil
	case uint8:
		return appendInt(int64(value)), true, nil
	case uint16:
		return appendInt(int64(value)), true, nil
	case uint32:
		return appendInt(int64(value)), true, nil
	case uint64:
		return appendUint(value), true, nil
	case float32:
		return appendFloat(float64(value), 32)
	case float64:
		return appendFloat(value, 64)
	case *float64:
		if value == nil {
			return scalar{}, false, nil
		}
		return appendFloat(*value, 64)
	default:
		return scalar{}, false, fmt.Errorf(
			"query: cannot bind %T as an ON literal", arg,
		)
	}
}

func (j *statementRelationJoin) releaseExecution(frame *statementFrame) {
	if j == nil {
		return
	}
	for i := range j.operands {
		op := &j.operands[i]
		clearExecBorrowedViews(&op.exec)
		if op.stmt != nil {
			op.stmt.releaseRelations(frame)
		}
		if op.cte != nil && op.cte.mode() != cteSharedMaterialized {
			op.cte.spool.reset()
			op.cte.activeBytes = 0
		}
		op.spool.reset()
		frame.intermediate.release(op.active)
		op.active = 0
		op.bound = nil
	}
	for i := range j.spools {
		j.spools[i].reset()
		frame.intermediate.release(j.activeBytes[i])
		j.activeBytes[i] = 0
	}
	j.final = nil
}

func (j *statementRelationJoin) discardExecution() {
	if j == nil {
		return
	}
	for i := range j.operands {
		op := &j.operands[i]
		clearExecBorrowedViews(&op.exec)
		if op.stmt != nil {
			op.stmt.discardRelations()
		}
		if op.cte != nil {
			op.cte.spool.reset()
			op.cte.activeBytes = 0
		}
		op.spool.reset()
		op.active = 0
		op.bound = nil
	}
	for i := range j.spools {
		j.spools[i].reset()
		j.activeBytes[i] = 0
	}
	j.final = nil
}

func (j *statementRelationJoin) release() {
	if j == nil {
		return
	}
	for i := range j.operands {
		op := &j.operands[i]
		if op.stmt != nil {
			op.stmt.Release()
		}
		op.exec.Release()
		op.spool.release()
	}
	for i := range j.spools {
		j.spools[i].release()
	}
	*j = statementRelationJoin{}
}

func (j *statementRelationJoin) publishStats(spool *relationSpool) {
	if j == nil || spool == nil {
		return
	}
	for i, pairs := range j.lastPairs {
		spool.joinStats.pairs += pairs
		if j.lastHash[i] {
			spool.joinStats.builds++
			spool.joinStats.buildRows += j.lastBuildRows[i]
		}
	}
}

func (s *Statement) explainRelationJoins(analyze bool) []explainJoin {
	j := s.relationJoin()
	if j == nil {
		return nil
	}
	result := make([]explainJoin, 0, len(j.stages))
	for i := range j.stages {
		stage := &j.stages[i]
		ref := stage.ref
		collection := ref.Name
		if collection == "" {
			collection = ref.Alias
		}
		item := explainJoin{
			Collection: collection,
			Type:       relationJoinTypeName(ref.Join),
			AccessPath: "relation-spool",
			Algorithm:  stage.algorithm,
			KeyCount:   len(stage.keys),
			Cross:      ref.Join == sqlast.JoinCross,
		}
		if len(stage.keys) != 0 {
			item.BuildSide = "right"
			if ref.Join == sqlast.JoinRight {
				item.BuildSide = "left"
			}
		}
		if ref.On != nil {
			item.Residual = ref.On.Residual
			item.Using = append(item.Using, ref.On.UsingColumns...)
			for k := range ref.On.Keys {
				item.Keys = append(item.Keys, explainJoinKey{
					Left:  s.explainRelationJoinPath(ref.On.Keys[k].Left),
					Right: s.explainRelationJoinPath(ref.On.Keys[k].Right),
				})
			}
		}
		if analyze {
			item.ActualAlgorithm = stage.algorithm
			item.Pairs = &j.lastPairs[i]
		}
		result = append(result, item)
	}
	return result
}

func (s *Statement) explainRelationJoinPath(path *sqlast.PathExpr) string {
	if path == nil {
		return ""
	}
	spec := path.Spec()
	if path.MergedUsing != 0 {
		return "USING(" + spec + ")"
	}
	if path.Source >= 0 && path.Source < len(s.tree.From) {
		alias := s.tree.From[path.Source].Alias
		if spec == "" {
			return alias + ".*"
		}
		return alias + "." + spec
	}
	return spec
}

func relationJoinTypeName(kind sqlast.JoinKind) string {
	switch kind {
	case sqlast.JoinLeft:
		return "left"
	case sqlast.JoinRight:
		return "right"
	case sqlast.JoinFull:
		return "full"
	case sqlast.JoinCross:
		return "cross"
	default:
		return "inner"
	}
}

func (o *relationJoinOperand) boundRows() int {
	if o == nil || o.bound == nil {
		return 0
	}
	return o.bound.rows
}

func (j *statementRelationJoin) drivingCollection() string {
	if j == nil {
		return ""
	}
	for i := range j.operands {
		op := &j.operands[i]
		switch {
		case op.physical:
			return op.ref.Name
		case op.stmt != nil && op.stmt.Collection() != "":
			return op.stmt.Collection()
		case op.cte != nil && op.cte.def != nil && op.cte.def.stmt != nil &&
			op.cte.def.stmt.Collection() != "":
			return op.cte.def.stmt.Collection()
		}
	}
	return ""
}
