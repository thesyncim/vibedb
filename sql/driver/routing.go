package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
)

// placementBinding is the runtime routing context for one placed table: its
// static placement, the distribution spec, the mapper and manifest that route
// the distribution, the admission policy, and the compiled shard-key pointers
// that extract post-coercion values from a document. It is immutable after
// construction and safe to share; each execution supplies its own Router.
type placementBinding struct {
	placement distribution.TablePlacement
	spec      distribution.DistributionSpec
	mapper    distribution.Mapper
	manifest  *distribution.Manifest
	policy    distribution.RoutePolicy
	pointers  []vibejson.CompiledPointer
}

// newPlacementBinding assembles the runtime binding for table from a validated
// cluster configuration, reporting false when the table is unplaced. It compiles
// one shard-key pointer per placement column and selects the frozen native
// mapper for the distribution's arity. The zero-value RoutePolicy is fail-closed
// (targeted routes only), the correct default for version 1 single-shard writes.
func newPlacementBinding(cfg distribution.ClusterConfig, table string) (*placementBinding, bool, error) {
	placement, ok := cfg.Placement(table)
	if !ok {
		return nil, false, nil
	}
	spec, ok := cfg.Spec(placement.Distribution)
	if !ok {
		return nil, false, fmt.Errorf(
			"vibedb: table %q references unknown distribution %q", table, placement.Distribution)
	}
	manifest, ok := cfg.Manifest(placement.Distribution)
	if !ok {
		return nil, false, fmt.Errorf(
			"vibedb: distribution %q has no manifest", placement.Distribution)
	}
	if spec.MapperVersion != distribution.NativeMapperVersion {
		return nil, false, fmt.Errorf(
			"vibedb: distribution %q requires unsupported mapper version %d",
			placement.Distribution, spec.MapperVersion)
	}
	if spec.Arity < 1 || spec.Arity > distribution.KeyspaceWidth {
		return nil, false, fmt.Errorf(
			"vibedb: distribution %q has arity %d outside 1..%d",
			placement.Distribution, spec.Arity, distribution.KeyspaceWidth)
	}
	if len(placement.Columns) != spec.Arity {
		return nil, false, fmt.Errorf(
			"vibedb: table %q lists %d shard-key columns but distribution %q has arity %d",
			table, len(placement.Columns), placement.Distribution, spec.Arity)
	}
	pointers := make([]vibejson.CompiledPointer, len(placement.Columns))
	for i, col := range placement.Columns {
		p, err := vibejson.CompilePointer(col)
		if err != nil {
			return nil, false, fmt.Errorf("vibedb: compile shard-key path %q: %w", col, err)
		}
		pointers[i] = p
	}
	return &placementBinding{
		placement: placement,
		spec:      spec,
		mapper:    distribution.NewNativeMapper(spec.Arity),
		manifest:  manifest,
		pointers:  pointers,
	}, true, nil
}

// validateShardKeyLocality enforces the uniqueness-locality invariant for a
// placed table: every unique or primary key must contain every shard-key column,
// so per-shard uniqueness remains global uniqueness. This dialect's only unique
// key is the single-path primary key (UNIQUE indexes are refused and non-unique
// indexes enforce no uniqueness), so the check reduces to the primary key
// containing every shard-key column. Pointer spellings are compared directly,
// matching the driver's column identity. It returns ErrShardKeyNotLocal on
// violation, failing closed.
func validateShardKeyLocality(table string, shardColumns []string, primaryKey string) error {
	if keyContainsShardColumns([]string{primaryKey}, shardColumns) {
		return nil
	}
	return fmt.Errorf(
		"%w: table %q primary key %q does not contain shard-key columns %v",
		ErrShardKeyNotLocal, table, primaryKey, shardColumns)
}

// keyContainsShardColumns reports whether every shard-key column appears in the
// unique key's column list, both as JSON-pointer spellings.
func keyContainsShardColumns(keyColumns, shardColumns []string) bool {
	for _, sc := range shardColumns {
		found := false
		for _, kc := range keyColumns {
			if sc == kc {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// constraintProgram discovers, once per placed statement shape, which WHERE
// conjuncts bind each shard-key ordinal, so binding typed arguments yields
// distribution.BoundConstraints without re-walking the SQL text. It is immutable
// after compilation and safe to share across executions; the bound values and
// the resulting route belong to one execution.
type constraintProgram struct {
	binding *placementBinding
	slots   []predicateSlot
}

// predicateSlot collects the narrowing predicates a WHERE tree places on one
// shard-key ordinal.
type predicateSlot struct {
	constraints []shardConstraint
}

// shardConstraint is one discovered equality or membership predicate over a
// shard-key ordinal. operands holds the equality operand or the IN alternatives;
// unbindable marks a predicate whose value cannot be bound (a subquery-fed set),
// which never narrows routing.
type shardConstraint struct {
	operands   []sqlast.Operand
	unbindable bool
}

// compileConstraintProgram walks a WHERE tree once, matching each conjunct leaf
// against binding's shard-key columns and recording the equality and membership
// predicates per ordinal. It is the plan-time half of route-after-bind; Bind is
// the per-execution half.
func compileConstraintProgram(binding *placementBinding, where *sqlast.Expr) *constraintProgram {
	prog := &constraintProgram{
		binding: binding,
		slots:   make([]predicateSlot, len(binding.placement.Columns)),
	}
	prog.discover(where)
	return prog
}

// discover splits conjuncts and records the equality and membership predicates
// each leaf places on a shard-key ordinal. Inequalities, ranges, negated IN, and
// non-shard-key leaves are ignored: they never narrow a finite domain, so the
// ordinal stays unknown and routing fails closed.
func (p *constraintProgram) discover(where *sqlast.Expr) {
	if where == nil {
		return
	}
	if where.Kind == sqlast.ExprAnd {
		for _, kid := range where.Kids {
			p.discover(kid)
		}
		return
	}
	ordinal, ok := p.matchColumn(where.Path)
	if !ok {
		return
	}
	switch where.Kind {
	case sqlast.ExprCompare:
		if where.Op != sqlast.OpEq {
			return
		}
		if where.Subquery != nil {
			p.slots[ordinal].constraints = append(
				p.slots[ordinal].constraints, shardConstraint{unbindable: true})
			return
		}
		p.slots[ordinal].constraints = append(
			p.slots[ordinal].constraints, shardConstraint{operands: []sqlast.Operand{where.Value}})
	case sqlast.ExprIn:
		if where.Negated {
			return
		}
		if where.Subquery != nil {
			p.slots[ordinal].constraints = append(
				p.slots[ordinal].constraints, shardConstraint{unbindable: true})
			return
		}
		p.slots[ordinal].constraints = append(
			p.slots[ordinal].constraints, shardConstraint{operands: where.List})
	}
}

// matchColumn returns the shard-key ordinal a base-table path addresses. Only
// the base table (source 0) carries the shard-key columns a single-table write
// routes on.
func (p *constraintProgram) matchColumn(path *sqlast.PathExpr) (int, bool) {
	if path == nil || path.Source != 0 {
		return 0, false
	}
	spelling := string(path.AppendPointer(nil))
	for i, col := range p.binding.placement.Columns {
		if spelling == col {
			return i, true
		}
	}
	return 0, false
}

// Bind converts the discovered predicates to distribution.BoundConstraints for
// one set of typed arguments. An ordinal with no narrowing predicate is
// DomainUnknown; intersecting predicates that contradict yield DomainEmpty; and
// a value that cannot be bound to a placement scalar preserves DomainUnknown, so
// routing is never narrowed by a value it cannot canonically encode.
func (p *constraintProgram) Bind(args []any) (distribution.BoundConstraints, error) {
	cons := make(distribution.BoundConstraints, len(p.slots))
	builder := distribution.NewConstraintBuilder()
	var scratch []distribution.Scalar
	for i := range p.slots {
		slot := &p.slots[i]
		if len(slot.constraints) == 0 {
			cons[i] = distribution.UnknownDomain()
			continue
		}
		builder.Reset()
		for _, c := range slot.constraints {
			if c.unbindable {
				builder.AddUnbindable()
				continue
			}
			scalars, unbindable, err := bindOperandScalars(c.operands, args, scratch[:0])
			if err != nil {
				return nil, err
			}
			scratch = scalars
			if unbindable {
				builder.AddUnbindable()
				continue
			}
			if err := builder.AddMembership(scalars); err != nil {
				return nil, err
			}
		}
		cons[i] = builder.Domain()
	}
	return cons, nil
}

// Route binds args and resolves the placed statement to an immutable route using
// the caller-owned router. It is the route-after-bind entry point.
func (p *constraintProgram) Route(router *distribution.Router, args []any) (distribution.Route, error) {
	cons, err := p.Bind(args)
	if err != nil {
		return distribution.Route{}, err
	}
	return router.Route(cons, p.binding.mapper, p.binding.manifest, p.binding.policy)
}

// bindOperandScalars converts a predicate's operands to placement scalars,
// skipping SQL NULLs, appending into dst. It reports unbindable when any non-null
// operand resolves to a value outside the closed String/Number placement set, so
// a predicate carrying such a value never narrows routing.
func bindOperandScalars(operands []sqlast.Operand, args []any, dst []distribution.Scalar) ([]distribution.Scalar, bool, error) {
	for _, operand := range operands {
		value, err := operandValue(operand, args)
		if err != nil {
			return dst, false, err
		}
		if value == nil {
			continue
		}
		scalar, ok, err := scalarFromValue(value)
		if err != nil {
			return dst, false, err
		}
		if !ok {
			return dst, true, nil
		}
		dst = append(dst, scalar)
	}
	return dst, false, nil
}

// scalarFromValue converts a coerced operand or document value to a placement
// scalar. It reports ok=false for a value outside the closed String/Number
// placement set (for example a bool). Numbers keep their exact spelling and
// native numeric parameters are rendered exactly as the primary-key path renders
// them, so a constant and a parameter carrying the same value canonicalize to
// one routing identity.
func scalarFromValue(value any) (distribution.Scalar, bool, error) {
	switch v := value.(type) {
	case string:
		return distribution.NewString(v), true, nil
	case []byte:
		return distribution.NewString(string(v)), true, nil
	case *string:
		if v == nil {
			return distribution.Scalar{}, false, nil
		}
		return distribution.NewString(*v), true, nil
	case json.Number:
		return numberScalar(string(v))
	case query.Number:
		return numberScalar(string(v))
	case *query.Number:
		if v == nil {
			return distribution.Scalar{}, false, nil
		}
		return numberScalar(string(*v))
	case int:
		return numberScalar(strconv.FormatInt(int64(v), 10))
	case int8:
		return numberScalar(strconv.FormatInt(int64(v), 10))
	case int16:
		return numberScalar(strconv.FormatInt(int64(v), 10))
	case int32:
		return numberScalar(strconv.FormatInt(int64(v), 10))
	case int64:
		return numberScalar(strconv.FormatInt(v, 10))
	case *int64:
		if v == nil {
			return distribution.Scalar{}, false, nil
		}
		return numberScalar(strconv.FormatInt(*v, 10))
	case uint:
		return numberScalar(strconv.FormatUint(uint64(v), 10))
	case uint8:
		return numberScalar(strconv.FormatUint(uint64(v), 10))
	case uint16:
		return numberScalar(strconv.FormatUint(uint64(v), 10))
	case uint32:
		return numberScalar(strconv.FormatUint(uint64(v), 10))
	case uint64:
		return numberScalar(strconv.FormatUint(v, 10))
	case float32:
		return numberScalar(strconv.FormatFloat(float64(v), 'g', -1, 64))
	case float64:
		return numberScalar(strconv.FormatFloat(v, 'g', -1, 64))
	case *float64:
		if v == nil {
			return distribution.Scalar{}, false, nil
		}
		return numberScalar(strconv.FormatFloat(*v, 'g', -1, 64))
	default:
		// bool, *bool, and any non-scalar value are not placement scalars.
		return distribution.Scalar{}, false, nil
	}
}

// numberScalar builds a Number scalar from a JSON number spelling.
func numberScalar(spelling string) (distribution.Scalar, bool, error) {
	s, err := distribution.NewNumber(spelling)
	if err != nil {
		return distribution.Scalar{}, false, err
	}
	return s, true, nil
}

// documentShardKey extracts the post-coercion shard-key scalars from a built
// insert or replacement document, one per placement column, using the same
// compiled-pointer + GetRaw path documentKey uses for the primary key. A
// missing, null, or non-scalar shard-key value is a routing error.
func documentShardKey(document []byte, binding *placementBinding, dst []distribution.Scalar) ([]distribution.Scalar, error) {
	dst = dst[:0]
	for i, ptr := range binding.pointers {
		value, found, err := ptr.GetRaw(document)
		if err != nil {
			return dst, fmt.Errorf("vibedb: invalid JSON document: %w", err)
		}
		column := binding.placement.Columns[i]
		if !found {
			return dst, fmt.Errorf("vibedb: shard-key column %q is missing", column)
		}
		if value.IsNull() {
			return dst, fmt.Errorf("vibedb: shard-key column %q is null", column)
		}
		switch value.Kind() {
		case jsondoc.String:
			text, ok, err := value.Text()
			if err != nil {
				return dst, fmt.Errorf(
					"vibedb: shard-key column %q has an invalid JSON string: %w", column, err)
			}
			if !ok {
				return dst, fmt.Errorf(
					"vibedb: shard-key column %q has an invalid JSON string", column)
			}
			dst = append(dst, distribution.NewString(text))
		case jsondoc.Number:
			number, ok := value.NumberText()
			if !ok {
				return dst, fmt.Errorf(
					"vibedb: shard-key column %q is not a valid JSON number", column)
			}
			scalar, err := distribution.NewNumber(number)
			if err != nil {
				return dst, fmt.Errorf("vibedb: shard-key column %q: %w", column, err)
			}
			dst = append(dst, scalar)
		default:
			return dst, fmt.Errorf(
				"vibedb: shard-key column %q must be a JSON string or number", column)
		}
	}
	return dst, nil
}

// routeShardKey routes one full shard key (one scalar per placement column) to
// an immutable route. A full key resolves to a single keyspace point and so to
// exactly one shard.
func routeShardKey(binding *placementBinding, router *distribution.Router, key []distribution.Scalar) (distribution.Route, error) {
	cons := make(distribution.BoundConstraints, len(key))
	for i := range key {
		domain, err := distribution.FiniteDomain(key[i])
		if err != nil {
			return distribution.Route{}, err
		}
		cons[i] = domain
	}
	return router.Route(cons, binding.mapper, binding.manifest, binding.policy)
}

// insertPreflight routes every row of an insert to its physical shard, mapping
// all rows before any dispatch, and returns the single target route. Rows that
// do not all resolve to the same one shard return ErrCrossShardWrite; no row is
// dispatched, matching version 1's single-target-shard rule.
func insertPreflight(binding *placementBinding, router *distribution.Router, documents [][]byte) (distribution.Route, error) {
	var chosen distribution.Route
	var scratch []distribution.Scalar
	for idx, document := range documents {
		key, err := documentShardKey(document, binding, scratch)
		if err != nil {
			return distribution.Route{}, err
		}
		scratch = key
		route, err := routeShardKey(binding, router, key)
		if err != nil {
			return distribution.Route{}, err
		}
		if route.Kind != distribution.RouteSingle {
			return distribution.Route{}, fmt.Errorf(
				"%w: row %d does not resolve to a single shard", ErrCrossShardWrite, idx)
		}
		if idx == 0 {
			chosen = route
			continue
		}
		if !sameShardSet(chosen, route) {
			return distribution.Route{}, fmt.Errorf(
				"%w: row %d routes to a different shard than row 0", ErrCrossShardWrite, idx)
		}
	}
	return chosen, nil
}

// singleShardRoute binds a placed update or delete predicate and requires it to
// resolve to exactly one physical shard, as version 1 demands. A RouteEmpty (no
// matching rows) is accepted as a no-op. An unknown, targeted multi-shard, or
// scatter route returns ErrScatterWrite before any dispatch.
func singleShardRoute(prog *constraintProgram, router *distribution.Router, args []any) (distribution.Route, error) {
	route, err := prog.Route(router, args)
	if err != nil {
		if errors.Is(err, distribution.ErrScatterRejected) {
			return distribution.Route{}, fmt.Errorf(
				"%w: predicate does not resolve to a single shard", ErrScatterWrite)
		}
		return distribution.Route{}, err
	}
	switch route.Kind {
	case distribution.RouteSingle, distribution.RouteEmpty:
		return route, nil
	default:
		return distribution.Route{}, fmt.Errorf(
			"%w: predicate resolves to a %s route", ErrScatterWrite, route.Kind)
	}
}

// checkShardKeyImmutable verifies a whole-document UPDATE's replacement routes to
// the same shard the update targets, so the replacement cannot move a row across
// shards. It is a no-op when the update matched no rows. A replacement whose
// shard-key columns route elsewhere returns ErrShardKeyImmutable.
func checkShardKeyImmutable(binding *placementBinding, router *distribution.Router, target distribution.Route, replacement []byte) error {
	if target.Kind == distribution.RouteEmpty {
		return nil
	}
	key, err := documentShardKey(replacement, binding, nil)
	if err != nil {
		return err
	}
	route, err := routeShardKey(binding, router, key)
	if err != nil {
		return err
	}
	if route.Kind != distribution.RouteSingle || !sameShardSet(target, route) {
		return fmt.Errorf(
			"%w: replacement document routes to a different shard", ErrShardKeyImmutable)
	}
	return nil
}

// sameShardSet reports whether two routes select the same physical shards.
func sameShardSet(a, b distribution.Route) bool {
	if len(a.Targets) != len(b.Targets) {
		return false
	}
	for i := range a.Targets {
		if a.Targets[i].Shard != b.Targets[i].Shard {
			return false
		}
	}
	return true
}
