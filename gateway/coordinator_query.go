package gateway

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// coordinatorPhysicalTables follows resolved relation kinds, including nested
// predicates and set TABLE leaves. CTE names are never mistaken for base tables.
func coordinatorPhysicalTables(root *sqlast.SelectStmt) []string {
	var tables []string
	seen := make(map[*sqlast.SelectStmt]bool)
	var selectNode func(*sqlast.SelectStmt)
	var setNode func(*sqlast.SetExpr)
	var predicate func(*sqlast.Expr)
	var scalar func(*sqlast.ScalarExpr)
	add := func(name string) {
		if !slices.Contains(tables, name) {
			tables = append(tables, name)
		}
	}
	scalar = func(e *sqlast.ScalarExpr) {
		if e == nil {
			return
		}
		scalar(e.Left)
		scalar(e.Right)
		scalar(e.Else)
		for _, arm := range e.Whens {
			predicate(arm.Predicate)
			scalar(arm.Match)
			scalar(arm.Result)
		}
	}
	predicate = func(e *sqlast.Expr) {
		if e == nil {
			return
		}
		selectNode(e.Subquery)
		scalar(e.ScalarLeft)
		scalar(e.ScalarRight)
		for _, child := range e.Kids {
			predicate(child)
		}
	}
	setNode = func(e *sqlast.SetExpr) {
		if e == nil {
			return
		}
		selectNode(e.Select)
		selectNode(e.First)
		if e.Table != nil {
			ref := e.Table.Ref
			if ref.Kind == sqlast.RelationCollection {
				add(ref.Name)
			} else {
				selectNode(ref.Query)
			}
		}
		setNode(e.Left)
		setNode(e.Right)
		setNode(e.Child)
	}
	selectNode = func(s *sqlast.SelectStmt) {
		if s == nil || seen[s] {
			return
		}
		seen[s] = true
		for _, ref := range s.From {
			if ref.Kind == sqlast.RelationCollection {
				add(ref.Name)
			} else {
				selectNode(ref.Query)
			}
			if ref.On != nil {
				predicate(ref.On.Expr)
			}
		}
		if s.Set != nil {
			setNode(s.Set.Root)
		}
		predicate(s.Where)
		predicate(s.Having)
		for _, column := range s.Columns {
			scalar(column.Scalar)
		}
		for _, order := range s.OrderBy {
			scalar(order.Scalar)
		}
	}
	selectNode(root)
	slices.Sort(tables)
	return tables
}

func quoteSQLIdentifier(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// queryCoordinator evaluates the original statement using the shared SQL engine
// over bounded source relations. Each shard reads all participating relations
// in its distribution in one SQL statement, preserving its coherent catalog
// snapshot. RF3 retains the ordinary independent group observation vector.
// Nothing from these transient relations is published back to a shard.
func (e *Executor) queryCoordinator(ctx context.Context, snap *Snapshot, q Query, prepared *PreparedPlan, profile Profile, args []any) (*Result, error) {
	types, err := postgresQueryParameterTypes(q.ParamTypes, prepared.params)
	if err != nil {
		return nil, err
	}
	compiled, err := query.PrepareParsedStatementWithParameterTypes(q.SQL, prepared.statement.Select, types)
	if err != nil {
		return nil, err
	}
	defer compiled.Release()
	sourcePlan, err := e.planCoordinator(ctx, snap, q, prepared, profile, args)
	if err != nil {
		return nil, err
	}
	resolve := snap.declaredSQLPathDomain
	compiled.ProveSQLPathComparisonDomains(resolve)
	tables := sourcePlan.tables
	all := sourcePlan.dispatch
	var sources store.Database
	collections := make([]*store.Collection, len(tables))
	for i, table := range tables {
		collections[i], err = sources.CreateCollection(table, store.Options{})
		if err != nil {
			return nil, err
		}
	}
	e.observePressureCalls(all.calls)
	e.metrics.observeRoute(all.kind, len(all.calls), all.scatter)
	// dispatch bounds the sum across every distribution, including all shard
	// response headers and source cells, before any source is retained.
	input, err := e.dispatch(ctx, all, profile)
	if err != nil {
		return nil, err
	}
	var sourceBytes uint64
	for ordinal, row := range input.Rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row) != 2 || row[0].Null || row[1].Null {
			return nil, ErrMalformedRequest
		}
		index, parseErr := strconv.Atoi(string(row[1].Bytes))
		if parseErr != nil || index < 0 || index >= len(collections) {
			return nil, ErrMalformedRequest
		}
		// Reserve source payload and structural overhead before building the heap
		// source. Query intermediates receive only the remaining operation budget.
		cost := uint64(len(row[0].Bytes)+20)*16 + 512
		if cost > profile.MaxAggregateBytes-sourceBytes {
			return nil, ErrResultLimit
		}
		sourceBytes += cost
		if _, err := collections[index].Put(strconv.Itoa(ordinal), row[0].Bytes); err != nil {
			return nil, err
		}
	}
	input = nil
	var cancel query.CancelFlag
	stop := context.AfterFunc(ctx, cancel.Cancel)
	defer stop()
	var exec query.Exec
	defer exec.Release()
	exec.Options.Cancel = &cancel
	exec.Options.ResultRows = int(min(profile.MaxAggregateRows, uint64(^uint(0)>>1)))
	exec.Options.ResultBytes = int64(profile.MaxAggregateBytes - sourceBytes)
	exec.Options.IntermediateBytes = exec.Options.ResultBytes
	if exec.Options.ResultBytes <= 0 {
		return nil, ErrResultLimit
	}
	cursor, err := compiled.RunInto(&exec, query.FromDatabase(sources.Snapshot(), compiled.Collection()), args)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	result := &Result{Kind: shardservice.ResponseRows, Generation: snap.Generation(), RouteKind: all.kind, ShardsFanned: len(all.calls), ScatterReason: all.scatter, PlanFingerprint: sourcePlan.explanation.PlanFingerprint, Planning: sourcePlan.explanation.Planning}
	for _, column := range compiled.Columns() {
		result.Columns = append(result.Columns, shardservice.Column{Name: column, TypeOID: 114})
	}
	for cursor.Next() {
		row := make([]shardservice.Cell, len(result.Columns))
		for i := range row {
			cell := cursor.Cell(i)
			row[i] = shardservice.Cell{Null: cell.IsNull()}
			if !cell.IsNull() {
				row[i].Bytes = cell.AppendJSON(nil)
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (snap *Snapshot) declaredSQLPathDomain(table, pointer string) query.SQLPathDomain {
	i, ok := slices.BinarySearchFunc(snap.replicatedTableDeclarations, table,
		func(a replicatedTableDeclaration, b string) int { return strings.Compare(a.declaration.Table, b) })
	if !ok {
		return query.SQLPathDomainUnknown
	}
	for _, column := range snap.replicatedTableDeclarations[i].info.Columns {
		if column.Path != pointer {
			continue
		}
		types := column.Types &^ sqlast.TypeNull
		switch {
		case types == sqlast.TypeBool:
			return query.SQLPathDomainBoolean
		case types != 0 && types&^(sqlast.TypeNumber|sqlast.TypeInteger) == 0:
			return query.SQLPathDomainNumeric
		case types == sqlast.TypeString:
			return query.SQLPathDomainText
		}
	}
	return query.SQLPathDomainUnknown
}

// Lexical references still need placement/name validation even when an unused
// CTE contributes no executable source scan.
func coordinatorHasPhysicalReferences(root *sqlast.SelectStmt) bool {
	found := false
	_ = sqlast.WalkSelectStatements(root, func(s *sqlast.SelectStmt) error {
		for _, ref := range s.From {
			found = found || ref.Kind == sqlast.RelationCollection
		}
		return nil
	})
	return found
}
