package driver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

type viewCatalogResolver struct {
	views map[string]*viewMeta
}

func (r viewCatalogResolver) ResolveSQLView(
	name string,
) (query.SQLViewDefinition, bool, error) {
	meta, ok := r.views[name]
	if !ok {
		return query.SQLViewDefinition{}, false, nil
	}
	if meta == nil {
		return query.SQLViewDefinition{}, false, fmt.Errorf(
			"vibedb: SQL view %q has null catalog metadata", name,
		)
	}
	return query.SQLViewDefinition{
		Name: name, Query: meta.Query, Columns: meta.Columns,
	}, true, nil
}

type viewDependency struct {
	name string
	pos  int
	meta *viewMeta
}

// preparedViewState is allocated only for view DDL or a query that actually
// expanded at least one view. Ordinary statements retain one nil sidecar and
// no view slices or lifecycle graph.
type preparedViewState struct {
	ddl          *preparedViewDDL
	dependencies []viewDependency
}

func (d *database) validateViewCatalog() error {
	if len(d.catalog.Views) == 0 {
		return nil
	}
	names := make([]string, 0, len(d.catalog.Views))
	for name := range d.catalog.Views {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		meta := d.catalog.Views[name]
		rebuilt, err := buildViewMeta(
			context.Background(), nil,
			name, meta.Query, meta.Columns,
			d.catalog.Views, d.tables, 0,
		)
		if err != nil {
			return fmt.Errorf(
				"vibedb: revalidate SQL catalog view %q: %w", name, err,
			)
		}
		if !equalViewMeta(meta, rebuilt) {
			return fmt.Errorf(
				"vibedb: SQL catalog view %q metadata does not match its normalized query",
				name,
			)
		}
	}
	return nil
}

func buildViewMeta(
	ctx context.Context,
	cancel *query.CancelFlag,
	name string,
	querySQL string,
	columns []string,
	views map[string]*viewMeta,
	tables map[string]*table,
	positionBase int,
) (*viewMeta, error) {
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	check := func() error {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		if cancel != nil && cancel.Canceled() {
			return query.ErrCanceled
		}
		return nil
	}
	definition := query.SQLViewDefinition{
		Name: name, Query: querySQL, Columns: columns,
	}
	tree, expansion, err := query.ExpandSQLViewDefinition(
		definition,
		viewCatalogResolver{views: views},
		query.SQLViewExpansionOptions{Check: check},
	)
	if err != nil {
		return nil, rebaseViewDefinitionError(err, positionBase)
	}
	prepared, err := query.PrepareParsedStatement(querySQL, tree)
	if err != nil {
		return nil, rebaseViewDefinitionError(err, positionBase)
	}
	outputs := prepared.Columns()
	if len(outputs) == 0 {
		prepared.Release()
		return nil, rebaseViewDefinitionError(
			errors.New("vibedb: a view must expose at least one output column"),
			positionBase,
		)
	}
	for i := range outputs {
		if outputs[i] == "" || outputs[i] == "*" {
			prepared.Release()
			return nil, rebaseViewDefinitionError(sqlast.NewFeatureNotSupportedError(
				querySQL, 0,
				"a durable view requires an explicit stable name for every output column",
			), positionBase)
		}
		for previous := 0; previous < i; previous++ {
			if outputs[previous] == outputs[i] {
				prepared.Release()
				return nil, rebaseViewDefinitionError(fmt.Errorf(
					"%w: view %q output %q appears more than once; use an explicit view column list",
					ErrDuplicateViewColumn, name, outputs[i],
				), positionBase)
			}
		}
	}
	ownedOutputs := cloneStrings(outputs)
	prepared.Release()

	physical := selectPhysicalDependencies(tree)
	if len(physical) > maxCatalogViewDependencies {
		return nil, fmt.Errorf(
			"vibedb: view %q has %d physical dependencies, maximum is %d",
			name, len(physical), maxCatalogViewDependencies,
		)
	}
	tableDependencies := make([]string, len(physical))
	for i := range physical {
		if err := check(); err != nil {
			return nil, err
		}
		if _, ok := tables[physical[i].name]; !ok {
			return nil, rebaseViewDefinitionError(missingTableDependency(
				physical[i].name, physical[i].pos, false,
			), positionBase)
		}
		tableDependencies[i] = strings.Clone(physical[i].name)
	}
	if len(expansion.Dependencies) > maxCatalogViewDependencies {
		return nil, fmt.Errorf(
			"vibedb: view %q has %d view dependencies, maximum is %d",
			name, len(expansion.Dependencies), maxCatalogViewDependencies,
		)
	}
	viewDependencies := make([]string, len(expansion.Dependencies))
	for i := range expansion.Dependencies {
		dependency := expansion.Dependencies[i].Name
		if views[dependency] == nil {
			return nil, rebaseViewDefinitionError(&viewDependencyError{
				name: dependency, pos: expansion.Dependencies[i].Pos,
			}, positionBase)
		}
		viewDependencies[i] = strings.Clone(dependency)
	}
	meta := &viewMeta{
		Query:             strings.Clone(querySQL),
		Columns:           cloneStrings(columns),
		Outputs:           ownedOutputs,
		ViewDependencies:  viewDependencies,
		TableDependencies: tableDependencies,
	}
	if err := validateCatalogViewMeta(name, meta); err != nil {
		return nil, rebaseViewDefinitionError(err, positionBase)
	}
	return meta, nil
}

type viewDefinitionError struct {
	err error
	pos int
}

func (e *viewDefinitionError) Error() string { return e.err.Error() }
func (e *viewDefinitionError) Unwrap() error { return e.err }
func (e *viewDefinitionError) Position() int { return e.pos }

func rebaseViewDefinitionError(err error, base int) error {
	if err == nil || base == 0 {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, query.ErrCanceled) {
		return err
	}
	position := 0
	var positioned interface{ Position() int }
	if errors.As(err, &positioned) {
		position = positioned.Position()
	} else {
		var parse *sqlast.ParseError
		if errors.As(err, &parse) {
			position = parse.Pos
		}
	}
	if position < 0 {
		position = 0
	}
	return &viewDefinitionError{err: err, pos: base + position}
}

func equalViewMeta(left, right *viewMeta) bool {
	if left == nil || right == nil || left.Query != right.Query {
		return left == right
	}
	return equalStrings(left.Columns, right.Columns) &&
		equalStrings(left.Outputs, right.Outputs) &&
		equalStrings(left.ViewDependencies, right.ViewDependencies) &&
		equalStrings(left.TableDependencies, right.TableDependencies)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	for i := range values {
		cloned[i] = strings.Clone(values[i])
	}
	return cloned
}

func (c *conn) expandPreparedViews(
	ctx context.Context,
	source string,
	tree *sqlast.SelectStmt,
) ([]viewDependency, error) {
	views, err := c.snapshotViewCatalog(ctx)
	if err != nil || len(views) == 0 {
		return nil, err
	}
	check := func() error {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		if cancel := c.exec.Options.Cancel; cancel != nil && cancel.Canceled() {
			return query.ErrCanceled
		}
		return nil
	}
	expansion, err := query.ExpandSQLViews(
		source, tree, viewCatalogResolver{views: views},
		query.SQLViewExpansionOptions{Check: check},
	)
	if err != nil {
		return nil, err
	}
	dependencies := make([]viewDependency, len(expansion.Dependencies))
	for i := range expansion.Dependencies {
		dependency := expansion.Dependencies[i]
		meta := views[dependency.Name]
		if meta == nil {
			return nil, &viewDependencyError{name: dependency.Name, pos: dependency.Pos}
		}
		dependencies[i] = viewDependency{
			name: dependency.Name, pos: dependency.Pos, meta: meta,
		}
	}
	return dependencies, nil
}

func (c *conn) snapshotViewCatalog(ctx context.Context) (map[string]*viewMeta, error) {
	if c.tx != nil {
		if c.tx.done {
			return nil, errors.New("vibedb: transaction is finished")
		}
		return c.tx.views, nil
	}
	if err := rlockContext(ctx, &c.db.mu); err != nil {
		return nil, err
	}
	defer c.db.mu.RUnlock()
	if len(c.db.catalog.Views) == 0 {
		return nil, nil
	}
	views := make(map[string]*viewMeta, len(c.db.catalog.Views))
	for name, meta := range c.db.catalog.Views {
		views[name] = meta
	}
	return views, nil
}

func (s *stmt) validateTransactionViewDependencies() error {
	if s.views == nil || len(s.views.dependencies) == 0 {
		return nil
	}
	if s.conn.tx == nil || s.conn.tx.done {
		return errors.New("vibedb: transaction is finished")
	}
	for i := range s.views.dependencies {
		dependency := &s.views.dependencies[i]
		if s.conn.tx.views[dependency.name] != dependency.meta {
			return &viewDependencyError{
				name: dependency.name, pos: dependency.pos, transaction: true,
			}
		}
	}
	return nil
}

func (s *stmt) validateViewDependenciesLocked() error {
	if s.views == nil {
		return nil
	}
	for i := range s.views.dependencies {
		dependency := &s.views.dependencies[i]
		if s.conn.db.catalog.Views[dependency.name] != dependency.meta {
			return &viewDependencyError{name: dependency.name, pos: dependency.pos}
		}
	}
	return nil
}

func (s *stmt) validatePreparedViewDependencies(ctx context.Context) error {
	if s.views == nil || len(s.views.dependencies) == 0 {
		return nil
	}
	if s.conn.tx != nil {
		return s.validateTransactionViewDependencies()
	}
	if err := rlockContext(ctx, &s.conn.db.mu); err != nil {
		return err
	}
	err := s.validateViewDependenciesLocked()
	s.conn.db.mu.RUnlock()
	return err
}
