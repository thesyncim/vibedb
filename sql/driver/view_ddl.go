package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"sort"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

type preparedViewDDL struct {
	source string
	create *sqlast.CreateViewStmt
	drop   *sqlast.DropViewStmt
}

func (c *conn) prepareViewDDL(
	ctx context.Context,
	source string,
	tree *sqlast.Statement,
) (*preparedViewDDL, error) {
	prepared := &preparedViewDDL{source: source}
	switch tree.Kind {
	case sqlast.KindCreateView:
		if tree.CreateView == nil || tree.CreateView.Materialized {
			return nil, fmt.Errorf("vibedb: invalid ordinary CREATE VIEW descriptor")
		}
		prepared.create = tree.CreateView
		if err := rlockContext(ctx, &c.db.mu); err != nil {
			return nil, err
		}
		_, err := buildViewMeta(
			ctx, c.exec.Options.Cancel,
			tree.CreateView.Name,
			tree.CreateView.QuerySQL,
			tree.CreateView.Columns,
			c.db.catalog.Views,
			c.db.tables,
			tree.CreateView.QueryPos,
		)
		c.db.mu.RUnlock()
		if err != nil {
			return nil, err
		}
	case sqlast.KindDropView:
		if tree.DropView == nil {
			return nil, fmt.Errorf("vibedb: invalid DROP VIEW descriptor")
		}
		prepared.drop = tree.DropView
		if tree.DropView.IfExists {
			return prepared, nil
		}
		if err := rlockContext(ctx, &c.db.mu); err != nil {
			return nil, err
		}
		_, exists := c.db.catalog.Views[tree.DropView.Name]
		c.db.mu.RUnlock()
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrViewNotFound, tree.DropView.Name)
		}
	default:
		return nil, fmt.Errorf("vibedb: statement is not view DDL")
	}
	return prepared, nil
}

func (c *conn) execViewDDL(
	ctx context.Context,
	prepared *preparedViewDDL,
) (sqldriver.Result, error) {
	ctx = withCooperativeCancellation(ctx, c.exec.Options.Cancel)
	if prepared == nil {
		return nil, fmt.Errorf("vibedb: missing prepared view DDL")
	}
	if c.tx != nil {
		return nil, ErrDDLInTransaction
	}
	if err := lockContext(ctx, &c.db.mu); err != nil {
		return nil, err
	}
	defer c.db.mu.Unlock()
	if c.db.closed {
		return nil, sqldriver.ErrBadConn
	}
	if err := c.db.settleCatalogLocked(); err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if prepared.create != nil {
		return c.db.createViewLocked(ctx, c.exec.Options.Cancel, prepared.create)
	}
	return c.db.dropViewLocked(ctx, prepared.drop)
}

func (d *database) createViewLocked(
	ctx context.Context,
	cancel *query.CancelFlag,
	definition *sqlast.CreateViewStmt,
) (sqldriver.Result, error) {
	name := definition.Name
	if err := validateCatalogTableName(name); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE VIEW: %w", err)
	}
	if _, exists := d.tables[name]; exists {
		return nil, fmt.Errorf("%w: relation %q is a table", ErrViewExists, name)
	}
	if _, exists := d.catalog.Views[name]; exists {
		return nil, fmt.Errorf("%w: %q", ErrViewExists, name)
	}
	if err := checkCatalogViewCount(len(d.catalog.Views) + 1); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE VIEW %q: %w", name, err)
	}
	meta, err := buildViewMeta(
		ctx, cancel, name, definition.QuerySQL, definition.Columns,
		d.catalog.Views, d.tables,
		definition.QueryPos,
	)
	if err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	previousPending := d.catalogWritePending
	d.catalog.Views[name] = meta
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			delete(d.catalog.Views, name)
			d.catalogWritePending = previousPending
		}
		return nil, err
	}
	return result{}, nil
}

func (d *database) dropViewLocked(
	ctx context.Context,
	drop *sqlast.DropViewStmt,
) (sqldriver.Result, error) {
	if drop == nil {
		return nil, fmt.Errorf("vibedb: missing DROP VIEW descriptor")
	}
	meta, exists := d.catalog.Views[drop.Name]
	if !exists {
		if drop.IfExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrViewNotFound, drop.Name)
	}
	if dependent := d.firstDependentViewLocked(drop.Name, true); dependent != "" {
		return nil, fmt.Errorf(
			"%w: view %q is required by view %q; DROP dependent views first",
			ErrDependentObjects, drop.Name, dependent,
		)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	previousPending := d.catalogWritePending
	delete(d.catalog.Views, drop.Name)
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			d.catalog.Views[drop.Name] = meta
			d.catalogWritePending = previousPending
		}
		return nil, err
	}
	return result{}, nil
}

func (d *database) firstDependentViewLocked(name string, view bool) string {
	dependents := make([]string, 0, 1)
	for candidate, meta := range d.catalog.Views {
		if candidate == name || meta == nil {
			continue
		}
		dependencies := meta.TableDependencies
		if view {
			dependencies = meta.ViewDependencies
		}
		for _, dependency := range dependencies {
			if dependency == name {
				dependents = append(dependents, candidate)
				break
			}
		}
	}
	if len(dependents) == 0 {
		return ""
	}
	sort.Strings(dependents)
	return dependents[0]
}
