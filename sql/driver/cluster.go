package driver

import (
	stdsql "database/sql"
	sqldriver "database/sql/driver"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// clusterRouting is the immutable routing state a local cluster facade attaches
// to one embedded database: the validated static configuration and one compiled
// placement binding per placed table. It is shared read-only across every
// connection the database serves; each connection owns its own Router.
type clusterRouting struct {
	config   distribution.ClusterConfig
	bindings map[string]*placementBinding
}

// newClusterRouting compiles one placement binding per placed table from an
// already-validated configuration, selecting the frozen native mapper and one
// manifest per distribution.
func newClusterRouting(cfg distribution.ClusterConfig) (*clusterRouting, error) {
	routing := &clusterRouting{
		config:   cfg,
		bindings: make(map[string]*placementBinding, len(cfg.Placements)),
	}
	for i := range cfg.Placements {
		table := cfg.Placements[i].Table
		binding, ok, err := newPlacementBinding(cfg, table)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		routing.bindings[table] = binding
	}
	return routing, nil
}

// binding returns the compiled placement binding for table, or nil when the
// table is unplaced.
func (cr *clusterRouting) binding(table string) *placementBinding {
	if cr == nil {
		return nil
	}
	return cr.bindings[table]
}

// validateOpenedSchema enforces the uniqueness-locality invariant for every
// placed table already present in the opened catalog: the primary key and
// every local unique index must contain every shard-key column. Tables and
// indexes created later are validated by their DDL paths. It reports
// violations in configuration order.
func (cr *clusterRouting) validateOpenedSchema(d *database) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for i := range cr.config.Placements {
		table := cr.config.Placements[i].Table
		binding := cr.bindings[table]
		if binding == nil {
			continue
		}
		meta, ok := d.catalog.Tables[table]
		if !ok {
			continue
		}
		if err := validateShardKeyLocality(
			table, binding.placement.Columns, meta.PrimaryKey,
		); err != nil {
			return err
		}
		for _, index := range meta.Indexes {
			if !index.Unique {
				continue
			}
			if !keyContainsShardColumns(index.Paths, binding.placement.Columns) {
				return fmt.Errorf(
					"%w: table %q unique index %q over %v does not contain shard-key columns %v",
					ErrShardKeyNotLocal, table, index.Name, index.Paths,
					binding.placement.Columns,
				)
			}
		}
	}
	return nil
}

// validatePlacementLocality enforces the uniqueness-locality invariant for a
// single table at CREATE TABLE time, before the table is persisted. It is a
// no-op for an unplaced table and for a database opened without a cluster
// configuration, so the default non-distributed CREATE TABLE path is unchanged.
func (cr *clusterRouting) validatePlacementLocality(table, primaryKey string) error {
	if cr == nil {
		return nil
	}
	binding := cr.bindings[table]
	if binding == nil {
		return nil
	}
	return validateShardKeyLocality(table, binding.placement.Columns, primaryKey)
}

// validateUniqueIndexLocality rejects a local unique index that could admit the
// same tuple on two physical shards. It is a no-op outside a placed local
// cluster; RF3 schema DDL has its own fail-closed gate.
func (cr *clusterRouting) validateUniqueIndexLocality(
	table, index string,
	paths []string,
) error {
	if cr == nil {
		return nil
	}
	binding := cr.bindings[table]
	if binding == nil ||
		keyContainsShardColumns(paths, binding.placement.Columns) {
		return nil
	}
	return fmt.Errorf(
		"%w: table %q unique index %q over %v does not contain shard-key columns %v",
		ErrShardKeyNotLocal, table, index, paths, binding.placement.Columns,
	)
}

// OpenClusterConnector opens the SQL database at dsn as a degenerate single-shard
// local cluster and returns a connector that routes every write through the full
// preflight before it reaches the one embedded store. cfg supplies the static
// distributions, placements, and one manifest per distribution; it is validated,
// and the uniqueness-locality invariant is checked against the opened schema,
// before the connector is returned. There is no network. It shares the ordinary
// driver's catalog ownership, so the returned connector must be closed, and it
// leaves Open, OpenConnector, and the default non-distributed behavior unchanged.
func OpenClusterConnector(dsn string, cfg distribution.ClusterConfig) (sqldriver.Connector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	routing, err := newClusterRouting(cfg)
	if err != nil {
		return nil, err
	}
	connector, err := openConnector(dsn)
	if err != nil {
		return nil, err
	}
	connector.db.cluster = routing
	if err := routing.validateOpenedSchema(connector.db); err != nil {
		_ = connector.Close()
		return nil, err
	}
	return connector, nil
}

// OpenCluster opens dsn as a degenerate single-shard local cluster and returns a
// *sql.DB whose writes route through the full router and write preflight before
// reaching the one embedded store, with no network. It is the opt-in distributed
// facade over the ordinary driver: absent a cluster configuration, sql.Open and
// the default non-distributed behavior are unchanged. See OpenClusterConnector.
func OpenCluster(dsn string, cfg distribution.ClusterConfig) (*stdsql.DB, error) {
	connector, err := OpenClusterConnector(dsn, cfg)
	if err != nil {
		return nil, err
	}
	return stdsql.OpenDB(connector), nil
}

// clusterBinding returns the placement binding for table when this connection
// runs under a cluster configuration and the table is placed, or nil otherwise.
// The nil-configuration fast path allocates nothing, keeping the default
// non-distributed write path unchanged.
func (c *conn) clusterBinding(table string) *placementBinding {
	if c.db == nil {
		return nil
	}
	return c.db.cluster.binding(table)
}

// clusterRouter returns this connection's Router, creating it on first use. One
// Router per connection is safe because database/sql never uses a connection
// concurrently, and the Router reuses scratch buffers across calls.
func (c *conn) clusterRouter() *distribution.Router {
	if c.routing == nil {
		c.routing = distribution.NewRouter()
	}
	return c.routing
}

// routeInsertDocuments routes every post-coercion document of a placed insert to
// its physical shard and rejects a cross-shard batch before any dispatch. It is
// a no-op for an unplaced table or a non-cluster connection.
func (c *conn) routeInsertDocuments(table string, documents [][]byte) error {
	binding := c.clusterBinding(table)
	if binding == nil {
		return nil
	}
	state := insertPreflightState{text: c.pointRaw[:0]}
	defer func() { c.pointRaw = state.text[:0] }()
	for i := range documents {
		if err := state.add(binding, documents[i], i); err != nil {
			return err
		}
	}
	return nil
}

// routeInsertSeeds routes the staged seeds of a non-transactional placed insert.
func (c *conn) routeInsertSeeds(table string, seeds []seedDocument) error {
	return c.routeInsertSeedsWithBinding(c.clusterBinding(table), seeds)
}

func (c *conn) routeInsertSeedsWithBinding(
	binding *placementBinding,
	seeds []seedDocument,
) error {
	if binding == nil {
		return nil
	}
	state := insertPreflightState{text: c.pointRaw[:0]}
	defer func() { c.pointRaw = state.text[:0] }()
	for i := range seeds {
		if err := state.add(binding, seeds[i].document, i); err != nil {
			return err
		}
	}
	return nil
}

// routeInsertStaged routes the staged documents of a transactional placed insert.
func (c *conn) routeInsertStaged(table string, staged []stagedTxMutation) error {
	return c.routeInsertStagedWithBinding(c.clusterBinding(table), staged)
}

func (c *conn) routeInsertStagedWithBinding(
	binding *placementBinding,
	staged []stagedTxMutation,
) error {
	if binding == nil {
		return nil
	}
	state := insertPreflightState{text: c.pointRaw[:0]}
	defer func() { c.pointRaw = state.text[:0] }()
	for i := range staged {
		if err := state.add(binding, staged[i].document, i); err != nil {
			return err
		}
	}
	return nil
}

// checkUpsertShardKeyImmutable proves that a conflicting INSERT post-image
// remains on the current row's physical shard. Candidate INSERT routing alone
// is insufficient: an assignment could otherwise turn one conflict into what
// looks like an ordinary insert for another shard.
func (c *conn) checkUpsertShardKeyImmutable(
	table string,
	current []byte,
	replacement []byte,
) error {
	binding := c.clusterBinding(table)
	if binding == nil {
		return nil
	}
	documents := [1][]byte{current}
	route, err := insertPreflight(binding, documents[:])
	if err != nil {
		return err
	}
	return checkShardKeyImmutable(
		binding, c.clusterRouter(), route, replacement,
	)
}

// routeUpdate enforces the current write rules for a placed whole-document
// UPDATE: its predicate must resolve to exactly one physical shard, and the
// replacement document must not move the row to another shard. It is a no-op for
// an unplaced table or a non-cluster connection.
func (c *conn) routeUpdate(statement *query.DMLStatement, args []any, document []byte) error {
	text, err := c.routeUpdateInto(
		statement, args, document, c.pointRaw[:0],
	)
	c.pointRaw = text[:0]
	return err
}

// routeUpdateInto is routeUpdate with caller-owned text scratch. Mutation
// capture uses it while conn.pointRaw contains the old document supplied to a
// callback, preventing escaped shard-key decoding from overwriting that row.
func (c *conn) routeUpdateInto(
	statement *query.DMLStatement,
	args []any,
	document []byte,
	text []byte,
) ([]byte, error) {
	binding := c.clusterBinding(statement.Collection())
	if binding == nil {
		return text, nil
	}
	router := c.clusterRouter()
	program := compileConstraintProgram(binding, mutationWhere(statement))
	route, err := singleShardRoute(program, router, args)
	if err != nil {
		return text, err
	}
	return checkShardKeyImmutableInto(
		binding, router, route, document, text[:0],
	)
}

// routeDelete enforces the current single-shard-only rule for a placed DELETE:
// its predicate must resolve to exactly one physical shard. It is a no-op for an
// unplaced table or a non-cluster connection.
func (c *conn) routeDelete(statement *query.DMLStatement, args []any) error {
	binding := c.clusterBinding(statement.Collection())
	if binding == nil {
		return nil
	}
	program := compileConstraintProgram(binding, mutationWhere(statement))
	_, err := singleShardRoute(program, c.clusterRouter(), args)
	return err
}

// mutationWhere returns the WHERE predicate of an UPDATE or DELETE, or nil when
// the statement has none, mirroring how matchingKeysLocked reads it. A nil
// predicate never narrows routing, so a placed write without a shard-key
// predicate fails closed as a scatter.
func mutationWhere(statement *query.DMLStatement) *sqlast.Expr {
	switch statement.Tree().Kind {
	case sqlast.KindUpdate:
		if filter := statement.Tree().Update.Filter; filter != nil {
			return filter.Where
		}
	case sqlast.KindDelete:
		if filter := statement.Tree().Delete.Filter; filter != nil {
			return filter.Where
		}
	}
	return nil
}
