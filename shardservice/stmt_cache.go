package shardservice

import (
	"context"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Bounds for the per-connection prepared-statement cache. A served
// connection executes its application's small statement set repeatedly; 16
// entries cover interleaved point reads and writes while keeping retained
// SQL text bounded. Longer texts bypass the cache instead of pinning
// unbounded request bytes.
const (
	shardStmtCacheMax    = 16
	shardStmtCacheMaxSQL = 4096
)

// shardStmtKey identifies one cacheable preparation: the exact SQL text, the
// wire parameter-type signature (nil and empty are the same empty signature),
// and the partial-aggregate lowering mode, which produces a different plan
// from the same text.
type shardStmtKey struct {
	sql     string
	params  string
	partial bool
}

func shardStmtCacheKey(
	sqlText string,
	parameterTypes []sqldriver.ParamType,
	partialAggregate bool,
) (shardStmtKey, bool) {
	if len(sqlText) == 0 || len(sqlText) > shardStmtCacheMaxSQL ||
		len(parameterTypes) > maxParams {
		return shardStmtKey{}, false
	}
	var signature string
	if len(parameterTypes) != 0 {
		types := make([]byte, len(parameterTypes))
		for index, parameterType := range parameterTypes {
			types[index] = byte(parameterType)
		}
		signature = string(types)
	}
	return shardStmtKey{sql: sqlText, params: signature, partial: partialAggregate}, true
}

// prepareCached returns a prepared statement for one request, reusing the
// connection's cached preparation when the SQL text, parameter types, and
// lowering mode match and the catalog/layout generation the cached plan was
// lowered against still governs execution.
//
// The returned release must run when the request finishes: it is a no-op for
// a cache hit and closes a freshly prepared statement that could not be
// cached. A borrowed hit must never be closed by the caller; every cached
// statement dies with the session at connection teardown.
func (c *shardConn) prepareCached(
	ctx context.Context,
	sqlText string,
	parameterTypes []sqldriver.ParamType,
	partialAggregate bool,
) (*sqldriver.Prepared, func(), error) {
	// A served connection is driven by one loop goroutine, so the cache
	// needs no lock; entries live until session teardown closes them.
	key, cacheable := shardStmtCacheKey(sqlText, parameterTypes, partialAggregate)
	if cacheable {
		if prep, hit := c.stmtCache[key]; hit {
			if prep.LayoutCurrent() {
				return prep, func() {}, nil
			}
			// A generation boundary retired this entry. Drop it before a
			// fresh preparation replaces the slot below.
			c.evictStmt(key)
		}
	}
	prep, err := prepareShardSQL(ctx, c.sess, sqlText, parameterTypes, partialAggregate)
	if err != nil {
		return nil, nil, err
	}
	if cacheable && prep.LayoutCurrent() {
		c.storeStmt(key, prep)
		return prep, func() {}, nil
	}
	return prep, func() { _ = prep.Close() }, nil
}

// storeStmt inserts a preparation, evicting the oldest entry when the cache
// is full. A replaced entry is closed so its compiler arenas are released;
// entries otherwise live until session teardown closes them.
func (c *shardConn) storeStmt(key shardStmtKey, prep *sqldriver.Prepared) {
	if c.stmtCache == nil {
		c.stmtCache = make(map[shardStmtKey]*sqldriver.Prepared, shardStmtCacheMax)
	}
	if _, exists := c.stmtCache[key]; !exists {
		for len(c.stmtOrder) >= shardStmtCacheMax {
			c.evictStmt(c.stmtOrder[0])
		}
		c.stmtOrder = append(c.stmtOrder, key)
	} else if old := c.stmtCache[key]; old != prep {
		_ = old.Close()
	}
	c.stmtCache[key] = prep
}

func (c *shardConn) evictStmt(key shardStmtKey) {
	prep, ok := c.stmtCache[key]
	if !ok {
		return
	}
	delete(c.stmtCache, key)
	_ = prep.Close()
	for index, queued := range c.stmtOrder {
		if queued == key {
			c.stmtOrder = append(c.stmtOrder[:index], c.stmtOrder[index+1:]...)
			break
		}
	}
}
