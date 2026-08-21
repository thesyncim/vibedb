# Select an API

VibeDB has four user entry points. They share storage and query components, but
they have different lifecycle and compatibility contracts.

| Entry point | Use it for | Primary package |
| --- | --- | --- |
| Native facade | A small embedded JSON database API | `github.com/thesyncim/vibedb` |
| Typed query API | Programmatic JSON query construction and reusable execution state | `github.com/thesyncim/vibedb/query` |
| SQL driver | Go applications that use `database/sql` | `github.com/thesyncim/vibedb/sql/driver` |
| PostgreSQL wire | PostgreSQL clients that connect over TCP | `github.com/thesyncim/vibedb/pgwire` |

Use the [native API](native.md) unless you need SQL or direct control of query
execution. Use [SQL](sql.md) when an application already uses `database/sql`.
Add the [wire server](pgwire.md) when a PostgreSQL client must connect to the
same SQL catalog.

Packages `store` and `store/durable` expose lower-level storage controls. These
packages have more configuration and more ownership rules. Read the [storage
model](../store.md) before you use them directly.
