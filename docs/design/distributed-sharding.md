# Static distributed sharding

The shipped distributed service uses static shard ownership. It does not start
Raft replication or automatic movement.

## Components

- A gateway catalog defines distributions, table placements, manifests, and
  endpoints.
- `vibedb-gateway` pins one catalog generation, plans a route, and merges
  results.
- `vibedb-shard` serves one local SQL catalog with one static ownership
  identity.

## Ownership identity

Each shard request carries distribution, shard ID, allocation generation,
routing version, and ownership epoch. The shard validates these fields before
SQL execution.

The service makes a durable local serving claim. The claim prevents a stale
restart on the same store. It does not revoke a process that serves a copied
store.

## Placement

A manifest covers the complete 64-bit keyspace with ordered adjacent half-open
ranges. A shard has a unique nonzero allocation generation and at least one
leader endpoint.

The router selects the first leader only. It does not balance or fail over
between endpoint entries.

A targeted route uses a bounded leading placement prefix. A shorter prefix can
map to the complete keyspace. Admission policy decides whether scatter is
permitted.

## Non-serving range splits

`autosplit.PlanSplit` validates one desired split manifest. The plan has at
most three child ranges. One child keeps the source allocation.

`internal/rangesplit` implements the non-serving data plane for that plan. It
uses one source scan and one compiled `vibejson` index per row. It sends each
borrowed key and document to one child and can skip the retained child copy.

The package writes bounded hash-chained artifacts for the other children. Its
verifier checks the complete artifact chain, strict key order, and the
placement of every document before a durable callback receives a chunk.

The tail translator accepts consecutive exact before-and-after transitions.
It emits one digested batch for every child at every source entry. A
shard-key move becomes a delete from the old child and a put to the new child.
An entry with no row change still advances every child.

This kernel does not capture the source transition stream. It does not persist
or apply destination batches, close the final source gap, prune the retained
range, transfer ownership, or publish the catalog.

## Security boundary

The gateway and shard commands accept loopback listeners only. Their protocols
have no authentication or TLS.

See [the operating guide](../operations/distributed.md) for exact commands and
[the distributed design](distributed-system.md) for the non-serving kernel
boundary.

## Implementation references

- `distribution/manifest.go`, `placement.go`, and `router.go`
- `gateway/catalog.go` and `executor.go`
- `shardservice/admit.go` and `server.go`
- `cmd/vibedb-gateway` and `cmd/vibedb-shard`
- `autosplit/action.go`
- `internal/rangesplit/partition.go`, `artifact.go`, and `tail.go`
