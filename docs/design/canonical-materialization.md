# Canonical page materialization

Canonical materialization is an opt-in durable path that can replace eligible
canonical pages in place. It uses a recovery capsule with complete before-image
regions.

## Configuration

`MaterializationDamageGranule` is a storage-stack assertion. It states the
largest complete region that a power failure can damage. Zero disables this
path.

The value is stored in the file and checked on each open. It is not inferred
from a filesystem or device block size.

Canonical sparse writes currently require buffered write mode. Direct-I/O
alignment depends on the device and is refused for this path.

## Fixed capsules

The mutable file prefix contains two 4 KiB materialization capsules. An
eligible change must fit all complete before-image sectors and metadata in one
bounded capsule.

The protocol records the before image, orders it before the canonical change,
validates the after image, and then publishes the root state. Recovery can undo
an incomplete canonical replacement.

If a mutation does not meet the eligibility and capacity checks, it uses the
ordinary copy-on-write path.

## Validation

Tests inject failure at capsule, data, and root cuts. They also run recovery a
second time to prove idempotence.

This injected model does not prove the asserted damage granule for a real
storage stack. Deployment evidence must establish that value.

## Implementation references

- `internal/storeio/materialization_journal.go`
- `internal/storeio/committer_materialization.go`
- `store/durable/store_file_options.go`
- `internal/storeio/mutable_file_layout.go`
