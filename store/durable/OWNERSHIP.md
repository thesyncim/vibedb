# Durable collection ownership

This page defines ownership and persistence rules for contributors to
`store/durable`.

## Source input

A write batch owns copied keys and documents. A point put also copies the data
that must remain valid after return. Caller mutation after admission must not
change a staged durable operation.

Query and scan callbacks can receive borrowed data. The caller must not retain
it beyond the documented callback or snapshot lifetime.

## File descriptor

`durable.Create` and `durable.Open` borrow the caller's primary descriptor for
exclusive engine use. The caller must keep it open and must not read, write,
seek, truncate, lock, rename, replace, or unlink the file while the collection
is open.

Collection close releases engine use but does not close a caller-owned primary
descriptor.

The engine owns descriptors that it opens for recovery journals, direct I/O,
or internal database catalogs. It closes them during completed teardown.

## Published state

A published root and every immutable page that it references must not change.
A later generation can share an unchanged page.

The writer owns unpublished descriptors and scratch. It can recycle them only
after it proves that no published root or active lease references the related
extent.

## Snapshot lease

A durable snapshot pins one generation and owns mutable scratch state. It is
single-consumer and must be closed.

Close releases the lease and invalidates borrowed views. A leaked snapshot can
pin retired extents and can make mutation or collection close return a
retryable capacity or teardown error.

## Synchronization

One mutable collection owns one writer lease. Internal and OS locks reject a
second cooperating writer.

Readers use immutable published generations. The publisher serializes root and
journal transitions. A database transaction takes participant gates in stable
order and holds the database publication boundary across the participant cut.

External processes that ignore the locks remain outside this contract.

## Persistence failure

A journal append, journal sync, page write, or root publication failure can
poison the writer. Later writes and checkpoints fail until close and reopen.

An unknown root or decision outcome accepts no different retry. Reopen must
resolve the durable state first.

Teardown can release all resources and still return a sticky persistence error.
Use `CloseCompleted` to distinguish completed release from retryable teardown.

## Review checklist

- Identify the owner of every input, output, descriptor, page, and buffer.
- State whether returned bytes are copied or borrowed.
- Keep a backing object alive for every unsafe view.
- Reserve bounded resources before a write becomes irreversible.
- Prevent reuse while a root or snapshot can reference an extent.
- Test mutation, snapshot, failure, reopen, and close races.
- Test an exact retry after an unknown persistence outcome.
