# Security policy

> [!CAUTION]
> VibeDB is unreleased development software with known defects and no support
> window. Do not expose it to untrusted networks or use it for sensitive or
> irreplaceable data. See [current status](docs/status.md).

## Supported versions

There are no supported versions. Security changes land on `main`; consumers
must select, audit, and test a fixing commit themselves. Different commits may
be wire- and disk-incompatible.

## Report a vulnerability

This repository does not currently publish a verified private vulnerability
reporting channel. Do not place exploit details, credentials, private data, or
a reproducer in a public issue, pull request, discussion, or commit.

You may open a public issue containing only a request for maintainers to enable
and verify a private reporting channel. Wait for that verified channel before
sending confidential material. There is no promised response or disclosure
deadline.

A useful private report should include:

- exact commit and dirty state;
- Go version, OS, architecture, filesystem, and build flags;
- the smallest safe reproducer;
- confidentiality, integrity, availability, or durability impact;
- a method to verify the fix without exposing secrets.

## Embedded boundary

The `vibedb`, `query`, and `sql/driver` packages open no listeners. The
embedding process owns network exposure, authentication, authorization,
filesystem permissions, backup protection, and process isolation.

Writer locks coordinate cooperating VibeDB processes only. They do not protect
against an administrator or external process that truncates, replaces, copies,
or edits a live file. Keep database files, journals, transaction logs, catalogs,
keys, and parent directories in one trusted administrative boundary.

## Network boundary

Distributed commands require TLS and an authorization policy except for
explicit literal-loopback development modes. Service identity comes from an
exact certificate extension and binary NodeID—not DNS names, certificate
subjects, or common names. Traffic classes and capabilities are separate.

The `-dev-plaintext-loopback` and `-pg-dev-listen` paths are unauthenticated
development conveniences. Do not expose them through a proxy, container port
mapping, tunnel, or port forward. The local RF3 tutorial uses trust auth and no
TLS intentionally; it is not a deployment template.

The standalone pgwire server must be configured with SCRAM, TLS,
`RequireTLS`, bounded deadlines, and an appropriate listener for any untrusted
boundary. `Trust()` is local-only. SSLRequest negotiation is implemented;
direct TLS and SCRAM-SHA-256-PLUS are not.

TLS rotation closes old-generation streams. A response lost after request send
can have an unknown outcome. Mutation protocols must retry the exact retained
identity and bytes, never synthesize a new command because a connection closed.

## Secrets and state

Protect at least:

- TLS private keys and roots;
- authorization policies and identity manifests;
- Raft WAL keys and durable acknowledgement keys;
- gateway session journals and route seeds;
- backup repositories, restore permits, and generated development PKI.

The Kubernetes qualification script creates disposable credentials and leaves
its private temporary directory for inspection. Remove that directory safely
after collecting required evidence.

## Current security-relevant gaps

At the audited commit, the malformed SQL string `SELECT 1 GARBAGE` is
misclassified as read-only by the service authorization classifier, and its
test fails. Do not treat current distributed SQL authorization as production
qualified. Other known sharp edges are listed in [current status](docs/status.md).

## Source map

- `internal/servicetls` and `internal/rafttransport/identity.go`
- `internal/serviceauthz` and `gateway/client_tls.go`
- `pgwire/server.go`, `session.go`, and `scram.go`
- `cmd/vibedb-gateway/serve.go` and `cmd/vibedb-shard/*.go`
