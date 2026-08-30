# Security policy

## Supported revisions

This development repository has no published support window. Security fixes
land on `main`. Consumers must move to a fixing commit after they review and
test it for their deployment.

## Report a vulnerability

This repository does not currently have a configured private vulnerability
reporting channel. GitHub private vulnerability reporting is not enabled. Do
not put exploit details, private data, affected code paths, or a reproducer in
a public issue, pull request, discussion, or commit.

You can open a public issue that contains only a request for the maintainers to
establish a private reporting path. Do not include vulnerability details in
that request. Wait until the maintainers publish and verify a private channel
before you send confidential information.

After a private channel is available, include this information in the report:

- The affected commit
- Go version, architecture, operating system, and build flags
- The smallest available reproducer
- The expected confidentiality, integrity, availability, or durability impact
- A safe method to validate a fix

Relevant reports include:

- Authentication or authorization bypass
- SQL, JSON, catalog, or protocol validation errors
- Out-of-bounds access or unsafe-pointer lifetime errors
- Data corruption or invalid recovery acceptance
- Unbounded resource use or denial of service
- A stale topology or identity fence that permits unintended execution
- A durability acknowledgement that does not match the selected contract

The project does not promise a fixed response or disclosure deadline. After a
maintainer accepts a report through a configured private channel, the report
must stay private while maintainers reproduce the issue and validate a fix.

### Maintainer release action: enable private vulnerability reporting

Before a release, maintainers must enable GitHub private vulnerability
reporting for this repository. They must verify that a non-maintainer can open
the private report form, then replace this section with the tested reporting
path. Do not publish a private-reporting link before that verification passes.

## Network boundary

The embedded `vibedb`, `query`, and `sql/driver` packages do not open network
listeners. Applications that expose them own the surrounding network and
authorization boundary.

The `vibedb-gateway serve` and `vibedb-shard serve` commands require mutual TLS
and a canonical authorization policy by default. Their
`-dev-plaintext-loopback` flag is an explicit unauthenticated development mode.
It is mutually exclusive with TLS and policy flags, and the commands reject a
non-loopback listener. Do not use a proxy, container port mapping, SSH tunnel,
or port forward to make that plaintext listener reachable from an untrusted
network.

`vibedb-shard serve-rf3` has no plaintext mode. Its peer, native, snapshot, and
control listeners use TLS 1.3 profiles that authenticate exact node identities.
The native and control paths also enforce capabilities from the configured
authorization policy. The gateway's authenticated client boundary checks the
complete request semantics before dispatch, and shards independently check
delegated requests.

The standalone `pgwire` package has a separate configuration boundary.
`pgwire.NewServer` requires the caller to select `SCRAM(...)` or `Trust()`
explicitly, but TLS is enabled only when the caller supplies `TLSConfig`.
Plaintext is rejected only when `RequireTLS` is true. Use SCRAM, configure TLS,
set `RequireTLS`, and bind an appropriate listener for an untrusted network.
`Trust()` and the gateway's `-pg-dev-listen` endpoint are for a trusted local
development boundary only.

TLS authenticates service and client identities. It does not make every
principal an operator. Keep authorization policies least-privileged. Protect
certificate private keys, WAL keys, durable ACK keys, and retained journals.
Rotate a policy generation when access changes.

## Local-file boundary

Writer locks coordinate cooperating VibeDB processes. They do not protect a
file from an external process that truncates, replaces, copies, or edits it.
Keep database files, journals, lock entries, catalogs, and parent directories
inside the same trusted administrative boundary.

## Implementation references

- `cmd/vibedb-gateway/serve.go`: `runServe` and `requireLoopbackListen`
- `cmd/vibedb-shard/main.go`: `runServe` and `requireLoopbackListen`
- `cmd/vibedb-shard/serve_rf3.go`: `servePreparedRF3`
- `internal/rafttransport/identity.go`: `PeerTLS`
- `gateway/client_tls.go`: `ClientTLS.ServeAuthorizedClients`
- `shardservice/server.go`: `Server.ServeAuthorizedConn`
- `pgwire/server.go`: `Options` and `NewServerWithBackend`
