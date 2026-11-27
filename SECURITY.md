# Security policy

> [!CAUTION]
> VibeDB is unreleased development software with no support window. Do not
> expose it to untrusted networks or store irreplaceable data. See
> [stability](docs/status.md).

## Supported versions and reporting

Security fixes land on `main` only; commits may be wire- and disk-incompatible.
This repository has no verified private reporting channel yet—do **not** post
exploits, credentials, or reproducers in public issues or PRs. Open a public
issue requesting a private channel first, and wait before sharing confidential
details (include commit, Go/OS/FS details, minimal reproducer, and impact).

## Trust boundaries

- **Embedded (`vibedb`, `query`, `sql/driver`):** Opens no sockets. The host
  application owns network exposure, auth, permissions, and backups. Writer
  locks coordinate cooperating VibeDB processes only—keep files, journals,
  `txn.vtm`, and directories in one trusted boundary.
- **Distributed services:** Require mTLS 1.3 with binary NodeIDs in a critical
  X.509 extension (DNS/CN ignored), per-class ALPN separation, and explicit
  capability policies ([security model](docs/design/security.md)).
- **Development plaintext & `pgwire`:** `-dev-plaintext-loopback`,
  `-pg-dev-listen`, and `pgwire.Trust()` are unauthenticated loopback-only
  conveniences—never expose them via proxies or port forwards. Remote `pgwire`
  listeners require `RequireTLS: true` + `SCRAM-SHA-256` (`PGTLSConfig` +
  `PGUsers`).
- **TLS rotation:** Rotating credentials revokes old-generation streams, which
  can leave an in-flight write outcome unknown—always retry with the exact
  retained request identity and bytes.
- **Secrets to protect:** TLS private keys/roots, `authorization-policy.vibejson`,
  `wal-key-source`, `durable-ack-key`, route seeds, session journals, backup
  repositories, and temporary Kind bootstrap directories.

## Source map

- [internal/servicetls/](internal/servicetls/) · [internal/serviceauthz/](internal/serviceauthz/) · [internal/rafttransport/identity.go](internal/rafttransport/identity.go)
- [pgwire/server.go](pgwire/server.go) · [pgwire/scram.go](pgwire/scram.go)
