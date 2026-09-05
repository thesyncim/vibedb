# Read-authority evidence archive

This directory contains compact, repository-tracked copies of the retained
Linux RF3 diagnostics. They are correctness and failure evidence only. They do
not establish a fault-qualification pass, a throughput result, or a comparison
with CockroachDB.

The archive catalog is [`qualification-manifest.json`](qualification-manifest.json)
and the archive checksums are in [`SHA256SUMS`](SHA256SUMS). Every tarball also
contains `archive-provenance.json`, which records the source revision and patch
identity, build metadata and binary SHA-256 values, exact redacted argv where
the runner captured it, and SHA-256 values for each retained raw file.

| Archive | Evidence | Source identity | Outcome |
| --- | --- | --- | --- |
| [`authority-smoke-startup-failed.tar.gz`](archives/authority-smoke-startup-failed.tar.gz) | Initial short startup smoke | `bcfce760cb420c2f51d2c38f97e93d9e96031964`, dirty | Startup failed before authority proof; untracked production files were not captured, so provenance is incomplete. |
| [`authority-smoke-success.tar.gz`](archives/authority-smoke-success.tar.gz) | Short 3-node/4-group `point_hit,point_miss` smoke | `91d1fd119b8c227c5356cd0f4cac9f0937407303`, clean | Strict oracle completed; diagnostic smoke only and predates the final assessment candidate. |
| [`authority-runtime-linux-e3b3e53e.tar.gz`](archives/authority-runtime-linux-e3b3e53e.tar.gz) | Linux `internal/raftmember` runtime tests | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, source tree blobs included | Ten tests passed with no skips; test binary omitted after recording its exact hash and build metadata. |
| [`authority-fault-long-failed.tar.gz`](archives/authority-fault-long-failed.tar.gz) | Long 3-node/4-group authority run | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, dirty | `mixed_read_update` failed before any fault signal; point workloads passed. |
| [`authority-fault-retry-interrupted.tar.gz`](archives/authority-fault-retry-interrupted.tar.gz) | Retry with measuring-wait fix | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, dirty | `mixed_uniform` was interrupted while preparing; a 7.0500745-second process pause was recorded, with no restart. |

The two long-run archives retain manifests, control commands, client and node
logs, reports, diagnostics, inventories, published state, fault snapshots,
source patches, and the qualification runner snapshots. The short startup and
success archives retain their corresponding raw files. Docker volumes and all
compiled binaries are omitted to keep the repository artifacts compact; their
names, sizes, and SHA-256 values remain in each archive's provenance record.

The first startup smoke has a specific provenance limitation: its recorder
listed untracked production Go files but did not copy them. The successful
smoke is immutable at `91d1fd...`, while both long attempts use the dirty
`e3b3e53e...` checkpoint plus their recorded patch and runner snapshots. The
runtime ELF has no VCS build revision field, so its archive includes the
`internal/raftmember` Git blob list at `e3b3e53e...` and the exact `go
version -m` output.

To inspect an archive without restoring a Docker volume:

```sh
tar -tzf archives/authority-fault-retry-interrupted.tar.gz
tar -xzf archives/authority-fault-retry-interrupted.tar.gz -C /tmp
```
