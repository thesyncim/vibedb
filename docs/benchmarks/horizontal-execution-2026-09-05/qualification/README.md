# Read-authority evidence archive

This directory contains compact, repository-tracked copies of the retained
Linux RF3 diagnostics. They are correctness and failure evidence only. They do
not establish a fault-qualification pass, a throughput result, or a comparison
with CockroachDB.

The archive catalog is [`qualification-manifest.json`](qualification-manifest.json)
and the archive checksums are in [`SHA256SUMS`](SHA256SUMS). Each run archive
retains its run manifest and raw evidence. The d898 filtered archive has a
[companion provenance record](archives/authority-fault-diagnostic-d898-provenance.json)
with source and build identity, binary SHA-256 values, and a SHA-256 for every
retained raw file.

| Archive | Evidence | Source identity | Outcome |
| --- | --- | --- | --- |
| [`authority-smoke-startup-failed.tar.gz`](archives/authority-smoke-startup-failed.tar.gz) | Initial short startup smoke | `bcfce760cb420c2f51d2c38f97e93d9e96031964`, dirty | Startup failed before authority proof; untracked production files were not captured, so provenance is incomplete. |
| [`authority-smoke-success.tar.gz`](archives/authority-smoke-success.tar.gz) | Short 3-node/4-group `point_hit,point_miss` smoke | `91d1fd119b8c227c5356cd0f4cac9f0937407303`, clean | Strict oracle completed; diagnostic smoke only and predates the final assessment candidate. |
| [`authority-diagnostic-2c887f4a.tar.gz`](archives/authority-diagnostic-2c887f4a.tar.gz) | Completed no-fault 3-node/4-group workload diagnostic | `2c887f4a20180f15be353a8492e27067e0a0b19d`, clean | Strict workload oracle checked 480,000 samples with zero errors; its 536 complete 21-member sidecar cycles contained 11,256 authentication/protocol sampling errors, so diagnostic cuts are invalid and provide no causal or fault-qualification proof. |
| [`authority-runtime-linux-e3b3e53e.tar.gz`](archives/authority-runtime-linux-e3b3e53e.tar.gz) | Linux `internal/raftmember` runtime tests | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, source tree blobs included | Ten tests passed with no skips; test binary omitted after recording its exact hash and build metadata. |
| [`authority-fault-long-failed.tar.gz`](archives/authority-fault-long-failed.tar.gz) | Long 3-node/4-group authority run | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, dirty | `mixed_read_update` failed before any fault signal; point workloads passed. |
| [`authority-fault-retry-interrupted.tar.gz`](archives/authority-fault-retry-interrupted.tar.gz) | Retry with measuring-wait fix | `e3b3e53e04a268d6d5a76ffab5fa14366bec255f`, dirty | `mixed_uniform` was interrupted while preparing; a 7.0500745-second process pause was recorded, with no restart. |
| [`authority-fault-diagnostic-d898.tar.gz`](archives/authority-fault-diagnostic-d898.tar.gz) | Full no-network-fault 3-node/4-group diagnostic with pause and same-volume restart | `d898f062726bdd0070d402e1b41463bbd7090434`, clean | Strict oracle completed 480,000 samples with zero errors; post-CONT latch validation passed offline with exact timestamp ordering; restart recovered; diagnostic only, with no qualification claim and no observable quarantine boolean. |

## d898 fault diagnostic and derived validation

The d898 run used 60,000 operations, 500 scans, warmup 1000, clients 1 and 8,
and all four workloads (`point_hit,point_miss,mixed_read_update,mixed_uniform`)
on three physical nodes hosting four RF3 groups. The strict client oracle
completed 480,000 samples with zero errors and exit status 0. The run was a
diagnostic workload with a process pause and same-volume restart; it was not a
network partition and it makes no throughput or qualification claim.

The selected node was `7f28932b0c5441531426ecc72377a5cd`, PID 293. Its
SIGSTOP/SIGCONT interval was 7.041228167 seconds. The configured read-authority
grant of 5 seconds and restart quarantine of about 6.11 seconds are elapsed
clock settings, not hard wall-clock upper bounds; the deployment assumes clock
rates within ±10%, including virtual-machine suspension effects. The run
recorded restart readiness after 7.634868084 seconds, recovered on the same
volume with exit status 0, and did not expose a quarantine boolean.

The original run manifest remains retained and unchanged with status
`incomplete-or-failed`, failure `diagnostic: post-CONT diagnostic latch was not
retained: latch armed has invalid UTC`, and SHA-256
`eefdcce6af86a6e473bd36378d1b008266f87d59193a0d5171656fc4e514355c`. The
[derived offline report](authority-fault-diagnostic-d898-offline-validation.json)
validates the retained post-CONT latch using integer epoch nanoseconds and
records `qualification_claimed=false`; it does not rewrite or reclassify the
original manifest.

The filtered archive is 555,366 bytes and contains 98 retained raw files plus
its embedded manifest. It retains the client oracle and logs, 48 per-node
workload diagnostic snapshots, per-group snapshots and the failed-table
timeline, pre/post pause and restart state, recovery records, control commands,
and source manifests. The [provenance record](archives/authority-fault-diagnostic-d898-provenance.json)
records every included-file checksum and the hashes of omitted payloads:
compiled binaries (186,349,310 bytes), the 486-file published data tree
(`abbe874fcd926482d4a1a17145adc18625200b2444c008252f1814b4aab262f1`), the
70,019,644-byte full SQL report
(`4d313ef2365dd03c147bf310683730a7a75dc875041434a004ebdb0713d7d3aa`), and
the zero-byte historical source patch payloads. Docker volumes and database/WAL
contents are omitted. The archive SHA-256 is
`854c6cfa022a42c39b92af500ca4d3f15257914d1e15f1162aa61fda05a530b3`.

The [human-readable run report](authority-fault-diagnostic-d898.md) links these
artifacts and records the exact binary and validator revisions.

The two long-run archives retain manifests, control commands, client and node
logs, reports, diagnostics, inventories, published state, fault snapshots,
source patches, and the qualification runner snapshots. The short startup and
success and no-fault diagnostic archives retain their corresponding raw files.
The latest no-fault diagnostic's workload report is an oracle result only: it
completed all four workloads at both client counts with 480,000 verified
samples, while the independent per-group sidecar produced 536 complete
21-member cycles but 11,256 authentication/protocol sampling errors. Docker
volumes and all compiled binaries are omitted to keep the repository artifacts
compact; their names, sizes, and SHA-256 values remain in each archive's
provenance record.

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
