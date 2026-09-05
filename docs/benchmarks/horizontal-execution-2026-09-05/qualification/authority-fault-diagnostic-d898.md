# d898 read-authority fault diagnostic

Status: **diagnostic evidence only; no qualification pass and no performance claim**.

The run used clean source revision
`d898f062726bdd0070d402e1b41463bbd7090434`, the pinned
`golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`
arm64 image, three physical nodes, and four RF3 groups. It ran 60,000
operations, 500 scans, warmup 1000, clients 1 and 8, and
`point_hit,point_miss,mixed_read_update,mixed_uniform`. The strict client
oracle completed all 480,000 samples with zero errors and exit status 0.

The selected process was node
`7f28932b0c5441531426ecc72377a5cd`, PID 293. The diagnostic recorded a
SIGSTOP/SIGCONT pause of 7.041228167 seconds, then a same-volume SIGKILL
restart. Restart readiness was 7.634868084 seconds and recovery completed with
exit status 0. The configured 5-second grant and approximately 6.11-second
restart quarantine are elapsed-clock settings rather than hard wall-clock
upper bounds; the deployment assumption is a clock rate within ±10%, including
VM suspension. A quarantine boolean was unavailable, so no quarantine proof is
claimed.

The first run manifest is preserved unchanged with status
`incomplete-or-failed`, failure `diagnostic: post-CONT diagnostic latch was not
retained: latch armed has invalid UTC`, and SHA-256
`eefdcce6af86a6e473bd36378d1b008266f87d59193a0d5171656fc4e514355c`. The
[derived offline validation report](authority-fault-diagnostic-d898-offline-validation.json)
uses integer epoch nanoseconds to validate the retained post-CONT latch and
records `qualification_claimed=false`. Its validator revisions are
`6f17929fbffadb8bc247b3f5f73030085b2bf201` and
`662407f12f9abc98501e85d6db52cd856132be4b`.

The [filtered raw archive](archives/authority-fault-diagnostic-d898.tar.gz) is
555,366 bytes with 98 retained raw files plus its embedded manifest. It keeps
the client oracle and logs, 48 per-node workload snapshots, per-group snapshots
and the failed-table timeline, pause/restart state, recovery records, control
commands, and source manifests. The [companion provenance record](archives/authority-fault-diagnostic-d898-provenance.json)
contains a checksum for every included file and hashes for omitted compiled
binaries, published data, full SQL reports, and historical source patch
payloads. Docker volumes and database/WAL contents are omitted. The archive
SHA-256 is
`854c6cfa022a42c39b92af500ca4d3f15257914d1e15f1162aa61fda05a530b3`; the
provenance SHA-256 is
`1337ca40dc60b9b0eed93ffff51a46cf3782827c5b53ae63b79e68e03c0d2a4b`; and the
derived report SHA-256 is
`a4c7051524f553ceff438becc0a36b491792ea0f8c613b61be6821861c6f977a`.

The archive and report are cataloged in
[`qualification-manifest.json`](qualification-manifest.json) and verified by
[`SHA256SUMS`](SHA256SUMS).
