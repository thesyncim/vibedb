# RF3 external fault qualification

[Performance guide](../../docs/performance.md) · [Evidence levels](../competitive/README.md#evidence-levels)

Runs the 3-process `vibedb-shard serve-rf3` fault harness repeatedly and writes
canonical TSV rows. Requires Linux (`/proc` RSS/IO, `SIGSTOP`/`SIGCONT`/
`SIGKILL`, and WAL allocation support; skips on Darwin fail closed).

## Run

```sh
go run ./bench/rf3chaos -output "$(pwd)/rf3-chaos.tsv" -runs 9 -timeout 5m
```

`-output` must be an absent absolute path; `-runs` is 1–1,024 (1 for local
feedback; ≥9 for publication evidence). The test binary is built and hashed
once, then run in a fresh process per repetition.

## Checks per repetition

- Quorum election while leader is `SIGSTOP`ped; stale linearizable read fence
  rejection on resume.
- Leader `SIGKILL` before proposal and in lost-response/unread-response windows
  with byte-identical recovery.
- Two asymmetric leader↔follower partition/restart/heal loops via directional
  proxies.
- Four waves of 64 waiters with bounded child-RSS and WAL allocation growth,
  plus durable ACK survival after full 3-replica restart.

Elapsed time covers the whole harness (not isolated failover latency) and does
not test gateway processes, horizontal scaling, or multi-host networks.
