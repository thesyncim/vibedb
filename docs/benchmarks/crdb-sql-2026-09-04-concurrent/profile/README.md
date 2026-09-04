# Diagnostic profile, excluded from benchmark timings

Clean source `3e9d2306`; 1,024 rows, 1,024 point/update operations, 128 scan
operations, 32 warmups, one repetition, clients 1 and 8. Both engines completed.
CPU profiling and Go execution traces were enabled on all VibeDB processes.

The gateway and data-member-3 (the data leader) traces and CPU profiles are
retained, with SHA-256 hashes of uncompressed bytes, manifest, client log and
process/leader mapping. Other profiles remain in the local capture directory
`/private/tmp/vibedb-sql-profile-direct-pool/raw/profiles`.

`regions.json` comes from Go 1.27 parsed trace events. All 2,128 preparation,
execution and write regions have matched begin/end edges. Region means include
waiting and initialization; nested durations must not be added to the parent.
`pg.direct.prepare` includes bounded read-admission backoff. The three outbox
saves are initialization work, not one save per direct request.

`gateway.syscall.txt` and `data-leader.syscall.txt` are syscall-delay profiles.
They include idle profiler/signal goroutines. Their percentage totals are not
CPU utilization or the write critical path. The data leader's approximately
0.80 s fdatasync and 0.31 s fsync totals cover the whole capture, not only updates.

Reproduce from the recorded revision:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /tmp/vibedb-concurrent-profile \
  --profile --rows 1024 --operations 1024 --scans 128 --warmup 32 --repetitions 1
```

After decompressing a gateway trace to `/tmp/gateway.trace`:

```sh
go tool trace -d=parsed /tmp/gateway.trace > /tmp/gateway-events.txt
python3 scripts/bench/summarize-go-trace-regions.py /tmp/gateway-events.txt
go tool trace -pprof=syscall /tmp/gateway.trace > /tmp/gateway-syscall.pprof
go tool pprof -top /tmp/gateway-syscall.pprof
```

If the Go distribution lacks prebuilt tools, build `cmd/trace` and `cmd/pprof`
with the same Go 1.27 version. The normal comparison ran separately with
profiling disabled and no concurrent tests or builds.
