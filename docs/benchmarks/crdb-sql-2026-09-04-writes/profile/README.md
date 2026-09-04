# Diagnostic profiles, not benchmark timings

Clean revision `dc48cd2e`, 1,024 rows, 1,024 point/update operations, 128 scan
operations, 32 warmups, one repetition, clients 1 and 8. CPU profiling and the
Go execution flight recorder were enabled on all VibeDB servers. Both engines
completed, but profiling timings are excluded from the performance comparison.

Retained here: gateway and data-member-1 CPU profiles and gzip-compressed raw Go
traces, hashes of their uncompressed bytes, the run manifest, client log, exit
codes, and the gateway region summary. Other member profiles remain in the local
run directory `/private/tmp/vibedb-sql-profile-live-completions/raw/profiles`.

To reproduce the capture from the recorded revision:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /tmp/vibedb-profile-reproduction \
  --profile --rows 1024 --operations 1024 --scans 128 --warmup 32 --repetitions 1
```

Decompress a gateway `.trace.gz` file to a temporary file, then use Go 1.27:

```sh
go tool trace -d=parsed /tmp/gateway.trace > /tmp/gateway-events.txt
python3 scripts/bench/summarize-go-trace-regions.py /tmp/gateway-events.txt
go tool pprof -top -cum gateway-372-1788508563551395631.cpu.pprof
```

Select the actual CPU profile filename from this directory. If a Go distribution
omits the prebuilt tools, build `cmd/trace` and `cmd/pprof` with that same Go
version. `pg.write` includes waiting for the table gate; its nested regions must
not be added to it. Unmatched flight-recorder region edges are reported and
excluded from duration statistics. p95 uses nearest rank; p50 is the median.
