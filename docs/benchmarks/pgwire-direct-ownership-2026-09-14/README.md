# Direct PostgreSQL write ownership

The direct PostgreSQL write lane previously copied every query through JSON
before preparation. This change copies the mutable parameter slices directly,
retains SQL as its immutable string, and keeps the exact JSON size check only
for requests whose conservative escaping bound can reach the journal limit.
The journal version validation and caller-buffer detachment remain unchanged.

## Isolated ownership cost

On an Apple M4 Max, the five-run `BenchmarkPostgreSQLDirectQueryOwnership`
benchmark reported these ranges and medians:

| path | ns/op range (median) | bytes/op | allocs/op |
| --- | ---: | ---: | ---: |
| JSON marshal/unmarshal | 3,108–7,672 (4,432) | 744 | 8 |
| typed clone | 401–726 (582) | 88 | 4 |

The typed ownership step is about 7.6× faster for this small bound request and
cuts its allocation volume by 88%. This is a preparation-side improvement;
durable Raft data and quorum barriers are unchanged.

## Matched SQL check

Both runs used source revision `ddb11394db8761683d3c4307925b6bf92cfaf027`,
CockroachDB `v26.3.1` at the runner's pinned digest, CRDB-first ordering, 8,192
rows, 2,000 measured `update_existing` operations, 1,000 warmups, three
repetitions, clients 1 and 8, and verification after every trial. Every trial
completed with zero errors and `verified=true`.

| build | C1 trials (ops/s) | C1 median | C8 trials (ops/s) | C8 median |
| --- | --- | ---: | --- | ---: |
| origin/main | 349.11, 398.06, 409.43 | 398.06 | 1,569.36, 1,370.37, 1,426.83 | 1,426.83 |
| typed ownership | 698.74, 271.20, 697.93 | 697.93 | 2,026.11, 2,095.20, 1,876.11 | 2,026.11 |

The paired run is directional rather than a stable end-to-end multiplier: the
CRDB C1 control median moved from 834.58 to 368.17 ops/s while its C8 median
was similar (2,724.35 versus 2,676.84). The SQL result therefore does not
support a 10× claim; the isolated ownership benchmark is the reliable signal.

Reproduction command:

```sh
PATH=/private/tmp/vibedb-go-shim:$PATH \
CODEX_AGENT_ID=write_validation \
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 2000 --scans 2000 \
  --warmup 1000 --repetitions 3 --clients 1,8 \
  --workloads update_existing --order crdb-first --timeout 45m
```

The runner removed its temporary container and volume after each run.

## Validation

All commands used `/Users/thesyncim/.codex/bin/project-env` and passed:

```sh
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test ./gateway -run 'TestReplicatedDirectMutation|TestDurableSQLPrepared' -count=1 -timeout=10m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test -race ./gateway -run 'TestReplicatedDirectMutation|TestDurableSQLPrepared' -count=1 -timeout=10m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test ./internal/gatewayruntime -count=1 -timeout=20m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test -race ./internal/gatewayruntime -run 'TestPostgreSQLDirectQueryOwnership|TestPostgreSQLDirectParallelIdentityUniqueness|TestPostgreSQLDirectUnknownRetainsExactCommand' -count=1 -timeout=10m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test ./internal/raftservice -run '^TestProposalIngress' -count=1 -timeout=10m
(cd integration/pgclient && CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test ./cmd/rf3-sqlbench -count=1 -timeout=10m)
```

The ownership tests compare typed cloning with the legacy JSON result, mutate
caller parameter and type buffers after cloning, cover nil/empty `ParamTypes`
normalization, and verify the exact large-input transaction byte limit before
preparation.
