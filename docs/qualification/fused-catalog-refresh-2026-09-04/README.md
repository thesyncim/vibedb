# Physical frontend visibility and retained-root restart

Frozen revision `6402842cb2a2942c0097f6df251c49ef49bc7acc` passes
`TestFusedRF3NodeProcessQualification` on Go 1.27 with `GOEXPERIMENT=simd`,
Linux ARM64. Both subtests ran; none skipped. The complete test took 76.137s.
Source and module-cache mounts were read-only, and test roots were fresh in
the Linux volume. The completed container was removed.

| Layout | Serving processes killed | User tables | Acknowledged rows | PG frontends checked | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Default physical3 | 3 | 3 | 18 | 1 | Pass, 33.46s |
| Explicit physical6 | 6 | 3 | 18 | 6 | Pass, 31.98s |

Both layouts also check native point reads through every physical frontend.
The test freezes the supervisor, verifies signal-9 exits of the exact serving
children, resumes and joins the supervisor, then opens the same roots. It
checks unchanged topology and identity manifests and repeats the complete
acknowledged-row oracles. The raw JSONL records both crash injections and
successful restart verification.

`baseline.json` retains the two pre-crash failures at `7dc21395`: physical3
native frontend2 could not resolve the table, and physical6 PostgreSQL
preparation reported missing catalog placement. It includes the hash and local
path of the full failing log. `review-verification.txt` records the focused
normal/race review checks. The successful source includes the later upstream
bounded-scan merge and catalog refresh fix; it excludes the unfinished batch
overlay and diagnostic-counter changes.

`fixed-command.txt`, `fixed-process.jsonl`, `fixed.json` and `sha256.json`
retain the exact command, all successful test output, source/image identity
and evidence hashes. No database files, node manifests or credentials are
archived here.

This clears the specific serving/restart gate. It does not measure performance
or prove single-node-failure availability, partitions, lost-reply retries,
interactive PostgreSQL SERIALIZABLE parity or scaling across independent hosts.
