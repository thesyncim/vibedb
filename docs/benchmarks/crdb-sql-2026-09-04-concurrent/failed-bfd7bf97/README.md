# Failed precursor: excluded from the final comparison

Clean source `bfd7bf97`. The first eight-client update trial failed one operation
because read-only preparation exhausted the leader's response admission budget
(`ReplicatedRefusalAdmissionBound`, code 2). No write was proposed by that failed
invocation. The run stopped at that failed trial; it has no valid eight-client
update throughput median. All raw samples, including the error, are retained.

The follow-up adds bounded preparation backoff and removes the idle Raft batching
wait. Its complete rerun is reported in the parent directory. A small diagnostic
read of this failed JSON was performed during the precursor's CockroachDB run;
this precursor is not used as comparative evidence for the final change.
