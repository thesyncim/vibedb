# Fused-node transport qualification checkpoint

Owned transport source is committed at `45ec6010`. These are correctness and
race-test results, not throughput measurements or whole-node qualification.
Luna implemented the draft; Sol independently reviewed, fixed and tested it.

The retained package run covers gateway, shardservice, rafttransport and
raftservice. The selected Linux race run requires strict filesystem allocation
and uses actual three-voter RF3 owners, SQL apply and authenticated TLS. The
verbose log explicitly reports PASS for the two real-owner tests without SKIP.
It includes loss of quorum at a local owner. Store data lived on a fresh Docker
volume; source and the module cache were read-only host mounts. Each test used
fresh temporary store paths. The exact command and environment are retained
in `linux-command.txt`; all original logs have hashes in `sha256.json`.

The added semantic differential fixture uses one Raft group. It covers local
versus TLS SQL results, stale schema, output bounds, authorization changes at
ReadIndex admission, retained result storage, isolated-owner refusal and credit
release. Other selected tests cover frame bounds, admission, credential
expiry/rotation and counters. See `review-handoff.md` for exact scope and fixes.

This does not establish the full fused-process startup/reload/shutdown behavior,
multi-group crash recovery, every direct-issuer/fallback failure permutation,
schema retirement, six-node scaling, or any performance improvement. Those
remain separate gates. Other runtime and provisioning files were under active
implementation when these owned-package checks ran; this is not a clean
whole-candidate qualification record.
