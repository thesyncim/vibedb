# Timer backpressure fixture race evidence — 2026-09-05

[Qualification index](../README.md)

This record preserves a Linux arm64 race reproduction of CI failure 33976341125
in `TestOwnerRejectedTimerOffersPreserveBoundedMessagesAndResume`. The test
released its fake Host and then sent a pulse without proving that the owner was
already retrying the blocked `RunOne`. The owner could drain the admitted tick
first, making the pulse a legitimate new admission and producing two applied
ticks. The correction adds a test-only blocked-`RunOne` handshake and
cancellation-aware cleanup. No production source changed.

## Source and method

The failing run used source `b0b16248878c0f282d15e1968782d5fc4b3ccd12` and
the fixed run used `5395306fc26c6061c1b42e7e266e2cafa39b01c9`. The fixed source
was later rebased onto `origin/main` at `2a8723ff723e326851407eacd583f8486f1b66f6`;
the resulting review commit is `ac20d838`.

Both runs used Go 1.27 in Linux arm64 Docker image
`sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`,
with `--network=none`, an anonymous volume for `GOCACHE` and `TMPDIR`, and
the repository mounted at `/workspace`. The test command was:

```sh
go test -race ./internal/raftservice \
  -run '^TestOwnerRejectedTimerOffersPreserveBoundedMessagesAndResume$' \
  -count=1000 -timeout=10m
```

The original command also used `-v`; the fixed output is compact. The raw
outputs are [before-linux-race.log](before-linux-race.log) and
[after-linux-race.log](after-linux-race.log). Their exact bytes are recorded
in [checksums.json](checksums.json).

## Results

| Source | Repetitions | Result |
| --- | ---: | --- |
| `b0b16248` before handshake | 1000 | 998 passed, 2 failed at the resume assertion |
| `5395306f` after handshake | 1000 | 1000 passed |

This is fixture scheduling evidence for the owner timer test. It does not
qualify production throughput or a broader distributed fault campaign.
