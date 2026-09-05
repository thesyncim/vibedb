# Documentation style

[Documentation](README.md) / Contributing

Write for the reader's next decision. A page should explain what they can do,
show the necessary steps or model, and link the exact behavior when more detail
is needed.

## Put each kind of content in its place

| Content | Home | Shape |
| --- | --- | --- |
| Project introduction | Root README | What VibeDB is, one example, interface choices, next links. |
| Tutorial | Getting started or local cluster | Prerequisites, numbered steps, expected output, a verification step. |
| API guide | `docs/api` | Usage, ownership, errors, focused examples. |
| Design explanation | Design index and linked technical guides | Components, data flow, invariants, and tradeoffs. |
| Operator procedure | `docs/operations` | Preconditions, commands, success checks, failure and recovery steps. |
| Reference | `docs/reference` | Exact syntax, defaults, limits, supported behavior. |
| Research record | Research index and linked proposals | Date/revision, hypothesis, evidence, remaining work. |
| Measurement | `docs/benchmarks` or `docs/qualification` | Exact revision, method, raw artifacts, results, limitations. |

Keep current guides separate from work plans and dated run reports. Do not
put conversation history, model assignments, permission discussions, or a
running task log into the README or an operating procedure. Historical
baselines belong in the record that used them.

## Lead with purpose

Start with one H1 and a short explanation. Add a link back to the relevant
index. Use sentence case for headings and descriptive link labels.

Link to [stability](status.md) for the shared development and compatibility
boundary. Repeat a warning only when it changes the action at hand: for
example, repack can modify its source, and an ambiguous write can have committed.
Put that warning beside the command or decision it affects.

Use precise terms: **development** for current changeable interfaces,
**experimental** for interfaces with substantial gaps, and **qualification**
for a named validation workflow. State the interface and proof behind a
compatibility, durability, memory, or performance claim.

## Make procedures runnable

- State the working directory, tool version, prerequisite service, and data state.
- Give a complete short example before showing optional configuration.
- Name placeholders explicitly; use an exact revision in installation examples.
- Handle errors and release results, snapshots, sessions, and database handles.
- Verify the outcome independently. A reopen example must read without rewriting.
- Use disposable paths and literal-loopback endpoints for local development.
- Distinguish commands to execute from protocol shapes and pseudocode.
- Retain the original operation identity when explaining ambiguous outcomes.

Use `sh`, `go`, `sql`, `json`, `text`, or `mermaid` on fenced blocks. Keep blank
lines around lists, headings, tables, and fences. Wrap prose around 80–100
columns when practical; leave code, links, and table rows intact when wrapping
would hurt readability.

## Explain design and operating consequences

Use a small Mermaid diagram when ownership or data flow is difficult to
explain linearly. Label the scope: one collection, one group, one physical
node, or the whole cluster. A diagram must preserve meaningful boundaries
such as local/remote dispatch and independent Raft groups.

Tables work well for comparable defaults, operator families, or symptoms.
Avoid a long list of internal symbols where a short explanation would help
more. Put implementation details and decisive tests in a compact **Source map**
at the end of technical guides. Use working relative links, without unstable
line numbers unless a particular line is essential.

## Preserve evidence

Link to a dated report for a measurement or validation result. Record failed,
skipped, and incomplete work accurately, with its actual revision. The current
status page should describe current limits and link historical records.

Keep recorded raw outputs, checksum-bound reports, and failed precursors
unchanged. Add an index or a superseding report rather than silently updating
old numbers. Some frozen reports link to raw files inside a sibling archive;
retain their extraction instructions.

Generated pages are maintained through their source:

| Page | Update path |
| --- | --- |
| `docs/capabilities.md` | `go generate ./internal/conformance` |
| `docs/distributed-feature-state.md` | `go generate ./internal/featurestate` |
| `bench/competitive/COVERAGE.md` | `go generate .` inside `bench/competitive` |
| `UNSAFE.md` inventory | `go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update` |

## Check a documentation change

Install the small Markdown-check dependency set once, preferably in a virtual
environment:

```sh
python3 -m venv /tmp/vibedb-docs-venv
/tmp/vibedb-docs-venv/bin/python -m pip install -r scripts/docs/requirements.txt
make docs-check PYTHON=/tmp/vibedb-docs-venv/bin/python
git diff --check
```

The checker parses all repository Markdown, including reference links and
images. It checks local paths, Markdown heading/HTML anchors, source line
anchors, and authored-page titles/fences. Links into frozen evidence archives
are checked against actual archive members without extracting them. It does
not fetch external URLs or certify technical claims. Frozen evidence pages
are exempt from authored-page title/fence style rules.

Compile complete Go examples and execute changed commands where practical.
Review rendered Markdown for table and diagram readability. The documentation
CI workflow runs the link/structure check on every pull request so source-file
moves cannot silently break the guides.

## Editorial references

The organization follows the concise entry point of
[DuckDB's README](https://github.com/duckdb/duckdb/blob/main/README.md), the
layer-by-layer explanations in
[CockroachDB's architecture guide](https://www.cockroachlabs.com/docs/stable/architecture/overview),
and the task-focused
[TigerBeetle operator guides](https://docs.tigerbeetle.com/operating/).
VibeDB's technical claims must come from its own source and evidence.
