# Documentation style

[Documentation](README.md) / Contributing

Write concise, decision-oriented documentation grounded in the source beside it.
Update code and its guide in the same change.

## Information architecture

| Content | Home | Shape |
| --- | --- | --- |
| Project overview | [README.md](../README.md) | What VibeDB is, one quickstart, interface table, boundaries |
| Tutorials | `getting-started.md`, `operations/local-cluster.md` | Prerequisites, numbered steps, expected output, reopen check |
| API guides | `docs/api/` | Usage, ownership, errors, runnable examples |
| Architecture & design | `docs/architecture.md`, `docs/design/` | Components, data flow, invariants, tradeoffs, source map |
| Operations | `docs/operations/` | Preconditions, exact commands, success checks, recovery |
| Reference | `docs/reference/` | Exact syntax, flags, wire grammar, defaults, limits |
| Verification | `docs/qualification/`, `docs/benchmarks/` | CI workflows, benchmark harnesses, reproducible methods |

Do not commit raw benchmark dumps, `.pprof` files, task logs, or superseded
work plans into `docs/`. Keep [stability](status.md) as the single home for
shared development disclaimers; repeat a warning only beside the command or
decision it affects.

## Writing and formatting

- Start every authored page with one H1, a breadcrumb link, and a 1–2 sentence
  summary. Use sentence case for headings.
- Use `sh`, `go`, `sql`, `json`, `text`, or `mermaid` on fenced code blocks.
- Keep Mermaid diagrams small and explicitly scoped (one collection, one group,
  one node, or the cluster).
- Put key implementation files and decisive tests in a short **Source map** at
  the end of technical pages using relative links without brittle line numbers.
- State exact behavior: **development** for changeable interfaces,
  **experimental** for incomplete ones, **qualification** for named CI gates.

## Generated documents

Never hand-edit generated pages; regenerate them from source:

| Page | Regenerate command |
| --- | --- |
| `docs/capabilities.md` | `go generate ./internal/conformance` |
| `docs/distributed-feature-state.md` | `go generate ./internal/featurestate` |
| `bench/competitive/COVERAGE.md` | `cd bench/competitive && go generate .` |
| `UNSAFE.md` | `go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update` |

## Verify changes

Run the link and structure checker (also enforced by `.github/workflows/docs.yml`):

```sh
python3 -m venv /tmp/vibedb-docs-venv
/tmp/vibedb-docs-venv/bin/python -m pip install -r scripts/docs/requirements.txt
make docs-check PYTHON=/tmp/vibedb-docs-venv/bin/python
git diff --check
```

`scripts/docs/check.py` verifies all local paths, heading/HTML anchors, source
line anchors, single-H1 titles, and closed code fences across every Markdown
file in the repository.
