# Documentation standard

Documentation is part of the executable contract. It should let a reader find
one honest answer quickly, reproduce it, and see where the source of truth
lives.

## Organize by reader intent

Use one dominant content type per page:

| Type | Purpose | Shape |
| --- | --- | --- |
| Tutorial | Learn by completing one safe path | Ordered, runnable, minimal choices |
| How-to | Complete one operational task | Preconditions, steps, verification, rollback/failure |
| Reference | Look up exact behavior | Tables, defaults, syntax, errors, compact examples |
| Explanation | Understand design and tradeoffs | Model, invariants, diagrams, non-guarantees |

Do not turn one page into a tutorial, design diary, roadmap, and symbol dump.
Link to the next layer instead.

## Lead with status and outcome

Every user-facing API, operations, protocol, or format page must state the
development boundary before instructions. Use GitHub admonitions for hazards:

```markdown
> [!CAUTION]
> Unreleased development contract. Pin one exact commit.
```

Use these maturity terms precisely:

- **Development**: present in the current source; may break at any commit.
- **Experimental**: usable for bounded evaluation, with major compatibility or
  qualification gaps.
- **Qualification only**: exists to collect evidence, not to deploy a product.
- **Generated evidence**: derived from a manifest or test; not a support or
  performance claim.

Avoid “shipped.” If a checked-in command constructs a path, say exactly that.
Never use “production-ready,” “PostgreSQL-compatible,” “automatic,” “global
snapshot,” “bounded memory,” or “zero allocation” without naming the exact
surface, mode, and proof.

## Write for scanning

- Lead with the answer or decision.
- Keep paragraphs short and headings descriptive.
- Use a table for repeated exact mappings or defaults.
- Use one small diagram when relationships are harder to explain linearly.
- Put optional internals after the task, not before it.
- Prefer code identifiers only when they help a reader act or verify.
- State copy/borrow, owner, lifetime, concurrency, and release rules together.
- State error atomicity separately from unknown persistence outcomes.

## Evidence rules

Every hand-written technical page ends with a compact **Source map**. Cite the
owning production file and decisive tests; do not narrate hundreds of internal
symbols in the main flow.

Claims must distinguish:

- implementation from integration;
- integration from a checked-in development command;
- a checked-in command from external qualification;
- qualification from a release or service-level promise;
- harness coverage from an executed result;
- a validated artifact inventory from a fair comparative claim.

When a known test fails, say so on `docs/status.md` and narrow any generated
qualification wording. Do not call the overall suite green.

## Examples

- Compile or execute examples when practical.
- Use exact commits instead of `main` in installation instructions.
- Handle `Close`, cancellation, and result release.
- Use disposable paths and literal-loopback addresses in development examples.
- Never copy large generated manifests into tutorials.
- Keep secrets, real hostnames, and user data out of fixtures.

## Links and files

Use relative repository links in Markdown. Link from tutorials to reference,
not the reverse. Keep filenames stable when code comments or tests use them; if
a page moves, update those references in the same commit.

Run a local-link check and `git diff --check` before review.

## Generated pages

The following are generated or contain generated blocks:

- `UNSAFE.md`
- `docs/capabilities.md`
- `docs/distributed-feature-state.md`
- `bench/competitive/COVERAGE.md`

Change their source manifest or renderer, run the documented generator, and
commit the output. Generated prose must still carry the development and claim
boundary.
