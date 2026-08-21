# Documentation language

This project uses a clear technical language that is informed by ASD-STE100.
The project does not claim formal ASD-STE100 compliance.

The documentation uses these controls:

- Use one industry term for one technical concept.
- Define an uncommon term at its first important use.
- Use active voice when the actor is important.
- Put one instruction in each numbered step.
- Put a condition before the action that depends on it.
- Use short sentences in procedures and safety information.
- Use lists and tables when they make a complex contract easier to scan.
- Use American English spelling.
- Do not use a semicolon.

Software terms are part of the project vocabulary. Examples include MVCC,
WAL, Raft, snapshot, shard, backpressure, and `database/sql`. Do not replace a
precise industry term with a longer general phrase.

Tutorials can use a natural developer voice. A tutorial can explain why a
choice is useful and can connect a sequence of ideas. Reference pages must be
more controlled. They must state exact types, defaults, limits, errors, and
state transitions.

Commands, identifiers, error text, protocol fields, and UI text must match the
source. Do not change them to satisfy a language rule.

## Evidence rules

Each product claim must come from one of these sources:

1. Production code that implements the contract.
2. A test that checks the contract.
3. A generated manifest that production tests also use.

Do not use an old document as the only source for a claim. Add an
`Implementation references` section when a page describes an internal
contract. Use repository-relative paths and stable Go symbols where possible.

Performance numbers need a reproducible result artifact. A benchmark name by
itself is not a performance claim. Record the commit, toolchain, platform,
command, data profile, and raw result when you publish a number.

## Status words

Use these status words consistently:

- **Supported** means that a public entry point and a test exist.
- **Experimental** means that the implementation exists, but its public or
  operational contract can change.
- **Internal** means that the package is not a public compatibility promise.
- **Unsupported** means that the implementation refuses the operation or does
  not provide it.

## Source standard

The official language reference is [ASD-STE100 Simplified Technical English,
Issue 9](https://www.asd-ste100.org/assets/files/ASD-STE100_ISSUE9.pdf).
This project applies its clarity principles with a software-specific
vocabulary. It does not restrict tutorials to the controlled dictionary.
