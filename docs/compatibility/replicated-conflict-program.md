# Replicated conflict expression format

VUC3 carries a candidate JSON row and a bounded, deterministic conflict program.
It replaces the unreleased VUC2 grammar. The apply-contract
hash binds the grammar and resource limits, so mismatched members cannot share
an apply identity.

All integers are unsigned little endian. The mutation value is `"VUC3"`, a
32-bit candidate byte length, candidate bytes, and program bytes. The program is
a 32-bit template byte length, template bytes, then bound values. Each bound
value is a 32-bit byte length followed by one canonical JSON scalar. Numbers
remain exact decimal text; they never pass through a float conversion.

The template starts with 16-bit assignment and parameter counts. A zero assignment
count denotes whole-document EXCLUDED replacement and requires a WHERE condition.
Each assignment
contains a 16-bit UTF-8 column byte length, column bytes, and a one-byte RHS tag:
0 for a direct operand, 1 for a scalar expression. Parameters use dense 16-bit
ordinals in first-reference order. After the assignments, one predicate tree
encodes the WHERE condition; tag 255 means no condition. A byte per parameter
after that predicate preserves the shared compiler's ParameterType enum. These types are part of the template cache key. Every ordinal
must be referenced; candidate-only parameters are absent. Duplicate target columns and trailing bytes fail.

Scalar, operator, cast, predicate and operand tags are the closed SQL AST enum
values frozen by this version. Node payloads follow the tagged shape in
`sql/driver/replicated_conflict_program.go`; 255 denotes an absent optional scalar,
predicate or path. Paths encode a one-byte source (0=current, 1=EXCLUDED), then a
16-bit UTF-8 column byte length and name. Only declared top-level columns are
valid. Subqueries, aggregates and unknown opcodes are rejected. The shared SQL
compiler remains authoritative for executable scalar/predicate combinations;
a parseable unsupported combination cannot enter apply through binary input.
Changing opcode meanings or executable semantics requires an apply-contract
change, even if the outer mutation envelope remains unchanged.

Encoding and decoding use one bounded structural traversal. Decoding validates
lengths before slicing or allocating, and caps nodes and recursion independently
of byte size. No SQL source or current-row preimage is replicated. Per relation,
one cached template owns its compiled shared projection and reusable execution
workspace. Binding values do not enter the cache key and are cleared after use.
Template replacement releases the prior projection and workspace.

Candidate, declaration and bind validation also run on the insert branch.
On an existing row, the WHERE condition runs once through the shared SQL filter
stage, before any assignment. Only TRUE selects the update; FALSE and UNKNOWN
produce zero affected rows and skip every RHS. The absent-row branch inserts the
candidate without evaluating the condition or assignments. Every expression sees
the same immutable pair of current and candidate rows. The shared patcher collects all
results before changing any column. The replicated state machine canonicalizes
and validates the final document, checks key/owner invariants, and publishes the
batch atomically. A skipped update returns the current row for these checks,
including the current ownership range after a transfer, before no-op elimination.
An executed assignment that produces the same value still counts as one affected
row. Retained terminal results prevent retries from evaluating an
increment or a skipped condition again; participant prepare retains the
mutation's branch-aware input. Conditions require no coordinator preimage read
and do not broaden routing beyond the candidate's owner.

The limits are 4 MiB per mutation, 1,024 assignments, 1,024 referenced parameters,
16,384 expression/operand nodes, and depth 128. Execution workspace, result,
intermediate, and exact-number budgets are each 16 MiB, with the relation's
separate document bound. These limits are deterministic apply semantics, not
node-local tunables. Global-index maintenance and RETURNING terminal payloads
remain separate distributed parity gaps. Replica-side expression failures currently
use the existing invalid-mutation terminal result; preserving detailed SQL error
codes and positions requires the terminal-result protocol to carry them.
