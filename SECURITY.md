# Security policy

## Supported revisions

This repository has no tagged release and no support window. Security fixes
land on `main`. Consumers must move to a fixing commit after they review and
test it for their deployment.

## Send a private report

Use [GitHub private vulnerability
reporting](https://github.com/thesyncim/vibedb/security/advisories/new). Do not
put exploit details, private data, or a reproducer in a public issue.

If private reporting is unavailable, open a public issue that asks for a
private contact channel. Do not include sensitive details in that issue.

Include this information in the private report:

- The affected commit
- Go version, architecture, operating system, and build flags
- The smallest available reproducer
- The expected confidentiality, integrity, availability, or durability impact
- A safe method to validate a fix

Relevant reports include:

- Authentication or authorization bypass
- SQL, JSON, catalog, or protocol validation errors
- Out-of-bounds access or unsafe-pointer lifetime errors
- Data corruption or invalid recovery acceptance
- Unbounded resource use or denial of service
- A stale topology or identity fence that permits unintended execution
- A durability acknowledgement that does not match the selected contract

The project does not promise a fixed response or disclosure deadline. Reports
stay private while maintainers reproduce the issue and validate a fix.

## Network boundary

The gateway NDJSON protocol and shard wire protocol have no authentication or
TLS. Their shipped commands refuse non-loopback listeners. Do not use a proxy,
container mapping, or port forward to expose them to an untrusted network.

The internal Raft transport package validates frames only. It does not provide
a socket, TLS, or peer authentication. An external transport must authenticate
the node identity before frame decode.

## Local-file boundary

Writer locks coordinate cooperating VibeDB processes. They do not protect a
file from an external process that truncates, replaces, copies, or edits it.
Keep database files, journals, lock entries, catalogs, and parent directories
inside the same trusted administrative boundary.
