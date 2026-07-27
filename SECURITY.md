# Security policy

## Supported revisions

No tagged release exists. Security fixes are made on `main`; consumers must
upgrade to a fixing revision after reviewing it for their deployment.

After releases begin, support policy will be stated per release. No support
window is implied before then.

## Private reports

Use [GitHub private vulnerability
reporting](https://github.com/thesyncim/vibejson/security/advisories/new). Do not
publish exploit details, private inputs, or a reproducer in an issue.

If private reporting is unavailable, open a public issue that asks only for a
private contact channel.

Include:

- affected repository revision;
- Go revision, architecture, and build flags;
- the smallest available reproducer;
- expected confidentiality, integrity, availability, or durability impact.

Relevant reports include parser acceptance errors, out-of-bounds access,
stale/hidden pointers, lifetime violations, data corruption, recovery accepting
an invalid generation, denial of service, and unsafe custom-method dispatch.

Reports remain private while they are reproduced and a fix is validated through
the relevant portable, SIMD, race, checkptr, differential, persistence, and
performance gates. No fixed response or disclosure deadline is promised.
